// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use super::data_evolution_reader::DataEvolutionReader;
use super::data_file_reader::DataFileReader;
use super::kv_file_reader::{KeyValueFileReader, KeyValueReadConfig};
use super::merge_tree_split_generator::{
    has_key_overlap, is_raw_convertible_file_group, split_requires_merge, KeyComparator,
};
use super::read_builder::split_scan_predicates;
use super::{ArrowRecordBatchStream, Table};
use crate::arrow::filtering::reader_pruning_predicates;
use crate::spec::{
    CoreOptions, DataField, DataType, MergeEngine, Predicate, TinyIntType, VALUE_KIND_FIELD_ID,
    VALUE_KIND_FIELD_NAME,
};
use crate::DataSplit;

/// Table read: reads data from splits (e.g. produced by [TableScan::plan]).
///
/// Reference: [pypaimon.read.table_read.TableRead](https://github.com/apache/paimon/blob/master/paimon-python/pypaimon/read/table_read.py)
#[derive(Debug, Clone)]
pub struct TableRead<'a> {
    table: &'a Table,
    read_type: Vec<DataField>,
    data_predicates: Vec<Predicate>,
}

impl<'a> TableRead<'a> {
    /// Create a new TableRead with a specific read type (projected fields).
    pub fn new(
        table: &'a Table,
        read_type: Vec<DataField>,
        data_predicates: Vec<Predicate>,
    ) -> Self {
        Self {
            table,
            read_type,
            data_predicates,
        }
    }

    /// Schema (fields) that this read will produce.
    pub fn read_type(&self) -> &[DataField] {
        &self.read_type
    }

    /// Data predicates for read-side pruning.
    pub fn data_predicates(&self) -> &[Predicate] {
        &self.data_predicates
    }

    /// Table for this read.
    pub fn table(&self) -> &Table {
        self.table
    }

    /// Set a filter predicate for conservative read-side pruning.
    pub fn with_filter(mut self, filter: Predicate) -> Self {
        let (_, data_predicates) = split_scan_predicates(self.table, filter);
        self.data_predicates = reader_pruning_predicates(data_predicates);
        self
    }

    /// Returns an [`ArrowRecordBatchStream`].
    pub fn to_arrow(&self, data_splits: &[DataSplit]) -> crate::Result<ArrowRecordBatchStream> {
        let has_primary_keys = !self.table.schema.primary_keys().is_empty();
        let core_options = CoreOptions::new(self.table.schema.options());
        let merge_engine = core_options.merge_engine()?;

        // PK table with Deduplicate engine: splits that may hold multiple
        // versions of a key (level-0 files or key-overlapping compacted
        // files) need KeyValueFileReader for sort-merge dedup; splits of
        // disjoint compacted files — and all compacted files of
        // deletion-vector tables, where DVs mask stale versions — use the
        // faster DataFileReader.
        // PartialUpdate / VersionedPartialUpdate / Aggregation always go
        // through KeyValueFileReader so per-key materialization runs on read.
        if has_primary_keys
            && matches!(
                merge_engine,
                MergeEngine::Deduplicate
                    | MergeEngine::PartialUpdate
                    | MergeEngine::VersionedPartialUpdate
                    | MergeEngine::Aggregation
            )
        {
            return self.read_pk(data_splits, &core_options);
        }

        if core_options.data_evolution_enabled() {
            self.read_with_evolution(data_splits, &core_options)
        } else {
            self.read_raw(data_splits)
        }
    }

    /// Read a PK table (Deduplicate / PartialUpdate / VersionedPartialUpdate /
    /// Aggregation).
    ///
    /// Routes splits to either KeyValueFileReader (sort-merge) or DataFileReader
    /// (raw read with `_VALUE_KIND` drop_deletes filter) using Java
    /// `MergeTreeSplitGenerator.java:69-81@e8938f347` rawConvertible semantics:
    /// a split is raw-convertible iff every file is L1+ AND has no residual
    /// DELETE rows AND keys do not overlap. Otherwise it must go through
    /// sort-merge — including DV-enabled splits with L0 files (DV+FRESHNESS),
    /// since Java does not treat those as rawConvertible regardless of DV mode.
    ///
    /// PartialUpdate / VersionedPartialUpdate / Aggregation currently take a
    /// short-circuit to sort-merge for *every* split — allowing L1+ raw splits
    /// would bypass the partial-column / MV / per-key aggregate merge that
    /// produces the user-visible row.
    fn read_pk(
        &self,
        data_splits: &[DataSplit],
        core_options: &CoreOptions,
    ) -> crate::Result<ArrowRecordBatchStream> {
        // PartialUpdate / VersionedPartialUpdate / Aggregation require sort-merge
        // on *every* split (no raw fast path) — letting L1+ raw splits bypass
        // sort-merge would expose un-merged partial columns / DELETE rows / MV
        // intermediate state / un-aggregated per-key rows. Future optimisation
        // should prove compacted files are fully materialised before opening
        // that path.
        if matches!(
            core_options.merge_engine()?,
            MergeEngine::PartialUpdate
                | MergeEngine::VersionedPartialUpdate
                | MergeEngine::Aggregation
        ) {
            return self.read_kv(data_splits, core_options);
        }

        // Mirror Java `MergeTreeSplitGenerator.java:69-81@e8938f347` rawConvertible
        // semantics on the reader side: a split is raw-convertible iff every
        // file is L1+ AND has no residual DELETE rows (`withoutDeleteRow`).
        // Even DV-enabled splits route to KV sort-merge when `delete_row_count
        // > 0` or any L0 file is present, because Java treats those as
        // non-rawConvertible regardless of DV mode.
        let comparator = KeyComparator::from_table_schema(self.table.schema());

        let mut kv_splits = Vec::new();
        let mut raw_splits = Vec::new();
        for split in data_splits {
            let raw_convertible = is_raw_convertible_file_group(split.data_files());
            let needs_merge = !raw_convertible
                || match comparator {
                    Some(ref c) => has_key_overlap(split.data_files(), c),
                    // No comparator (unsupported PK type) → fall back to
                    // merging rather than risk raw-reading duplicate keys.
                    None => true,
                };
            if needs_merge {
                kv_splits.push(split.clone());
            } else {
                // Stage 4a contract: raw splits are non-overlapping L1+ with no
                // residual DELETE rows. The check above already enforces
                // `is_raw_convertible_file_group` + `!has_key_overlap`; the
                // assertion is a belt-and-suspenders guard for contract
                // violations in tests/CI without runtime overhead in release.
                if let Some(ref cmp) = comparator {
                    debug_assert!(
                        !split_requires_merge(split.data_files(), cmp),
                        "Stage 4a contract: PK raw-read split must not require merge \
                         (planner should not produce overlapping L1+ for raw dispatch)"
                    );
                }
                raw_splits.push(split.clone());
            }
        }

        if raw_splits.is_empty() {
            return self.read_kv(&kv_splits, core_options);
        }
        if kv_splits.is_empty() {
            return self.read_pk_raw_drop_deletes(&raw_splits);
        }

        let kv_stream = self.read_kv(&kv_splits, core_options)?;
        let raw_stream = self.read_pk_raw_drop_deletes(&raw_splits)?;
        Ok(Box::pin(futures::stream::select_all([
            kv_stream, raw_stream,
        ])))
    }

    /// Read splits via KeyValueFileReader (sort-merge dedup).
    fn read_kv(
        &self,
        splits: &[DataSplit],
        core_options: &CoreOptions,
    ) -> crate::Result<ArrowRecordBatchStream> {
        let reader = KeyValueFileReader::new(
            self.table.file_io.clone(),
            KeyValueReadConfig {
                table_name: self.table.identifier().full_name(),
                table_options: self.table.schema().options().clone(),
                schema_manager: self.table.schema_manager().clone(),
                table_schema_id: self.table.schema().id(),
                table_fields: self.table.schema.fields().to_vec(),
                read_type: self.read_type().to_vec(),
                predicates: self.data_predicates.clone(),
                primary_keys: self.table.schema.trimmed_primary_keys(),
                merge_engine: core_options.merge_engine()?,
                sequence_fields: core_options
                    .sequence_fields()
                    .iter()
                    .map(|s| s.to_string())
                    .collect(),
                batch_size: core_options.read_batch_size(),
                parquet_page_index_enabled: core_options.parquet_page_index_enabled(),
            },
        );
        reader.read(splits)
    }

    /// Read with data-evolution support.
    fn read_with_evolution(
        &self,
        data_splits: &[DataSplit],
        core_options: &CoreOptions,
    ) -> crate::Result<ArrowRecordBatchStream> {
        let reader = DataEvolutionReader::new(
            self.table.file_io.clone(),
            self.table.schema_manager().clone(),
            self.table.schema().id(),
            self.table.schema.fields().to_vec(),
            self.read_type().to_vec(),
            core_options.blob_as_descriptor(),
            core_options.blob_descriptor_fields(),
            core_options.read_batch_size(),
            core_options.parquet_page_index_enabled(),
        )?;
        reader.read(data_splits)
    }

    /// Read raw data files without dedup or evolution.
    fn read_raw(&self, data_splits: &[DataSplit]) -> crate::Result<ArrowRecordBatchStream> {
        self.new_data_file_reader().read(data_splits)
    }

    /// Stage 4a: PK raw-read short-circuit with `_VALUE_KIND` filter.
    ///
    /// Used when `read_pk` dispatch routes a split through the raw path
    /// (full L1+ + key non-overlapping). The `DataFileReader` is configured
    /// to read `_VALUE_KIND` from the parquet file and post-filter rows
    /// whose `RowKind` is not `is_add()` — i.e. drop residual DELETE /
    /// UPDATE_BEFORE physical rows that the sort-merge path would otherwise
    /// strip via `DropDeleteReader`. The `_VALUE_KIND` column itself is
    /// removed from the user-visible output.
    fn read_pk_raw_drop_deletes(
        &self,
        data_splits: &[DataSplit],
    ) -> crate::Result<ArrowRecordBatchStream> {
        self.new_data_file_reader_drop_deletes().read(data_splits)
    }

    fn new_data_file_reader(&self) -> DataFileReader {
        let core_options = CoreOptions::new(self.table.schema.options());
        DataFileReader::new(
            self.table.file_io.clone(),
            self.table.schema_manager().clone(),
            self.table.schema().id(),
            self.table.schema.fields().to_vec(),
            self.read_type().to_vec(),
            self.data_predicates.clone(),
            core_options.read_batch_size(),
            core_options.parquet_page_index_enabled(),
        )
    }

    /// Mirror of [`new_data_file_reader`] for the PK raw-read short-circuit:
    /// prepends `_VALUE_KIND` to `read_type` so the parquet reader actually
    /// loads the column, then enables `with_drop_deletes(true)` so the
    /// post-decode filter removes DELETE / UPDATE_BEFORE rows and drops the
    /// `_VALUE_KIND` column from the yielded batches.
    fn new_data_file_reader_drop_deletes(&self) -> DataFileReader {
        let core_options = CoreOptions::new(self.table.schema.options());
        let read_type = raw_read_type_with_value_kind(self.read_type());
        DataFileReader::new(
            self.table.file_io.clone(),
            self.table.schema_manager().clone(),
            self.table.schema().id(),
            self.table.schema.fields().to_vec(),
            read_type,
            self.data_predicates.clone(),
            core_options.read_batch_size(),
            core_options.parquet_page_index_enabled(),
        )
        .with_drop_deletes(true)
    }
}

/// Build a `read_type` for the PK raw-read short-circuit by prepending the
/// `_VALUE_KIND` field (TinyInt) to the user's projection. The added column
/// is consumed by [`DataFileReader::with_drop_deletes`] and removed from the
/// yielded batches; callers do not see it in the output.
fn raw_read_type_with_value_kind(user_read_type: &[DataField]) -> Vec<DataField> {
    let mut fields = Vec::with_capacity(user_read_type.len() + 1);
    fields.push(DataField::new(
        VALUE_KIND_FIELD_ID,
        VALUE_KIND_FIELD_NAME.to_string(),
        DataType::TinyInt(TinyIntType::new()),
    ));
    fields.extend_from_slice(user_read_type);
    fields
}

#[cfg(test)]
mod tests {
    //! Stage 4a dispatch tests: exercise the raw-vs-sort-merge routing
    //! decision in `read_pk` via its underlying helpers
    //! (`is_raw_convertible_file_group` + `has_key_overlap`). These are
    //! release-mode safety nets — without them, the previous DV-only
    //! `level == 0` check in `read_pk` would have routed an L1+ split with
    //! `delete_row_count > 0` (or PK overlap) to raw read, contradicting
    //! Java `MergeTreeSplitGenerator.java:69-81@e8938f347` rawConvertible
    //! semantics. The earlier `debug_assert!(!split_requires_merge(...))`
    //! fires only in debug builds; these tests prove the routing decision
    //! is correct in release as well.
    use crate::spec::stats::BinaryTableStats;
    use crate::spec::{BinaryRowBuilder, DataFileMeta, DataType, IntType};
    use crate::table::merge_tree_split_generator::{
        has_key_overlap, is_raw_convertible_file_group, KeyComparator,
    };
    use chrono::{DateTime, Utc};

    fn int_key(value: i32) -> Vec<u8> {
        let mut builder = BinaryRowBuilder::new(1);
        builder.write_int(0, value);
        builder.build_serialized()
    }

    fn int_comparator() -> KeyComparator {
        KeyComparator::new(vec![DataType::Int(IntType::new())])
    }

    fn keyed_file(name: &str, min: i32, max: i32, level: i32) -> DataFileMeta {
        DataFileMeta {
            file_name: name.to_string(),
            file_size: 100,
            row_count: 100,
            min_key: int_key(min),
            max_key: int_key(max),
            key_stats: BinaryTableStats::new(Vec::new(), Vec::new(), Vec::new()),
            value_stats: BinaryTableStats::new(Vec::new(), Vec::new(), Vec::new()),
            min_sequence_number: 0,
            max_sequence_number: 0,
            schema_id: 0,
            level,
            extra_files: Vec::new(),
            creation_time: DateTime::<Utc>::from_timestamp(0, 0),
            delete_row_count: Some(0),
            embedded_index: None,
            first_row_id: None,
            write_cols: None,
            external_path: None,
            file_source: None,
            value_stats_cols: None,
            commit_snapshot_id: None,
            merge_mode: None,
        }
    }

    /// Mirrors the dispatch decision in `read_pk`: a split is raw-eligible
    /// iff `is_raw_convertible_file_group` AND not `has_key_overlap`.
    /// Otherwise it must be routed to KV sort-merge.
    fn read_pk_needs_merge(files: &[DataFileMeta], comparator: Option<&KeyComparator>) -> bool {
        let raw_convertible = is_raw_convertible_file_group(files);
        !raw_convertible
            || match comparator {
                Some(c) => has_key_overlap(files, c),
                None => true,
            }
    }

    /// Two L1 files with overlapping PK ranges must route to sort-merge,
    /// not raw read. Pre-fix the DV-enabled branch only checked
    /// `level == 0`, which would have raw-read this overlap split and
    /// emitted duplicate keys.
    #[test]
    fn test_read_pk_routes_overlap_l1_split_to_kv() {
        let comparator = int_comparator();
        let files = vec![
            keyed_file("l1-a", 1, 50, 1),
            keyed_file("l1-b", 40, 90, 2), // overlaps [40, 50] with l1-a
        ];
        assert!(
            read_pk_needs_merge(&files, Some(&comparator)),
            "overlap-L1 split must route to KV sort-merge, not raw read"
        );
    }

    /// L1 file with `delete_row_count > 0` must route to sort-merge.
    /// Pre-fix the DV-enabled branch ignored `delete_row_count` entirely
    /// and relied on Stage 4a `drop_deletes` for residual DELETE rows;
    /// Java `MergeTreeSplitGenerator.java:69-81@e8938f347` treats this as
    /// non-rawConvertible and refuses to fall into raw dispatch.
    #[test]
    fn test_read_pk_routes_l1_with_delete_row_count_to_kv() {
        let comparator = int_comparator();
        let mut dirty = keyed_file("l1-dirty", 1, 50, 1);
        dirty.delete_row_count = Some(3);
        assert!(
            read_pk_needs_merge(&[dirty], Some(&comparator)),
            "L1 file with delete_row_count > 0 must route to KV sort-merge"
        );
    }

    /// Disjoint L1+ files with no DELETE rows can be read raw — the only
    /// case where the raw path is taken under the new contract.
    #[test]
    fn test_read_pk_routes_clean_l1_split_to_raw() {
        let comparator = int_comparator();
        let files = vec![keyed_file("l1-a", 1, 30, 1), keyed_file("l2-b", 31, 90, 2)];
        assert!(
            !read_pk_needs_merge(&files, Some(&comparator)),
            "disjoint L1+ split with no DELETE rows must take the raw path"
        );
    }

    /// A split that contains any L0 file must route to sort-merge regardless
    /// of DV state. This is the pre-fix DV-enabled branch's old assumption,
    /// preserved by `is_raw_convertible_file_group` (every file must be
    /// `level != 0`).
    #[test]
    fn test_read_pk_routes_any_l0_split_to_kv() {
        let comparator = int_comparator();
        let files = vec![keyed_file("l0", 1, 10, 0), keyed_file("l1", 11, 20, 1)];
        assert!(
            read_pk_needs_merge(&files, Some(&comparator)),
            "L0 in split must route to KV sort-merge"
        );
    }

    /// PK comparator unavailable (unsupported PK type) → fall back to
    /// merging rather than risk raw-reading duplicate keys.
    #[test]
    fn test_read_pk_no_comparator_falls_back_to_merge() {
        let files = vec![keyed_file("clean-l1", 1, 30, 1)];
        assert!(
            read_pk_needs_merge(&files, None),
            "no PK comparator → fall back to KV sort-merge"
        );
    }
}
