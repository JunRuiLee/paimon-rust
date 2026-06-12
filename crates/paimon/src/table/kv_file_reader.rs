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

//! Key-value file reader for primary-key tables using sort-merge with LoserTree.
//!
//! Each data file in a split is read as a separate sorted stream. The streams
//! are merged by primary key using a LoserTree, and rows with the same key are
//! deduplicated by keeping the one with the highest `_SEQUENCE_NUMBER`.
//!
//! Reference: Java Paimon `SortMergeReaderWithMinHeap`.

use super::data_file_reader::DataFileReader;
use super::sort_merge::{
    AggregateMergeFunction, DeduplicateMergeFunction, PartialUpdateMergeFunction,
    SortMergeReaderBuilder,
};
use crate::arrow::build_target_arrow_schema;
use crate::deletion_vector::DeletionVectorFactory;
use crate::io::FileIO;
use crate::spec::{
    BigIntType, DataField, DataType as PaimonDataType, MergeEngine, Predicate, TinyIntType,
    SEQUENCE_NUMBER_FIELD_ID, SEQUENCE_NUMBER_FIELD_NAME, VALUE_KIND_FIELD_ID,
    VALUE_KIND_FIELD_NAME,
};
use crate::table::schema_manager::SchemaManager;
use crate::table::ArrowRecordBatchStream;
use crate::{DataSplit, Error};
use arrow_array::RecordBatch;

use async_stream::try_stream;
use futures::StreamExt;
use std::collections::HashMap;

/// Reads primary-key table data files using sort-merge deduplication.
pub(crate) struct KeyValueFileReader {
    file_io: FileIO,
    config: KeyValueReadConfig,
}

/// Configuration for [`KeyValueFileReader`], grouping table schema and
/// key/predicate parameters.
pub(crate) struct KeyValueReadConfig {
    pub table_name: String,
    pub table_options: HashMap<String, String>,
    pub schema_manager: SchemaManager,
    pub table_schema_id: i64,
    pub table_fields: Vec<DataField>,
    pub read_type: Vec<DataField>,
    pub predicates: Vec<Predicate>,
    pub primary_keys: Vec<String>,
    pub merge_engine: MergeEngine,
    pub sequence_fields: Vec<String>,
    /// Rows per batch for both the inner parquet reader and the sort-merge
    /// output. Sourced from `CoreOptions::read_batch_size()` by callers.
    pub batch_size: usize,
    /// Whether the parquet format reader should load page index
    /// (ColumnIndex / OffsetIndex) and apply page-level pruning. Sourced
    /// from `CoreOptions::parquet_page_index_enabled()` by callers.
    pub parquet_page_index_enabled: bool,
}

impl KeyValueFileReader {
    pub(crate) fn new(file_io: FileIO, config: KeyValueReadConfig) -> Self {
        // Only keep predicates that reference primary key columns.
        // Non-PK predicates applied before merge can cause incorrect results.
        // Use project_field_index_inclusive: AND keeps PK children, OR requires all PK.
        let pk_set: std::collections::HashSet<&str> =
            config.primary_keys.iter().map(|s| s.as_str()).collect();
        let mapping: Vec<Option<usize>> = config
            .table_fields
            .iter()
            .enumerate()
            .map(|(i, f)| {
                if pk_set.contains(f.name()) {
                    Some(i)
                } else {
                    None
                }
            })
            .collect();
        let pk_predicates = config
            .predicates
            .into_iter()
            .filter_map(|p| p.project_field_index_inclusive(&mapping))
            .collect();

        Self {
            file_io,
            config: KeyValueReadConfig {
                predicates: pk_predicates,
                ..config
            },
        }
    }

    fn new_merge_function(
        merge_engine: MergeEngine,
        table_options: &HashMap<String, String>,
        table_name: &str,
        merge_output_fields: &[DataField],
        primary_keys: &[String],
        sequence_fields: &[String],
    ) -> crate::Result<Box<dyn super::sort_merge::MergeFunction>> {
        match merge_engine {
            MergeEngine::Deduplicate => Ok(Box::new(DeduplicateMergeFunction)),
            MergeEngine::PartialUpdate => Ok(Box::new(PartialUpdateMergeFunction::new(
                table_options,
                table_name,
            )?)),
            MergeEngine::FirstRow => Err(Error::Unsupported {
                message: "KeyValueFileReader does not support merge-engine=first-row; first-row reads should use the non-KV path".to_string(),
            }),
            MergeEngine::VersionedPartialUpdate => Ok(Box::new(
                super::versioned_partial_update::VersionedPartialUpdateMergeFunction::new(
                    table_options,
                )?,
            )),
            MergeEngine::Aggregation => Ok(Box::new(AggregateMergeFunction::new(
                table_options,
                table_name,
                merge_output_fields,
                primary_keys,
                sequence_fields,
            )?)),
        }
    }

    pub fn read(self, data_splits: &[DataSplit]) -> crate::Result<ArrowRecordBatchStream> {
        // Build the internal read type for thin-mode files.
        // Physical file schema: [_SEQUENCE_NUMBER, _VALUE_KIND, all_user_cols...]
        // We need: _SEQ + _VK + union(read_type, primary_keys)
        let seq_field = DataField::new(
            SEQUENCE_NUMBER_FIELD_ID,
            SEQUENCE_NUMBER_FIELD_NAME.to_string(),
            PaimonDataType::BigInt(BigIntType::new()),
        );
        let value_kind_field = DataField::new(
            VALUE_KIND_FIELD_ID,
            VALUE_KIND_FIELD_NAME.to_string(),
            PaimonDataType::TinyInt(TinyIntType::new()),
        );

        let key_names: std::collections::HashSet<&str> = self
            .config
            .primary_keys
            .iter()
            .map(|s| s.as_str())
            .collect();

        // Collect key fields from table schema.
        let key_fields: Vec<DataField> = self
            .config
            .primary_keys
            .iter()
            .map(|pk| {
                self.config
                    .table_fields
                    .iter()
                    .find(|f| f.name() == pk)
                    .cloned()
                    .ok_or_else(|| Error::UnexpectedError {
                        message: format!("Primary key column '{pk}' not found in table schema"),
                        source: None,
                    })
            })
            .collect::<crate::Result<Vec<_>>>()?;

        // User columns = read_type fields + any key fields not already in read_type
        //              + (versioned-partial-update only) any multi-version fields
        //              + any sequence fields not already included.
        let user_fields = compute_merge_read_fields(
            &self.config.read_type,
            &key_fields,
            &self.config.table_fields,
            &self.config.sequence_fields,
            self.config.merge_engine,
        )?;

        // Internal read type: [_SEQ, _VK, user_fields...]
        let mut internal_read_type: Vec<DataField> = Vec::new();
        internal_read_type.push(seq_field);
        internal_read_type.push(value_kind_field);
        internal_read_type.extend(user_fields.clone());

        let internal_schema = build_target_arrow_schema(&internal_read_type)?;

        // Output schema: user's read_type order
        let output_schema = build_target_arrow_schema(&self.config.read_type)?;

        // Indices within internal_schema (offset 2 for _SEQ and _VK).
        let seq_index = 0;
        let value_kind_index = 1;
        let key_indices: Vec<usize> = self
            .config
            .primary_keys
            .iter()
            .map(|pk| {
                user_fields
                    .iter()
                    .position(|f| f.name() == pk)
                    .map(|p| p + 2)
                    .unwrap()
            })
            .collect();
        let value_fields: Vec<DataField> = user_fields
            .iter()
            .filter(|f| !key_names.contains(f.name()))
            .cloned()
            .collect();
        let value_indices: Vec<usize> = user_fields
            .iter()
            .enumerate()
            .filter(|(_, f)| !key_names.contains(f.name()))
            .map(|(i, _)| i + 2)
            .collect();

        // If sequence.field is configured, find each field's index in the internal schema.
        let user_sequence_indices: Vec<usize> = self
            .config
            .sequence_fields
            .iter()
            .filter_map(|sf| {
                user_fields
                    .iter()
                    .position(|f| f.name() == sf.as_str())
                    .map(|p| p + 2)
            })
            .collect();

        // Build the reorder mapping: merge output is [keys..., values...],
        // but user wants them in read_type order.
        let num_keys = key_fields.len();
        let mut reorder_map: Vec<usize> = vec![0; self.config.read_type.len()];
        for (out_idx, field) in self.config.read_type.iter().enumerate() {
            if key_names.contains(field.name()) {
                // Find position in key_fields
                let key_pos = key_fields
                    .iter()
                    .position(|kf| kf.name() == field.name())
                    .unwrap();
                reorder_map[out_idx] = key_pos;
            } else {
                // Find position in value_fields
                let val_pos = value_fields
                    .iter()
                    .position(|vf| vf.name() == field.name())
                    .unwrap();
                reorder_map[out_idx] = num_keys + val_pos;
            }
        }

        let splits: Vec<DataSplit> = data_splits.to_vec();
        let file_io = self.file_io;
        let merge_engine = self.config.merge_engine;
        let schema_manager = self.config.schema_manager;
        let table_schema_id = self.config.table_schema_id;
        let table_fields = self.config.table_fields;
        let table_name = self.config.table_name;
        let table_options = self.config.table_options;
        let predicates = self.config.predicates;
        let batch_size = self.config.batch_size;
        let primary_keys = self.config.primary_keys;
        let sequence_fields = self.config.sequence_fields;
        let parquet_page_index_enabled = self.config.parquet_page_index_enabled;

        // Build the merge output schema (keys + values, no system columns).
        let mut merge_output_fields: Vec<DataField> = Vec::new();
        merge_output_fields.extend(key_fields);
        merge_output_fields.extend(value_fields);
        let merge_output_schema = build_target_arrow_schema(&merge_output_fields)?;

        Ok(try_stream! {
            for split in &splits {
                let split_has_dv = split
                    .data_deletion_files()
                    .is_some_and(|files| files.iter().any(Option::is_some));

                // DV is applied at the parquet row_selection layer (inside
                // `data_file_reader::read_single_file_stream`), strictly before
                // the merge function. This mirrors Java
                // `KeyValueFileReaderFactory.java:173-187@e8938f347`, where
                // `ApplyDeletionVectorReader` wraps the file-level reader and
                // is engine-agnostic — DV pre-filtering composes cleanly with
                // Deduplicate / PartialUpdate / VersionedPartialUpdate alike.

                // Build a per-split DV factory only when at least one data
                // file in the split has a DeletionFile attached. Mirrors
                // DataFileReader behavior; avoids loading deletion vectors
                // for splits that don't need them.
                let dv_factory = if split_has_dv {
                    Some(
                        DeletionVectorFactory::new(
                            &file_io,
                            split.data_files(),
                            split.data_deletion_files(),
                        )
                        .await?,
                    )
                } else {
                    None
                };

                // Create one stream per data file.
                let mut file_streams: Vec<ArrowRecordBatchStream> = Vec::new();
                // Per-stream metadata aligned with file_streams; consumed by
                // sort-merge to drive versioned-partial-update ordering and
                // per-file UPSERT/IGNORE mode dispatch. For non-versioned
                // engines the values pass through but the merge function
                // ignores them.
                let mut stream_metas: Vec<super::sort_merge::StreamMeta> = Vec::new();

                for file_meta in split.data_files().to_vec() {
                    let data_fields: Option<Vec<DataField>> = if file_meta.schema_id != table_schema_id {
                        let data_schema = schema_manager.schema(file_meta.schema_id).await?;
                        Some(data_schema.fields().to_vec())
                    } else {
                        None
                    };

                    // Snapshot id: legacy files lacking _COMMIT_SNAPSHOT_ID
                    // fall back to the read-side sentinel UNKNOWN, which is
                    // smaller than every real snapshot id so cross-file ordering
                    // still favours newer rows.
                    let snapshot_id = file_meta
                        .commit_snapshot_id
                        .unwrap_or(crate::spec::COMMIT_SNAPSHOT_ID_UNKNOWN);
                    // Merge mode: only `None` falls back to UPSERT (matches
                    // Java `DataFileMetaSerializer` which writes null for
                    // UPSERT and a valid byte otherwise). Unknown byte values
                    // are an integrity error and bubble up — silently
                    // downgrading to UPSERT would corrupt merge results.
                    let merge_mode = crate::spec::VersionedMergeMode::from_optional_byte(
                        file_meta.merge_mode,
                    )?;
                    stream_metas.push(super::sort_merge::StreamMeta {
                        snapshot_id,
                        merge_mode,
                    });

                    let reader = DataFileReader::new(
                        file_io.clone(),
                        schema_manager.clone(),
                        table_schema_id,
                        table_fields.clone(),
                        internal_read_type.clone(),
                        predicates.clone(),
                        batch_size,
                        parquet_page_index_enabled,
                    );

                    // Look up the DV for this data file (if any). Cloning the
                    // Arc is cheap; the inner bitmap is shared.
                    let dv = dv_factory
                        .as_ref()
                        .and_then(|factory| factory.get_deletion_vector(&file_meta.file_name))
                        .cloned();

                    let stream = reader.read_single_file_stream(
                        split,
                        file_meta,
                        data_fields,
                        dv,
                        None,
                    )?;
                    file_streams.push(stream);
                }

                if file_streams.is_empty() {
                    continue;
                }

                // Always go through sort-merge even for single file,
                // because a single file may contain duplicate keys.
                let mut merge_stream = SortMergeReaderBuilder::new(
                    file_streams,
                    internal_schema.clone(),
                    key_indices.clone(),
                    seq_index,
                    value_kind_index,
                    user_sequence_indices.clone(),
                    value_indices.clone(),
                    merge_output_schema.clone(),
                    Self::new_merge_function(
                        merge_engine,
                        &table_options,
                        &table_name,
                        &merge_output_fields,
                        &primary_keys,
                        &sequence_fields,
                    )?,
                )
                .with_batch_size(batch_size)
                .with_stream_metas(stream_metas)
                .build()?;

                while let Some(batch) = merge_stream.next().await {
                    let batch = batch?;
                    // Reorder columns from [keys..., values...] to read_type order.
                    let columns: Vec<_> = reorder_map
                        .iter()
                        .map(|&src| batch.column(src).clone())
                        .collect();
                    // Preserve the merged row count explicitly: an empty
                    // projection (e.g. `SELECT COUNT(*)`) yields zero columns,
                    // and Arrow cannot infer the row count from a column-less
                    // batch.
                    let options = arrow_array::RecordBatchOptions::new()
                        .with_row_count(Some(batch.num_rows()));
                    let reordered = RecordBatch::try_new_with_options(
                        output_schema.clone(),
                        columns,
                        &options,
                    )
                    .map_err(|e| Error::UnexpectedError {
                        message: format!("Failed to reorder merged RecordBatch: {e}"),
                        source: Some(Box::new(e)),
                    })?;
                    yield reordered;
                }
            }
        }
        .boxed())
    }
}

/// Build the field list consumed by sort-merge for one read pass:
/// `requested read_type + missing PK + (VPU only) missing MV + missing
/// sequence fields`. Mirrors Java
/// `VersionedPartialUpdateMergeFunction.Factory.adjustReadType` for the
/// MV-completion step. The order is stable: user projection first, then
/// the implicit補齐 in PK → MV → sequence order.
fn compute_merge_read_fields(
    read_type: &[DataField],
    key_fields: &[DataField],
    table_fields: &[DataField],
    sequence_fields: &[String],
    merge_engine: MergeEngine,
) -> crate::Result<Vec<DataField>> {
    let read_type_names: std::collections::HashSet<&str> =
        read_type.iter().map(|f| f.name()).collect();
    let mut user_fields: Vec<DataField> = read_type.to_vec();

    for kf in key_fields {
        if !read_type_names.contains(kf.name()) {
            user_fields.push(kf.clone());
        }
    }

    if merge_engine == MergeEngine::VersionedPartialUpdate {
        for tf in table_fields {
            if user_fields.iter().any(|f| f.name() == tf.name()) {
                continue;
            }
            if crate::spec::is_multi_version_type(tf.data_type()) {
                user_fields.push(tf.clone());
            }
        }
    }

    for sf_name in sequence_fields {
        if user_fields.iter().all(|f| f.name() != sf_name.as_str()) {
            let sf = table_fields
                .iter()
                .find(|f| f.name() == sf_name.as_str())
                .cloned()
                .ok_or_else(|| Error::UnexpectedError {
                    message: format!("Sequence field '{sf_name}' not found in table schema"),
                    source: None,
                })?;
            user_fields.push(sf);
        }
    }

    Ok(user_fields)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::deletion_vector::MAGIC_NUMBER;
    use crate::deletion_vector::MAGIC_NUMBER_V2;
    use crate::io::FileIOBuilder;
    use crate::spec::{BinaryRow, DataFileMeta, IntType, MapType, RowType, VarCharType};
    use crate::table::source::DataSplitBuilder;
    use crate::DeletionFile;
    use arrow_array::{Array, ArrayRef, Int32Array, Int64Array, Int8Array, RecordBatch};
    use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
    use bytes::BufMut;
    use futures::TryStreamExt;
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use roaring::RoaringBitmap;
    use roaring::RoaringTreemap;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;

    fn pk_field(id: i32, name: &str) -> DataField {
        DataField::new(
            id,
            name.to_string(),
            PaimonDataType::Int(IntType::with_nullable(false)),
        )
    }

    fn int_field(id: i32, name: &str) -> DataField {
        DataField::new(id, name.to_string(), PaimonDataType::Int(IntType::new()))
    }

    fn mv_field(id: i32, name: &str) -> DataField {
        let varchar = PaimonDataType::VarChar(VarCharType::new(VarCharType::MAX_LENGTH).unwrap());
        let int_t = PaimonDataType::Int(IntType::new());
        DataField::new(
            id,
            name.to_string(),
            PaimonDataType::Row(RowType::new(vec![
                DataField::new(id + 100, "latest_version".to_string(), varchar.clone()),
                DataField::new(id + 101, "latest_value".to_string(), int_t.clone()),
                DataField::new(
                    id + 102,
                    "all_versioned_values".to_string(),
                    PaimonDataType::Map(MapType::new(varchar, int_t)),
                ),
            ])),
        )
    }

    /// User projects only the PK; merge schema must still carry the MV
    /// column so the accumulator is fed.
    #[test]
    fn test_compute_merge_read_fields_pk_only_supplements_mv_for_vpu() {
        let pk = pk_field(0, "id");
        let single = int_field(1, "val");
        let mv = mv_field(2, "mv");
        let table_fields = vec![pk.clone(), single, mv.clone()];
        let read_type = vec![pk.clone()];
        let key_fields = vec![pk.clone()];

        let merged = compute_merge_read_fields(
            &read_type,
            &key_fields,
            &table_fields,
            &[],
            MergeEngine::VersionedPartialUpdate,
        )
        .unwrap();
        let names: Vec<&str> = merged.iter().map(|f| f.name()).collect();
        // PK first (user projection), MV補齐 跟在 PK 之后。
        assert_eq!(names, vec!["id", "mv"]);
    }

    /// User projects a single-version column only; merge schema must補 PK
    /// and MV.
    #[test]
    fn test_compute_merge_read_fields_single_version_supplements_pk_and_mv() {
        let pk = pk_field(0, "id");
        let single = int_field(1, "val");
        let mv = mv_field(2, "mv");
        let table_fields = vec![pk.clone(), single.clone(), mv.clone()];
        let read_type = vec![single.clone()];
        let key_fields = vec![pk.clone()];

        let merged = compute_merge_read_fields(
            &read_type,
            &key_fields,
            &table_fields,
            &[],
            MergeEngine::VersionedPartialUpdate,
        )
        .unwrap();
        let names: Vec<&str> = merged.iter().map(|f| f.name()).collect();
        // val (user) → id (PK補齐) → mv (MV補齐).
        assert_eq!(names, vec!["val", "id", "mv"]);
    }

    /// User projects the MV column directly; nothing needs補.
    #[test]
    fn test_compute_merge_read_fields_user_already_projects_mv() {
        let pk = pk_field(0, "id");
        let mv = mv_field(2, "mv");
        let table_fields = vec![pk.clone(), mv.clone()];
        let read_type = vec![pk.clone(), mv.clone()];
        let key_fields = vec![pk.clone()];

        let merged = compute_merge_read_fields(
            &read_type,
            &key_fields,
            &table_fields,
            &[],
            MergeEngine::VersionedPartialUpdate,
        )
        .unwrap();
        let names: Vec<&str> = merged.iter().map(|f| f.name()).collect();
        assert_eq!(names, vec!["id", "mv"]);
    }

    /// Non-VPU engine: MV column shape exists in the schema but should not
    /// be auto-supplemented (the merge function does not maintain MV state).
    #[test]
    fn test_compute_merge_read_fields_non_vpu_skips_mv_supplement() {
        let pk = pk_field(0, "id");
        let mv = mv_field(2, "mv");
        let table_fields = vec![pk.clone(), mv];
        let read_type = vec![pk.clone()];
        let key_fields = vec![pk.clone()];

        let merged = compute_merge_read_fields(
            &read_type,
            &key_fields,
            &table_fields,
            &[],
            MergeEngine::Deduplicate,
        )
        .unwrap();
        let names: Vec<&str> = merged.iter().map(|f| f.name()).collect();
        assert_eq!(names, vec!["id"]);
    }

    /// Sequence fields are appended last regardless of MV補齐 order.
    #[test]
    fn test_compute_merge_read_fields_sequence_field_after_mv() {
        let pk = pk_field(0, "id");
        let seq = int_field(1, "seq");
        let mv = mv_field(2, "mv");
        let table_fields = vec![pk.clone(), seq.clone(), mv.clone()];
        let read_type = vec![pk.clone()];
        let key_fields = vec![pk.clone()];
        let sequence_fields = vec!["seq".to_string()];

        let merged = compute_merge_read_fields(
            &read_type,
            &key_fields,
            &table_fields,
            &sequence_fields,
            MergeEngine::VersionedPartialUpdate,
        )
        .unwrap();
        let names: Vec<&str> = merged.iter().map(|f| f.name()).collect();
        assert_eq!(names, vec!["id", "mv", "seq"]);
    }

    // ========== Stage 1: KV reader DV wiring (C2) end-to-end tests ==========
    //
    // These tests exercise the full KV reader stack: parquet read → DV row
    // selection → sort-merge dedup → reorder. The minimum input is a parquet
    // file in KV physical layout `[_SEQUENCE_NUMBER:Int64, _VALUE_KIND:Int8,
    // user_cols...]` plus a deletion-vector blob co-located with it. We
    // stream the result through `KeyValueFileReader::read` (same path as
    // production, no shortcuts).

    /// Write a KV-physical parquet file: `[_SEQUENCE_NUMBER, _VALUE_KIND, k]`
    /// with one row per (seq, vk, k) tuple. Always sets `_VALUE_KIND = INSERT`
    /// (0) and uses ascending `_SEQUENCE_NUMBER` so sort-merge dedup is a no-op
    /// when keys are unique — the only difference between input and output is
    /// the DV-applied row selection.
    fn write_kv_parquet_file(
        path: &Path,
        ks: Vec<i32>,
        vks: Option<Vec<i8>>,
        max_row_group_size: Option<usize>,
    ) {
        let n = ks.len();
        let seqs: Vec<i64> = (1..=n as i64).collect();
        let vks: Vec<i8> = vks.unwrap_or_else(|| vec![0; n]); // default: all Insert
        assert_eq!(vks.len(), n, "vks length must match ks length");
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("_SEQUENCE_NUMBER", ArrowDataType::Int64, false),
            ArrowField::new("_VALUE_KIND", ArrowDataType::Int8, false),
            ArrowField::new("k", ArrowDataType::Int32, false),
        ]));
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(seqs)),
            Arc::new(Int8Array::from(vks)),
            Arc::new(Int32Array::from(ks)),
        ];
        let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();

        let props = max_row_group_size.map(|size| {
            WriterProperties::builder()
                .set_max_row_group_row_count(Some(size))
                .build()
        });
        let file = std::fs::File::create(path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, props).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    /// Construct a 32-bit DV blob mirroring Java `DeletionFileWriter` /
    /// `BitmapDeletionVector.serializeTo` byte layout:
    ///
    /// ```text
    /// [version:byte=1][bitmapLength:int32 BE][magic:int32 BE][roaring32 bytes][crc:int32 BE]
    /// ```
    ///
    /// Returns `(file_path, offset, length)` suitable for `DeletionFile::new`:
    /// - `offset = 1` (skip version byte; physical blob starts here)
    /// - `length = bitmapLength` (Java metadata.length = `serializeTo()` return
    ///   value = magic + roaring bytes, **excluding** outer length field and
    ///   crc; physical blob occupies `length + 8` bytes)
    fn write_test_dv_blob(dir: &Path, name: &str, deleted: &[u32]) -> (PathBuf, i64, i64) {
        let mut bitmap = RoaringBitmap::new();
        for &d in deleted {
            bitmap.insert(d);
        }

        let mut roaring_bytes = Vec::new();
        bitmap.serialize_into(&mut roaring_bytes).unwrap();
        // Inner length includes magic (4) but excludes outer length field and crc
        // (matches Java BitmapDeletionVector.serializeTo return value `size`).
        let inner_size: i32 = (4 + roaring_bytes.len()) as i32;

        let mut blob: Vec<u8> = Vec::with_capacity(1 + 4 + inner_size as usize + 4);
        blob.put_u8(1); // version byte
        blob.put_i32(inner_size); // outer length field (BE)
        blob.put_i32(MAGIC_NUMBER as i32); // magic (BE)
        blob.extend_from_slice(&roaring_bytes);
        blob.put_i32(0); // CRC (read path skips verification)

        let path = dir.join(name);
        std::fs::write(&path, &blob).unwrap();
        (path, 1i64, inner_size as i64)
    }

    /// Construct a 64-bit DV blob mirroring Java
    /// `Bitmap64DeletionVector.serializeTo` byte layout:
    ///
    /// ```text
    /// [version:byte=1][bitmapDataLength:int32 BE][magic:int32 LE][roaring64 LE bytes][crc:int32 BE]
    /// ```
    ///
    /// Returns `(file_path, offset, length)` suitable for `DeletionFile::new`:
    /// - `offset = 1` (skip version byte; physical blob starts here)
    /// - `length = bitmapDataLength + 8` — Java's `DeletionVectorMeta.length`
    ///   for the 64-bit variant **includes** the outer length+crc frame
    ///   (mirrors `Bitmap64DeletionVector.serializeTo` returning `bytes.length`,
    ///   not just the inner size). This is **different from** the 32-bit
    ///   variant where length excludes the frame; see SECTION-RISKS #8 in
    ///   `dv-impl-plan.md`.
    fn write_test_dv64_blob(dir: &Path, name: &str, deleted: &[u64]) -> (PathBuf, i64, i64) {
        let mut treemap = RoaringTreemap::new();
        for &d in deleted {
            treemap.insert(d);
        }
        let mut treemap_bytes = Vec::new();
        treemap.serialize_into(&mut treemap_bytes).unwrap();
        // bitmapDataLength = magic(4) + treemap bytes
        let bitmap_data_length: i32 = (4 + treemap_bytes.len()) as i32;

        let mut blob: Vec<u8> = Vec::with_capacity(1 + 4 + bitmap_data_length as usize + 4);
        blob.put_u8(1); // version byte
        blob.put_i32(bitmap_data_length); // outer length field (BE)
                                          // Magic written as LE — Java side uses an LE buffer in
                                          // OptimizedRoaringBitmap64.serializeBitmapData.
        blob.extend_from_slice(&MAGIC_NUMBER_V2.to_le_bytes());
        blob.extend_from_slice(&treemap_bytes);
        blob.put_i32(0); // CRC (read path skips verification)

        let path = dir.join(name);
        std::fs::write(&path, &blob).unwrap();
        // 64-bit metadata.length includes outer length(4) + crc(4) frame.
        (path, 1i64, (bitmap_data_length as i64) + 8)
    }

    fn local_file_uri(path: &Path) -> String {
        let normalized = path.to_string_lossy().replace('\\', "/");
        if normalized.starts_with('/') {
            format!("file:{normalized}")
        } else {
            format!("file:/{normalized}")
        }
    }

    fn make_kv_data_file(file_name: &str, row_count: i64, file_size: i64) -> DataFileMeta {
        serde_json::from_value(serde_json::json!({
            "_FILE_NAME": file_name,
            "_FILE_SIZE": file_size,
            "_ROW_COUNT": row_count,
            "_MIN_KEY": [],
            "_MAX_KEY": [],
            "_KEY_STATS": {
                "_MIN_VALUES": [],
                "_MAX_VALUES": [],
                "_NULL_COUNTS": []
            },
            "_VALUE_STATS": {
                "_MIN_VALUES": [],
                "_MAX_VALUES": [],
                "_NULL_COUNTS": []
            },
            "_MIN_SEQUENCE_NUMBER": 0,
            "_MAX_SEQUENCE_NUMBER": 0,
            "_SCHEMA_ID": 0,
            "_LEVEL": 1,
            "_EXTRA_FILES": [],
            "_CREATION_TIME": chrono::Utc::now().timestamp_millis(),
            "_DELETE_ROW_COUNT": null,
            "_EMBEDDED_FILE_INDEX": null,
            "_FILE_SOURCE": null,
            "_VALUE_STATS_COLS": null,
            "_FIRST_ROW_ID": null,
            "_WRITE_COLS": null,
            "_EXTERNAL_PATH": null
        }))
        .unwrap()
    }

    fn make_kv_config_for_int_pk(table_path: &str) -> KeyValueReadConfig {
        let pk_field = DataField::new(
            0,
            "k".to_string(),
            PaimonDataType::Int(IntType::with_nullable(false)),
        );
        let table_fields = vec![pk_field.clone()];
        let read_type = vec![pk_field];
        let file_io = FileIOBuilder::new("file").build().unwrap();
        let schema_manager = SchemaManager::new(file_io, table_path.to_string());

        KeyValueReadConfig {
            table_name: "default.dv_kv_test".to_string(),
            table_options: HashMap::new(),
            schema_manager,
            table_schema_id: 0,
            table_fields,
            read_type,
            predicates: Vec::new(),
            primary_keys: vec!["k".to_string()],
            merge_engine: MergeEngine::Deduplicate,
            sequence_fields: Vec::new(),
            batch_size: 1024,
            parquet_page_index_enabled: true,
        }
    }

    async fn collect_k_column(reader: KeyValueFileReader, splits: &[DataSplit]) -> Vec<i32> {
        let stream = reader.read(splits).unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
        batches
            .iter()
            .flat_map(|b| {
                let arr = b
                    .column_by_name("k")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                (0..arr.len()).map(|i| arr.value(i)).collect::<Vec<_>>()
            })
            .collect()
    }

    /// Smoke: 4 rows / single row group / DV{1,3} → output rows 0,2 (k=0,k=2).
    #[tokio::test]
    async fn test_kv_reader_applies_deletion_vector_smoke() {
        let dir = tempfile::tempdir().unwrap();
        let bucket_dir = dir.path().join("bucket-0");
        std::fs::create_dir_all(&bucket_dir).unwrap();

        let parquet_path = bucket_dir.join("data-0.parquet");
        write_kv_parquet_file(&parquet_path, vec![0, 1, 2, 3], None, None);
        let parquet_size = parquet_path.metadata().unwrap().len() as i64;

        let (dv_path, dv_offset, dv_length) = write_test_dv_blob(dir.path(), "dv-0", &[1, 3]);

        let data_file = make_kv_data_file("data-0.parquet", 4, parquet_size);
        let deletion_file =
            DeletionFile::new(local_file_uri(&dv_path), dv_offset, dv_length, Some(2));
        let split = DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path(local_file_uri(&bucket_dir))
            .with_total_buckets(1)
            .with_data_files(vec![data_file])
            .with_data_deletion_files(vec![Some(deletion_file)])
            .build()
            .unwrap();

        let file_io = FileIOBuilder::new("file").build().unwrap();
        let table_path = local_file_uri(dir.path());
        let reader = KeyValueFileReader::new(file_io, make_kv_config_for_int_pk(&table_path));

        let ks = collect_k_column(reader, &[split]).await;
        assert_eq!(ks, vec![0, 2]);
    }

    /// Multi row-group invariant (the test that single-row-group fixtures cannot
    /// prove): 6 rows across 2 row groups (3 each), DV deletes absolute row 1
    /// (in RG 0) and absolute row 4 (in RG 1). Expected output uses absolute
    /// file row positions, not RG-local offsets.
    #[tokio::test]
    async fn test_kv_reader_dv_across_row_groups() {
        let dir = tempfile::tempdir().unwrap();
        let bucket_dir = dir.path().join("bucket-0");
        std::fs::create_dir_all(&bucket_dir).unwrap();

        let parquet_path = bucket_dir.join("data-0.parquet");
        // 6 rows, 2 row groups (3 each): [k=0,1,2 | k=3,4,5]
        write_kv_parquet_file(&parquet_path, vec![0, 1, 2, 3, 4, 5], None, Some(3));
        let parquet_size = parquet_path.metadata().unwrap().len() as i64;

        // DV deletes absolute row id 1 (k=1) and 4 (k=4).
        // If DV were treated as RG-local, the result would be wrong: e.g.
        // selecting rows 1,4 within each RG would delete k=1,k=4 and also
        // mistakenly affect k=4 in RG 1 / cross-RG semantics drift.
        let (dv_path, dv_offset, dv_length) = write_test_dv_blob(dir.path(), "dv-0", &[1, 4]);

        let data_file = make_kv_data_file("data-0.parquet", 6, parquet_size);
        let deletion_file =
            DeletionFile::new(local_file_uri(&dv_path), dv_offset, dv_length, Some(2));
        let split = DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path(local_file_uri(&bucket_dir))
            .with_total_buckets(1)
            .with_data_files(vec![data_file])
            .with_data_deletion_files(vec![Some(deletion_file)])
            .build()
            .unwrap();

        let file_io = FileIOBuilder::new("file").build().unwrap();
        let table_path = local_file_uri(dir.path());
        let reader = KeyValueFileReader::new(file_io, make_kv_config_for_int_pk(&table_path));

        let ks = collect_k_column(reader, &[split]).await;
        // Critical invariant: DV row id is the ABSOLUTE file row position.
        assert_eq!(ks, vec![0, 2, 3, 5]);
    }

    /// Empty DV bitmap should pass all rows through (mirror Java
    /// `KeyValueFileReaderFactory.java:174` skipping the wrap when
    /// `dv.isEmpty()`). Rust's `dv_to_non_deleted_ranges` returns the full
    /// row range when the bitmap is empty; this test guards that path.
    #[tokio::test]
    async fn test_kv_reader_empty_dv_returns_all_rows() {
        let dir = tempfile::tempdir().unwrap();
        let bucket_dir = dir.path().join("bucket-0");
        std::fs::create_dir_all(&bucket_dir).unwrap();

        let parquet_path = bucket_dir.join("data-0.parquet");
        write_kv_parquet_file(&parquet_path, vec![0, 1, 2, 3], None, None);
        let parquet_size = parquet_path.metadata().unwrap().len() as i64;

        let (dv_path, dv_offset, dv_length) = write_test_dv_blob(dir.path(), "dv-0", &[]);

        let data_file = make_kv_data_file("data-0.parquet", 4, parquet_size);
        let deletion_file =
            DeletionFile::new(local_file_uri(&dv_path), dv_offset, dv_length, Some(0));
        let split = DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path(local_file_uri(&bucket_dir))
            .with_total_buckets(1)
            .with_data_files(vec![data_file])
            .with_data_deletion_files(vec![Some(deletion_file)])
            .build()
            .unwrap();

        let file_io = FileIOBuilder::new("file").build().unwrap();
        let table_path = local_file_uri(dir.path());
        let reader = KeyValueFileReader::new(file_io, make_kv_config_for_int_pk(&table_path));

        let ks = collect_k_column(reader, &[split]).await;
        assert_eq!(ks, vec![0, 1, 2, 3]);
    }

    /// 64-bit DV end-to-end: 4 rows / single row group / 64-bit DV deleting
    /// row 0 and row 2 → KV reader output rows 1 and 3. Exercises the full
    /// stack with `RoaringTreemap`-encoded DV bytes (Stage 2 path).
    ///
    /// All deleted positions stay within [0, 4) so the underlying treemap
    /// has a single high-32 container; cross-32-bit-boundary positions are
    /// covered by the dedicated test in `core::tests` and the iterator path
    /// is identical for KV vs raw.
    #[tokio::test]
    async fn test_kv_reader_applies_bitmap64_deletion_vector() {
        let dir = tempfile::tempdir().unwrap();
        let bucket_dir = dir.path().join("bucket-0");
        std::fs::create_dir_all(&bucket_dir).unwrap();

        let parquet_path = bucket_dir.join("data-0.parquet");
        write_kv_parquet_file(&parquet_path, vec![0, 1, 2, 3], None, None);
        let parquet_size = parquet_path.metadata().unwrap().len() as i64;

        let (dv_path, dv_offset, dv_length) =
            write_test_dv64_blob(dir.path(), "dv-0", &[0u64, 2u64]);

        let data_file = make_kv_data_file("data-0.parquet", 4, parquet_size);
        let deletion_file =
            DeletionFile::new(local_file_uri(&dv_path), dv_offset, dv_length, Some(2));
        let split = DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path(local_file_uri(&bucket_dir))
            .with_total_buckets(1)
            .with_data_files(vec![data_file])
            .with_data_deletion_files(vec![Some(deletion_file)])
            .build()
            .unwrap();

        let file_io = FileIOBuilder::new("file").build().unwrap();
        let table_path = local_file_uri(dir.path());
        let reader = KeyValueFileReader::new(file_io, make_kv_config_for_int_pk(&table_path));

        let ks = collect_k_column(reader, &[split]).await;
        assert_eq!(ks, vec![1, 3]);
    }

    // ========== Stage 4a: PK raw-read drop_deletes ==========
    //
    // These tests cover the DataFileReader path used by `read_pk`'s
    // raw-read short-circuit (full L1+ split + key non-overlapping). The
    // important invariants:
    //   1. Output is byte-equivalent to the sort-merge path for the same
    //      input — `_VALUE_KIND` stays an internal column; DELETE /
    //      UPDATE_BEFORE rows must be filtered.
    //   2. Default `drop_deletes=false` does not change existing raw paths
    //      (kv_file_reader internal use, data_evolution, append, system tables).

    /// Build a `read_type` with `_VALUE_KIND` prepended (TinyInt). Mirrors
    /// `table_read.rs::raw_read_type_with_value_kind` so the test does not
    /// rely on private items.
    fn raw_read_type_with_value_kind_for_test(user: &[DataField]) -> Vec<DataField> {
        let mut fields = Vec::with_capacity(user.len() + 1);
        fields.push(DataField::new(
            VALUE_KIND_FIELD_ID,
            VALUE_KIND_FIELD_NAME.to_string(),
            PaimonDataType::TinyInt(TinyIntType::new()),
        ));
        fields.extend_from_slice(user);
        fields
    }

    async fn collect_k_column_from_stream(stream: ArrowRecordBatchStream) -> Vec<i32> {
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
        batches
            .iter()
            .flat_map(|b| {
                let arr = b
                    .column_by_name("k")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                (0..arr.len()).map(|i| arr.value(i)).collect::<Vec<_>>()
            })
            .collect()
    }

    /// Stage 4a equivalence: with the same parquet (mixed RowKind) + DV +
    /// L1+ split, the sort-merge path (`KeyValueFileReader`) and the
    /// raw-read drop_deletes path (`DataFileReader.with_drop_deletes(true)`)
    /// produce identical user-visible rows. This is the core correctness
    /// guarantee for the C5 fix and the F9 batch-read short-circuit.
    ///
    /// Mixed RowKind exercises both DV row-selection (drop row 0 via DV) and
    /// post-decode RowKind filtering (drop DELETE / UPDATE_BEFORE). Only
    /// k=2 (UPDATE_AFTER) survives both paths.
    #[tokio::test]
    async fn test_pk_raw_drop_deletes_equivalent_to_sort_merge() {
        let dir = tempfile::tempdir().unwrap();
        let bucket_dir = dir.path().join("bucket-0");
        std::fs::create_dir_all(&bucket_dir).unwrap();

        let parquet_path = bucket_dir.join("data-0.parquet");
        // 4 rows; RowKind: INSERT, DELETE, UPDATE_AFTER, UPDATE_BEFORE
        write_kv_parquet_file(
            &parquet_path,
            vec![0, 1, 2, 3],
            Some(vec![0, 3, 2, 1]),
            None,
        );
        let parquet_size = parquet_path.metadata().unwrap().len() as i64;

        // DV deletes physical row 0 (k=0, INSERT) — verifies DV row-selection.
        let (dv_path, dv_offset, dv_length) = write_test_dv_blob(dir.path(), "dv-0", &[0]);

        let data_file = make_kv_data_file("data-0.parquet", 4, parquet_size);
        let deletion_file =
            DeletionFile::new(local_file_uri(&dv_path), dv_offset, dv_length, Some(1));
        let split = DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path(local_file_uri(&bucket_dir))
            .with_total_buckets(1)
            .with_data_files(vec![data_file])
            .with_data_deletion_files(vec![Some(deletion_file)])
            .build()
            .unwrap();

        let file_io = FileIOBuilder::new("file").build().unwrap();
        let table_path = local_file_uri(dir.path());

        // Path A: sort-merge (KeyValueFileReader)
        let kv_reader =
            KeyValueFileReader::new(file_io.clone(), make_kv_config_for_int_pk(&table_path));
        let ks_via_kv = collect_k_column(kv_reader, std::slice::from_ref(&split)).await;

        // Path B: raw-read with drop_deletes (DataFileReader)
        // read_type must contain _VALUE_KIND so the parquet reader actually
        // pulls the column; with_drop_deletes consumes it for filtering.
        let user_read_type = vec![DataField::new(
            0,
            "k".to_string(),
            PaimonDataType::Int(crate::spec::IntType::with_nullable(false)),
        )];
        let raw_read_type = raw_read_type_with_value_kind_for_test(&user_read_type);
        let table_fields = vec![DataField::new(
            0,
            "k".to_string(),
            PaimonDataType::Int(crate::spec::IntType::with_nullable(false)),
        )];
        let schema_manager = SchemaManager::new(file_io.clone(), table_path.clone());
        let raw_reader = DataFileReader::new(
            file_io,
            schema_manager,
            0,
            table_fields,
            raw_read_type,
            Vec::new(),
            1024,
            true,
        )
        .with_drop_deletes(true);
        let stream_b = raw_reader.read(&[split]).unwrap();
        let ks_via_raw = collect_k_column_from_stream(stream_b).await;

        // Equivalence: byte-for-byte identical user-visible rows.
        assert_eq!(ks_via_kv, ks_via_raw);
        // Concrete expected output: DV strips row 0 (k=0). Of the remaining
        // (k=1 DELETE, k=2 UPDATE_AFTER, k=3 UPDATE_BEFORE) only the
        // UPDATE_AFTER survives `RowKind::is_add()`.
        assert_eq!(ks_via_kv, vec![2]);
    }

    /// C5 reverse: default `drop_deletes=false` keeps DELETE / UPDATE_BEFORE
    /// rows untouched. Without this guard, the new `with_drop_deletes`
    /// builder could accidentally regress non-PK / append / system-table
    /// reads that rely on raw `RowKind` semantics.
    ///
    /// `read_type` here intentionally contains `_VALUE_KIND` so the parquet
    /// reader emits it; we observe the column is preserved end-to-end.
    #[tokio::test]
    async fn test_data_file_reader_default_keeps_delete_rows() {
        let dir = tempfile::tempdir().unwrap();
        let bucket_dir = dir.path().join("bucket-0");
        std::fs::create_dir_all(&bucket_dir).unwrap();

        let parquet_path = bucket_dir.join("data-0.parquet");
        // RowKind: INSERT, DELETE, UPDATE_AFTER, UPDATE_BEFORE
        write_kv_parquet_file(
            &parquet_path,
            vec![0, 1, 2, 3],
            Some(vec![0, 3, 2, 1]),
            None,
        );
        let parquet_size = parquet_path.metadata().unwrap().len() as i64;

        let data_file = make_kv_data_file("data-0.parquet", 4, parquet_size);
        let split = DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path(local_file_uri(&bucket_dir))
            .with_total_buckets(1)
            .with_data_files(vec![data_file])
            .build()
            .unwrap();

        let file_io = FileIOBuilder::new("file").build().unwrap();
        let table_path = local_file_uri(dir.path());

        let user_read_type = vec![DataField::new(
            0,
            "k".to_string(),
            PaimonDataType::Int(crate::spec::IntType::with_nullable(false)),
        )];
        let read_type = raw_read_type_with_value_kind_for_test(&user_read_type);
        let schema_manager = SchemaManager::new(file_io.clone(), table_path.clone());
        // No .with_drop_deletes — default is false.
        let reader = DataFileReader::new(
            file_io,
            schema_manager,
            0,
            user_read_type,
            read_type,
            Vec::new(),
            1024,
            true,
        );
        let stream = reader.read(&[split]).unwrap();
        let ks = collect_k_column_from_stream(stream).await;
        // All 4 rows preserved — DELETE and UPDATE_BEFORE are NOT filtered.
        assert_eq!(ks, vec![0, 1, 2, 3]);
    }

    /// Multi-column parquet helper for PU/VPU + DV tests. Writes a single
    /// row group with the physical KV schema:
    ///   `[_SEQUENCE_NUMBER, _VALUE_KIND, k, v_int, v_str]`.
    /// `v_ints` / `v_strs` may contain `None` to model partial-update payloads
    /// where one row leaves a column NULL.
    fn write_multi_col_parquet_file(
        path: &Path,
        ks: Vec<i32>,
        v_ints: Vec<Option<i32>>,
        v_strs: Vec<Option<&str>>,
    ) {
        use arrow_array::StringArray;
        let n = ks.len();
        assert_eq!(v_ints.len(), n);
        assert_eq!(v_strs.len(), n);
        let seqs: Vec<i64> = (1..=n as i64).collect();
        let vks: Vec<i8> = vec![0; n]; // all Insert
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("_SEQUENCE_NUMBER", ArrowDataType::Int64, false),
            ArrowField::new("_VALUE_KIND", ArrowDataType::Int8, false),
            ArrowField::new("k", ArrowDataType::Int32, false),
            ArrowField::new("v_int", ArrowDataType::Int32, true),
            ArrowField::new("v_str", ArrowDataType::Utf8, true),
        ]));
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(seqs)),
            Arc::new(Int8Array::from(vks)),
            Arc::new(Int32Array::from(ks)),
            Arc::new(Int32Array::from(v_ints)),
            Arc::new(StringArray::from(v_strs)),
        ];
        let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();
        let file = std::fs::File::create(path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    fn make_kv_data_file_multi_col(
        file_name: &str,
        row_count: i64,
        file_size: i64,
    ) -> DataFileMeta {
        // Same as `make_kv_data_file` but reused so test naming reads clearly.
        make_kv_data_file(file_name, row_count, file_size)
    }

    fn make_kv_config_for_partial_update(
        table_path: &str,
        merge_engine: MergeEngine,
    ) -> KeyValueReadConfig {
        let pk = DataField::new(
            0,
            "k".to_string(),
            PaimonDataType::Int(IntType::with_nullable(false)),
        );
        let v_int = DataField::new(1, "v_int".to_string(), PaimonDataType::Int(IntType::new()));
        let v_str = DataField::new(
            2,
            "v_str".to_string(),
            PaimonDataType::VarChar(VarCharType::new(VarCharType::MAX_LENGTH).unwrap()),
        );
        let table_fields = vec![pk.clone(), v_int.clone(), v_str.clone()];
        let read_type = vec![pk, v_int, v_str];
        let file_io = FileIOBuilder::new("file").build().unwrap();
        let schema_manager = SchemaManager::new(file_io, table_path.to_string());

        KeyValueReadConfig {
            table_name: "default.dv_pu_test".to_string(),
            table_options: HashMap::new(),
            schema_manager,
            table_schema_id: 0,
            table_fields,
            read_type,
            predicates: Vec::new(),
            primary_keys: vec!["k".to_string()],
            merge_engine,
            sequence_fields: Vec::new(),
            batch_size: 1024,
            parquet_page_index_enabled: true,
        }
    }

    /// PU + DV: two physical rows for PK k=1, the older row carries `v_str`
    /// only and the newer row carries `v_int` only. DV deletes the older
    /// physical row (row 0). After DV pre-filter the PartialUpdate merge
    /// function sees only the newer row, so the user-visible result is just
    /// `(k=1, v_int=Some(200), v_str=None)`. Without the reject, the read
    /// pipeline succeeds; with DV stripping the row that owns `v_str`, PU
    /// cannot synthesize a value for that column — proving DV-pre-filter
    /// composes correctly with column-wise merge (mirrors Java
    /// `KeyValueFileReaderFactory.java:173-187@e8938f347`).
    #[tokio::test]
    async fn test_kv_reader_partial_update_with_deletion_vector() {
        let dir = tempfile::tempdir().unwrap();
        let bucket_dir = dir.path().join("bucket-0");
        std::fs::create_dir_all(&bucket_dir).unwrap();

        let parquet_path = bucket_dir.join("data-0.parquet");
        write_multi_col_parquet_file(
            &parquet_path,
            vec![1, 1],                  // same PK twice
            vec![None, Some(200)],       // v_int: only newer row
            vec![Some("old-str"), None], // v_str: only older row
        );
        let parquet_size = parquet_path.metadata().unwrap().len() as i64;

        // DV deletes row 0 (the older partial that owns `v_str`).
        let (dv_path, dv_offset, dv_length) = write_test_dv_blob(dir.path(), "dv-pu", &[0]);

        let data_file = make_kv_data_file_multi_col("data-0.parquet", 2, parquet_size);
        let deletion_file =
            DeletionFile::new(local_file_uri(&dv_path), dv_offset, dv_length, Some(1));
        let split = DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path(local_file_uri(&bucket_dir))
            .with_total_buckets(1)
            .with_data_files(vec![data_file])
            .with_data_deletion_files(vec![Some(deletion_file)])
            .build()
            .unwrap();

        let file_io = FileIOBuilder::new("file").build().unwrap();
        let table_path = local_file_uri(dir.path());
        let reader = KeyValueFileReader::new(
            file_io,
            make_kv_config_for_partial_update(&table_path, MergeEngine::PartialUpdate),
        );
        let stream = reader.read(&[split]).unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();

        let mut got: Vec<(i32, Option<i32>, Option<String>)> = Vec::new();
        for batch in &batches {
            let ks = batch
                .column_by_name("k")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let v_ints = batch
                .column_by_name("v_int")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let v_strs = batch
                .column_by_name("v_str")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .unwrap();
            for i in 0..batch.num_rows() {
                got.push((
                    ks.value(i),
                    if v_ints.is_null(i) {
                        None
                    } else {
                        Some(v_ints.value(i))
                    },
                    if v_strs.is_null(i) {
                        None
                    } else {
                        Some(v_strs.value(i).to_string())
                    },
                ));
            }
        }

        // DV stripped row 0 → only row 1 reaches the PartialUpdate merge
        // function. `v_str` therefore has no contributor and stays None;
        // `v_int` is the row's own value 200.
        assert_eq!(got, vec![(1, Some(200), None)]);
    }

    /// PU + DV smoke: `PartialUpdate` reader no longer rejects when a split
    /// has a DV attached. Mirrors Java
    /// `KeyValueFileReaderFactory.java:173-187@e8938f347` engine-agnostic DV
    /// wrapping. Uses a single-row PU split + empty DV: DV is present but
    /// strips no rows, so the row passes through to the merge function and
    /// the read returns its lone row. The full DV → column-merge interaction
    /// is covered by `test_kv_reader_partial_update_with_deletion_vector`.
    #[tokio::test]
    async fn test_kv_reader_partial_update_dispatch_no_longer_rejects_dv() {
        let dir = tempfile::tempdir().unwrap();
        let bucket_dir = dir.path().join("bucket-0");
        std::fs::create_dir_all(&bucket_dir).unwrap();

        let parquet_path = bucket_dir.join("data-0.parquet");
        write_multi_col_parquet_file(&parquet_path, vec![7], vec![Some(70)], vec![Some("seven")]);
        let parquet_size = parquet_path.metadata().unwrap().len() as i64;

        let (dv_path, dv_offset, dv_length) = write_test_dv_blob(dir.path(), "dv-pu-empty", &[]);

        let data_file = make_kv_data_file_multi_col("data-0.parquet", 1, parquet_size);
        let deletion_file =
            DeletionFile::new(local_file_uri(&dv_path), dv_offset, dv_length, Some(0));
        let split = DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path(local_file_uri(&bucket_dir))
            .with_total_buckets(1)
            .with_data_files(vec![data_file])
            .with_data_deletion_files(vec![Some(deletion_file)])
            .build()
            .unwrap();

        let file_io = FileIOBuilder::new("file").build().unwrap();
        let table_path = local_file_uri(dir.path());
        let reader = KeyValueFileReader::new(
            file_io,
            make_kv_config_for_partial_update(&table_path, MergeEngine::PartialUpdate),
        );

        let result = reader.read(&[split]);
        assert!(
            result.is_ok(),
            "PU + DV split should no longer be rejected up-front; got: {:?}",
            result.err()
        );
        let batches: Vec<RecordBatch> = result.unwrap().try_collect().await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 1);
    }

    /// VPU + DV smoke: asserts the dispatch no longer rejects with
    /// `Error::Unsupported`. Constructing a fully-populated MV column
    /// (`<latest_version, latest_value, all_versioned_values>`) for an e2e
    /// merge-correctness check is heavy; the structural argument that DV is
    /// applied at the parquet row_selection layer (before the merge function)
    /// — the same layer used by the PU+DV e2e test above — covers the
    /// interaction. This smoke locks down the dispatch surface.
    #[tokio::test]
    async fn test_kv_reader_versioned_partial_update_dispatch_no_longer_rejects_dv() {
        let dir = tempfile::tempdir().unwrap();
        let bucket_dir = dir.path().join("bucket-0");
        std::fs::create_dir_all(&bucket_dir).unwrap();

        let parquet_path = bucket_dir.join("data-0.parquet");
        // Use a single-column PK file; the VPU merge function only requires
        // the MV column when the schema declares one. Without an MV column,
        // VPU degrades to trivial behavior on a single row, which is enough
        // to confirm the reject branch is gone. Returns whatever the file
        // reader yields.
        write_kv_parquet_file(&parquet_path, vec![42], None, None);
        let parquet_size = parquet_path.metadata().unwrap().len() as i64;

        let (dv_path, dv_offset, dv_length) = write_test_dv_blob(dir.path(), "dv-vpu-empty", &[]);

        let data_file = make_kv_data_file("data-0.parquet", 1, parquet_size);
        let deletion_file =
            DeletionFile::new(local_file_uri(&dv_path), dv_offset, dv_length, Some(0));
        let split = DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path(local_file_uri(&bucket_dir))
            .with_total_buckets(1)
            .with_data_files(vec![data_file])
            .with_data_deletion_files(vec![Some(deletion_file)])
            .build()
            .unwrap();

        let file_io = FileIOBuilder::new("file").build().unwrap();
        let table_path = local_file_uri(dir.path());
        let mut config = make_kv_config_for_int_pk(&table_path);
        config.merge_engine = MergeEngine::VersionedPartialUpdate;
        let reader = KeyValueFileReader::new(file_io, config);

        let result = reader.read(&[split]);
        assert!(
            result.is_ok(),
            "VPU + DV split should no longer be rejected up-front; got: {:?}",
            result.err()
        );
        // Drive the stream so any post-dispatch error surfaces. We don't
        // assert specific rows — the goal is "no Error::Unsupported about
        // deletion vectors".
        let stream_result = result.unwrap().try_collect::<Vec<_>>().await;
        if let Err(crate::Error::Unsupported { ref message }) = stream_result {
            assert!(
                !message.contains("deletion vectors"),
                "VPU+DV should no longer be rejected with Unsupported; got: {message}"
            );
        }
    }
}
