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

//! ReadBuilder and TableRead for table read API.
//!
//! Reference: [Java ReadBuilder.withProjection](https://github.com/apache/paimon/blob/master/paimon-core/src/main/java/org/apache/paimon/table/source/ReadBuilder.java)
//! and [TypeUtils.project](https://github.com/apache/paimon/blob/master/paimon-common/src/main/java/org/apache/paimon/utils/TypeUtils.java).

use super::bucket_filter::{extract_predicate_for_keys, split_partition_and_data_predicates};
use super::partition_filter::PartitionFilter;
use super::table_read::TableRead;
use super::{Table, TableScan};
use crate::arrow::filtering::reader_pruning_predicates;
use crate::spec::{CoreOptions, DataField, Predicate};
use crate::table::source::RowRange;
use crate::{Error, Result};
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, Default)]
struct NormalizedFilter {
    partition_predicate: Option<Predicate>,
    data_predicates: Vec<Predicate>,
    bucket_predicate: Option<Predicate>,
}

/// Whether a translated predicate is exact at the table-provider boundary.
///
/// Exact filters are fully enforced by paimon-core scan planning using only
/// partition-owned semantics, without requiring residual filtering above the
/// scan.
fn is_exact_filter_pushdown_for_schema(
    fields: &[DataField],
    partition_keys: &[String],
    filter: &Predicate,
) -> bool {
    if partition_keys.is_empty() {
        return false;
    }

    let (_, data_predicates) =
        split_partition_and_data_predicates(filter.clone(), fields, partition_keys);
    data_predicates.is_empty()
}

pub(super) fn split_scan_predicates(
    table: &Table,
    filter: Predicate,
) -> (Option<Predicate>, Vec<Predicate>) {
    let partition_keys = table.schema().partition_keys();
    if partition_keys.is_empty() {
        (None, filter.split_and())
    } else {
        split_partition_and_data_predicates(filter, table.schema().fields(), partition_keys)
    }
}

fn bucket_predicate(table: &Table, filter: &Predicate) -> Option<Predicate> {
    let core_options = CoreOptions::new(table.schema().options());
    if !core_options.is_default_bucket_function() {
        return None;
    }

    let bucket_keys = core_options.bucket_key().unwrap_or_else(|| {
        if table.schema().trimmed_primary_keys().is_empty() {
            Vec::new()
        } else {
            table.schema().trimmed_primary_keys()
        }
    });
    if bucket_keys.is_empty() {
        return None;
    }

    let has_all_bucket_fields = bucket_keys.iter().all(|key| {
        table
            .schema()
            .fields()
            .iter()
            .any(|field| field.name() == key)
    });
    if !has_all_bucket_fields {
        return None;
    }

    extract_predicate_for_keys(filter, table.schema().fields(), &bucket_keys)
}

fn normalize_filter(table: &Table, filter: Predicate) -> NormalizedFilter {
    let (partition_predicate, data_predicates) = split_scan_predicates(table, filter.clone());
    NormalizedFilter {
        partition_predicate,
        data_predicates,
        bucket_predicate: bucket_predicate(table, &filter),
    }
}

/// Builder for table scan and table read (new_scan, new_read).
///
/// Rust keeps a names-based projection API for ergonomics, while aligning the
/// resulting read semantics with Java Paimon's order-preserving projection.
#[derive(Debug, Clone)]
pub struct ReadBuilder<'a> {
    table: &'a Table,
    projected_fields: Option<Vec<String>>,
    filter: NormalizedFilter,
    limit: Option<usize>,
    row_ranges: Option<Vec<RowRange>>,
    /// Whether column-name matching (projection and predicate column
    /// resolution) is case-sensitive. Defaults to `false` (kwai: case-insensitive).
    case_sensitive: bool,
}

impl<'a> ReadBuilder<'a> {
    pub(crate) fn new(table: &'a Table) -> Self {
        Self {
            table,
            projected_fields: None,
            filter: NormalizedFilter::default(),
            limit: None,
            row_ranges: None,
            // kwai default: case-INSENSITIVE column matching (ASCII case-fold).
            // Use `with_case_sensitive(true)` to restore exact-case matching.
            case_sensitive: false,
        }
    }

    /// Set column projection by name. Output order follows the caller-specified order.
    /// An empty list is a valid zero-column projection.
    ///
    /// Name resolution is deferred to read build time (order-independent with
    /// [`with_case_sensitive`](Self::with_case_sensitive)): the names are stored
    /// and resolved against the schema in [`new_read`](Self::new_read) using the
    /// case sensitivity effective then. Unknown, duplicate, or (under
    /// case-insensitive matching) ambiguous names cause `new_read()` to fail.
    pub fn with_projection(&mut self, columns: &[&str]) -> &mut Self {
        self.projected_fields = Some(columns.iter().map(|c| (*c).to_string()).collect());
        self
    }

    /// Set whether column-name matching (projection and predicate column
    /// resolution) is case-sensitive. Defaults to `false` (kwai: case-insensitive
    /// ASCII case-folding; an ambiguous case-colliding request errors). Pass
    /// `true` for exact-case matching. This mirrors the per-read case
    /// sensitivity engines like Spark drive from `spark.sql.caseSensitive`,
    /// rather than being a table property.
    ///
    /// Projection resolution is lazy, so this affects a projection set via
    /// [`with_projection`](Self::with_projection) regardless of call order (the
    /// projected names are resolved at read build time using the flag effective
    /// then). Predicates built via `PredicateBuilder` capture case sensitivity at
    /// their own construction time, so this flag does not retroactively change a
    /// predicate already passed to [`with_filter`](Self::with_filter).
    pub fn with_case_sensitive(&mut self, case_sensitive: bool) -> &mut Self {
        self.case_sensitive = case_sensitive;
        self
    }

    /// Set a filter predicate for scan planning and conservative read pruning.
    ///
    /// The predicate should use table schema field indices (as produced by
    /// [`PredicateBuilder`]). During [`TableScan::plan`], partition-only
    /// conjuncts are used for partition pruning and supported data conjuncts
    /// may be used for conservative file-stats pruning.
    ///
    /// Stats pruning is per file. Files with a different `schema_id`,
    /// incompatible stats layout, or inconclusive stats are kept.
    ///
    /// [`TableRead`] may use supported non-partition data predicates on regular
    /// Parquet and ORC read paths for conservative row-group pruning. Parquet
    /// may also use native row filtering. Unsupported predicates, formats
    /// without reader pruning, and data-evolution reads remain residual and
    /// should still be applied by the caller if exact filtering semantics are
    /// required.
    pub fn with_filter(&mut self, filter: Predicate) -> &mut Self {
        self.filter = normalize_filter(self.table, filter);
        self.try_extract_row_id_ranges();
        self
    }

    /// Whether a translated predicate is exact at the table-provider boundary.
    ///
    /// Exact filters are fully enforced by paimon-core scan planning, without
    /// requiring residual filtering above the scan.
    pub fn is_exact_filter_pushdown(&self, filter: &Predicate) -> bool {
        is_exact_filter_pushdown_for_schema(
            self.table.schema().fields(),
            self.table.schema().partition_keys(),
            filter,
        )
    }

    /// Set row ID ranges `[from, to]` (inclusive) for filtering in data evolution mode.
    pub fn with_row_ranges(&mut self, ranges: Vec<RowRange>) -> &mut Self {
        self.row_ranges = if ranges.is_empty() {
            None
        } else {
            Some(ranges)
        };
        self
    }

    /// Extract `_ROW_ID` predicates from data_predicates into row_ranges.
    /// Only runs when no explicit row_ranges have been set.
    fn try_extract_row_id_ranges(&mut self) {
        if self.row_ranges.is_some() || self.filter.data_predicates.is_empty() {
            return;
        }
        let combined = Predicate::and(self.filter.data_predicates.clone());
        if let Some(ranges) = super::row_id_predicate::extract_row_id_ranges(&combined) {
            self.row_ranges = Some(ranges);
            self.filter.data_predicates = self
                .filter
                .data_predicates
                .iter()
                .filter_map(super::row_id_predicate::remove_row_id_filter)
                .collect();
        }
    }

    /// Push a row-limit hint down to scan planning.
    ///
    /// This allows paimon-core scan planning to generate fewer splits when the
    /// current scan state keeps split-level `merged_row_count()` conservative.
    ///
    /// Note: This method does not guarantee that exactly `limit` rows will be
    /// returned by [`TableRead`]. It is only a pushdown hint for planning.
    /// Callers or query engines are responsible for enforcing the final LIMIT.
    pub fn with_limit(&mut self, limit: usize) -> &mut Self {
        self.limit = Some(limit);
        self
    }

    /// Create a table scan. Call [TableScan::plan] to get splits.
    pub fn new_scan(&self) -> TableScan<'a> {
        let partition_filter = self.filter.partition_predicate.clone().map(|pred| {
            PartitionFilter::from_predicate(pred, &self.table.schema().partition_fields())
        });
        TableScan::new(
            self.table,
            partition_filter,
            self.filter.data_predicates.clone(),
            self.filter.bucket_predicate.clone(),
            self.limit,
            self.row_ranges.clone(),
        )
    }

    /// Create a table read for consuming splits (e.g. from a scan plan).
    pub fn new_read(&self) -> Result<TableRead<'a>> {
        let read_type = match &self.projected_fields {
            None => self.table.schema.fields().to_vec(),
            Some(projected) => self.resolve_projected_fields(projected)?,
        };

        Ok(TableRead::new(
            self.table,
            read_type,
            reader_pruning_predicates(self.filter.data_predicates.clone()),
        ))
    }

    /// Resolve the projected column names against the schema using the effective
    /// case sensitivity (order-independent with `with_case_sensitive` because
    /// resolution is deferred to read build time).
    fn resolve_projected_fields(&self, projected_fields: &[String]) -> Result<Vec<DataField>> {
        resolve_projected_fields(
            self.table.identifier().full_name(),
            self.table.schema.fields(),
            projected_fields,
            self.case_sensitive,
        )
    }
}

pub(super) fn resolve_projected_fields(
    full_name: String,
    fields: &[DataField],
    projection_names: &[String],
    case_sensitive: bool,
) -> Result<Vec<DataField>> {
    if projection_names.is_empty() {
        return Ok(Vec::new());
    }

    // Build the name index once (O(fields)) so resolution is O(fields +
    // projections) rather than scanning the whole schema per projected name.
    // Case-sensitive: exact name -> field. Case-insensitive: ASCII-folded name
    // -> the unique field, or `None` when two or more fields collide under
    // folding (ambiguous, mirroring Spark's `AMBIGUOUS` behavior).
    let sensitive_index: HashMap<&str, &DataField> = if case_sensitive {
        fields.iter().map(|f| (f.name(), f)).collect()
    } else {
        HashMap::new()
    };
    let mut folded_index: HashMap<String, Option<&DataField>> = HashMap::new();
    if !case_sensitive {
        for f in fields {
            folded_index
                .entry(f.name().to_ascii_lowercase())
                .and_modify(|slot| *slot = None)
                .or_insert(Some(f));
        }
    }

    let mut seen: HashSet<String> = HashSet::with_capacity(projection_names.len());
    let mut resolved = Vec::with_capacity(projection_names.len());

    for name in projection_names {
        // Dedup under the same case sensitivity used for resolution: with
        // `case-sensitive=false`, `["Name","name"]` must flag a duplicate rather
        // than resolve the same field twice.
        let dedup_key = if case_sensitive {
            name.clone()
        } else {
            name.to_ascii_lowercase()
        };
        if !seen.insert(dedup_key) {
            return Err(Error::ConfigInvalid {
                message: format!("Duplicate projection column '{name}' for table {full_name}"),
            });
        }

        if name == crate::spec::ROW_ID_FIELD_NAME {
            resolved.push(DataField::new(
                crate::spec::ROW_ID_FIELD_ID,
                crate::spec::ROW_ID_FIELD_NAME.to_string(),
                crate::spec::DataType::BigInt(crate::spec::BigIntType::with_nullable(true)),
            ));
            continue;
        }

        let field = if case_sensitive {
            sensitive_index.get(name.as_str()).copied()
        } else {
            match folded_index.get(&name.to_ascii_lowercase()) {
                Some(Some(f)) => Some(*f),
                Some(None) => {
                    return Err(Error::ConfigInvalid {
                        message: format!(
                            "Ambiguous projection column '{name}' for table {full_name}: multiple fields match case-insensitively"
                        ),
                    });
                }
                None => None,
            }
        };
        let field = field.ok_or_else(|| Error::ColumnNotExist {
            full_name: full_name.clone(),
            column: name.clone(),
        })?;
        resolved.push(field.clone());
    }

    Ok(resolved)
}

#[cfg(test)]
mod tests {
    use crate::table::TableRead;
    mod test_utils {
        include!(concat!(env!("CARGO_MANIFEST_DIR"), "/../test_utils.rs"));
    }

    use super::ReadBuilder;
    use crate::catalog::Identifier;
    use crate::io::FileIOBuilder;
    use crate::spec::{
        BinaryRow, DataField, DataType, IntType, Predicate, PredicateBuilder, Schema, TableSchema,
        VarCharType,
    };
    use crate::table::{DataSplitBuilder, Table};
    use arrow_array::{Int32Array, RecordBatch};
    use futures::TryStreamExt;
    use std::fs;
    use tempfile::tempdir;
    use test_utils::{local_file_path, test_data_file, write_int_parquet_file};

    fn collect_int_column(batches: &[RecordBatch], column_name: &str) -> Vec<i32> {
        batches
            .iter()
            .flat_map(|batch| {
                let column_index = batch.schema().index_of(column_name).unwrap();
                let array = batch.column(column_index);
                let values = array.as_any().downcast_ref::<Int32Array>().unwrap();
                (0..values.len())
                    .map(|index| values.value(index))
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    fn simple_table() -> Table {
        let file_io = FileIOBuilder::new("file").build().unwrap();
        let table_schema = TableSchema::new(
            0,
            &Schema::builder()
                .column("dt", DataType::VarChar(VarCharType::string_type()))
                .column("id", DataType::Int(IntType::new()))
                .partition_keys(["dt"])
                .build()
                .unwrap(),
        );
        Table::new(
            file_io,
            Identifier::new("default", "t"),
            "/tmp/test-read-builder".to_string(),
            table_schema,
            None,
        )
    }

    fn partial_update_dv_pk_table() -> Table {
        let file_io = FileIOBuilder::new("file").build().unwrap();
        let table_schema = TableSchema::new(
            0,
            &Schema::builder()
                .column("id", DataType::Int(IntType::new()))
                .column("value", DataType::Int(IntType::new()))
                .primary_key(["id"])
                .option("merge-engine", "partial-update")
                .option("deletion-vectors.enabled", "true")
                .build()
                .unwrap(),
        );
        Table::new(
            file_io,
            Identifier::new("default", "partial_update_dv_t"),
            "/tmp/test-partial-update-dv-read-builder".to_string(),
            table_schema,
            None,
        )
    }

    #[test]
    fn test_with_projection_validates_unknown_projection() {
        // kwai defers resolution to new_read(): a column that cannot match under
        // any case sensitivity surfaces the error there.
        let table = simple_table();
        let mut builder = ReadBuilder::new(&table);
        builder.with_projection(&["missing"]);
        let err = builder.new_read().unwrap_err();

        assert!(matches!(
            err,
            crate::Error::ColumnNotExist {
                full_name,
                column,
            } if full_name == "default.t" && column == "missing"
        ));
    }

    #[test]
    fn test_with_projection_validates_duplicate_projection() {
        // Resolution is deferred: the duplicate error surfaces at new_read().
        let table = simple_table();
        let mut builder = ReadBuilder::new(&table);
        builder.with_projection(&["id", "id"]);
        let err = builder.new_read().unwrap_err();

        assert!(matches!(
            err,
            crate::Error::ConfigInvalid { message }
                if message.contains("Duplicate projection column 'id'")
        ));
    }

    fn mixed_case_table() -> Table {
        let file_io = FileIOBuilder::new("file").build().unwrap();
        let table_schema = TableSchema::new(
            0,
            &Schema::builder()
                .column("id", DataType::Int(IntType::new()))
                .column("Name", DataType::VarChar(VarCharType::new(50).unwrap()))
                .build()
                .unwrap(),
        );
        Table::new(
            file_io,
            Identifier::new("default", "t"),
            "/tmp/test-read-builder-ci".to_string(),
            table_schema,
            None,
        )
    }

    #[test]
    fn test_read_builder_default_case_insensitive_resolves_wrong_case() {
        // kwai default (case-insensitive): a wrong-case projection resolves to the
        // canonical schema field name. Resolution is deferred to new_read().
        let table = mixed_case_table();
        let mut builder = ReadBuilder::new(&table);
        builder.with_projection(&["NAME"]);
        let read = builder.new_read().unwrap();
        assert_eq!(read.read_type().len(), 1);
        assert_eq!(read.read_type()[0].name(), "Name");
    }

    #[test]
    fn test_read_builder_explicit_case_sensitive_rejects_wrong_case() {
        // Explicit with_case_sensitive(true): a wrong-case projection must not
        // resolve; the error surfaces at new_read() (resolution is deferred).
        let table = mixed_case_table();
        let mut builder = ReadBuilder::new(&table);
        builder.with_case_sensitive(true).with_projection(&["NAME"]);
        let err = builder.new_read().unwrap_err();
        assert!(matches!(err, crate::Error::ColumnNotExist { .. }));
    }

    #[test]
    fn test_read_builder_with_case_sensitive_false_resolves_to_canonical() {
        // After with_case_sensitive(false), a wrong-case projection resolves to
        // the canonical schema field name.
        let table = mixed_case_table();
        let mut builder = ReadBuilder::new(&table);
        builder
            .with_case_sensitive(false)
            .with_projection(&["nAmE"]);
        let read = builder.new_read().unwrap();
        assert_eq!(read.read_type().len(), 1);
        assert_eq!(read.read_type()[0].name(), "Name");
    }

    #[test]
    fn test_projection_then_case_sensitive_false_is_order_independent() {
        // with_projection BEFORE with_case_sensitive(false): the wrong-case name
        // still resolves case-insensitively because resolution is deferred.
        let table = mixed_case_table();
        let mut builder = ReadBuilder::new(&table);
        builder.with_projection(&["name"]);
        builder.with_case_sensitive(false);
        let read = builder.new_read().unwrap();
        assert_eq!(read.read_type().len(), 1);
        assert_eq!(read.read_type()[0].name(), "Name");
    }

    #[test]
    fn test_case_sensitive_false_then_projection_is_order_independent() {
        // with_case_sensitive(false) BEFORE with_projection: same result.
        let table = mixed_case_table();
        let mut builder = ReadBuilder::new(&table);
        builder.with_case_sensitive(false);
        builder.with_projection(&["name"]);
        let read = builder.new_read().unwrap();
        assert_eq!(read.read_type().len(), 1);
        assert_eq!(read.read_type()[0].name(), "Name");
    }

    #[test]
    fn test_default_case_insensitive_wrong_case_resolves_at_new_read() {
        // kwai default (no with_case_sensitive) + wrong-case projection resolves
        // to the canonical name at read build time.
        let table = mixed_case_table();
        let mut builder = ReadBuilder::new(&table);
        builder.with_projection(&["name"]);
        let read = builder.new_read().unwrap();
        assert_eq!(read.read_type()[0].name(), "Name");
    }

    #[test]
    fn test_new_scan_defers_projection_error_to_new_read() {
        // Contract: new_scan is infallible — it does not resolve the projection,
        // while the same resolution surfaces the error from new_read. Use a
        // genuinely-unknown column so it fails to resolve under any case
        // sensitivity (default is now case-insensitive).
        let table = mixed_case_table();
        let mut builder = ReadBuilder::new(&table);
        builder.with_projection(&["definitely_absent"]);
        let _scan = builder.new_scan(); // must not panic / must succeed
        let err = builder.new_read().unwrap_err();
        assert!(matches!(err, crate::Error::ColumnNotExist { .. }));
    }

    fn ci_fields() -> Vec<DataField> {
        vec![
            DataField::new(0, "id".to_string(), DataType::Int(IntType::new())),
            DataField::new(
                1,
                "Name".to_string(),
                DataType::VarChar(VarCharType::new(50).unwrap()),
            ),
        ]
    }

    #[test]
    fn test_resolve_projection_case_sensitive_exact() {
        // Default (case-sensitive): exact names resolve, wrong case does not.
        let out = super::resolve_projected_fields(
            "db.t".to_string(),
            &ci_fields(),
            &["Name".into()],
            true,
        )
        .unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].name(), "Name");

        let err = super::resolve_projected_fields(
            "db.t".to_string(),
            &ci_fields(),
            &["NAME".into()],
            true,
        )
        .unwrap_err();
        assert!(matches!(err, crate::Error::ColumnNotExist { .. }));
    }

    #[test]
    fn test_resolve_projection_case_insensitive_matches_and_keeps_canonical() {
        // Case-insensitive: wrong-case request resolves to the canonical field.
        let out = super::resolve_projected_fields(
            "db.t".to_string(),
            &ci_fields(),
            &["nAmE".into()],
            false,
        )
        .unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].name(), "Name", "canonical schema name is preserved");
        assert_eq!(out[0].id(), 1);
    }

    #[test]
    fn test_resolve_projection_case_insensitive_ambiguous_errors() {
        let fields = vec![
            DataField::new(0, "Col".to_string(), DataType::Int(IntType::new())),
            DataField::new(1, "col".to_string(), DataType::Int(IntType::new())),
        ];
        let err =
            super::resolve_projected_fields("db.t".to_string(), &fields, &["COL".into()], false)
                .unwrap_err();
        assert!(matches!(err, crate::Error::ConfigInvalid { .. }));
    }

    #[test]
    fn test_resolve_projection_case_insensitive_dedups_by_folded_name() {
        // With case-insensitive matching, `["Name","name"]` both resolve to the
        // canonical `Name` field, so it must be flagged as a duplicate rather
        // than returning the column twice.
        let err = super::resolve_projected_fields(
            "db.t".to_string(),
            &ci_fields(),
            &["Name".into(), "name".into()],
            false,
        )
        .unwrap_err();
        assert!(matches!(err, crate::Error::ConfigInvalid { message }
            if message.contains("Duplicate projection column")));

        // A single request still resolves cleanly.
        let out = super::resolve_projected_fields(
            "db.t".to_string(),
            &ci_fields(),
            &["Name".into()],
            false,
        )
        .unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].name(), "Name");
    }

    #[test]
    fn test_exact_filter_pushdown_is_true_for_partition_only_filter() {
        let table = simple_table();
        let predicate = PredicateBuilder::new(table.schema().fields())
            .equal("dt", crate::spec::Datum::String("2024-01-01".to_string()))
            .unwrap();

        let builder = table.new_read_builder();

        assert!(builder.is_exact_filter_pushdown(&predicate));
    }

    #[test]
    fn test_exact_filter_pushdown_is_false_for_data_filter() {
        let table = simple_table();
        let predicate = PredicateBuilder::new(table.schema().fields())
            .greater_than("id", crate::spec::Datum::Int(1))
            .unwrap();

        let builder = table.new_read_builder();

        assert!(!builder.is_exact_filter_pushdown(&predicate));
    }

    #[tokio::test]
    async fn test_new_read_pushes_filter_to_reader_when_filter_column_not_projected() {
        let tempdir = tempdir().unwrap();
        let table_path = local_file_path(tempdir.path());
        let bucket_dir = tempdir.path().join("bucket-0");
        fs::create_dir_all(&bucket_dir).unwrap();

        let parquet_path = bucket_dir.join("data.parquet");
        write_int_parquet_file(
            &parquet_path,
            vec![("id", vec![1, 2, 3, 4]), ("value", vec![1, 2, 20, 30])],
            Some(2),
        );
        let file_size = fs::metadata(&parquet_path).unwrap().len() as i64;

        let file_io = FileIOBuilder::new("file").build().unwrap();
        let table_schema = TableSchema::new(
            0,
            &Schema::builder()
                .column("id", DataType::Int(IntType::new()))
                .column("value", DataType::Int(IntType::new()))
                .build()
                .unwrap(),
        );
        let table = Table::new(
            file_io,
            Identifier::new("default", "t"),
            table_path,
            table_schema,
            None,
        );

        let split = DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path(local_file_path(&bucket_dir))
            .with_total_buckets(1)
            .with_data_files(vec![test_data_file("data.parquet", 4, file_size)])
            .build()
            .unwrap();

        let predicate = PredicateBuilder::new(table.schema().fields())
            .greater_or_equal("value", crate::spec::Datum::Int(10))
            .unwrap();

        let mut builder = table.new_read_builder();
        builder.with_projection(&["id"]).with_filter(predicate);
        let read = builder.new_read().unwrap();
        let batches = read
            .to_arrow(&[split])
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(collect_int_column(&batches, "id"), vec![3, 4]);
    }

    #[tokio::test]
    async fn test_direct_table_read_with_filter_pushes_filter_to_reader() {
        let tempdir = tempdir().unwrap();
        let table_path = local_file_path(tempdir.path());
        let bucket_dir = tempdir.path().join("bucket-0");
        fs::create_dir_all(&bucket_dir).unwrap();

        let parquet_path = bucket_dir.join("data.parquet");
        write_int_parquet_file(
            &parquet_path,
            vec![("id", vec![1, 2, 3, 4]), ("value", vec![1, 2, 20, 30])],
            Some(2),
        );
        let file_size = fs::metadata(&parquet_path).unwrap().len() as i64;

        let file_io = FileIOBuilder::new("file").build().unwrap();
        let table_schema = TableSchema::new(
            0,
            &Schema::builder()
                .column("id", DataType::Int(IntType::new()))
                .column("value", DataType::Int(IntType::new()))
                .build()
                .unwrap(),
        );
        let table = Table::new(
            file_io,
            Identifier::new("default", "t"),
            table_path,
            table_schema,
            None,
        );

        let split = DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path(local_file_path(&bucket_dir))
            .with_total_buckets(1)
            .with_data_files(vec![test_data_file("data.parquet", 4, file_size)])
            .build()
            .unwrap();

        let predicate = PredicateBuilder::new(table.schema().fields())
            .greater_or_equal("value", crate::spec::Datum::Int(10))
            .unwrap();
        let read = TableRead::new(&table, vec![table.schema().fields()[0].clone()], Vec::new())
            .with_filter(predicate);
        let batches = read
            .to_arrow(&[split])
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(collect_int_column(&batches, "id"), vec![3, 4]);
    }

    #[tokio::test]
    async fn test_new_read_row_filter_filters_rows_within_matching_row_group() {
        let tempdir = tempdir().unwrap();
        let table_path = local_file_path(tempdir.path());
        let bucket_dir = tempdir.path().join("bucket-0");
        fs::create_dir_all(&bucket_dir).unwrap();

        let parquet_path = bucket_dir.join("data.parquet");
        write_int_parquet_file(
            &parquet_path,
            vec![("id", vec![1, 2, 3, 4]), ("value", vec![5, 20, 30, 40])],
            Some(2),
        );
        let file_size = fs::metadata(&parquet_path).unwrap().len() as i64;

        let file_io = FileIOBuilder::new("file").build().unwrap();
        let table_schema = TableSchema::new(
            0,
            &Schema::builder()
                .column("id", DataType::Int(IntType::new()))
                .column("value", DataType::Int(IntType::new()))
                .build()
                .unwrap(),
        );
        let table = Table::new(
            file_io,
            Identifier::new("default", "t"),
            table_path,
            table_schema,
            None,
        );

        let split = DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path(local_file_path(&bucket_dir))
            .with_total_buckets(1)
            .with_data_files(vec![test_data_file("data.parquet", 4, file_size)])
            .build()
            .unwrap();

        let predicate = PredicateBuilder::new(table.schema().fields())
            .greater_or_equal("value", crate::spec::Datum::Int(10))
            .unwrap();

        let mut builder = table.new_read_builder();
        builder.with_projection(&["id"]).with_filter(predicate);
        let read = builder.new_read().unwrap();
        let batches = read
            .to_arrow(&[split])
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(collect_int_column(&batches, "id"), vec![2, 3, 4]);
    }

    #[tokio::test]
    async fn test_reader_pruning_ignores_partition_conjuncts() {
        let tempdir = tempdir().unwrap();
        let table_path = local_file_path(tempdir.path());
        let bucket_dir = tempdir.path().join("dt=2024-01-01").join("bucket-0");
        fs::create_dir_all(&bucket_dir).unwrap();

        write_int_parquet_file(
            &bucket_dir.join("data.parquet"),
            vec![("id", vec![1, 2, 3, 4]), ("value", vec![1, 2, 20, 30])],
            Some(2),
        );
        let file_size = fs::metadata(bucket_dir.join("data.parquet")).unwrap().len() as i64;

        let file_io = FileIOBuilder::new("file").build().unwrap();
        let table_schema = TableSchema::new(
            0,
            &Schema::builder()
                .column("dt", DataType::VarChar(VarCharType::string_type()))
                .column("id", DataType::Int(IntType::new()))
                .column("value", DataType::Int(IntType::new()))
                .partition_keys(["dt"])
                .build()
                .unwrap(),
        );
        let table = Table::new(
            file_io,
            Identifier::new("default", "t"),
            table_path,
            table_schema,
            None,
        );

        let split = DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(BinaryRow::new(1))
            .with_bucket(0)
            .with_bucket_path(local_file_path(&bucket_dir))
            .with_total_buckets(1)
            .with_data_files(vec![test_data_file("data.parquet", 4, file_size)])
            .build()
            .unwrap();

        let predicate = Predicate::and(vec![
            PredicateBuilder::new(table.schema().fields())
                .equal("dt", crate::spec::Datum::String("2024-01-01".to_string()))
                .unwrap(),
            PredicateBuilder::new(table.schema().fields())
                .greater_or_equal("value", crate::spec::Datum::Int(10))
                .unwrap(),
        ]);

        let mut builder = table.new_read_builder();
        builder.with_projection(&["id"]).with_filter(predicate);
        let read = builder.new_read().unwrap();
        let batches = read
            .to_arrow(&[split])
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(collect_int_column(&batches, "id"), vec![3, 4]);
    }

    #[test]
    fn test_with_filter_extracts_row_id_ranges() {
        let file_io = FileIOBuilder::new("file").build().unwrap();
        let table_schema = TableSchema::new(
            0,
            &Schema::builder()
                .column("id", DataType::Int(IntType::new()))
                .column("value", DataType::Int(IntType::new()))
                .build()
                .unwrap(),
        );
        let table = Table::new(
            file_io,
            Identifier::new("default", "t"),
            "/tmp/test".to_string(),
            table_schema,
            None,
        );

        let mut builder = table.new_read_builder();
        let filter = Predicate::and(vec![
            Predicate::Leaf {
                column: crate::spec::ROW_ID_FIELD_NAME.to_string(),
                index: 0,
                data_type: DataType::BigInt(crate::spec::BigIntType::new()),
                op: crate::spec::PredicateOperator::GtEq,
                literals: vec![crate::spec::Datum::Long(10)],
            },
            Predicate::Leaf {
                column: crate::spec::ROW_ID_FIELD_NAME.to_string(),
                index: 0,
                data_type: DataType::BigInt(crate::spec::BigIntType::new()),
                op: crate::spec::PredicateOperator::LtEq,
                literals: vec![crate::spec::Datum::Long(20)],
            },
            PredicateBuilder::new(table.schema().fields())
                .equal("value", crate::spec::Datum::Int(42))
                .unwrap(),
        ]);
        builder.with_filter(filter);

        // _ROW_ID predicates should be extracted into row_ranges
        assert!(builder.row_ranges.is_some());
        let ranges = builder.row_ranges.as_ref().unwrap();
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0].from(), 10);
        assert_eq!(ranges[0].to(), 20);

        // _ROW_ID predicates should be removed from data_predicates
        assert!(!builder.filter.data_predicates.is_empty());
        for p in &builder.filter.data_predicates {
            if let Predicate::Leaf { column, .. } = p {
                assert_ne!(column, crate::spec::ROW_ID_FIELD_NAME);
            }
        }
    }

    #[test]
    fn test_with_filter_skips_extraction_when_row_ranges_set() {
        let file_io = FileIOBuilder::new("file").build().unwrap();
        let table_schema = TableSchema::new(
            0,
            &Schema::builder()
                .column("id", DataType::Int(IntType::new()))
                .build()
                .unwrap(),
        );
        let table = Table::new(
            file_io,
            Identifier::new("default", "t"),
            "/tmp/test".to_string(),
            table_schema,
            None,
        );

        let mut builder = table.new_read_builder();
        builder.with_row_ranges(vec![crate::table::source::RowRange::new(0, 5)]);

        let filter = Predicate::Leaf {
            column: crate::spec::ROW_ID_FIELD_NAME.to_string(),
            index: 0,
            data_type: DataType::BigInt(crate::spec::BigIntType::new()),
            op: crate::spec::PredicateOperator::GtEq,
            literals: vec![crate::spec::Datum::Long(10)],
        };
        builder.with_filter(filter);

        // Explicit row_ranges should be preserved, not overwritten
        let ranges = builder.row_ranges.as_ref().unwrap();
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0].from(), 0);
        assert_eq!(ranges[0].to(), 5);
    }

    #[tokio::test]
    async fn test_direct_table_read_partial_update_with_dv_no_longer_rejected() {
        // Java `KeyValueFileReaderFactory.java:173-187@e8938f347` applies DV at
        // the file-level reader, before any merge function. PartialUpdate/VPU
        // + DV is supported on the read path; the previous "Unsupported"
        // short-circuit has been removed. End-to-end PU+DV correctness is
        // covered by `kv_file_reader::tests::
        // test_kv_reader_partial_update_with_deletion_vector` which uses real
        // parquet + real DV bytes; here we only assert the dispatch no longer
        // rejects with `Error::Unsupported` before any IO.
        let table = partial_update_dv_pk_table();
        let split = DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path("/tmp/test-partial-update-dv-read-builder/bucket-0".to_string())
            .with_total_buckets(1)
            .with_data_files(vec![test_data_file("data.parquet", 1, 0)])
            .with_data_deletion_files(vec![Some(crate::table::source::DeletionFile::new(
                "/tmp/test-partial-update-dv-read-builder/index/dv".to_string(),
                0,
                0,
                None,
            ))])
            .build()
            .unwrap();
        let result = TableRead::new(&table, table.schema().fields().to_vec(), Vec::new())
            .to_arrow(&[split])
            .unwrap()
            .try_collect::<Vec<_>>()
            .await;

        // The fake bucket / DV paths still cannot be opened, so the read
        // eventually fails with an IO-layer error; what matters is that the
        // `Error::Unsupported { message: "...deletion vectors..." }` short-
        // circuit is gone.
        if let Err(crate::Error::Unsupported { ref message }) = result {
            assert!(
                !message.contains("deletion vectors"),
                "PU+DV should no longer be rejected on the read path; got: {message}"
            );
        }
    }
}
