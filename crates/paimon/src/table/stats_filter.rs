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

//! Data-level stats predicate filtering for manifest entries and data evolution groups.

use super::Table;
use crate::arrow::schema_evolution::create_index_mapping;
use crate::predicate_stats::{
    data_leaf_may_match, missing_field_may_match, predicates_may_match_with_schema, StatsAccessor,
};
use crate::spec::{
    extract_datum, BinaryRow, DataField, DataFileMeta, DataType, Datum, DvReadMode, MergeEngine,
    Predicate,
};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub(super) struct FileStatsRows {
    pub(super) row_count: i64,
    min_values: Option<BinaryRow>,
    max_values: Option<BinaryRow>,
    null_counts: Vec<Option<i64>>,
    /// Maps schema field index → stats index. `None` means identity mapping
    /// (stats cover all schema fields in order). `Some` is used when
    /// `value_stats_cols` or `write_cols` is present (dense mode).
    stats_col_mapping: Option<Vec<Option<usize>>>,
}

impl FileStatsRows {
    /// Build a `FileStatsRows` for manifest-level partition stats (no column mapping).
    pub(super) fn for_manifest_partition(
        row_count: i64,
        min_values: Option<BinaryRow>,
        max_values: Option<BinaryRow>,
        null_counts: Vec<Option<i64>>,
    ) -> Self {
        Self {
            row_count,
            min_values,
            max_values,
            null_counts,
            stats_col_mapping: None,
        }
    }

    /// Build file stats from a data file, respecting `value_stats_cols`.
    ///
    /// When `value_stats_cols` is `None`, stats cover all fields in `schema_fields` order.
    /// When `value_stats_cols` is `Some`, stats are in dense mode — only covering those
    /// columns, and the mapping from schema field index to stats index is built by name.
    pub(super) fn from_data_file(file: &DataFileMeta, schema_fields: &[DataField]) -> Self {
        let stats_col_mapping = if let Some(cols) = &file.value_stats_cols {
            let col_index: HashMap<&str, usize> = cols
                .iter()
                .enumerate()
                .map(|(i, c)| (c.as_str(), i))
                .collect();
            let mapping: Vec<Option<usize>> = schema_fields
                .iter()
                .map(|field| col_index.get(field.name()).copied())
                .collect();
            Some(mapping)
        } else if let Some(cols) = &file.write_cols {
            let col_index: HashMap<&str, usize> = cols
                .iter()
                .enumerate()
                .map(|(i, c)| (c.as_str(), i))
                .collect();
            let mapping: Vec<Option<usize>> = schema_fields
                .iter()
                .map(|field| col_index.get(field.name()).copied())
                .collect();
            Some(mapping)
        } else {
            None
        };

        Self {
            row_count: file.row_count,
            min_values: BinaryRow::from_serialized_bytes(file.value_stats.min_values()).ok(),
            max_values: BinaryRow::from_serialized_bytes(file.value_stats.max_values()).ok(),
            null_counts: file.value_stats.null_counts().clone(),
            stats_col_mapping,
        }
    }

    /// Resolve a schema field index to the corresponding stats index.
    fn stats_index(&self, schema_index: usize) -> Option<usize> {
        match &self.stats_col_mapping {
            None => Some(schema_index),
            Some(mapping) => mapping.get(schema_index).copied().flatten(),
        }
    }

    fn stats_null_count(&self, stats_index: usize) -> Option<i64> {
        self.null_counts.get(stats_index).copied().flatten()
    }
}

impl StatsAccessor for FileStatsRows {
    fn row_count(&self) -> i64 {
        self.row_count
    }

    fn null_count(&self, index: usize) -> Option<i64> {
        let stats_index = self.stats_index(index)?;
        self.stats_null_count(stats_index)
    }

    fn min_value(&self, index: usize, data_type: &DataType) -> Option<Datum> {
        let stats_index = self.stats_index(index)?;
        self.min_values
            .as_ref()
            .and_then(|row| extract_stats_datum(row, stats_index, data_type))
    }

    fn max_value(&self, index: usize, data_type: &DataType) -> Option<Datum> {
        let stats_index = self.stats_index(index)?;
        self.max_values
            .as_ref()
            .and_then(|row| extract_stats_datum(row, stats_index, data_type))
    }
}

#[derive(Debug)]
pub(super) struct ResolvedStatsSchema {
    file_fields: Vec<DataField>,
    field_mapping: Vec<Option<usize>>,
}

fn identity_field_mapping(num_fields: usize) -> Vec<Option<usize>> {
    (0..num_fields).map(Some).collect()
}

fn normalize_field_mapping(mapping: Option<Vec<i32>>, num_fields: usize) -> Vec<Option<usize>> {
    mapping
        .map(|field_mapping| {
            field_mapping
                .into_iter()
                .map(|index| usize::try_from(index).ok())
                .collect()
        })
        .unwrap_or_else(|| identity_field_mapping(num_fields))
}

/// Whether [`data_file_matches_predicates`] should run for a given manifest entry.
///
/// Returning `false` means **skip the value-stats filter and keep the entry
/// unconditionally**, mirroring Java
/// `paimon-core/.../KeyValueFileStoreScan.java:154-162@e8938f347` "FRESHNESS:
/// keep L0 unconditionally; stats pruning only applies to L1+".
///
/// Stage 3 of `docs/dv-impl-plan.md` unifies three concerns through this
/// helper:
///
/// 1. **C7** — DV + FRESHNESS L0 must skip the stats filter (Java contract;
///    L0 in FRESHNESS reaches the reader for data freshness).
/// 2. **C4** — non-DV PK Deduplicate / PartialUpdate / VersionedPartialUpdate
///    L0 must skip the stats filter. Multiple L0 files with PK overlap can
///    have non-overlapping `value_stats`; if a predicate prunes the file
///    holding the *newer* version while keeping the file with the older
///    version, sort-merge sees a partial PK group and **returns the stale
///    value**. See `dv-impl-plan.md` SECTION-RISKS #3 (note: triggers only
///    when L0 stats are non-overlapping; same-stats prunes are wasteful but
///    safe).
/// 3. **L1+ stats pruning is always preserved** — compacted files do not
///    have PK overlap by construction, so stats are trustworthy and the
///    pruning is a performance invariant that this helper must not regress.
///
/// `merge_engine` takes [`Option<MergeEngine>`] so the caller can convert a
/// `Result<MergeEngine>` parse outcome once with `.ok()` and pass the result
/// through filter closures (the `crate::Error` half of `Result` is not `Copy`,
/// so threading the `Result` itself through closures is awkward). `None`
/// falls open to `Deduplicate` semantics, matching the safe-default style of
/// `should_skip_level_zero_for_scan`.
pub(crate) fn should_apply_value_stats_to_entry(
    level: i32,
    has_primary_keys: bool,
    dv_enabled: bool,
    dv_read_mode: DvReadMode,
    merge_engine: Option<MergeEngine>,
) -> bool {
    // L1+ files are always safe — compacted, no PK overlap → stats are
    // trustworthy and pruning is a performance invariant.
    if level > 0 {
        return true;
    }
    // Non-PK tables: L0 stats pruning is safe (no sort-merge concern).
    if !has_primary_keys {
        return true;
    }
    // PK + DV enabled: PERFORMANCE strips L0 in the plan upper layer (helper
    // unreachable on L0); FRESHNESS keeps L0 but must skip the stats filter to
    // mirror Java's "unconditional keep" contract.
    if dv_enabled {
        return match dv_read_mode {
            DvReadMode::Performance => true,
            DvReadMode::Freshness => false,
        };
    }
    // PK + non-DV: C4 fix.
    // FirstRow strips L0 in the plan upper layer (helper unreachable on L0);
    // other engines (Deduplicate / PartialUpdate / VersionedPartialUpdate)
    // keep L0 visible and must skip the stats filter to preserve the
    // sort-merge inputs (otherwise C4 stale-value bug surfaces).
    let engine = merge_engine.unwrap_or(MergeEngine::Deduplicate);
    !matches!(
        engine,
        MergeEngine::Deduplicate | MergeEngine::PartialUpdate | MergeEngine::VersionedPartialUpdate
    )
}

/// Pruning is evaluated per file and fails open when stats cannot be
/// interpreted safely, including schema mismatches, incompatible stats arity,
/// and missing or corrupted stats.
///
/// L0 PK / FRESHNESS / non-DV-Dedup gating is **separate** — see
/// [`should_apply_value_stats_to_entry`] for whether this function should be
/// called at all for a given entry.
pub(super) fn data_file_matches_predicates(
    file: &DataFileMeta,
    predicates: &[Predicate],
    current_schema_id: i64,
    schema_fields: &[DataField],
) -> bool {
    if predicates.is_empty() {
        return true;
    }

    if predicates
        .iter()
        .any(|p| matches!(p, Predicate::AlwaysFalse))
    {
        return false;
    }
    if predicates
        .iter()
        .all(|p| matches!(p, Predicate::AlwaysTrue))
    {
        return true;
    }

    if file.schema_id != current_schema_id {
        return true;
    }

    let stats = FileStatsRows::from_data_file(file, schema_fields);
    let field_mapping = identity_field_mapping(schema_fields.len());
    predicates_may_match_with_schema(predicates, &stats, &field_mapping, schema_fields)
}

async fn resolve_stats_schema(
    table: &Table,
    file_schema_id: i64,
    schema_cache: &mut HashMap<i64, Option<Arc<ResolvedStatsSchema>>>,
) -> Option<Arc<ResolvedStatsSchema>> {
    if let Some(cached) = schema_cache.get(&file_schema_id) {
        return cached.clone();
    }

    let table_schema = table.schema();
    let current_fields = table_schema.fields();
    let resolved = if file_schema_id == table_schema.id() {
        Some(Arc::new(ResolvedStatsSchema {
            file_fields: current_fields.to_vec(),
            field_mapping: identity_field_mapping(current_fields.len()),
        }))
    } else {
        let file_schema = table.schema_manager().schema(file_schema_id).await.ok()?;
        let file_fields = file_schema.fields().to_vec();
        Some(Arc::new(ResolvedStatsSchema {
            field_mapping: normalize_field_mapping(
                create_index_mapping(current_fields, &file_fields),
                current_fields.len(),
            ),
            file_fields,
        }))
    };

    schema_cache.insert(file_schema_id, resolved.clone());
    resolved
}

pub(super) async fn data_file_matches_predicates_for_table(
    table: &Table,
    file: &DataFileMeta,
    predicates: &[Predicate],
    schema_cache: &mut HashMap<i64, Option<Arc<ResolvedStatsSchema>>>,
) -> bool {
    if predicates.is_empty() {
        return true;
    }

    if file.schema_id == table.schema().id() {
        return data_file_matches_predicates(
            file,
            predicates,
            table.schema().id(),
            table.schema().fields(),
        );
    }

    let Some(resolved) = resolve_stats_schema(table, file.schema_id, schema_cache).await else {
        return true;
    };

    let stats = FileStatsRows::from_data_file(file, &resolved.file_fields);
    predicates_may_match_with_schema(
        predicates,
        &stats,
        &resolved.field_mapping,
        &resolved.file_fields,
    )
}

fn extract_stats_datum(row: &BinaryRow, index: usize, data_type: &DataType) -> Option<Datum> {
    let min_row_len = BinaryRow::cal_fix_part_size_in_bytes(row.arity()) as usize;
    if index >= row.arity() as usize || row.data().len() < min_row_len {
        return None;
    }

    match extract_datum(row, index, data_type) {
        Ok(Some(datum)) => Some(datum),
        Ok(None) | Err(_) => None,
    }
}

/// Check whether a data-evolution file group *may* contain rows matching all `predicates`.
///
/// In data evolution mode, a logical row can be spread across multiple files with
/// different column sets. After `group_by_overlapping_row_id`, each group contains
/// files covering the same row ID range. Stats for each field come from the file
/// with the highest `max_sequence_number` that actually contains that field.
///
/// Reference: [DataEvolutionFileStoreScan.evolutionStats](https://github.com/apache/paimon/blob/master/paimon-core/src/main/java/org/apache/paimon/operation/DataEvolutionFileStoreScan.java)
pub(super) fn data_evolution_group_matches_predicates(
    group: &[DataFileMeta],
    predicates: &[Predicate],
    table_fields: &[DataField],
) -> bool {
    if predicates.is_empty() || group.is_empty() {
        return true;
    }

    if predicates
        .iter()
        .any(|p| matches!(p, Predicate::AlwaysFalse))
    {
        return false;
    }
    if predicates
        .iter()
        .all(|p| matches!(p, Predicate::AlwaysTrue))
    {
        return true;
    }

    // Sort files by max_sequence_number descending so the highest-seq file wins per field.
    let mut sorted_files: Vec<&DataFileMeta> = group.iter().collect();
    sorted_files.sort_by_key(|f| std::cmp::Reverse(f.max_sequence_number));

    // For each table field, find which file (index in sorted_files) provides it.
    // Use file_data_columns (based on write_cols) to determine which file contains
    // the field, not file_stats_columns (based on value_stats_cols) which only
    // indicates stats coverage.
    let field_sources: Vec<Option<(usize, usize)>> = {
        let per_file_columns: Vec<Vec<&str>> = sorted_files
            .iter()
            .map(|file| file_data_columns(file, table_fields))
            .collect();
        table_fields
            .iter()
            .enumerate()
            .map(|(field_idx, field)| {
                for (file_idx, cols) in per_file_columns.iter().enumerate() {
                    if cols.iter().any(|c| *c == field.name()) {
                        return Some((file_idx, field_idx));
                    }
                }
                None
            })
            .collect()
    };

    // Build per-file stats without arity validation — data evolution files
    // may have fewer columns than the current table schema.
    let file_stats: Vec<FileStatsRows> = sorted_files
        .iter()
        .map(|file| FileStatsRows::from_data_file(file, table_fields))
        .collect();

    // row_count is the max across the group (overlapping row ranges).
    let row_count = group.iter().map(|f| f.row_count).max().unwrap_or(0);

    predicates.iter().all(|predicate| {
        data_evolution_predicate_may_match(
            predicate,
            table_fields,
            &field_sources,
            &file_stats,
            row_count,
        )
    })
}

/// Resolve which columns a file actually contains (for field source resolution).
/// Uses `write_cols` if present, otherwise assumes all table fields.
/// This is distinct from `file_stats_columns` which resolves stats coverage.
fn file_data_columns<'a>(file: &'a DataFileMeta, table_fields: &'a [DataField]) -> Vec<&'a str> {
    match &file.write_cols {
        Some(cols) => cols.iter().map(|s| s.as_str()).collect(),
        None => table_fields.iter().map(|f| f.name()).collect(),
    }
}

fn data_evolution_predicate_may_match(
    predicate: &Predicate,
    table_fields: &[DataField],
    field_sources: &[Option<(usize, usize)>],
    file_stats: &[FileStatsRows],
    row_count: i64,
) -> bool {
    match predicate {
        Predicate::AlwaysTrue => true,
        Predicate::AlwaysFalse => false,
        Predicate::And(children) => children.iter().all(|child| {
            data_evolution_predicate_may_match(
                child,
                table_fields,
                field_sources,
                file_stats,
                row_count,
            )
        }),
        Predicate::Or(_) | Predicate::Not(_) => true,
        Predicate::Leaf {
            index,
            data_type,
            op,
            literals,
            ..
        } => {
            let Some(source) = field_sources.get(*index).copied().flatten() else {
                return missing_field_may_match(*op, row_count);
            };
            let (file_idx, field_index) = source;
            let stats = &file_stats[file_idx];
            let stats_data_type = table_fields
                .get(*index)
                .map(|f| f.data_type())
                .unwrap_or(data_type);
            data_leaf_may_match(
                field_index,
                stats_data_type,
                data_type,
                *op,
                literals,
                stats,
            )
        }
    }
}

/// Groups data files by overlapping `row_id_range` for data evolution.
///
/// Files are sorted by `(first_row_id, -max_sequence_number)`. Files whose row ID ranges
/// overlap are merged into the same group (they contain different columns for the same rows).
/// Files without `first_row_id` become their own group.
///
/// Reference: [DataEvolutionSplitGenerator](https://github.com/apache/paimon/blob/master/paimon-core/src/main/java/org/apache/paimon/table/source/splitread/DataEvolutionSplitGenerator.java)
pub(crate) fn group_by_overlapping_row_id(mut files: Vec<DataFileMeta>) -> Vec<Vec<DataFileMeta>> {
    files.sort_by(|a, b| {
        let a_row_id = a.first_row_id.unwrap_or(i64::MIN);
        let b_row_id = b.first_row_id.unwrap_or(i64::MIN);
        a_row_id
            .cmp(&b_row_id)
            .then_with(|| b.max_sequence_number.cmp(&a.max_sequence_number))
    });

    let mut result: Vec<Vec<DataFileMeta>> = Vec::new();
    let mut current_group: Vec<DataFileMeta> = Vec::new();
    let mut current_range_end: i64 = i64::MIN;

    for file in files {
        match file.row_id_range() {
            None => {
                if !current_group.is_empty() {
                    result.push(std::mem::take(&mut current_group));
                    current_range_end = i64::MIN;
                }
                result.push(vec![file]);
            }
            Some((start, end)) => {
                if current_group.is_empty() || start <= current_range_end {
                    if end > current_range_end {
                        current_range_end = end;
                    }
                    current_group.push(file);
                } else {
                    result.push(std::mem::take(&mut current_group));
                    current_range_end = end;
                    current_group.push(file);
                }
            }
        }
    }
    if !current_group.is_empty() {
        result.push(current_group);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    /// L1+ files always run stats pruning (performance invariant; compacted
    /// files do not have PK overlap, stats are trustworthy).
    #[test]
    fn test_should_apply_value_stats_l1_always_true() {
        for engine in [
            MergeEngine::Deduplicate,
            MergeEngine::PartialUpdate,
            MergeEngine::VersionedPartialUpdate,
            MergeEngine::FirstRow,
        ] {
            for dv_enabled in [false, true] {
                for mode in [DvReadMode::Performance, DvReadMode::Freshness] {
                    assert!(
                        should_apply_value_stats_to_entry(1, true, dv_enabled, mode, Some(engine)),
                        "L1 must always apply stats (engine={engine:?}, dv={dv_enabled}, mode={mode:?})"
                    );
                }
            }
        }
    }

    /// Non-PK tables: L0 stats pruning is safe (no sort-merge concern).
    #[test]
    fn test_should_apply_value_stats_non_pk_l0_always_true() {
        assert!(should_apply_value_stats_to_entry(
            0,
            false,
            false,
            DvReadMode::Performance,
            Some(MergeEngine::Deduplicate),
        ));
        assert!(should_apply_value_stats_to_entry(
            0,
            false,
            true,
            DvReadMode::Freshness,
            Some(MergeEngine::Deduplicate),
        ));
    }

    /// DV + FRESHNESS + L0 must skip the stats filter (Java keep-L0
    /// unconditionally contract). This is the corrected matrix row vs.
    /// `dv-impl-plan.md` line 324 which originally wrote `true`.
    #[test]
    fn test_should_apply_value_stats_dv_freshness_l0_is_false() {
        assert!(!should_apply_value_stats_to_entry(
            0,
            true,
            true,
            DvReadMode::Freshness,
            Some(MergeEngine::Deduplicate),
        ));
    }

    /// DV + PERFORMANCE + L0: helper is unreachable in practice because the
    /// planner already strips L0; the safe default is `true` (run pruning if
    /// somehow reached).
    #[test]
    fn test_should_apply_value_stats_dv_performance_l0_safe_default() {
        assert!(should_apply_value_stats_to_entry(
            0,
            true,
            true,
            DvReadMode::Performance,
            Some(MergeEngine::Deduplicate),
        ));
    }

    /// **C4 fix** — non-DV PK + (Deduplicate / PartialUpdate / VPU) + L0 must
    /// skip the stats filter to keep the full PK overlap group reachable for
    /// sort-merge. Pruning here would surface stale values when L0 stats
    /// are non-overlapping (see `dv-impl-plan.md` SECTION-RISKS #3).
    #[test]
    fn test_should_apply_value_stats_non_dv_pk_l0_dedup_skips() {
        for engine in [
            MergeEngine::Deduplicate,
            MergeEngine::PartialUpdate,
            MergeEngine::VersionedPartialUpdate,
        ] {
            assert!(
                !should_apply_value_stats_to_entry(
                    0,
                    true,
                    false,
                    DvReadMode::Performance,
                    Some(engine),
                ),
                "C4: PK + non-DV + {engine:?} + L0 must skip value-stats filter"
            );
        }
    }

    /// FirstRow + L0: helper is unreachable in practice because the planner
    /// strips L0 for FirstRow. Safe default is `true`.
    #[test]
    fn test_should_apply_value_stats_first_row_l0_safe_default() {
        assert!(should_apply_value_stats_to_entry(
            0,
            true,
            false,
            DvReadMode::Performance,
            Some(MergeEngine::FirstRow),
        ));
    }

    /// merge_engine = None (parse error → caller passes `.ok()`): helper
    /// falls open to Deduplicate semantics. L0 + PK + non-DV → skip stats.
    #[test]
    fn test_should_apply_value_stats_merge_engine_none_falls_open() {
        assert!(!should_apply_value_stats_to_entry(
            0,
            true,
            false,
            DvReadMode::Performance,
            None,
        ));
    }

    /// **C4 trigger scenario** (`dv-impl-plan.md` SECTION-RISKS #3 minimal
    /// repro): two L0 files for PK k=1 with non-overlapping `value_stats`
    /// (file_old covers OLD_V, file_new covers NEW_V). Without the C4 fix a
    /// per-file value-stats prune would drop one side and leak the other's
    /// stale value through sort-merge. This test asserts the helper returns
    /// `false` for both files at L0 — i.e. neither is gated out — so the
    /// sort-merge pipeline sees both versions and the latest seq wins.
    /// Mirrors Java `KeyValueFileStoreScan.java:154-162@e8938f347`'s
    /// keep-L0-unfiltered behavior.
    #[test]
    fn test_should_apply_value_stats_overlapping_l0_pk_dedup_skips_both_files() {
        // Both entries share the helper inputs (level=0, has_pk=true,
        // dv=false, performance, dedup) — the helper is per-entry and only
        // reads (level, table-shape, mode), not the file's value_stats.
        // The C4 fix lives in the gate, not in stats inspection.
        let apply = should_apply_value_stats_to_entry(
            0,
            true,
            false,
            DvReadMode::Performance,
            Some(MergeEngine::Deduplicate),
        );
        assert!(
            !apply,
            "C4: both L0 entries with disjoint stats must skip value-stats prune; \
             pruning either side would expose stale values through sort-merge"
        );

        // Same scenario at L1+ MUST still apply stats — pruning here is a
        // performance invariant (compacted files have no PK overlap, stats
        // are trustworthy). This guards against a regression that would
        // turn off the prune everywhere just to fix C4.
        let apply_l1 = should_apply_value_stats_to_entry(
            1,
            true,
            false,
            DvReadMode::Performance,
            Some(MergeEngine::Deduplicate),
        );
        assert!(
            apply_l1,
            "C4 fix must NOT regress L1+ stats pruning (performance invariant)"
        );
    }
}
