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

//! `versioned-partial-update` merge engine — **batch SELECT** merge function.
//!
//! Stages 1-4 (per `docs/versioned-partial-update-impl-plan.md`) are
//! implemented: single-version columns and multi-version (MV) columns
//! `RowType<latest_version VARCHAR, latest_value T, all_versioned_values
//! MAP<VARCHAR, T>>` both go through the same merge function; structural
//! recognition runs via [`is_multi_version_type`].
//!
//! # Semantics summary
//!
//! Same-PK rows are applied in `(snapshot_id, sequence_number, batch_idx,
//! row_idx)` ascending order (the function sorts internally — `SortMergeReader`
//! only guarantees PK order at this layer). For each non-PK column:
//!
//! * **Single-version + UPSERT mode file**: a non-null source value overwrites
//!   the current accumulator slot.
//! * **Single-version + IGNORE mode file**: a non-null source value fills the
//!   slot only when the slot is currently empty.
//! * **Multi-version**: each `(version, value)` entry from the input row's
//!   MAP child (or the single `(latest_version, latest_value)` pair if MAP is
//!   null) is folded into the accumulator's `BTreeMap<String, ArrayRef>`;
//!   UPSERT overwrites, IGNORE inserts only when the version key is absent.
//! * `_VALUE_KIND = DELETE` clears the accumulator and flags
//!   `current_delete_row = true` (skipped entirely when `ignore-delete` is set).
//! * `_VALUE_KIND = UPDATE_BEFORE` is silently dropped (matches Java).
//! * `_VALUE_KIND = INSERT / UPDATE_AFTER` clears any prior delete flag.
//!
//! Output:
//! * `current_delete_row && !meet_insert` ⇒ [`MergeResult::Omit`] — **batch
//!   SELECT semantics only**. This is **not** suitable for compaction / CDC /
//!   changelog-producer paths that must preserve tombstones; revisit when
//!   streaming output is added (plan risk #3).
//! * Otherwise materialise a one-row [`MergeResult::MaterializedRow`]. MV
//!   columns are rebuilt from the BTreeMap state via
//!   [`build_mv_output`] using `arrow_select::concat`.
//!
//! # Out of scope (deferred to later stages / future PRs)
//!
//! * Aggregator merge engine framework — `fields.<f>.aggregate-function`
//!   columns are statically rejected at schema-validation time (stage 5,
//!   `validate_versioned_partial_update`), so this function never sees one.
//! * IGNORE-mode files require lookup capability (DV / changelog=lookup /
//!   force-lookup); stage-5 validation enforces that constraint at table
//!   load. Read-side handling is the same for both UPSERT and IGNORE files
//!   today.

use crate::spec::{RowKind, VersionedMergeMode};
use crate::table::sort_merge::{BufferedBatch, MergeFunction, MergeResult, MergeRow};
use crate::Error;
use arrow_array::{
    new_null_array, Array, ArrayRef, MapArray, RecordBatch, StringArray, StructArray,
};
use arrow_buffer::OffsetBuffer;
use arrow_schema::SchemaRef;
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, OnceLock};

/// Merge function for `merge-engine=versioned-partial-update`.
pub(crate) struct VersionedPartialUpdateMergeFunction {
    /// Drops DELETE / UPDATE_BEFORE rows entirely when set
    /// (`ignore-delete=true`). Default `false`.
    ignore_delete: bool,
    /// MV column indices in the merge `output_schema`, computed once on the
    /// first merge call. The schema is constant across a single reader run,
    /// so caching avoids the per-PK-group structural rescan that Java does
    /// at Factory time. The OnceLock is `Send + Sync` so this struct stays
    /// usable as `dyn MergeFunction`.
    mv_indices_cache: OnceLock<Vec<usize>>,
}

impl VersionedPartialUpdateMergeFunction {
    pub(crate) fn new(table_options: &HashMap<String, String>) -> crate::Result<Self> {
        let ignore_delete = table_options
            .get("ignore-delete")
            .map(|v| v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);
        Ok(Self {
            ignore_delete,
            mv_indices_cache: OnceLock::new(),
        })
    }

    /// Cached lookup: MV column indices in the supplied `output_schema`.
    /// Initialised on the first call. Subsequent calls reuse the cached
    /// result; if the caller ever passes a different schema, the cached
    /// indices would still be returned — but the kv reader builds the merge
    /// output schema once per read and never mutates it, so that case does
    /// not arise in practice.
    fn mv_indices(&self, output_schema: &SchemaRef) -> &[usize] {
        self.mv_indices_cache.get_or_init(|| {
            let mut indices = Vec::new();
            for (col_idx, field) in output_schema.fields().iter().enumerate() {
                if is_multi_version_type(field.data_type()) {
                    indices.push(col_idx);
                }
            }
            indices
        })
    }
}

impl MergeFunction for VersionedPartialUpdateMergeFunction {
    fn merge(
        &self,
        rows: &[MergeRow],
        batch_buffer: &[BufferedBatch],
        source_output_col_indices: &[usize],
        output_schema: &SchemaRef,
    ) -> crate::Result<MergeResult> {
        if rows.is_empty() {
            return Err(Error::UnexpectedError {
                message: "merge called with empty rows".to_string(),
                source: None,
            });
        }

        // Identify multi-version (MV) columns. Cached on the first call so
        // the structural scan does not repeat per PK group; mirrors Java's
        // Factory-time identification (factory:407-432).
        let n_cols = output_schema.fields().len();
        let mv_indices_slice: &[usize] = self.mv_indices(output_schema);
        let is_mv = |col_idx: usize| -> bool { mv_indices_slice.binary_search(&col_idx).is_ok() };

        // Sort same-PK rows by (snapshot_id, sequence_number, batch_idx, row_idx)
        // ascending. SortMergeReader only orders by PK at this layer — without
        // this internal sort, mixed-stream input would apply rows out of
        // commit/sequence order and silently corrupt UPSERT/IGNORE outcomes.
        let mut ordered: Vec<usize> = (0..rows.len()).collect();
        ordered.sort_by(|&l, &r| {
            let a = &rows[l];
            let b = &rows[r];
            a.snapshot_id
                .cmp(&b.snapshot_id)
                .then_with(|| a.sequence_number.cmp(&b.sequence_number))
                .then_with(|| (a.batch_idx, a.row_idx).cmp(&(b.batch_idx, b.row_idx)))
        });

        // Per-column "current winner" slot — for non-MV columns, the
        // (batch_idx, row_idx) whose value should fill the output column.
        let mut winner_by_col: Vec<Option<(usize, usize)>> = vec![None; n_cols];
        // Per-MV-column accumulator state. Keys are version strings sorted in
        // ascending lex order (BTreeMap iter order); values are 1-row
        // ArrayRefs sliced from the source so `concat` can rebuild the MAP.
        let mut mv_states: HashMap<usize, BTreeMap<String, ArrayRef>> = HashMap::new();
        let mut current_delete_row = false;
        let mut meet_insert = false;

        for &i in &ordered {
            let row = &rows[i];
            let kind = RowKind::from_value(row.value_kind)?;

            if !kind.is_add() {
                // UPDATE_BEFORE is informational and silently dropped.
                if matches!(kind, RowKind::UpdateBefore) {
                    continue;
                }
                // DELETE: clear all accumulators + flag retract, unless ignore_delete.
                if !self.ignore_delete {
                    winner_by_col.fill(None);
                    mv_states.clear();
                    current_delete_row = true;
                    meet_insert = false;
                }
                continue;
            }

            // INSERT or UPDATE_AFTER overrides any prior delete and seeds an
            // insert flag for the final emit decision.
            meet_insert = true;
            current_delete_row = false;

            for col_idx in 0..n_cols {
                let source_array = batch_buffer[row.batch_idx]
                    .column_for_output(col_idx, source_output_col_indices);
                if source_array.is_null(row.row_idx) {
                    continue;
                }
                if is_mv(col_idx) {
                    let state = mv_states.entry(col_idx).or_default();
                    merge_mv_column(state, &*source_array, row.row_idx, row.merge_mode)?;
                    continue;
                }
                match row.merge_mode {
                    VersionedMergeMode::Upsert => {
                        winner_by_col[col_idx] = Some((row.batch_idx, row.row_idx));
                    }
                    VersionedMergeMode::Ignore => {
                        if winner_by_col[col_idx].is_none() {
                            winner_by_col[col_idx] = Some((row.batch_idx, row.row_idx));
                        }
                    }
                }
            }
        }

        // Final retract → batch SELECT semantics: omit the row entirely.
        if current_delete_row && !meet_insert {
            return Ok(MergeResult::Omit);
        }

        // Materialise the merged row from the accumulator slots.
        let output_columns: Vec<ArrayRef> = output_schema
            .fields()
            .iter()
            .enumerate()
            .map(|(col_idx, field)| -> crate::Result<ArrayRef> {
                if is_mv(col_idx) {
                    return match mv_states.get(&col_idx) {
                        Some(state) if !state.is_empty() => build_mv_output(field, state),
                        _ => Ok(new_null_array(field.data_type(), 1)),
                    };
                }
                Ok(match winner_by_col[col_idx] {
                    Some((batch_idx, row_idx)) => batch_buffer[batch_idx]
                        .column_for_output(col_idx, source_output_col_indices)
                        .slice(row_idx, 1),
                    None => {
                        if !field.is_nullable() {
                            return Err(Error::DataInvalid {
                                message: format!(
                                    "versioned-partial-update produced NULL for non-nullable field '{}'",
                                    field.name()
                                ),
                                source: None,
                            });
                        }
                        new_null_array(field.data_type(), 1)
                    }
                })
            })
            .collect::<crate::Result<Vec<_>>>()?;

        let batch = RecordBatch::try_new(output_schema.clone(), output_columns).map_err(|e| {
            Error::UnexpectedError {
                message: format!("Failed to build versioned-partial-update materialized row: {e}"),
                source: Some(Box::new(e)),
            }
        })?;

        Ok(MergeResult::MaterializedRow(batch))
    }
}

/// Apply one MV-column input to the accumulator state. Mirrors Java
/// `mergeMultiVersionColumn` (paimon-core/.../VersionedPartialUpdateMergeFunction.java:262-296).
///
/// Two input shapes:
/// 1. **MAP path** — `all_versioned_values` (struct child 2) is non-null.
///    Iterate every (key, value) entry of that row's map.
/// 2. **Single-pair path** — the MAP is null but `latest_version` (child 0)
///    is non-null. Treat (`latest_version`, `latest_value`) as one entry.
///
/// Per [`merge_version_entry`]: UPSERT overwrites; IGNORE inserts only when
/// the version key is absent.
fn merge_mv_column(
    state: &mut BTreeMap<String, ArrayRef>,
    source: &dyn Array,
    row_idx: usize,
    mode: VersionedMergeMode,
) -> crate::Result<()> {
    let struct_arr = source
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| Error::UnexpectedError {
            message: "versioned-partial-update MV column source is not StructArray".to_string(),
            source: None,
        })?;
    if struct_arr.is_null(row_idx) {
        return Ok(());
    }

    let map_col = struct_arr.column(2);
    let map_arr =
        map_col
            .as_any()
            .downcast_ref::<MapArray>()
            .ok_or_else(|| Error::UnexpectedError {
                message: "versioned-partial-update MV column field 2 is not MapArray".to_string(),
                source: None,
            })?;

    if !map_arr.is_null(row_idx) {
        // MAP path: iterate this row's entries via offsets, since
        // MapArray::value(i) clones the full entries struct sliced — same
        // effect, but we avoid the allocation by indexing directly.
        let offsets = map_arr.value_offsets();
        let start = offsets[row_idx] as usize;
        let end = offsets[row_idx + 1] as usize;
        let entries = map_arr.entries();
        let key_col = entries.column(0);
        let value_col = entries.column(1);
        let key_str = key_col
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| Error::UnexpectedError {
                message: "versioned-partial-update MV map key is not StringArray".to_string(),
                source: None,
            })?;
        for j in start..end {
            if key_str.is_null(j) {
                continue;
            }
            merge_version_entry(
                state,
                key_str.value(j).to_string(),
                value_col.slice(j, 1),
                mode,
            );
        }
        return Ok(());
    }

    // Single-pair path. struct_arr.column(0) is Utf8 by virtue of
    // is_multi_version_type already passing.
    let lv_col = struct_arr.column(0);
    if lv_col.is_null(row_idx) {
        return Ok(());
    }
    let lv_str = lv_col
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| Error::UnexpectedError {
            message: "versioned-partial-update MV latest_version is not StringArray".to_string(),
            source: None,
        })?;
    let key = lv_str.value(row_idx).to_string();
    let val_slice = struct_arr.column(1).slice(row_idx, 1);
    merge_version_entry(state, key, val_slice, mode);
    Ok(())
}

/// Insert one (version, value) entry into the accumulator under the given
/// mode. UPSERT always overwrites; IGNORE skips when the key is already
/// present (parity with Java
/// `mergeVersionEntry`,
/// paimon-core/.../VersionedPartialUpdateMergeFunction.java:304-310).
fn merge_version_entry(
    state: &mut BTreeMap<String, ArrayRef>,
    key: String,
    val: ArrayRef,
    mode: VersionedMergeMode,
) {
    match mode {
        VersionedMergeMode::Upsert => {
            state.insert(key, val);
        }
        VersionedMergeMode::Ignore => {
            state.entry(key).or_insert(val);
        }
    }
}

/// Build the 1-row Arrow `Struct<latest_version, latest_value, all_versioned_values>`
/// from accumulated state. The BTreeMap iteration is lex-ascending, so the
/// last entry is the lex-greatest version — matching Java's `getResult`
/// (paimon-core/.../VersionedPartialUpdateMergeFunction.java:317-340).
fn build_mv_output(
    field: &arrow_schema::FieldRef,
    state: &BTreeMap<String, ArrayRef>,
) -> crate::Result<ArrayRef> {
    use arrow_schema::DataType;
    let DataType::Struct(struct_fields) = field.data_type() else {
        return Err(Error::UnexpectedError {
            message: format!("MV column '{}' is not Struct", field.name()),
            source: None,
        });
    };
    let DataType::Map(entries_field, sorted) = struct_fields[2].data_type() else {
        return Err(Error::UnexpectedError {
            message: format!(
                "MV column '{}' field 2 is not Map (build_mv_output expects \
                 is_multi_version_type to have already passed)",
                field.name()
            ),
            source: None,
        });
    };
    let DataType::Struct(entry_fields) = entries_field.data_type() else {
        return Err(Error::UnexpectedError {
            message: format!("MV column '{}' map entries is not Struct", field.name()),
            source: None,
        });
    };

    // BTreeMap iteration is ascending lex; last() is the greatest key.
    let (latest_key, latest_value) = state.iter().next_back().expect("caller verified non-empty");

    let lv_arr: ArrayRef = Arc::new(StringArray::from(vec![Some(latest_key.clone())]));
    let val_arr: ArrayRef = latest_value.clone();

    // Build the MAP child for all entries.
    let keys: Vec<Option<String>> = state.keys().map(|k| Some(k.clone())).collect();
    let key_array: ArrayRef = Arc::new(StringArray::from(keys));
    let value_arrays: Vec<&dyn Array> = state.values().map(|a| a.as_ref()).collect();
    let value_concat: ArrayRef =
        arrow_select::concat::concat(&value_arrays).map_err(|e| Error::UnexpectedError {
            message: format!("Failed to concat MV map values: {e}"),
            source: Some(Box::new(e)),
        })?;
    let entries_struct = StructArray::new(
        entry_fields.clone(),
        vec![key_array, value_concat],
        None, // entries struct itself is non-null
    );
    let offsets = OffsetBuffer::new(arrow_buffer::ScalarBuffer::from(vec![
        0i32,
        state.len() as i32,
    ]));
    let map_arr = MapArray::new(
        entries_field.clone(),
        offsets,
        entries_struct,
        None,
        *sorted,
    );

    let mv_struct = StructArray::new(
        struct_fields.clone(),
        vec![lv_arr, val_arr, Arc::new(map_arr)],
        None,
    );
    Ok(Arc::new(mv_struct))
}

/// Return `true` if `data_type` matches the structural shape of a paimon
/// **multi-version (MV) column** as it appears on the Arrow side: a
/// `Struct<latest_version: Utf8, latest_value: T, all_versioned_values:
/// Map<Struct{key: Utf8, value: T'}>>` with `T` and `T'` equal up to
/// nullability.
///
/// Mirrors Java
/// `VersionedPartialUpdateMergeFunction.Factory.isMultiVersionType`
/// (paimon-core/.../mergetree/compact/VersionedPartialUpdateMergeFunction.java
/// :438-457). The Arrow encoding of paimon Row/Map/VarChar lives in
/// `crates/paimon/src/arrow/mod.rs:60-104`.
pub(crate) fn is_multi_version_type(data_type: &arrow_schema::DataType) -> bool {
    use arrow_schema::DataType;
    let DataType::Struct(fields) = data_type else {
        return false;
    };
    if fields.len() != 3 {
        return false;
    }
    if fields[0].data_type() != &DataType::Utf8 {
        return false;
    }
    let DataType::Map(entries_field, _) = fields[2].data_type() else {
        return false;
    };
    let DataType::Struct(entry_fields) = entries_field.data_type() else {
        return false;
    };
    if entry_fields.len() != 2 {
        return false;
    }
    if entry_fields[0].data_type() != &DataType::Utf8 {
        return false;
    }
    types_equal_ignore_nullable(fields[1].data_type(), entry_fields[1].data_type())
}

/// Compare two Arrow `DataType`s ignoring `nullable` flags everywhere they
/// appear (struct/map/list child fields). Java's `equalsIgnoreFieldId`
/// retains nullable in the comparison, but Arrow injects `false`-by-default
/// nullable on `Map` entries / inner key fields, so the only safe parity
/// check is to normalise nullable away on both sides.
fn types_equal_ignore_nullable(a: &arrow_schema::DataType, b: &arrow_schema::DataType) -> bool {
    use arrow_schema::DataType;
    use std::sync::Arc;
    fn normalize_field(f: &arrow_schema::Field) -> arrow_schema::Field {
        arrow_schema::Field::new(f.name(), normalize(f.data_type()), true)
    }
    fn normalize(dt: &DataType) -> DataType {
        match dt {
            DataType::List(inner) => DataType::List(Arc::new(normalize_field(inner))),
            DataType::LargeList(inner) => DataType::LargeList(Arc::new(normalize_field(inner))),
            DataType::FixedSizeList(inner, n) => {
                DataType::FixedSizeList(Arc::new(normalize_field(inner)), *n)
            }
            DataType::Struct(fields) => {
                let normalized: Vec<arrow_schema::Field> =
                    fields.iter().map(|f| normalize_field(f)).collect();
                DataType::Struct(normalized.into())
            }
            DataType::Map(entries, sorted) => {
                DataType::Map(Arc::new(normalize_field(entries)), *sorted)
            }
            other => other.clone(),
        }
    }
    normalize(a) == normalize(b)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int32Array, RecordBatch};
    use arrow_buffer::NullBuffer;
    use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
    use std::sync::Arc;

    /// `testSingleVersionUpsert` parity (Java line 244): two UPSERT rows with
    /// `"A"` then `"B"` over the same PK → output keeps `"B"`.
    #[test]
    fn test_single_version_upsert() {
        let schema = pk_val_schema();
        let buf = build_buf(&schema, &[(1, Some(10)), (1, Some(20))]);
        let rows = vec![
            row(
                0,
                0,
                /*snap*/ 1,
                /*seq*/ 1,
                RowKind::Insert,
                VersionedMergeMode::Upsert,
            ),
            row(
                1,
                0,
                /*snap*/ 1,
                /*seq*/ 2,
                RowKind::Insert,
                VersionedMergeMode::Upsert,
            ),
        ];
        let out = run_merge(
            VersionedPartialUpdateMergeFunction::new(&HashMap::new()).unwrap(),
            &rows,
            &buf,
            &schema,
        );
        assert_eq!(extract_int(&out, 1), Some(20));
    }

    /// `testSingleVersionIgnore` parity (Java line 253): UPSERT `"A"` then
    /// IGNORE `"B"` over the same PK → output keeps `"A"` (IGNORE doesn't
    /// overwrite a non-null slot).
    #[test]
    fn test_single_version_ignore_does_not_overwrite() {
        let schema = pk_val_schema();
        let buf = build_buf(&schema, &[(1, Some(10)), (1, Some(99))]);
        let rows = vec![
            row(0, 0, 1, 1, RowKind::Insert, VersionedMergeMode::Upsert),
            row(1, 0, 1, 2, RowKind::Insert, VersionedMergeMode::Ignore),
        ];
        let out = run_merge(
            VersionedPartialUpdateMergeFunction::new(&HashMap::new()).unwrap(),
            &rows,
            &buf,
            &schema,
        );
        assert_eq!(extract_int(&out, 1), Some(10));
    }

    /// `testDeleteRemovesRecord` (Java line 388): UPSERT followed by DELETE
    /// over the same PK → result is omitted entirely.
    #[test]
    fn test_delete_removes_record() {
        let schema = pk_val_schema();
        let buf = build_buf(&schema, &[(1, Some(10)), (1, None)]);
        let rows = vec![
            row(0, 0, 1, 1, RowKind::Insert, VersionedMergeMode::Upsert),
            row(1, 0, 1, 2, RowKind::Delete, VersionedMergeMode::Upsert),
        ];
        let result = run_merge_raw(
            VersionedPartialUpdateMergeFunction::new(&HashMap::new()).unwrap(),
            &rows,
            &buf,
            &schema,
        );
        assert!(matches!(result, MergeResult::Omit));
    }

    /// `testRetractIgnoredWhenConfigured` (Java line 398): with
    /// `ignore-delete=true`, the DELETE is silently dropped and the INSERT
    /// survives.
    #[test]
    fn test_retract_ignored_when_configured() {
        let schema = pk_val_schema();
        let buf = build_buf(&schema, &[(1, Some(10)), (1, None)]);
        let rows = vec![
            row(0, 0, 1, 1, RowKind::Insert, VersionedMergeMode::Upsert),
            row(1, 0, 1, 2, RowKind::Delete, VersionedMergeMode::Upsert),
        ];
        let mut opts = HashMap::new();
        opts.insert("ignore-delete".to_string(), "true".to_string());
        let out = run_merge(
            VersionedPartialUpdateMergeFunction::new(&opts).unwrap(),
            &rows,
            &buf,
            &schema,
        );
        assert_eq!(extract_int(&out, 1), Some(10));
    }

    /// Stage-3 Rust-specific: rows arrive at merge() in the WRONG order
    /// (newer snapshot first). Internal sort must still pick the latest
    /// `(snapshot, seq)` row.
    #[test]
    fn test_internal_sort_handles_out_of_order_input() {
        let schema = pk_val_schema();
        let buf = build_buf(&schema, &[(1, Some(99)), (1, Some(10))]);
        // First row has SNAPSHOT=2 (newer); second has SNAPSHOT=1 (older).
        // Without internal sort, the latter would "win" with UPSERT.
        let rows = vec![
            row(
                0,
                0,
                /*snap*/ 2,
                1,
                RowKind::Insert,
                VersionedMergeMode::Upsert,
            ),
            row(
                1,
                0,
                /*snap*/ 1,
                5,
                RowKind::Insert,
                VersionedMergeMode::Upsert,
            ),
        ];
        let out = run_merge(
            VersionedPartialUpdateMergeFunction::new(&HashMap::new()).unwrap(),
            &rows,
            &buf,
            &schema,
        );
        // Snapshot=2 wins regardless of input order or sequence number.
        assert_eq!(extract_int(&out, 1), Some(99));
    }

    /// Stage-3 Rust-specific: same `(snapshot, sequence)` tie. Stable
    /// tie-breaker `(batch_idx, row_idx)` ensures the result is
    /// deterministic; latest-position wins with UPSERT.
    #[test]
    fn test_tie_breaker_is_deterministic() {
        let schema = pk_val_schema();
        let buf = build_buf(&schema, &[(1, Some(10)), (1, Some(20))]);
        let rows = vec![
            row(0, 0, 1, 10, RowKind::Insert, VersionedMergeMode::Upsert),
            row(1, 0, 1, 10, RowKind::Insert, VersionedMergeMode::Upsert),
        ];
        let out = run_merge(
            VersionedPartialUpdateMergeFunction::new(&HashMap::new()).unwrap(),
            &rows,
            &buf,
            &schema,
        );
        // batch_idx=1 sorts after batch_idx=0 → row 1 wins under UPSERT.
        assert_eq!(extract_int(&out, 1), Some(20));
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    /// `(pk Int32 NOT NULL, val Int32 NULL)` — minimal output schema for
    /// Stage-3 single-version tests.
    fn pk_val_schema() -> SchemaRef {
        Arc::new(ArrowSchema::new(vec![
            ArrowField::new("pk", ArrowDataType::Int32, false),
            ArrowField::new("val", ArrowDataType::Int32, true),
        ]))
    }

    /// One `BufferedBatch::Source` per `(pk, val)` pair, each with a single row.
    fn build_buf(schema: &SchemaRef, rows: &[(i32, Option<i32>)]) -> Vec<BufferedBatch> {
        rows.iter()
            .map(|(pk, val)| {
                let pk_arr = Int32Array::from(vec![*pk]);
                let val_arr = Int32Array::from(vec![*val]);
                let batch =
                    RecordBatch::try_new(schema.clone(), vec![Arc::new(pk_arr), Arc::new(val_arr)])
                        .unwrap();
                BufferedBatch::Source(batch)
            })
            .collect()
    }

    fn row(
        batch_idx: usize,
        row_idx: usize,
        snapshot_id: i64,
        seq: i64,
        kind: RowKind,
        mode: VersionedMergeMode,
    ) -> MergeRow {
        MergeRow {
            batch_idx,
            row_idx,
            snapshot_id,
            sequence_number: seq,
            value_kind: kind as i8,
            user_sequences: vec![],
            merge_mode: mode,
        }
    }

    fn run_merge_raw(
        merge_fn: VersionedPartialUpdateMergeFunction,
        rows: &[MergeRow],
        buf: &[BufferedBatch],
        schema: &SchemaRef,
    ) -> MergeResult {
        // For the Stage-3 tests output_schema == source schema; identity map.
        let source_output: Vec<usize> = (0..schema.fields().len()).collect();
        merge_fn.merge(rows, buf, &source_output, schema).unwrap()
    }

    fn run_merge(
        merge_fn: VersionedPartialUpdateMergeFunction,
        rows: &[MergeRow],
        buf: &[BufferedBatch],
        schema: &SchemaRef,
    ) -> MergeResult {
        let result = run_merge_raw(merge_fn, rows, buf, schema);
        // The non-raw helper expects a row to be emitted; tests using DELETE
        // semantics call `run_merge_raw` directly.
        if matches!(result, MergeResult::Omit) {
            panic!("expected MaterializedRow, got Omit");
        }
        result
    }

    fn extract_int(result: &MergeResult, col_idx: usize) -> Option<i32> {
        match result {
            MergeResult::MaterializedRow(batch) => {
                let arr = batch
                    .column(col_idx)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .expect("test fixture column should be Int32");
                if arr.is_null(0) {
                    None
                } else {
                    Some(arr.value(0))
                }
            }
            MergeResult::SourceRow { .. } => panic!("expected MaterializedRow, got SourceRow"),
            MergeResult::Omit => panic!("expected MaterializedRow, got Omit"),
        }
    }

    // ===========================================================================
    // Stage-6 ordering / tombstone matrix (single-version columns)
    // ===========================================================================

    /// Plan matrix case 6 — same `(snapshot, sequence)` tie at different
    /// snapshots: snap=1 wins or loses purely by snapshot id, never by
    /// sequence. Latest snapshot must dominate even when both rows share a
    /// sequence number.
    #[test]
    fn test_cross_snapshot_same_seq_uses_later_snapshot() {
        let schema = pk_val_schema();
        let buf = build_buf(&schema, &[(1, Some(10)), (1, Some(99))]);
        // (snap=1, seq=10, val=10) then (snap=2, seq=10, val=99). Snap=2 wins.
        let rows = vec![
            row(
                0,
                0,
                /*snap*/ 1,
                /*seq*/ 10,
                RowKind::Insert,
                VersionedMergeMode::Upsert,
            ),
            row(
                1,
                0,
                /*snap*/ 2,
                /*seq*/ 10,
                RowKind::Insert,
                VersionedMergeMode::Upsert,
            ),
        ];
        let out = run_merge(
            VersionedPartialUpdateMergeFunction::new(&HashMap::new()).unwrap(),
            &rows,
            &buf,
            &schema,
        );
        assert_eq!(extract_int(&out, 1), Some(99));
    }

    /// Plan matrix case 7 — different sequences but later snapshot has the
    /// smaller seq. Snapshot id dominates over sequence per the
    /// `(snapshot_id, sequence_number, batch_idx, row_idx)` ordering, so
    /// the snap=2 row wins even though its seq is lower.
    #[test]
    fn test_cross_snapshot_diff_seq_snapshot_id_dominates() {
        let schema = pk_val_schema();
        let buf = build_buf(&schema, &[(1, Some(10)), (1, Some(99))]);
        // (snap=1, seq=20, val=10) then (snap=2, seq=10, val=99). Snap=2 wins.
        let rows = vec![
            row(
                0,
                0,
                /*snap*/ 1,
                /*seq*/ 20,
                RowKind::Insert,
                VersionedMergeMode::Upsert,
            ),
            row(
                1,
                0,
                /*snap*/ 2,
                /*seq*/ 10,
                RowKind::Insert,
                VersionedMergeMode::Upsert,
            ),
        ];
        let out = run_merge(
            VersionedPartialUpdateMergeFunction::new(&HashMap::new()).unwrap(),
            &rows,
            &buf,
            &schema,
        );
        assert_eq!(extract_int(&out, 1), Some(99));
    }

    /// Plan matrix case 8 (DELETE + lower-level old row, the **core
    /// tombstone test**) — a stale L1 INSERT, a fresher L0 INSERT, and an
    /// L0 DELETE all arrive in the SAME `merge()` call. The DELETE must
    /// produce `MergeResult::Omit` despite the L1 row existing earlier in
    /// the rows slice; the planner can no longer accidentally let the old
    /// L1 value resurrect (regression guard for plan risk #3).
    #[test]
    fn test_delete_with_lower_level_old_row_in_same_merge() {
        let schema = pk_val_schema();
        // batch 0 = L1 old row val=A(=10), batch 1 = L0 new row val=B(=20),
        // batch 2 = L0 DELETE marker (val arbitrary).
        let buf = build_buf(&schema, &[(1, Some(10)), (1, Some(20)), (1, None)]);
        let rows = vec![
            // L1 row at older snapshot.
            row(
                0,
                0,
                /*snap*/ 1,
                /*seq*/ 5,
                RowKind::Insert,
                VersionedMergeMode::Upsert,
            ),
            // L0 row at newer snapshot, then its DELETE.
            row(
                1,
                0,
                /*snap*/ 2,
                /*seq*/ 10,
                RowKind::Insert,
                VersionedMergeMode::Upsert,
            ),
            row(
                2,
                0,
                /*snap*/ 2,
                /*seq*/ 11,
                RowKind::Delete,
                VersionedMergeMode::Upsert,
            ),
        ];
        let result = run_merge_raw(
            VersionedPartialUpdateMergeFunction::new(&HashMap::new()).unwrap(),
            &rows,
            &buf,
            &schema,
        );
        // The L1 old row (snap=1, seq=5) is processed first in the
        // (snap, seq, batch, row) sort, then L0 INSERT (snap=2, seq=10),
        // then L0 DELETE (snap=2, seq=11) which clears state and flags
        // current_delete_row without a subsequent insert → Omit.
        assert!(
            matches!(result, MergeResult::Omit),
            "expected MergeResult::Omit",
        );
    }

    // ===========================================================================
    // Stage-4 multi-version (MV) column tests
    // ===========================================================================

    /// `(pk Int32 NOT NULL, mv Struct<latest_version: Utf8, latest_value: Utf8,
    /// all_versioned_values: Map<Struct{key:Utf8, value:Utf8}>>)` —
    /// minimal Arrow schema covering the MV column shape produced by
    /// paimon-rust's spec→Arrow conversion (see `crates/paimon/src/arrow/mod.rs`).
    fn pk_mv_schema() -> SchemaRef {
        let entry_struct = ArrowDataType::Struct(
            vec![
                ArrowField::new("key", ArrowDataType::Utf8, false),
                ArrowField::new("value", ArrowDataType::Utf8, true),
            ]
            .into(),
        );
        let mv_struct = ArrowDataType::Struct(
            vec![
                ArrowField::new("latest_version", ArrowDataType::Utf8, true),
                ArrowField::new("latest_value", ArrowDataType::Utf8, true),
                ArrowField::new(
                    "all_versioned_values",
                    ArrowDataType::Map(
                        Arc::new(ArrowField::new("entries", entry_struct, false)),
                        false,
                    ),
                    true,
                ),
            ]
            .into(),
        );
        Arc::new(ArrowSchema::new(vec![
            ArrowField::new("pk", ArrowDataType::Int32, false),
            ArrowField::new("mv", mv_struct, true),
        ]))
    }

    /// Build a single-row `BufferedBatch` for `pk_mv_schema()`. `entries`:
    /// - `None` → MAP child is null, single-pair path
    /// - `Some(&[..])` → MAP child is non-null with the supplied entries
    ///   (may be empty)
    fn build_mv_row(
        schema: &SchemaRef,
        pk: i32,
        latest_version: Option<&str>,
        latest_value: Option<&str>,
        entries: Option<&[(&str, Option<&str>)]>,
    ) -> BufferedBatch {
        let pk_arr: ArrayRef = Arc::new(Int32Array::from(vec![pk]));

        let ArrowDataType::Struct(struct_fields) = schema.field(1).data_type() else {
            unreachable!()
        };
        let ArrowDataType::Map(entries_field, sorted) = struct_fields[2].data_type() else {
            unreachable!()
        };
        let ArrowDataType::Struct(entry_fields) = entries_field.data_type() else {
            unreachable!()
        };

        let lv_arr: ArrayRef = Arc::new(StringArray::from(vec![latest_version.map(String::from)]));
        let val_arr: ArrayRef = Arc::new(StringArray::from(vec![latest_value.map(String::from)]));

        let map_arr: ArrayRef = match entries {
            None => {
                // Null MAP child (single-pair path expected at merge time).
                let empty_keys: ArrayRef =
                    Arc::new(StringArray::from(Vec::<Option<String>>::new()));
                let empty_vals: ArrayRef =
                    Arc::new(StringArray::from(Vec::<Option<String>>::new()));
                let entries_struct =
                    StructArray::new(entry_fields.clone(), vec![empty_keys, empty_vals], None);
                let offsets = OffsetBuffer::new(arrow_buffer::ScalarBuffer::from(vec![0i32, 0i32]));
                let nulls = NullBuffer::from(vec![false]);
                Arc::new(MapArray::new(
                    entries_field.clone(),
                    offsets,
                    entries_struct,
                    Some(nulls),
                    *sorted,
                ))
            }
            Some(pairs) => {
                let keys: Vec<Option<String>> =
                    pairs.iter().map(|(k, _)| Some(k.to_string())).collect();
                let values: Vec<Option<String>> =
                    pairs.iter().map(|(_, v)| v.map(String::from)).collect();
                let key_arr: ArrayRef = Arc::new(StringArray::from(keys));
                let val_arr_inner: ArrayRef = Arc::new(StringArray::from(values));
                let entries_struct =
                    StructArray::new(entry_fields.clone(), vec![key_arr, val_arr_inner], None);
                let offsets = OffsetBuffer::new(arrow_buffer::ScalarBuffer::from(vec![
                    0i32,
                    pairs.len() as i32,
                ]));
                Arc::new(MapArray::new(
                    entries_field.clone(),
                    offsets,
                    entries_struct,
                    None,
                    *sorted,
                ))
            }
        };

        let mv_struct: ArrayRef = Arc::new(StructArray::new(
            struct_fields.clone(),
            vec![lv_arr, val_arr, map_arr],
            None,
        ));
        let batch = RecordBatch::try_new(schema.clone(), vec![pk_arr, mv_struct]).unwrap();
        BufferedBatch::Source(batch)
    }

    /// Returns `(latest_version, latest_value, sorted_pairs)` for the MV column
    /// in the materialised result.
    fn extract_mv(
        result: &MergeResult,
        col_idx: usize,
    ) -> (String, Option<String>, Vec<(String, Option<String>)>) {
        let MergeResult::MaterializedRow(batch) = result else {
            panic!("expected MaterializedRow")
        };
        let mv = batch
            .column(col_idx)
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("MV column should be Struct");
        let lv = mv
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("latest_version is Utf8");
        let val = mv
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("latest_value is Utf8");
        let map = mv
            .column(2)
            .as_any()
            .downcast_ref::<MapArray>()
            .expect("MV map");
        let entries = map.entries();
        let key_arr = entries
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("map key is Utf8");
        let val_arr = entries
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("map value is Utf8");
        let offsets = map.value_offsets();
        let start = offsets[0] as usize;
        let end = offsets[1] as usize;
        let mut pairs: Vec<(String, Option<String>)> = (start..end)
            .map(|j| {
                (
                    key_arr.value(j).to_string(),
                    if val_arr.is_null(j) {
                        None
                    } else {
                        Some(val_arr.value(j).to_string())
                    },
                )
            })
            .collect();
        pairs.sort_by(|a, b| a.0.cmp(&b.0));
        (
            lv.value(0).to_string(),
            if val.is_null(0) {
                None
            } else {
                Some(val.value(0).to_string())
            },
            pairs,
        )
    }

    /// Java parity: `testMultiVersionExistingKeyUpsert` (line 371). Same key
    /// `v1` arrives twice with UPSERT — second value overwrites first.
    #[test]
    fn test_mv_existing_key_upsert() {
        let schema = pk_mv_schema();
        let buf = vec![
            build_mv_row(&schema, 1, Some("v1"), Some("original"), None),
            build_mv_row(&schema, 1, Some("v1"), Some("updated"), None),
        ];
        let rows = vec![
            row(0, 0, 1, 1, RowKind::Insert, VersionedMergeMode::Upsert),
            row(1, 0, 1, 2, RowKind::Insert, VersionedMergeMode::Upsert),
        ];
        let out = run_merge(
            VersionedPartialUpdateMergeFunction::new(&HashMap::new()).unwrap(),
            &rows,
            &buf,
            &schema,
        );
        let (lv, val, pairs) = extract_mv(&out, 1);
        assert_eq!(lv, "v1");
        assert_eq!(val.as_deref(), Some("updated"));
        assert_eq!(pairs, vec![("v1".to_string(), Some("updated".to_string()))]);
    }

    /// Java parity: `testMultiVersionExistingKeyIgnore` (line 362). Same key
    /// `v1` arrives twice — second is IGNORE mode and must NOT overwrite.
    #[test]
    fn test_mv_existing_key_ignore() {
        let schema = pk_mv_schema();
        let buf = vec![
            build_mv_row(&schema, 1, Some("v1"), Some("original"), None),
            build_mv_row(&schema, 1, Some("v1"), Some("updated"), None),
        ];
        let rows = vec![
            row(0, 0, 1, 1, RowKind::Insert, VersionedMergeMode::Upsert),
            row(1, 0, 1, 2, RowKind::Insert, VersionedMergeMode::Ignore),
        ];
        let out = run_merge(
            VersionedPartialUpdateMergeFunction::new(&HashMap::new()).unwrap(),
            &rows,
            &buf,
            &schema,
        );
        let (lv, val, pairs) = extract_mv(&out, 1);
        assert_eq!(lv, "v1");
        assert_eq!(val.as_deref(), Some("original"));
        assert_eq!(
            pairs,
            vec![("v1".to_string(), Some("original".to_string()))]
        );
    }

    /// Java parity: `testUpsertExistingKeyUpdatesLatestByLexicographicOrder`
    /// (line 530). Sequence v1, v2, v1=val1_updated under UPSERT. Final
    /// state: latest is v2 (lex max), v1 holds the updated value, both keys
    /// in the map.
    #[test]
    fn test_mv_upsert_existing_key_updates_latest_by_lex_order() {
        let schema = pk_mv_schema();
        let buf = vec![
            build_mv_row(&schema, 1, Some("v1"), Some("val1"), None),
            build_mv_row(&schema, 1, Some("v2"), Some("val2"), None),
            build_mv_row(&schema, 1, Some("v1"), Some("val1_updated"), None),
        ];
        let rows = vec![
            row(0, 0, 1, 1, RowKind::Insert, VersionedMergeMode::Upsert),
            row(1, 0, 1, 2, RowKind::Insert, VersionedMergeMode::Upsert),
            row(2, 0, 1, 3, RowKind::Insert, VersionedMergeMode::Upsert),
        ];
        let out = run_merge(
            VersionedPartialUpdateMergeFunction::new(&HashMap::new()).unwrap(),
            &rows,
            &buf,
            &schema,
        );
        let (lv, val, pairs) = extract_mv(&out, 1);
        assert_eq!(lv, "v2");
        assert_eq!(val.as_deref(), Some("val2"));
        assert_eq!(
            pairs,
            vec![
                ("v1".to_string(), Some("val1_updated".to_string())),
                ("v2".to_string(), Some("val2".to_string())),
            ]
        );
    }

    /// Java parity: `testMapPathLatestIsDerivedFromLexicographicOrder`
    /// (line 540). Compacted base row carries MAP {v1,v2,v3} plus
    /// latest_version="v2"; result must derive latest from the MAP and pick
    /// "v3" (the lex-greatest), regardless of the base row's claim.
    #[test]
    fn test_mv_map_path_latest_derived_from_lex_order() {
        let schema = pk_mv_schema();
        let entries: Vec<(&str, Option<&str>)> = vec![
            ("v1", Some("val1")),
            ("v2", Some("val2")),
            ("v3", Some("val3")),
        ];
        let buf = vec![build_mv_row(
            &schema,
            1,
            Some("v2"),
            Some("val2"),
            Some(&entries),
        )];
        let rows = vec![row(0, 0, 1, 5, RowKind::Insert, VersionedMergeMode::Upsert)];
        let out = run_merge(
            VersionedPartialUpdateMergeFunction::new(&HashMap::new()).unwrap(),
            &rows,
            &buf,
            &schema,
        );
        let (lv, val, pairs) = extract_mv(&out, 1);
        assert_eq!(lv, "v3");
        assert_eq!(val.as_deref(), Some("val3"));
        assert_eq!(
            pairs,
            vec![
                ("v1".to_string(), Some("val1".to_string())),
                ("v2".to_string(), Some("val2".to_string())),
                ("v3".to_string(), Some("val3".to_string())),
            ]
        );
    }

    /// Stage-4 Rust-specific: MAP path where fields[0]/[1] are both null.
    /// The merge function must still consume the MAP entries via the MAP
    /// path and produce a correct result (Java's `mvRow.isNullAt(2)` check
    /// would otherwise short-circuit before the single-pair branch).
    #[test]
    fn test_mv_map_path_with_null_latest_pair() {
        let schema = pk_mv_schema();
        let entries: Vec<(&str, Option<&str>)> = vec![("v1", Some("a")), ("v9", Some("z"))];
        let buf = vec![build_mv_row(&schema, 1, None, None, Some(&entries))];
        let rows = vec![row(0, 0, 1, 1, RowKind::Insert, VersionedMergeMode::Upsert)];
        let out = run_merge(
            VersionedPartialUpdateMergeFunction::new(&HashMap::new()).unwrap(),
            &rows,
            &buf,
            &schema,
        );
        let (lv, val, pairs) = extract_mv(&out, 1);
        assert_eq!(lv, "v9");
        assert_eq!(val.as_deref(), Some("z"));
        assert_eq!(
            pairs,
            vec![
                ("v1".to_string(), Some("a".to_string())),
                ("v9".to_string(), Some("z".to_string())),
            ]
        );
    }

    /// Stage-4 sanity: `is_multi_version_type` matches the canonical Arrow
    /// shape and rejects close-but-not-quite shapes.
    #[test]
    fn test_is_multi_version_type_recognition() {
        let mv_dt = pk_mv_schema().field(1).data_type().clone();
        assert!(is_multi_version_type(&mv_dt));

        // Wrong number of fields.
        let bad_two_fields = ArrowDataType::Struct(
            vec![
                ArrowField::new("latest_version", ArrowDataType::Utf8, true),
                ArrowField::new("latest_value", ArrowDataType::Utf8, true),
            ]
            .into(),
        );
        assert!(!is_multi_version_type(&bad_two_fields));

        // Mismatched value type (Utf8 vs Int32).
        let entry_struct_int = ArrowDataType::Struct(
            vec![
                ArrowField::new("key", ArrowDataType::Utf8, false),
                ArrowField::new("value", ArrowDataType::Int32, true),
            ]
            .into(),
        );
        let mismatched = ArrowDataType::Struct(
            vec![
                ArrowField::new("latest_version", ArrowDataType::Utf8, true),
                ArrowField::new("latest_value", ArrowDataType::Utf8, true),
                ArrowField::new(
                    "all_versioned_values",
                    ArrowDataType::Map(
                        Arc::new(ArrowField::new("entries", entry_struct_int, false)),
                        false,
                    ),
                    true,
                ),
            ]
            .into(),
        );
        assert!(!is_multi_version_type(&mismatched));

        // Plain non-MV column.
        assert!(!is_multi_version_type(&ArrowDataType::Int32));
    }
}
