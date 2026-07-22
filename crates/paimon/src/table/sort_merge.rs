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

//! Sort-merge reader with LoserTree for primary-key table reads.
//!
//! Merges multiple sorted `ArrowRecordBatchStream`s by primary key using a
//! tournament tree (LoserTree), applying a [`MergeFunction`] to deduplicate
//! rows sharing the same key.
//!
//! Reference:
//! - Java Paimon: `SortMergeReaderWithMinHeap`
//! - DataFusion: `SortPreservingMergeStream` (LoserTree layout)
//! - Arrow-row: `RowConverter` for efficient key comparison

use crate::spec::{AggregationConfig, DataField, PartialUpdateConfig, RowKind, VersionedMergeMode};
use crate::table::aggregator::{new_aggregator, FieldAggregator};
use crate::table::ArrowRecordBatchStream;
use crate::Error;
use arrow_array::{new_null_array, ArrayRef, Int64Array, Int8Array, RecordBatch};
use arrow_row::{RowConverter, Rows, SortField};
use arrow_schema::SchemaRef;
use arrow_select::interleave::interleave;
use async_stream::try_stream;
use futures::StreamExt;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex};

// ---------------------------------------------------------------------------
// MergeFunction
// ---------------------------------------------------------------------------

/// Buffered batches used by the merge reader.
///
/// Source batches keep the internal read schema, while materialized batches
/// already match the merge output schema.
#[derive(Clone)]
pub(crate) enum BufferedBatch {
    Source(RecordBatch),
    Materialized(RecordBatch),
}

impl BufferedBatch {
    /// Arrow memory footprint of the wrapped batch, used for explicit
    /// per-query memory accounting of the merge buffer.
    pub(crate) fn memory_size(&self) -> usize {
        match self {
            BufferedBatch::Source(b) | BufferedBatch::Materialized(b) => b.get_array_memory_size(),
        }
    }

    pub(crate) fn column_for_output<'a>(
        &'a self,
        output_col_idx: usize,
        source_output_col_indices: &[usize],
    ) -> &'a dyn arrow_array::Array {
        match self {
            Self::Source(batch) => batch
                .column(source_output_col_indices[output_col_idx])
                .as_ref(),
            Self::Materialized(batch) => batch.column(output_col_idx).as_ref(),
        }
    }
}

/// A row reference as an index into the batch buffer.
pub(crate) struct MergeRow {
    /// Index into the shared batch buffer.
    pub batch_idx: usize,
    pub row_idx: usize,
    /// Commit snapshot id of the file this row came from. Used by
    /// `versioned-partial-update` merge to apply same-PK rows in
    /// `(snapshot_id, sequence_number)` order. Set to `0` when the
    /// engine doesn't care (Deduplicate / PartialUpdate).
    pub snapshot_id: i64,
    pub sequence_number: i64,
    pub value_kind: i8,
    /// User-defined sequence values from `sequence.field` (empty if not configured).
    pub user_sequences: Vec<Option<i128>>,
    /// Per-file merge mode (UPSERT / IGNORE) carried via `DataFileMeta._MERGE_MODE`.
    /// `Upsert` for engines other than `versioned-partial-update`.
    pub merge_mode: VersionedMergeMode,
}

/// Per-stream metadata shared by all rows from one input stream. Aligned by
/// stream index with `SortMergeReaderBuilder.streams`. Stage-3 introduces
/// this to drive `versioned-partial-update` ordering and per-file mode
/// dispatch; other engines pass an array of defaults.
#[derive(Debug, Clone, Copy)]
pub(crate) struct StreamMeta {
    pub snapshot_id: i64,
    pub merge_mode: VersionedMergeMode,
}

impl Default for StreamMeta {
    fn default() -> Self {
        Self {
            snapshot_id: 0,
            merge_mode: VersionedMergeMode::Upsert,
        }
    }
}

#[cfg(test)]
impl MergeRow {
    fn source_batch<'a>(
        &self,
        batch_buffer: &'a [BufferedBatch],
    ) -> crate::Result<&'a RecordBatch> {
        match batch_buffer.get(self.batch_idx) {
            Some(BufferedBatch::Source(batch)) => Ok(batch),
            Some(BufferedBatch::Materialized(_)) => Err(Error::UnexpectedError {
                message: format!(
                    "Merge row unexpectedly referenced a materialized batch at index {}",
                    self.batch_idx
                ),
                source: None,
            }),
            None => Err(Error::UnexpectedError {
                message: format!(
                    "Merge row referenced batch index {} outside the current buffer",
                    self.batch_idx
                ),
                source: None,
            }),
        }
    }
}

/// Merge result for rows sharing the same primary key.
pub(crate) enum MergeResult {
    /// Reuse an existing source row from the batch buffer.
    SourceRow { batch_idx: usize, row_idx: usize },
    /// Emit a synthesized one-row batch matching the merge output schema.
    MaterializedRow(RecordBatch),
    /// Omit this key from the output.
    Omit,
}

/// Merge function applied to rows sharing the same primary key.
///
/// Deduplicate-style engines can keep returning a source row. Future
/// field-wise engines may instead materialize a new output row.
pub(crate) trait MergeFunction: Send + Sync {
    /// Merge all rows sharing the same key into a final output result.
    fn merge(
        &self,
        rows: &[MergeRow],
        batch_buffer: &[BufferedBatch],
        source_output_col_indices: &[usize],
        output_schema: &SchemaRef,
    ) -> crate::Result<MergeResult>;
}

/// Deduplicate merge: keeps the row with the highest sequence.
/// When `sequence.field` is configured (one or more fields), compares user
/// sequences lexicographically first, then falls back to system
/// `_SEQUENCE_NUMBER` as tie-breaker.
/// When sequence numbers are equal, keeps the last-added row (last-writer-wins).
/// Filters out DELETE and UPDATE_BEFORE rows.
pub(crate) struct DeduplicateMergeFunction;

fn compare_sequence_order(lhs: &MergeRow, rhs: &MergeRow) -> Ordering {
    match (lhs.user_sequences.is_empty(), rhs.user_sequences.is_empty()) {
        (false, false) => lhs
            .user_sequences
            .cmp(&rhs.user_sequences)
            .then_with(|| lhs.sequence_number.cmp(&rhs.sequence_number)),
        _ => lhs.sequence_number.cmp(&rhs.sequence_number),
    }
}

impl MergeFunction for DeduplicateMergeFunction {
    fn merge(
        &self,
        rows: &[MergeRow],
        _batch_buffer: &[BufferedBatch],
        _source_output_col_indices: &[usize],
        _output_schema: &SchemaRef,
    ) -> crate::Result<MergeResult> {
        let winner = rows
            .iter()
            .reduce(|best, r| {
                let ord = compare_sequence_order(r, best);
                // >= semantics: last-writer-wins for equal values.
                if ord.is_ge() {
                    r
                } else {
                    best
                }
            })
            .expect("merge called with empty rows");
        if RowKind::from_value(winner.value_kind)?.is_add() {
            Ok(MergeResult::SourceRow {
                batch_idx: winner.batch_idx,
                row_idx: winner.row_idx,
            })
        } else {
            Ok(MergeResult::Omit)
        }
    }
}

/// Basic partial-update merge: for each non-key column, keep the latest
/// non-null value ordered by user sequence (if configured) then system sequence.
///
/// DELETE / UPDATE_BEFORE rows are treated as unsupported in this mode.
#[derive(Debug, Clone, Copy)]
pub(crate) struct PartialUpdateMergeFunction(());

impl PartialUpdateMergeFunction {
    pub(crate) fn new(
        table_options: &HashMap<String, String>,
        table_name: &str,
    ) -> crate::Result<Self> {
        PartialUpdateConfig::new(table_options).validate_runtime_mode(true, table_name)?;
        Ok(Self(()))
    }
}

impl MergeFunction for PartialUpdateMergeFunction {
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

        let mut ordered_row_indices: Vec<usize> = (0..rows.len()).collect();
        ordered_row_indices.sort_by(|&lhs_idx, &rhs_idx| {
            compare_sequence_order(&rows[lhs_idx], &rows[rhs_idx])
                .then_with(|| lhs_idx.cmp(&rhs_idx))
        });

        let mut latest_non_null_by_col: Vec<Option<(usize, usize)>> =
            vec![None; output_schema.fields().len()];

        for row_idx in ordered_row_indices {
            let row = &rows[row_idx];
            if !RowKind::from_value(row.value_kind)?.is_add() {
                return Err(crate::Error::Unsupported {
                    message: "merge-engine=partial-update basic mode does not support DELETE or UPDATE_BEFORE rows".to_string(),
                });
            }

            for (output_col_idx, latest_non_null) in latest_non_null_by_col.iter_mut().enumerate() {
                let source_array = batch_buffer[row.batch_idx]
                    .column_for_output(output_col_idx, source_output_col_indices);
                if !source_array.is_null(row.row_idx) {
                    *latest_non_null = Some((row.batch_idx, row.row_idx));
                }
            }
        }

        let output_columns: Vec<ArrayRef> = output_schema
            .fields()
            .iter()
            .enumerate()
            .map(|(output_col_idx, field)| {
                Ok(match latest_non_null_by_col[output_col_idx] {
                    Some((batch_idx, row_idx)) => batch_buffer[batch_idx]
                        .column_for_output(output_col_idx, source_output_col_indices)
                        .slice(row_idx, 1),
                    None => {
                        if !field.is_nullable() {
                            return Err(Error::DataInvalid {
                                message: format!(
                                    "merge-engine=partial-update produced NULL for non-nullable field '{}'",
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
                message: format!("Failed to build partial-update materialized row: {e}"),
                source: Some(Box::new(e)),
            }
        })?;

        Ok(MergeResult::MaterializedRow(batch))
    }
}

// ---------------------------------------------------------------------------
// AggregateMergeFunction
// ---------------------------------------------------------------------------

/// Basic aggregation merge: apply a per-field aggregator across all rows
/// sharing the same primary key.
///
/// For each output column, the aggregator is selected by the following
/// priority (matching Java `AggregateMergeFunction#getAggFuncName`):
///
/// 1. Columns listed in the `sequence_fields` constructor argument (which
///    the reader populates from the `sequence.field` table option) are
///    forced to `last_value`, regardless of any per-field configuration.
/// 2. Primary-key columns get no aggregator; their value is copied through
///    from a representative row (Paimon guarantees same-PK rows share the
///    same key).
/// 3. `fields.<col>.aggregate-function`, if set.
/// 4. `fields.default-aggregate-function`, if set.
/// 5. Fall back to `last_non_null_value`.
///
/// Sequence is checked before primary key so a column that is both a PK
/// and a sequence field still gets `last_value`.
///
/// DELETE / UPDATE_BEFORE rows are rejected at runtime; retract handling
/// is left to a follow-up commit.
///
/// `aggregators` is held behind a `Mutex` so the implementation can mutate
/// per-key accumulators inside `MergeFunction::merge(&self, ...)` without
/// changing the trait signature shared with the other merge functions.  The
/// merge function is invoked sequentially by `sort_merge_stream`, so the
/// lock is effectively uncontended.
///
/// Reference: Java `org.apache.paimon.mergetree.compact.aggregate.AggregateMergeFunction`.
#[derive(Debug)]
pub(crate) struct AggregateMergeFunction {
    /// One slot per output column.  `None` marks primary-key columns that are
    /// copied through; `Some` holds the aggregator that owns the column.
    aggregators: Mutex<Vec<Option<Box<dyn FieldAggregator>>>>,
}

impl AggregateMergeFunction {
    pub(crate) fn new(
        table_options: &HashMap<String, String>,
        table_name: &str,
        output_fields: &[DataField],
        primary_keys: &[String],
        sequence_fields: &[String],
    ) -> crate::Result<Self> {
        let config = AggregationConfig::new(table_options);
        config.validate_runtime_mode(true, table_name)?;

        let pk_set: HashSet<&str> = primary_keys.iter().map(String::as_str).collect();
        let seq_set: HashSet<&str> = sequence_fields.iter().map(String::as_str).collect();

        // Per Java AggregateMergeFunction#createFieldAggregators, the priority
        // order is:
        //   1. sequence fields  → `last_value`
        //   2. primary keys     → no aggregator (PK columns are copied through)
        //   3. per-field `fields.<col>.aggregate-function`
        //   4. table-level `fields.default-aggregate-function`
        //   5. fall back to `last_non_null_value`
        // Sequence is checked before PK so a column that is both PK and sequence
        // field still gets `last_value` (matching Java).
        let aggregators: Vec<Option<Box<dyn FieldAggregator>>> = output_fields
            .iter()
            .map(|field| -> crate::Result<Option<Box<dyn FieldAggregator>>> {
                let name = field.name();
                let agg_name: &str = if seq_set.contains(name) {
                    "last_value"
                } else if pk_set.contains(name) {
                    return Ok(None);
                } else if let Some(per_field) = config.agg_function_for_field(name) {
                    per_field
                } else if let Some(default) = config.default_agg_function() {
                    default
                } else {
                    "last_non_null_value"
                };
                Ok(Some(new_aggregator(
                    agg_name,
                    name,
                    field.data_type(),
                    table_options,
                )?))
            })
            .collect::<crate::Result<Vec<_>>>()?;

        Ok(Self {
            aggregators: Mutex::new(aggregators),
        })
    }
}

impl MergeFunction for AggregateMergeFunction {
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

        // Reject retract rows up-front so partial accumulation cannot leak
        // into the output if a DELETE shows up mid-group.
        for row in rows {
            if !RowKind::from_value(row.value_kind)?.is_add() {
                return Err(crate::Error::Unsupported {
                    message: "merge-engine=aggregation basic mode does not support DELETE or UPDATE_BEFORE rows".to_string(),
                });
            }
        }

        // Sort row indices by user sequence (if configured), then system
        // sequence, so per-field aggregators see the canonical "ascending
        // sequence" order documented in their contracts.
        let mut ordered_row_indices: Vec<usize> = (0..rows.len()).collect();
        ordered_row_indices.sort_by(|&lhs_idx, &rhs_idx| {
            compare_sequence_order(&rows[lhs_idx], &rows[rhs_idx])
                .then_with(|| lhs_idx.cmp(&rhs_idx))
        });

        let mut aggregators = self
            .aggregators
            .lock()
            .map_err(|e| Error::UnexpectedError {
                message: format!("AggregateMergeFunction aggregator mutex poisoned: {e}"),
                source: None,
            })?;
        for slot in aggregators.iter_mut() {
            if let Some(agg) = slot.as_mut() {
                agg.reset();
            }
        }

        for &row_idx in &ordered_row_indices {
            let row = &rows[row_idx];
            for (col_idx, slot) in aggregators.iter_mut().enumerate() {
                if let Some(agg) = slot.as_mut() {
                    let source_array = batch_buffer[row.batch_idx]
                        .column_for_output(col_idx, source_output_col_indices);
                    agg.agg(source_array, row.row_idx)?;
                }
            }
        }

        // Use the last sorted row to source primary-key column values: every
        // row in the group shares the same PK by construction, so any row
        // works; picking the last one keeps the slice cheap to compute.
        let pk_source = &rows[*ordered_row_indices.last().unwrap()];

        let output_columns: Vec<ArrayRef> = aggregators
            .iter()
            .enumerate()
            .map(|(col_idx, slot)| -> crate::Result<ArrayRef> {
                match slot {
                    Some(agg) => agg.result(),
                    None => Ok(batch_buffer[pk_source.batch_idx]
                        .column_for_output(col_idx, source_output_col_indices)
                        .slice(pk_source.row_idx, 1)),
                }
            })
            .collect::<crate::Result<Vec<_>>>()?;

        // Defensive check: non-nullable fields must not contain NULL on the
        // merged output (e.g. `min` on an all-NULL value group, or a NULL
        // primary-key cell on the source row).  Split the message so the
        // operator knows whether to look at the aggregator config or at the
        // upstream data.
        for (col_idx, field) in output_schema.fields().iter().enumerate() {
            if !field.is_nullable() && output_columns[col_idx].is_null(0) {
                let message = match aggregators[col_idx].as_ref() {
                    Some(agg) => format!(
                        "merge-engine=aggregation: aggregator '{}' produced NULL for \
                         non-nullable field '{}'",
                        agg.name(),
                        field.name()
                    ),
                    None => format!(
                        "merge-engine=aggregation: primary-key column '{}' contains NULL on a \
                         source row; declare the column nullable or fix the upstream data",
                        field.name()
                    ),
                };
                return Err(Error::DataInvalid {
                    message,
                    source: None,
                });
            }
        }

        let batch = RecordBatch::try_new(output_schema.clone(), output_columns).map_err(|e| {
            Error::UnexpectedError {
                message: format!("Failed to build aggregation materialized row: {e}"),
                source: Some(Box::new(e)),
            }
        })?;

        Ok(MergeResult::MaterializedRow(batch))
    }
}

// ---------------------------------------------------------------------------
// SortMergeCursor
// ---------------------------------------------------------------------------

/// Cursor tracking position within a single stream's current RecordBatch.
struct SortMergeCursor {
    batch: RecordBatch,
    /// Row-encoded keys for the current batch (via arrow-row).
    rows: Rows,
    offset: usize,
}

impl SortMergeCursor {
    fn is_finished(&self) -> bool {
        self.offset >= self.rows.num_rows()
    }

    fn current_row(&self) -> arrow_row::Row<'_> {
        self.rows.row(self.offset)
    }

    fn advance(&mut self) {
        self.offset += 1;
    }

    fn sequence_number(&self, seq_index: usize) -> i64 {
        let col = self.batch.column(seq_index);
        let arr = col
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("_SEQUENCE_NUMBER column must be Int64");
        arr.value(self.offset)
    }

    fn value_kind(&self, value_kind_index: usize) -> i8 {
        let col = self.batch.column(value_kind_index);
        match col.as_any().downcast_ref::<Int8Array>() {
            Some(arr) if !col.is_null(self.offset) => arr.value(self.offset),
            _ => 0, // default to INSERT for NULL or missing _VALUE_KIND
        }
    }

    /// Read the user-defined sequence field value (cast to i64 for ordering).
    /// Returns None if the column is NULL at this row.
    ///
    /// Supports the same types as Java Paimon's `UserDefinedSeqComparator`:
    /// TinyInt, SmallInt, Int, BigInt, Timestamp, Date, Decimal.
    fn user_sequence(&self, user_seq_index: usize) -> Option<i128> {
        let col = self.batch.column(user_seq_index);
        if col.is_null(self.offset) {
            return None;
        }
        use arrow_array::*;
        let any = col.as_any();
        if let Some(arr) = any.downcast_ref::<Int64Array>() {
            return Some(arr.value(self.offset) as i128);
        }
        if let Some(arr) = any.downcast_ref::<Int32Array>() {
            return Some(arr.value(self.offset) as i128);
        }
        if let Some(arr) = any.downcast_ref::<Int16Array>() {
            return Some(arr.value(self.offset) as i128);
        }
        if let Some(arr) = any.downcast_ref::<Int8Array>() {
            return Some(arr.value(self.offset) as i128);
        }
        // Timestamps are stored as i64 internally (micros, millis, seconds, nanos).
        if let Some(arr) = any.downcast_ref::<TimestampMicrosecondArray>() {
            return Some(arr.value(self.offset) as i128);
        }
        if let Some(arr) = any.downcast_ref::<TimestampMillisecondArray>() {
            return Some(arr.value(self.offset) as i128);
        }
        if let Some(arr) = any.downcast_ref::<TimestampNanosecondArray>() {
            return Some(arr.value(self.offset) as i128);
        }
        if let Some(arr) = any.downcast_ref::<TimestampSecondArray>() {
            return Some(arr.value(self.offset) as i128);
        }
        if let Some(arr) = any.downcast_ref::<Date32Array>() {
            return Some(arr.value(self.offset) as i128);
        }
        if let Some(arr) = any.downcast_ref::<Date64Array>() {
            return Some(arr.value(self.offset) as i128);
        }
        // Decimal128: use raw i128 value for ordering (same precision/scale within a column).
        if let Some(arr) = any.downcast_ref::<Decimal128Array>() {
            return Some(arr.value(self.offset));
        }
        None
    }
}

// ---------------------------------------------------------------------------
// LoserTree
// ---------------------------------------------------------------------------

/// A LoserTree (tournament tree) for k-way merge.
///
/// Layout follows DataFusion's `SortPreservingMergeStream`:
/// - `nodes[0]` = overall winner index
/// - `nodes[1..k]` = loser at each internal node
///
/// Reference: <https://en.wikipedia.org/wiki/K-way_merge_algorithm#Tournament_Tree>
struct LoserTree {
    /// nodes[0] = winner, nodes[1..] = losers
    nodes: Vec<usize>,
    num_streams: usize,
}

impl LoserTree {
    fn new(num_streams: usize) -> Self {
        Self {
            nodes: vec![usize::MAX; num_streams],
            num_streams,
        }
    }

    fn winner(&self) -> usize {
        self.nodes[0]
    }

    /// Leaf node index for a given stream index.
    fn leaf_index(&self, stream_idx: usize) -> usize {
        (self.num_streams + stream_idx) / 2
    }

    fn parent_index(node_idx: usize) -> usize {
        node_idx / 2
    }

    /// Build the tree from scratch given a comparison function.
    /// `is_gt(a, b)` returns true if stream `a` > stream `b`.
    fn init(&mut self, is_gt: impl Fn(usize, usize) -> bool) {
        self.nodes.fill(usize::MAX);
        for i in 0..self.num_streams {
            let mut winner = i;
            let mut cmp_node = self.leaf_index(i);
            while cmp_node != 0 && self.nodes[cmp_node] != usize::MAX {
                let challenger = self.nodes[cmp_node];
                if is_gt(winner, challenger) {
                    self.nodes[cmp_node] = winner;
                    winner = challenger;
                }
                cmp_node = Self::parent_index(cmp_node);
            }
            self.nodes[cmp_node] = winner;
        }
    }

    /// Update the tree after the winner has been consumed/advanced.
    fn update(&mut self, is_gt: impl Fn(usize, usize) -> bool) {
        let mut winner = self.nodes[0];
        let mut cmp_node = self.leaf_index(winner);
        while cmp_node != 0 {
            let challenger = self.nodes[cmp_node];
            if is_gt(winner, challenger) {
                self.nodes[cmp_node] = winner;
                winner = challenger;
            }
            cmp_node = Self::parent_index(cmp_node);
        }
        self.nodes[0] = winner;
    }
}

// ---------------------------------------------------------------------------
// SortMergeReader
// ---------------------------------------------------------------------------

/// Configuration for building a [`SortMergeReader`].
pub(crate) struct SortMergeReaderBuilder {
    streams: Vec<ArrowRecordBatchStream>,
    /// Full schema of the input streams (key + seq + value_kind + value columns).
    input_schema: SchemaRef,
    /// Indices of primary key columns in input_schema.
    key_indices: Vec<usize>,
    /// Index of _SEQUENCE_NUMBER column in input_schema.
    seq_index: usize,
    /// Index of _VALUE_KIND column in input_schema.
    value_kind_index: usize,
    /// Indices of user-defined sequence field columns in input_schema (if configured).
    user_sequence_indices: Vec<usize>,
    /// Indices of user value columns in input_schema (output columns).
    value_indices: Vec<usize>,
    /// Output schema (key + value columns, no system columns).
    output_schema: SchemaRef,
    merge_function: Box<dyn MergeFunction>,
    batch_size: usize,
    /// Per-stream metadata aligned by index with `streams`. Empty (default
    /// fallback applied) for engines that don't need it.
    stream_metas: Vec<StreamMeta>,
    /// Soft cap on `batch_buffer.len()`. When the buffer reaches this
    /// length the loop forces an early flush — bounds peak memory at the
    /// cost of (sometimes) smaller output batches. `None` disables the
    /// cap (default behaviour).
    soft_cap: Option<usize>,
    /// Optional `AtomicUsize` whose `fetch_max` is bumped after every
    /// `batch_buffer.push`. Tests use it to assert peak buffer length stays
    /// bounded; production callers leave it `None`.
    peak_observer: Option<Arc<AtomicUsize>>,
}

impl SortMergeReaderBuilder {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        streams: Vec<ArrowRecordBatchStream>,
        input_schema: SchemaRef,
        key_indices: Vec<usize>,
        seq_index: usize,
        value_kind_index: usize,
        user_sequence_indices: Vec<usize>,
        value_indices: Vec<usize>,
        output_schema: SchemaRef,
        merge_function: Box<dyn MergeFunction>,
    ) -> Self {
        Self {
            streams,
            input_schema,
            key_indices,
            seq_index,
            value_kind_index,
            user_sequence_indices,
            value_indices,
            output_schema,
            merge_function,
            batch_size: 1024,
            stream_metas: Vec::new(),
            soft_cap: None,
            peak_observer: None,
        }
    }

    pub(crate) fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Set the sort-merge `batch_buffer` soft cap. `None` (default)
    /// disables the cap. When set, the loop forces an early flush as
    /// soon as the buffer reaches `cap` entries — useful for bounding
    /// memory on pathological wide-K splits where the regular
    /// `read.batch-size` flush would not fire often enough.
    pub(crate) fn with_soft_cap(mut self, cap: Option<usize>) -> Self {
        self.soft_cap = cap;
        self
    }

    /// Attach an observer that records the peak `batch_buffer` length over
    /// the merge run. Test-only: production callers leave it unset.
    #[cfg(test)]
    pub(crate) fn with_peak_observer(mut self, observer: Arc<AtomicUsize>) -> Self {
        self.peak_observer = Some(observer);
        self
    }

    /// Override per-stream metadata. Length must equal `streams.len()`.
    /// When unset, every stream gets [`StreamMeta::default()`].
    pub(crate) fn with_stream_metas(mut self, metas: Vec<StreamMeta>) -> Self {
        self.stream_metas = metas;
        self
    }

    /// Build the sort-merge stream.
    pub(crate) fn build(self) -> crate::Result<ArrowRecordBatchStream> {
        let sort_fields: Vec<SortField> = self
            .key_indices
            .iter()
            .map(|&idx| SortField::new(self.input_schema.field(idx).data_type().clone()))
            .collect();

        let row_converter = RowConverter::new(sort_fields).map_err(|e| Error::UnexpectedError {
            message: format!("Failed to create RowConverter: {e}"),
            source: Some(Box::new(e)),
        })?;

        let stream_metas = if self.stream_metas.is_empty() {
            vec![StreamMeta::default(); self.streams.len()]
        } else if self.stream_metas.len() == self.streams.len() {
            self.stream_metas
        } else {
            return Err(Error::UnexpectedError {
                message: format!(
                    "stream_metas length {} does not match streams length {}",
                    self.stream_metas.len(),
                    self.streams.len()
                ),
                source: None,
            });
        };

        sort_merge_stream(
            self.streams,
            stream_metas,
            row_converter,
            self.key_indices,
            self.seq_index,
            self.value_kind_index,
            self.user_sequence_indices,
            self.value_indices,
            self.output_schema,
            self.merge_function,
            self.batch_size,
            self.soft_cap,
            self.peak_observer,
        )
    }
}

/// Convert a RecordBatch's key columns into arrow-row `Rows`.
fn convert_batch_keys(
    batch: &RecordBatch,
    key_indices: &[usize],
    converter: &mut RowConverter,
) -> crate::Result<Rows> {
    let key_columns: Vec<ArrayRef> = key_indices
        .iter()
        .map(|&idx| batch.column(idx).clone())
        .collect();
    converter
        .convert_columns(&key_columns)
        .map_err(|e| Error::UnexpectedError {
            message: format!("Failed to convert key columns to Rows: {e}"),
            source: Some(Box::new(e)),
        })
}

/// Compare two cursors by their current key. `None` cursors are treated as
/// greater than any value (exhausted streams sink to the bottom).
fn compare_cursors(cursors: &[Option<SortMergeCursor>], a: usize, b: usize) -> Ordering {
    match (&cursors[a], &cursors[b]) {
        (None, None) => Ordering::Equal,
        (None, _) => Ordering::Greater,
        (_, None) => Ordering::Less,
        (Some(ca), Some(cb)) => ca.current_row().cmp(&cb.current_row()),
    }
}

/// The main sort-merge stream implementation.
///
/// Uses an interleave-based output strategy (like DataFusion's BatchBuilder):
/// instead of slicing individual rows and concatenating, we record
/// `(batch_idx, row_idx)` indices and use `arrow_select::interleave` to
/// gather all output rows in one pass per column.
#[allow(clippy::too_many_arguments)]
fn sort_merge_stream(
    mut streams: Vec<ArrowRecordBatchStream>,
    stream_metas: Vec<StreamMeta>,
    mut row_converter: RowConverter,
    key_indices: Vec<usize>,
    seq_index: usize,
    value_kind_index: usize,
    user_sequence_indices: Vec<usize>,
    value_indices: Vec<usize>,
    output_schema: SchemaRef,
    merge_function: Box<dyn MergeFunction>,
    batch_size: usize,
    soft_cap: Option<usize>,
    peak_observer: Option<Arc<AtomicUsize>>,
) -> crate::Result<ArrowRecordBatchStream> {
    let num_streams = streams.len();
    if num_streams == 0 {
        return Ok(futures::stream::empty().boxed());
    }

    // Output column indices for source batches: key columns + value columns
    // (skip system columns like _SEQUENCE_NUMBER).
    let source_output_col_indices: Vec<usize> = key_indices
        .iter()
        .chain(value_indices.iter())
        .copied()
        .collect();

    Ok(try_stream! {
        // Initialize cursors: read first non-empty batch from each stream.
        // Loop to skip empty batches (e.g. from predicate filtering).
        let mut cursors: Vec<Option<SortMergeCursor>> = Vec::with_capacity(num_streams);
        for stream in &mut streams {
            let mut found = false;
            while let Some(batch_result) = stream.next().await {
                let batch = batch_result?;
                if batch.num_rows() > 0 {
                    let rows = convert_batch_keys(&batch, &key_indices, &mut row_converter)?;
                    cursors.push(Some(SortMergeCursor { batch, rows, offset: 0 }));
                    found = true;
                    break;
                }
            }
            if !found {
                cursors.push(None);
            }
        }

        // Build loser tree.
        let mut tree = LoserTree::new(num_streams);
        tree.init(|a, b| compare_cursors(&cursors, a, b).then_with(|| a.cmp(&b)).is_gt());

        // Batch buffer: stores RecordBatches referenced by output indices.
        // Each cursor's current batch gets an entry; when a cursor advances
        // to a new batch, the old one stays in the buffer until the output
        // batch is flushed.
        let mut batch_buffer: Vec<BufferedBatch> = Vec::new();
        // Map from stream_idx -> current batch_buffer index.
        let mut stream_batch_idx: Vec<Option<usize>> = vec![None; num_streams];

        // Register initial batches.
        for (i, cursor) in cursors.iter().enumerate() {
            if let Some(c) = cursor {
                let idx = batch_buffer.len();
                batch_buffer.push(BufferedBatch::Source(c.batch.clone()));
                stream_batch_idx[i] = Some(idx);
            }
        }

        // Output indices: (batch_buffer_idx, row_idx) for interleave.
        let mut output_indices: Vec<(usize, usize)> = Vec::with_capacity(batch_size);

        // Materialized accumulator state (P0-2 fix). Producers
        // (`PartialUpdate`, `Aggregate`) emit single-row RecordBatches via
        // `MergeResult::MaterializedRow`; pushing one `BufferedBatch::Materialized`
        // per row would let `batch_buffer` grow with every same-PK group
        // until flush, accumulating ~`batch_size` 1-row RecordBatch headers.
        // Instead we keep a single rolling `Materialized` slot in
        // `batch_buffer` (placeholder until flush) and append the incoming
        // 1-row batches into `pending_materialized`. At flush time we
        // `concat_batches` once and overwrite the slot, so the
        // `interleave`-based output gathers the correct rows by
        // `(slot_idx, row_within_mat)` indices recorded as we went.
        let mut pending_materialized: Vec<RecordBatch> = Vec::new();
        let mut materialized_slot: Option<usize> = None;

        // Helper macro for observing peak buffer length after every push.
        // Using a macro keeps the conditional cheap and inline at every
        // push site without threading the observer through more functions.
        macro_rules! observe_peak {
            ($buf:expr) => {
                if let Some(obs) = peak_observer.as_ref() {
                    obs.fetch_max($buf.len(), AtomicOrdering::Relaxed);
                }
            };
        }
        // Bump for the initial source registrations done above.
        observe_peak!(batch_buffer);

        // Explicit per-query memory accounting of the merge buffer (the MOR
        // memory hot spot). Recomputed after every buffer mutation; Drop
        // releases the remainder on completion / early drop / error.
        let mut mem_acct = MergeMemAccount::new();
        mem_acct.sync(&batch_buffer, &cursors);

        loop {
            let winner_idx = tree.winner();
            // Check if all streams are exhausted.
            if cursors[winner_idx].is_none() {
                break;
            }

            // Capture the winner's key for grouping same-key rows.
            let winner_key = {
                let cursor = cursors[winner_idx].as_ref().unwrap();
                cursor.current_row().owned()
            };

            // Collect all rows with the same key across all streams.
            let mut same_key_rows: Vec<MergeRow> = Vec::new();

            loop {
                let current_winner = tree.winner();
                let matches = match &cursors[current_winner] {
                    None => false,
                    Some(c) => c.current_row().cmp(&winner_key.row()) == Ordering::Equal,
                };
                if !matches {
                    break;
                }

                // Record this row.
                {
                    let cursor = cursors[current_winner].as_ref().unwrap();
                    let buf_idx = stream_batch_idx[current_winner].unwrap();
                    let meta = stream_metas[current_winner];
                    same_key_rows.push(MergeRow {
                        batch_idx: buf_idx,
                        row_idx: cursor.offset,
                        snapshot_id: meta.snapshot_id,
                        sequence_number: cursor.sequence_number(seq_index),
                        value_kind: cursor.value_kind(value_kind_index),
                        user_sequences: user_sequence_indices.iter().map(|&idx| cursor.user_sequence(idx)).collect(),
                        merge_mode: meta.merge_mode,
                    });
                }

                // Advance the cursor.
                {
                    let cursor = cursors[current_winner].as_mut().unwrap();
                    cursor.advance();
                    if cursor.is_finished() {
                        // Try to get next non-empty batch from this stream.
                        // Loop to skip empty batches.
                        cursors[current_winner] = None;
                        while let Some(batch_result) = streams[current_winner].next().await {
                            let batch = batch_result?;
                            if batch.num_rows() > 0 {
                                let rows = convert_batch_keys(&batch, &key_indices, &mut row_converter)?;
                                let buf_idx = batch_buffer.len();
                                batch_buffer.push(BufferedBatch::Source(batch.clone()));
                                observe_peak!(batch_buffer);
                                mem_acct.sync(&batch_buffer, &cursors);
                                stream_batch_idx[current_winner] = Some(buf_idx);
                                cursors[current_winner] = Some(SortMergeCursor { batch, rows, offset: 0 });
                                break;
                            }
                        }
                    }
                }

                // Update loser tree after advancing.
                tree.update(|a, b| compare_cursors(&cursors, a, b).then_with(|| a.cmp(&b)).is_gt());
            }

            match merge_function.merge(
                &same_key_rows,
                &batch_buffer,
                &source_output_col_indices,
                &output_schema,
            )? {
                MergeResult::SourceRow { batch_idx, row_idx } => {
                    output_indices.push((batch_idx, row_idx));
                }
                MergeResult::MaterializedRow(batch) => {
                    if batch.num_rows() != 1 {
                        Err(Error::UnexpectedError {
                            message: format!(
                                "Materialized merge result must contain exactly one row, got {}",
                                batch.num_rows()
                            ),
                            source: None,
                        })?;
                    }
                    if batch.schema().as_ref() != output_schema.as_ref() {
                        Err(Error::UnexpectedError {
                            message: "Materialized merge result schema does not match merge output schema".to_string(),
                            source: None,
                        })?;
                    }
                    // Single rolling slot. The placeholder is empty (0 rows
                    // / output_schema); it gets overwritten with the
                    // concat'd accumulator at flush time, before
                    // `build_output_interleave` reads it.
                    let slot = if let Some(slot) = materialized_slot {
                        slot
                    } else {
                        let slot = batch_buffer.len();
                        batch_buffer.push(BufferedBatch::Materialized(
                            RecordBatch::new_empty(output_schema.clone()),
                        ));
                        observe_peak!(batch_buffer);
                        materialized_slot = Some(slot);
                        slot
                    };
                    let row_within_mat = pending_materialized.len();
                    pending_materialized.push(batch);
                    output_indices.push((slot, row_within_mat));
                }
                MergeResult::Omit => {}
            }

            // Yield a batch when we've accumulated enough rows OR when the
            // soft cap on `batch_buffer.len()` triggers. The cap-driven
            // flush reuses the same path so accumulator state is finalized
            // identically.
            let cap_hit = matches!(soft_cap, Some(cap) if batch_buffer.len() >= cap);
            if output_indices.len() >= batch_size || cap_hit {
                // Finalize accumulator into the rolling slot before
                // `build_output_interleave` reads it. The slot was pushed
                // as an empty placeholder; overwrite with the concat of
                // every 1-row batch that arrived this window.
                if let Some(slot) = materialized_slot {
                    if !pending_materialized.is_empty() {
                        let merged = arrow_select::concat::concat_batches(
                            &output_schema,
                            pending_materialized.iter(),
                        )
                        .map_err(|e| Error::UnexpectedError {
                            message: format!(
                                "Failed to concat materialized rows: {e}"
                            ),
                            source: Some(Box::new(e)),
                        })?;
                        batch_buffer[slot] = BufferedBatch::Materialized(merged);
                    }
                }
                let batch = build_output_interleave(
                    &output_schema,
                    &batch_buffer,
                    &source_output_col_indices,
                    &output_indices,
                )?;
                output_indices.clear();
                // Reset accumulator state — the slot is now unreferenced
                // by `output_indices` and will be dropped by
                // `compact_batch_buffer` below.
                pending_materialized.clear();
                materialized_slot = None;
                // Compact batch buffer after the pending output rows have been
                // materialized. Source batches still referenced by cursors stay
                // alive; materialized batches can be dropped here because they
                // are referenced only by the flushed output_indices above.
                compact_batch_buffer(
                    &mut batch_buffer,
                    &mut stream_batch_idx,
                    &cursors,
                );
                // Buffer shrank (dead batches dropped) and/or a materialized
                // batch was written above; reconcile the accounted bytes.
                mem_acct.sync(&batch_buffer, &cursors);
                yield batch;
            }
        }

        // Yield remaining rows.
        if !output_indices.is_empty() {
            // Finalize accumulator for the trailing window — same as the
            // mid-loop flush, but no compaction needed afterwards.
            if let Some(slot) = materialized_slot {
                if !pending_materialized.is_empty() {
                    let merged = arrow_select::concat::concat_batches(
                        &output_schema,
                        pending_materialized.iter(),
                    )
                    .map_err(|e| Error::UnexpectedError {
                        message: format!(
                            "Failed to concat materialized rows: {e}"
                        ),
                        source: Some(Box::new(e)),
                    })?;
                    batch_buffer[slot] = BufferedBatch::Materialized(merged);
                }
            }
            let batch = build_output_interleave(
                &output_schema,
                &batch_buffer,
                &source_output_col_indices,
                &output_indices,
            )?;
            yield batch;
        }
    }
    .boxed())
}

/// Build an output RecordBatch using `interleave` to gather rows from the
/// batch buffer in one pass per column.
fn build_output_interleave(
    schema: &SchemaRef,
    batch_buffer: &[BufferedBatch],
    source_output_col_indices: &[usize],
    indices: &[(usize, usize)],
) -> crate::Result<RecordBatch> {
    let columns: Vec<ArrayRef> = (0..schema.fields().len())
        .map(|output_col_idx| {
            let arrays: Vec<&dyn arrow_array::Array> = batch_buffer
                .iter()
                .map(|batch| batch.column_for_output(output_col_idx, source_output_col_indices))
                .collect();
            interleave(&arrays, indices).map_err(|e| Error::UnexpectedError {
                message: format!("Failed to interleave output column {output_col_idx}: {e}"),
                source: Some(Box::new(e)),
            })
        })
        .collect::<crate::Result<Vec<_>>>()?;

    RecordBatch::try_new(schema.clone(), columns).map_err(|e| Error::UnexpectedError {
        message: format!("Failed to build interleaved RecordBatch: {e}"),
        source: Some(Box::new(e)),
    })
}

/// Explicit per-query memory accounting for the merge's resident memory.
///
/// The sort-merge simultaneously holds, per split (N = files in the split):
///   - the accumulating `batch_buffer` of input/materialized `RecordBatch`es
///     (arrow array bytes), and
///   - one arrow-row key encoding (`Rows`) per live cursor.
/// Together these are the bulk of merge-on-read memory. The cdylib no longer
/// uses a global allocator (IO frees happen on untagged blocking-pool threads
/// and drift), so we account them explicitly here: this runs on the tagged
/// scanner thread that drives the merge, so `mem_tag::account` charges the right
/// query. When no tag is installed (tests, non-FFI callers) `account` is a
/// no-op.
///
/// Note we do NOT separately count `cursor.batch`: for a Source batch it is the
/// same Arc already counted in `batch_buffer`, so counting it again would double
/// count. Only the cursor's `rows` (an independent key buffer) is added.
///
/// We recompute the total after every mutation and charge only the delta;
/// `Drop` returns whatever is still charged, covering normal completion, early
/// stream drop, and errors.
struct MergeMemAccount {
    charged: i64,
}

impl MergeMemAccount {
    fn new() -> Self {
        MergeMemAccount { charged: 0 }
    }

    fn sync(&mut self, buffer: &[BufferedBatch], cursors: &[Option<SortMergeCursor>]) {
        let buffer_bytes: i64 = buffer.iter().map(|b| b.memory_size() as i64).sum();
        let rows_bytes: i64 = cursors
            .iter()
            .filter_map(|c| c.as_ref())
            .map(|c| c.rows.size() as i64)
            .sum();
        let total = buffer_bytes + rows_bytes;
        let delta = total - self.charged;
        if delta != 0 {
            crate::mem_tag::account(delta);
            self.charged = total;
        }
    }
}

impl Drop for MergeMemAccount {
    fn drop(&mut self) {
        if self.charged != 0 {
            crate::mem_tag::account(-self.charged);
        }
    }
}

/// Compact the batch buffer by removing batches no longer referenced by any
/// cursor, and updating indices accordingly.
fn compact_batch_buffer(
    batch_buffer: &mut Vec<BufferedBatch>,
    stream_batch_idx: &mut [Option<usize>],
    cursors: &[Option<SortMergeCursor>],
) {
    // Collect which buffer indices are still alive (referenced by a cursor).
    let mut alive: Vec<bool> = vec![false; batch_buffer.len()];
    for (i, cursor) in cursors.iter().enumerate() {
        if cursor.is_some() {
            if let Some(idx) = stream_batch_idx[i] {
                alive[idx] = true;
            }
        }
    }

    // Build old->new index mapping.
    let mut new_indices: Vec<Option<usize>> = vec![None; batch_buffer.len()];
    let mut new_buffer: Vec<BufferedBatch> = Vec::new();
    for (old_idx, is_alive) in alive.iter().enumerate() {
        if *is_alive {
            new_indices[old_idx] = Some(new_buffer.len());
            new_buffer.push(batch_buffer[old_idx].clone());
        }
    }

    *batch_buffer = new_buffer;

    // Remap stream_batch_idx.
    for (i, cursor) in cursors.iter().enumerate() {
        if cursor.is_some() {
            if let Some(old_idx) = stream_batch_idx[i] {
                stream_batch_idx[i] = new_indices[old_idx];
            }
        } else {
            stream_batch_idx[i] = None;
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Array, Int32Array, Int64Array, Int8Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use futures::TryStreamExt;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn make_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("pk", DataType::Int32, false),
            Field::new("_SEQUENCE_NUMBER", DataType::Int64, false),
            Field::new("_VALUE_KIND", DataType::Int8, false),
            Field::new("value", DataType::Utf8, true),
        ]))
    }

    fn make_output_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("pk", DataType::Int32, false),
            Field::new("value", DataType::Utf8, true),
        ]))
    }

    fn make_batch(
        schema: &SchemaRef,
        pks: Vec<i32>,
        seqs: Vec<i64>,
        values: Vec<Option<&str>>,
    ) -> RecordBatch {
        let len = pks.len();
        make_batch_with_kind(schema, pks, seqs, vec![0i8; len], values)
    }

    fn make_batch_with_kind(
        schema: &SchemaRef,
        pks: Vec<i32>,
        seqs: Vec<i64>,
        kinds: Vec<i8>,
        values: Vec<Option<&str>>,
    ) -> RecordBatch {
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(pks)),
                Arc::new(Int64Array::from(seqs)),
                Arc::new(Int8Array::from(kinds)),
                Arc::new(StringArray::from(values)),
            ],
        )
        .unwrap()
    }

    fn stream_from_batches(batches: Vec<RecordBatch>) -> ArrowRecordBatchStream {
        futures::stream::iter(batches.into_iter().map(Ok)).boxed()
    }

    struct MaterializingMergeFunction;

    impl MergeFunction for MaterializingMergeFunction {
        fn merge(
            &self,
            rows: &[MergeRow],
            batch_buffer: &[BufferedBatch],
            source_output_col_indices: &[usize],
            output_schema: &SchemaRef,
        ) -> crate::Result<MergeResult> {
            let first = rows.first().expect("merge called with empty rows");
            let source_batch = first.source_batch(batch_buffer)?;
            let pk = source_batch
                .column(source_output_col_indices[0])
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("pk column must be Int32")
                .value(first.row_idx);

            let batch = RecordBatch::try_new(
                output_schema.clone(),
                vec![
                    Arc::new(Int32Array::from(vec![pk])) as ArrayRef,
                    Arc::new(StringArray::from(vec![Some("merged")])) as ArrayRef,
                ],
            )
            .map_err(|e| Error::UnexpectedError {
                message: format!("Failed to build materialized merge batch: {e}"),
                source: Some(Box::new(e)),
            })?;

            Ok(MergeResult::MaterializedRow(batch))
        }
    }

    #[tokio::test]
    async fn test_loser_tree_basic() {
        // 3 streams, verify init produces correct winner
        let schema = make_schema();
        let s0 = stream_from_batches(vec![make_batch(
            &schema,
            vec![1, 3],
            vec![1, 1],
            vec![Some("a"), Some("c")],
        )]);
        let s1 = stream_from_batches(vec![make_batch(
            &schema,
            vec![2, 4],
            vec![1, 1],
            vec![Some("b"), Some("d")],
        )]);
        let s2 = stream_from_batches(vec![make_batch(&schema, vec![5], vec![1], vec![Some("e")])]);

        let output_schema = make_output_schema();
        let result = SortMergeReaderBuilder::new(
            vec![s0, s1, s2],
            schema,
            vec![0], // key: pk
            1,       // seq index
            2,       // value_kind index
            vec![],  // no user sequence fields
            vec![3], // value index
            output_schema,
            Box::new(DeduplicateMergeFunction),
        )
        .build()
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

        let pks: Vec<i32> = result
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
            })
            .collect();
        assert_eq!(pks, vec![1, 2, 3, 4, 5]);
    }

    #[tokio::test]
    async fn test_deduplicate_merge() {
        // Two streams with overlapping keys, different sequence numbers
        let schema = make_schema();
        let s0 = stream_from_batches(vec![make_batch(
            &schema,
            vec![1, 2, 3],
            vec![1, 1, 1],
            vec![Some("old_a"), Some("old_b"), Some("old_c")],
        )]);
        let s1 = stream_from_batches(vec![make_batch(
            &schema,
            vec![1, 2, 4],
            vec![2, 2, 2],
            vec![Some("new_a"), Some("new_b"), Some("new_d")],
        )]);

        let output_schema = make_output_schema();
        let result = SortMergeReaderBuilder::new(
            vec![s0, s1],
            schema,
            vec![0],
            1,
            2,       // value_kind index
            vec![],  // no user sequence fields
            vec![3], // value index
            output_schema,
            Box::new(DeduplicateMergeFunction),
        )
        .build()
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

        let pks: Vec<i32> = result
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
            })
            .collect();
        let values: Vec<String> = result
            .iter()
            .flat_map(|b| {
                let arr = b.column(1).as_any().downcast_ref::<StringArray>().unwrap();
                (0..arr.len())
                    .map(|i| arr.value(i).to_string())
                    .collect::<Vec<_>>()
            })
            .collect();

        assert_eq!(pks, vec![1, 2, 3, 4]);
        // key 1,2: newer seq wins; key 3: only in s0; key 4: only in s1
        assert_eq!(values, vec!["new_a", "new_b", "old_c", "new_d"]);
    }

    #[tokio::test]
    async fn test_empty_streams() {
        let schema = make_schema();
        let output_schema = make_output_schema();
        let result = SortMergeReaderBuilder::new(
            vec![],
            schema,
            vec![0],
            1,
            2,       // value_kind index
            vec![],  // no user sequence fields
            vec![3], // value index
            output_schema,
            Box::new(DeduplicateMergeFunction),
        )
        .build()
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn test_single_stream_no_duplicates() {
        let schema = make_schema();
        let s0 = stream_from_batches(vec![make_batch(
            &schema,
            vec![1, 2, 3],
            vec![1, 1, 1],
            vec![Some("a"), Some("b"), Some("c")],
        )]);

        let output_schema = make_output_schema();
        let result = SortMergeReaderBuilder::new(
            vec![s0],
            schema,
            vec![0],
            1,
            2,
            vec![],
            vec![3],
            output_schema,
            Box::new(DeduplicateMergeFunction),
        )
        .build()
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

        let pks: Vec<i32> = result
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
            })
            .collect();
        assert_eq!(pks, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn test_multi_batch_per_stream() {
        let schema = make_schema();
        // Stream 0: two batches
        let s0 = stream_from_batches(vec![
            make_batch(&schema, vec![1, 3], vec![1, 1], vec![Some("a"), Some("c")]),
            make_batch(&schema, vec![5, 7], vec![1, 1], vec![Some("e"), Some("g")]),
        ]);
        // Stream 1: two batches
        let s1 = stream_from_batches(vec![
            make_batch(&schema, vec![2, 4], vec![1, 1], vec![Some("b"), Some("d")]),
            make_batch(&schema, vec![6], vec![1], vec![Some("f")]),
        ]);

        let output_schema = make_output_schema();
        let result = SortMergeReaderBuilder::new(
            vec![s0, s1],
            schema,
            vec![0],
            1,
            2,
            vec![],
            vec![3],
            output_schema,
            Box::new(DeduplicateMergeFunction),
        )
        .build()
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

        let pks: Vec<i32> = result
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
            })
            .collect();
        assert_eq!(pks, vec![1, 2, 3, 4, 5, 6, 7]);
    }

    #[tokio::test]
    async fn test_batch_size_boundary() {
        let schema = make_schema();
        let s0 = stream_from_batches(vec![make_batch(
            &schema,
            vec![1, 2, 3, 4, 5],
            vec![1, 1, 1, 1, 1],
            vec![Some("a"), Some("b"), Some("c"), Some("d"), Some("e")],
        )]);

        let output_schema = make_output_schema();
        let result = SortMergeReaderBuilder::new(
            vec![s0],
            schema,
            vec![0],
            1,
            2,
            vec![],
            vec![3],
            output_schema,
            Box::new(DeduplicateMergeFunction),
        )
        .with_batch_size(2)
        .build()
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

        // Should produce 3 batches: [2, 2, 1] rows
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].num_rows(), 2);
        assert_eq!(result[1].num_rows(), 2);
        assert_eq!(result[2].num_rows(), 1);
    }

    #[tokio::test]
    async fn test_multi_sequence_fields() {
        // Schema: pk, _SEQUENCE_NUMBER, _VALUE_KIND, seq1, seq2, value
        let schema = Arc::new(Schema::new(vec![
            Field::new("pk", DataType::Int32, false),
            Field::new("_SEQUENCE_NUMBER", DataType::Int64, false),
            Field::new("_VALUE_KIND", DataType::Int8, false),
            Field::new("seq1", DataType::Int64, false),
            Field::new("seq2", DataType::Int64, false),
            Field::new("value", DataType::Utf8, true),
        ]));
        let output_schema = Arc::new(Schema::new(vec![
            Field::new("pk", DataType::Int32, false),
            Field::new("value", DataType::Utf8, true),
        ]));

        // pk=1: s0 has (seq1=10, seq2=1), s1 has (seq1=10, seq2=2) → s1 wins (second field higher)
        // pk=2: s0 has (seq1=20, seq2=1), s1 has (seq1=10, seq2=99) → s0 wins (first field higher)
        let s0 = stream_from_batches(vec![RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(Int64Array::from(vec![1, 1])),
                Arc::new(Int8Array::from(vec![0, 0])),
                Arc::new(Int64Array::from(vec![10, 20])),
                Arc::new(Int64Array::from(vec![1, 1])),
                Arc::new(StringArray::from(vec!["old_a", "winner_b"])),
            ],
        )
        .unwrap()]);
        let s1 = stream_from_batches(vec![RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(Int64Array::from(vec![2, 2])),
                Arc::new(Int8Array::from(vec![0, 0])),
                Arc::new(Int64Array::from(vec![10, 10])),
                Arc::new(Int64Array::from(vec![2, 99])),
                Arc::new(StringArray::from(vec!["winner_a", "loser_b"])),
            ],
        )
        .unwrap()]);

        let result = SortMergeReaderBuilder::new(
            vec![s0, s1],
            schema,
            vec![0],    // key: pk
            1,          // seq index
            2,          // value_kind index
            vec![3, 4], // user sequence fields: seq1, seq2
            vec![5],    // value index
            output_schema,
            Box::new(DeduplicateMergeFunction),
        )
        .build()
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

        let values: Vec<String> = result
            .iter()
            .flat_map(|b| {
                let arr = b.column(1).as_any().downcast_ref::<StringArray>().unwrap();
                (0..arr.len())
                    .map(|i| arr.value(i).to_string())
                    .collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(values, vec!["winner_a", "winner_b"]);
    }

    #[tokio::test]
    async fn test_delete_row_filtered() {
        let schema = make_schema();
        // Stream 0: pk=1 INSERT (seq=1), pk=2 INSERT (seq=1)
        let s0 = stream_from_batches(vec![make_batch_with_kind(
            &schema,
            vec![1, 2],
            vec![1, 1],
            vec![0, 0],
            vec![Some("a"), Some("b")],
        )]);
        // Stream 1: pk=1 DELETE (seq=2) — should win and be filtered out
        let s1 = stream_from_batches(vec![make_batch_with_kind(
            &schema,
            vec![1],
            vec![2],
            vec![3], // DELETE
            vec![Some("a")],
        )]);

        let output_schema = make_output_schema();
        let result = SortMergeReaderBuilder::new(
            vec![s0, s1],
            schema,
            vec![0],
            1,
            2,
            vec![],
            vec![3],
            output_schema,
            Box::new(DeduplicateMergeFunction),
        )
        .build()
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

        let pks: Vec<i32> = result
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
            })
            .collect();
        // pk=1 deleted, only pk=2 remains
        assert_eq!(pks, vec![2]);
    }

    #[tokio::test]
    async fn test_single_stream_duplicate_keys() {
        let schema = make_schema();
        // Single stream with duplicate pk=1 (seq 1 and 2), unique pk=2
        let s0 = stream_from_batches(vec![make_batch(
            &schema,
            vec![1, 1, 2],
            vec![1, 2, 1],
            vec![Some("old"), Some("new"), Some("only")],
        )]);

        let output_schema = make_output_schema();
        let result = SortMergeReaderBuilder::new(
            vec![s0],
            schema,
            vec![0],
            1,
            2,
            vec![],
            vec![3],
            output_schema,
            Box::new(DeduplicateMergeFunction),
        )
        .build()
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

        let pks: Vec<i32> = result
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
            })
            .collect();
        let values: Vec<String> = result
            .iter()
            .flat_map(|b| {
                let arr = b.column(1).as_any().downcast_ref::<StringArray>().unwrap();
                (0..arr.len())
                    .map(|i| arr.value(i).to_string())
                    .collect::<Vec<_>>()
            })
            .collect();

        assert_eq!(pks, vec![1, 2]);
        assert_eq!(values, vec!["new", "only"]);
    }

    #[tokio::test]
    async fn test_single_row_per_stream() {
        let schema = make_schema();
        let s0 = stream_from_batches(vec![make_batch(&schema, vec![3], vec![1], vec![Some("c")])]);
        let s1 = stream_from_batches(vec![make_batch(&schema, vec![1], vec![1], vec![Some("a")])]);
        let s2 = stream_from_batches(vec![make_batch(&schema, vec![2], vec![1], vec![Some("b")])]);

        let output_schema = make_output_schema();
        let result = SortMergeReaderBuilder::new(
            vec![s0, s1, s2],
            schema,
            vec![0],
            1,
            2,
            vec![],
            vec![3],
            output_schema,
            Box::new(DeduplicateMergeFunction),
        )
        .build()
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

        let pks: Vec<i32> = result
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
            })
            .collect();
        let values: Vec<String> = result
            .iter()
            .flat_map(|b| {
                let arr = b.column(1).as_any().downcast_ref::<StringArray>().unwrap();
                (0..arr.len())
                    .map(|i| arr.value(i).to_string())
                    .collect::<Vec<_>>()
            })
            .collect();

        assert_eq!(pks, vec![1, 2, 3]);
        assert_eq!(values, vec!["a", "b", "c"]);
    }

    /// Helper to create an empty batch with the test schema.
    fn make_empty_batch(schema: &SchemaRef) -> RecordBatch {
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(Vec::<i32>::new())),
                Arc::new(Int64Array::from(Vec::<i64>::new())),
                Arc::new(Int8Array::from(Vec::<i8>::new())),
                Arc::new(StringArray::from(Vec::<Option<&str>>::new())),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_empty_batches_skipped() {
        // Regression: empty batches (e.g. from predicate filtering) must be
        // skipped, not treated as stream exhaustion.
        let schema = make_schema();

        // Stream 0: empty batch at start, then real data
        let s0 = stream_from_batches(vec![
            make_empty_batch(&schema),
            make_batch(&schema, vec![1, 3], vec![1, 1], vec![Some("a"), Some("c")]),
        ]);
        // Stream 1: data, empty batch in the middle, then more data
        let s1 = stream_from_batches(vec![
            make_batch(&schema, vec![2], vec![1], vec![Some("b")]),
            make_empty_batch(&schema),
            make_empty_batch(&schema),
            make_batch(&schema, vec![4], vec![1], vec![Some("d")]),
        ]);

        let output_schema = make_output_schema();
        let result = SortMergeReaderBuilder::new(
            vec![s0, s1],
            schema,
            vec![0],
            1,
            2,
            vec![],
            vec![3],
            output_schema,
            Box::new(DeduplicateMergeFunction),
        )
        .build()
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

        let pks: Vec<i32> = result
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
            })
            .collect();
        let values: Vec<String> = result
            .iter()
            .flat_map(|b| {
                let arr = b.column(1).as_any().downcast_ref::<StringArray>().unwrap();
                (0..arr.len())
                    .map(|i| arr.value(i).to_string())
                    .collect::<Vec<_>>()
            })
            .collect();

        assert_eq!(pks, vec![1, 2, 3, 4]);
        assert_eq!(values, vec!["a", "b", "c", "d"]);
    }

    #[tokio::test]
    async fn test_materialized_merge_result_path() {
        let schema = make_schema();
        let s0 = stream_from_batches(vec![make_batch(
            &schema,
            vec![1, 2],
            vec![1, 1],
            vec![Some("old_a"), Some("old_b")],
        )]);
        let s1 = stream_from_batches(vec![make_batch(
            &schema,
            vec![1, 3],
            vec![2, 1],
            vec![Some("new_a"), Some("c")],
        )]);

        let result = SortMergeReaderBuilder::new(
            vec![s0, s1],
            schema,
            vec![0],
            1,
            2,
            vec![],
            vec![3],
            make_output_schema(),
            Box::new(MaterializingMergeFunction),
        )
        .build()
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

        let pks: Vec<i32> = result
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
            })
            .collect();
        let values: Vec<String> = result
            .iter()
            .flat_map(|b| {
                let arr = b.column(1).as_any().downcast_ref::<StringArray>().unwrap();
                (0..arr.len())
                    .map(|i| arr.value(i).to_string())
                    .collect::<Vec<_>>()
            })
            .collect();

        assert_eq!(pks, vec![1, 2, 3]);
        assert_eq!(values, vec!["merged", "merged", "merged"]);
    }

    #[tokio::test]
    async fn test_partial_update_merge_keeps_latest_non_null_values() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("pk", DataType::Int32, false),
            Field::new("_SEQUENCE_NUMBER", DataType::Int64, false),
            Field::new("_VALUE_KIND", DataType::Int8, false),
            Field::new("v_int", DataType::Int32, true),
            Field::new("v_str", DataType::Utf8, true),
        ]));
        let output_schema = Arc::new(Schema::new(vec![
            Field::new("pk", DataType::Int32, false),
            Field::new("v_int", DataType::Int32, true),
            Field::new("v_str", DataType::Utf8, true),
        ]));

        let s0 = stream_from_batches(vec![RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(Int64Array::from(vec![1, 1])),
                Arc::new(Int8Array::from(vec![0, 0])),
                Arc::new(Int32Array::from(vec![10, 20])),
                Arc::new(StringArray::from(vec![Some("old-1"), Some("old-2")])),
            ],
        )
        .unwrap()]);
        let s1 = stream_from_batches(vec![RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int64Array::from(vec![2, 2, 1])),
                Arc::new(Int8Array::from(vec![0, 0, 0])),
                Arc::new(Int32Array::from(vec![None, Some(200), Some(30)])),
                Arc::new(StringArray::from(vec![Some("new-1"), None, None])),
            ],
        )
        .unwrap()]);

        let result = SortMergeReaderBuilder::new(
            vec![s0, s1],
            schema,
            vec![0],
            1,
            2,
            vec![],
            vec![3, 4],
            output_schema,
            Box::new(PartialUpdateMergeFunction::new(&HashMap::new(), "test_table").unwrap()),
        )
        .build()
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

        let mut rows: Vec<(i32, Option<i32>, Option<String>)> = Vec::new();
        for batch in &result {
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let ints = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let strs = batch
                .column(2)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            for i in 0..batch.num_rows() {
                rows.push((
                    ids.value(i),
                    if ints.is_null(i) {
                        None
                    } else {
                        Some(ints.value(i))
                    },
                    if strs.is_null(i) {
                        None
                    } else {
                        Some(strs.value(i).to_string())
                    },
                ));
            }
        }
        rows.sort_by_key(|row| row.0);

        assert_eq!(
            rows,
            vec![
                (1, Some(10), Some("new-1".to_string())),
                (2, Some(200), Some("old-2".to_string())),
                (3, Some(30), None),
            ]
        );
    }

    #[tokio::test]
    async fn test_partial_update_merge_rejects_delete_like_rows() {
        let schema = make_schema();
        let output_schema = make_output_schema();
        let s0 = stream_from_batches(vec![make_batch_with_kind(
            &schema,
            vec![1],
            vec![1],
            vec![0],
            vec![Some("old")],
        )]);
        let s1 = stream_from_batches(vec![make_batch_with_kind(
            &schema,
            vec![1],
            vec![2],
            vec![3],
            vec![Some("delete")],
        )]);

        let err = SortMergeReaderBuilder::new(
            vec![s0, s1],
            schema,
            vec![0],
            1,
            2,
            vec![],
            vec![3],
            output_schema,
            Box::new(PartialUpdateMergeFunction::new(&HashMap::new(), "test_table").unwrap()),
        )
        .build()
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap_err();

        assert!(matches!(
            err,
            Error::Unsupported { message }
            if message.contains("partial-update basic mode does not support DELETE or UPDATE_BEFORE")
        ));
    }

    #[test]
    fn test_partial_update_merge_function_new_rejects_unsupported_options() {
        let options = HashMap::from([
            ("merge-engine".to_string(), "partial-update".to_string()),
            (
                "fields.price.aggregate-function".to_string(),
                "last_non_null".to_string(),
            ),
        ]);

        let err = PartialUpdateMergeFunction::new(&options, "default.t").unwrap_err();

        assert!(matches!(
            err,
            Error::Unsupported { message }
            if message.contains("fields.price.aggregate-function")
        ));
    }

    #[tokio::test]
    async fn test_deduplicate_user_seq_tiebreak_falls_back_to_system_seq() {
        // When user sequence fields are equal, the deduplicate merge must fall
        // back to the system `_SEQUENCE_NUMBER` to pick the winner. This covers
        // the `.then_with(sequence_number)` branch in `compare_sequence_order`.
        let schema = Arc::new(Schema::new(vec![
            Field::new("pk", DataType::Int32, false),
            Field::new("_SEQUENCE_NUMBER", DataType::Int64, false),
            Field::new("_VALUE_KIND", DataType::Int8, false),
            Field::new("seq1", DataType::Int64, false),
            Field::new("value", DataType::Utf8, true),
        ]));
        let output_schema = make_output_schema();

        // pk=1: both rows share user seq1=100, but s1 has a higher _SEQUENCE_NUMBER
        // (9 > 5) → s1 must win via the system-sequence tie-breaker.
        let s0 = stream_from_batches(vec![RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(Int64Array::from(vec![5])),
                Arc::new(Int8Array::from(vec![0])),
                Arc::new(Int64Array::from(vec![100])),
                Arc::new(StringArray::from(vec![Some("low_sys_seq")])),
            ],
        )
        .unwrap()]);
        let s1 = stream_from_batches(vec![RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(Int64Array::from(vec![9])),
                Arc::new(Int8Array::from(vec![0])),
                Arc::new(Int64Array::from(vec![100])),
                Arc::new(StringArray::from(vec![Some("high_sys_seq")])),
            ],
        )
        .unwrap()]);

        let result = SortMergeReaderBuilder::new(
            vec![s0, s1],
            schema,
            vec![0], // key: pk
            1,       // seq index
            2,       // value_kind index
            vec![3], // user sequence field: seq1
            vec![4], // value index
            output_schema,
            Box::new(DeduplicateMergeFunction),
        )
        .build()
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

        let values: Vec<String> = result
            .iter()
            .flat_map(|b| {
                let arr = b.column(1).as_any().downcast_ref::<StringArray>().unwrap();
                (0..arr.len())
                    .map(|i| arr.value(i).to_string())
                    .collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(values, vec!["high_sys_seq"]);
    }

    #[tokio::test]
    async fn test_partial_update_non_nullable_missing_column_errors() {
        // A non-nullable output column that no input row ever fills (all NULL)
        // must surface a DataInvalid error rather than emitting a NULL.
        let schema = Arc::new(Schema::new(vec![
            Field::new("pk", DataType::Int32, false),
            Field::new("_SEQUENCE_NUMBER", DataType::Int64, false),
            Field::new("_VALUE_KIND", DataType::Int8, false),
            Field::new("v_req", DataType::Int32, true),
        ]));
        // Output declares v_req as NON-nullable.
        let output_schema = Arc::new(Schema::new(vec![
            Field::new("pk", DataType::Int32, false),
            Field::new("v_req", DataType::Int32, false),
        ]));

        let s0 = stream_from_batches(vec![RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(Int8Array::from(vec![0])),
                Arc::new(Int32Array::from(vec![None])),
            ],
        )
        .unwrap()]);

        let err = SortMergeReaderBuilder::new(
            vec![s0],
            schema,
            vec![0],
            1,
            2,
            vec![],
            vec![3], // v_req
            output_schema,
            Box::new(PartialUpdateMergeFunction::new(&HashMap::new(), "test_table").unwrap()),
        )
        .build()
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap_err();

        assert!(matches!(
            err,
            Error::DataInvalid { message, .. }
            if message.contains("non-nullable field 'v_req'")
        ));
    }

    // ---------- AggregateMergeFunction ----------

    use crate::spec::{DataType as PaimonDataType, IntType, VarCharType};

    fn aggregation_output_fields() -> Vec<DataField> {
        vec![
            DataField::new(0, "pk".into(), PaimonDataType::Int(IntType::new())),
            DataField::new(1, "amount".into(), PaimonDataType::Int(IntType::new())),
            DataField::new(
                2,
                "tag".into(),
                // listagg requires unbounded VARCHAR (STRING), matching Java.
                PaimonDataType::VarChar(VarCharType::string_type()),
            ),
        ]
    }

    fn aggregation_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("pk", DataType::Int32, false),
            Field::new("_SEQUENCE_NUMBER", DataType::Int64, false),
            Field::new("_VALUE_KIND", DataType::Int8, false),
            Field::new("amount", DataType::Int32, true),
            Field::new("tag", DataType::Utf8, true),
        ]))
    }

    fn aggregation_output_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("pk", DataType::Int32, false),
            Field::new("amount", DataType::Int32, true),
            Field::new("tag", DataType::Utf8, true),
        ]))
    }

    fn agg_options(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        let mut opts = HashMap::from([("merge-engine".to_string(), "aggregation".to_string())]);
        for (k, v) in pairs {
            opts.insert((*k).to_string(), (*v).to_string());
        }
        opts
    }

    fn build_agg_function(options: HashMap<String, String>) -> Box<dyn MergeFunction> {
        Box::new(
            AggregateMergeFunction::new(
                &options,
                "test_table",
                &aggregation_output_fields(),
                &["pk".to_string()],
                &[],
            )
            .unwrap(),
        )
    }

    #[tokio::test]
    async fn test_aggregate_merge_sum_and_listagg() {
        let schema = aggregation_schema();
        let output_schema = aggregation_output_schema();
        let s0 = stream_from_batches(vec![RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(Int64Array::from(vec![1, 1])),
                Arc::new(Int8Array::from(vec![0, 0])),
                Arc::new(Int32Array::from(vec![Some(10), Some(20)])),
                Arc::new(StringArray::from(vec![Some("a"), Some("x")])),
            ],
        )
        .unwrap()]);
        let s1 = stream_from_batches(vec![RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int64Array::from(vec![2, 2, 1])),
                Arc::new(Int8Array::from(vec![0, 0, 0])),
                Arc::new(Int32Array::from(vec![Some(5), Some(7), Some(99)])),
                Arc::new(StringArray::from(vec![Some("b"), None, Some("solo")])),
            ],
        )
        .unwrap()]);

        let options = agg_options(&[
            ("fields.amount.aggregate-function", "sum"),
            ("fields.tag.aggregate-function", "listagg"),
            ("fields.tag.list-agg-delimiter", "|"),
        ]);

        let batches = SortMergeReaderBuilder::new(
            vec![s0, s1],
            schema,
            vec![0],
            1,
            2,
            vec![],
            vec![3, 4],
            output_schema,
            build_agg_function(options),
        )
        .build()
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

        let mut rows: Vec<(i32, Option<i32>, Option<String>)> = Vec::new();
        for batch in &batches {
            let pks = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let amounts = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let tags = batch
                .column(2)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            for i in 0..batch.num_rows() {
                rows.push((
                    pks.value(i),
                    if amounts.is_null(i) {
                        None
                    } else {
                        Some(amounts.value(i))
                    },
                    if tags.is_null(i) {
                        None
                    } else {
                        Some(tags.value(i).to_string())
                    },
                ));
            }
        }
        rows.sort_by_key(|row| row.0);

        assert_eq!(
            rows,
            vec![
                (1, Some(15), Some("a|b".to_string())),
                (2, Some(27), Some("x".to_string())),
                (3, Some(99), Some("solo".to_string())),
            ]
        );
    }

    #[tokio::test]
    async fn test_aggregate_merge_rejects_delete_rows() {
        let schema = aggregation_schema();
        let output_schema = aggregation_output_schema();
        let s0 = stream_from_batches(vec![RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(Int8Array::from(vec![0])),
                Arc::new(Int32Array::from(vec![Some(10)])),
                Arc::new(StringArray::from(vec![Some("a")])),
            ],
        )
        .unwrap()]);
        // Row kind 3 = DELETE
        let s1 = stream_from_batches(vec![RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(Int64Array::from(vec![2])),
                Arc::new(Int8Array::from(vec![3])),
                Arc::new(Int32Array::from(vec![Some(99)])),
                Arc::new(StringArray::from(vec![Some("b")])),
            ],
        )
        .unwrap()]);

        let options = agg_options(&[
            ("fields.amount.aggregate-function", "sum"),
            ("fields.tag.aggregate-function", "last_value"),
        ]);

        let err = SortMergeReaderBuilder::new(
            vec![s0, s1],
            schema,
            vec![0],
            1,
            2,
            vec![],
            vec![3, 4],
            output_schema,
            build_agg_function(options),
        )
        .build()
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap_err();

        assert!(matches!(
            err,
            Error::Unsupported { ref message }
            if message.contains("aggregation basic mode does not support DELETE")
        ));
    }

    #[test]
    fn test_aggregate_merge_function_falls_back_to_last_non_null_value() {
        // With no per-field nor default aggregate-function configured, each
        // value column should fall back to last_non_null_value (matching Java
        // AggregateMergeFunction#getAggFuncName).
        let options = HashMap::from([("merge-engine".to_string(), "aggregation".to_string())]);
        let mf = AggregateMergeFunction::new(
            &options,
            "test_table",
            &aggregation_output_fields(),
            &["pk".to_string()],
            &[],
        )
        .expect("aggregation engine must accept tables without explicit per-field config");
        // Smoke check: construction succeeds.
        let _ = mf;
    }

    #[test]
    fn test_aggregate_merge_function_sequence_field_forced_last_value() {
        // 'tag' is the sequence field; even though user configured listagg,
        // it should be forced to last_value (so the latest tag survives).
        let schema = aggregation_schema();
        let output_schema = aggregation_output_schema();
        let options = agg_options(&[
            ("fields.amount.aggregate-function", "sum"),
            ("fields.tag.aggregate-function", "listagg"),
            ("sequence.field", "tag"),
        ]);
        let mf = AggregateMergeFunction::new(
            &options,
            "test_table",
            &aggregation_output_fields(),
            &["pk".to_string()],
            &["tag".to_string()],
        )
        .unwrap();

        let s0 = stream_from_batches(vec![RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 1])),
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(Int8Array::from(vec![0, 0])),
                Arc::new(Int32Array::from(vec![Some(3), Some(4)])),
                Arc::new(StringArray::from(vec![Some("first"), Some("second")])),
            ],
        )
        .unwrap()]);

        // Use the merge function via builder.
        let batches = futures::executor::block_on(async {
            SortMergeReaderBuilder::new(
                vec![s0],
                schema,
                vec![0],
                1,
                2,
                vec![],
                vec![3, 4],
                output_schema,
                Box::new(mf),
            )
            .build()
            .unwrap()
            .try_collect::<Vec<RecordBatch>>()
            .await
            .unwrap()
        });

        let batch = &batches[0];
        let tags = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let amounts = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(tags.value(0), "second"); // last_value, not listagg.
        assert_eq!(amounts.value(0), 7); // sum of 3 + 4.
    }

    #[test]
    fn test_aggregate_merge_function_default_function_applies_when_per_field_absent() {
        let options = agg_options(&[("fields.default-aggregate-function", "last_non_null_value")]);
        let mf = AggregateMergeFunction::new(
            &options,
            "test_table",
            &aggregation_output_fields(),
            &["pk".to_string()],
            &[],
        )
        .unwrap();
        // Construction must succeed: amount and tag fall back to the default.
        // Tag is VarChar — last_non_null_value supports any type.
        let _ = mf;
    }

    #[test]
    fn test_aggregate_merge_function_rejects_unsupported_options() {
        let options = agg_options(&[("fields.amount.ignore-retract", "true")]);
        let err = AggregateMergeFunction::new(
            &options,
            "test_table",
            &aggregation_output_fields(),
            &["pk".to_string()],
            &[],
        )
        .unwrap_err();
        assert!(
            matches!(err, Error::Unsupported { message } if message.contains("ignore-retract"))
        );
    }

    /// Regression test for the Materialized accumulator (P0-2 fix). With
    /// the old "push one BufferedBatch::Materialized per merged row"
    /// approach, `batch_buffer` would grow with every same-PK group —
    /// roughly N entries for N output rows before a flush. The fix keeps
    /// a single rolling Materialized slot per flush window plus a
    /// side-channel pending vec.
    ///
    /// Setup: 50 same-PK pairs across two streams (PKs 0..50, each
    /// appearing once per stream), `batch_size=200` so no flush fires
    /// during the run. Without the fix, peak `batch_buffer.len()` after
    /// the first source-advance would reach ~52 (2 source slots + 50
    /// materialized 1-row entries). With the fix, peak ≤ 5 (2 source
    /// slots + 1 materialized slot + slack).
    #[tokio::test]
    async fn test_materialized_accumulator_single_buffer_slot() {
        // Schema: (pk INT, _SEQUENCE_NUMBER INT64, _VALUE_KIND INT8, value STRING).
        let schema = make_schema();

        // Build batches with PKs 0..50 in each stream.
        let pks: Vec<i32> = (0..50).collect();
        let seq_a: Vec<i64> = (0..50).map(|i| i as i64).collect();
        let seq_b: Vec<i64> = (50..100).map(|i| i as i64).collect();
        let values: Vec<Option<&str>> = pks.iter().map(|_| Some("v")).collect();

        let s0 = stream_from_batches(vec![make_batch(
            &schema,
            pks.clone(),
            seq_a,
            values.clone(),
        )]);
        let s1 = stream_from_batches(vec![make_batch(&schema, pks.clone(), seq_b, values)]);

        let observer = Arc::new(AtomicUsize::new(0));
        let result = SortMergeReaderBuilder::new(
            vec![s0, s1],
            schema,
            vec![0],
            1,
            2,
            vec![],
            vec![3],
            make_output_schema(),
            Box::new(MaterializingMergeFunction),
        )
        .with_batch_size(200) // > 50, so no mid-run flush
        .with_peak_observer(observer.clone())
        .build()
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

        // 50 unique PKs, each materialized once.
        let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 50);

        let peak = observer.load(AtomicOrdering::Relaxed);
        assert!(
            peak <= 5,
            "peak batch_buffer.len() must be small with accumulator (got {peak}); \
             without the fix it grows ~linear in merged rows"
        );
    }

    /// `with_soft_cap` forces an early flush as soon as `batch_buffer.len()`
    /// reaches the cap, even when `output_indices.len() < batch_size`. The
    /// effect: the same total row count is delivered across more (smaller)
    /// output batches.
    ///
    /// Setup: two streams with 30 disjoint PKs each (no merge collapse),
    /// `batch_size=10000` so the row-count flush never fires. With
    /// `soft_cap=4`, the buffer can hold at most 4 entries before a flush;
    /// since the test pushes one source slot per advanced batch and a few
    /// from the initial registration, output is split into multiple chunks.
    #[tokio::test]
    async fn test_soft_cap_forces_flush() {
        let schema = make_schema();

        // 30 unique PKs per stream, written one batch at a time so each
        // advance grows `batch_buffer` and exposes the cap.
        let mut s0_batches = Vec::new();
        let mut s1_batches = Vec::new();
        for i in 0..30 {
            // Stream 0: even-indexed PKs (i.e. 0, 2, 4, ...)
            s0_batches.push(make_batch(
                &schema,
                vec![i * 2],
                vec![i as i64],
                vec![Some("a")],
            ));
            // Stream 1: odd-indexed PKs (1, 3, 5, ...)
            s1_batches.push(make_batch(
                &schema,
                vec![i * 2 + 1],
                vec![i as i64 + 1000],
                vec![Some("b")],
            ));
        }

        let observer = Arc::new(AtomicUsize::new(0));
        let result = SortMergeReaderBuilder::new(
            vec![
                stream_from_batches(s0_batches),
                stream_from_batches(s1_batches),
            ],
            schema,
            vec![0],
            1,
            2,
            vec![],
            vec![3],
            make_output_schema(),
            Box::new(DeduplicateMergeFunction),
        )
        .with_batch_size(10000) // huge — only the soft cap should drive flushes
        .with_soft_cap(Some(4))
        .with_peak_observer(observer.clone())
        .build()
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

        // All 60 unique rows are present.
        let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 60);

        // Multiple output batches — the cap forced flushes the row-count
        // flush would not have triggered.
        assert!(
            result.len() > 1,
            "soft cap=4 with batch_size=10000 must produce multiple output batches, \
             got {} batch(es)",
            result.len()
        );

        // Peak buffer length stays bounded by the cap (with a small slack
        // for the push-then-flush ordering).
        let peak = observer.load(AtomicOrdering::Relaxed);
        assert!(
            peak <= 6,
            "soft cap=4 must keep peak buffer ≤ ~6 (got {peak})"
        );
    }
}
