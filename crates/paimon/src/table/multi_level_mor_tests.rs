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

//! White-box correctness tests for multi-level merge-on-read of primary key
//! tables.
//!
//! The Rust writer only ever emits level-0 files (no compaction), so a plain
//! write+commit loop can only produce multiple L0 runs — never genuine
//! L1/L2 state. These tests force true multi-level layouts by writing real KV
//! parquet files via [`TableWrite`], then mutating the resulting
//! [`DataFileMeta::level`] inside the [`CommitMessage`] before committing
//! (Mechanism B in the test plan). The subsequent read still goes through the
//! real `scan -> split -> route -> sort-merge` pipeline; only the level
//! metadata is hand-placed.
//!
//! Each scenario is constructed so that an incorrect (skipped) merge would
//! produce an observably wrong result — e.g. a stale duplicate row, a
//! resurrected deleted key, or an un-merged partial row.

use super::*;
use crate::catalog::Identifier;
use crate::io::{FileIO, FileIOBuilder};
use crate::spec::{Datum, PredicateBuilder, Schema, TableSchema};
use arrow_array::{Array, Int32Array, Int8Array, RecordBatch};
use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use futures::TryStreamExt;
use std::sync::Arc;

fn test_file_io() -> FileIO {
    FileIOBuilder::new("memory").build().unwrap()
}

async fn setup_dirs(file_io: &FileIO, table_path: &str) {
    file_io
        .mkdirs(&format!("{table_path}/snapshot/"))
        .await
        .unwrap();
    file_io
        .mkdirs(&format!("{table_path}/manifest/"))
        .await
        .unwrap();
}

/// Deduplicate (default merge engine) PK table, single bucket.
fn dedup_table(file_io: &FileIO, table_path: &str, extra_options: &[(&str, &str)]) -> Table {
    use crate::spec::{DataType, IntType};
    let mut builder = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("value", DataType::Int(IntType::new()))
        .primary_key(["id"])
        .option("bucket", "1");
    for (k, v) in extra_options {
        builder = builder.option(*k, *v);
    }
    Table::new(
        file_io.clone(),
        Identifier::new("default", "ml_dedup"),
        table_path.to_string(),
        TableSchema::new(0, &builder.build().unwrap()),
        None,
    )
}

/// Partial-update PK table with two nullable value columns, single bucket.
fn partial_update_table(file_io: &FileIO, table_path: &str) -> Table {
    use crate::spec::{DataType, IntType};
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("a", DataType::Int(IntType::new()))
        .column("b", DataType::Int(IntType::new()))
        .primary_key(["id"])
        .option("bucket", "1")
        .option("merge-engine", "partial-update")
        .build()
        .unwrap();
    Table::new(
        file_io.clone(),
        Identifier::new("default", "ml_partial_update"),
        table_path.to_string(),
        TableSchema::new(0, &schema),
        None,
    )
}

fn id_value_batch(ids: Vec<i32>, values: Vec<i32>) -> RecordBatch {
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Int32, false),
        ArrowField::new("value", ArrowDataType::Int32, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(ids)),
            Arc::new(Int32Array::from(values)),
        ],
    )
    .unwrap()
}

/// `id`/`value` batch carrying an explicit `_VALUE_KIND` column
/// (0 = INSERT, 1 = UPDATE_BEFORE, 2 = UPDATE_AFTER, 3 = DELETE).
fn id_value_kind_batch(ids: Vec<i32>, values: Vec<i32>, kinds: Vec<i8>) -> RecordBatch {
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Int32, false),
        ArrowField::new("value", ArrowDataType::Int32, false),
        ArrowField::new(
            crate::spec::VALUE_KIND_FIELD_NAME,
            ArrowDataType::Int8,
            false,
        ),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(ids)),
            Arc::new(Int32Array::from(values)),
            Arc::new(Int8Array::from(kinds)),
        ],
    )
    .unwrap()
}

/// `id`/`a`/`b` partial-update batch where `a`/`b` are nullable.
fn id_a_b_batch(ids: Vec<i32>, a: Vec<Option<i32>>, b: Vec<Option<i32>>) -> RecordBatch {
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Int32, false),
        ArrowField::new("a", ArrowDataType::Int32, true),
        ArrowField::new("b", ArrowDataType::Int32, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(ids)),
            Arc::new(Int32Array::from(a)),
            Arc::new(Int32Array::from(b)),
        ],
    )
    .unwrap()
}

/// Write one batch and commit it, forcing every produced data file to the given
/// LSM `level`. Returns nothing; the data lives in the table's manifest.
async fn commit_batch_at_level(table: &Table, batch: RecordBatch, level: i32) {
    let mut tw = TableWrite::new(table, "test-user".to_string()).unwrap();
    tw.write_arrow_batch(&batch).await.unwrap();
    let mut msgs = tw.prepare_commit().await.unwrap();
    for m in &mut msgs {
        for f in &mut m.new_files {
            f.level = level;
        }
    }
    TableCommit::new(table.clone(), "test-user".to_string())
        .commit(msgs)
        .await
        .unwrap();
}

/// Read the full table and return `(id, value)` rows sorted by id.
async fn read_id_value(table: &Table) -> Vec<(i32, i32)> {
    let rb = table.new_read_builder();
    let plan = rb.new_scan().plan().await.unwrap();
    let read = rb.new_read().unwrap();
    let batches: Vec<RecordBatch> =
        TryStreamExt::try_collect(read.to_arrow(plan.splits()).unwrap())
            .await
            .unwrap();
    rows_id_value(&batches)
}

fn rows_id_value(batches: &[RecordBatch]) -> Vec<(i32, i32)> {
    let mut rows: Vec<(i32, i32)> = batches
        .iter()
        .flat_map(|b| {
            let ids = b.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
            let values = b.column(1).as_any().downcast_ref::<Int32Array>().unwrap();
            (0..b.num_rows()).map(|i| (ids.value(i), values.value(i)))
        })
        .collect();
    rows.sort_unstable();
    rows
}

/// Collect the distinct set of LSM levels present across all files of all
/// splits in a plan, sorted ascending.
fn plan_levels(splits: &[crate::table::DataSplit]) -> Vec<i32> {
    let mut levels: Vec<i32> = splits
        .iter()
        .flat_map(|s| s.data_files().iter().map(|f| f.level))
        .collect();
    levels.sort_unstable();
    levels.dedup();
    levels
}

// ---------------------------------------------------------------------------
// Deduplicate: cross-level version selection
// ---------------------------------------------------------------------------

/// An older row sits at L1, a newer row for the same key sits at L0. The
/// sort-merge reader must pick the higher-sequence (L0) version. A skipped
/// merge would surface both rows.
#[tokio::test]
async fn test_dedup_cross_level_overlap_newer_wins() {
    let file_io = test_file_io();
    let table_path = "memory:/ml_dedup_cross_level_overlap";
    setup_dirs(&file_io, table_path).await;
    let table = dedup_table(&file_io, table_path, &[]);

    // Older value at L1 (lower sequence), newer value at L0 (higher sequence).
    commit_batch_at_level(&table, id_value_batch(vec![1], vec![10]), 1).await;
    commit_batch_at_level(&table, id_value_batch(vec![1], vec![99]), 0).await;

    // The plan must place the two overlapping files in a single split spanning
    // both levels — proving this is genuine multi-level, not multi-L0.
    let plan = table.new_read_builder().new_scan().plan().await.unwrap();
    assert_eq!(plan.splits().len(), 1, "overlapping files share one split");
    assert_eq!(plan.splits()[0].data_files().len(), 2);
    assert_eq!(
        plan_levels(plan.splits()),
        vec![0, 1],
        "split spans L0 and L1"
    );

    assert_eq!(read_id_value(&table).await, vec![(1, 99)]);
}

/// A DELETE at the newer level (L0) must suppress an older INSERT at L1.
/// If the tombstone were not merged across levels, the stale L1 row would leak.
#[tokio::test]
async fn test_dedup_cross_level_tombstone_suppresses_older_row() {
    let file_io = test_file_io();
    let table_path = "memory:/ml_dedup_cross_level_tombstone";
    setup_dirs(&file_io, table_path).await;
    let table = dedup_table(&file_io, table_path, &[]);

    // L1: INSERT id=1; L0: DELETE id=1 (newer).
    commit_batch_at_level(&table, id_value_kind_batch(vec![1], vec![10], vec![0]), 1).await;
    commit_batch_at_level(&table, id_value_kind_batch(vec![1], vec![0], vec![3]), 0).await;

    assert_eq!(
        read_id_value(&table).await,
        vec![],
        "tombstone wins, row gone"
    );
}

/// A newer INSERT at L0 must resurrect a key that was deleted at the older L1.
#[tokio::test]
async fn test_dedup_cross_level_resurrect_after_delete() {
    let file_io = test_file_io();
    let table_path = "memory:/ml_dedup_cross_level_resurrect";
    setup_dirs(&file_io, table_path).await;
    let table = dedup_table(&file_io, table_path, &[]);

    // L1: DELETE id=1 (older); L0: INSERT id=1 value=20 (newer).
    commit_batch_at_level(&table, id_value_kind_batch(vec![1], vec![0], vec![3]), 1).await;
    commit_batch_at_level(&table, id_value_kind_batch(vec![1], vec![20], vec![0]), 0).await;

    assert_eq!(read_id_value(&table).await, vec![(1, 20)]);
}

/// Three levels (L2/L1/L0) all overlapping on the same key: the winner must be
/// the highest-sequence row regardless of how deep the older versions sit.
#[tokio::test]
async fn test_dedup_three_levels_overlap_newest_wins() {
    let file_io = test_file_io();
    let table_path = "memory:/ml_dedup_three_levels";
    setup_dirs(&file_io, table_path).await;
    let table = dedup_table(&file_io, table_path, &[]);

    commit_batch_at_level(&table, id_value_batch(vec![1], vec![10]), 2).await;
    commit_batch_at_level(&table, id_value_batch(vec![1], vec![20]), 1).await;
    commit_batch_at_level(&table, id_value_batch(vec![1], vec![30]), 0).await;

    let plan = table.new_read_builder().new_scan().plan().await.unwrap();
    assert_eq!(plan.splits().len(), 1);
    assert_eq!(plan_levels(plan.splits()), vec![0, 1, 2]);

    assert_eq!(read_id_value(&table).await, vec![(1, 30)]);
}

/// Disjoint keys across levels with no L0 file take the raw (non-merging) read
/// path. The result must still be complete and correct.
#[tokio::test]
async fn test_dedup_cross_level_disjoint_raw_path() {
    let file_io = test_file_io();
    let table_path = "memory:/ml_dedup_cross_level_disjoint";
    setup_dirs(&file_io, table_path).await;
    let table = dedup_table(&file_io, table_path, &[]);

    // Disjoint key ranges, both compacted (no L0): {1,2,3} at L1, {4,5,6} at L2.
    commit_batch_at_level(&table, id_value_batch(vec![1, 2, 3], vec![10, 20, 30]), 1).await;
    commit_batch_at_level(&table, id_value_batch(vec![4, 5, 6], vec![40, 50, 60]), 2).await;

    let plan = table.new_read_builder().new_scan().plan().await.unwrap();
    assert_eq!(plan_levels(plan.splits()), vec![1, 2], "no L0 present");

    assert_eq!(
        read_id_value(&table).await,
        vec![(1, 10), (2, 20), (3, 30), (4, 40), (5, 50), (6, 60)]
    );
}

/// One read that mixes a merging split (overlapping L0+L1) with a raw split
/// (single disjoint L1 file). Exercises the `select_all([kv, raw])` seam:
/// the merging split must still pick the newest version while the raw split
/// passes its row through untouched.
#[tokio::test]
async fn test_mixed_routing_kv_and_raw_in_one_read() {
    let file_io = test_file_io();
    let table_path = "memory:/ml_mixed_routing";
    setup_dirs(&file_io, table_path).await;
    // Tiny split target forces disjoint sections into separate splits.
    let table = dedup_table(
        &file_io,
        table_path,
        &[
            ("source.split.target-size", "1b"),
            ("source.split.open-file-cost", "1b"),
        ],
    );

    // Section A (key=1): overlapping L1 (old) + L0 (new) -> must merge.
    commit_batch_at_level(&table, id_value_batch(vec![1], vec![10]), 1).await;
    commit_batch_at_level(&table, id_value_batch(vec![1], vec![99]), 0).await;
    // Section B (key=100): single disjoint L1 file, no L0 -> raw read.
    commit_batch_at_level(&table, id_value_batch(vec![100], vec![500]), 1).await;

    let plan = table.new_read_builder().new_scan().plan().await.unwrap();
    assert_eq!(
        plan.splits().len(),
        2,
        "overlapping section and disjoint section form two splits"
    );

    assert_eq!(read_id_value(&table).await, vec![(1, 99), (100, 500)]);
}

// ---------------------------------------------------------------------------
// Deduplicate: projection / filter pushdown stacked on cross-level merge
// ---------------------------------------------------------------------------

/// Projection must not bypass the cross-level merge: selecting only the key
/// still has to collapse the two overlapping versions into one row.
#[tokio::test]
async fn test_dedup_cross_level_with_projection() {
    let file_io = test_file_io();
    let table_path = "memory:/ml_dedup_projection";
    setup_dirs(&file_io, table_path).await;
    let table = dedup_table(&file_io, table_path, &[]);

    commit_batch_at_level(&table, id_value_batch(vec![1], vec![10]), 1).await;
    commit_batch_at_level(&table, id_value_batch(vec![1], vec![99]), 0).await;

    let mut rb = table.new_read_builder();
    rb.with_projection(&["id"]);
    let plan = rb.new_scan().plan().await.unwrap();
    let read = rb.new_read().unwrap();
    let batches: Vec<RecordBatch> =
        TryStreamExt::try_collect(read.to_arrow(plan.splits()).unwrap())
            .await
            .unwrap();

    let ids: Vec<i32> = batches
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
    assert_eq!(ids, vec![1], "projection still yields a single merged key");
}

/// Filter-pushdown semantics on the cross-level merge path.
///
/// The sort-merge reader intentionally keeps only primary-key predicates and
/// drops non-PK predicates before merging (a non-PK predicate applied
/// pre-merge could discard a version needed to compute the correct winner — see
/// `kv_file_reader.rs`). Non-PK filters are therefore residual: the caller
/// (e.g. DataFusion) applies them for exact semantics. The critical invariant
/// this test pins down is that whatever the engine returns, it is always the
/// correctly merged winner (value=99) and never the stale L1 version
/// (value=10).
#[tokio::test]
async fn test_dedup_cross_level_filter_semantics() {
    let file_io = test_file_io();
    let table_path = "memory:/ml_dedup_filter";
    setup_dirs(&file_io, table_path).await;
    let table = dedup_table(&file_io, table_path, &[]);

    commit_batch_at_level(&table, id_value_batch(vec![1], vec![10]), 1).await;
    commit_batch_at_level(&table, id_value_batch(vec![1], vec![99]), 0).await;

    let builder = PredicateBuilder::new(table.schema().fields());

    let read_with = |pred| {
        let table = &table;
        async move {
            let mut rb = table.new_read_builder();
            rb.with_filter(pred);
            let plan = rb.new_scan().plan().await.unwrap();
            let read = rb.new_read().unwrap();
            let batches: Vec<RecordBatch> =
                TryStreamExt::try_collect(read.to_arrow(plan.splits()).unwrap())
                    .await
                    .unwrap();
            rows_id_value(&batches)
        }
    };

    // Non-PK predicates are residual on the merge path: both the matching and
    // the non-matching value filter return the merged winner — never the stale
    // L1 value (10). Exact value filtering is the caller's responsibility.
    let gt = read_with(builder.greater_than("value", Datum::Int(50)).unwrap()).await;
    assert_eq!(gt, vec![(1, 99)]);
    let lt = read_with(builder.less_than("value", Datum::Int(50)).unwrap()).await;
    assert_eq!(
        lt,
        vec![(1, 99)],
        "non-PK filter is residual; stale 10 never leaks"
    );

    // PK predicates ARE honored and must not break the cross-level merge:
    // id=1 keeps the merged winner, id=2 prunes everything.
    let pk_hit = read_with(builder.equal("id", Datum::Int(1)).unwrap()).await;
    assert_eq!(pk_hit, vec![(1, 99)]);
    let pk_miss = read_with(builder.equal("id", Datum::Int(2)).unwrap()).await;
    assert_eq!(pk_miss, vec![], "PK filter prunes the absent key cleanly");
}

// ---------------------------------------------------------------------------
// Partial-update: cross-level field merge
// ---------------------------------------------------------------------------

/// Partial-update across levels: column `a` is only present in the older L1
/// row, column `b` only in the newer L0 row. The merged result must combine
/// both non-null fields into a single row.
#[tokio::test]
async fn test_partial_update_cross_level_field_merge() {
    let file_io = test_file_io();
    let table_path = "memory:/ml_partial_update_cross_level";
    setup_dirs(&file_io, table_path).await;
    let table = partial_update_table(&file_io, table_path);

    // L1 (older): a=10, b=NULL. L0 (newer): a=NULL, b=20.
    commit_batch_at_level(&table, id_a_b_batch(vec![1], vec![Some(10)], vec![None]), 1).await;
    commit_batch_at_level(&table, id_a_b_batch(vec![1], vec![None], vec![Some(20)]), 0).await;

    let plan = table.new_read_builder().new_scan().plan().await.unwrap();
    assert_eq!(plan_levels(plan.splits()), vec![0, 1], "spans L0 and L1");

    let rb = table.new_read_builder();
    let plan = rb.new_scan().plan().await.unwrap();
    let read = rb.new_read().unwrap();
    let batches: Vec<RecordBatch> =
        TryStreamExt::try_collect(read.to_arrow(plan.splits()).unwrap())
            .await
            .unwrap();

    let mut rows: Vec<(i32, Option<i32>, Option<i32>)> = Vec::new();
    for b in &batches {
        let ids = b.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        let a = b.column(1).as_any().downcast_ref::<Int32Array>().unwrap();
        let bb = b.column(2).as_any().downcast_ref::<Int32Array>().unwrap();
        for i in 0..b.num_rows() {
            rows.push((
                ids.value(i),
                (!a.is_null(i)).then(|| a.value(i)),
                (!bb.is_null(i)).then(|| bb.value(i)),
            ));
        }
    }
    rows.sort_by_key(|r| r.0);
    assert_eq!(rows, vec![(1, Some(10), Some(20))]);
}
