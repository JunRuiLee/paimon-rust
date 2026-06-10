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

//! E2E integration tests for version management combined with read/write.
//!
//! Verifies, on a pure in-memory FileIO (no HDFS/S3/REST):
//!   1. multiple commits produce a linear snapshot history
//!   2. time-travel by snapshot id (`scan.version`)
//!   3. time-travel by timestamp (`scan.timestamp-millis`)
//!   4. tag creation and reading data through a tag (`scan.version=<tag>`)
//!   5. branch metadata operations (create / from-tag / rename / drop / validation)
//!   6. the branch read/write gap: no first-class branch parameter on the
//!      read/write builders; only a manual path-redirection workaround exists.

use std::collections::HashMap;
use std::time::Duration;

use arrow_array::{Int32Array, RecordBatch};
use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use futures::TryStreamExt;
use paimon::catalog::Identifier;
use paimon::io::FileIOBuilder;
use paimon::spec::{CommitKind, DataType, IntType, Schema, TableSchema};
use paimon::table::{BranchManager, SnapshotManager, Table, TagManager};
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Helpers (mirrors append_tables.rs conventions)
// ---------------------------------------------------------------------------

fn memory_file_io() -> paimon::io::FileIO {
    FileIOBuilder::new("memory").build().unwrap()
}

async fn setup_dirs(file_io: &paimon::io::FileIO, table_path: &str) {
    file_io
        .mkdirs(&format!("{table_path}/snapshot/"))
        .await
        .unwrap();
    file_io
        .mkdirs(&format!("{table_path}/manifest/"))
        .await
        .unwrap();
}

fn unpartitioned_schema() -> TableSchema {
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("value", DataType::Int(IntType::new()))
        .build()
        .unwrap();
    TableSchema::new(0, &schema)
}

fn make_table(file_io: &paimon::io::FileIO, table_path: &str, schema: TableSchema) -> Table {
    Table::new(
        file_io.clone(),
        Identifier::new("default", "test"),
        table_path.to_string(),
        schema,
        None,
    )
}

/// Persist `schema-0` to disk so schema-dependent ops (e.g. branch creation,
/// which copies the latest schema) have something to read. The normal
/// read/write path uses the in-memory schema on `Table`, so the other tests
/// don't need this.
async fn persist_schema(file_io: &paimon::io::FileIO, table_path: &str, schema: &TableSchema) {
    file_io
        .mkdirs(&format!("{table_path}/schema/"))
        .await
        .unwrap();
    let json = serde_json::to_string(schema).unwrap();
    let out = file_io
        .new_output(&format!("{table_path}/schema/schema-0"))
        .unwrap();
    out.write(bytes::Bytes::from(json)).await.unwrap();
}

fn int_batch(ids: Vec<i32>, values: Vec<i32>) -> RecordBatch {
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

/// Write one batch and commit it as a single snapshot.
async fn write_one_commit(table: &Table, batch: RecordBatch) {
    let wb = table.new_write_builder();
    let mut tw = wb.new_write().unwrap();
    tw.write_arrow_batch(&batch).await.unwrap();
    wb.new_commit()
        .commit(tw.prepare_commit().await.unwrap())
        .await
        .unwrap();
}

/// Scan + read all rows of a table, returning sorted `id` column values.
async fn read_ids(table: &Table) -> Vec<i32> {
    let rb = table.new_read_builder();
    let plan = rb.new_scan().plan().await.unwrap();
    let read = rb.new_read().unwrap();
    let batches: Vec<RecordBatch> = read
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut ids: Vec<i32> = batches
        .iter()
        .flat_map(|b| {
            let idx = b.schema().index_of("id").unwrap();
            b.column(idx)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect();
    ids.sort();
    ids
}

fn snapshot_manager(table: &Table) -> SnapshotManager {
    SnapshotManager::new(table.file_io().clone(), table.location().to_string())
}

/// Build a table with three commits: ids [1,2], [3,4], [5,6].
async fn three_commit_table(file_io: &paimon::io::FileIO, path: &str) -> Table {
    setup_dirs(file_io, path).await;
    let table = make_table(file_io, path, unpartitioned_schema());
    write_one_commit(&table, int_batch(vec![1, 2], vec![10, 20])).await;
    write_one_commit(&table, int_batch(vec![3, 4], vec![30, 40])).await;
    write_one_commit(&table, int_batch(vec![5, 6], vec![50, 60])).await;
    table
}

// ---------------------------------------------------------------------------
// Case 1: multiple commits → linear snapshot history
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_multi_commit_snapshot_history() {
    let file_io = memory_file_io();
    let path = "memory:/ver_snapshots";
    let table = three_commit_table(&file_io, path).await;

    let sm = snapshot_manager(&table);

    let ids = sm.list_all_ids().await.unwrap();
    assert_eq!(ids, vec![1, 2, 3], "expected linear snapshot ids 1,2,3");

    assert_eq!(sm.get_latest_snapshot_id().await.unwrap(), Some(3));
    assert_eq!(sm.earliest_snapshot_id().await.unwrap(), Some(1));

    let mut last_ts = 0u64;
    for id in &ids {
        let s = sm.get_snapshot(*id).await.unwrap();
        assert_eq!(s.id(), *id);
        assert_eq!(s.schema_id(), 0, "all commits use schema 0");
        assert_eq!(*s.commit_kind(), CommitKind::APPEND);
        assert!(
            s.time_millis() >= last_ts,
            "snapshot timestamps must be non-decreasing"
        );
        last_ts = s.time_millis();
    }
}

// ---------------------------------------------------------------------------
// Case 2: time-travel by snapshot id (scan.version=<id>)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_time_travel_by_snapshot_id() {
    let file_io = memory_file_io();
    let path = "memory:/ver_tt_id";
    let table = three_commit_table(&file_io, path).await;

    // Latest (no option): all six rows.
    assert_eq!(read_ids(&table).await, vec![1, 2, 3, 4, 5, 6]);

    // scan.version="1" → state after first commit only.
    let at1 = table.copy_with_options(HashMap::from([(
        "scan.version".to_string(),
        "1".to_string(),
    )]));
    assert_eq!(read_ids(&at1).await, vec![1, 2]);

    // scan.version="2" → state after first two commits.
    let at2 = table.copy_with_options(HashMap::from([(
        "scan.version".to_string(),
        "2".to_string(),
    )]));
    assert_eq!(read_ids(&at2).await, vec![1, 2, 3, 4]);

    // scan.version="3" → equals latest.
    let at3 = table.copy_with_options(HashMap::from([(
        "scan.version".to_string(),
        "3".to_string(),
    )]));
    assert_eq!(read_ids(&at3).await, vec![1, 2, 3, 4, 5, 6]);
}

// ---------------------------------------------------------------------------
// Case 3: time-travel by timestamp (scan.timestamp-millis=<ms>)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_time_travel_by_timestamp() {
    let file_io = memory_file_io();
    let path = "memory:/ver_tt_ts";
    setup_dirs(&file_io, path).await;
    let table = make_table(&file_io, path, unpartitioned_schema());

    // Space the commits out so each snapshot lands on a distinct millisecond.
    write_one_commit(&table, int_batch(vec![1, 2], vec![10, 20])).await;
    std::thread::sleep(Duration::from_millis(15));
    write_one_commit(&table, int_batch(vec![3, 4], vec![30, 40])).await;
    std::thread::sleep(Duration::from_millis(15));
    write_one_commit(&table, int_batch(vec![5, 6], vec![50, 60])).await;

    let sm = snapshot_manager(&table);
    let s1_ts = sm.get_snapshot(1).await.unwrap().time_millis();
    let s2_ts = sm.get_snapshot(2).await.unwrap().time_millis();
    let s3_ts = sm.get_snapshot(3).await.unwrap().time_millis();
    assert!(
        s1_ts < s2_ts && s2_ts < s3_ts,
        "timestamps must be distinct"
    );

    // At s1's timestamp → only the first commit.
    let at_s1 = table.copy_with_options(HashMap::from([(
        "scan.timestamp-millis".to_string(),
        s1_ts.to_string(),
    )]));
    assert_eq!(read_ids(&at_s1).await, vec![1, 2]);

    // At s2's timestamp → first two commits (latest snapshot <= ts).
    let at_s2 = table.copy_with_options(HashMap::from([(
        "scan.timestamp-millis".to_string(),
        s2_ts.to_string(),
    )]));
    assert_eq!(read_ids(&at_s2).await, vec![1, 2, 3, 4]);

    // At s3's timestamp → everything.
    let at_s3 = table.copy_with_options(HashMap::from([(
        "scan.timestamp-millis".to_string(),
        s3_ts.to_string(),
    )]));
    assert_eq!(read_ids(&at_s3).await, vec![1, 2, 3, 4, 5, 6]);

    // Before the earliest snapshot → no snapshot satisfies the predicate.
    let before = table.copy_with_options(HashMap::from([(
        "scan.timestamp-millis".to_string(),
        (s1_ts - 1).to_string(),
    )]));
    let rb = before.new_read_builder();
    assert!(
        rb.new_scan().plan().await.is_err(),
        "a timestamp before the first snapshot should fail to resolve"
    );
}

// ---------------------------------------------------------------------------
// Case 4: tag creation + reading data through a tag
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_tag_create_and_read() {
    let file_io = memory_file_io();
    let path = "memory:/ver_tag";
    let table = three_commit_table(&file_io, path).await;

    let sm = snapshot_manager(&table);
    let tm = TagManager::new(table.file_io().clone(), table.location().to_string());

    let s1 = sm.get_snapshot(1).await.unwrap();
    let s2 = sm.get_snapshot(2).await.unwrap();
    tm.create("v1", &s1).await.unwrap();
    tm.create("v2", &s2).await.unwrap();

    assert!(tm.tag_exists("v1").await.unwrap());
    let mut names = tm.list_all_names().await.unwrap();
    names.sort();
    assert_eq!(names, vec!["v1".to_string(), "v2".to_string()]);
    assert_eq!(tm.get("v1").await.unwrap().unwrap().id(), 1);

    // Read through tag "v1" → snapshot 1 state.
    let at_v1 = table.copy_with_options(HashMap::from([(
        "scan.version".to_string(),
        "v1".to_string(),
    )]));
    assert_eq!(read_ids(&at_v1).await, vec![1, 2]);

    // Read through tag "v2" → snapshot 2 state.
    let at_v2 = table.copy_with_options(HashMap::from([(
        "scan.version".to_string(),
        "v2".to_string(),
    )]));
    assert_eq!(read_ids(&at_v2).await, vec![1, 2, 3, 4]);
}

// ---------------------------------------------------------------------------
// Case 5: branch metadata operations
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_branch_metadata_ops() {
    // Branch ops rely on filesystem-style directory semantics (recursive
    // copy, whole-tree rename, delete_dir, directory existence). The in-memory
    // object backend models those poorly, so this lifecycle test runs on a real
    // local filesystem. The object-store limitation is captured separately in
    // `test_branch_rename_object_store_limitation`.
    let file_io = FileIOBuilder::new("file").build().unwrap();
    let path = format!("/tmp/paimon_ver_branch_meta_{}", std::process::id());
    let path = path.as_str();
    let _ = file_io.delete_dir(path).await; // clean any stale run
    let schema = unpartitioned_schema();
    let table = three_commit_table(&file_io, path).await;
    // create_branch copies the latest schema, which must exist on disk.
    persist_schema(&file_io, path, &schema).await;

    let bm = BranchManager::new(table.file_io().clone(), table.location().to_string());

    // Empty initially.
    assert!(bm.list_all().await.unwrap().is_empty());

    // Create a plain branch.
    bm.create_branch("dev").await.unwrap();
    assert!(bm.branch_exists("dev").await.unwrap());
    assert_eq!(bm.list_all().await.unwrap(), vec!["dev".to_string()]);

    // Invalid names are rejected.
    assert!(bm.create_branch("main").await.is_err(), "'main' reserved");
    assert!(bm.create_branch("123").await.is_err(), "pure numeric");
    assert!(bm.create_branch("   ").await.is_err(), "blank");

    // Branch from a tag.
    let sm = snapshot_manager(&table);
    let tm = TagManager::new(table.file_io().clone(), table.location().to_string());
    tm.create("v1", &sm.get_snapshot(1).await.unwrap())
        .await
        .unwrap();
    bm.create_branch_from_tag("rel", "v1").await.unwrap();
    assert!(bm.branch_exists("rel").await.unwrap());

    // Rename.
    bm.rename_branch("dev", "dev2").await.unwrap();
    assert!(!bm.branch_exists("dev").await.unwrap());
    assert!(bm.branch_exists("dev2").await.unwrap());

    // Drop.
    bm.drop_branch("dev2").await.unwrap();
    assert!(!bm.branch_exists("dev2").await.unwrap());

    let _ = file_io.delete_dir(path).await;
}

/// Regression for the object-store directory-rename fix: `rename_branch` must
/// relocate the whole branch subtree even on backends without server-side
/// directory move. opendal `rename` only moves single files, so `FileIO`
/// implements directory rename via recursive copy + delete; this verifies it on
/// the in-memory (object-store-like) backend.
#[tokio::test]
async fn test_branch_rename_on_object_store() {
    let file_io = memory_file_io();
    let path = "memory:/ver_branch_rename_obj";
    let schema = unpartitioned_schema();
    let _table = three_commit_table(&file_io, path).await;
    persist_schema(&file_io, path, &schema).await;

    let bm = BranchManager::new(file_io.clone(), path.to_string());
    bm.create_branch("dev").await.unwrap();
    assert!(bm.branch_exists("dev").await.unwrap());

    bm.rename_branch("dev", "dev2").await.unwrap();
    assert!(
        bm.branch_exists("dev2").await.unwrap(),
        "renamed branch must be materialized on object-store backends"
    );
    assert!(
        !bm.branch_exists("dev").await.unwrap(),
        "source branch must be removed after rename"
    );
}

// ---------------------------------------------------------------------------
// Case 6: branch read/write gap (取证)
//
// There is no `with_branch()` on ReadBuilder / WriteBuilder / Table, so a
// branch cannot be read or written through the normal builders. The only way
// to reach branch data is to point a fresh `Table` at the branch's physical
// directory (`<table>/branch/branch-<name>`). This test documents that the
// data plane IS isolated per branch and reachable via that path workaround —
// quantifying that the missing piece is the ergonomic API, not the storage.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_branch_read_write_requires_path_workaround() {
    let file_io = memory_file_io();
    let path = "memory:/ver_branch_rw";
    let schema = unpartitioned_schema();

    // Main branch: two rows.
    setup_dirs(&file_io, path).await;
    let main = make_table(&file_io, path, schema.clone());
    write_one_commit(&main, int_batch(vec![1, 2], vec![10, 20])).await;
    persist_schema(&file_io, path, &schema).await;

    // Create branch metadata.
    let bm = BranchManager::new(file_io.clone(), path.to_string());
    bm.create_branch("dev").await.unwrap();

    // No first-class branch read/write API: must build a Table at the branch
    // directory directly. (If ReadBuilder/WriteBuilder gained with_branch(),
    // this manual redirection would be unnecessary.)
    let branch_path = format!("{path}/branch/branch-dev");
    setup_dirs(&file_io, &branch_path).await;
    let branch = make_table(&file_io, &branch_path, schema.clone());
    write_one_commit(&branch, int_batch(vec![100, 200], vec![1, 2])).await;

    // Writes are isolated: main and branch see only their own data.
    assert_eq!(
        read_ids(&main).await,
        vec![1, 2],
        "main unaffected by branch"
    );
    assert_eq!(
        read_ids(&branch).await,
        vec![100, 200],
        "branch holds only its own rows"
    );
}
