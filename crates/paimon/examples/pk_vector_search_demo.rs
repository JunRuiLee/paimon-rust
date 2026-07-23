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

//! Runnable end-to-end demo of the primary-key vector search read path, driven
//! by a table **written by Apache Paimon's Java writer** (not assembled in Rust).
//!
//! Run with:
//!   cargo run --example pk_vector_search_demo -p paimon
//!
//! Every scenario opens the SAME committed Java-produced table directory
//! `testdata/pkvector/pk_vector_demo` — a real primary-key vector table written
//! by the production Java `ivf-flat` indexer (real data parquet + real ANN index
//! segment + snapshot/manifest/index-manifest). The scenarios differ only in what
//! the Rust reader does at read time (query vector, residual filter, batch,
//! refine-factor via `copy_with_options`, `global-index.thread-num`, column-name
//! case), so this is a true cross-language read-back, not a Rust-built fixture.
//!
//! Each scenario self-validates: it computes brute-force exact squared-L2 top-k
//! ground truth in Rust from the known fixture vectors, then asserts the read
//! result matches and prints a PASS line with actual-vs-expected. Any mismatch
//! panics, so a clean run == everything matched.
//!
//! Provenance of `testdata/pkvector/pk_vector_demo` (opaque Java-written binary
//! table dir; regenerate rather than hand-edit):
//!   * Source: Apache Paimon Java, module `paimon-vector`, commit `c0a9dca3d`.
//!   * Generator: `PkVectorFixtureGenerator#generatePkVectorDemoFixture`.
//!   * Command: `mvn -pl paimon-vector test \
//!       -Dtest='PkVectorFixtureGenerator#generatePkVectorDemoFixture' \
//!       -Dgen.pkvector.fixture=true -Drun.e2e.tests=true \
//!       -Dspotless.check.skip=true -Dcheckstyle.skip=true`.
//!   * Config: PK `id`, vector col `embedding` VECTOR(2,FLOAT), `ivf-flat`,
//!     `nlist = 1` (exact single inverted list), `deduplicate`, deletion-vectors
//!     enabled. `id == row position` (Java writes no `first_row_id` on PK tables).
//!   * Rows (id == position): [0,4] [8,0] [0,5] [7,0] [0,6] [9,0].
//!   * Expected top-3: query [10,0] -> ids [5,1,3]; residual id>=3 over [10,0] ->
//!     ids [5,3,4]; query [0,10] -> ids [4,2,0]; query [6,3] -> ids [3,1,5].
//!   * Fixture tree checksum: `d5001e91911ca384c20fd87e83e6e2d05a93fd6a`.

use std::collections::HashMap;
use std::path::Path;

use arrow_array::{Array, FixedSizeListArray, Float32Array, Int32Array, RecordBatch};
use futures::TryStreamExt;
use paimon::catalog::Identifier;
use paimon::io::{FileIO, FileIOBuilder};
use paimon::spec::{Datum, Predicate, PredicateBuilder};
use paimon::table::{ArrowRecordBatchStream, DataSplit, SchemaManager, Table};

const VECTOR_COLUMN: &str = "embedding";
const FIXTURE: &str = "testdata/pkvector/pk_vector_demo";

/// The exact vectors the Java generator wrote, `id == row position`. Kept in sync
/// with `PkVectorFixtureGenerator#DEMO_VECTORS`; the demo asserts read results
/// against brute-force top-k derived from these.
const VECTORS: &[[f32; 2]] = &[
    [0.0, 4.0], // id 0
    [8.0, 0.0], // id 1
    [0.0, 5.0], // id 2
    [7.0, 0.0], // id 3
    [0.0, 6.0], // id 4
    [9.0, 0.0], // id 5
];

// ----------------------------------------------------------------------------
// Ground truth (mirrors the vindex/PK kernel: squared-L2, score = 1/(1+dist)).
// ----------------------------------------------------------------------------

fn l2_score(distance: f32) -> f32 {
    1.0 / (1.0 + distance)
}

/// Brute-force exact squared-L2 top-k, best-first, optionally restricted to ids
/// passing `keep`. `position == id` because the fixture pins `id == row position`.
fn analytic_topk(query: &[f32], k: usize, keep: impl Fn(u64) -> bool) -> Vec<(u64, f32)> {
    let mut scored: Vec<(u64, f32)> = VECTORS
        .iter()
        .enumerate()
        .filter(|(pos, _)| keep(*pos as u64))
        .map(|(pos, v)| {
            let dist: f32 = v
                .iter()
                .zip(query.iter())
                .map(|(a, b)| (a - b) * (a - b))
                .sum();
            (pos as u64, dist)
        })
        .collect();
    scored.sort_by(|a, b| a.1.total_cmp(&b.1));
    scored.truncate(k);
    scored
}

// ----------------------------------------------------------------------------
// Open the Java-written table (copy to a temp dir first so reads never mutate
// the committed testdata), exactly like `pk_vector_java_fixture_test.rs`.
// ----------------------------------------------------------------------------

async fn open_java_table() -> (tempfile::TempDir, Table) {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let src = Path::new(manifest_dir).join(FIXTURE);
    let tmp = tempfile::tempdir().expect("create temp dir");
    let dst = tmp.path().join("pk_vector_demo");
    copy_dir(&src, &dst);

    let location = format!("file://{}", dst.display());
    let file_io: FileIO = FileIOBuilder::new("file").build().expect("build fs FileIO");
    let schema = SchemaManager::new(file_io.clone(), location.clone())
        .latest()
        .await
        .expect("failed to list schemas")
        .expect("fixture table has no schema");
    let table = Table::new(
        file_io,
        Identifier::new("default", "pk_vector_demo"),
        location,
        (*schema).clone(),
        None,
    );
    (tmp, table)
}

fn copy_dir(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).unwrap();
    for entry in std::fs::read_dir(src).unwrap() {
        let entry = entry.unwrap();
        let from = entry.path();
        let to = dst.join(entry.file_name());
        if from.is_dir() {
            copy_dir(&from, &to);
        } else {
            std::fs::copy(&from, &to).unwrap();
        }
    }
}

// ----------------------------------------------------------------------------
// Read helpers.
// ----------------------------------------------------------------------------

async fn drain(stream: ArrowRecordBatchStream) -> (Vec<i32>, Vec<f32>) {
    let batches = stream
        .try_collect::<Vec<_>>()
        .await
        .expect("collecting read batches failed");
    (
        col_i32(&batches, "id"),
        col_f32(&batches, "__paimon_search_score"),
    )
}

fn col_i32(batches: &[RecordBatch], col: &str) -> Vec<i32> {
    batches
        .iter()
        .flat_map(|b| {
            let idx = b.schema().index_of(col).unwrap();
            b.column(idx)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect()
}

fn col_f32(batches: &[RecordBatch], col: &str) -> Vec<f32> {
    batches
        .iter()
        .flat_map(|b| {
            let idx = b.schema().index_of(col).unwrap();
            b.column(idx)
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect()
}

async fn single_read(table: &Table, query: Vec<f32>, limit: usize) -> (Vec<i32>, Vec<f32>) {
    let mut builder = table.new_vector_search_builder();
    builder
        .with_vector_column(VECTOR_COLUMN)
        .with_query_vector(query)
        .with_limit(limit);
    let stream = builder.execute_read().await.expect("vector read failed");
    drain(stream).await
}

// ----------------------------------------------------------------------------
// Assertion helper (print PASS + actual-vs-expected; panic on mismatch).
// ----------------------------------------------------------------------------

fn scores_close(a: &[f32], b: &[f32]) -> bool {
    a.len() == b.len() && a.iter().zip(b).all(|(x, y)| (x - y).abs() < 1e-4)
}

fn check(scenario: &str, got_ids: &[i32], got_scores: &[f32], want: &[(u64, f32)]) {
    let want_ids: Vec<i32> = want.iter().map(|(id, _)| *id as i32).collect();
    let want_scores: Vec<f32> = want.iter().map(|(_, d)| l2_score(*d)).collect();
    let ok = got_ids == want_ids && scores_close(got_scores, &want_scores);
    let fmt = |s: &[f32]| {
        s.iter()
            .map(|x| format!("{x:.4}"))
            .collect::<Vec<_>>()
            .join(",")
    };
    println!(
        "  [{}] {}\n      ids   got={:?} want={:?}\n      score got=[{}] want=[{}]",
        if ok { "PASS" } else { "FAIL" },
        scenario,
        got_ids,
        want_ids,
        fmt(got_scores),
        fmt(&want_scores),
    );
    assert!(ok, "scenario `{scenario}` did not match ground truth");
}

// ----------------------------------------------------------------------------
// Scenarios — all read the SAME Java-written table, varying only the read.
// ----------------------------------------------------------------------------

/// (0) Fixture integrity: plain-read the Java-written table and assert the
/// materialized `(id, embedding)` rows are exactly the declared `VECTORS`. This
/// pins the Rust ground truth to what Java actually wrote, so the search
/// assertions below cannot silently drift from the on-disk data.
async fn scenario_fixture_integrity(table: &Table) {
    println!("\n== Scenario 0: fixture integrity (read back what Java wrote) ==");
    let read_builder = table.new_read_builder();
    let splits: Vec<DataSplit> = read_builder
        .new_scan()
        .plan()
        .await
        .expect("plan failed")
        .splits()
        .to_vec();
    let batches = read_builder
        .new_read()
        .expect("new_read failed")
        .to_arrow(&splits)
        .expect("to_arrow failed")
        .try_collect::<Vec<_>>()
        .await
        .expect("collect failed");

    // Gather (id -> [f32;2]) across batches.
    let mut got: Vec<(i32, Vec<f32>)> = Vec::new();
    for b in &batches {
        let ids = b
            .column(b.schema().index_of("id").unwrap())
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let fsl = b
            .column(b.schema().index_of(VECTOR_COLUMN).unwrap())
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .expect("embedding must materialize as FixedSizeList");
        for row in 0..b.num_rows() {
            let v = fsl.value(row);
            let floats = v.as_any().downcast_ref::<Float32Array>().unwrap();
            got.push((ids.value(row), floats.values().to_vec()));
        }
    }
    got.sort_by_key(|(id, _)| *id);

    assert_eq!(got.len(), VECTORS.len(), "row count mismatch");
    for (id, vec) in &got {
        let want = &VECTORS[*id as usize];
        assert!(
            vec.len() == 2 && (vec[0] - want[0]).abs() < 1e-6 && (vec[1] - want[1]).abs() < 1e-6,
            "id {id} vector {vec:?} != declared {want:?}"
        );
    }
    println!(
        "  [PASS] {} rows read back; every id's embedding matches the declared fixture vectors",
        got.len()
    );
}

/// (1) single-query top-k + (2) best-first ordering (not id/position order).
async fn scenario_single_and_ordering(table: &Table) {
    println!("\n== Scenario 1+2: single-query top-k, best-first ordering ==");
    let query = vec![10.0, 0.0];
    let want = analytic_topk(&query, 3, |_| true);
    let (ids, scores) = single_read(table, query, 3).await;
    check(
        "top-3 best-first (expect ids [5,1,3], not id order)",
        &ids,
        &scores,
        &want,
    );
    assert_eq!(
        ids,
        vec![5, 1, 3],
        "best-first must not be ascending id/position"
    );
    assert!(
        scores.windows(2).all(|w| w[0] >= w[1]),
        "scores must be non-increasing (best-first)"
    );
}

/// (3) residual data predicate applied post-recall (differs from unfiltered).
async fn scenario_residual_filter(table: &Table) {
    println!("\n== Scenario 3: residual filter (id >= 3) ==");
    let query = vec![10.0, 0.0];
    let unfiltered = analytic_topk(&query, 3, |_| true);
    let restricted = analytic_topk(&query, 3, |id| id >= 3);

    let filter: Predicate = PredicateBuilder::new(table.schema().fields())
        .greater_or_equal("id", Datum::Int(3))
        .expect("build residual predicate");
    let mut builder = table.new_vector_search_builder();
    builder
        .with_vector_column(VECTOR_COLUMN)
        .with_query_vector(query)
        .with_limit(3)
        .with_filter(filter);
    let stream = builder.execute_read().await.expect("residual read failed");
    let (ids, scores) = drain(stream).await;

    println!(
        "      (unfiltered top-3 would be ids {:?})",
        unfiltered.iter().map(|(id, _)| *id).collect::<Vec<_>>()
    );
    check(
        "residual id>=3 top-3 (expect [5,3,4])",
        &ids,
        &scores,
        &restricted,
    );
    assert_ne!(
        ids,
        unfiltered
            .iter()
            .map(|(id, _)| *id as i32)
            .collect::<Vec<_>>(),
        "residual result must differ from unfiltered"
    );
}

/// (4) batch multi-vector over a shared plan; per-query correctness +
/// batch-of-one == single-query.
async fn scenario_batch(table: &Table) {
    println!("\n== Scenario 4: batch multi-vector search ==");
    let queries = vec![
        vec![10.0, 0.0], // -> [5,1,3]
        vec![0.0, 10.0], // -> [4,2,0]
        vec![6.0, 3.0],  // -> [3,1,5]
    ];
    let mut builder = table.new_batch_vector_search_builder();
    let streams = builder
        .with_vector_column(VECTOR_COLUMN)
        .with_query_vectors(queries.clone())
        .with_limit(3)
        .execute_read()
        .await
        .expect("batch read failed");
    assert_eq!(
        streams.len(),
        queries.len(),
        "one stream per query, in order"
    );

    for (i, stream) in streams.into_iter().enumerate() {
        let want = analytic_topk(&queries[i], 3, |_| true);
        let (ids, scores) = drain(stream).await;
        check(
            &format!("batch query #{i} {:?}", queries[i]),
            &ids,
            &scores,
            &want,
        );
    }

    let single = single_read(table, queries[0].clone(), 3).await;
    let mut b1 = table.new_batch_vector_search_builder();
    let mut s1 = b1
        .with_vector_column(VECTOR_COLUMN)
        .with_query_vectors(vec![queries[0].clone()])
        .with_limit(3)
        .execute_read()
        .await
        .expect("batch-of-one read failed");
    assert_eq!(s1.len(), 1);
    let b1r = drain(s1.remove(0)).await;
    assert_eq!(b1r.0, single.0, "batch-of-one ids must equal single-query");
    assert!(
        scores_close(&b1r.1, &single.1),
        "batch-of-one scores must equal single-query"
    );
    println!("  [PASS] batch-of-one == single-query (ids {:?})", b1r.0);
}

/// (5) refine-factor exact-rerank path enabled at READ time via copy_with_options;
/// with the exhaustive `nlist=1` index the result must still equal exact top-k
/// (the rerank path is exact-preserving, not order-changing).
async fn scenario_refine_factor(table: &Table) {
    println!("\n== Scenario 5: refine-factor rerank path (read-time option) ==");
    let query = vec![10.0, 0.0];
    let want = analytic_topk(&query, 3, |_| true);
    let refined = table.copy_with_options(HashMap::from([(
        format!("fields.{VECTOR_COLUMN}.ivf.refine-factor"),
        "4".to_string(),
    )]));
    let (ids, scores) = single_read(&refined, query, 3).await;
    check("refine-factor=4 top-3 (== exact)", &ids, &scores, &want);
}

/// (6) PK `execute_scored()` must fail loud, directing callers to `execute_read`.
async fn scenario_execute_scored_fails_loud(table: &Table) {
    println!("\n== Scenario 6: execute_scored() fails loud on PK-vector table ==");
    let mut builder = table.new_vector_search_builder();
    let err = builder
        .with_vector_column(VECTOR_COLUMN)
        .with_query_vector(vec![10.0, 0.0])
        .with_limit(3)
        .execute_scored()
        .await
        .expect_err("PK execute_scored must fail loud");
    let msg = format!("{err:?}");
    assert!(
        msg.contains("execute_read"),
        "error should point at execute_read, got: {msg}"
    );
    println!("  [PASS] execute_scored errored and pointed at execute_read");
}

/// (7) concurrency (#556): `global-index.thread-num` 4 vs 1 must give identical
/// results (concurrency changes fan-out, never the answer).
async fn scenario_concurrency(table: &Table) {
    println!("\n== Scenario 7: concurrency (global-index.thread-num 4 vs 1) ==");
    let query = vec![10.0, 0.0];
    let want = analytic_topk(&query, 3, |_| true);

    let t4 = table.copy_with_options(HashMap::from([(
        "global-index.thread-num".to_string(),
        "4".to_string(),
    )]));
    let (ids4, scores4) = single_read(&t4, query.clone(), 3).await;
    check("thread-num=4 top-3", &ids4, &scores4, &want);

    let t1 = table.copy_with_options(HashMap::from([(
        "global-index.thread-num".to_string(),
        "1".to_string(),
    )]));
    let (ids1, scores1) = single_read(&t1, query, 3).await;
    check("thread-num=1 top-3", &ids1, &scores1, &want);

    assert_eq!(ids4, ids1, "concurrency must not change result order");
    assert!(
        scores_close(&scores4, &scores1),
        "concurrency must not change scores"
    );
    println!("  [PASS] thread-num 4 and 1 produced identical results");
}

/// (8) case-insensitivity boundary (#496 + kwai default flip):
///   (a) wrong-case VECTOR column name fails loud (vector-column resolution is
///       exact by design — the flip does not cover it);
///   (b) wrong-case PREDICATE column resolves under the case-insensitive default.
async fn scenario_case_insensitive_boundary(table: &Table) {
    println!("\n== Scenario 8: case-insensitivity boundary ==");

    // (a) wrong-case vector column -> fails loud.
    let mut b = table.new_vector_search_builder();
    let err = b
        .with_vector_column("EMBEDDING")
        .with_query_vector(vec![10.0, 0.0])
        .with_limit(3)
        .execute_read()
        .await
        .err();
    assert!(
        err.is_some(),
        "wrong-case vector column must fail loud (exact-match by design)"
    );
    println!("  [PASS] (8a) wrong-case vector column `EMBEDDING` fails loud (exact by design)");

    // (b) wrong-case predicate column resolves under the case-insensitive default.
    let query = vec![10.0, 0.0];
    let restricted = analytic_topk(&query, 3, |id| id >= 3);
    // PredicateBuilder::new defaults to case-insensitive on kwai: "ID" -> "id".
    let filter = PredicateBuilder::new(table.schema().fields())
        .greater_or_equal("ID", Datum::Int(3))
        .expect("wrong-case predicate column should resolve under insensitive default");
    let mut builder = table.new_vector_search_builder();
    builder
        .with_vector_column(VECTOR_COLUMN)
        .with_query_vector(query)
        .with_limit(3)
        .with_filter(filter);
    let stream = builder
        .execute_read()
        .await
        .expect("residual read with wrong-case predicate column failed");
    let (ids, scores) = drain(stream).await;
    check(
        "(8b) residual with wrong-case column `ID` (>=3)",
        &ids,
        &scores,
        &restricted,
    );
}

#[tokio::main]
async fn main() {
    println!("=== primary-key vector search — end-to-end demo (Java-written table) ===");
    println!("(reads testdata/pkvector/pk_vector_demo, produced by Apache Paimon's Java ivf-flat writer)");

    let (_tmp, table) = open_java_table().await;

    scenario_fixture_integrity(&table).await;
    scenario_single_and_ordering(&table).await;
    scenario_residual_filter(&table).await;
    scenario_batch(&table).await;
    scenario_refine_factor(&table).await;
    scenario_execute_scored_fails_loud(&table).await;
    scenario_concurrency(&table).await;
    scenario_case_insensitive_boundary(&table).await;

    println!("\n=== ALL SCENARIOS PASSED ===");
}
