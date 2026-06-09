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

//! E2E integration tests for `merge-engine=versioned-partial-update` via
//! DataFusion SQL.
//!
//! Stage 6 (single-version columns only). Multi-version (MV) column E2E
//! coverage is deferred — DataFusion SQL's `INSERT INTO ... VALUES` cannot
//! produce the nested `Struct<latest_version, latest_value, all_versioned_values:
//! Map<...>>` literal that paimon-rust expects, so MV write paths need a
//! lower-level harness (or a Java-written fixture) which is tracked in
//! `docs/versioned-partial-update-impl-plan.md` Stage-6 follow-up.
//!
//! All tests below use `versioned-partial-update.ignore-mode.enabled=false`
//! to side-step Stage-5 lookup-capability validation. The IGNORE-mode read
//! path is exercised by merge-function unit tests in
//! `crates/paimon/src/table/versioned_partial_update.rs`.

mod common;

use common::{collect_id_value, row_count, setup_sql_context};

/// Plan matrix case 1 — basic round-trip. Two batches mutate the same PKs;
/// the later snapshot must win under UPSERT mode.
#[tokio::test]
async fn test_vpu_basic_round_trip() {
    let (_tmp, sql_context) = setup_sql_context().await;

    sql_context
        .sql(
            "CREATE TABLE paimon.test_db.t_vpu_basic (
                id INT NOT NULL, value INT,
                PRIMARY KEY (id)
            ) WITH (
                'bucket' = '1',
                'merge-engine' = 'versioned-partial-update',
                'sequence.snapshot-ordering' = 'true',
                'versioned-partial-update.ignore-mode.enabled' = 'false'
            )",
        )
        .await
        .expect("CREATE TABLE failed");

    // Snapshot 1: seed two rows.
    sql_context
        .sql("INSERT INTO paimon.test_db.t_vpu_basic VALUES (1, 10), (2, 20)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Snapshot 2: overwrite id=1, leave id=2 alone, add id=3.
    sql_context
        .sql("INSERT INTO paimon.test_db.t_vpu_basic VALUES (1, 100), (3, 30)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let rows =
        collect_id_value(&sql_context, "SELECT id, value FROM paimon.test_db.t_vpu_basic").await;
    assert_eq!(rows, vec![(1, 100), (2, 20), (3, 30)]);
}

/// Plan matrix case "projection: 仅 PK" — user only projects the PK
/// column; the merge function must still run on every same-PK group so
/// duplicate PKs across snapshots collapse to one row each. The final
/// output schema is just the PK.
#[tokio::test]
async fn test_vpu_projection_pk_only() {
    let (_tmp, sql_context) = setup_sql_context().await;

    sql_context
        .sql(
            "CREATE TABLE paimon.test_db.t_vpu_pk_only (
                id INT NOT NULL, value INT,
                PRIMARY KEY (id)
            ) WITH (
                'bucket' = '1',
                'merge-engine' = 'versioned-partial-update',
                'sequence.snapshot-ordering' = 'true',
                'versioned-partial-update.ignore-mode.enabled' = 'false'
            )",
        )
        .await
        .expect("CREATE TABLE failed");

    // Three commits: id=1 mutates twice, id=2 / id=3 once each. Projection
    // of just the PK must still produce 3 rows, not 5.
    for stmt in [
        "INSERT INTO paimon.test_db.t_vpu_pk_only VALUES (1, 10), (2, 20)",
        "INSERT INTO paimon.test_db.t_vpu_pk_only VALUES (1, 100), (3, 30)",
        "INSERT INTO paimon.test_db.t_vpu_pk_only VALUES (1, 1000)",
    ] {
        sql_context
            .sql(stmt)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
    }

    let count = row_count(
        &sql_context,
        "SELECT id FROM paimon.test_db.t_vpu_pk_only",
    )
    .await;
    assert_eq!(count, 3, "PK-only projection must collapse same-PK rows");

    // And the value column survives the merge: id=1 must read as 1000
    // (last snapshot wins) when re-projected.
    let rows = collect_id_value(
        &sql_context,
        "SELECT id, value FROM paimon.test_db.t_vpu_pk_only",
    )
    .await;
    assert_eq!(rows, vec![(1, 1000), (2, 20), (3, 30)]);
}

/// Plan matrix "validation reject" cases — the schema validator rejects
/// configurations the merge engine cannot serve. Tests three of the six
/// rules from `validate_versioned_partial_update`; the rest live in the
/// `spec/schema.rs` unit test mod.
#[tokio::test]
async fn test_vpu_validation_rejects_invalid_config() {
    let (_tmp, sql_context) = setup_sql_context().await;

    // Missing snapshot-ordering — Java VPU rule #1.
    let err = sql_context
        .sql(
            "CREATE TABLE paimon.test_db.t_vpu_no_ordering (
                id INT NOT NULL, value INT, PRIMARY KEY (id)
            ) WITH ('bucket' = '1', 'merge-engine' = 'versioned-partial-update')",
        )
        .await;
    assert!(
        err.is_err()
            && err
                .unwrap_err()
                .to_string()
                .contains("sequence.snapshot-ordering"),
        "expected snapshot-ordering required error",
    );

    // sequence.field forbidden — Java VPU rule #4.
    let err = sql_context
        .sql(
            "CREATE TABLE paimon.test_db.t_vpu_with_seq_field (
                id INT NOT NULL, ts INT, PRIMARY KEY (id)
            ) WITH (
                'bucket' = '1',
                'merge-engine' = 'versioned-partial-update',
                'sequence.snapshot-ordering' = 'true',
                'versioned-partial-update.ignore-mode.enabled' = 'false',
                'sequence.field' = 'ts'
            )",
        )
        .await;
    assert!(
        err.is_err() && err.unwrap_err().to_string().contains("sequence.field"),
        "expected sequence.field rejection",
    );

    // ignore-mode.enabled=true without lookup capability — Java VPU rule #3.
    let err = sql_context
        .sql(
            "CREATE TABLE paimon.test_db.t_vpu_ignore_no_lookup (
                id INT NOT NULL, value INT, PRIMARY KEY (id)
            ) WITH (
                'bucket' = '1',
                'merge-engine' = 'versioned-partial-update',
                'sequence.snapshot-ordering' = 'true',
                'versioned-partial-update.ignore-mode.enabled' = 'true'
            )",
        )
        .await;
    let msg = err.unwrap_err().to_string();
    assert!(
        msg.contains("ignore-mode") && msg.contains("lookup"),
        "expected ignore-mode lookup-capability rejection, got: {msg}",
    );
}
