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

//! Differential tests for multi-level merge-on-read against Spark/Java.
//!
//! The fixtures are provisioned by `dev/spark/provision.py`: each table is
//! written and compacted by Spark (producing genuine level >= 1 files), then a
//! final un-compacted INSERT adds level-0 files overlapping the compacted keys.
//! Spark also dumps its own `SELECT` result as the authoritative oracle under
//! `<warehouse>/_rust_expected/<name>.json`. These tests read the same table
//! through paimon-rust and assert that the merge-on-read result matches Spark
//! row-for-row — the cross-implementation oracle that pure Rust tests cannot
//! provide.
//!
//! Run after provisioning:
//! ```bash
//! make docker-up
//! PAIMON_TEST_WAREHOUSE=/tmp/paimon-warehouse \
//!     cargo test -p paimon-integration-tests --test mor_multi_level_e2e
//! ```

use std::path::PathBuf;

use arrow_array::{Array, Int32Array, RecordBatch};
use futures::TryStreamExt;
use paimon::catalog::Identifier;
use paimon::common::Options;
use paimon::{Catalog, CatalogOptions, FileSystemCatalog};

fn warehouse_raw() -> String {
    std::env::var("PAIMON_TEST_WAREHOUSE").unwrap_or_else(|_| "/tmp/paimon-warehouse".to_string())
}

/// Filesystem path to the warehouse (strips any `file://` scheme used by the
/// catalog) for reading the oracle dumps directly.
fn warehouse_fs_path() -> PathBuf {
    let raw = warehouse_raw();
    PathBuf::from(raw.strip_prefix("file://").unwrap_or(&raw).to_string())
}

fn create_catalog() -> FileSystemCatalog {
    let mut options = Options::new();
    options.set(CatalogOptions::WAREHOUSE, warehouse_raw());
    FileSystemCatalog::new(options).expect("create FileSystemCatalog")
}

/// Read every column of a table as i64 rows (all fixture columns are INT),
/// sorted lexicographically for a stable, order-independent comparison.
async fn read_int_rows(catalog: &FileSystemCatalog, table_name: &str) -> Vec<Vec<i64>> {
    let table = catalog
        .get_table(&Identifier::new("default", table_name))
        .await
        .unwrap_or_else(|e| panic!("get_table {table_name}: {e}"));

    let read_builder = table.new_read_builder();
    let plan = read_builder.new_scan().plan().await.expect("plan scan");
    let read = read_builder.new_read().expect("new_read");
    let batches: Vec<RecordBatch> = read
        .to_arrow(plan.splits())
        .expect("to_arrow")
        .try_collect()
        .await
        .expect("collect batches");

    let mut rows: Vec<Vec<i64>> = Vec::new();
    for batch in &batches {
        let cols: Vec<&Int32Array> = (0..batch.num_columns())
            .map(|c| {
                batch
                    .column(c)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap_or_else(|| {
                        panic!("column {c} of {table_name} is not Int32 (fixtures are INT-only)")
                    })
            })
            .collect();
        for r in 0..batch.num_rows() {
            let row = cols
                .iter()
                .map(|c| {
                    assert!(!c.is_null(r), "unexpected NULL in merged fixture {table_name}");
                    c.value(r) as i64
                })
                .collect();
            rows.push(row);
        }
    }
    rows.sort();
    rows
}

/// Load the Spark-produced oracle rows for `table_name`, sorted to match.
fn load_oracle(table_name: &str) -> Vec<Vec<i64>> {
    let path = warehouse_fs_path()
        .join("_rust_expected")
        .join(format!("{table_name}.json"));
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| {
        panic!(
            "missing oracle {}: {e}. Provision fixtures first with `make docker-up`.",
            path.display()
        )
    });
    let parsed: serde_json::Value = serde_json::from_str(&text).expect("parse oracle json");
    let mut rows: Vec<Vec<i64>> = parsed["rows"]
        .as_array()
        .expect("oracle.rows is an array")
        .iter()
        .map(|row| {
            row.as_array()
                .expect("oracle row is an array")
                .iter()
                .map(|v| v.as_i64().expect("oracle cell is an integer"))
                .collect()
        })
        .collect();
    rows.sort();
    rows
}

/// Deduplicate engine: Spark-compacted L1 + a fresh overlapping L0. The
/// sort-merge reader must pick the newest value per key (id=1 -> 999,
/// id=2 -> 888) while keeping the compacted-only keys.
#[tokio::test]
async fn test_dedup_multi_level_matches_spark() {
    let catalog = create_catalog();
    let actual = read_int_rows(&catalog, "mor_dedup_multi_level").await;
    let expected = load_oracle("mor_dedup_multi_level");
    assert_eq!(
        actual, expected,
        "dedup multi-level MOR read must match the Spark oracle"
    );
}

/// Partial-update engine across levels: column values merged from compacted
/// files and a fresh L0 update must match Spark's own read.
#[tokio::test]
async fn test_partial_update_multi_level_matches_spark() {
    let catalog = create_catalog();
    let actual = read_int_rows(&catalog, "mor_partial_update_multi_level").await;
    let expected = load_oracle("mor_partial_update_multi_level");
    assert_eq!(
        actual, expected,
        "partial-update multi-level MOR read must match the Spark oracle"
    );
}

/// Combined dimensions: a compacted file at schema-0 and a fresh level-0 file
/// at schema-1 (produced by an option-only ALTER) overlap on keys. The reader
/// must sort-merge across both LSM levels AND schema ids. Diffed against Spark.
#[tokio::test]
async fn test_dedup_schema_evolution_matches_spark() {
    let catalog = create_catalog();
    let actual = read_int_rows(&catalog, "mor_dedup_schema_evolution").await;
    let expected = load_oracle("mor_dedup_schema_evolution");
    assert_eq!(
        actual, expected,
        "dedup cross-level + cross-schema MOR read must match the Spark oracle"
    );
}
