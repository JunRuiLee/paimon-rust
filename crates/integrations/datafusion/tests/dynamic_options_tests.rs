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

//! Integration tests for `PaimonTableProvider`'s dynamic-options surface:
//! `with_dynamic_options(...)` and `with_respect_session_batch_size(...)`.
//!
//! Each test seeds a small append-only table via SQL using a `FileSystemCatalog`
//! over a temp warehouse, then re-fetches the paimon `Table` through that same
//! catalog and constructs a fresh `PaimonTableProvider` configured with the
//! option(s) under test. We run `provider.scan(...).execute()` and check the
//! produced batches' `num_rows` against the expected effective `read.batch-size`.

mod common;

use std::collections::HashMap;
use std::sync::Arc;

use common::create_test_env;
use datafusion::arrow::array::RecordBatch;
use datafusion::datasource::TableProvider;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use paimon::catalog::Identifier;
use paimon::table::Table;
use paimon::{Catalog, FileSystemCatalog};
use paimon_datafusion::{PaimonTableProvider, SQLContext};
use tempfile::TempDir;

/// Total rows we seed across multiple commits. Picked large enough that the
/// default Paimon read.batch-size (1024) yields a single batch, while small
/// dynamic batch sizes (e.g. 256) split the output into multiple batches.
const SEEDED_ROWS: i32 = 600;

/// Build a temp catalog + SQL context, seed an append-only `id INT, value INT`
/// table populated with `SEEDED_ROWS` rows across two commits, and return the
/// loaded paimon `Table` plus the supporting handles (so the temp dir lives
/// for the duration of the test).
async fn seed_append_table(
    table_name: &str,
) -> (TempDir, Arc<FileSystemCatalog>, Table) {
    let (tmp, catalog) = create_test_env();
    let mut sql_context = SQLContext::new();
    sql_context
        .register_catalog("paimon", catalog.clone() as Arc<dyn Catalog>)
        .await
        .expect("register catalog");

    run_sql(&sql_context, "CREATE SCHEMA paimon.test_db").await;
    run_sql(
        &sql_context,
        &format!(
            "CREATE TABLE paimon.test_db.{table_name} (\n  id INT NOT NULL,\n  value INT NOT NULL\n)"
        ),
    )
    .await;

    let half = SEEDED_ROWS / 2;
    seed_commit(&sql_context, table_name, 0, half).await;
    seed_commit(&sql_context, table_name, half, SEEDED_ROWS).await;

    let identifier = Identifier::new("test_db", table_name);
    let table = catalog
        .get_table(&identifier)
        .await
        .expect("get_table after seed");
    (tmp, catalog, table)
}

async fn run_sql(sql_context: &SQLContext, sql: &str) {
    sql_context
        .sql(sql)
        .await
        .unwrap_or_else(|e| panic!("SQL parse failed for `{sql}`: {e}"))
        .collect()
        .await
        .unwrap_or_else(|e| panic!("SQL execute failed for `{sql}`: {e}"));
}

async fn seed_commit(sql_context: &SQLContext, table_name: &str, lo: i32, hi: i32) {
    let values: Vec<String> = (lo..hi).map(|i| format!("({i}, {i})")).collect();
    let sql = format!(
        "INSERT INTO paimon.test_db.{table_name} VALUES {}",
        values.join(", ")
    );
    run_sql(sql_context, &sql).await;
}

/// Run `provider.scan(...)` against a session with the given batch size and
/// collect every output record batch's row count.
async fn batch_row_counts(provider: &PaimonTableProvider, session_batch_size: usize) -> Vec<usize> {
    let config = SessionConfig::new().with_batch_size(session_batch_size);
    let ctx = SessionContext::new_with_config(config);
    let state = ctx.state();
    let plan = provider
        .scan(&state, None, &[], None)
        .await
        .expect("scan() should succeed");
    let batches: Vec<RecordBatch> = collect(plan, ctx.task_ctx()).await.expect("execute plan");
    batches.iter().map(|b| b.num_rows()).collect()
}

#[tokio::test]
async fn test_with_dynamic_options_overrides_read_batch_size() {
    let (_tmp, _catalog, table) = seed_append_table("t_dyn").await;

    let provider = PaimonTableProvider::try_new(table)
        .expect("try_new")
        .with_dynamic_options(HashMap::from([(
            "read.batch-size".to_string(),
            "256".to_string(),
        )]));

    // DataFusion's default session batch_size is 8192; the dynamic option
    // must still cap each output batch at 256 rows.
    let counts = batch_row_counts(&provider, 8192).await;

    assert!(!counts.is_empty(), "expected at least one batch");
    assert!(
        counts.iter().all(|&n| n <= 256),
        "every batch must be <= 256 rows under dynamic read.batch-size=256, got {counts:?}"
    );
    assert_eq!(
        counts.iter().sum::<usize>(),
        SEEDED_ROWS as usize,
        "total rows should still equal the seed count"
    );
}

#[tokio::test]
async fn test_respect_session_batch_size_translates_session_value() {
    let (_tmp, _catalog, table) = seed_append_table("t_session").await;

    let provider = PaimonTableProvider::try_new(table)
        .expect("try_new")
        .with_respect_session_batch_size(true);

    let counts = batch_row_counts(&provider, 200).await;

    assert!(!counts.is_empty(), "expected at least one batch");
    assert!(
        counts.iter().all(|&n| n <= 200),
        "every batch must be <= 200 rows under session batch_size=200, got {counts:?}"
    );
}

#[tokio::test]
async fn test_explicit_dynamic_wins_over_session_batch_size() {
    let (_tmp, _catalog, table) = seed_append_table("t_priority").await;

    let provider = PaimonTableProvider::try_new(table)
        .expect("try_new")
        .with_dynamic_options(HashMap::from([(
            "read.batch-size".to_string(),
            "128".to_string(),
        )]))
        .with_respect_session_batch_size(true);

    // The session says 4096, but the explicit dynamic option must win.
    let counts = batch_row_counts(&provider, 4096).await;

    assert!(!counts.is_empty(), "expected at least one batch");
    assert!(
        counts.iter().all(|&n| n <= 128),
        "explicit dynamic read.batch-size=128 must win over session=4096, got {counts:?}"
    );
}

#[tokio::test]
async fn test_default_provider_uses_paimon_default_batch_size() {
    let (_tmp, _catalog, table) = seed_append_table("t_default").await;

    let provider = PaimonTableProvider::try_new(table).expect("try_new");

    // Session batch_size is 8192; without `with_respect_session_batch_size`,
    // batches must stay capped at the Paimon default 1024.
    let counts = batch_row_counts(&provider, 8192).await;

    assert!(!counts.is_empty(), "expected at least one batch");
    assert!(
        counts.iter().all(|&n| n <= 1024),
        "without opt-in, batches must stay <= paimon default 1024, got {counts:?}"
    );
}

#[tokio::test]
async fn test_dynamic_options_do_not_mutate_original_table() {
    let (_tmp, catalog, table) = seed_append_table("t_isolation").await;

    let key = "read.batch-size".to_string();
    let original_present = table.schema().options().contains_key(&key);

    let provider = PaimonTableProvider::try_new(table.clone())
        .expect("try_new")
        .with_dynamic_options(HashMap::from([(key.clone(), "256".to_string())]));
    // Force option resolution by running a scan.
    let _ = batch_row_counts(&provider, 8192).await;

    // Both the cloned `table` we held and the provider's cached Table must
    // still reflect the unmodified schema option set.
    assert_eq!(
        provider.table().schema().options().contains_key(&key),
        original_present,
        "dynamic options must not bleed into the provider's cached Table schema"
    );
    assert_eq!(
        table.schema().options().contains_key(&key),
        original_present,
        "dynamic options must not mutate the source Table's schema options"
    );

    // Re-fetching through the catalog must also see the unmodified schema.
    let refetched = catalog
        .get_table(&Identifier::new("test_db", "t_isolation"))
        .await
        .expect("re-fetch table");
    assert_eq!(
        refetched.schema().options().contains_key(&key),
        original_present,
        "catalog re-fetch must see the unmodified schema"
    );
}
