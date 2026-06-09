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

//! Local end-to-end smoke test against a real Paimon REST catalog server.
//!
//! Unlike `rest_catalog_example.rs` (hardcoded sample.net + DLF), this connects
//! to a LOCAL bearer/anonymous REST server and exercises:
//!   1. metadata CRUD (db + table create/list/get/rename/drop)
//!   2. append write + commit (through RESTSnapshotCommit)
//!   3. scan + read back
//!
//! # Usage
//! ```bash
//! REST_URI=http://localhost:8080 REST_WAREHOUSE=/tmp/paimon-warehouse \
//!   cargo run -p paimon --example rest_local_smoke
//! ```

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use futures::TryStreamExt;

use paimon::catalog::Identifier;
use paimon::common::{CatalogOptions, Options};
use paimon::spec::{DataType, IntType, Schema, VarCharType};
use paimon::CatalogFactory;

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn build_schema() -> paimon::Result<Schema> {
    // Append-only table (no primary key) — keeps us off the PK/MoR read path.
    Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("name", DataType::VarChar(VarCharType::new(255).unwrap()))
        .option("bucket", "1")
        .option("bucket-key", "id")
        .build()
}

fn build_batch() -> RecordBatch {
    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Int32, true),
        ArrowField::new("name", ArrowDataType::Utf8, true),
    ]));
    RecordBatch::try_new(
        arrow_schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["alice", "bob", "carol"])),
        ],
    )
    .expect("batch build failed")
}

async fn run() -> paimon::Result<()> {
    let uri = env_or("REST_URI", "http://localhost:8080");
    let warehouse = env_or("REST_WAREHOUSE", "/tmp/paimon-warehouse");
    let token = env_or("PAIMON_REST_TOKEN", "dummy-token");

    let mut options = Options::new();
    options.set(CatalogOptions::METASTORE, "rest");
    options.set(CatalogOptions::URI, &uri);
    options.set(CatalogOptions::WAREHOUSE, &warehouse);
    options.set(CatalogOptions::TOKEN_PROVIDER, "bear");
    options.set(CatalogOptions::TOKEN, &token);

    println!("=== Connecting to REST catalog at {uri} (warehouse={warehouse}) ===");
    let catalog = CatalogFactory::create(options).await?;

    // ---------- Part 1: metadata CRUD ----------
    println!("\n=== Part 1: metadata ===");
    let db = "smoke_db";
    catalog.create_database(db, true, HashMap::new()).await?;
    println!("created database '{db}'");
    println!("databases: {:?}", catalog.list_databases().await?);
    let got = catalog.get_database(db).await?;
    println!("get_database('{db}') options: {:?}", got.options);

    let ident = Identifier::new(db, "users");
    catalog.drop_table(&ident, true).await?;
    catalog.create_table(&ident, build_schema()?, false).await?;
    println!("created table {}", ident.full_name());
    println!("tables in '{db}': {:?}", catalog.list_tables(db).await?);

    let ident2 = Identifier::new(db, "users_renamed");
    catalog.drop_table(&ident2, true).await?;
    catalog.rename_table(&ident, &ident2, false).await?;
    println!("renamed users -> users_renamed");
    catalog.rename_table(&ident2, &ident, false).await?;
    println!("renamed back users_renamed -> users");

    // ---------- Part 2: append write + commit ----------
    println!("\n=== Part 2: write + commit ===");
    let table = catalog.get_table(&ident).await?;
    let write_builder = table.new_write_builder();
    let mut writer = write_builder.new_write()?;
    writer.write_arrow_batch(&build_batch()).await?;
    let messages = writer.prepare_commit().await?;
    println!("prepared {} commit message(s)", messages.len());
    write_builder.new_commit().commit(messages).await?;
    println!("committed 3 rows");

    // ---------- Part 3: scan + read back ----------
    println!("\n=== Part 3: read back ===");
    let table = catalog.get_table(&ident).await?;
    let read_builder = table.new_read_builder();
    let plan = read_builder.new_scan().plan().await?;
    println!("splits: {}", plan.splits().len());
    let read = read_builder.new_read()?;
    let mut stream = read.to_arrow(plan.splits())?;
    let mut total = 0usize;
    while let Some(batch) = stream.try_next().await? {
        total += batch.num_rows();
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let names = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..batch.num_rows() {
            println!("  row: id={}, name={}", ids.value(i), names.value(i));
        }
    }
    println!("read back {total} rows total");

    if total == 3 {
        println!("\n✅ SMOKE TEST PASSED (metadata + write + read all OK)");
    } else {
        println!("\n❌ expected 3 rows, got {total}");
        std::process::exit(1);
    }

    println!("\n(leaving table {} on disk for Java round-trip)", ident.full_name());
    Ok(())
}

#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        eprintln!("FAILED: {e}");
        std::process::exit(1);
    }
}
