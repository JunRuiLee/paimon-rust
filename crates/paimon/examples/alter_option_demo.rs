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

//! Example: ALTER TABLE on a paimon table on local filesystem to add /
//! override a single option, without touching the data files.
//!
//! Useful for tweaking read-side knobs like `source.split.target-size` on an
//! existing table — `Table::schema().options()` is the only place
//! `TableScan::plan()` reads them from, so a SET-OPTION schema change is
//! enough; the alter writes a new `schema-N+1` file under
//! `<table_path>/schema/`.
//!
//! # Usage
//! ```bash
//! cargo run -p paimon --example alter_option_demo --release -- \
//!     <table_path> <key> <value>
//! # e.g.
//! cargo run -p paimon --example alter_option_demo --release -- \
//!     /tmp/paimon_local/bench_db.db/mor_primitive_100m \
//!     source.split.target-size 2gb
//! ```

use paimon::catalog::Identifier;
use paimon::common::{CatalogOptions, Options};
use paimon::spec::SchemaChange;
use paimon::CatalogFactory;

fn print_usage(argv0: &str) {
    eprintln!("Usage: {argv0} <table_path> <key> <value>");
    eprintln!("  <table_path>: e.g. /tmp/paimon_local/bench_db.db/mor_primitive_100m");
    eprintln!("  <key>:        e.g. source.split.target-size");
    eprintln!("  <value>:      e.g. 2gb");
}

/// Split `<table_path>` into (warehouse, database, table). Same logic the
/// read demo uses; lifted verbatim so this example stays self-contained.
fn split_table_path(path: &str) -> Result<(String, String, String), String> {
    let trimmed = path.trim_end_matches('/');
    if trimmed.is_empty() {
        return Err(format!("invalid table_path: {path}"));
    }
    let p = std::path::Path::new(trimmed);
    let table = p
        .file_name()
        .and_then(|s| s.to_str())
        .ok_or_else(|| format!("cannot extract table name from: {path}"))?;
    let parent = p
        .parent()
        .ok_or_else(|| format!("table_path has no parent (db dir): {path}"))?;
    let db_dir = parent
        .file_name()
        .and_then(|s| s.to_str())
        .ok_or_else(|| format!("cannot extract db dir from: {path}"))?;
    let db = db_dir
        .strip_suffix(".db")
        .ok_or_else(|| format!("expected db directory ending in '.db', got: {db_dir}"))?;
    if db.is_empty() {
        return Err(format!("empty db name parsed from: {path}"));
    }
    let warehouse = parent
        .parent()
        .ok_or_else(|| format!("table_path's db dir has no parent (warehouse): {path}"))?
        .to_str()
        .ok_or_else(|| format!("warehouse path is not valid UTF-8: {path}"))?;
    if warehouse.is_empty() {
        return Err(format!("empty warehouse parsed from: {path}"));
    }
    Ok((warehouse.to_string(), db.to_string(), table.to_string()))
}

#[tokio::main]
async fn main() {
    let argv: Vec<String> = std::env::args().collect();
    let argv0 = argv
        .first()
        .cloned()
        .unwrap_or_else(|| "demo".to_string());

    if argv.len() != 4 {
        eprintln!("error: expected exactly 3 arguments: <table_path> <key> <value>");
        print_usage(&argv0);
        std::process::exit(2);
    }
    let table_path = &argv[1];
    let key = &argv[2];
    let value = &argv[3];

    let (warehouse, db, tbl) = match split_table_path(table_path) {
        Ok(t) => t,
        Err(e) => {
            eprintln!("error: {e}");
            std::process::exit(2);
        }
    };
    println!("warehouse={warehouse} db={db} table={tbl}");
    println!("set option: {key} = {value}");

    let mut options = Options::new();
    options.set(CatalogOptions::METASTORE, "filesystem");
    options.set(CatalogOptions::WAREHOUSE, &warehouse);

    let catalog = match CatalogFactory::create(options).await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("failed to create FileSystemCatalog: {e}");
            std::process::exit(1);
        }
    };

    let ident = Identifier::new(&db, &tbl);
    let change = SchemaChange::set_option(key.clone(), value.clone());

    if let Err(e) = catalog
        .alter_table(&ident, vec![change], /*ignore_if_not_exists=*/ false)
        .await
    {
        eprintln!("alter_table failed: {e}");
        std::process::exit(1);
    }

    println!("ok — wrote a new schema version to <table>/schema/");
}
