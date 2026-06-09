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

//! Read one table via the local REST catalog. Used to verify Java->Rust:
//! Java writes a table, paimon-rust reads it back.
//!
//! Usage: cargo run -p paimon --example rest_read_table -- <db> <table>

use arrow_array::{Int32Array, StringArray};
use futures::TryStreamExt;

use paimon::catalog::Identifier;
use paimon::common::{CatalogOptions, Options};
use paimon::CatalogFactory;

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let db = args.get(1).cloned().unwrap_or_else(|| "java_db".to_string());
    let table = args.get(2).cloned().unwrap_or_else(|| "jtable".to_string());

    let uri = std::env::var("REST_URI").unwrap_or_else(|_| "http://localhost:8080".to_string());
    let warehouse =
        std::env::var("REST_WAREHOUSE").unwrap_or_else(|_| "/tmp/paimon-warehouse".to_string());

    let mut options = Options::new();
    options.set(CatalogOptions::METASTORE, "rest");
    options.set(CatalogOptions::URI, &uri);
    options.set(CatalogOptions::WAREHOUSE, &warehouse);
    options.set(CatalogOptions::TOKEN_PROVIDER, "bear");
    options.set(CatalogOptions::TOKEN, "dummy-token");

    let catalog = match CatalogFactory::create(options).await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("FAILED to create catalog: {e}");
            std::process::exit(1);
        }
    };

    let ident = Identifier::new(&db, &table);
    println!("=== Rust reads Java-written table {}.{} ===", db, table);
    let t = match catalog.get_table(&ident).await {
        Ok(t) => t,
        Err(e) => {
            eprintln!("JAVA->RUST FAILED at get_table: {e}");
            std::process::exit(1);
        }
    };

    let read_builder = t.new_read_builder();
    let plan = match read_builder.new_scan().plan().await {
        Ok(p) => p,
        Err(e) => {
            eprintln!("JAVA->RUST FAILED at plan: {e}");
            std::process::exit(1);
        }
    };
    println!("splits: {}", plan.splits().len());

    let read = read_builder.new_read().expect("new_read");
    let mut stream = match read.to_arrow(plan.splits()) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("JAVA->RUST FAILED at to_arrow: {e}");
            std::process::exit(1);
        }
    };

    let mut total = 0usize;
    loop {
        match stream.try_next().await {
            Ok(Some(batch)) => {
                total += batch.num_rows();
                let ids = batch.column(0).as_any().downcast_ref::<Int32Array>();
                let names = batch.column(1).as_any().downcast_ref::<StringArray>();
                for i in 0..batch.num_rows() {
                    let id = ids.map(|a| a.value(i).to_string()).unwrap_or_default();
                    let name = names.map(|a| a.value(i).to_string()).unwrap_or_default();
                    println!("  row: id={id}, name={name}");
                }
            }
            Ok(None) => break,
            Err(e) => {
                eprintln!("JAVA->RUST FAILED while reading: {e}");
                std::process::exit(1);
            }
        }
    }
    println!("Rust read {total} rows from Java-written table");
    if total == 2 {
        println!("JAVA->RUST OK");
    } else {
        println!("JAVA->RUST WRONG COUNT (expected 2, got {total})");
        std::process::exit(1);
    }
}
