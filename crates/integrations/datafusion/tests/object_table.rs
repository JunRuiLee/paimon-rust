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

use std::collections::HashMap;
use std::fs;
use std::sync::Arc;

use datafusion::arrow::array::Array;
use datafusion::arrow::util::display::array_value_to_string;
use paimon::catalog::Identifier;
use paimon::spec::{BigIntType, DataType, Schema};
use paimon::{Catalog, CatalogOptions, FileSystemCatalog, Options};
use paimon_datafusion::SQLContext;
use tempfile::TempDir;

fn object_table_schema(location: &str) -> Schema {
    Schema::builder()
        .column("ignored", DataType::BigInt(BigIntType::new()))
        .option("type", "object-table")
        .option("path", location)
        .build()
        .unwrap()
}

#[tokio::test]
async fn object_table_lists_files_recursively() {
    let object_dir = TempDir::new().unwrap();
    fs::write(object_dir.path().join("root.txt"), b"root").unwrap();
    fs::create_dir(object_dir.path().join("nested")).unwrap();
    fs::write(object_dir.path().join("nested/child.bin"), b"child").unwrap();

    let location = format!("file://{}", object_dir.path().display());
    let mut options = Options::new();
    options.set(CatalogOptions::WAREHOUSE, "memory:/warehouse");
    let catalog = Arc::new(FileSystemCatalog::new(options).unwrap());
    catalog
        .create_database("db", false, HashMap::new())
        .await
        .unwrap();
    catalog
        .create_table(
            &Identifier::new("db", "objects"),
            object_table_schema(&location),
            false,
        )
        .await
        .unwrap();

    let mut ctx = SQLContext::new();
    ctx.register_catalog("cat", catalog).await.unwrap();
    let batches = ctx
        .sql(
            "SELECT path, name, length, mtime > 0 AS has_mtime, atime, owner \
             FROM cat.db.objects ORDER BY path",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let mut rows = Vec::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            rows.push(
                batch
                    .columns()
                    .iter()
                    .map(|column| {
                        if column.is_null(row) {
                            "NULL".to_string()
                        } else {
                            array_value_to_string(column.as_ref(), row).unwrap()
                        }
                    })
                    .collect::<Vec<_>>(),
            );
        }
    }

    assert_eq!(
        rows,
        vec![
            vec!["nested/child.bin", "child.bin", "5", "true", "0", "NULL"],
            vec!["root.txt", "root.txt", "4", "true", "0", "NULL"],
        ]
    );

    let count = ctx
        .sql("SELECT COUNT(*) FROM cat.db.objects")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        array_value_to_string(count[0].column(0).as_ref(), 0).unwrap(),
        "2"
    );

    let limited = ctx
        .sql("SELECT path FROM cat.db.objects LIMIT 1")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        limited.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        1
    );
    assert!(["nested/child.bin", "root.txt"].contains(
        &array_value_to_string(limited[0].column(0).as_ref(), 0)
            .unwrap()
            .as_str()
    ));

    let ordered_limited = ctx
        .sql("SELECT path FROM cat.db.objects ORDER BY path LIMIT 1")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        array_value_to_string(ordered_limited[0].column(0).as_ref(), 0).unwrap(),
        "nested/child.bin"
    );

    let error = ctx
        .sql(
            "INSERT INTO cat.db.objects \
             VALUES ('path', 'name', 1, 1, 0, NULL)",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap_err();
    assert!(error.to_string().contains("read-only"), "{error}");
}
