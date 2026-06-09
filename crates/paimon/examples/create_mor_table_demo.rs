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

//! Example: create a 1-bucket MoR primary-key paimon table on local disk and
//! populate it with N rows split across K commits.
//!
//! Faithful Rust port of `paimon-cpp/examples/create_paimon_mor_table.cpp`:
//! same 13-column schema, same per-row data generator, same defaults
//! (`/tmp/paimon_local`, `bench_db`, `mor_primitive_1m`, 1,000,000 rows, 8
//! commits, bucket=1). One implementation difference: the C++ demo manually
//! computes per-row bucket IDs via `BucketIdCalculator` and submits one batch
//! per bucket; paimon-rust's `TableWrite::write_arrow_batch` handles partition
//! + bucket routing internally, so the demo just hands a whole `RecordBatch`
//! to it.
//!
//! # PK distribution
//!
//! Commit `c` in `[0, K)` writes `pk = c, c+K, c+2K, ...` so every commit
//! spans the full PK range. Combined with `write-only=true`, this leaves
//! K un-compacted L0 sorted runs that overlap on PK — a realistic MoR read
//! workload at scan time, not a degenerate non-overlapping case.
//!
//! # Usage
//! ```bash
//! cargo run -p paimon --example create_mor_table_demo --release -- \
//!     [--warehouse PATH] [--db NAME] [--table NAME] \
//!     [--rows N] [--commits K] [--bucket B]
//! ```

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use arrow_array::{
    BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
    Int16Array, Int32Array, Int64Array, Int8Array, RecordBatch, StringArray,
    TimestampMicrosecondArray,
};
use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema, TimeUnit};

use paimon::catalog::Identifier;
use paimon::common::{CatalogOptions, Options};
use paimon::spec::{
    BigIntType, BooleanType, DataType, DateType, DecimalType, DoubleType, FloatType, IntType,
    Schema, SmallIntType, TimestampType, TinyIntType, VarBinaryType, VarCharType,
};
use paimon::CatalogFactory;

const DEFAULT_WAREHOUSE: &str = "/tmp/paimon_local";
const DEFAULT_DB: &str = "bench_db";
const DEFAULT_TABLE: &str = "mor_primitive_1m";
const DEFAULT_ROWS: i64 = 1_000_000;
const DEFAULT_COMMITS: i32 = 8;
const DEFAULT_BUCKET: i32 = 1;

// Same fixed unit + epoch as the C++ demo. arrow::timestamp(MICRO) →
// TimestampType(precision=6).
const TIMESTAMP_PRECISION: u32 = 6;
const TIMESTAMP_BASE_MICROS: i64 = 1_700_000_000_000_000;

// Decimal(18, 2) — paimon stores as int128 unscaled; matches the C++ side.
const DECIMAL_PRECISION: u32 = 18;
const DECIMAL_SCALE: u32 = 2;

#[derive(Debug)]
struct Args {
    warehouse: String,
    db: String,
    table: String,
    rows: i64,
    commits: i32,
    bucket: i32,
}

fn print_usage(argv0: &str) {
    eprintln!(
        "Usage: {argv0} [--warehouse PATH] [--db NAME] [--table NAME] \
         [--rows N] [--commits K] [--bucket B]"
    );
    eprintln!(
        "  defaults: warehouse={DEFAULT_WAREHOUSE} db={DEFAULT_DB} \
         table={DEFAULT_TABLE} rows={DEFAULT_ROWS} commits={DEFAULT_COMMITS} \
         bucket={DEFAULT_BUCKET}"
    );
}

fn need_arg(argv: &[String], i: &mut usize, flag: &str) -> Result<String, String> {
    *i += 1;
    argv.get(*i)
        .cloned()
        .ok_or_else(|| format!("{flag} needs an argument"))
}

fn parse_args() -> Result<Args, String> {
    let argv: Vec<String> = std::env::args().collect();
    let mut a = Args {
        warehouse: DEFAULT_WAREHOUSE.to_string(),
        db: DEFAULT_DB.to_string(),
        table: DEFAULT_TABLE.to_string(),
        rows: DEFAULT_ROWS,
        commits: DEFAULT_COMMITS,
        bucket: DEFAULT_BUCKET,
    };

    let mut i = 1;
    while i < argv.len() {
        let arg = argv[i].clone();
        match arg.as_str() {
            "--warehouse" => a.warehouse = need_arg(&argv, &mut i, "--warehouse")?,
            "--db" => a.db = need_arg(&argv, &mut i, "--db")?,
            "--table" => a.table = need_arg(&argv, &mut i, "--table")?,
            "--rows" => {
                a.rows = need_arg(&argv, &mut i, "--rows")?
                    .parse::<i64>()
                    .map_err(|e| format!("--rows: {e}"))?;
            }
            "--commits" => {
                a.commits = need_arg(&argv, &mut i, "--commits")?
                    .parse::<i32>()
                    .map_err(|e| format!("--commits: {e}"))?;
            }
            "--bucket" => {
                a.bucket = need_arg(&argv, &mut i, "--bucket")?
                    .parse::<i32>()
                    .map_err(|e| format!("--bucket: {e}"))?;
            }
            "-h" | "--help" => {
                print_usage(&argv[0]);
                std::process::exit(0);
            }
            other => return Err(format!("unknown argument: {other}")),
        }
        i += 1;
    }

    if a.rows <= 0 {
        return Err("--rows must be positive".to_string());
    }
    if a.commits <= 0 {
        return Err("--commits must be positive".to_string());
    }
    if a.bucket <= 0 {
        return Err("--bucket must be positive".to_string());
    }
    if a.rows % (a.commits as i64) != 0 {
        return Err(format!(
            "--rows ({}) must be divisible by --commits ({})",
            a.rows, a.commits
        ));
    }

    Ok(a)
}

/// 13-column schema, mirroring `BuildFields` in the C++ demo. `id` is the
/// non-nullable primary key; all other columns are nullable.
fn build_schema(bucket: i32) -> paimon::Result<Schema> {
    Schema::builder()
        .column("id", DataType::BigInt(BigIntType::with_nullable(false)))
        .column("v_bool", DataType::Boolean(BooleanType::new()))
        .column("v_tiny", DataType::TinyInt(TinyIntType::new()))
        .column("v_short", DataType::SmallInt(SmallIntType::new()))
        .column("v_int", DataType::Int(IntType::new()))
        .column("v_bigint", DataType::BigInt(BigIntType::new()))
        .column("v_float", DataType::Float(FloatType::new()))
        .column("v_double", DataType::Double(DoubleType::new()))
        .column(
            "v_decimal",
            DataType::Decimal(DecimalType::new(DECIMAL_PRECISION, DECIMAL_SCALE)?),
        )
        .column("v_string", DataType::VarChar(VarCharType::string_type()))
        .column("v_date", DataType::Date(DateType::new()))
        .column(
            "v_timestamp",
            DataType::Timestamp(TimestampType::new(TIMESTAMP_PRECISION)?),
        )
        // arrow::binary() is variable-length, no max → mirror with paimon's
        // VARBINARY(MAX_LENGTH) which is the i32::MAX equivalent of Java's
        // `VarBinaryType.MAX_LENGTH = Integer.MAX_VALUE`.
        .column(
            "v_binary",
            DataType::VarBinary(VarBinaryType::new(VarBinaryType::MAX_LENGTH)?),
        )
        .primary_key(["id"])
        .option("bucket", bucket.to_string())
        // Keep the K LSM runs at L0 un-compacted so reads exercise SortMergeReader
        // for real (== "MoR table" in the benchmark sense).
        .option("write-only", "true")
        // Bin-pack threshold for split planning. paimon-rust's split_for_batch
        // packs files (sorted by min_sequence_number) into bins capped at this
        // size; 2 GiB is large enough to swallow this demo's full bucket
        // (8 commits × ~200 MB ≈ 1.6 GB) into a single split.
        .option("source.split.target-size", "2gb")
        .build()
}

/// Build one commit's worth of rows. PKs follow the C++ demo's pattern:
/// `pk = pk_start + i * pk_stride` for `i in [0, pk_count)`. With
/// `pk_start = c` and `pk_stride = K` per commit `c`, every commit spans the
/// full PK range so reads must sort-merge to dedupe.
///
/// Per-column generators are identical to `BuildBatch` in
/// `create_paimon_mor_table.cpp` — same masks, multipliers, base epoch, etc.
fn build_batch(pk_start: i64, pk_stride: i64, pk_count: i64) -> RecordBatch {
    let n = pk_count as usize;
    let mut ids = Vec::with_capacity(n);
    let mut bools = Vec::with_capacity(n);
    let mut tinys = Vec::with_capacity(n);
    let mut shorts = Vec::with_capacity(n);
    let mut ints = Vec::with_capacity(n);
    let mut bigints = Vec::with_capacity(n);
    let mut floats = Vec::with_capacity(n);
    let mut doubles = Vec::with_capacity(n);
    let mut decimals = Vec::with_capacity(n);
    let mut strs = Vec::with_capacity(n);
    let mut dates = Vec::with_capacity(n);
    let mut timestamps = Vec::with_capacity(n);
    let mut binaries: Vec<[u8; 2]> = Vec::with_capacity(n);

    for i in 0..pk_count {
        let pk = pk_start + i * pk_stride;
        ids.push(pk);
        bools.push((pk & 1) == 0);
        tinys.push((pk & 0x7F) as i8);
        shorts.push((pk & 0x7FFF) as i16);
        ints.push((pk & 0x7FFF_FFFF) as i32);
        bigints.push(pk * 1000);
        floats.push((pk as f32) * 0.5);
        doubles.push((pk as f64) * 1.5);
        // Decimal(18,2) unscaled = pk*100 + 50  (== pk + 0.50). For the largest
        // pk this demo can produce (rows-1, < 2^63), pk*100 fits safely in i128.
        decimals.push((pk as i128) * 100 + 50);
        strs.push(format!("str-{pk}"));
        dates.push((pk % 10000) as i32);
        timestamps.push(TIMESTAMP_BASE_MICROS + pk);
        binaries.push([(pk & 0xFF) as u8, ((pk >> 8) & 0xFF) as u8]);
    }

    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Int64, false),
        ArrowField::new("v_bool", ArrowDataType::Boolean, true),
        ArrowField::new("v_tiny", ArrowDataType::Int8, true),
        ArrowField::new("v_short", ArrowDataType::Int16, true),
        ArrowField::new("v_int", ArrowDataType::Int32, true),
        ArrowField::new("v_bigint", ArrowDataType::Int64, true),
        ArrowField::new("v_float", ArrowDataType::Float32, true),
        ArrowField::new("v_double", ArrowDataType::Float64, true),
        ArrowField::new(
            "v_decimal",
            ArrowDataType::Decimal128(DECIMAL_PRECISION as u8, DECIMAL_SCALE as i8),
            true,
        ),
        ArrowField::new("v_string", ArrowDataType::Utf8, true),
        ArrowField::new("v_date", ArrowDataType::Date32, true),
        ArrowField::new(
            "v_timestamp",
            ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
        ArrowField::new("v_binary", ArrowDataType::Binary, true),
    ]));

    let decimal_arr = Decimal128Array::from(decimals)
        .with_precision_and_scale(DECIMAL_PRECISION as u8, DECIMAL_SCALE as i8)
        .expect("Decimal128Array precision/scale rejected — schema mismatch (programmer bug)");

    let binary_refs: Vec<&[u8]> = binaries.iter().map(|b| b.as_slice()).collect();
    let binary_arr = BinaryArray::from_vec(binary_refs);

    RecordBatch::try_new(
        arrow_schema,
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(BooleanArray::from(bools)),
            Arc::new(Int8Array::from(tinys)),
            Arc::new(Int16Array::from(shorts)),
            Arc::new(Int32Array::from(ints)),
            Arc::new(Int64Array::from(bigints)),
            Arc::new(Float32Array::from(floats)),
            Arc::new(Float64Array::from(doubles)),
            Arc::new(decimal_arr),
            Arc::new(StringArray::from(strs)),
            Arc::new(Date32Array::from(dates)),
            Arc::new(TimestampMicrosecondArray::from(timestamps)),
            Arc::new(binary_arr),
        ],
    )
    .expect("RecordBatch::try_new failed — schema/array shape mismatch (programmer bug)")
}

async fn run(args: Args) -> paimon::Result<()> {
    let mut catalog_options = Options::new();
    catalog_options.set(CatalogOptions::METASTORE, "filesystem");
    catalog_options.set(CatalogOptions::WAREHOUSE, &args.warehouse);

    let catalog = CatalogFactory::create(catalog_options).await?;

    println!("=== Create paimon MoR primitive table ===");
    println!("Warehouse: {}", args.warehouse);
    println!("Database:  {}", args.db);
    println!("Table:     {}", args.table);
    println!("Rows:      {}", args.rows);
    println!("Commits:   {}", args.commits);
    println!("Bucket:    {}", args.bucket);
    println!();

    // 1. Create database (idempotent).
    catalog
        .create_database(&args.db, /*ignore_if_exists=*/ true, HashMap::new())
        .await?;

    // 2. Drop & recreate the table for an idempotent run — every invocation
    //    yields a fresh table with exactly K L0 runs.
    let ident = Identifier::new(&args.db, &args.table);
    catalog
        .drop_table(&ident, /*ignore_if_not_exists=*/ true)
        .await?;
    catalog
        .create_table(
            &ident,
            build_schema(args.bucket)?,
            /*ignore_if_exists=*/ false,
        )
        .await?;
    println!("Created {}.{}", args.db, args.table);

    // 3. Get a fresh table handle to start writing.
    let table = catalog.get_table(&ident).await?;
    let rows_per_commit = args.rows / (args.commits as i64);

    let t_total = Instant::now();
    for c in 0..args.commits {
        let t_commit = Instant::now();

        // Each commit gets a fresh WriteBuilder → TableWrite → TableCommit.
        let write_builder = table.new_write_builder();
        let mut writer = write_builder.new_write()?;
        let batch = build_batch(c as i64, args.commits as i64, rows_per_commit);
        writer.write_arrow_batch(&batch).await?;
        let messages = writer.prepare_commit().await?;
        write_builder.new_commit().commit(messages).await?;

        let dt_ms = t_commit.elapsed().as_millis();
        println!(
            "commit {}/{}: {} rows in {} ms",
            c + 1,
            args.commits,
            rows_per_commit,
            dt_ms
        );
    }
    let total_ms = t_total.elapsed().as_millis();

    println!();
    println!(
        "Wrote {} rows in {} commits in {} ms total",
        args.rows, args.commits, total_ms
    );
    println!(
        "Path:           {}/{}.db/{}",
        args.warehouse, args.db, args.table
    );
    println!(
        "LSM runs at L0: {} (write-only=true preserved them; read will sort-merge)",
        args.commits
    );
    Ok(())
}

#[tokio::main]
async fn main() {
    let argv0 = std::env::args().next().unwrap_or_else(|| "demo".to_string());

    let args = match parse_args() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("error: {e}");
            print_usage(&argv0);
            std::process::exit(2);
        }
    };

    if let Err(e) = run(args).await {
        eprintln!("Failed: {e}");
        std::process::exit(1);
    }
}
