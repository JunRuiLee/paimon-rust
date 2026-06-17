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

//! Cross-engine validation dump: emit `(id, sum(<column>))` TSV from a paimon
//! table for byte-by-byte comparison against the Java side
//! (`PaimonReadValidate`). Pair with `compare_id_sum.sh`:
//!
//! ```bash
//! cargo run -p paimon --example validate_id_sum_demo --release -- \
//!     <table_path> --out /tmp/rust.tsv [--id-col id] [--embedding-col embedding] \
//!     [--target-size 2gb] [--read-mode performance|freshness] [--limit N]
//! ```
//!
//! The `<column>` referenced by `--embedding-col` may be:
//! * `FixedSizeList<float|int>` / `List<float|int>` — elements summed as f64.
//! * Scalar `Float32/Float64/Int*` — value used directly (1-element "sum").
//!
//! NULL handling matches the Java side: NULL id rows are skipped; NULL cell or
//! NULL element contributes 0.0. Output values use `{:.17e}` so f64 round-trips
//! through `from_str`.
//!
//! For the broader read benchmark / debugging tool (counting, projecting,
//! filter pushdown timing, multi-table summary), see `read_local_demo.rs`.

use std::time::Instant;

use futures::TryStreamExt;

use paimon::catalog::Identifier;
use paimon::common::{CatalogOptions, Options};
use paimon::{CatalogFactory, DataSplit};

// Optional jemalloc allocator. Enable via:
//   cargo run -p paimon --release --features jemalloc --example validate_id_sum_demo -- ...
// Together with PAIMON_MEM_STATS_INTERVAL_SECS=N a background task prints
// allocator stats every N seconds. Linux-only; the cfg is a no-op elsewhere
// (the `jemalloc` deps in Cargo.toml are gated on target_os = "linux").
#[cfg(all(feature = "jemalloc", target_os = "linux"))]
#[global_allocator]
static GLOBAL: paimon::alloc::Jemalloc = paimon::alloc::Jemalloc;

/// Worker-thread count for tokio runtime AND per-pass split fan-out. Mirrors
/// the cap in `read_local_demo.rs` so behaviour is comparable.
const PARALLELISM: usize = 16;

#[derive(Debug)]
struct Args {
    table_path: String,
    out_path: String,
    id_col: String,
    embedding_col: String,
    /// `read.batch-size` override; 0 = respect persisted/default.
    batch_size: usize,
    /// `source.split.target-size` override; None = respect persisted/default.
    target_size: Option<String>,
    /// `deletion-vectors.read-mode` override; None = respect persisted/default.
    read_mode: Option<String>,
    /// Hard row cap (across all workers). None = unlimited.
    limit: Option<usize>,
}

fn print_usage(argv0: &str) {
    eprintln!(
        "Usage: {argv0} <table_path> --out <tsv_path> \
         [--id-col id] [--embedding-col embedding] \
         [--batch-size N] [--target-size SIZE] \
         [--read-mode performance|freshness] [--limit N]"
    );
    eprintln!("  Emits one TSV row per record: <id>\\t<sum(<col>) as %.17e>\\n");
    eprintln!("  Pair with PaimonReadValidate (Java) + compare_id_sum.sh.");
}

fn parse_args() -> Result<Args, String> {
    let argv: Vec<String> = std::env::args().collect();
    let mut positional: Vec<String> = Vec::new();
    let mut out_path: Option<String> = None;
    let mut id_col: String = "id".to_string();
    let mut embedding_col: String = "embedding".to_string();
    let mut batch_size: usize = 8192;
    let mut target_size: Option<String> = None;
    let mut read_mode: Option<String> = None;
    let mut limit: Option<usize> = None;

    let mut i = 1;
    while i < argv.len() {
        let a = &argv[i];
        match a.as_str() {
            "--out" => {
                i += 1;
                let v = argv
                    .get(i)
                    .ok_or_else(|| "--out needs an output path".to_string())?;
                if v.is_empty() {
                    return Err("--out path is empty".to_string());
                }
                out_path = Some(v.clone());
            }
            "--id-col" => {
                i += 1;
                id_col = argv
                    .get(i)
                    .ok_or_else(|| "--id-col needs a column name".to_string())?
                    .clone();
            }
            "--embedding-col" => {
                i += 1;
                embedding_col = argv
                    .get(i)
                    .ok_or_else(|| "--embedding-col needs a column name".to_string())?
                    .clone();
            }
            "--batch-size" => {
                i += 1;
                let v = argv
                    .get(i)
                    .ok_or_else(|| "--batch-size needs an integer argument".to_string())?;
                batch_size = v
                    .parse::<usize>()
                    .map_err(|_| format!("--batch-size must be a non-negative integer, got: {v}"))?;
            }
            "--target-size" => {
                i += 1;
                let v = argv
                    .get(i)
                    .ok_or_else(|| "--target-size needs an argument (e.g. 2gb)".to_string())?;
                if v.is_empty() {
                    return Err("--target-size value is empty".to_string());
                }
                target_size = Some(v.clone());
            }
            "--read-mode" => {
                i += 1;
                let v = argv.get(i).ok_or_else(|| {
                    "--read-mode needs an argument (performance|freshness)".to_string()
                })?;
                let lower = v.to_ascii_lowercase();
                if lower != "performance" && lower != "freshness" {
                    return Err(format!(
                        "--read-mode must be 'performance' or 'freshness', got: {v}"
                    ));
                }
                read_mode = Some(lower);
            }
            "--limit" => {
                i += 1;
                let v = argv
                    .get(i)
                    .ok_or_else(|| "--limit needs an integer argument".to_string())?;
                limit = Some(
                    v.parse::<usize>()
                        .map_err(|_| format!("--limit must be a non-negative integer, got: {v}"))?,
                );
            }
            _ => positional.push(a.clone()),
        }
        i += 1;
    }

    if positional.len() != 1 {
        return Err(format!(
            "expected exactly one positional argument <table_path>, got: {:?}",
            positional
        ));
    }
    let out_path = out_path.ok_or_else(|| "--out is required".to_string())?;
    Ok(Args {
        table_path: positional.into_iter().next().unwrap(),
        out_path,
        id_col,
        embedding_col,
        batch_size,
        target_size,
        read_mode,
        limit,
    })
}

/// Split `<table_path>` into (warehouse, database, table). Same rules as
/// `read_local_demo.rs::split_table_path`.
fn split_table_path(path: &str) -> Result<(String, String, String), String> {
    let trimmed = path.trim_end_matches('/');
    if trimmed.is_empty() {
        return Err(format!("invalid table_path: {path}"));
    }
    let (rest, table) = trimmed
        .rsplit_once('/')
        .ok_or_else(|| format!("table_path has no parent (db dir): {path}"))?;
    if table.is_empty() {
        return Err(format!("empty table name in: {path}"));
    }
    let (warehouse, db_dir) = rest
        .rsplit_once('/')
        .ok_or_else(|| format!("table_path's db dir has no parent (warehouse): {path}"))?;
    let db = db_dir
        .strip_suffix(".db")
        .ok_or_else(|| format!("expected db directory ending in '.db', got: {db_dir}"))?;
    if db.is_empty() {
        return Err(format!("empty db name parsed from: {path}"));
    }
    if warehouse.is_empty() {
        return Err(format!("empty warehouse parsed from: {path}"));
    }
    Ok((warehouse.to_string(), db.to_string(), table.to_string()))
}

/// Write `(id, sum(<col>))` TSV rows for one record batch into `out`.
///
/// `id_idx` / `col_idx` are positions in the projected batch (this demo always
/// projects `[id_col, embedding_col]` so they're 0/1).
///
/// Behaviour matches `PaimonReadValidate`:
/// * id NULL → skip the row.
/// * cell NULL → emit `id\t0.0e0`.
/// * For ARRAY columns, NULL element contributes 0.0.
/// * Sum is `f64`; printed as `{:.17e}` (full double round-trip).
fn emit_id_sum_batch<W: std::io::Write>(
    batch: &arrow_array::RecordBatch,
    id_idx: usize,
    col_idx: usize,
    out: &mut W,
    scratch: &mut String,
) -> Result<usize, String> {
    use arrow_array::*;
    use std::fmt::Write as _;

    let n = batch.num_rows();
    if n == 0 {
        return Ok(0);
    }
    let id_col = batch.column(id_idx).as_ref();
    let val_col = batch.column(col_idx).as_ref();

    enum IdKind<'a> {
        I32(&'a Int32Array),
        I64(&'a Int64Array),
        I16(&'a Int16Array),
        I8(&'a Int8Array),
    }
    let id_kind = if let Some(a) = id_col.as_any().downcast_ref::<Int64Array>() {
        IdKind::I64(a)
    } else if let Some(a) = id_col.as_any().downcast_ref::<Int32Array>() {
        IdKind::I32(a)
    } else if let Some(a) = id_col.as_any().downcast_ref::<Int16Array>() {
        IdKind::I16(a)
    } else if let Some(a) = id_col.as_any().downcast_ref::<Int8Array>() {
        IdKind::I8(a)
    } else {
        return Err(format!(
            "validate-id-sum: unsupported id column type {:?}",
            id_col.data_type()
        ));
    };

    let fixed_emb = val_col.as_any().downcast_ref::<FixedSizeListArray>();
    let list_emb = val_col.as_any().downcast_ref::<ListArray>();
    let scalar_kind = scalar_kind_for(val_col);
    if fixed_emb.is_none() && list_emb.is_none() && scalar_kind.is_none() {
        return Err(format!(
            "validate-id-sum: column must be FixedSizeList/List of \
             float|int, or a scalar float|int; got {:?}",
            val_col.data_type()
        ));
    }

    let mut written = 0usize;
    for row in 0..n {
        let id_str: String = match &id_kind {
            IdKind::I64(a) => {
                if a.is_null(row) {
                    continue;
                }
                a.value(row).to_string()
            }
            IdKind::I32(a) => {
                if a.is_null(row) {
                    continue;
                }
                a.value(row).to_string()
            }
            IdKind::I16(a) => {
                if a.is_null(row) {
                    continue;
                }
                a.value(row).to_string()
            }
            IdKind::I8(a) => {
                if a.is_null(row) {
                    continue;
                }
                a.value(row).to_string()
            }
        };

        let sum: f64 = if val_col.is_null(row) {
            0.0
        } else if let Some(fa) = fixed_emb {
            let elem = fa.value(row);
            sum_array_as_f64(elem.as_ref())?
        } else if let Some(la) = list_emb {
            let elem = la.value(row);
            sum_array_as_f64(elem.as_ref())?
        } else {
            scalar_value_as_f64(val_col, row, scalar_kind.unwrap())
        };

        scratch.clear();
        write!(scratch, "{}\t{:.17e}\n", id_str, sum)
            .map_err(|e| format!("validate-id-sum: format failed: {e}"))?;
        out.write_all(scratch.as_bytes())
            .map_err(|e| format!("validate-id-sum: write failed: {e}"))?;
        written += 1;
    }
    Ok(written)
}

fn sum_array_as_f64(arr: &dyn arrow_array::Array) -> Result<f64, String> {
    use arrow_array::*;
    let any = arr.as_any();
    let n = arr.len();
    if let Some(a) = any.downcast_ref::<Float32Array>() {
        let mut s = 0.0f64;
        for i in 0..n {
            if !a.is_null(i) {
                s += a.value(i) as f64;
            }
        }
        return Ok(s);
    }
    if let Some(a) = any.downcast_ref::<Float64Array>() {
        let mut s = 0.0f64;
        for i in 0..n {
            if !a.is_null(i) {
                s += a.value(i);
            }
        }
        return Ok(s);
    }
    if let Some(a) = any.downcast_ref::<Int8Array>() {
        let mut s = 0.0f64;
        for i in 0..n {
            if !a.is_null(i) {
                s += a.value(i) as f64;
            }
        }
        return Ok(s);
    }
    if let Some(a) = any.downcast_ref::<Int16Array>() {
        let mut s = 0.0f64;
        for i in 0..n {
            if !a.is_null(i) {
                s += a.value(i) as f64;
            }
        }
        return Ok(s);
    }
    if let Some(a) = any.downcast_ref::<Int32Array>() {
        let mut s = 0.0f64;
        for i in 0..n {
            if !a.is_null(i) {
                s += a.value(i) as f64;
            }
        }
        return Ok(s);
    }
    if let Some(a) = any.downcast_ref::<Int64Array>() {
        let mut s = 0.0f64;
        for i in 0..n {
            if !a.is_null(i) {
                s += a.value(i) as f64;
            }
        }
        return Ok(s);
    }
    Err(format!(
        "validate-id-sum: unsupported element type {:?}",
        arr.data_type()
    ))
}

#[derive(Clone, Copy)]
enum ScalarKind {
    F32,
    F64,
    I8,
    I16,
    I32,
    I64,
}

fn scalar_kind_for(arr: &dyn arrow_array::Array) -> Option<ScalarKind> {
    use arrow_array::*;
    let any = arr.as_any();
    if any.downcast_ref::<Float32Array>().is_some() {
        Some(ScalarKind::F32)
    } else if any.downcast_ref::<Float64Array>().is_some() {
        Some(ScalarKind::F64)
    } else if any.downcast_ref::<Int8Array>().is_some() {
        Some(ScalarKind::I8)
    } else if any.downcast_ref::<Int16Array>().is_some() {
        Some(ScalarKind::I16)
    } else if any.downcast_ref::<Int32Array>().is_some() {
        Some(ScalarKind::I32)
    } else if any.downcast_ref::<Int64Array>().is_some() {
        Some(ScalarKind::I64)
    } else {
        None
    }
}

fn scalar_value_as_f64(arr: &dyn arrow_array::Array, row: usize, kind: ScalarKind) -> f64 {
    use arrow_array::*;
    let any = arr.as_any();
    match kind {
        ScalarKind::F32 => any.downcast_ref::<Float32Array>().unwrap().value(row) as f64,
        ScalarKind::F64 => any.downcast_ref::<Float64Array>().unwrap().value(row),
        ScalarKind::I8 => any.downcast_ref::<Int8Array>().unwrap().value(row) as f64,
        ScalarKind::I16 => any.downcast_ref::<Int16Array>().unwrap().value(row) as f64,
        ScalarKind::I32 => any.downcast_ref::<Int32Array>().unwrap().value(row) as f64,
        ScalarKind::I64 => any.downcast_ref::<Int64Array>().unwrap().value(row) as f64,
    }
}

fn concat_shards(out_path: &str, parallelism: usize) -> Result<(), String> {
    use std::io::{BufReader, Read, Write};

    let mut out = std::fs::File::create(out_path)
        .map_err(|e| format!("validate-id-sum: open {out_path}: {e}"))?;
    let mut buf = vec![0u8; 1 << 20];
    for i in 0..parallelism {
        let shard = format!("{out_path}.shard-{i}");
        let f = match std::fs::File::open(&shard) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => return Err(format!("validate-id-sum: open {shard}: {e}")),
        };
        let mut r = BufReader::new(f);
        loop {
            let n = r
                .read(&mut buf)
                .map_err(|e| format!("validate-id-sum: read {shard}: {e}"))?;
            if n == 0 {
                break;
            }
            out.write_all(&buf[..n])
                .map_err(|e| format!("validate-id-sum: write {out_path}: {e}"))?;
        }
        let _ = std::fs::remove_file(&shard);
    }
    Ok(())
}

async fn run(args: &Args) -> Result<(), String> {
    let (warehouse, db, tbl) =
        split_table_path(&args.table_path).map_err(|e| format!("{}: {e}", args.table_path))?;
    println!("warehouse={warehouse} db={db} table={tbl}");

    let mut options = Options::new();
    options.set(CatalogOptions::METASTORE, "filesystem");
    options.set(CatalogOptions::WAREHOUSE, &warehouse);
    let catalog = CatalogFactory::create(options)
        .await
        .map_err(|e| format!("failed to create FileSystemCatalog: {e}"))?;

    let table = catalog
        .get_table(&Identifier::new(&db, &tbl))
        .await
        .map_err(|e| format!("get_table: {e}"))?;

    // Per-run option overrides via `Table::copy_with_options` (not persisted).
    let table = {
        let mut extra = std::collections::HashMap::new();
        if args.batch_size > 0 {
            extra.insert("read.batch-size".to_string(), args.batch_size.to_string());
        }
        if let Some(ref ts) = args.target_size {
            extra.insert("source.split.target-size".to_string(), ts.clone());
        }
        if let Some(ref rm) = args.read_mode {
            extra.insert("deletion-vectors.read-mode".to_string(), rm.clone());
        }
        if extra.is_empty() {
            table
        } else {
            table.copy_with_options(extra)
        }
    };

    // Force projection to [id, embedding] so worker batches always have id at
    // pos 0 and the validation column at pos 1.
    let projection = vec![args.id_col.clone(), args.embedding_col.clone()];

    let t_plan = Instant::now();
    let mut read_builder = table.new_read_builder();
    let proj_refs: Vec<&str> = projection.iter().map(String::as_str).collect();
    read_builder.with_projection(&proj_refs);
    if let Some(lim) = args.limit {
        read_builder.with_limit(lim);
    }
    let scan = read_builder.new_scan();
    let plan = scan.plan().await.map_err(|e| format!("plan: {e}"))?;
    let splits: Vec<DataSplit> = plan.splits().to_vec();
    let plan_ms = t_plan.elapsed().as_millis();
    println!(
        "plan: splits={} plan_ms={} id={} col={} out={}",
        splits.len(),
        plan_ms,
        args.id_col,
        args.embedding_col,
        args.out_path,
    );

    if splits.is_empty() {
        // Still produce a 0-byte output so downstream tooling doesn't blow up.
        std::fs::File::create(&args.out_path)
            .map_err(|e| format!("create empty out: {e}"))?;
        println!("done: rows=0 plan_ms={plan_ms} drain_ms=0 out={}", args.out_path);
        return Ok(());
    }

    let parallelism = PARALLELISM.min(splits.len());
    let mut chunks: Vec<Vec<DataSplit>> = (0..parallelism).map(|_| Vec::new()).collect();
    for (i, s) in splits.into_iter().enumerate() {
        chunks[i % parallelism].push(s);
    }

    let t_drain = Instant::now();
    let mut handles = Vec::with_capacity(parallelism);
    for (worker_idx, chunk) in chunks.into_iter().enumerate() {
        let table = table.clone();
        let projection = projection.clone();
        let limit = args.limit;
        let shard_path = format!("{}.shard-{worker_idx}", args.out_path);
        handles.push(tokio::spawn(async move {
            let mut rb = table.new_read_builder();
            let refs: Vec<&str> = projection.iter().map(String::as_str).collect();
            rb.with_projection(&refs);
            if let Some(lim) = limit {
                rb.with_limit(lim);
            }
            let read = rb
                .new_read()
                .map_err(|e| format!("new_read: {e}"))?;
            let mut stream = read
                .to_arrow(&chunk)
                .map_err(|e| format!("to_arrow: {e}"))?;

            let f = std::fs::File::create(&shard_path)
                .map_err(|e| format!("open {shard_path}: {e}"))?;
            let mut writer = std::io::BufWriter::with_capacity(1 << 20, f);
            let mut scratch = String::with_capacity(64);
            let mut rows = 0usize;
            let mut batches = 0usize;

            while let Some(batch) = stream
                .try_next()
                .await
                .map_err(|e| format!("stream: {e}"))?
            {
                let written = emit_id_sum_batch(&batch, 0, 1, &mut writer, &mut scratch)?;
                rows += written;
                batches += 1;
            }
            use std::io::Write as _;
            writer
                .flush()
                .map_err(|e| format!("flush {shard_path}: {e}"))?;
            Ok::<(usize, usize), String>((rows, batches))
        }));
    }

    let mut total_rows = 0usize;
    let mut total_batches = 0usize;
    for h in handles {
        match h.await {
            Ok(Ok((rows, batches))) => {
                total_rows += rows;
                total_batches += batches;
            }
            Ok(Err(e)) => return Err(e),
            Err(e) => return Err(format!("drain task join error: {e}")),
        }
    }
    let drain_ms = t_drain.elapsed().as_millis();

    concat_shards(&args.out_path, parallelism)?;

    println!(
        "done: rows={} batches={} plan_ms={} drain_ms={} out={}",
        total_rows, total_batches, plan_ms, drain_ms, args.out_path,
    );
    Ok(())
}

#[tokio::main(worker_threads = 16)]
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
    paimon::alloc::print_stats("startup");
    let _stats_task = spawn_periodic_mem_stats();

    if let Err(e) = run(&args).await {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
    paimon::alloc::print_stats("end");
}

/// If `PAIMON_MEM_STATS_INTERVAL_SECS` is set to a positive integer, spawn a
/// detached task that prints allocator stats every N seconds. Returns a guard
/// whose Drop aborts the task when main returns. No-op on parse failures or
/// when the env var is unset / zero.
fn spawn_periodic_mem_stats() -> Option<tokio::task::JoinHandle<()>> {
    let secs: u64 = std::env::var("PAIMON_MEM_STATS_INTERVAL_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    if secs == 0 {
        return None;
    }
    Some(tokio::spawn(async move {
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(secs));
        // Skip the immediate first tick — startup stats are already printed.
        tick.tick().await;
        let mut i: u64 = 0;
        loop {
            tick.tick().await;
            paimon::alloc::print_stats(&format!("periodic[{i}]"));
            i += 1;
        }
    }))
}
