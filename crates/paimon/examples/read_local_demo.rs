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

//! Example: read a paimon table from a filesystem-style path (local / HDFS /
//! viewfs / S3 / OSS — anything `FileSystemCatalog` + opendal can resolve).
//!
//! Modeled on `paimon-cpp/examples/read_hdfs_demo.cpp` but adapted to the Rust
//! `Catalog → Table → ReadBuilder → Scan/Read` API. The Rust API exposes a
//! streaming `RecordBatch` reader, so we drop the C++ demo's worker-thread
//! Executor and Doris-style accumulator and just iterate the stream.
//!
//! # Path layout
//!
//! `<table_path>` is the directory of a paimon table. Accepted forms:
//! * Local absolute path: `/tmp/warehouse/mydb.db/users`
//! * HDFS / viewfs URI: `hdfs://nn:8020/user/me/warehouse/mydb.db/users`
//! * S3 / OSS URI: `s3://bucket/warehouse/mydb.db/users`
//!
//! The demo splits the path into warehouse, database (last `.db`-suffixed
//! segment, with the suffix stripped), and table (final segment).
//!
//! # Building & running on HDFS
//!
//! `paimon-rust`'s default features only enable local FS / OSS / memory.
//! For HDFS add the `storage-hdfs` feature:
//! ```bash
//! cargo run -p paimon --example read_local_demo --release \
//!     --features storage-hdfs -- \
//!     hdfs://nn:8020/user/me/warehouse/mydb.db/users
//! ```
//! Authentication / cluster config flows through opendal's hdfs-native client;
//! set `HADOOP_CONF_DIR`, `HADOOP_HOME` etc. in the environment per the
//! cluster's normal client setup. For Kerberos: `kinit` first, or wire a
//! ticket-cache path via opendal's hdfs config.
//!
//! # Usage
//! ```bash
//! cargo run -p paimon --example read_local_demo -- <table_path> \
//!     [--collect] [--count|-c] [--repeat N] [--limit N] [--columns col1,col2,...]
//! ```
//!
//! # Modes
//! * default / `--count` / `-c`: drain the stream, sum `num_rows()`. No
//!   per-batch retention. Cheapest read-pipeline measurement.
//! * `--collect`: retain every `RecordBatch` and pretty-print the first few
//!   rows of each. Full memory.

use std::time::Instant;

use futures::TryStreamExt;

use paimon::catalog::Identifier;
use paimon::common::{CatalogOptions, Options};
use paimon::spec::{Datum, Predicate, PredicateBuilder};
use paimon::{Catalog, CatalogFactory, DataSplit};

// Optional jemalloc allocator. Enable via:
//   cargo run -p paimon --release --features jemalloc --example read_local_demo -- ...
// Together with PAIMON_MEM_STATS_INTERVAL_SECS=N a background task prints
// allocator stats every N seconds. Linux-only; the cfg is a no-op elsewhere
// (the `jemalloc` deps in Cargo.toml are gated on target_os = "linux").
#[cfg(all(feature = "jemalloc", target_os = "linux"))]
#[global_allocator]
static GLOBAL: paimon::alloc::Jemalloc = paimon::alloc::Jemalloc;

/// Default worker-thread count for the tokio runtime AND the per-pass split
/// fan-out. Overridable via `--parallelism N` (which sets both consistently).
///
/// Underlying `KeyValueFileReader::read` / `DataFileReader::read` are
/// `try_stream! { for split in splits { ... } }` — strictly sequential per
/// stream. To parallelize across splits we partition `Plan.splits()` into N
/// chunks, build N independent `TableRead`s, and `tokio::spawn` each chunk.
/// Tokio's `worker_threads` is set to the same N so we don't oversubscribe.
const DEFAULT_PARALLELISM: usize = 16;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    /// Pure row count — drain the stream and read `batch.num_rows()` only,
    /// no per-cell access. Cheapest path; mirrors the C++ demo's `--count`.
    Count,
    /// Default. Touch every cell via `consume_batch` to pay the realistic
    /// downstream-consumer cost (analogous to the C++ demo's Doris-sim mode).
    Consume,
    /// Retain every batch and pretty-print the first few rows. Full memory.
    Collect,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FilterFieldType {
    Int,
    BigInt,
    Double,
    String,
}

/// One leaf comparison from a `--filter` argument.
#[derive(Clone, Debug)]
struct FilterArg {
    field: String,
    field_type: FilterFieldType,
    op: String,
    raw_value: String,
}

#[derive(Debug)]
struct Args {
    table_paths: Vec<String>,
    mode: Mode,
    repeat: usize,
    limit: Option<usize>,
    columns: Option<Vec<String>>,
    filters: Vec<FilterArg>,
    /// `read.batch-size` for this run only. Applied via
    /// `Table::copy_with_options` so the catalog/schema is not mutated.
    /// Default 8192 preserves the demo's prior throughput when the
    /// option-plumbing change dropped the library default to 1024 (Java
    /// parity). Use `0` to respect whatever the table already has persisted.
    batch_size: usize,
    /// `source.split.target-size` for this run only. Applied via
    /// `Table::copy_with_options` so the catalog/schema is not mutated.
    /// `None` respects whatever the table has persisted (lib default
    /// 128 MiB). Accepts the same string format as the option itself
    /// (`"2gb"`, `"128mb"`, raw bytes, etc.).
    target_size: Option<String>,
    /// `deletion-vectors.read-mode` for this run only. Applied via
    /// `Table::copy_with_options` so the catalog/schema is not mutated.
    /// `None` respects whatever the table has persisted (lib default
    /// PERFORMANCE). Accepted values: `"performance"` / `"freshness"`
    /// (case-insensitive — parsed by `core_options.deletion_vectors_read_mode`).
    /// Only meaningful for tables with `deletion-vectors.enabled=true`.
    read_mode: Option<String>,
    /// `scan.manifest-parallelism` for this run only. Applied via
    /// `Table::copy_with_options` so the catalog/schema is not mutated.
    /// `None` respects whatever the table has persisted (lib default 64,
    /// matching the previously hardcoded value).
    manifest_parallelism: Option<usize>,
    /// `read.sort-merge-buffer-soft-cap` for this run only. Applied via
    /// `Table::copy_with_options` so the catalog/schema is not mutated.
    /// `None` respects whatever the table has persisted (lib default unset =
    /// no cap). Caps the sort-merge `batch_buffer` length: when reached, the
    /// merge loop forces an early flush, bounding peak memory at the cost of
    /// more (smaller) output batches. Only meaningful for PK tables.
    sort_merge_soft_cap: Option<usize>,
    /// Per-pass split fan-out AND tokio runtime worker_threads. Defaults to
    /// `DEFAULT_PARALLELISM`. Lower values cap concurrent parquet decoders
    /// and the in-flight column-chunk buffer they hold — directly trades
    /// throughput for peak RSS on row-groups-with-wide-columns workloads
    /// (e.g. embedding tables).
    parallelism: usize,
}

/// Stat for one read pass. `residual_dropped` is the number of rows that
/// paimon-rust returned but were filtered out by the demo's residual evaluator
/// — non-zero only when `is_exact_filter_pushdown == false`.
#[derive(Default, Debug, Clone)]
struct RunStats {
    plan_ms: u128,
    drain_ms: u128,
    emit_ms: u128,
    rows: usize,
    batches: usize,
    splits: usize,
    /// Wrapping checksum over every cell value in every batch. Forces actual
    /// column-data access so the benchmark measures decode + materialise, not
    /// just metadata. Printed at the end of each run so the optimiser can't
    /// dead-code-eliminate the consume loop.
    checksum: u64,
    /// Rows dropped by the demo-side residual filter. Stays 0 when paimon
    /// guarantees exact filter pushdown for the predicate.
    residual_dropped: usize,
    /// Whether paimon's filter pushdown is exact for this run's predicate.
    /// `true` when there's no filter or all conjuncts hit the parquet row
    /// filter; `false` means the demo had to re-filter on top.
    exact_pushdown: bool,
}

fn print_usage(argv0: &str) {
    eprintln!(
        "Usage: {argv0} <table_path> [<table_path> ...] [--collect] [--count|-c] \
         [--repeat N] [--limit N] [--columns col1,col2,...] \
         [--filter <field>:<TYPE><op><value>]... [--batch-size N]"
    );
    eprintln!("  <table_path>: one or more, e.g. /tmp/warehouse/mydb.db/users");
    eprintln!("  default mode is consume (touch every cell);");
    eprintln!("  --count/-c skips per-cell access (cheap row-count benchmark);");
    eprintln!("  --collect retains and prints batches.");
    eprintln!("  --filter syntax: <field>:<TYPE><op><value>");
    eprintln!("    <TYPE> ∈ INT|BIGINT|DOUBLE|STRING; <op> ∈ =, !=, <, <=, >, >=");
    eprintln!("    repeatable; multiple filters AND-combined.");
    eprintln!("  --batch-size N: per-run `read.batch-size=N` override applied via");
    eprintln!("    `Table::copy_with_options` (does NOT mutate the catalog).");
    eprintln!("    Default 8192 (matches the demo's prior hardcoded perf baseline).");
    eprintln!("    Pass 0 to respect the persisted option (lib default 1024).");
    eprintln!("  --target-size SIZE: per-run `source.split.target-size=SIZE` override");
    eprintln!("    applied via `Table::copy_with_options` (does NOT mutate the");
    eprintln!("    catalog). Accepts \"2gb\", \"128mb\", raw bytes, etc.");
    eprintln!("    Default unset → respects the persisted option (lib default 128 MiB).");
    eprintln!("  --read-mode {{performance|freshness}}: per-run override of");
    eprintln!("    `deletion-vectors.read-mode` for DV-enabled tables. Default unset →");
    eprintln!("    respects the persisted option (lib default PERFORMANCE).");
    eprintln!("    Has no effect on non-DV tables. Use to A/B both modes against the");
    eprintln!("    same table and confirm they return identical results.");
    eprintln!("  --manifest-parallelism N: per-run `scan.manifest-parallelism=N` override");
    eprintln!("    applied via `Table::copy_with_options`. Caps the in-flight manifest");
    eprintln!("    fetch concurrency during scan planning (clamped to [1, 1024] by");
    eprintln!("    paimon-core). Default unset → uses lib default 64.");
    eprintln!("  --sort-merge-soft-cap N: per-run `read.sort-merge-buffer-soft-cap=N`");
    eprintln!("    override applied via `Table::copy_with_options`. Forces the sort-merge");
    eprintln!("    `batch_buffer` to flush once it reaches N entries (PK tables only).");
    eprintln!("    Lower N bounds peak buffer memory, at the cost of more, smaller output");
    eprintln!("    batches. Default unset → uses persisted value (lib default no cap).");
    eprintln!("  --parallelism N: per-pass split fan-out AND tokio worker_threads.");
    eprintln!("    Default {DEFAULT_PARALLELISM}. Lower values bound peak RSS at the cost");
    eprintln!("    of throughput — each concurrent split holds an in-flight parquet");
    eprintln!("    column-chunk buffer (can be GiB-scale on embedding columns).");
}

fn parse_args() -> Result<Args, String> {
    let argv: Vec<String> = std::env::args().collect();
    let mut positional: Vec<String> = Vec::new();
    let mut mode = Mode::Consume;
    let mut repeat: usize = 1;
    let mut limit: Option<usize> = None;
    let mut columns: Option<Vec<String>> = None;
    let mut filters_raw: Vec<String> = Vec::new();
    let mut batch_size: usize = 8192;
    let mut target_size: Option<String> = None;
    let mut read_mode: Option<String> = None;
    let mut manifest_parallelism: Option<usize> = None;
    let mut sort_merge_soft_cap: Option<usize> = None;
    let mut parallelism: usize = DEFAULT_PARALLELISM;

    let mut i = 1;
    while i < argv.len() {
        let a = &argv[i];
        match a.as_str() {
            "--collect" => mode = Mode::Collect,
            "--count" | "-c" => mode = Mode::Count,
            "--repeat" => {
                i += 1;
                let v = argv
                    .get(i)
                    .ok_or_else(|| "--repeat needs an integer argument".to_string())?;
                repeat = v
                    .parse::<usize>()
                    .map_err(|_| format!("--repeat must be a positive integer, got: {v}"))?;
                if repeat == 0 {
                    return Err("--repeat must be > 0".to_string());
                }
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
            "--columns" => {
                i += 1;
                let v = argv
                    .get(i)
                    .ok_or_else(|| "--columns needs a comma-separated argument".to_string())?;
                let parts: Vec<String> = v
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect();
                if parts.is_empty() {
                    return Err("--columns is empty".to_string());
                }
                columns = Some(parts);
            }
            "--filter" => {
                i += 1;
                let v = argv
                    .get(i)
                    .ok_or_else(|| "--filter needs an argument (field:TYPE<op>value)".to_string())?;
                filters_raw.push(v.clone());
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
                let v = argv
                    .get(i)
                    .ok_or_else(|| {
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
            "--manifest-parallelism" => {
                i += 1;
                let v = argv
                    .get(i)
                    .ok_or_else(|| "--manifest-parallelism needs an integer argument".to_string())?;
                let n = v
                    .parse::<usize>()
                    .map_err(|_| {
                        format!("--manifest-parallelism must be a positive integer, got: {v}")
                    })?;
                if n == 0 {
                    return Err("--manifest-parallelism must be > 0".to_string());
                }
                manifest_parallelism = Some(n);
            }
            "--sort-merge-soft-cap" => {
                i += 1;
                let v = argv.get(i).ok_or_else(|| {
                    "--sort-merge-soft-cap needs an integer argument".to_string()
                })?;
                let n = v.parse::<usize>().map_err(|_| {
                    format!("--sort-merge-soft-cap must be a positive integer, got: {v}")
                })?;
                if n == 0 {
                    return Err("--sort-merge-soft-cap must be > 0".to_string());
                }
                sort_merge_soft_cap = Some(n);
            }
            "--parallelism" => {
                i += 1;
                let v = argv
                    .get(i)
                    .ok_or_else(|| "--parallelism needs an integer argument".to_string())?;
                let n = v
                    .parse::<usize>()
                    .map_err(|_| format!("--parallelism must be a positive integer, got: {v}"))?;
                if n == 0 {
                    return Err("--parallelism must be > 0".to_string());
                }
                parallelism = n;
            }
            _ => positional.push(a.clone()),
        }
        i += 1;
    }

    if positional.is_empty() {
        return Err("expected at least one positional argument: <table_path>".to_string());
    }

    let mut filters: Vec<FilterArg> = Vec::with_capacity(filters_raw.len());
    for s in &filters_raw {
        filters.push(parse_filter(s)?);
    }

    Ok(Args {
        table_paths: positional,
        mode,
        repeat,
        limit,
        columns,
        filters,
        batch_size,
        target_size,
        read_mode,
        manifest_parallelism,
        sort_merge_soft_cap,
        parallelism,
    })
}

/// Parse `<field>:<TYPE><op><value>` — pick the leftmost op among
/// `{!=, <=, >=, =, <, >}`. Multi-char ops are checked before their
/// single-char prefixes (e.g. `!=` before `=`) at the same position.
/// Mirrors `ParseFilter` in `paimon-cpp/examples/read_hdfs_demo.cpp`.
fn parse_filter(s: &str) -> Result<FilterArg, String> {
    let colon = s
        .find(':')
        .ok_or_else(|| format!("filter must contain ':' (field:TYPE<op>value): {s}"))?;
    let field = s[..colon].to_string();
    if field.is_empty() {
        return Err(format!("filter has empty field name: {s}"));
    }
    let rest = &s[colon + 1..];

    const OPS: &[&str] = &["!=", "<=", ">=", "=", "<", ">"];
    let mut op_pos: Option<usize> = None;
    let mut op_len: usize = 0;
    let mut chosen_op: &str = "";
    for op in OPS {
        if let Some(p) = rest.find(op) {
            let take = match op_pos {
                None => true,
                Some(cur) if p < cur => true,
                Some(cur) if p == cur && op.len() > op_len => true,
                _ => false,
            };
            if take {
                op_pos = Some(p);
                op_len = op.len();
                chosen_op = op;
            }
        }
    }
    let op_pos = op_pos
        .ok_or_else(|| format!("filter missing operator (=, !=, <, <=, >, >=): {s}"))?;

    let type_str = &rest[..op_pos];
    let field_type = match type_str {
        "INT" => FilterFieldType::Int,
        "BIGINT" => FilterFieldType::BigInt,
        "DOUBLE" => FilterFieldType::Double,
        "STRING" => FilterFieldType::String,
        other => {
            return Err(format!(
                "unsupported filter type '{other}' (expected INT|BIGINT|DOUBLE|STRING)"
            ));
        }
    };

    let raw_value = rest[op_pos + op_len..].to_string();
    if raw_value.is_empty() {
        return Err(format!("filter has empty value: {s}"));
    }
    Ok(FilterArg {
        field,
        field_type,
        op: chosen_op.to_string(),
        raw_value,
    })
}

/// Convert a `FilterArg`'s raw string value to the typed `Datum` literal that
/// `PredicateBuilder` consumes.
fn filter_literal(f: &FilterArg) -> Result<Datum, String> {
    match f.field_type {
        FilterFieldType::Int => f
            .raw_value
            .parse::<i32>()
            .map(Datum::Int)
            .map_err(|e| format!("failed to parse INT '{}': {e}", f.raw_value)),
        FilterFieldType::BigInt => f
            .raw_value
            .parse::<i64>()
            .map(Datum::Long)
            .map_err(|e| format!("failed to parse BIGINT '{}': {e}", f.raw_value)),
        FilterFieldType::Double => f
            .raw_value
            .parse::<f64>()
            .map(Datum::Double)
            .map_err(|e| format!("failed to parse DOUBLE '{}': {e}", f.raw_value)),
        FilterFieldType::String => Ok(Datum::String(f.raw_value.clone())),
    }
}

/// Build a single leaf predicate from a parsed `FilterArg` and the table's
/// `PredicateBuilder` (constructed against the full table schema fields, so
/// any field name resolves correctly).
fn build_leaf(builder: &PredicateBuilder, f: &FilterArg) -> Result<Predicate, String> {
    let lit = filter_literal(f)?;
    let r = match f.op.as_str() {
        "=" => builder.equal(&f.field, lit),
        "!=" => builder.not_equal(&f.field, lit),
        "<" => builder.less_than(&f.field, lit),
        "<=" => builder.less_or_equal(&f.field, lit),
        ">" => builder.greater_than(&f.field, lit),
        ">=" => builder.greater_or_equal(&f.field, lit),
        other => return Err(format!("unsupported op: {other}")),
    };
    r.map_err(|e| format!("filter '{}' rejected: {e}", f.field))
}

/// Demo-side residual filter: evaluate `filters` against `batch` and return
/// the surviving sub-batch. Only invoked when paimon-rust signals that its
/// pushdown is non-exact for the predicate (`is_exact_filter_pushdown ==
/// false`); otherwise paimon already guarantees exact row-level filtering and
/// the residual pass would be wasteful.
///
/// Errors out if a filter field is missing from the batch or its column type
/// doesn't match the declared `FilterFieldType` — both indicate caller bug
/// (`--columns` excluded the field, or filter type was wrong).
fn apply_residual_filter(
    batch: &arrow_array::RecordBatch,
    filters: &[FilterArg],
) -> Result<arrow_array::RecordBatch, String> {
    use arrow_array::*;

    let n = batch.num_rows();
    let mut acc = vec![true; n];

    for f in filters {
        let Some((col_idx, _)) = batch.schema().column_with_name(&f.field) else {
            return Err(format!(
                "residual filter: field '{}' not in batch (was it excluded by --columns?)",
                f.field
            ));
        };
        let col = batch.column(col_idx);
        match f.field_type {
            FilterFieldType::Int => {
                let arr = col
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .ok_or_else(|| format!("column '{}' is not INT", f.field))?;
                let v: i32 = f
                    .raw_value
                    .parse()
                    .map_err(|e| format!("residual filter parse INT: {e}"))?;
                for i in 0..n {
                    if !acc[i] {
                        continue;
                    }
                    let row = arr.value(i);
                    acc[i] = compare_op(&f.op, row, v);
                }
            }
            FilterFieldType::BigInt => {
                let arr = col
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| format!("column '{}' is not BIGINT", f.field))?;
                let v: i64 = f
                    .raw_value
                    .parse()
                    .map_err(|e| format!("residual filter parse BIGINT: {e}"))?;
                for i in 0..n {
                    if !acc[i] {
                        continue;
                    }
                    let row = arr.value(i);
                    acc[i] = compare_op(&f.op, row, v);
                }
            }
            FilterFieldType::Double => {
                let arr = col
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| format!("column '{}' is not DOUBLE", f.field))?;
                let v: f64 = f
                    .raw_value
                    .parse()
                    .map_err(|e| format!("residual filter parse DOUBLE: {e}"))?;
                for i in 0..n {
                    if !acc[i] {
                        continue;
                    }
                    let row = arr.value(i);
                    acc[i] = compare_op(&f.op, row, v);
                }
            }
            FilterFieldType::String => {
                let arr = col
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| format!("column '{}' is not STRING", f.field))?;
                let v: &str = &f.raw_value;
                for i in 0..n {
                    if !acc[i] {
                        continue;
                    }
                    let row = arr.value(i);
                    acc[i] = compare_op(&f.op, row, v);
                }
            }
        }
    }

    let mask = BooleanArray::from(acc);
    arrow_select::filter::filter_record_batch(batch, &mask)
        .map_err(|e| format!("residual filter apply: {e}"))
}

/// Generic comparison helper for the 6 supported ops. `T: PartialOrd` covers
/// numeric primitives and `&str` (the demo's filter types).
fn compare_op<T: PartialOrd>(op: &str, lhs: T, rhs: T) -> bool {
    match op {
        "=" => lhs == rhs,
        "!=" => lhs != rhs,
        "<" => lhs < rhs,
        "<=" => lhs <= rhs,
        ">" => lhs > rhs,
        ">=" => lhs >= rhs,
        _ => false,
    }
}

/// Split `<table_path>` into (warehouse, database, table).
///
/// `path` is expected to look like `[…]/<warehouse>/<db>.db/<table>`. We peel
/// off the last two `/`-separated segments via byte-level `rsplit_once`,
/// which keeps URI prefixes (`hdfs://nn:8020`, `viewfs://`, `s3://bucket`)
/// intact in the warehouse string — `std::path::Path` would mangle the `://`.
/// The penultimate segment must end with `.db` per paimon's filesystem layout.
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

#[cfg(test)]
mod split_table_path_tests {
    use super::split_table_path;

    #[test]
    fn local_absolute() {
        let (w, d, t) = split_table_path("/tmp/warehouse/mydb.db/users").unwrap();
        assert_eq!(w, "/tmp/warehouse");
        assert_eq!(d, "mydb");
        assert_eq!(t, "users");
    }

    #[test]
    fn hdfs_uri() {
        let (w, d, t) =
            split_table_path("hdfs://nn:8020/user/me/wh/mydb.db/users").unwrap();
        assert_eq!(w, "hdfs://nn:8020/user/me/wh");
        assert_eq!(d, "mydb");
        assert_eq!(t, "users");
    }

    #[test]
    fn viewfs_uri() {
        let (w, d, _) =
            split_table_path("viewfs://cluster/wh/db.db/t").unwrap();
        assert_eq!(w, "viewfs://cluster/wh");
        assert_eq!(d, "db");
    }

    #[test]
    fn s3_uri() {
        let (w, d, _) = split_table_path("s3://bucket/path/db.db/t").unwrap();
        assert_eq!(w, "s3://bucket/path");
        assert_eq!(d, "db");
    }

    #[test]
    fn trailing_slash_is_trimmed() {
        let (w, _, t) = split_table_path("/wh/db.db/t/").unwrap();
        assert_eq!(w, "/wh");
        assert_eq!(t, "t");
    }

    #[test]
    fn missing_db_suffix_rejected() {
        assert!(split_table_path("/wh/db/t").is_err());
    }
}

/// Touch every cell of every column so the read-pipeline benchmark measures
/// real decode + materialisation cost, not just metadata.
///
/// Returns a wrapping `u64` checksum that mixes in each value. Printed by the
/// caller so the optimiser can't dead-code-eliminate the loop. Mirrors the
/// "doris-sim" intent of the C++ demo (Arrow-import each batch, force
/// materialisation, drop) without retaining the batches.
fn consume_batch(batch: &arrow_array::RecordBatch) -> u64 {
    use arrow_array::*;
    let mut acc: u64 = 0;
    for col in batch.columns() {
        let any = col.as_any();
        if let Some(arr) = any.downcast_ref::<Int64Array>() {
            for v in arr.values() {
                acc = acc.wrapping_add(*v as u64);
            }
        } else if let Some(arr) = any.downcast_ref::<Int32Array>() {
            for v in arr.values() {
                acc = acc.wrapping_add(*v as u64);
            }
        } else if let Some(arr) = any.downcast_ref::<Int16Array>() {
            for v in arr.values() {
                acc = acc.wrapping_add(*v as u64);
            }
        } else if let Some(arr) = any.downcast_ref::<Int8Array>() {
            for v in arr.values() {
                acc = acc.wrapping_add(*v as u64);
            }
        } else if let Some(arr) = any.downcast_ref::<BooleanArray>() {
            for i in 0..arr.len() {
                if arr.value(i) {
                    acc = acc.wrapping_add(1);
                }
            }
        } else if let Some(arr) = any.downcast_ref::<Float32Array>() {
            for v in arr.values() {
                acc = acc.wrapping_add(v.to_bits() as u64);
            }
        } else if let Some(arr) = any.downcast_ref::<Float64Array>() {
            for v in arr.values() {
                acc = acc.wrapping_add(v.to_bits());
            }
        } else if let Some(arr) = any.downcast_ref::<Decimal128Array>() {
            for v in arr.values() {
                acc = acc.wrapping_add(*v as u64);
            }
        } else if let Some(arr) = any.downcast_ref::<Date32Array>() {
            for v in arr.values() {
                acc = acc.wrapping_add(*v as u64);
            }
        } else if let Some(arr) = any.downcast_ref::<TimestampMicrosecondArray>() {
            for v in arr.values() {
                acc = acc.wrapping_add(*v as u64);
            }
        } else if let Some(arr) = any.downcast_ref::<StringArray>() {
            for i in 0..arr.len() {
                acc = acc.wrapping_add(arr.value(i).len() as u64);
            }
        } else if let Some(arr) = any.downcast_ref::<BinaryArray>() {
            for i in 0..arr.len() {
                acc = acc.wrapping_add(arr.value(i).len() as u64);
            }
        }
        // else: silently skip types not produced by the demo's writer; extend
        // here when adding more column types.
    }
    acc
}

fn array_value_to_string(array: &dyn arrow_array::Array, row: usize) -> String {
    use arrow_array::*;

    if array.is_null(row) {
        return "null".to_string();
    }
    if let Some(arr) = array.as_any().downcast_ref::<Int8Array>() {
        return arr.value(row).to_string();
    }
    if let Some(arr) = array.as_any().downcast_ref::<Int16Array>() {
        return arr.value(row).to_string();
    }
    if let Some(arr) = array.as_any().downcast_ref::<Int32Array>() {
        return arr.value(row).to_string();
    }
    if let Some(arr) = array.as_any().downcast_ref::<Int64Array>() {
        return arr.value(row).to_string();
    }
    if let Some(arr) = array.as_any().downcast_ref::<Float32Array>() {
        return arr.value(row).to_string();
    }
    if let Some(arr) = array.as_any().downcast_ref::<Float64Array>() {
        return arr.value(row).to_string();
    }
    if let Some(arr) = array.as_any().downcast_ref::<BooleanArray>() {
        return arr.value(row).to_string();
    }
    if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
        return arr.value(row).to_string();
    }
    if let Some(arr) = array.as_any().downcast_ref::<LargeStringArray>() {
        return arr.value(row).to_string();
    }
    if let Some(arr) = array.as_any().downcast_ref::<Decimal128Array>() {
        // Print unscaled int128 + scale so the round-trip value is unambiguous.
        return format!("{}e-{}", arr.value(row), arr.scale());
    }
    if let Some(arr) = array.as_any().downcast_ref::<Date32Array>() {
        return format!("date32:{}", arr.value(row));
    }
    if let Some(arr) = array.as_any().downcast_ref::<TimestampMicrosecondArray>() {
        return format!("ts_us:{}", arr.value(row));
    }
    if let Some(arr) = array.as_any().downcast_ref::<BinaryArray>() {
        let bytes = arr.value(row);
        let hex: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
        return format!("0x{hex}");
    }
    format!("<unsupported type: {:?}>", array.data_type())
}

async fn run_one_pass(
    catalog: &dyn Catalog,
    db: &str,
    tbl: &str,
    args: &Args,
) -> paimon::Result<RunStats> {
    let mut stats = RunStats::default();

    // Phase 1 — plan.
    let t_plan = Instant::now();
    let table = catalog.get_table(&Identifier::new(db, tbl)).await?;

    // `--batch-size`, `--target-size`, and `--read-mode` are per-run read-time
    // overrides, not persisted schema options. Apply via `copy_with_options`
    // so this run sees the override values without mutating the catalog.
    let table = {
        let mut extra = std::collections::HashMap::new();
        if args.batch_size > 0 {
            extra.insert("read.batch-size".to_string(), args.batch_size.to_string());
        }
        if let Some(ref ts) = args.target_size {
            extra.insert("source.split.target-size".to_string(), ts.clone());
        }
        if let Some(ref rm) = args.read_mode {
            // Option key matches `core_options.rs::DELETION_VECTORS_READ_MODE_OPTION`.
            extra.insert("deletion-vectors.read-mode".to_string(), rm.clone());
        }
        if let Some(mp) = args.manifest_parallelism {
            extra.insert("scan.manifest-parallelism".to_string(), mp.to_string());
        }
        if let Some(sc) = args.sort_merge_soft_cap {
            extra.insert(
                "read.sort-merge-buffer-soft-cap".to_string(),
                sc.to_string(),
            );
        }
        if extra.is_empty() {
            table
        } else {
            table.copy_with_options(extra)
        }
    };

    let mut read_builder = table.new_read_builder();
    if let Some(cols) = &args.columns {
        let refs: Vec<&str> = cols.iter().map(String::as_str).collect();
        read_builder.with_projection(&refs);
    }
    if let Some(limit) = args.limit {
        read_builder.with_limit(limit);
    }

    // Build the predicate once on the main task — `PredicateBuilder` borrows
    // table schema fields, so we can't move it into `tokio::spawn`. The
    // predicate is also applied to the scan-side `read_builder` below for
    // plan-time partition + file-stats pruning.
    let predicate: Option<Predicate> = if !args.filters.is_empty() {
        let pb = PredicateBuilder::new(table.schema().fields());
        let mut leaves = Vec::with_capacity(args.filters.len());
        for f in &args.filters {
            let leaf = build_leaf(&pb, f).map_err(|e| paimon::Error::ConfigInvalid {
                message: e,
            })?;
            leaves.push(leaf);
        }
        Some(if leaves.len() == 1 {
            leaves.into_iter().next().unwrap()
        } else {
            Predicate::and(leaves)
        })
    } else {
        None
    };
    if let Some(p) = predicate.clone() {
        read_builder.with_filter(p);
    }

    // Whether paimon-rust will enforce the predicate exactly at row level.
    // No filter → trivially exact. With filter → ask `ReadBuilder` (this
    // checks both partition-only and parquet-row-filter coverage). When
    // `false`, the demo will run a per-batch residual filter to make the
    // visible row count match the predicate — which is what a real consumer
    // (Doris, datafusion) would have to do.
    let exact = match &predicate {
        Some(p) => read_builder.is_exact_filter_pushdown(p),
        None => true,
    };
    stats.exact_pushdown = exact;

    let scan = read_builder.new_scan();
    let plan = scan.plan().await?;
    let splits: Vec<DataSplit> = plan.splits().to_vec();
    stats.splits = splits.len();
    stats.plan_ms = t_plan.elapsed().as_millis();

    if splits.is_empty() {
        return Ok(stats);
    }

    // Phase 2 — drain. Splits are partitioned round-robin across up to
    // args.parallelism tasks (mirrors the C++ demo's bucket strategy: balances
    // size-skew better than contiguous chunks). Each task builds its own
    // `TableRead` from a cloned `Table` and drains its chunk's stream
    // independently.
    let t_drain = Instant::now();
    let parallelism = args.parallelism.min(splits.len());

    let mut chunks: Vec<Vec<DataSplit>> = (0..parallelism).map(|_| Vec::new()).collect();
    for (i, s) in splits.into_iter().enumerate() {
        chunks[i % parallelism].push(s);
    }

    let mode = args.mode;
    let projection = args.columns.clone();
    let limit = args.limit;
    // Filters are only used for residual evaluation (when paimon-rust's
    // pushdown is non-exact). When `exact` is true, every spawned task skips
    // the residual pass — saves the per-cell scan.
    let residual_filters: Vec<FilterArg> = if !exact {
        args.filters.clone()
    } else {
        Vec::new()
    };

    let mut handles = Vec::with_capacity(parallelism);
    for chunk in chunks.into_iter() {
        let table = table.clone();
        let projection = projection.clone();
        let predicate = predicate.clone();
        let residual_filters = residual_filters.clone();
        handles.push(tokio::spawn(async move {
            let mut rb = table.new_read_builder();
            if let Some(cols) = &projection {
                let refs: Vec<&str> = cols.iter().map(String::as_str).collect();
                rb.with_projection(&refs);
            }
            if let Some(lim) = limit {
                rb.with_limit(lim);
            }
            if let Some(p) = predicate {
                rb.with_filter(p);
            }
            let read = rb.new_read()?;
            let mut stream = read.to_arrow(&chunk)?;

            let mut rows = 0usize;
            let mut batch_count = 0usize;
            let mut chunk_checksum: u64 = 0;
            let mut chunk_residual_dropped: usize = 0;
            let mut collected: Option<Vec<arrow_array::RecordBatch>> =
                if matches!(mode, Mode::Collect) {
                    Some(Vec::new())
                } else {
                    None
                };
            while let Some(batch) = stream.try_next().await? {
                // Residual filter only when paimon couldn't enforce the
                // predicate exactly (`exact == false`). For exact pushdown,
                // `residual_filters` is empty so we skip the eval entirely.
                let batch = if residual_filters.is_empty() {
                    batch
                } else {
                    let raw_rows = batch.num_rows();
                    let filtered = apply_residual_filter(&batch, &residual_filters)
                        .map_err(|e| paimon::Error::ConfigInvalid { message: e })?;
                    chunk_residual_dropped += raw_rows - filtered.num_rows();
                    filtered
                };

                rows += batch.num_rows();
                batch_count += 1;
                // Mode::Count skips per-cell access — measures the read
                // pipeline alone. Consume mode pays the realistic
                // downstream-consumer cost.
                if matches!(mode, Mode::Consume) {
                    chunk_checksum = chunk_checksum.wrapping_add(consume_batch(&batch));
                }
                if let Some(c) = collected.as_mut() {
                    c.push(batch);
                }
            }
            Ok::<_, paimon::Error>((
                rows,
                batch_count,
                chunk_checksum,
                chunk_residual_dropped,
                collected,
            ))
        }));
    }

    let mut all_batches: Vec<arrow_array::RecordBatch> = Vec::new();
    for h in handles {
        match h.await {
            Ok(Ok((rows, batch_count, chunk_checksum, chunk_residual_dropped, collected))) => {
                stats.rows += rows;
                stats.batches += batch_count;
                stats.checksum = stats.checksum.wrapping_add(chunk_checksum);
                stats.residual_dropped += chunk_residual_dropped;
                if let Some(c) = collected {
                    all_batches.extend(c);
                }
            }
            Ok(Err(e)) => return Err(e),
            Err(e) => {
                return Err(paimon::Error::DataInvalid {
                    message: format!("drain task join error: {e}"),
                    source: None,
                });
            }
        }
    }
    stats.drain_ms = t_drain.elapsed().as_millis();

    // Phase 3 — emit (only for collect mode, kept out of drain_ms).
    if matches!(args.mode, Mode::Collect) {
        let t_emit = Instant::now();
        for (idx, batch) in all_batches.iter().enumerate() {
            println!(
                "--- batch {idx} ({} rows, {} cols) ---",
                batch.num_rows(),
                batch.num_columns()
            );
            println!("schema: {}", batch.schema());
            let display_rows = batch.num_rows().min(5);
            for row in 0..display_rows {
                let mut cells = Vec::with_capacity(batch.num_columns());
                for c in 0..batch.num_columns() {
                    cells.push(array_value_to_string(batch.column(c).as_ref(), row));
                }
                println!("  row {row}: [{}]", cells.join(", "));
            }
            if batch.num_rows() > display_rows {
                println!("  ... ({} more rows omitted)", batch.num_rows() - display_rows);
            }
        }
        stats.emit_ms = t_emit.elapsed().as_millis();
    }

    Ok(stats)
}

fn print_run(idx: usize, s: &RunStats, warmup: bool) {
    let rps = if s.drain_ms > 0 {
        (s.rows as f64) * 1000.0 / (s.drain_ms as f64)
    } else {
        0.0
    };
    let warmup_tag = if warmup { " [warmup]" } else { "" };
    println!(
        "run={idx}{warmup_tag} splits={} plan_ms={} drain_ms={} emit_ms={} \
         rows={} batches={} rows_per_sec={} checksum=0x{:016x} \
         exact={} residual_dropped={}",
        s.splits,
        s.plan_ms,
        s.drain_ms,
        s.emit_ms,
        s.rows,
        s.batches,
        rps as u64,
        s.checksum,
        s.exact_pushdown,
        s.residual_dropped,
    );
}

fn print_summary(stats: &[RunStats]) {
    if stats.len() < 2 {
        return;
    }
    // Skip first run (warmup), summarise the rest.
    let mut drain_ms: Vec<u128> = stats.iter().skip(1).map(|s| s.drain_ms).collect();
    drain_ms.sort();
    let median = drain_ms[drain_ms.len() / 2];
    let p95_idx = (drain_ms.len() * 95 / 100).min(drain_ms.len() - 1);
    let p95 = drain_ms[p95_idx];
    println!(
        "summary (excl. warmup, n={}): drain_ms median={} p95={}",
        drain_ms.len(),
        median,
        p95
    );
}

/// One row of cross-table summary, printed at the very end.
struct TableSummary {
    label: String,
    splits: usize,
    rows: usize,
    drain_ms_median: u128,
    rows_per_sec: u64,
}

async fn process_one_table(args: &Args, table_path: &str) -> Result<TableSummary, String> {
    let (warehouse, db, tbl) =
        split_table_path(table_path).map_err(|e| format!("{table_path}: {e}"))?;
    println!("=== {db}.{tbl} ===");
    println!("warehouse={warehouse} db={db} table={tbl}");

    let mut options = Options::new();
    options.set(CatalogOptions::METASTORE, "filesystem");
    options.set(CatalogOptions::WAREHOUSE, &warehouse);
    let catalog = CatalogFactory::create(options)
        .await
        .map_err(|e| format!("failed to create FileSystemCatalog: {e}"))?;

    // `--batch-size` and `--target-size` are per-run overrides applied
    // inside `run_one_pass` via `Table::copy_with_options`; the catalog
    // schema is intentionally not altered here.

    let mut all_stats: Vec<RunStats> = Vec::with_capacity(args.repeat);
    for i in 0..args.repeat {
        match run_one_pass(catalog.as_ref(), &db, &tbl, args).await {
            Ok(s) => {
                let warmup = args.repeat > 1 && i == 0;
                print_run(i, &s, warmup);
                all_stats.push(s);
            }
            Err(e) => return Err(format!("{db}.{tbl} run {i} failed: {e}")),
        }
    }
    print_summary(&all_stats);

    // Cross-table summary uses median drain_ms (excluding warmup if repeat>1)
    // so a single noisy run doesn't drag the comparison.
    let stats_for_summary: Vec<&RunStats> = if all_stats.len() > 1 {
        all_stats.iter().skip(1).collect()
    } else {
        all_stats.iter().collect()
    };
    let mut drain_ms: Vec<u128> = stats_for_summary.iter().map(|s| s.drain_ms).collect();
    drain_ms.sort();
    let drain_ms_median = drain_ms.get(drain_ms.len() / 2).copied().unwrap_or(0);
    let rows = stats_for_summary.first().map(|s| s.rows).unwrap_or(0);
    let splits = stats_for_summary.first().map(|s| s.splits).unwrap_or(0);
    let rows_per_sec = if drain_ms_median > 0 {
        ((rows as f64) * 1000.0 / (drain_ms_median as f64)) as u64
    } else {
        0
    };

    println!();
    Ok(TableSummary {
        label: format!("{db}.{tbl}"),
        splits,
        rows,
        drain_ms_median,
        rows_per_sec,
    })
}

fn print_cross_table_summary(summaries: &[TableSummary]) {
    if summaries.len() <= 1 {
        return;
    }
    println!("=== cross-table summary (drain_ms uses median excl. warmup) ===");
    let label_w = summaries
        .iter()
        .map(|s| s.label.len())
        .max()
        .unwrap_or(0)
        .max(5);
    println!(
        "{:<label_w$}  {:>8}  {:>13}  {:>13}  {:>15}",
        "table",
        "splits",
        "rows",
        "drain_ms",
        "rows_per_sec",
        label_w = label_w
    );
    for s in summaries {
        println!(
            "{:<label_w$}  {:>8}  {:>13}  {:>13}  {:>15}",
            s.label,
            s.splits,
            s.rows,
            s.drain_ms_median,
            s.rows_per_sec,
            label_w = label_w
        );
    }
}

fn main() {
    let argv0 = std::env::args().next().unwrap_or_else(|| "demo".to_string());

    let args = match parse_args() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("error: {e}");
            print_usage(&argv0);
            std::process::exit(2);
        }
    };

    // Build the tokio runtime explicitly so worker_threads tracks
    // args.parallelism — the `#[tokio::main]` macro only accepts literals
    // and would lock us at the compile-time default.
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(args.parallelism)
        .enable_all()
        .build()
        .expect("failed to build tokio runtime");

    rt.block_on(async {
        paimon::alloc::print_stats("startup");
        let _stats_task = spawn_periodic_mem_stats();

        let mut summaries: Vec<TableSummary> = Vec::with_capacity(args.table_paths.len());
        for (idx, path) in args.table_paths.iter().enumerate() {
            match process_one_table(&args, path).await {
                Ok(s) => summaries.push(s),
                Err(e) => {
                    eprintln!("error: {e}");
                    std::process::exit(1);
                }
            }
            paimon::alloc::print_stats(&format!("after_table[{idx}]"));
        }

        print_cross_table_summary(&summaries);
        paimon::alloc::print_stats("end");
    });
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
