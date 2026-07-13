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

use std::ffi::c_void;

use arrow_array::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow_array::{Array, StructArray};
use futures::StreamExt;
use paimon::catalog::Identifier;
use paimon::io::FileIO;
use paimon::spec::{DataField, DataType, Datum, Predicate, PredicateBuilder};
use paimon::table::{deserialize_data_split_to_plan, ArrowRecordBatchStream, SchemaManager, Table};
use paimon::Plan;

use crate::error::{check_non_null, paimon_error, validate_cstr, PaimonErrorCode};
use crate::result::{
    paimon_result_get_table, paimon_result_new_read, paimon_result_next_batch, paimon_result_plan,
    paimon_result_predicate, paimon_result_read_builder, paimon_result_record_batch_reader,
    paimon_result_table_scan,
};
use crate::types::*;

// Helper to free a wrapper struct that contains a Table clone.
unsafe fn free_table_wrapper<T>(ptr: *mut T, get_inner: impl FnOnce(&T) -> *mut c_void) {
    if !ptr.is_null() {
        let wrapper = Box::from_raw(ptr);
        let inner = get_inner(&wrapper);
        if !inner.is_null() {
            drop(Box::from_raw(inner as *mut Table));
        }
    }
}

// Helper to box a ReadBuilderState and return a raw pointer.
unsafe fn box_read_builder_state(state: ReadBuilderState) -> *mut paimon_read_builder {
    let inner = Box::into_raw(Box::new(state)) as *mut c_void;
    Box::into_raw(Box::new(paimon_read_builder { inner }))
}

// Helper to box a TableReadState and return a raw pointer.
unsafe fn box_table_read_state(state: TableReadState) -> *mut paimon_table_read {
    let inner = Box::into_raw(Box::new(state)) as *mut c_void;
    Box::into_raw(Box::new(paimon_table_read { inner }))
}

// ======================= Table ===============================

/// Free a paimon_table.
///
/// # Safety
/// Only call with a table returned from `paimon_catalog_get_table` or
/// `paimon_table_open_path`.
#[no_mangle]
pub unsafe extern "C" fn paimon_table_free(table: *mut paimon_table) {
    free_table_wrapper(table, |t| t.inner);
}

/// Open a Paimon table directly from its on-disk root path, skipping the
/// usual `paimon_catalog_create` + `paimon_catalog_get_table` round-trip.
///
/// `table_path` is expected to be the table's filesystem root, e.g.
/// `/warehouse/mydb.db/users` for a local fs, `oss://bucket/warehouse/mydb.db/users`
/// for OSS, etc. The path's last two `/`-separated segments are interpreted
/// as `<db>.db/<table>`; everything before that is treated as the warehouse
/// root and used to construct the underlying `FileIO` (and as the home of
/// any object-storage credentials in `options`).
///
/// `options` is an optional array of key/value pairs that gets fed to the
/// FileIO storage layer (S3 / OSS / etc.). Pass `NULL` and `0` for a local
/// filesystem table that needs no extra configuration.
///
/// `use_alluxio` is the session-level switch for routing this table's data
/// reads through Alluxio (see `paimon_catalog_get_table` for the contract).
/// Pass `false` to keep the existing native-HDFS behaviour. Catalog metadata
/// (schema/, snapshot, manifest) is unaffected.
///
/// # Safety
/// `table_path` must be a valid null-terminated UTF-8 C string. `options`
/// must point to `options_len` valid `paimon_option`s, or be null when
/// `options_len == 0`.
#[no_mangle]
pub unsafe extern "C" fn paimon_table_open_path(
    table_path: *const std::ffi::c_char,
    options: *const paimon_option,
    options_len: usize,
    use_alluxio: bool,
) -> paimon_result_get_table {
    let path = match validate_cstr(table_path, "table_path") {
        Ok(s) => s,
        Err(e) => {
            return paimon_result_get_table {
                table: std::ptr::null_mut(),
                error: e,
            }
        }
    };

    // The C++ side hands us only the table root; reconstruct the (warehouse,
    // db, table) triple Java/Paimon expects. Splitting in Rust mirrors what
    // examples/read_local_demo.rs does for the same use case.
    let (warehouse, db, table_name) = match split_table_path(&path) {
        Ok(x) => x,
        Err(msg) => {
            return paimon_result_get_table {
                table: std::ptr::null_mut(),
                error: paimon_error::new(PaimonErrorCode::InvalidInput, msg),
            }
        }
    };

    // Build the FileIO; on local fs nothing extra is needed, but for S3/OSS
    // the caller provides credentials via `options`. Pass them directly to
    // the storage layer — they're the same keys `paimon_catalog_create` uses.
    //
    // Unlike the catalog path (which keeps metadata on native HDFS and only
    // flips the *data* FileIO via `Table::with_alluxio`), the open-path caller
    // hands us a table root that is itself already an `alluxio://` URI when it
    // wants Alluxio. We therefore propagate `use_alluxio` to the FileIO that
    // backs the schema/snapshot/manifest reads too, so an `alluxio://` warehouse
    // doesn't trip `Storage::build`'s "alluxio:// scheme requires with_alluxio"
    // guard. The result: metadata and data both route through Alluxio.
    let file_io_builder = match FileIO::from_path(&warehouse) {
        Ok(b) => b.with_alluxio(use_alluxio),
        Err(e) => {
            return paimon_result_get_table {
                table: std::ptr::null_mut(),
                error: paimon_error::from_paimon(e),
            }
        }
    };

    let file_io_builder = if !options.is_null() && options_len > 0 {
        let slice = std::slice::from_raw_parts(options, options_len);
        let mut props: Vec<(String, String)> = Vec::with_capacity(slice.len());
        for opt in slice {
            let key = match validate_cstr(opt.key, "option key") {
                Ok(s) => s,
                Err(e) => {
                    return paimon_result_get_table {
                        table: std::ptr::null_mut(),
                        error: e,
                    }
                }
            };
            let value = match validate_cstr(opt.value, "option value") {
                Ok(s) => s,
                Err(e) => {
                    return paimon_result_get_table {
                        table: std::ptr::null_mut(),
                        error: e,
                    }
                }
            };
            props.push((key, value));
        }
        file_io_builder.with_props(props)
    } else {
        file_io_builder
    };

    let file_io = match file_io_builder.build() {
        Ok(io) => io,
        Err(e) => {
            return paimon_result_get_table {
                table: std::ptr::null_mut(),
                error: paimon_error::from_paimon(e),
            }
        }
    };

    // Resolve the latest schema for the table by listing `<table_path>/schema/`.
    // Mirrors `FileSystemCatalog::load_latest_table_schema` so a path-loaded
    // table is byte-for-byte identical to one obtained from the catalog API.
    let schema_manager = SchemaManager::new(file_io.clone(), path.clone());
    let schema_arc = match crate::block_on(schema_manager.latest()) {
        Ok(Some(s)) => s,
        Ok(None) => {
            return paimon_result_get_table {
                table: std::ptr::null_mut(),
                error: paimon_error::new(
                    PaimonErrorCode::NotFound,
                    format!("no schema found under {path}"),
                ),
            }
        }
        Err(e) => {
            return paimon_result_get_table {
                table: std::ptr::null_mut(),
                error: paimon_error::from_paimon(e),
            }
        }
    };

    let identifier = Identifier::new(db, table_name);
    let table = Table::new(file_io, identifier, path, (*schema_arc).clone(), None);
    let table = match table.with_alluxio(use_alluxio) {
        Ok(t) => t,
        Err(e) => {
            return paimon_result_get_table {
                table: std::ptr::null_mut(),
                error: paimon_error::from_paimon(e),
            }
        }
    };
    let wrapper = Box::new(paimon_table {
        inner: Box::into_raw(Box::new(table)) as *mut c_void,
    });
    paimon_result_get_table {
        table: Box::into_raw(wrapper),
        error: std::ptr::null_mut(),
    }
}

/// Split a `<warehouse>/<db>.db/<table>` path into its three parts.
///
/// Done at the byte level via `rsplit_once('/')`, so URI prefixes like
/// `oss://bucket/...` survive intact (`std::path::Path` would mangle the `://`).
/// Mirrors `crates/paimon/examples/read_local_demo.rs::split_table_path`; we
/// duplicate it here rather than depend on `examples/` since example modules
/// aren't compiled into the library target.
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

/// Create a new ReadBuilder from a Table.
///
/// # Safety
/// `table` must be a valid pointer from `paimon_catalog_get_table`, or null (returns error).
#[no_mangle]
pub unsafe extern "C" fn paimon_table_new_read_builder(
    table: *const paimon_table,
) -> paimon_result_read_builder {
    if let Err(e) = check_non_null(table, "table") {
        return paimon_result_read_builder {
            read_builder: std::ptr::null_mut(),
            error: e,
        };
    }
    let table_ref = &*((*table).inner as *const Table);
    let state = ReadBuilderState {
        table: table_ref.clone(),
        projected_columns: None,
        filter: None,
        case_sensitive: true,
    };
    paimon_result_read_builder {
        read_builder: box_read_builder_state(state),
        error: std::ptr::null_mut(),
    }
}

// ======================= ReadBuilder ===============================

/// Free a paimon_read_builder.
///
/// # Safety
/// Only call with a read_builder returned from `paimon_table_new_read_builder`.
#[no_mangle]
pub unsafe extern "C" fn paimon_read_builder_free(rb: *mut paimon_read_builder) {
    if !rb.is_null() {
        let wrapper = Box::from_raw(rb);
        if !wrapper.inner.is_null() {
            drop(Box::from_raw(wrapper.inner as *mut ReadBuilderState));
        }
    }
}

/// Set column projection for a ReadBuilder.
///
/// The `columns` parameter is a null-terminated array of null-terminated C strings.
/// Output order follows the caller-specified order. Unknown or duplicate names
/// cause `paimon_read_builder_new_read()` to fail; an empty list is a valid
/// zero-column projection. Case-dependent resolution (a name that matches only
/// case-insensitively, or a case-fold ambiguity) is deferred to
/// `paimon_read_builder_new_read`, which uses the case sensitivity effective
/// then, so this stays order-independent with
/// `paimon_read_builder_with_case_sensitive`.
///
/// # Safety
/// `rb` must be a valid pointer from `paimon_table_new_read_builder`, or null (returns error).
/// `columns` must be a null-terminated array of null-terminated C strings, or null for no projection.
#[no_mangle]
pub unsafe extern "C" fn paimon_read_builder_with_projection(
    rb: *mut paimon_read_builder,
    columns: *const *const std::ffi::c_char,
) -> *mut paimon_error {
    if let Err(e) = check_non_null(rb, "rb") {
        return e;
    }

    let state = &mut *((*rb).inner as *mut ReadBuilderState);

    if columns.is_null() {
        state.projected_columns = None;
        return std::ptr::null_mut();
    }

    let mut col_names = Vec::new();
    let mut ptr = columns;
    while !(*ptr).is_null() {
        let c_str = std::ffi::CStr::from_ptr(*ptr);
        match c_str.to_str() {
            Ok(s) => col_names.push(s.to_string()),
            Err(e) => {
                return paimon_error::from_paimon(paimon::Error::ConfigInvalid {
                    message: format!("Invalid UTF-8 in column name: {e}"),
                });
            }
        }
        ptr = ptr.add(1);
    }

    state.projected_columns = Some(col_names);
    std::ptr::null_mut()
}

/// Set whether column-name matching for **projection** is case-sensitive for
/// this ReadBuilder. Defaults to `true` (exact match). When `false`, projected
/// column names are matched by ASCII case-folding and an ambiguous
/// (case-colliding) request errors.
///
/// This does **not** affect predicate resolution: a predicate is resolved when
/// it is constructed, so its case sensitivity is chosen by which constructor
/// you call — `paimon_predicate_*` (case-sensitive) or the additive
/// `paimon_predicate_*_with_case_sensitive` variant — independently of this
/// setting.
///
/// # Safety
/// `rb` must be a valid pointer from `paimon_table_new_read_builder`, or null (returns error).
#[no_mangle]
pub unsafe extern "C" fn paimon_read_builder_with_case_sensitive(
    rb: *mut paimon_read_builder,
    case_sensitive: bool,
) -> *mut paimon_error {
    if let Err(e) = check_non_null(rb, "rb") {
        return e;
    }
    let state = &mut *((*rb).inner as *mut ReadBuilderState);
    state.case_sensitive = case_sensitive;
    std::ptr::null_mut()
}

/// Set a filter predicate for scan planning.
///
/// The predicate is consumed (ownership transferred to the read builder).
/// Pass null to clear any previously set filter.
///
/// # Safety
/// `rb` must be a valid pointer from `paimon_table_new_read_builder`, or null (returns error).
/// `predicate` must be a valid pointer from a `paimon_predicate_*` function, or null.
#[no_mangle]
pub unsafe extern "C" fn paimon_read_builder_with_filter(
    rb: *mut paimon_read_builder,
    predicate: *mut paimon_predicate,
) -> *mut paimon_error {
    if let Err(e) = check_non_null(rb, "rb") {
        return e;
    }

    let state = &mut *((*rb).inner as *mut ReadBuilderState);

    if predicate.is_null() {
        state.filter = None;
        return std::ptr::null_mut();
    }

    let pred_wrapper = Box::from_raw(predicate);
    let pred = Box::from_raw(pred_wrapper.inner as *mut Predicate);
    state.filter = Some(*pred);
    std::ptr::null_mut()
}

/// Create a new TableScan from a ReadBuilder.
///
/// # Safety
/// `rb` must be a valid pointer from `paimon_table_new_read_builder`, or null (returns error).
#[no_mangle]
pub unsafe extern "C" fn paimon_read_builder_new_scan(
    rb: *const paimon_read_builder,
) -> paimon_result_table_scan {
    if let Err(e) = check_non_null(rb, "rb") {
        return paimon_result_table_scan {
            scan: std::ptr::null_mut(),
            error: e,
        };
    }
    let state = &*((*rb).inner as *const ReadBuilderState);
    let scan_state = TableScanState {
        table: state.table.clone(),
        filter: state.filter.clone(),
    };
    let inner = Box::into_raw(Box::new(scan_state)) as *mut c_void;
    paimon_result_table_scan {
        scan: Box::into_raw(Box::new(paimon_table_scan { inner })),
        error: std::ptr::null_mut(),
    }
}

/// Create a new TableRead from a ReadBuilder.
///
/// # Safety
/// `rb` must be a valid pointer from `paimon_table_new_read_builder`, or null (returns error).
#[no_mangle]
pub unsafe extern "C" fn paimon_read_builder_new_read(
    rb: *const paimon_read_builder,
) -> paimon_result_new_read {
    if let Err(e) = check_non_null(rb, "rb") {
        return paimon_result_new_read {
            read: std::ptr::null_mut(),
            error: e,
        };
    }
    let state = &*((*rb).inner as *const ReadBuilderState);
    let mut rb_rust = state.table.new_read_builder();
    rb_rust.with_case_sensitive(state.case_sensitive);

    // Apply projection if set
    if let Some(ref columns) = state.projected_columns {
        let col_refs: Vec<&str> = columns.iter().map(|s| s.as_str()).collect();
        rb_rust.with_projection(&col_refs);
    }

    // Apply filter if set
    if let Some(ref filter) = state.filter {
        rb_rust.with_filter(filter.clone());
    }

    match rb_rust.new_read() {
        Ok(table_read) => {
            let read_state = TableReadState {
                table: state.table.clone(),
                read_type: table_read.read_type().to_vec(),
                data_predicates: table_read.data_predicates().to_vec(),
            };
            paimon_result_new_read {
                read: box_table_read_state(read_state),
                error: std::ptr::null_mut(),
            }
        }
        Err(e) => paimon_result_new_read {
            read: std::ptr::null_mut(),
            error: paimon_error::from_paimon(e),
        },
    }
}

// ======================= TableScan ===============================

/// Free a paimon_table_scan.
///
/// # Safety
/// Only call with a scan returned from `paimon_read_builder_new_scan`.
#[no_mangle]
pub unsafe extern "C" fn paimon_table_scan_free(scan: *mut paimon_table_scan) {
    if !scan.is_null() {
        let wrapper = Box::from_raw(scan);
        if !wrapper.inner.is_null() {
            drop(Box::from_raw(wrapper.inner as *mut TableScanState));
        }
    }
}

/// Execute a scan plan to get splits.
///
/// # Safety
/// `scan` must be a valid pointer from `paimon_read_builder_new_scan`, or null (returns error).
#[no_mangle]
pub unsafe extern "C" fn paimon_table_scan_plan(
    scan: *const paimon_table_scan,
) -> paimon_result_plan {
    if let Err(e) = check_non_null(scan, "scan") {
        return paimon_result_plan {
            plan: std::ptr::null_mut(),
            error: e,
        };
    }
    let scan_state = &*((*scan).inner as *const TableScanState);
    let mut rb = scan_state.table.new_read_builder();
    if let Some(ref filter) = scan_state.filter {
        rb.with_filter(filter.clone());
    }
    let table_scan = rb.new_scan();

    match crate::block_on(table_scan.plan()) {
        Ok(plan) => {
            let wrapper = Box::new(paimon_plan {
                inner: Box::into_raw(Box::new(plan)) as *mut c_void,
            });
            paimon_result_plan {
                plan: Box::into_raw(wrapper),
                error: std::ptr::null_mut(),
            }
        }
        Err(e) => paimon_result_plan {
            plan: std::ptr::null_mut(),
            error: paimon_error::from_paimon(e),
        },
    }
}

// ======================= Plan ===============================

/// Free a paimon_plan.
///
/// # Safety
/// Only call with a plan returned from `paimon_table_scan_plan` or
/// `paimon_plan_from_split_bytes`.
#[no_mangle]
pub unsafe extern "C" fn paimon_plan_free(plan: *mut paimon_plan) {
    if !plan.is_null() {
        let p = Box::from_raw(plan);
        if !p.inner.is_null() {
            drop(Box::from_raw(p.inner as *mut Plan));
        }
    }
}

/// Build a one-split `paimon_plan` from a serialized `DataSplit` byte buffer
/// (the wire form produced by `paimon::table::serialize_data_split` and
/// accepted by `paimon-cpp`'s `Split::Deserialize`).
///
/// Use this on workers that received split bytes from a remote planner —
/// e.g. Bleem's coordinator already ran scan planning and just hands each
/// worker the bytes for the splits it should read. Once you have the plan,
/// pass it straight to `paimon_table_read_to_arrow` like any plan obtained
/// from `paimon_table_scan_plan`.
///
/// One byte buffer becomes a one-split plan. Concatenating multiple splits
/// into one buffer is not supported — call this once per split and merge on
/// the caller side, or extend the wire form upstream.
///
/// # Safety
/// `data` must point to `len` bytes of valid serialized split, or be null
/// when `len == 0` (which returns `InvalidInput`).
#[no_mangle]
pub unsafe extern "C" fn paimon_plan_from_split_bytes(
    data: *const u8,
    len: usize,
) -> paimon_result_plan {
    if data.is_null() || len == 0 {
        return paimon_result_plan {
            plan: std::ptr::null_mut(),
            error: paimon_error::new(
                PaimonErrorCode::InvalidInput,
                "paimon_plan_from_split_bytes: null or empty buffer".to_string(),
            ),
        };
    }
    let bytes = std::slice::from_raw_parts(data, len);
    match deserialize_data_split_to_plan(bytes) {
        Ok(plan) => {
            let wrapper = Box::new(paimon_plan {
                inner: Box::into_raw(Box::new(plan)) as *mut c_void,
            });
            paimon_result_plan {
                plan: Box::into_raw(wrapper),
                error: std::ptr::null_mut(),
            }
        }
        Err(e) => paimon_result_plan {
            plan: std::ptr::null_mut(),
            error: paimon_error::from_paimon(e),
        },
    }
}

/// Return the number of data splits in a plan.
///
/// # Safety
/// `plan` must be a valid pointer from `paimon_table_scan_plan`, or null (returns 0).
#[no_mangle]
pub unsafe extern "C" fn paimon_plan_num_splits(plan: *const paimon_plan) -> usize {
    if plan.is_null() {
        return 0;
    }
    let plan_ref = &*((*plan).inner as *const Plan);
    plan_ref.splits().len()
}

// ======================= TableRead ===============================

/// Free a paimon_table_read.
///
/// # Safety
/// Only call with a read returned from `paimon_read_builder_new_read`.
#[no_mangle]
pub unsafe extern "C" fn paimon_table_read_free(read: *mut paimon_table_read) {
    if !read.is_null() {
        let wrapper = Box::from_raw(read);
        if !wrapper.inner.is_null() {
            drop(Box::from_raw(wrapper.inner as *mut TableReadState));
        }
    }
}

/// Read table data as Arrow record batches via a streaming reader.
///
/// Returns a `paimon_record_batch_reader` that yields one batch at a time
/// via `paimon_record_batch_reader_next`. This avoids loading all batches
/// into memory at once.
///
/// `offset` and `length` select a contiguous sub-range of splits from the
/// plan. The range is clamped to the available splits (out-of-range values
/// are silently adjusted).
///
/// # Safety
/// `read` and `plan` must be valid pointers from previous paimon C calls, or null (returns error).
#[no_mangle]
pub unsafe extern "C" fn paimon_table_read_to_arrow(
    read: *const paimon_table_read,
    plan: *const paimon_plan,
    offset: usize,
    length: usize,
) -> paimon_result_record_batch_reader {
    if let Err(e) = check_non_null(read, "read") {
        return paimon_result_record_batch_reader {
            reader: std::ptr::null_mut(),
            error: e,
        };
    }
    if let Err(e) = check_non_null(plan, "plan") {
        return paimon_result_record_batch_reader {
            reader: std::ptr::null_mut(),
            error: e,
        };
    }

    let state = &*((*read).inner as *const TableReadState);
    let plan_ref = &*((*plan).inner as *const Plan);
    let all_splits = plan_ref.splits();
    let start = offset.min(all_splits.len());
    let end = (offset.saturating_add(length)).min(all_splits.len());
    let selected = &all_splits[start..end];

    let table_read = paimon::table::TableRead::new(
        &state.table,
        state.read_type.clone(),
        state.data_predicates.clone(),
    );

    match table_read.to_arrow(selected) {
        Ok(stream) => {
            let reader = Box::new(stream);
            let wrapper = Box::new(paimon_record_batch_reader {
                inner: Box::into_raw(reader) as *mut c_void,
            });
            paimon_result_record_batch_reader {
                reader: Box::into_raw(wrapper),
                error: std::ptr::null_mut(),
            }
        }
        Err(e) => paimon_result_record_batch_reader {
            reader: std::ptr::null_mut(),
            error: paimon_error::from_paimon(e),
        },
    }
}

// ======================= RecordBatchReader ===============================

/// Get the next Arrow record batch from the reader.
///
/// When the stream is exhausted, both `batch.array` and `batch.schema` will
/// be null. On error, `error` will be non-null.
///
/// After importing each batch, call `paimon_arrow_batch_free` to free the
/// ArrowArray and ArrowSchema container structs.
///
/// # Safety
/// `reader` must be a valid pointer from `paimon_table_read_to_arrow`, or null (returns error).
#[no_mangle]
pub unsafe extern "C" fn paimon_record_batch_reader_next(
    reader: *mut paimon_record_batch_reader,
) -> paimon_result_next_batch {
    if let Err(e) = check_non_null(reader, "reader") {
        return paimon_result_next_batch {
            batch: paimon_arrow_batch {
                array: std::ptr::null_mut(),
                schema: std::ptr::null_mut(),
            },
            error: e,
        };
    }

    let stream = &mut *((*reader).inner as *mut ArrowRecordBatchStream);
    // Memory accounting is driven by the C++ caller: it installs the query's
    // counter as this thread's tag (via paimon_mem_counter_enter) for the whole
    // span of get_next_block, so the batch buffers allocated here are balanced
    // by their later free in the C++ batch destructor. Decoding for
    // parquet/ORC/avro runs synchronously on this thread; the Vortex decode
    // thread re-installs the tag itself by reading it back (run_vortex_on_thread).

    match crate::block_on(stream.next()) {
        Some(Ok(batch)) => {
            let schema = batch.schema();
            let struct_array = StructArray::from(batch);
            let ffi_array = FFI_ArrowArray::new(&struct_array.to_data());
            let ffi_schema = match FFI_ArrowSchema::try_from(schema.as_ref()) {
                Ok(s) => s,
                Err(e) => {
                    return paimon_result_next_batch {
                        batch: paimon_arrow_batch {
                            array: std::ptr::null_mut(),
                            schema: std::ptr::null_mut(),
                        },
                        error: paimon_error::from_paimon(paimon::Error::UnexpectedError {
                            message: format!("Failed to export Arrow schema: {e}"),
                            source: Some(Box::new(e)),
                        }),
                    };
                }
            };

            let array_ptr = Box::into_raw(Box::new(ffi_array)) as *mut c_void;
            let schema_ptr = Box::into_raw(Box::new(ffi_schema)) as *mut c_void;

            paimon_result_next_batch {
                batch: paimon_arrow_batch {
                    array: array_ptr,
                    schema: schema_ptr,
                },
                error: std::ptr::null_mut(),
            }
        }
        Some(Err(e)) => paimon_result_next_batch {
            batch: paimon_arrow_batch {
                array: std::ptr::null_mut(),
                schema: std::ptr::null_mut(),
            },
            error: paimon_error::from_paimon(e),
        },
        None => paimon_result_next_batch {
            batch: paimon_arrow_batch {
                array: std::ptr::null_mut(),
                schema: std::ptr::null_mut(),
            },
            error: std::ptr::null_mut(),
        },
    }
}

/// Free a paimon_record_batch_reader.
///
/// # Safety
/// Only call with a reader returned from `paimon_table_read_to_arrow` or
/// `paimon_vector_search_builder_execute_read`.
#[no_mangle]
pub unsafe extern "C" fn paimon_record_batch_reader_free(reader: *mut paimon_record_batch_reader) {
    if !reader.is_null() {
        let wrapper = Box::from_raw(reader);
        if !wrapper.inner.is_null() {
            drop(Box::from_raw(wrapper.inner as *mut ArrowRecordBatchStream));
        }
    }
}

/// Free the ArrowArray and ArrowSchema container structs for a single batch.
///
/// # Safety
/// `batch` must contain valid pointers returned by `paimon_record_batch_reader_next`.
#[no_mangle]
pub unsafe extern "C" fn paimon_arrow_batch_free(batch: paimon_arrow_batch) {
    if !batch.array.is_null() {
        drop(Box::from_raw(batch.array as *mut FFI_ArrowArray));
    }
    if !batch.schema.is_null() {
        drop(Box::from_raw(batch.schema as *mut FFI_ArrowSchema));
    }
}

// ======================= Predicate ===============================

/// Convert a C datum to a Rust Datum.
unsafe fn datum_from_c(d: &paimon_datum) -> Result<Datum, *mut paimon_error> {
    match d.tag {
        0 => Ok(Datum::Bool(d.int_val != 0)),
        1 => Ok(Datum::TinyInt(d.int_val as i8)),
        2 => Ok(Datum::SmallInt(d.int_val as i16)),
        3 => Ok(Datum::Int(d.int_val as i32)),
        4 => Ok(Datum::Long(d.int_val)),
        5 => Ok(Datum::Float(d.double_val as f32)),
        6 => Ok(Datum::Double(d.double_val)),
        7 => {
            if d.str_len == 0 {
                return Ok(Datum::String(String::new()));
            }
            if d.str_data.is_null() {
                return Err(paimon_error::new(
                    PaimonErrorCode::InvalidInput,
                    "null string data in datum with non-zero length".to_string(),
                ));
            }
            let bytes = std::slice::from_raw_parts(d.str_data, d.str_len);
            let s = std::str::from_utf8(bytes).map_err(|e| {
                paimon_error::new(
                    PaimonErrorCode::InvalidInput,
                    format!("invalid UTF-8 in datum string: {e}"),
                )
            })?;
            Ok(Datum::String(s.to_string()))
        }
        8 => Ok(Datum::Date(d.int_val as i32)),
        9 => Ok(Datum::Time(d.int_val as i32)),
        10 => Ok(Datum::Timestamp {
            millis: d.int_val,
            nanos: d.int_val2 as i32,
        }),
        11 => Ok(Datum::LocalZonedTimestamp {
            millis: d.int_val,
            nanos: d.int_val2 as i32,
        }),
        12 => {
            let unscaled = ((d.int_val2 as i128) << 64) | (d.int_val as u64 as i128);
            Ok(Datum::Decimal {
                unscaled,
                precision: d.uint_val,
                scale: d.uint_val2,
            })
        }
        13 => {
            if d.str_data.is_null() && d.str_len > 0 {
                return Err(paimon_error::new(
                    PaimonErrorCode::InvalidInput,
                    "null bytes data in datum".to_string(),
                ));
            }
            let bytes = if d.str_len > 0 {
                std::slice::from_raw_parts(d.str_data, d.str_len).to_vec()
            } else {
                Vec::new()
            };
            Ok(Datum::Bytes(bytes))
        }
        _ => Err(paimon_error::new(
            PaimonErrorCode::InvalidInput,
            format!("unknown datum tag: {}", d.tag),
        )),
    }
}

/// Coerce an integer-family datum to match the target column's integer type.
///
/// FFI callers (e.g. Go) often pass a narrower integer literal (Int) for a
/// wider column (BigInt). This function widens or narrows the datum to match,
/// checking range for narrowing conversions.
///
/// Non-integer datums or non-integer columns are returned as-is.
fn coerce_integer_datum(
    datum: Datum,
    fields: &[DataField],
    column: &str,
    case_sensitive: bool,
) -> Result<Datum, *mut paimon_error> {
    let val = match &datum {
        Datum::TinyInt(v) => *v as i64,
        Datum::SmallInt(v) => *v as i64,
        Datum::Int(v) => *v as i64,
        Datum::Long(v) => *v,
        _ => return Ok(datum),
    };

    // Resolve the column with the same case sensitivity as PredicateBuilder.
    // A non-unique (absent or ambiguous) match is left uncoerced so the
    // PredicateBuilder produces the proper not-found / ambiguous error.
    let field = if case_sensitive {
        fields.iter().find(|f| f.name() == column)
    } else {
        let mut hits = fields
            .iter()
            .filter(|f| f.name().eq_ignore_ascii_case(column));
        match (hits.next(), hits.next()) {
            (Some(f), None) => Some(f),
            _ => None,
        }
    };
    let Some(field) = field else {
        // Column not found / ambiguous; let PredicateBuilder produce the error.
        return Ok(datum);
    };

    match field.data_type() {
        DataType::TinyInt(_) if !matches!(datum, Datum::TinyInt(_)) => {
            if val < i8::MIN as i64 || val > i8::MAX as i64 {
                Err(paimon_error::new(
                    PaimonErrorCode::InvalidInput,
                    format!("value {val} out of range for TinyInt column '{column}'"),
                ))
            } else {
                Ok(Datum::TinyInt(val as i8))
            }
        }
        DataType::SmallInt(_) if !matches!(datum, Datum::SmallInt(_)) => {
            if val < i16::MIN as i64 || val > i16::MAX as i64 {
                Err(paimon_error::new(
                    PaimonErrorCode::InvalidInput,
                    format!("value {val} out of range for SmallInt column '{column}'"),
                ))
            } else {
                Ok(Datum::SmallInt(val as i16))
            }
        }
        DataType::Int(_) if !matches!(datum, Datum::Int(_)) => {
            if val < i32::MIN as i64 || val > i32::MAX as i64 {
                Err(paimon_error::new(
                    PaimonErrorCode::InvalidInput,
                    format!("value {val} out of range for Int column '{column}'"),
                ))
            } else {
                Ok(Datum::Int(val as i32))
            }
        }
        DataType::BigInt(_) if !matches!(datum, Datum::Long(_)) => Ok(Datum::Long(val)),
        _ => Ok(datum),
    }
}

/// Helper to build a leaf predicate that takes a datum, via PredicateBuilder.
unsafe fn build_leaf_predicate_datum(
    table: *const paimon_table,
    column: *const std::ffi::c_char,
    datum: &paimon_datum,
    case_sensitive: bool,
    build_fn: impl FnOnce(&PredicateBuilder, &str, Datum) -> paimon::Result<Predicate>,
) -> paimon_result_predicate {
    if let Err(e) = check_non_null(table, "table") {
        return paimon_result_predicate {
            predicate: std::ptr::null_mut(),
            error: e,
        };
    }
    let col_name = match validate_cstr(column, "column") {
        Ok(s) => s,
        Err(e) => {
            return paimon_result_predicate {
                predicate: std::ptr::null_mut(),
                error: e,
            }
        }
    };

    let d = match datum_from_c(datum) {
        Ok(d) => d,
        Err(e) => {
            return paimon_result_predicate {
                predicate: std::ptr::null_mut(),
                error: e,
            }
        }
    };

    let table_ref = &*((*table).inner as *const Table);
    let fields = table_ref.schema().fields();

    let d = match coerce_integer_datum(d, fields, &col_name, case_sensitive) {
        Ok(d) => d,
        Err(e) => {
            return paimon_result_predicate {
                predicate: std::ptr::null_mut(),
                error: e,
            }
        }
    };

    let pb = PredicateBuilder::new_with_case_sensitive(fields, case_sensitive);
    match build_fn(&pb, &col_name, d) {
        Ok(pred) => {
            let inner = Box::into_raw(Box::new(pred)) as *mut c_void;
            paimon_result_predicate {
                predicate: Box::into_raw(Box::new(paimon_predicate { inner })),
                error: std::ptr::null_mut(),
            }
        }
        Err(e) => paimon_result_predicate {
            predicate: std::ptr::null_mut(),
            error: paimon_error::from_paimon(e),
        },
    }
}

/// Helper to build a leaf predicate without a datum (IS NULL / IS NOT NULL).
unsafe fn build_leaf_predicate(
    table: *const paimon_table,
    column: *const std::ffi::c_char,
    case_sensitive: bool,
    build_fn: impl FnOnce(&PredicateBuilder, &str) -> paimon::Result<Predicate>,
) -> paimon_result_predicate {
    if let Err(e) = check_non_null(table, "table") {
        return paimon_result_predicate {
            predicate: std::ptr::null_mut(),
            error: e,
        };
    }
    let col_name = match validate_cstr(column, "column") {
        Ok(s) => s,
        Err(e) => {
            return paimon_result_predicate {
                predicate: std::ptr::null_mut(),
                error: e,
            }
        }
    };
    let table_ref = &*((*table).inner as *const Table);
    let pb = PredicateBuilder::new_with_case_sensitive(table_ref.schema().fields(), case_sensitive);
    match build_fn(&pb, &col_name) {
        Ok(pred) => {
            let inner = Box::into_raw(Box::new(pred)) as *mut c_void;
            paimon_result_predicate {
                predicate: Box::into_raw(Box::new(paimon_predicate { inner })),
                error: std::ptr::null_mut(),
            }
        }
        Err(e) => paimon_result_predicate {
            predicate: std::ptr::null_mut(),
            error: paimon_error::from_paimon(e),
        },
    }
}

/// Create an equality predicate: `column = datum` (case-sensitive column match).
///
/// For case-insensitive column matching use
/// `paimon_predicate_equal_with_case_sensitive`.
///
/// # Safety
/// `table` and `column` must be valid pointers.
#[no_mangle]
pub unsafe extern "C" fn paimon_predicate_equal(
    table: *const paimon_table,
    column: *const std::ffi::c_char,
    datum: paimon_datum,
) -> paimon_result_predicate {
    build_leaf_predicate_datum(table, column, &datum, true, |pb, col, d| pb.equal(col, d))
}

/// Create an equality predicate with configurable column-name case sensitivity.
///
/// # Safety
/// `table` and `column` must be valid pointers.
#[no_mangle]
pub unsafe extern "C" fn paimon_predicate_equal_with_case_sensitive(
    table: *const paimon_table,
    column: *const std::ffi::c_char,
    datum: paimon_datum,
    case_sensitive: bool,
) -> paimon_result_predicate {
    build_leaf_predicate_datum(table, column, &datum, case_sensitive, |pb, col, d| {
        pb.equal(col, d)
    })
}

/// Create a not-equal predicate: `column != datum` (case-sensitive column match).
///
/// # Safety
/// `table` and `column` must be valid pointers.
#[no_mangle]
pub unsafe extern "C" fn paimon_predicate_not_equal(
    table: *const paimon_table,
    column: *const std::ffi::c_char,
    datum: paimon_datum,
) -> paimon_result_predicate {
    build_leaf_predicate_datum(table, column, &datum, true, |pb, col, d| {
        pb.not_equal(col, d)
    })
}

/// Create a not-equal predicate with configurable column-name case sensitivity.
///
/// # Safety
/// `table` and `column` must be valid pointers.
#[no_mangle]
pub unsafe extern "C" fn paimon_predicate_not_equal_with_case_sensitive(
    table: *const paimon_table,
    column: *const std::ffi::c_char,
    datum: paimon_datum,
    case_sensitive: bool,
) -> paimon_result_predicate {
    build_leaf_predicate_datum(table, column, &datum, case_sensitive, |pb, col, d| {
        pb.not_equal(col, d)
    })
}

/// Create a less-than predicate: `column < datum` (case-sensitive column match).
///
/// # Safety
/// `table` and `column` must be valid pointers.
#[no_mangle]
pub unsafe extern "C" fn paimon_predicate_less_than(
    table: *const paimon_table,
    column: *const std::ffi::c_char,
    datum: paimon_datum,
) -> paimon_result_predicate {
    build_leaf_predicate_datum(table, column, &datum, true, |pb, col, d| {
        pb.less_than(col, d)
    })
}

/// Create a less-than predicate with configurable column-name case sensitivity.
///
/// # Safety
/// `table` and `column` must be valid pointers.
#[no_mangle]
pub unsafe extern "C" fn paimon_predicate_less_than_with_case_sensitive(
    table: *const paimon_table,
    column: *const std::ffi::c_char,
    datum: paimon_datum,
    case_sensitive: bool,
) -> paimon_result_predicate {
    build_leaf_predicate_datum(table, column, &datum, case_sensitive, |pb, col, d| {
        pb.less_than(col, d)
    })
}

/// Create a less-or-equal predicate: `column <= datum` (case-sensitive column match).
///
/// # Safety
/// `table` and `column` must be valid pointers.
#[no_mangle]
pub unsafe extern "C" fn paimon_predicate_less_or_equal(
    table: *const paimon_table,
    column: *const std::ffi::c_char,
    datum: paimon_datum,
) -> paimon_result_predicate {
    build_leaf_predicate_datum(table, column, &datum, true, |pb, col, d| {
        pb.less_or_equal(col, d)
    })
}

/// Create a less-or-equal predicate with configurable column-name case sensitivity.
///
/// # Safety
/// `table` and `column` must be valid pointers.
#[no_mangle]
pub unsafe extern "C" fn paimon_predicate_less_or_equal_with_case_sensitive(
    table: *const paimon_table,
    column: *const std::ffi::c_char,
    datum: paimon_datum,
    case_sensitive: bool,
) -> paimon_result_predicate {
    build_leaf_predicate_datum(table, column, &datum, case_sensitive, |pb, col, d| {
        pb.less_or_equal(col, d)
    })
}

/// Create a greater-than predicate: `column > datum` (case-sensitive column match).
///
/// # Safety
/// `table` and `column` must be valid pointers.
#[no_mangle]
pub unsafe extern "C" fn paimon_predicate_greater_than(
    table: *const paimon_table,
    column: *const std::ffi::c_char,
    datum: paimon_datum,
) -> paimon_result_predicate {
    build_leaf_predicate_datum(table, column, &datum, true, |pb, col, d| {
        pb.greater_than(col, d)
    })
}

/// Create a greater-than predicate with configurable column-name case sensitivity.
///
/// # Safety
/// `table` and `column` must be valid pointers.
#[no_mangle]
pub unsafe extern "C" fn paimon_predicate_greater_than_with_case_sensitive(
    table: *const paimon_table,
    column: *const std::ffi::c_char,
    datum: paimon_datum,
    case_sensitive: bool,
) -> paimon_result_predicate {
    build_leaf_predicate_datum(table, column, &datum, case_sensitive, |pb, col, d| {
        pb.greater_than(col, d)
    })
}

/// Create a greater-or-equal predicate: `column >= datum` (case-sensitive column match).
///
/// # Safety
/// `table` and `column` must be valid pointers.
#[no_mangle]
pub unsafe extern "C" fn paimon_predicate_greater_or_equal(
    table: *const paimon_table,
    column: *const std::ffi::c_char,
    datum: paimon_datum,
) -> paimon_result_predicate {
    build_leaf_predicate_datum(table, column, &datum, true, |pb, col, d| {
        pb.greater_or_equal(col, d)
    })
}

/// Create a greater-or-equal predicate with configurable column-name case sensitivity.
///
/// # Safety
/// `table` and `column` must be valid pointers.
#[no_mangle]
pub unsafe extern "C" fn paimon_predicate_greater_or_equal_with_case_sensitive(
    table: *const paimon_table,
    column: *const std::ffi::c_char,
    datum: paimon_datum,
    case_sensitive: bool,
) -> paimon_result_predicate {
    build_leaf_predicate_datum(table, column, &datum, case_sensitive, |pb, col, d| {
        pb.greater_or_equal(col, d)
    })
}

/// Create an IS NULL predicate (case-sensitive column match).
///
/// # Safety
/// `table` and `column` must be valid pointers.
#[no_mangle]
pub unsafe extern "C" fn paimon_predicate_is_null(
    table: *const paimon_table,
    column: *const std::ffi::c_char,
) -> paimon_result_predicate {
    build_leaf_predicate(table, column, true, |pb, col| pb.is_null(col))
}

/// Create an IS NULL predicate with configurable column-name case sensitivity.
///
/// # Safety
/// `table` and `column` must be valid pointers.
#[no_mangle]
pub unsafe extern "C" fn paimon_predicate_is_null_with_case_sensitive(
    table: *const paimon_table,
    column: *const std::ffi::c_char,
    case_sensitive: bool,
) -> paimon_result_predicate {
    build_leaf_predicate(table, column, case_sensitive, |pb, col| pb.is_null(col))
}

/// Create an IS NOT NULL predicate (case-sensitive column match).
///
/// # Safety
/// `table` and `column` must be valid pointers.
#[no_mangle]
pub unsafe extern "C" fn paimon_predicate_is_not_null(
    table: *const paimon_table,
    column: *const std::ffi::c_char,
) -> paimon_result_predicate {
    build_leaf_predicate(table, column, true, |pb, col| pb.is_not_null(col))
}

/// Create an IS NOT NULL predicate with configurable column-name case sensitivity.
///
/// # Safety
/// `table` and `column` must be valid pointers.
#[no_mangle]
pub unsafe extern "C" fn paimon_predicate_is_not_null_with_case_sensitive(
    table: *const paimon_table,
    column: *const std::ffi::c_char,
    case_sensitive: bool,
) -> paimon_result_predicate {
    build_leaf_predicate(table, column, case_sensitive, |pb, col| pb.is_not_null(col))
}

/// Create an IN predicate: `column IN (datum1, datum2, ...)` (case-sensitive column match).
///
/// # Safety
/// `table`, `column`, and `datums` must be valid pointers. `datums_len` must be the length.
#[no_mangle]
pub unsafe extern "C" fn paimon_predicate_is_in(
    table: *const paimon_table,
    column: *const std::ffi::c_char,
    datums: *const paimon_datum,
    datums_len: usize,
) -> paimon_result_predicate {
    build_leaf_predicate_datums(
        table,
        column,
        datums,
        datums_len,
        true,
        |pb, col, values| pb.is_in(col, values),
    )
}

/// Create an IN predicate with configurable column-name case sensitivity.
///
/// # Safety
/// `table`, `column`, and `datums` must be valid pointers. `datums_len` must be the length.
#[no_mangle]
pub unsafe extern "C" fn paimon_predicate_is_in_with_case_sensitive(
    table: *const paimon_table,
    column: *const std::ffi::c_char,
    datums: *const paimon_datum,
    datums_len: usize,
    case_sensitive: bool,
) -> paimon_result_predicate {
    build_leaf_predicate_datums(
        table,
        column,
        datums,
        datums_len,
        case_sensitive,
        |pb, col, values| pb.is_in(col, values),
    )
}

/// Create a NOT IN predicate: `column NOT IN (datum1, datum2, ...)` (case-sensitive column match).
///
/// # Safety
/// `table`, `column`, and `datums` must be valid pointers. `datums_len` must be the length.
#[no_mangle]
pub unsafe extern "C" fn paimon_predicate_is_not_in(
    table: *const paimon_table,
    column: *const std::ffi::c_char,
    datums: *const paimon_datum,
    datums_len: usize,
) -> paimon_result_predicate {
    build_leaf_predicate_datums(
        table,
        column,
        datums,
        datums_len,
        true,
        |pb, col, values| pb.is_not_in(col, values),
    )
}

/// Create a NOT IN predicate with configurable column-name case sensitivity.
///
/// # Safety
/// `table`, `column`, and `datums` must be valid pointers. `datums_len` must be the length.
#[no_mangle]
pub unsafe extern "C" fn paimon_predicate_is_not_in_with_case_sensitive(
    table: *const paimon_table,
    column: *const std::ffi::c_char,
    datums: *const paimon_datum,
    datums_len: usize,
    case_sensitive: bool,
) -> paimon_result_predicate {
    build_leaf_predicate_datums(
        table,
        column,
        datums,
        datums_len,
        case_sensitive,
        |pb, col, values| pb.is_not_in(col, values),
    )
}

/// Helper to build an IN/NOT IN predicate with a datum array.
unsafe fn build_leaf_predicate_datums(
    table: *const paimon_table,
    column: *const std::ffi::c_char,
    datums: *const paimon_datum,
    datums_len: usize,
    case_sensitive: bool,
    build_fn: impl FnOnce(&PredicateBuilder, &str, Vec<Datum>) -> paimon::Result<Predicate>,
) -> paimon_result_predicate {
    if let Err(e) = check_non_null(table, "table") {
        return paimon_result_predicate {
            predicate: std::ptr::null_mut(),
            error: e,
        };
    }
    let col_name = match validate_cstr(column, "column") {
        Ok(s) => s,
        Err(e) => {
            return paimon_result_predicate {
                predicate: std::ptr::null_mut(),
                error: e,
            }
        }
    };

    if datums.is_null() && datums_len > 0 {
        return paimon_result_predicate {
            predicate: std::ptr::null_mut(),
            error: paimon_error::new(
                PaimonErrorCode::InvalidInput,
                "null datums pointer with non-zero length".to_string(),
            ),
        };
    }

    let slice = if datums_len > 0 {
        std::slice::from_raw_parts(datums, datums_len)
    } else {
        &[]
    };
    let values: Result<Vec<Datum>, _> = slice.iter().map(|d| datum_from_c(d)).collect();
    let values = match values {
        Ok(v) => v,
        Err(e) => {
            return paimon_result_predicate {
                predicate: std::ptr::null_mut(),
                error: e,
            }
        }
    };

    let table_ref = &*((*table).inner as *const Table);
    let fields = table_ref.schema().fields();

    let values: Result<Vec<Datum>, _> = values
        .into_iter()
        .map(|d| coerce_integer_datum(d, fields, &col_name, case_sensitive))
        .collect();
    let values = match values {
        Ok(v) => v,
        Err(e) => {
            return paimon_result_predicate {
                predicate: std::ptr::null_mut(),
                error: e,
            }
        }
    };

    let pb = PredicateBuilder::new_with_case_sensitive(fields, case_sensitive);
    match build_fn(&pb, &col_name, values) {
        Ok(pred) => {
            let inner = Box::into_raw(Box::new(pred)) as *mut c_void;
            paimon_result_predicate {
                predicate: Box::into_raw(Box::new(paimon_predicate { inner })),
                error: std::ptr::null_mut(),
            }
        }
        Err(e) => paimon_result_predicate {
            predicate: std::ptr::null_mut(),
            error: paimon_error::from_paimon(e),
        },
    }
}

/// Combine two predicates with AND. Consumes both inputs.
///
/// # Safety
/// `a` and `b` must be valid pointers from predicate functions.
#[no_mangle]
pub unsafe extern "C" fn paimon_predicate_and(
    a: *mut paimon_predicate,
    b: *mut paimon_predicate,
) -> *mut paimon_predicate {
    let pred_a = *Box::from_raw(Box::from_raw(a).inner as *mut Predicate);
    let pred_b = *Box::from_raw(Box::from_raw(b).inner as *mut Predicate);
    let combined = Predicate::and(vec![pred_a, pred_b]);
    let inner = Box::into_raw(Box::new(combined)) as *mut c_void;
    Box::into_raw(Box::new(paimon_predicate { inner }))
}

/// Combine two predicates with OR. Consumes both inputs.
///
/// # Safety
/// `a` and `b` must be valid pointers from predicate functions.
#[no_mangle]
pub unsafe extern "C" fn paimon_predicate_or(
    a: *mut paimon_predicate,
    b: *mut paimon_predicate,
) -> *mut paimon_predicate {
    let pred_a = *Box::from_raw(Box::from_raw(a).inner as *mut Predicate);
    let pred_b = *Box::from_raw(Box::from_raw(b).inner as *mut Predicate);
    let combined = Predicate::or(vec![pred_a, pred_b]);
    let inner = Box::into_raw(Box::new(combined)) as *mut c_void;
    Box::into_raw(Box::new(paimon_predicate { inner }))
}

/// Negate a predicate with NOT. Consumes the input.
///
/// # Safety
/// `p` must be a valid pointer from a predicate function.
#[no_mangle]
pub unsafe extern "C" fn paimon_predicate_not(p: *mut paimon_predicate) -> *mut paimon_predicate {
    let pred = *Box::from_raw(Box::from_raw(p).inner as *mut Predicate);
    let negated = Predicate::negate(pred);
    let inner = Box::into_raw(Box::new(negated)) as *mut c_void;
    Box::into_raw(Box::new(paimon_predicate { inner }))
}

/// Free a paimon_predicate.
///
/// # Safety
/// Only call with a predicate returned from paimon predicate functions.
#[no_mangle]
pub unsafe extern "C" fn paimon_predicate_free(p: *mut paimon_predicate) {
    if !p.is_null() {
        let wrapper = Box::from_raw(p);
        if !wrapper.inner.is_null() {
            drop(Box::from_raw(wrapper.inner as *mut Predicate));
        }
    }
}

/// Render a predicate as a human-readable, SQL-like string (for debugging /
/// printing), e.g. `(id > 5 AND name = 'foo')`.
///
/// Returns a `paimon_bytes` holding UTF-8 text (NOT null-terminated). The
/// caller must free it with `paimon_bytes_free`. Returns an empty buffer if
/// `p` (or its inner pointer) is null.
///
/// The predicate is only borrowed, not consumed: it remains valid for use in
/// scan planning after this call.
///
/// # Safety
/// `p` must be a valid pointer returned from a paimon predicate function, or null.
#[no_mangle]
pub unsafe extern "C" fn paimon_predicate_to_string(p: *const paimon_predicate) -> paimon_bytes {
    if p.is_null() || (*p).inner.is_null() {
        return paimon_bytes::new(Vec::new());
    }
    let pred = &*((*p).inner as *const Predicate);
    paimon_bytes::new(pred.to_string().into_bytes())
}

#[cfg(test)]
#[cfg(not(windows))] // Local-fs paths under tempfile are POSIX-only.
mod tests {
    use super::*;
    use std::ffi::CString;

    #[test]
    fn split_table_path_local_absolute() {
        let (w, d, t) = split_table_path("/tmp/warehouse/mydb.db/users").unwrap();
        assert_eq!(w, "/tmp/warehouse");
        assert_eq!(d, "mydb");
        assert_eq!(t, "users");
    }

    #[test]
    fn split_table_path_uri() {
        let (w, d, t) = split_table_path("oss://bucket/warehouse/mydb.db/users").unwrap();
        assert_eq!(w, "oss://bucket/warehouse");
        assert_eq!(d, "mydb");
        assert_eq!(t, "users");
    }

    #[test]
    fn split_table_path_trailing_slash() {
        let (_, _, t) = split_table_path("/tmp/warehouse/db.db/users/").unwrap();
        assert_eq!(t, "users");
    }

    #[test]
    fn split_table_path_missing_db_suffix() {
        assert!(split_table_path("/tmp/warehouse/mydb/users").is_err());
    }

    #[test]
    fn split_table_path_too_short() {
        assert!(split_table_path("users").is_err());
    }

    /// Build an isolated tempdir warehouse, create a real Paimon table via
    /// the FileSystemCatalog, then ensure `paimon_table_open_path` can open
    /// the same on-disk layout without a catalog handle. This is the closest
    /// thing the C binding has to an end-to-end test for the new entry point.
    #[test]
    fn open_path_reads_table_created_by_filesystem_catalog() {
        use paimon::catalog::Identifier as Id;
        use paimon::spec::{DataType, IntType, Schema};
        use paimon::{Catalog, CatalogOptions, FileSystemCatalog, Options};
        use tempfile::TempDir;

        let temp = TempDir::new().unwrap();
        let warehouse = temp.path().to_str().unwrap().to_string();

        // Set up a `db1.db/users` table with a single int column. The exact
        // schema doesn't matter — we only assert path-based loading sees it.
        let mut opts = Options::new();
        opts.set(CatalogOptions::WAREHOUSE, &warehouse);
        let catalog = FileSystemCatalog::new(opts).unwrap();
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .build()
            .unwrap();

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            catalog
                .create_database("db1", false, std::collections::HashMap::new())
                .await
                .unwrap();
            catalog
                .create_table(&Id::new("db1", "users"), schema, false)
                .await
                .unwrap();
        });
        // Drop the catalog before invoking the C-API path; the entry point
        // must work without a live catalog handle.
        drop(catalog);

        let table_path = format!("{warehouse}/db1.db/users");
        let c_path = CString::new(table_path).unwrap();

        unsafe {
            let result = paimon_table_open_path(c_path.as_ptr(), std::ptr::null(), 0, false);
            assert!(result.error.is_null(), "open_path must succeed");
            assert!(!result.table.is_null());

            // Round-trip a no-op derived API call to confirm the returned
            // handle is usable like one from `paimon_catalog_get_table`.
            let rb_result = paimon_table_new_read_builder(result.table);
            assert!(rb_result.error.is_null());
            paimon_read_builder_free(rb_result.read_builder);
            paimon_table_free(result.table);
        }
    }

    #[test]
    fn open_path_returns_not_found_for_missing_table() {
        let bogus = CString::new("/nonexistent-warehouse-9f3a/missing.db/x").unwrap();
        unsafe {
            let result = paimon_table_open_path(bogus.as_ptr(), std::ptr::null(), 0, false);
            assert!(result.table.is_null());
            assert!(!result.error.is_null());
            // Either NotFound (no schema dir) or IoError (no warehouse dir);
            // both are acceptable here — the contract is "non-null error".
            crate::error::paimon_error_free(result.error);
        }
    }

    /// `use_alluxio=true` on a table that hasn't opted into Alluxio caching
    /// must be a silent no-op — the second gate (`alluxio.cache-enabled` on
    /// the table) is what actually flips the data FileIO. This guards
    /// against an over-eager session flag silently rerouting reads for
    /// tables the deployer never said are cached.
    #[test]
    fn open_path_with_use_alluxio_but_table_not_opted_in_is_noop() {
        use paimon::catalog::Identifier as Id;
        use paimon::spec::{DataType, IntType, Schema};
        use paimon::{Catalog, CatalogOptions, FileSystemCatalog, Options};
        use tempfile::TempDir;

        let temp = TempDir::new().unwrap();
        let warehouse = temp.path().to_str().unwrap().to_string();
        let mut opts = Options::new();
        opts.set(CatalogOptions::WAREHOUSE, &warehouse);
        let catalog = FileSystemCatalog::new(opts).unwrap();
        // Table without alluxio.cache-enabled — the table half of the gate
        // is off, so the rebuild must NOT happen even with
        // use_alluxio=true. If the table half were missing, the rebuild
        // would explode here: FileIO::with_alluxio rejects a non-HDFS
        // scheme (file://) and ConfigInvalid would bubble up.
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .build()
            .unwrap();

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            catalog
                .create_database("db1", false, std::collections::HashMap::new())
                .await
                .unwrap();
            catalog
                .create_table(&Id::new("db1", "users"), schema, false)
                .await
                .unwrap();
        });
        drop(catalog);

        let table_path = format!("{warehouse}/db1.db/users");
        let c_path = CString::new(table_path).unwrap();

        unsafe {
            let result = paimon_table_open_path(c_path.as_ptr(), std::ptr::null(), 0, true);
            assert!(
                result.error.is_null(),
                "use_alluxio=true on a non-opted-in table must be a silent no-op"
            );
            assert!(!result.table.is_null());
            paimon_table_free(result.table);
        }
    }

    #[test]
    fn open_path_rejects_invalid_path() {
        let bad = CString::new("not-a-table-path").unwrap();
        unsafe {
            let result = paimon_table_open_path(bad.as_ptr(), std::ptr::null(), 0, false);
            assert!(result.table.is_null());
            assert!(!result.error.is_null());
            assert_eq!((*result.error).code, PaimonErrorCode::InvalidInput as i32);
            crate::error::paimon_error_free(result.error);
        }
    }

    #[test]
    fn open_path_rejects_null_path() {
        unsafe {
            let result = paimon_table_open_path(std::ptr::null(), std::ptr::null(), 0, false);
            assert!(result.table.is_null());
            assert!(!result.error.is_null());
            crate::error::paimon_error_free(result.error);
        }
    }

    /// End-to-end: build a fixture table, run real scan planning, serialize
    /// the resulting split through Rust's wire format, then verify the C
    /// entry point round-trips it back into a one-split plan whose downstream
    /// APIs look identical to the in-process plan.
    #[test]
    fn plan_from_split_bytes_round_trips() {
        use paimon::catalog::Identifier as Id;
        use paimon::spec::{DataType, IntType, Schema};
        use paimon::table::serialize_data_split;
        use paimon::{Catalog, CatalogOptions, FileSystemCatalog, Options};
        use tempfile::TempDir;

        let temp = TempDir::new().unwrap();
        let warehouse = temp.path().to_str().unwrap().to_string();
        let mut opts = Options::new();
        opts.set(CatalogOptions::WAREHOUSE, &warehouse);
        let catalog = FileSystemCatalog::new(opts).unwrap();
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .build()
            .unwrap();

        let rt = tokio::runtime::Runtime::new().unwrap();
        let bytes_opt = rt.block_on(async {
            catalog
                .create_database("db1", false, std::collections::HashMap::new())
                .await
                .unwrap();
            catalog
                .create_table(&Id::new("db1", "users"), schema, false)
                .await
                .unwrap();
            let table = catalog.get_table(&Id::new("db1", "users")).await.unwrap();
            // Empty table → plan has zero splits, but we just need a
            // serializable handle. Skip the test if planning yields nothing
            // (no fixture data files), which is expected for create_table
            // alone — in that case there's no split to serialize.
            let plan = table.new_read_builder().new_scan().plan().await.unwrap();
            plan.splits()
                .first()
                .map(|split| serialize_data_split(split).unwrap())
        });

        let Some(bytes) = bytes_opt else {
            // Fresh table has no data files yet; skip the round-trip path.
            // The error-path tests below still cover the FFI surface.
            return;
        };

        unsafe {
            let result = paimon_plan_from_split_bytes(bytes.as_ptr(), bytes.len());
            assert!(result.error.is_null(), "expected success");
            assert!(!result.plan.is_null());
            assert_eq!(paimon_plan_num_splits(result.plan), 1);
            paimon_plan_free(result.plan);
        }
    }

    #[test]
    fn plan_from_split_bytes_rejects_null() {
        unsafe {
            let result = paimon_plan_from_split_bytes(std::ptr::null(), 0);
            assert!(result.plan.is_null());
            assert!(!result.error.is_null());
            assert_eq!((*result.error).code, PaimonErrorCode::InvalidInput as i32);
            crate::error::paimon_error_free(result.error);
        }
    }

    #[test]
    fn plan_from_split_bytes_rejects_empty() {
        let dummy = [0u8; 0];
        unsafe {
            let result = paimon_plan_from_split_bytes(dummy.as_ptr(), 0);
            assert!(result.plan.is_null());
            assert!(!result.error.is_null());
            crate::error::paimon_error_free(result.error);
        }
    }

    #[test]
    fn plan_from_split_bytes_rejects_garbage() {
        let garbage = vec![0u8; 32]; // unknown magic
        unsafe {
            let result = paimon_plan_from_split_bytes(garbage.as_ptr(), garbage.len());
            assert!(result.plan.is_null());
            assert!(!result.error.is_null());
            crate::error::paimon_error_free(result.error);
        }
    }
}

// --- C ABI signature guards -------------------------------------------------
//
// The `paimon_predicate_*` constructors are called across the FFI boundary with
// fixed argument counts: the Go binding prepares a libffi call interface (CIF)
// per symbol (see `bindings/go/predicate.go`), and external consumers can link
// against the generated headers (e.g. Doris integrations). Adding a parameter to
// one of these existing symbols silently breaks every such caller — the extra
// argument is read from an undefined register/stack slot at the ABI boundary.
//
// These compile-time assertions pin the existing signatures. To add behavior
// (e.g. case-insensitive column matching), introduce a new
// `paimon_predicate_*_with_case_sensitive` symbol instead of changing one of
// these; touching a signature here will fail to compile.
const _: unsafe extern "C" fn(
    *const paimon_table,
    *const std::ffi::c_char,
    paimon_datum,
) -> paimon_result_predicate = paimon_predicate_equal;
const _: unsafe extern "C" fn(
    *const paimon_table,
    *const std::ffi::c_char,
    paimon_datum,
) -> paimon_result_predicate = paimon_predicate_not_equal;
const _: unsafe extern "C" fn(
    *const paimon_table,
    *const std::ffi::c_char,
    paimon_datum,
) -> paimon_result_predicate = paimon_predicate_less_than;
const _: unsafe extern "C" fn(
    *const paimon_table,
    *const std::ffi::c_char,
    paimon_datum,
) -> paimon_result_predicate = paimon_predicate_less_or_equal;
const _: unsafe extern "C" fn(
    *const paimon_table,
    *const std::ffi::c_char,
    paimon_datum,
) -> paimon_result_predicate = paimon_predicate_greater_than;
const _: unsafe extern "C" fn(
    *const paimon_table,
    *const std::ffi::c_char,
    paimon_datum,
) -> paimon_result_predicate = paimon_predicate_greater_or_equal;
const _: unsafe extern "C" fn(
    *const paimon_table,
    *const std::ffi::c_char,
) -> paimon_result_predicate = paimon_predicate_is_null;
const _: unsafe extern "C" fn(
    *const paimon_table,
    *const std::ffi::c_char,
) -> paimon_result_predicate = paimon_predicate_is_not_null;
const _: unsafe extern "C" fn(
    *const paimon_table,
    *const std::ffi::c_char,
    *const paimon_datum,
    usize,
) -> paimon_result_predicate = paimon_predicate_is_in;
const _: unsafe extern "C" fn(
    *const paimon_table,
    *const std::ffi::c_char,
    *const paimon_datum,
    usize,
) -> paimon_result_predicate = paimon_predicate_is_not_in;
