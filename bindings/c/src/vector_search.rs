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

//! C FFI bindings for vector search.
//!
//! Wraps the Rust vector-search builder over the C ABI: a
//! `paimon_vector_search_builder` is created from a table, then configured with
//! the query vector, target column, result limit, and an optional scalar
//! filter. This binding targets primary-key vector tables; a query that does
//! not resolve to the primary-key vector path fails loud.
//!
//! This module provides the builder constructor, its setters, the terminal
//! that runs the search and returns a streaming Arrow reader, and the free
//! function.

use std::collections::HashMap;
use std::ffi::{c_char, c_void};

use paimon::spec::Predicate;
use paimon::table::{deserialize_data_split, Table};

use crate::block_on;
use crate::error::{check_non_null, paimon_error, validate_cstr, PaimonErrorCode};
use crate::result::{paimon_result_record_batch_reader, paimon_result_vector_search_builder};
use crate::types::*;

/// Create a new vector-search builder from a Table.
///
/// # Safety
/// `table` must be a valid pointer from `paimon_catalog_get_table`, or null (returns error).
#[no_mangle]
pub unsafe extern "C" fn paimon_table_new_vector_search_builder(
    table: *const paimon_table,
) -> paimon_result_vector_search_builder {
    if let Err(e) = check_non_null(table, "table") {
        return paimon_result_vector_search_builder {
            builder: std::ptr::null_mut(),
            error: e,
        };
    }
    let table_ref = &*((*table).inner as *const Table);
    let state = VectorSearchState {
        table: table_ref.clone(),
        vector_column: None,
        query_vector: None,
        limit: None,
        options: HashMap::new(),
        filter: None,
    };
    let inner = Box::into_raw(Box::new(state)) as *mut c_void;
    paimon_result_vector_search_builder {
        builder: Box::into_raw(Box::new(paimon_vector_search_builder { inner })),
        error: std::ptr::null_mut(),
    }
}

/// Set the target vector column for a vector-search builder.
///
/// # Safety
/// `b` must be a valid pointer from `paimon_table_new_vector_search_builder`, or
/// null (returns error). `column` must be a valid C string.
#[no_mangle]
pub unsafe extern "C" fn paimon_vector_search_builder_with_vector_column(
    b: *mut paimon_vector_search_builder,
    column: *const c_char,
) -> *mut paimon_error {
    if let Err(e) = check_non_null(b, "b") {
        return e;
    }
    let col = match validate_cstr(column, "vector column") {
        Ok(s) => s,
        Err(e) => return e,
    };
    let state = &mut *((*b).inner as *mut VectorSearchState);
    state.vector_column = Some(col);
    std::ptr::null_mut()
}

/// Set the query vector for a vector-search builder.
///
/// The `len` floats at `data` are copied into the builder; the caller retains
/// ownership of `data`. An empty vector (`len == 0`) is rejected.
///
/// # Safety
/// `b` must be a valid pointer from `paimon_table_new_vector_search_builder`, or
/// null (returns error). `data` must point to `len` `f32` values when `len > 0`.
#[no_mangle]
pub unsafe extern "C" fn paimon_vector_search_builder_with_query_vector(
    b: *mut paimon_vector_search_builder,
    data: *const f32,
    len: usize,
) -> *mut paimon_error {
    if let Err(e) = check_non_null(b, "b") {
        return e;
    }
    if len == 0 {
        return paimon_error::new(
            PaimonErrorCode::InvalidInput,
            "query vector must not be empty".to_string(),
        );
    }
    if data.is_null() {
        return paimon_error::new(
            PaimonErrorCode::InvalidInput,
            "null query vector pointer with non-zero length".to_string(),
        );
    }
    let state = &mut *((*b).inner as *mut VectorSearchState);
    state.query_vector = Some(std::slice::from_raw_parts(data, len).to_vec());
    std::ptr::null_mut()
}

/// Set the maximum number of results for a vector-search builder.
///
/// # Safety
/// `b` must be a valid pointer from `paimon_table_new_vector_search_builder`, or
/// null (returns error).
#[no_mangle]
pub unsafe extern "C" fn paimon_vector_search_builder_with_limit(
    b: *mut paimon_vector_search_builder,
    limit: usize,
) -> *mut paimon_error {
    if let Err(e) = check_non_null(b, "b") {
        return e;
    }
    let state = &mut *((*b).inner as *mut VectorSearchState);
    state.limit = Some(limit);
    std::ptr::null_mut()
}

/// Set scan/search options for a vector-search builder.
///
/// # Safety
/// `b` must be a valid pointer from `paimon_table_new_vector_search_builder`, or
/// null (returns error). `options` must be a valid pointer to `len`
/// `paimon_option` values, or null when `len` is 0.
#[no_mangle]
pub unsafe extern "C" fn paimon_vector_search_builder_with_options(
    b: *mut paimon_vector_search_builder,
    options: *const paimon_option,
    len: usize,
) -> *mut paimon_error {
    if let Err(e) = check_non_null(b, "b") {
        return e;
    }
    if options.is_null() && len > 0 {
        return paimon_error::new(
            PaimonErrorCode::InvalidInput,
            "null options pointer with non-zero length".to_string(),
        );
    }
    let mut map = HashMap::with_capacity(len);
    if len > 0 {
        let slice = std::slice::from_raw_parts(options, len);
        for opt in slice {
            let key = match validate_cstr(opt.key, "option key") {
                Ok(s) => s,
                Err(e) => return e,
            };
            let value = match validate_cstr(opt.value, "option value") {
                Ok(s) => s,
                Err(e) => return e,
            };
            map.insert(key, value);
        }
    }
    let state = &mut *((*b).inner as *mut VectorSearchState);
    state.options = map;
    std::ptr::null_mut()
}

/// Set an optional scalar residual filter for a vector-search builder.
///
/// The predicate is consumed (ownership transferred to the builder). Pass null
/// to clear any previously set filter.
///
/// # Safety
/// `b` must be a valid pointer from `paimon_table_new_vector_search_builder`, or
/// null (returns error). `predicate` must be a valid pointer from a
/// `paimon_predicate_*` function, or null.
#[no_mangle]
pub unsafe extern "C" fn paimon_vector_search_builder_with_filter(
    b: *mut paimon_vector_search_builder,
    predicate: *mut paimon_predicate,
) -> *mut paimon_error {
    if let Err(e) = check_non_null(b, "b") {
        return e;
    }

    let state = &mut *((*b).inner as *mut VectorSearchState);

    if predicate.is_null() {
        state.filter = None;
        return std::ptr::null_mut();
    }

    let pred_wrapper = Box::from_raw(predicate);
    let pred = Box::from_raw(pred_wrapper.inner as *mut Predicate);
    state.filter = Some(*pred);
    std::ptr::null_mut()
}

/// Free a paimon_vector_search_builder.
///
/// # Safety
/// Only call with a builder returned from `paimon_table_new_vector_search_builder`.
#[no_mangle]
pub unsafe extern "C" fn paimon_vector_search_builder_free(b: *mut paimon_vector_search_builder) {
    if !b.is_null() {
        let wrapper = Box::from_raw(b);
        if !wrapper.inner.is_null() {
            drop(Box::from_raw(wrapper.inner as *mut VectorSearchState));
        }
    }
}

/// Execute the vector search and return a streaming Arrow reader over the
/// materialized rows (projected user columns plus `__paimon_search_score`).
/// Targets primary-key vector tables; a query that does not resolve to the
/// primary-key vector path fails loud. Consume via
/// `paimon_record_batch_reader_next` and free with `paimon_record_batch_reader_free`.
///
/// # Safety
/// `b` must be a valid pointer from `paimon_table_new_vector_search_builder`, or
/// null (returns an error result).
#[no_mangle]
pub unsafe extern "C" fn paimon_vector_search_builder_execute_read(
    b: *mut paimon_vector_search_builder,
) -> paimon_result_record_batch_reader {
    if let Err(e) = check_non_null(b, "b") {
        return paimon_result_record_batch_reader {
            reader: std::ptr::null_mut(),
            error: e,
        };
    }
    let state = &*((*b).inner as *const VectorSearchState);

    // This binding drives the primary-key vector path, whose ANN reader is
    // configured from the table options directly; there is no per-search
    // options override on the builder. Reject options rather than silently
    // dropping them so a caller relying on them fails loud.
    if !state.options.is_empty() {
        return paimon_result_record_batch_reader {
            reader: std::ptr::null_mut(),
            error: paimon_error::new(
                PaimonErrorCode::Unsupported,
                "per-search options are not supported; configure the ANN reader via table options"
                    .to_string(),
            ),
        };
    }

    let mut builder = state.table.new_vector_search_builder();
    if let Some(col) = &state.vector_column {
        builder.with_vector_column(col);
    }
    if let Some(v) = &state.query_vector {
        builder.with_query_vector(v.clone());
    }
    if let Some(limit) = state.limit {
        builder.with_limit(limit);
    }
    if let Some(f) = &state.filter {
        builder.with_filter(f.clone());
    }

    match block_on(builder.execute_read()) {
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

/// Execute the primary-key vector search scoped to a single caller-supplied
/// `DataSplit` (one bucket), returning a streaming Arrow reader over that split's
/// local Top-K (projected columns + `__paimon_search_score`, best-first). Intended
/// for a query engine that plans buckets itself and fans one whole-bucket split
/// out per node, then merges the per-split results by `__paimon_search_score`.
/// `split_bytes` is the Paimon-native serialized `DataSplit`.
///
/// # Safety
/// `b` must be a valid pointer from `paimon_table_new_vector_search_builder`, or
/// null (returns an error result). `split_bytes` must point to `split_len` valid,
/// initialized bytes that stay live for the duration of the call; the caller
/// retains ownership of the buffer (it is copied/decoded here, not freed).
#[no_mangle]
pub unsafe extern "C" fn paimon_vector_search_builder_execute_read_for_data_split(
    b: *mut paimon_vector_search_builder,
    split_bytes: *const u8,
    split_len: usize,
) -> paimon_result_record_batch_reader {
    if let Err(e) = check_non_null(b, "b") {
        return paimon_result_record_batch_reader {
            reader: std::ptr::null_mut(),
            error: e,
        };
    }
    if split_bytes.is_null() || split_len == 0 {
        return paimon_result_record_batch_reader {
            reader: std::ptr::null_mut(),
            error: paimon_error::new(
                PaimonErrorCode::InvalidInput,
                "execute_read_for_data_split: null or empty split bytes".to_string(),
            ),
        };
    }
    let state = &*((*b).inner as *const VectorSearchState);

    // Same as `execute_read`: the PK vector path configures its ANN reader from
    // table options, so reject per-search options rather than silently drop them.
    if !state.options.is_empty() {
        return paimon_result_record_batch_reader {
            reader: std::ptr::null_mut(),
            error: paimon_error::new(
                PaimonErrorCode::Unsupported,
                "per-search options are not supported; configure the ANN reader via table options"
                    .to_string(),
            ),
        };
    }

    let split = match deserialize_data_split(std::slice::from_raw_parts(split_bytes, split_len)) {
        Ok(s) => s,
        Err(e) => {
            return paimon_result_record_batch_reader {
                reader: std::ptr::null_mut(),
                error: paimon_error::from_paimon(e),
            };
        }
    };

    let mut builder = state.table.new_vector_search_builder();
    if let Some(col) = &state.vector_column {
        builder.with_vector_column(col);
    }
    if let Some(v) = &state.query_vector {
        builder.with_query_vector(v.clone());
    }
    if let Some(limit) = state.limit {
        builder.with_limit(limit);
    }
    if let Some(f) = &state.filter {
        builder.with_filter(f.clone());
    }

    match block_on(builder.execute_read_for_data_split(split)) {
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

// --- C ABI signature guards -------------------------------------------------
//
// These symbols are called across the FFI boundary with fixed argument counts:
// bindings prepare a libffi call interface (CIF) per symbol, and external
// consumers link against the generated headers (e.g. Doris integrations).
// Adding or reordering a parameter on one of these existing symbols silently
// breaks every such caller — the extra argument is read from an undefined
// register/stack slot at the ABI boundary.
//
// These compile-time assertions pin the existing signatures. To add behavior,
// introduce a new symbol instead of changing one of these; touching a signature
// here will fail to compile.
const _: unsafe extern "C" fn(*const paimon_table) -> paimon_result_vector_search_builder =
    paimon_table_new_vector_search_builder;
const _: unsafe extern "C" fn(
    *mut paimon_vector_search_builder,
    *const c_char,
) -> *mut paimon_error = paimon_vector_search_builder_with_vector_column;
const _: unsafe extern "C" fn(
    *mut paimon_vector_search_builder,
    *const f32,
    usize,
) -> *mut paimon_error = paimon_vector_search_builder_with_query_vector;
const _: unsafe extern "C" fn(*mut paimon_vector_search_builder, usize) -> *mut paimon_error =
    paimon_vector_search_builder_with_limit;
const _: unsafe extern "C" fn(
    *mut paimon_vector_search_builder,
    *const paimon_option,
    usize,
) -> *mut paimon_error = paimon_vector_search_builder_with_options;
const _: unsafe extern "C" fn(
    *mut paimon_vector_search_builder,
    *mut paimon_predicate,
) -> *mut paimon_error = paimon_vector_search_builder_with_filter;
const _: unsafe extern "C" fn(*mut paimon_vector_search_builder) =
    paimon_vector_search_builder_free;
const _: unsafe extern "C" fn(
    *mut paimon_vector_search_builder,
) -> paimon_result_record_batch_reader = paimon_vector_search_builder_execute_read;
const _: unsafe extern "C" fn(
    *mut paimon_vector_search_builder,
    *const u8,
    usize,
) -> paimon_result_record_batch_reader = paimon_vector_search_builder_execute_read_for_data_split;

#[cfg(test)]
mod tests {
    use super::*;

    // The happy path (searching a real bucket split) is covered end-to-end by the
    // Rust integration test `pk_vector_java_fixture_test`. Here we only pin the
    // C-symbol guard that needs no table: a null builder fails loud rather than
    // dereferencing a bad pointer.
    #[test]
    fn execute_read_for_data_split_rejects_null_builder() {
        let split = [1u8, 2, 3];
        unsafe {
            let result = paimon_vector_search_builder_execute_read_for_data_split(
                std::ptr::null_mut(),
                split.as_ptr(),
                split.len(),
            );
            assert!(result.reader.is_null());
            assert!(!result.error.is_null());
            crate::error::paimon_error_free(result.error);
        }
    }
}
