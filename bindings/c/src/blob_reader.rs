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
use std::ffi::c_void;
use std::io::SeekFrom;

use paimon::{BlobReader, BlobStream};

use crate::error::{check_non_null, paimon_error, validate_cstr, PaimonErrorCode};
use crate::result::{
    paimon_result_blob_reader, paimon_result_blob_stream, paimon_result_blob_stream_read,
    paimon_result_blob_stream_seek, paimon_result_read_blobs,
};
use crate::runtime;
use crate::types::{
    paimon_blob_reader, paimon_blob_stream, paimon_byte_slice, paimon_bytes_array, paimon_option,
    paimon_table,
};

fn new_reader(reader: BlobReader) -> paimon_result_blob_reader {
    let reader = Box::new(reader);
    let wrapper = Box::new(paimon_blob_reader {
        inner: Box::into_raw(reader) as *mut c_void,
    });
    paimon_result_blob_reader {
        reader: Box::into_raw(wrapper),
        error: std::ptr::null_mut(),
    }
}

fn read_error(error: *mut paimon_error) -> paimon_result_read_blobs {
    paimon_result_read_blobs {
        blobs: paimon_bytes_array::empty(),
        error,
    }
}

fn reader_error(error: *mut paimon_error) -> paimon_result_blob_reader {
    paimon_result_blob_reader {
        reader: std::ptr::null_mut(),
        error,
    }
}

fn stream_error(error: *mut paimon_error) -> paimon_result_blob_stream {
    paimon_result_blob_stream {
        stream: std::ptr::null_mut(),
        error,
    }
}

/// # Safety
/// `options` is null for zero length or points to valid UTF-8 C-string pairs.
#[no_mangle]
pub unsafe extern "C" fn paimon_blob_reader_new(
    options: *const paimon_option,
    options_len: usize,
) -> paimon_result_blob_reader {
    if options_len > 0 && options.is_null() {
        return reader_error(paimon_error::new(
            PaimonErrorCode::InvalidInput,
            "null pointer passed for `options`".to_string(),
        ));
    }

    let mut storage_options = HashMap::with_capacity(options_len);
    if options_len > 0 {
        for option in std::slice::from_raw_parts(options, options_len) {
            let key = match validate_cstr(option.key, "option key") {
                Ok(value) => value,
                Err(error) => return reader_error(error),
            };
            let value = match validate_cstr(option.value, "option value") {
                Ok(value) => value,
                Err(error) => return reader_error(error),
            };
            storage_options.insert(key, value);
        }
    }

    new_reader(BlobReader::new(storage_options))
}

/// Create a reader using a table's FileIO.
///
/// # Safety
/// `table` is a valid handle returned by the Paimon C API.
#[no_mangle]
pub unsafe extern "C" fn paimon_table_new_blob_reader(
    table: *const paimon_table,
) -> paimon_result_blob_reader {
    if let Err(error) = check_non_null(table, "table") {
        return reader_error(error);
    }

    let table = &*((*table).inner as *const paimon::Table);
    new_reader(BlobReader::from_file_io(table.file_io().clone()))
}

/// # Safety
/// The handle and input slices are valid for this call. Free the output with
/// `paimon_bytes_array_free`.
#[no_mangle]
pub unsafe extern "C" fn paimon_blob_reader_read_blobs(
    reader: *const paimon_blob_reader,
    descriptors: *const paimon_byte_slice,
    descriptors_len: usize,
) -> paimon_result_read_blobs {
    if let Err(error) = check_non_null(reader, "blob reader") {
        return read_error(error);
    }
    if descriptors_len > 0 && descriptors.is_null() {
        return read_error(paimon_error::new(
            PaimonErrorCode::InvalidInput,
            "null pointer passed for `descriptors`".to_string(),
        ));
    }

    let mut owned = Vec::with_capacity(descriptors_len);
    if descriptors_len > 0 {
        for (index, descriptor) in std::slice::from_raw_parts(descriptors, descriptors_len)
            .iter()
            .enumerate()
        {
            if descriptor.len > 0 && descriptor.data.is_null() {
                return read_error(paimon_error::new(
                    PaimonErrorCode::InvalidInput,
                    format!(
                        "null data pointer for BlobDescriptor input index {index}, URI unavailable"
                    ),
                ));
            }
            let bytes = if descriptor.len == 0 {
                &[]
            } else {
                std::slice::from_raw_parts(descriptor.data, descriptor.len)
            };
            owned.push(bytes.to_vec());
        }
    }

    let reader = &*((*reader).inner as *const BlobReader);
    match runtime().block_on(reader.read_blobs(&owned)) {
        Ok(values) => paimon_result_read_blobs {
            blobs: paimon_bytes_array::new(values),
            error: std::ptr::null_mut(),
        },
        Err(error) => read_error(paimon_error::from_paimon(error)),
    }
}

/// Open one descriptor for incremental reads.
///
/// # Safety
/// `reader` is valid and `descriptor` points to `descriptor_len` bytes.
#[no_mangle]
pub unsafe extern "C" fn paimon_blob_reader_open_blob(
    reader: *const paimon_blob_reader,
    descriptor: *const u8,
    descriptor_len: usize,
) -> paimon_result_blob_stream {
    if let Err(error) = check_non_null(reader, "blob reader") {
        return stream_error(error);
    }
    if descriptor_len > 0 && descriptor.is_null() {
        return stream_error(paimon_error::new(
            PaimonErrorCode::InvalidInput,
            "null pointer passed for `descriptor`".to_string(),
        ));
    }
    let bytes = if descriptor_len == 0 {
        &[]
    } else {
        std::slice::from_raw_parts(descriptor, descriptor_len)
    };
    let reader = &*((*reader).inner as *const BlobReader);
    match reader.open_blob(bytes) {
        Ok(stream) => {
            let stream = Box::new(stream);
            let wrapper = Box::new(paimon_blob_stream {
                inner: Box::into_raw(stream) as *mut c_void,
            });
            paimon_result_blob_stream {
                stream: Box::into_raw(wrapper),
                error: std::ptr::null_mut(),
            }
        }
        Err(error) => stream_error(paimon_error::from_paimon(error)),
    }
}

/// Read at most `buffer_len` bytes into caller-owned memory.
///
/// A zero `bytes_read` result means end of stream when `buffer_len` is nonzero.
///
/// # Safety
/// `stream` is valid and `buffer` points to `buffer_len` writable bytes.
#[no_mangle]
pub unsafe extern "C" fn paimon_blob_stream_read(
    stream: *mut paimon_blob_stream,
    buffer: *mut u8,
    buffer_len: usize,
) -> paimon_result_blob_stream_read {
    if let Err(error) = check_non_null(stream, "blob stream") {
        return paimon_result_blob_stream_read {
            bytes_read: 0,
            error,
        };
    }
    if buffer_len > 0 && buffer.is_null() {
        return paimon_result_blob_stream_read {
            bytes_read: 0,
            error: paimon_error::new(
                PaimonErrorCode::InvalidInput,
                "null pointer passed for `buffer`".to_string(),
            ),
        };
    }

    let stream = &mut *((*stream).inner as *mut BlobStream);
    match runtime().block_on(stream.read(buffer_len)) {
        Ok(bytes) => {
            if !bytes.is_empty() {
                std::ptr::copy_nonoverlapping(bytes.as_ptr(), buffer, bytes.len());
            }
            paimon_result_blob_stream_read {
                bytes_read: bytes.len(),
                error: std::ptr::null_mut(),
            }
        }
        Err(error) => paimon_result_blob_stream_read {
            bytes_read: 0,
            error: paimon_error::from_paimon(error),
        },
    }
}

/// Seek within the descriptor's range. `whence` uses the standard 0, 1, 2 values.
///
/// # Safety
/// `stream` is valid.
#[no_mangle]
pub unsafe extern "C" fn paimon_blob_stream_seek(
    stream: *mut paimon_blob_stream,
    offset: i64,
    whence: i32,
) -> paimon_result_blob_stream_seek {
    if let Err(error) = check_non_null(stream, "blob stream") {
        return paimon_result_blob_stream_seek { position: 0, error };
    }
    let from = match whence {
        0 if offset >= 0 => SeekFrom::Start(offset as u64),
        1 => SeekFrom::Current(offset),
        2 => SeekFrom::End(offset),
        _ => {
            return paimon_result_blob_stream_seek {
                position: 0,
                error: paimon_error::new(
                    PaimonErrorCode::InvalidInput,
                    "invalid blob stream seek".to_string(),
                ),
            };
        }
    };
    let stream = &mut *((*stream).inner as *mut BlobStream);
    match runtime().block_on(stream.seek(from)) {
        Ok(position) => paimon_result_blob_stream_seek {
            position,
            error: std::ptr::null_mut(),
        },
        Err(error) => paimon_result_blob_stream_seek {
            position: 0,
            error: paimon_error::from_paimon(error),
        },
    }
}

/// # Safety
/// `stream` is null or was returned by `paimon_blob_reader_open_blob`.
#[no_mangle]
pub unsafe extern "C" fn paimon_blob_stream_free(stream: *mut paimon_blob_stream) {
    if stream.is_null() {
        return;
    }
    let stream = Box::from_raw(stream);
    if !stream.inner.is_null() {
        drop(Box::from_raw(stream.inner as *mut BlobStream));
    }
}

/// # Safety
/// `reader` is null or was returned by `paimon_blob_reader_new`.
#[no_mangle]
pub unsafe extern "C" fn paimon_blob_reader_free(reader: *mut paimon_blob_reader) {
    if reader.is_null() {
        return;
    }
    let reader = Box::from_raw(reader);
    if !reader.inner.is_null() {
        drop(Box::from_raw(reader.inner as *mut BlobReader));
    }
}
