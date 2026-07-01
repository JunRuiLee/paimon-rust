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

//! Per-query memory accounting exposed to the C++ caller.
//!
//! The cdylib's global allocator (`crate::alloc`) attributes every Rust
//! alloc/dealloc to the "current" net-bytes counter installed on the calling
//! thread (via [`paimon::mem_tag`]). Here we expose an opaque counter handle so
//! the C++ side (Doris) can drive that installation itself.
//!
//! # Why C++ drives the tag (not the Rust `..._next` function)
//!
//! A reader's Rust allocations are not confined to the `..._next` call: the
//! Arrow batch buffers are allocated inside `..._next` but freed later, when
//! C++ destroys the imported batch (its release callback runs back into Rust,
//! on the scanner thread, outside the `..._next` stack). Conversely, the
//! table/schema/plan handles are allocated during open and freed during free.
//! If the tag only covered `..._next`, those allocs and frees would land on
//! opposite sides of the tag boundary and never balance — the counter would
//! drift (too large for batch buffers, negative for open-time structures).
//!
//! So Doris installs the tag for the whole span of every C++ method that calls
//! into Rust — `init_reader`, `get_next_block` (including the batch
//! destructor), and `close` — mirroring how Doris natively wraps a scanner
//! thread in `SCOPED_ATTACH_TASK`. Every Rust alloc and its matching dealloc
//! then fall inside the same tag, so the counter reflects the reader's true
//! resident memory at any moment.
//!
//! The handle is created/destroyed by Doris (it must outlive the reader, since
//! open-time allocations happen before the reader exists), and is independent
//! of any single `paimon_record_batch_reader`.

use std::ffi::c_void;
use std::sync::atomic::{AtomicI64, Ordering};

/// Opaque per-query memory counter. `inner` is a `Box<AtomicI64>` holding the
/// net allocated bytes (alloc − dealloc) attributed to this counter.
#[repr(C)]
pub struct paimon_mem_counter {
    inner: *mut c_void,
}

/// Create a memory counter. Returns an owning pointer the caller must release
/// with `paimon_mem_counter_destroy`.
#[no_mangle]
pub extern "C" fn paimon_mem_counter_create() -> *mut paimon_mem_counter {
    let counter = Box::new(AtomicI64::new(0));
    let wrapper = Box::new(paimon_mem_counter {
        inner: Box::into_raw(counter) as *mut c_void,
    });
    Box::into_raw(wrapper)
}

/// Destroy a counter created by `paimon_mem_counter_create`. No-op on null.
///
/// # Safety
/// `counter` must be null or a pointer from `paimon_mem_counter_create`, and
/// must not be currently installed as any thread's accounting target.
#[no_mangle]
pub unsafe extern "C" fn paimon_mem_counter_destroy(counter: *mut paimon_mem_counter) {
    if counter.is_null() {
        return;
    }
    let wrapper = Box::from_raw(counter);
    if !wrapper.inner.is_null() {
        drop(Box::from_raw(wrapper.inner as *mut AtomicI64));
    }
}

/// Install `counter` as the calling thread's accounting target for the
/// allocations that follow, returning the previous target as an opaque token.
/// Pass that token to `paimon_mem_counter_restore` to undo the installation.
/// A null `counter` installs "no target" (allocations are not attributed).
///
/// # Safety
/// `counter` must be null or a pointer from `paimon_mem_counter_create`, and
/// must stay alive until the matching `paimon_mem_counter_restore`.
#[no_mangle]
pub unsafe extern "C" fn paimon_mem_counter_enter(counter: *const paimon_mem_counter) -> *mut c_void {
    let target: *const AtomicI64 = if counter.is_null() || (*counter).inner.is_null() {
        std::ptr::null()
    } else {
        (*counter).inner as *const AtomicI64
    };
    paimon::mem_tag::set_current(target) as *mut c_void
}

/// Restore the accounting target to the token returned by a prior
/// `paimon_mem_counter_enter`.
///
/// # Safety
/// `old` must be a token returned by `paimon_mem_counter_enter` on this thread,
/// restored in LIFO order.
#[no_mangle]
pub unsafe extern "C" fn paimon_mem_counter_restore(old: *mut c_void) {
    paimon::mem_tag::restore_current(old as *const AtomicI64);
}

/// Net bytes (alloc − dealloc) currently attributed to `counter`. Returns 0 on
/// null.
///
/// # Safety
/// `counter` must be null or a pointer from `paimon_mem_counter_create`.
#[no_mangle]
pub unsafe extern "C" fn paimon_mem_counter_net_bytes(counter: *const paimon_mem_counter) -> i64 {
    if counter.is_null() || (*counter).inner.is_null() {
        return 0;
    }
    let atomic = &*((*counter).inner as *const AtomicI64);
    atomic.load(Ordering::Relaxed)
}
