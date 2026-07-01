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

//! Memory-counting global allocator for the C cdylib.
//!
//! Installed as `#[global_allocator]` in `lib.rs`, this wraps the system
//! allocator and, on every (de)allocation, attributes the byte delta to the
//! "current reader" via [`paimon::mem_tag`]. The C++ caller (Doris) reads each
//! reader's net byte count through `paimon_record_batch_reader_mem_net_bytes`
//! and charges it to the owning query's `MemTrackerLimiter`.
//!
//! The wrapper is unconditional: when no reader tag is installed on the
//! current thread (which is the case for every allocation that happens outside
//! a `paimon_record_batch_reader_next` call), `mem_tag::account` is a cheap
//! thread-local read that does nothing. So the only overhead on the global
//! hot path is one thread-local load + null check + the underlying System
//! allocator call.
//!
//! A `#[global_allocator]` only takes effect in the final artifact, which is
//! why this lives in the cdylib crate (`bindings/c`) rather than in the
//! reusable `paimon` library crate.

use std::alloc::{GlobalAlloc, Layout, System};

use paimon::mem_tag;

pub struct CountingAlloc;

unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = System.alloc(layout);
        if !ptr.is_null() {
            mem_tag::account(layout.size() as i64);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout);
        mem_tag::account(-(layout.size() as i64));
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = System.alloc_zeroed(layout);
        if !ptr.is_null() {
            mem_tag::account(layout.size() as i64);
        }
        ptr
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new_ptr = System.realloc(ptr, layout, new_size);
        if !new_ptr.is_null() {
            // `layout.size()` is the old size; charge only the difference.
            mem_tag::account(new_size as i64 - layout.size() as i64);
        }
        new_ptr
    }
}
