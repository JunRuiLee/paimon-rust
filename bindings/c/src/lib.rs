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

// This crate is the C binding for the Paimon project.
// So it's type node can't meet camel case.
#![allow(non_camel_case_types)]

#[allow(dead_code)]
mod alloc;
mod catalog;
mod error;
mod identifier;
mod mem;
mod result;
mod table;
mod types;

use std::cell::RefCell;
use std::future::Future;
use std::rc::Rc;
use std::sync::OnceLock;
use tokio::runtime::{Builder, Runtime};

// NOTE: the global allocator is intentionally DISABLED.
//
// It attributed every Rust allocation to the current thread's tag, but a large
// share of IO/decoding memory is allocated and/or freed on tokio blocking-pool
// threads (opendal `tokio::fs`, hyper) that carry no tag, producing GB-scale
// two-way drift in the query MemTracker (observed +173G reading alluxio, -33G
// reading local). Memory is now accounted EXPLICITLY in the read path
// (sort_merge / data_file_reader) via `paimon::mem_tag::account`, which runs on
// the tagged scanner thread. `alloc::CountingAlloc` is kept for reference.
//
// #[global_allocator]
// static GLOBAL: alloc::CountingAlloc = alloc::CountingAlloc;

/// Legacy shared multi-thread runtime. Retained for easy fallback/comparison
/// but intentionally NOT called: with it, futures spawned by the IO stack were
/// work-stolen to worker threads and dropped there (outside the memory tag),
/// producing GB-scale query MemTracker drift. `block_on` below uses a per-thread
/// current-thread runtime instead (see CT_RUNTIME). To revert, point `block_on`
/// Shared multi-thread runtime used by all `block_on` calls.
static RUNTIME: OnceLock<Runtime> = OnceLock::new();

fn runtime() -> &'static Runtime {
    RUNTIME.get_or_init(|| Runtime::new().expect("Failed to create tokio runtime"))
}

/// Run `future` to completion on the shared multi-thread runtime.
///
/// Memory is accounted explicitly in the read path (sort_merge /
/// data_file_reader / parquet) on the tagged scanner thread that polls the
/// stream, so it no longer matters which thread the IO stack allocates on —
/// the multi-thread runtime is used for its IO parallelism. (A per-thread
/// current-thread runtime is kept below as `block_on_current_thread` for
/// fallback; it was only needed by the retired global-allocator scheme.)
pub(crate) fn block_on<F: Future>(future: F) -> F::Output {
    runtime().block_on(future)
}

thread_local! {
    /// One current-thread tokio runtime per calling thread. Retained for
    /// fallback only (see `block_on_current_thread`); not used by `block_on`.
    static CT_RUNTIME: RefCell<Option<Rc<Runtime>>> = const { RefCell::new(None) };
}

/// Fallback: run `future` on this thread's own current-thread runtime, so every
/// task is polled and dropped on the calling thread. Only relevant if memory is
/// ever moved back to a thread-local-tag allocator scheme. Currently unused.
#[allow(dead_code)]
pub(crate) fn block_on_current_thread<F: Future>(future: F) -> F::Output {
    let rt = CT_RUNTIME.with(|cell| {
        cell.borrow_mut()
            .get_or_insert_with(|| {
                Rc::new(
                    Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .expect("Failed to create current-thread tokio runtime"),
                )
            })
            .clone()
    });
    rt.block_on(future)
}
