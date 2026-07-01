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

//! Per-thread memory accounting tag, shared by the C bindings' global
//! allocator and the readers in this crate.
//!
//! # Why this lives in `paimon` (not in `bindings/c`)
//!
//! The C cdylib installs a `#[global_allocator]` that, on every
//! alloc/dealloc, attributes the byte delta to the "current reader" so that
//! Doris (the C++ caller) can charge that memory to the right query's
//! `MemTrackerLimiter`. "Current reader" is a thread-local pointer to an
//! `AtomicI64` net-bytes counter owned by the live reader.
//!
//! Most of the read path decodes synchronously on the thread that calls the
//! FFI `..._next` function, so the thread-local tag set there covers it. The
//! exception is the Vortex format, which spawns a dedicated OS thread
//! (`run_vortex_on_thread`) to decode. That thread does not inherit the
//! parent's thread-local, so it must re-install the tag itself. Since
//! `run_vortex_on_thread` lives in this crate, the tag machinery has to be
//! reachable from here — hence this module rather than `bindings/c`.
//!
//! When no tag is installed (the common case for any binary that links this
//! crate without the counting allocator, e.g. tests, the REST server, or the
//! Python bindings) [`account`] is a cheap thread-local read + null check and
//! does nothing else.

use std::cell::Cell;
use std::sync::atomic::{AtomicI64, Ordering};

thread_local! {
    /// Points to the net-bytes counter of the reader currently executing on
    /// this thread, or null when nothing is being accounted.
    static CURRENT_COUNTER: Cell<*const AtomicI64> = const { Cell::new(std::ptr::null()) };
}

/// Attribute `delta` bytes (positive on alloc, negative on dealloc) to the
/// counter installed on the current thread, if any. Called from the global
/// allocator hot path, so it must stay allocation-free and cheap.
/// Attribute `delta` bytes (positive on alloc, negative on free) to the
/// counter installed on the current thread, if any. Called explicitly from the
/// read path (sort_merge / data_file_reader) when a large arrow batch is
/// buffered or released; must run on the tagged scanner thread.
#[inline]
pub fn account(delta: i64) {
    CURRENT_COUNTER.with(|c| {
        let p = c.get();
        if !p.is_null() {
            // SAFETY: a non-null tag is only ever installed via `set_current`
            // / `CounterGuard` for the duration of a call into a live reader,
            // and is always restored before that reader can be freed. The
            // pointee `AtomicI64` therefore outlives every accounted access.
            unsafe { (*p).fetch_add(delta, Ordering::Relaxed) };
        }
    });
}

/// Install `counter` as the current thread's accounting target, returning the
/// previous target so it can be restored (supports nesting). Pass null to
/// clear. Prefer [`CounterGuard`] over calling this directly.
#[inline]
pub fn set_current(counter: *const AtomicI64) -> *const AtomicI64 {
    CURRENT_COUNTER.with(|c| {
        let old = c.get();
        c.set(counter);
        old
    })
}

/// Restore a target previously returned by [`set_current`].
#[inline]
pub fn restore_current(old: *const AtomicI64) {
    CURRENT_COUNTER.with(|c| c.set(old));
}

/// Read the current thread's accounting target. Used to propagate the tag to
/// a worker thread (e.g. the Vortex decode thread). The returned pointer is
/// only valid while the owning reader is alive; callers that move it to
/// another thread must ensure that thread is joined before the reader is
/// freed (see `run_vortex_on_thread`).
#[inline]
pub fn current() -> *const AtomicI64 {
    CURRENT_COUNTER.with(|c| c.get())
}

/// RAII guard that installs an accounting target on construction and restores
/// the previous one on drop.
pub struct CounterGuard(*const AtomicI64);

impl CounterGuard {
    /// Install `counter` for the lifetime of the guard.
    #[inline]
    pub fn enter(counter: *const AtomicI64) -> Self {
        CounterGuard(set_current(counter))
    }
}

impl Drop for CounterGuard {
    #[inline]
    fn drop(&mut self) {
        restore_current(self.0);
    }
}

/// RAII accounting for a fixed number of bytes held for the guard's lifetime:
/// charges `bytes` to the current thread's tag on construction and releases the
/// same amount on drop. Used by the read path to account resident arrow /
/// decode buffers (`account` is a no-op when no tag is installed).
pub struct ScopedBytes {
    bytes: i64,
}

impl ScopedBytes {
    #[inline]
    pub fn new(bytes: i64) -> Self {
        account(bytes);
        ScopedBytes { bytes }
    }
}

impl Drop for ScopedBytes {
    #[inline]
    fn drop(&mut self) {
        account(-self.bytes);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn account_is_noop_without_tag() {
        // No tag installed on this thread: must not panic or touch anything.
        account(123);
        account(-123);
        assert!(current().is_null());
    }

    #[test]
    fn guard_sets_and_restores() {
        let counter = AtomicI64::new(0);
        let ptr = &counter as *const AtomicI64;
        assert!(current().is_null());
        {
            let _g = CounterGuard::enter(ptr);
            assert_eq!(current(), ptr);
            account(1024);
            account(-24);
        }
        // Restored to null after the guard drops.
        assert!(current().is_null());
        assert_eq!(counter.load(Ordering::Relaxed), 1000);
    }

    #[test]
    fn guards_nest() {
        let a = AtomicI64::new(0);
        let b = AtomicI64::new(0);
        let pa = &a as *const AtomicI64;
        let pb = &b as *const AtomicI64;
        {
            let _ga = CounterGuard::enter(pa);
            account(10);
            {
                let _gb = CounterGuard::enter(pb);
                account(100);
            }
            // back to a
            assert_eq!(current(), pa);
            account(1);
        }
        assert!(current().is_null());
        assert_eq!(a.load(Ordering::Relaxed), 11);
        assert_eq!(b.load(Ordering::Relaxed), 100);
    }
}
