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

//! Optional jemalloc allocator + introspection helpers, used to investigate
//! high-RSS situations on the read path.
//!
//! Both deps are Linux-only `optional = true` entries in `Cargo.toml`. Calling
//! `--features jemalloc` on macOS / Windows therefore still compiles, but the
//! symbols below collapse to no-ops — `Jemalloc` is not exported and the stats
//! helpers do nothing. This keeps `cargo build --features jemalloc` portable
//! while restricting the actual allocator swap to the platform that gets a
//! supported jemalloc build out of the box.
//!
//! # Wiring
//!
//! Install the allocator at a binary entry point (e.g. an `examples/*.rs` or
//! `src/main.rs`). Gate on both the feature AND `target_os = "linux"` so
//! non-Linux builds with `--features jemalloc` still link cleanly:
//!
//! ```ignore
//! #[cfg(all(feature = "jemalloc", target_os = "linux"))]
//! #[global_allocator]
//! static GLOBAL: paimon::alloc::Jemalloc = paimon::alloc::Jemalloc;
//! ```
//!
//! # Stats
//!
//! Call [`print_stats`] at lifecycle points (startup, post-scan, shutdown) to
//! emit `allocated / active / resident / mapped` to stderr. Without the
//! feature it is a no-op so leaving the calls in place costs nothing.
//!
//! # Heap profiling
//!
//! Build with `--features jemalloc-profiling` (first build is slow — jemalloc
//! is recompiled with `--enable-prof`) and run with profiling turned on:
//!
//! ```bash
//! MALLOC_CONF=prof:true,prof_active:true,lg_prof_sample:19,lg_prof_interval:30,prof_prefix:./jeprof \
//!     cargo run -p paimon --release --features jemalloc-profiling \
//!         --example read_local_demo -- <table_path>
//! ```
//!
//! `lg_prof_interval=30` writes a `.heap` file every `2^30` bytes allocated.
//! [`dump_heap_profile`] forces a snapshot on demand. Render with:
//!
//! ```bash
//! jeprof --collapsed ./target/release/examples/read_local_demo ./jeprof.*.heap \
//!     | flamegraph.pl > heap.svg
//! ```

#[cfg(all(feature = "jemalloc", target_os = "linux"))]
pub use tikv_jemallocator::Jemalloc;

#[cfg(all(feature = "jemalloc", target_os = "linux"))]
mod imp {
    use tikv_jemalloc_ctl::{epoch, stats};

    fn read() -> Option<(u64, u64, u64, u64)> {
        // jemalloc stats are cached per epoch; advance to refresh them.
        epoch::advance().ok()?;
        Some((
            stats::allocated::read().ok()? as u64,
            stats::active::read().ok()? as u64,
            stats::resident::read().ok()? as u64,
            stats::mapped::read().ok()? as u64,
        ))
    }

    fn human(bytes: u64) -> String {
        const UNITS: &[&str] = &["B", "KiB", "MiB", "GiB", "TiB"];
        let mut v = bytes as f64;
        let mut u = 0;
        while v >= 1024.0 && u < UNITS.len() - 1 {
            v /= 1024.0;
            u += 1;
        }
        format!("{v:.2}{}", UNITS[u])
    }

    pub fn print_stats(label: &str) {
        match read() {
            Some((allocated, active, resident, mapped)) => eprintln!(
                "[jemalloc:{label}] allocated={} active={} resident={} mapped={}",
                human(allocated),
                human(active),
                human(resident),
                human(mapped),
            ),
            None => eprintln!("[jemalloc:{label}] stats unavailable"),
        }
    }

    #[cfg(feature = "jemalloc-profiling")]
    pub fn dump_heap_profile(path: &str) -> Result<(), String> {
        use std::ffi::{c_char, CString};
        use tikv_jemalloc_ctl::raw;

        let cpath = CString::new(path).map_err(|e| e.to_string())?;
        // SAFETY: `prof.dump` is an MALLCTL key that takes a `*const c_char`
        // null-terminated filename. We pass exactly that type, and `cpath`
        // (the owning CString) lives until the end of the function so the
        // pointer is valid for the duration of the synchronous write.
        unsafe {
            raw::write::<*const c_char>(b"prof.dump\0", cpath.as_ptr())
                .map_err(|e| format!("prof.dump failed: {e}"))
        }
    }

    #[cfg(not(feature = "jemalloc-profiling"))]
    pub fn dump_heap_profile(_path: &str) -> Result<(), String> {
        Err("dump_heap_profile requires --features jemalloc-profiling".into())
    }
}

#[cfg(not(all(feature = "jemalloc", target_os = "linux")))]
mod imp {
    pub fn print_stats(_label: &str) {}

    pub fn dump_heap_profile(_path: &str) -> Result<(), String> {
        Err("dump_heap_profile requires --features jemalloc on Linux".into())
    }
}

pub use imp::{dump_heap_profile, print_stats};
