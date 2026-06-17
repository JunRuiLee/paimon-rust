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

//! Generates `include/paimon.h` from this crate's `extern "C"` surface using
//! cbindgen. Runs on every `cargo build -p paimon-c`; the header is checked
//! into the repo so consumers don't need cbindgen themselves.

use std::env;
use std::path::PathBuf;

fn main() {
    let crate_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
    let config_path = crate_dir.join("cbindgen.toml");
    let out_path = crate_dir.join("include").join("paimon.h");

    println!("cargo:rerun-if-changed=src");
    println!("cargo:rerun-if-changed=cbindgen.toml");
    println!("cargo:rerun-if-changed=build.rs");

    let config =
        cbindgen::Config::from_file(&config_path).expect("failed to parse cbindgen.toml");

    let bindings = match cbindgen::Builder::new()
        .with_crate(&crate_dir)
        .with_config(config)
        .generate()
    {
        Ok(b) => b,
        Err(e) => {
            // Emit a warning rather than failing the build — cbindgen errors
            // are common on transient parser issues and shouldn't block the
            // .so itself from being produced.
            println!("cargo:warning=cbindgen generation failed: {e}");
            return;
        }
    };

    if let Some(parent) = out_path.parent() {
        std::fs::create_dir_all(parent).expect("create include/ dir");
    }
    bindings.write_to_file(&out_path);
}
