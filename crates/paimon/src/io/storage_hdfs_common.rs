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

//! Helpers shared by both HDFS-family backends (`storage_hdfs` for the
//! pure-Rust native client and `storage_hdfs_jni` for the libhdfs/JNI
//! client). Compiled whenever either feature is on, so each backend can
//! be enabled independently without forcing the other to come along.

use crate::error::Error;
use crate::Result;

/// Strip the first matching scheme prefix from `path`, returning the
/// `authority/path` remainder. Returns `None` if no listed prefix matched.
pub(crate) fn strip_hdfs_family_scheme<'a>(
    path: &'a str,
    allowed_prefixes: &[&str],
) -> Option<&'a str> {
    allowed_prefixes
        .iter()
        .find_map(|prefix| path.strip_prefix(prefix))
}

/// Parse an HDFS-family URL and return the path relative to the cluster
/// root. `allowed_prefixes` enumerates the schemes the caller accepts
/// (e.g. `["hdfs://", "viewfs://"]` for the native backend; the JNI
/// backend adds `"alluxio://"`). Errors include the allowed list so the
/// caller sees the exact set of schemes that would have matched.
///
/// Examples (with the native scheme set):
/// - `"hdfs://namenode:8020/warehouse/db/table"` -> `"warehouse/db/table"`
/// - `"viewfs://cluster/warehouse/db/table"`     -> `"warehouse/db/table"`
pub(crate) fn hdfs_family_relative_path<'a>(
    path: &'a str,
    allowed_prefixes: &[&str],
) -> Result<&'a str> {
    let after_scheme = strip_hdfs_family_scheme(path, allowed_prefixes).ok_or_else(|| {
        Error::ConfigInvalid {
            message: format!(
                "Invalid HDFS-family path: {path}, should start with one of: {}",
                allowed_prefixes.join(", ")
            ),
        }
    })?;
    match after_scheme.find('/') {
        Some(pos) => Ok(&after_scheme[pos + 1..]),
        None => Err(Error::ConfigInvalid {
            message: format!("Invalid HDFS-family path: {path}, missing path component"),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hdfs_family_relative_path_error_lists_allowed_schemes() {
        // The error message must enumerate the accepted schemes so callers
        // can diagnose mismatches without reading source.
        let err = hdfs_family_relative_path("s3://bucket/key", &["hdfs://", "viewfs://"])
            .unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("hdfs://"), "missing hdfs:// in {msg}");
        assert!(msg.contains("viewfs://"), "missing viewfs:// in {msg}");
    }

    #[test]
    fn test_hdfs_family_relative_path_accepts_extended_scheme_set() {
        // Calling with an extended scheme set should accept paths under any
        // of them. This is the contract the JNI backend relies on.
        let extended = ["hdfs://", "viewfs://", "alluxio://"];
        assert_eq!(
            hdfs_family_relative_path("alluxio://cluster/warehouse/db", &extended).unwrap(),
            "warehouse/db"
        );
        assert_eq!(
            hdfs_family_relative_path("hdfs://nn:8020/p/q", &extended).unwrap(),
            "p/q"
        );
    }

    #[test]
    fn test_hdfs_family_relative_path_missing_path_component() {
        // Authority-only URL with no `/<path>` part is rejected with a
        // distinct error message, mirroring the older
        // `hdfs_relative_path` contract.
        let err = hdfs_family_relative_path("hdfs://nn:8020", &["hdfs://"]).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("missing path component"), "got {msg}");
    }
}
