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

//! JNI-based HDFS-family backend, used to reach Alluxio (and any other
//! Hadoop FileSystem SPI implementation) from paimon-rust.
//!
//! # Why JNI when we already have a native HDFS backend
//!
//! The default `storage-hdfs` feature uses opendal's `services-hdfs-native`,
//! a pure-Rust implementation of the HDFS RPC protocol. Alluxio's master
//! does not speak HDFS RPC — it has its own gRPC protocol — so the native
//! backend cannot reach Alluxio directly. Bleem's BE side already solves
//! this by going through `org.apache.hadoop.fs.FileSystem.get(uri, conf)`
//! plus `alluxio-client.jar`'s Hadoop FS SPI registration
//! (`fs.alluxio.impl=alluxio.hadoop.FileSystem`). This module wires
//! paimon-rust into the same code path via opendal's libhdfs-backed
//! `services-hdfs` feature.
//!
//! See `docs/alluxio-via-libhdfs-impl-plan.md` for the full plan, the
//! two-level (`session_use_alluxio` ∧ `alluxio.cache-enabled`) gating
//! semantics, and the catalog/data FileIO split.
//!
//! # Deployment requirements
//!
//! Compile-time:
//! - `JAVA_HOME` must point at a JDK; opendal pulls in `hdfs-sys` which
//!   links against `libjvm.so/dylib` at build time.
//!
//! Runtime:
//! - A reachable `libjvm` (set `LD_LIBRARY_PATH` / `DYLD_LIBRARY_PATH` or
//!   `java.library.path`).
//! - The CLASSPATH must contain `alluxio-client-*.jar` plus a hadoop-common
//!   runtime (libhdfs needs hadoop's own jars to start the JVM).
//! - A hadoop conf dir (`HADOOP_CONF_DIR` or `HADOOP_HOME/etc/hadoop`)
//!   whose `core-site.xml` registers the Alluxio FS SPI:
//!     - `fs.alluxio.impl = alluxio.hadoop.FileSystem`
//!     - `fs.AbstractFileSystem.alluxio.impl = alluxio.hadoop.AlluxioFileSystem`
//!   Alluxio 2.x ships a `ServiceLoader` SPI that auto-registers these two
//!   keys, so explicit entries are only needed when the alluxio-client jar
//!   has been stripped.
//!
//! Process-global JVM:
//! - libhdfs calls `JNI_CreateJavaVM` exactly once per process. All
//!   `HdfsJni` storages in the same process share the same JVM; heap / GC
//!   are tuned via `LIBHDFS_OPTS` or hadoop's `HADOOP_OPTS`.
//!
//! # Path rewriting
//!
//! Paimon catalog metadata stays on the native backend and uses the
//! original `hdfs://` / `viewfs://` URL. Only data FileIOs that opt into
//! Alluxio (`Table::with_alluxio(true)`) hit this module. When they do,
//! `hdfs://nn:8020/p` and `viewfs://cluster/p` are rewritten to
//! `alluxio://nn:8020/p` / `alluxio://cluster/p` before reaching libhdfs,
//! mirroring bleem's `HiveKwaiUtils.convertToAlluxioPath`. The authority
//! is forwarded verbatim — environments where the Alluxio master name
//! differs from the viewfs mount-table name need to extend the mapping
//! (tracked in the plan's Risk #6).

use std::collections::HashMap;

use opendal::services::HdfsConfig;
use opendal::Operator;
use url::Url;

use crate::error::Error;
use crate::Result;

use super::storage_hdfs_common::hdfs_family_relative_path;

/// HDFS-family schemes accepted by this backend. Includes `alluxio://` since
/// `hdfs_to_alluxio_path` produces that form, plus the original `hdfs://` /
/// `viewfs://` for callers that pass an untranslated path (defensive).
pub(crate) const HDFS_JNI_SCHEME_PREFIXES: [&str; 3] =
    ["hdfs://", "viewfs://", "alluxio://"];

/// Optional namenode override — same key as the native backend, so callers
/// configuring paimon don't have to remember which HDFS implementation they
/// happen to be on.
const HDFS_NAME_NODE: &str = "hdfs.name-node";

/// Toggle `hdfsBuilderEnableAppend` on the libhdfs operator. Defaults to
/// the libhdfs default (off).
const HDFS_ENABLE_APPEND: &str = "hdfs.enable-append";

/// Optional kerberos ticket cache path forwarded to libhdfs.
const HDFS_KERBEROS_TICKET_CACHE_PATH: &str = "hdfs.kerberos-ticket-cache-path";

/// Optional `hadoop.user.name` override; useful for Alluxio impersonation
/// or when the binary runs under a different OS user than the table owner.
const HDFS_USER: &str = "hdfs.user";

/// Parse paimon catalog options into an [`HdfsConfig`].
///
/// All keys are optional. `name_node` will be derived from the path at
/// operator build time when missing. The set of recognised keys mirrors
/// the native backend so callers don't have to branch on backend choice.
pub(crate) fn hdfs_jni_config_parse(props: HashMap<String, String>) -> Result<HdfsConfig> {
    let mut cfg = HdfsConfig::default();

    cfg.name_node = props.get(HDFS_NAME_NODE).cloned();
    cfg.user = props.get(HDFS_USER).cloned();
    cfg.kerberos_ticket_cache_path = props.get(HDFS_KERBEROS_TICKET_CACHE_PATH).cloned();

    if let Some(v) = props.get(HDFS_ENABLE_APPEND) {
        if v.eq_ignore_ascii_case("true") {
            cfg.enable_append = true;
        }
    }

    Ok(cfg)
}

/// Rewrite an HDFS-family path so libhdfs goes through Alluxio's Hadoop FS
/// SPI.
///
/// Mirrors bleem's `HiveKwaiUtils.convertToAlluxioPath`: scheme is swapped
/// to `alluxio://` while authority and path are preserved verbatim.
/// `alluxio://` is returned unchanged so callers don't have to special-case
/// already-translated paths.
///
/// - `hdfs://nn:8020/p`     -> `alluxio://nn:8020/p`
/// - `viewfs://cluster/p`   -> `alluxio://cluster/p`
/// - `alluxio://cluster/p`  -> `alluxio://cluster/p`
/// - anything else          -> `ConfigInvalid`
pub(crate) fn hdfs_to_alluxio_path(path: &str) -> Result<String> {
    for prefix in HDFS_JNI_SCHEME_PREFIXES {
        if let Some(remainder) = path.strip_prefix(prefix) {
            return Ok(format!("alluxio://{remainder}"));
        }
    }
    Err(Error::ConfigInvalid {
        message: format!(
            "Cannot rewrite {path} to alluxio://: should start with one of: {}",
            HDFS_JNI_SCHEME_PREFIXES.join(", ")
        ),
    })
}

/// Build an [`Operator`] backed by libhdfs for the given path.
///
/// `path` is expected to already be in `alluxio://` form (the caller routes
/// through [`hdfs_to_alluxio_path`] before getting here). If the config has
/// no `name_node` set, it's derived from the path URL with the scheme
/// preserved — for `alluxio://`, that means libhdfs sees an
/// `alluxio://<authority>` name node and delegates to the Hadoop FS SPI.
///
/// `root` is forced to `/` so that paimon's absolute paths work; opendal
/// treats `root` as a prefix prepended to every IO call.
pub(crate) fn hdfs_jni_config_build(cfg: &HdfsConfig, path: &str) -> Result<Operator> {
    let mut cfg = cfg.clone();

    if cfg.name_node.is_none() {
        cfg.name_node = Some(name_node_from_url(path)?);
    }

    cfg.root = Some("/".to_string());

    Ok(Operator::from_config(cfg)
        .map_err(|e| Error::ConfigInvalid {
            message: format!("Failed to build HDFS JNI operator for {path}: {e}"),
        })?
        .finish())
}

/// Pull the `<scheme>://<host>[:port]` prefix out of a path. Scheme is
/// preserved verbatim — for an `alluxio://` URL libhdfs needs to see the
/// alluxio scheme so it routes to the Alluxio Hadoop FS implementation.
fn name_node_from_url(path: &str) -> Result<String> {
    let url = Url::parse(path).map_err(|_| Error::ConfigInvalid {
        message: format!("Invalid HDFS-family url: {path}"),
    })?;
    let scheme = url.scheme();
    let host = url.host_str().ok_or_else(|| Error::ConfigInvalid {
        message: format!("Invalid HDFS-family url: {path}, missing host"),
    })?;
    let port_part = url.port().map(|p| format!(":{p}")).unwrap_or_default();
    Ok(format!("{scheme}://{host}{port_part}"))
}

/// Path relative to the operator root for a libhdfs operator. Accepts all
/// HDFS-family schemes so callers don't have to track whether a path has
/// already been rewritten to `alluxio://`.
pub(crate) fn hdfs_jni_relative_path(path: &str) -> Result<&str> {
    hdfs_family_relative_path(path, &HDFS_JNI_SCHEME_PREFIXES)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_props(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn test_hdfs_to_alluxio_path_hdfs() {
        assert_eq!(
            hdfs_to_alluxio_path("hdfs://namenode:8020/warehouse/db/t").unwrap(),
            "alluxio://namenode:8020/warehouse/db/t"
        );
    }

    #[test]
    fn test_hdfs_to_alluxio_path_viewfs() {
        // viewfs authority is a mount-table name; bleem behaviour is to
        // forward it verbatim into the alluxio scheme.
        assert_eq!(
            hdfs_to_alluxio_path("viewfs://cluster/warehouse/db/t").unwrap(),
            "alluxio://cluster/warehouse/db/t"
        );
    }

    #[test]
    fn test_hdfs_to_alluxio_path_already_alluxio() {
        // Idempotent: caller doesn't have to remember whether a path was
        // already rewritten.
        assert_eq!(
            hdfs_to_alluxio_path("alluxio://master/warehouse").unwrap(),
            "alluxio://master/warehouse"
        );
    }

    #[test]
    fn test_hdfs_to_alluxio_path_rejects_unrelated_scheme() {
        let err = hdfs_to_alluxio_path("s3://bucket/key").unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("hdfs://"), "missing hdfs:// in {msg}");
        assert!(msg.contains("alluxio://"), "missing alluxio:// in {msg}");
    }

    #[test]
    fn test_hdfs_jni_config_parse_empty_props() {
        let cfg = hdfs_jni_config_parse(HashMap::new()).unwrap();
        assert!(cfg.name_node.is_none());
        assert!(cfg.user.is_none());
        assert!(cfg.kerberos_ticket_cache_path.is_none());
        assert!(!cfg.enable_append);
    }

    #[test]
    fn test_hdfs_jni_config_parse_picks_up_all_keys() {
        let props = make_props(&[
            ("hdfs.name-node", "alluxio://master:19998"),
            ("hdfs.enable-append", "true"),
            ("hdfs.user", "paimon"),
            ("hdfs.kerberos-ticket-cache-path", "/tmp/krb5cc_1000"),
        ]);
        let cfg = hdfs_jni_config_parse(props).unwrap();
        assert_eq!(cfg.name_node.as_deref(), Some("alluxio://master:19998"));
        assert_eq!(cfg.user.as_deref(), Some("paimon"));
        assert_eq!(
            cfg.kerberos_ticket_cache_path.as_deref(),
            Some("/tmp/krb5cc_1000")
        );
        assert!(cfg.enable_append);
    }

    #[test]
    fn test_hdfs_jni_config_parse_unrelated_keys_ignored() {
        // Object-store-style keys mixed in must not pollute the config — the
        // backend silently drops anything outside its known set.
        let props = make_props(&[
            ("s3.endpoint", "https://s3.amazonaws.com"),
            ("fs.oss.endpoint", "https://oss.aliyuncs.com"),
            ("hdfs.name-node", "alluxio://master"),
        ]);
        let cfg = hdfs_jni_config_parse(props).unwrap();
        assert_eq!(cfg.name_node.as_deref(), Some("alluxio://master"));
    }

    #[test]
    fn test_name_node_from_url_preserves_alluxio_scheme() {
        // libhdfs needs to see `alluxio://...` so it routes to the alluxio
        // Hadoop FS impl. Rewriting to `hdfs://` would break the SPI lookup.
        assert_eq!(
            name_node_from_url("alluxio://master:19998/warehouse").unwrap(),
            "alluxio://master:19998"
        );
    }

    #[test]
    fn test_name_node_from_url_missing_host_rejected() {
        let err = name_node_from_url("alluxio:///no/host").unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("missing host"), "got {msg}");
    }

    #[test]
    fn test_hdfs_jni_relative_path_accepts_alluxio() {
        assert_eq!(
            hdfs_jni_relative_path("alluxio://master/warehouse/db/t").unwrap(),
            "warehouse/db/t"
        );
    }

    #[test]
    fn test_hdfs_jni_relative_path_accepts_native_schemes() {
        // Defensive — JNI relative-path helper should still understand the
        // original native schemes so a caller that hands us a pre-rewrite
        // path doesn't fail in a confusing way.
        assert_eq!(
            hdfs_jni_relative_path("hdfs://nn:8020/p").unwrap(),
            "p"
        );
        assert_eq!(
            hdfs_jni_relative_path("viewfs://cluster/p").unwrap(),
            "p"
        );
    }

    #[test]
    #[ignore = "requires libjvm + hadoop CLASSPATH; covered by the alluxio smoke test"]
    fn test_hdfs_jni_config_build_extracts_name_node_from_alluxio_path() {
        let cfg = HdfsConfig::default();
        let op = hdfs_jni_config_build(&cfg, "alluxio://master:19998/warehouse/db").unwrap();
        // opendal's services-hdfs registers under the "hdfs" scheme; the
        // backend it actually drives is whatever libhdfs resolves the
        // name node URI to.
        assert_eq!(op.info().scheme().to_string(), "hdfs");
    }
}
