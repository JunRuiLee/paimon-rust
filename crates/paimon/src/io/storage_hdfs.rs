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
use std::sync::OnceLock;

use opendal::services::HdfsNativeConfig;
use opendal::Operator;
use url::Url;

use crate::error::Error;
use crate::Result;

use super::storage_hdfs_common::hdfs_family_relative_path;

/// Supported HDFS-family scheme prefixes for the native (hdfs-native) backend.
const HDFS_NATIVE_SCHEME_PREFIXES: [&str; 2] = ["hdfs://", "viewfs://"];

/// Native-backend convenience: only accepts `hdfs://` / `viewfs://`. The JNI
/// backend (`storage_hdfs_jni`) has its own helper that adds `alluxio://`.
pub(crate) fn hdfs_relative_path(path: &str) -> Result<&str> {
    hdfs_family_relative_path(path, &HDFS_NATIVE_SCHEME_PREFIXES)
}

/// Built-in Hadoop configuration baked into the binary, so that ViewFS works
/// out of the box without the deployer setting up `HADOOP_CONF_DIR`.
///
/// These are the `core-site.xml` (with the viewfs mount table + `linkFallback`
/// inlined) and `hdfs-site.xml` (with the target nameservice's HA config) that
/// hdfs-native needs. They are materialized to a process-private directory on
/// first ViewFS use; see [`ensure_builtin_hadoop_conf`].
const BUILTIN_CORE_SITE: &str = include_str!("../../resources/hadoop-conf/core-site.xml");
const BUILTIN_HDFS_SITE: &str = include_str!("../../resources/hadoop-conf/hdfs-site.xml");

/// Environment flag to opt back into an external Hadoop config.
///
/// By default the built-in config is used for `viewfs://` and any external
/// `HADOOP_CONF_DIR` / `HADOOP_HOME` is ignored. Set this to a truthy value
/// (`1` / `true` / `yes` / `on`) to instead defer to the external config.
const EXTERNAL_HADOOP_CONF_FLAG: &str = "PAIMON_USE_EXTERNAL_HADOOP_CONF";

/// Override for the base directory the built-in Hadoop conf is materialized
/// under.
///
/// By default the conf is written to `std::env::temp_dir()` (e.g. `/tmp`). Set
/// this to an absolute directory to place the `paimon-builtin-hadoop-conf`
/// subdir elsewhere — useful when `/tmp` is noexec / tiny / shared, or when the
/// deployer wants the materialized conf on a known, inspectable path.
const BUILTIN_HADOOP_CONF_DIR_PREFIX_ENV: &str = "PAIMON_BUILTIN_HADOOP_CONF_DIR_PREFIX";

/// Base directory under which the built-in Hadoop conf subdir is created.
///
/// Uses [`BUILTIN_HADOOP_CONF_DIR_PREFIX_ENV`] when set (and non-empty),
/// otherwise falls back to the system temp dir.
fn builtin_hadoop_conf_dir_prefix() -> std::path::PathBuf {
    match std::env::var(BUILTIN_HADOOP_CONF_DIR_PREFIX_ENV) {
        Ok(v) if !v.trim().is_empty() => std::path::PathBuf::from(v.trim()),
        _ => std::env::temp_dir(),
    }
}

/// Whether the deployer opted into using an external Hadoop config.
fn external_hadoop_conf_enabled() -> bool {
    std::env::var(EXTERNAL_HADOOP_CONF_FLAG)
        .map(|v| {
            matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false)
}

/// Make hdfs-native use the built-in Hadoop config for ViewFS.
///
/// By default this materializes the embedded `core-site.xml` / `hdfs-site.xml`
/// to a process-private directory and forces `HADOOP_CONF_DIR` to point at it,
/// **overriding any external `HADOOP_CONF_DIR` / `HADOOP_HOME`**, so deployment
/// needs no Hadoop config on disk.
///
/// Opt out by setting [`EXTERNAL_HADOOP_CONF_FLAG`] to a truthy value: then this
/// is a no-op and hdfs-native reads the external `HADOOP_CONF_DIR` /
/// `HADOOP_HOME` as usual. Called only for `viewfs://` paths (see
/// [`hdfs_config_build`]); plain `hdfs://` single-NameNode access needs no mount
/// table and is never affected.
///
/// # Concurrency
///
/// The materialize-and-`set_var` step runs **exactly once per process**, gated
/// by a [`OnceLock`]. Each `Storage::HdfsNative` instance has its own operator
/// lock, but they all materialize to the same fixed path
/// (`<prefix>/paimon-builtin-hadoop-conf/`). Without the gate, concurrent
/// instances (multiple queries / splits) would race on `std::fs::write`, which
/// truncates the target to zero before rewriting it — a reader building its
/// operator could then read a mid-write empty file and hit hdfs-native's
/// `the document does not have a root node` XML error. Doing the write once,
/// before any hdfs-native `Configuration` reads the files, removes the race.
fn ensure_builtin_hadoop_conf() -> Result<()> {
    if external_hadoop_conf_enabled() {
        // Deployer opted into their own HADOOP_CONF_DIR / HADOOP_HOME.
        return Ok(());
    }

    // Materialize the built-in conf and point HADOOP_CONF_DIR at it exactly
    // once. Subsequent calls return the cached result instead of rewriting the
    // shared files (which would truncate them and race concurrent readers).
    static MATERIALIZED: OnceLock<std::result::Result<(), String>> = OnceLock::new();
    MATERIALIZED
        .get_or_init(materialize_builtin_hadoop_conf)
        .clone()
        .map_err(|message| Error::ConfigInvalid { message })
}

/// Write the embedded conf files and set `HADOOP_CONF_DIR`. Runs once, under
/// the [`OnceLock`] in [`ensure_builtin_hadoop_conf`]. Returns a `String` error
/// (not [`Error`]) so the result is `Clone`-able for caching in the `OnceLock`.
fn materialize_builtin_hadoop_conf() -> std::result::Result<(), String> {
    let dir = builtin_hadoop_conf_dir_prefix().join("paimon-builtin-hadoop-conf");
    let write = |name: &str, content: &str| -> std::result::Result<(), String> {
        std::fs::write(dir.join(name), content)
            .map_err(|e| format!("Failed to write built-in hadoop conf {name} to {dir:?}: {e}"))
    };

    std::fs::create_dir_all(&dir)
        .map_err(|e| format!("Failed to create built-in hadoop conf dir {dir:?}: {e}"))?;
    write("core-site.xml", BUILTIN_CORE_SITE)?;
    write("hdfs-site.xml", BUILTIN_HDFS_SITE)?;

    // Force HADOOP_CONF_DIR to the built-in dir, overriding any external value.
    // hdfs-native reads it when the client is built; gated by the OnceLock so
    // this runs before any operator reads the files, never racing a rewrite.
    std::env::set_var("HADOOP_CONF_DIR", &dir);
    Ok(())
}

/// Configuration key for HDFS name node URL.
///
/// Example: "hdfs://namenode:8020" or "hdfs://nameservice1" (HA).
const HDFS_NAME_NODE: &str = "hdfs.name-node";

/// Configuration key to enable HDFS append capability.
const HDFS_ENABLE_APPEND: &str = "hdfs.enable-append";

/// Parse paimon catalog options into an [`HdfsNativeConfig`].
///
/// Extracts HDFS-related configuration from the properties map.
/// The `hdfs.name-node` key is optional — if omitted, the name node
/// will be extracted from the file path URL at operator build time.
pub(crate) fn hdfs_config_parse(props: HashMap<String, String>) -> Result<HdfsNativeConfig> {
    let mut cfg = HdfsNativeConfig::default();

    cfg.name_node = props.get(HDFS_NAME_NODE).cloned();

    if let Some(v) = props.get(HDFS_ENABLE_APPEND) {
        if v.eq_ignore_ascii_case("true") {
            cfg.enable_append = true;
        }
    }

    Ok(cfg)
}

/// Build an [`Operator`] for the given HDFS or ViewFS path.
///
/// If the config has no `name_node` set, it will be extracted from the path URL,
/// preserving the original scheme. The root is set to "/" so that relative paths
/// work correctly.
///
/// For `viewfs://` URLs the authority is the mount-table name, not a NameNode
/// host; the scheme must be kept intact so hdfs-native resolves the mount table
/// (loaded from Hadoop xml via `HADOOP_CONF_DIR` / `HADOOP_HOME`) instead of
/// trying to dial the mount-table name as a NameNode.
///
/// Example paths:
/// - "hdfs://namenode:8020/warehouse/db/table"
/// - "viewfs://cluster/warehouse/db/table"
pub(crate) fn hdfs_config_build(cfg: &HdfsNativeConfig, path: &str) -> Result<Operator> {
    // ViewFS needs a mount table, which hdfs-native loads from Hadoop xml. Drop
    // the built-in conf in place (unless the deployer set HADOOP_CONF_DIR) so
    // viewfs:// works without external setup. Plain hdfs:// does not need this.
    if path.starts_with("viewfs://") {
        ensure_builtin_hadoop_conf()?;
    }

    let mut cfg = cfg.clone();

    if cfg.name_node.is_none() {
        cfg.name_node = Some(name_node_from_url(path)?);
    }

    cfg.root = Some("/".to_string());

    Ok(Operator::from_config(cfg)?.finish())
}

/// Derive the hdfs-native `name_node` URL from a table path, keeping the scheme.
///
/// - "hdfs://namenode:8020/warehouse/db" -> "hdfs://namenode:8020"
/// - "viewfs://cluster/warehouse/db"     -> "viewfs://cluster"
///
/// The scheme is preserved verbatim: a `viewfs://` authority is a mount-table
/// name, so rewriting it to `hdfs://` would make hdfs-native dial the
/// mount-table name as if it were a NameNode.
fn name_node_from_url(path: &str) -> Result<String> {
    let url = Url::parse(path).map_err(|_| Error::ConfigInvalid {
        message: format!("Invalid HDFS url: {path}"),
    })?;
    let scheme = url.scheme();
    let host = url.host_str().ok_or_else(|| Error::ConfigInvalid {
        message: format!("Invalid HDFS url: {path}, missing name node host"),
    })?;
    let port_part = url.port().map(|p| format!(":{p}")).unwrap_or_default();
    Ok(format!("{scheme}://{host}{port_part}"))
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
    fn test_hdfs_config_parse_with_name_node() {
        let props = make_props(&[("hdfs.name-node", "hdfs://namenode:8020")]);
        let cfg = hdfs_config_parse(props).unwrap();
        assert_eq!(cfg.name_node.as_deref(), Some("hdfs://namenode:8020"));
        assert!(!cfg.enable_append);
    }

    #[test]
    fn test_hdfs_config_parse_with_enable_append() {
        let props = make_props(&[
            ("hdfs.name-node", "hdfs://namenode:8020"),
            ("hdfs.enable-append", "true"),
        ]);
        let cfg = hdfs_config_parse(props).unwrap();
        assert!(cfg.enable_append);
    }

    #[test]
    fn test_hdfs_config_parse_empty_props() {
        let cfg = hdfs_config_parse(HashMap::new()).unwrap();
        assert!(cfg.name_node.is_none());
        assert!(!cfg.enable_append);
    }

    #[test]
    fn test_hdfs_config_build_extracts_name_node_from_path() {
        let cfg = HdfsNativeConfig::default();
        let op = hdfs_config_build(&cfg, "hdfs://namenode:8020/warehouse/db").unwrap();
        assert_eq!(op.info().scheme().to_string(), "hdfs-native");
    }

    #[test]
    fn test_hdfs_config_build_uses_config_name_node() {
        let mut cfg = HdfsNativeConfig::default();
        cfg.name_node = Some("hdfs://my-cluster:9000".to_string());
        let op = hdfs_config_build(&cfg, "hdfs://my-cluster:9000/warehouse").unwrap();
        assert_eq!(op.info().scheme().to_string(), "hdfs-native");
    }

    #[test]
    fn test_name_node_from_url_hdfs_with_port() {
        assert_eq!(
            name_node_from_url("hdfs://namenode:8020/warehouse/db").unwrap(),
            "hdfs://namenode:8020"
        );
    }

    #[test]
    fn test_name_node_from_url_hdfs_ha_no_port() {
        assert_eq!(
            name_node_from_url("hdfs://nameservice1/warehouse/db").unwrap(),
            "hdfs://nameservice1"
        );
    }

    #[test]
    fn test_name_node_from_url_preserves_viewfs_scheme() {
        // viewfs authority is a mount-table name; the scheme must survive so
        // hdfs-native resolves the mount table instead of dialing it as a host.
        assert_eq!(
            name_node_from_url("viewfs://my-cluster/warehouse/db").unwrap(),
            "viewfs://my-cluster"
        );
    }

    #[test]
    fn test_name_node_from_url_missing_host() {
        assert!(name_node_from_url("hdfs:///path/without/host").is_err());
    }

    #[test]
    fn test_hdfs_relative_path_viewfs() {
        let result = hdfs_relative_path("viewfs://cluster/warehouse/db/table");
        assert_eq!(result.unwrap(), "warehouse/db/table");
    }

    #[test]
    fn test_hdfs_relative_path_viewfs_root_slash() {
        let result = hdfs_relative_path("viewfs://cluster/");
        assert_eq!(result.unwrap(), "");
    }

    #[test]
    fn test_hdfs_relative_path_viewfs_missing_path_component() {
        let result = hdfs_relative_path("viewfs://cluster");
        assert!(result.is_err());
    }

    #[test]
    fn test_hdfs_config_build_invalid_url() {
        let cfg = HdfsNativeConfig::default();
        let result = hdfs_config_build(&cfg, "not-a-valid-url");
        assert!(result.is_err());
    }

    #[test]
    fn test_hdfs_config_build_missing_host() {
        let cfg = HdfsNativeConfig::default();
        let result = hdfs_config_build(&cfg, "hdfs:///path/without/host");
        assert!(result.is_err());
    }

    #[test]
    fn test_hdfs_config_parse_enable_append_false() {
        let props = make_props(&[
            ("hdfs.name-node", "hdfs://namenode:8020"),
            ("hdfs.enable-append", "false"),
        ]);
        let cfg = hdfs_config_parse(props).unwrap();
        assert!(!cfg.enable_append);
    }

    #[test]
    fn test_hdfs_config_parse_unrelated_keys_ignored() {
        let props = make_props(&[
            ("s3.endpoint", "https://s3.amazonaws.com"),
            ("fs.oss.endpoint", "https://oss.aliyuncs.com"),
            ("hdfs.name-node", "hdfs://namenode:8020"),
        ]);
        let cfg = hdfs_config_parse(props).unwrap();
        assert_eq!(cfg.name_node.as_deref(), Some("hdfs://namenode:8020"));
    }

    #[test]
    fn test_hdfs_relative_path_normal() {
        let result = hdfs_relative_path("hdfs://namenode:8020/warehouse/db/table");
        assert_eq!(result.unwrap(), "warehouse/db/table");
    }

    #[test]
    fn test_hdfs_relative_path_root_slash() {
        let result = hdfs_relative_path("hdfs://namenode:8020/");
        assert_eq!(result.unwrap(), "");
    }

    #[test]
    fn test_hdfs_relative_path_no_port() {
        let result = hdfs_relative_path("hdfs://nameservice1/warehouse/data");
        assert_eq!(result.unwrap(), "warehouse/data");
    }

    #[test]
    fn test_hdfs_relative_path_missing_path_component() {
        let result = hdfs_relative_path("hdfs://namenode:8020");
        assert!(result.is_err());
    }

    #[test]
    fn test_hdfs_relative_path_wrong_scheme() {
        let result = hdfs_relative_path("s3://bucket/key");
        assert!(result.is_err());
    }

    /// The env var override for the built-in hadoop conf base dir. All three
    /// cases live in one test because the env var is process-global — running
    /// them as separate `#[test]`s would let cargo's parallel runner interleave
    /// the set/remove and make assertions flaky.
    #[test]
    fn test_builtin_hadoop_conf_dir_prefix_override() {
        // Save and restore whatever the surrounding environment had, so this
        // test leaves no residue for others.
        let saved = std::env::var(BUILTIN_HADOOP_CONF_DIR_PREFIX_ENV).ok();

        // Unset -> falls back to the system temp dir.
        std::env::remove_var(BUILTIN_HADOOP_CONF_DIR_PREFIX_ENV);
        assert_eq!(builtin_hadoop_conf_dir_prefix(), std::env::temp_dir());

        // Set to an absolute dir -> used verbatim.
        std::env::set_var(BUILTIN_HADOOP_CONF_DIR_PREFIX_ENV, "/data/paimon");
        assert_eq!(
            builtin_hadoop_conf_dir_prefix(),
            std::path::PathBuf::from("/data/paimon")
        );

        // Surrounding whitespace is trimmed.
        std::env::set_var(BUILTIN_HADOOP_CONF_DIR_PREFIX_ENV, "  /data/paimon  ");
        assert_eq!(
            builtin_hadoop_conf_dir_prefix(),
            std::path::PathBuf::from("/data/paimon")
        );

        // Empty / whitespace-only -> treated as unset, falls back to temp dir.
        std::env::set_var(BUILTIN_HADOOP_CONF_DIR_PREFIX_ENV, "   ");
        assert_eq!(builtin_hadoop_conf_dir_prefix(), std::env::temp_dir());

        match saved {
            Some(v) => std::env::set_var(BUILTIN_HADOOP_CONF_DIR_PREFIX_ENV, v),
            None => std::env::remove_var(BUILTIN_HADOOP_CONF_DIR_PREFIX_ENV),
        }
    }
}
