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

use opendal::services::HdfsNativeConfig;
use opendal::Operator;
use url::Url;

use crate::error::Error;
use crate::Result;

/// Supported HDFS-family scheme prefixes, both routed through hdfs-native.
const HDFS_SCHEME_PREFIXES: [&str; 2] = ["hdfs://", "viewfs://"];

/// Strip a supported HDFS-family scheme prefix (`hdfs://` or `viewfs://`),
/// returning the `authority/path` remainder. Returns `None` for any other scheme.
fn strip_hdfs_scheme(path: &str) -> Option<&str> {
    HDFS_SCHEME_PREFIXES
        .iter()
        .find_map(|prefix| path.strip_prefix(prefix))
}

/// Parse an HDFS or ViewFS path to get the relative path from the cluster root.
///
/// Examples:
/// - "hdfs://namenode:8020/warehouse/db/table" -> "warehouse/db/table"
/// - "viewfs://cluster/warehouse/db/table"     -> "warehouse/db/table"
pub(crate) fn hdfs_relative_path(path: &str) -> Result<&str> {
    let after_scheme = strip_hdfs_scheme(path).ok_or_else(|| Error::ConfigInvalid {
        message: format!("Invalid HDFS path: {path}, should start with hdfs:// or viewfs://"),
    })?;
    match after_scheme.find('/') {
        Some(pos) => Ok(&after_scheme[pos + 1..]),
        None => Err(Error::ConfigInvalid {
            message: format!("Invalid HDFS path: {path}, missing path component"),
        }),
    }
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
}
