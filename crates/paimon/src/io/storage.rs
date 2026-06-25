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
#[cfg(any(
    feature = "storage-azdls",
    feature = "storage-cos",
    feature = "storage-gcs",
    feature = "storage-oss",
    feature = "storage-obs",
    feature = "storage-s3",
    feature = "storage-hdfs",
    feature = "storage-hdfs-jni"
))]
use std::sync::Mutex;
#[cfg(any(
    feature = "storage-azdls",
    feature = "storage-cos",
    feature = "storage-gcs",
    feature = "storage-oss",
    feature = "storage-obs",
    feature = "storage-s3"
))]
use std::sync::MutexGuard;

#[cfg(feature = "storage-azdls")]
use super::AzdlsStorageConfig;
#[cfg(feature = "storage-cos")]
use opendal::services::CosConfig;
#[cfg(feature = "storage-gcs")]
use opendal::services::GcsConfig;
#[cfg(feature = "storage-hdfs-jni")]
use opendal::services::HdfsConfig;
#[cfg(feature = "storage-hdfs")]
use opendal::services::HdfsNativeConfig;
#[cfg(feature = "storage-obs")]
use opendal::services::ObsConfig;
#[cfg(feature = "storage-oss")]
use opendal::services::OssConfig;
#[cfg(feature = "storage-s3")]
use opendal::services::S3Config;
use opendal::{Operator, Scheme};
#[cfg(any(
    feature = "storage-cos",
    feature = "storage-gcs",
    feature = "storage-oss",
    feature = "storage-obs",
    feature = "storage-s3"
))]
use url::Url;

use crate::error;

use super::FileIOBuilder;

/// The storage carries all supported storage services in paimon
#[derive(Debug)]
pub enum Storage {
    #[cfg(feature = "storage-memory")]
    Memory { op: Operator },
    #[cfg(feature = "storage-fs")]
    LocalFs { op: Operator },
    #[cfg(feature = "storage-oss")]
    Oss {
        config: Box<OssConfig>,
        operators: Mutex<HashMap<String, Operator>>,
    },
    #[cfg(feature = "storage-s3")]
    S3 {
        config: Box<S3Config>,
        operators: Mutex<HashMap<String, Operator>>,
    },
    #[cfg(feature = "storage-cos")]
    Cos {
        config: Box<CosConfig>,
        operators: Mutex<HashMap<String, Operator>>,
    },
    #[cfg(feature = "storage-azdls")]
    Azdls {
        config: Box<AzdlsStorageConfig>,
        operators: Mutex<HashMap<String, Operator>>,
    },
    #[cfg(feature = "storage-obs")]
    Obs {
        config: Box<ObsConfig>,
        operators: Mutex<HashMap<String, Operator>>,
    },
    #[cfg(feature = "storage-gcs")]
    Gcs {
        config: Box<GcsConfig>,
        operators: Mutex<HashMap<String, Operator>>,
    },
    #[cfg(feature = "storage-hdfs")]
    HdfsNative {
        config: Box<HdfsNativeConfig>,
        op: Mutex<Option<Operator>>,
    },
    #[cfg(feature = "storage-hdfs-jni")]
    HdfsJni {
        config: Box<HdfsConfig>,
        op: Mutex<Option<Operator>>,
    },
}

impl Storage {
    pub(crate) fn build(file_io_builder: FileIOBuilder) -> crate::Result<Self> {
        let (scheme_str, props, use_alluxio) = file_io_builder.into_parts();
        let is_hdfs_family =
            matches!(scheme_str.as_str(), "hdfs" | "viewfs" | "alluxio");

        // Two refusals enforced regardless of feature flags, so the error
        // surface stays predictable even on binaries that disabled storage
        // backends. (1) alluxio:// without use_alluxio doesn't make sense:
        // the native HDFS RPC client cannot reach an alluxio master.
        if !use_alluxio && scheme_str == "alluxio" {
            return Err(error::Error::ConfigInvalid {
                message:
                    "alluxio:// scheme requires FileIOBuilder::with_alluxio(true)"
                        .to_string(),
            });
        }
        // (2) use_alluxio=true with a non-HDFS-family scheme is nonsense —
        // alluxio caching only spans HDFS / ViewFS clusters.
        if use_alluxio && !is_hdfs_family {
            return Err(error::Error::ConfigInvalid {
                message: format!(
                    "alluxio mode only supports hdfs/viewfs/alluxio scheme, got: {scheme_str}"
                ),
            });
        }

        let scheme = Self::parse_scheme(&scheme_str)?;

        // HDFS-family route: pick native or JNI based on use_alluxio.
        // Falls through to the per-scheme match below for everything else.
        if is_hdfs_family {
            return Self::build_hdfs_family(use_alluxio, props);
        }

        match scheme {
            #[cfg(feature = "storage-memory")]
            Scheme::Memory => Ok(Self::Memory {
                op: super::memory_config_build()?,
            }),
            #[cfg(feature = "storage-fs")]
            Scheme::Fs => Ok(Self::LocalFs {
                op: super::fs_config_build()?,
            }),
            #[cfg(feature = "storage-oss")]
            Scheme::Oss => {
                let config = super::oss_config_parse(props)?;
                Ok(Self::Oss {
                    config: Box::new(config),
                    operators: Mutex::new(HashMap::new()),
                })
            }
            #[cfg(feature = "storage-s3")]
            Scheme::S3 => {
                let config = super::s3_config_parse(props)?;
                Ok(Self::S3 {
                    config: Box::new(config),
                    operators: Mutex::new(HashMap::new()),
                })
            }
            #[cfg(feature = "storage-cos")]
            Scheme::Cos => {
                let config = super::cos_config_parse(props)?;
                Ok(Self::Cos {
                    config: Box::new(config),
                    operators: Mutex::new(HashMap::new()),
                })
            }
            #[cfg(feature = "storage-azdls")]
            Scheme::Azdls => {
                let config = super::azdls_config_parse(props)?;
                Ok(Self::Azdls {
                    config: Box::new(config),
                    operators: Mutex::new(HashMap::new()),
                })
            }
            #[cfg(feature = "storage-obs")]
            Scheme::Obs => {
                let config = super::obs_config_parse(props)?;
                Ok(Self::Obs {
                    config: Box::new(config),
                    operators: Mutex::new(HashMap::new()),
                })
            }
            #[cfg(feature = "storage-gcs")]
            Scheme::Gcs => {
                let config = super::gcs_config_parse(props)?;
                Ok(Self::Gcs {
                    config: Box::new(config),
                    operators: Mutex::new(HashMap::new()),
                })
            }
            _ => Err(error::Error::IoUnsupported {
                message: "Unsupported storage feature".to_string(),
            }),
        }
    }

    /// Build the HDFS-family branch of [`Storage::build`]. Splits on
    /// `use_alluxio` after both top-level guards in `build()` have already
    /// rejected the impossible combinations (alluxio scheme without the
    /// flag, alluxio flag with a non-HDFS-family scheme).
    fn build_hdfs_family(
        use_alluxio: bool,
        props: HashMap<String, String>,
    ) -> crate::Result<Self> {
        if use_alluxio {
            #[cfg(feature = "storage-hdfs-jni")]
            {
                let config = super::hdfs_jni_config_parse(props)?;
                return Ok(Self::HdfsJni {
                    config: Box::new(config),
                    op: Mutex::new(None),
                });
            }
            #[cfg(not(feature = "storage-hdfs-jni"))]
            {
                let _ = props;
                return Err(error::Error::ConfigInvalid {
                    message:
                        "use_alluxio=true requires the paimon crate to be built with the storage-hdfs-jni feature"
                            .to_string(),
                });
            }
        }
        #[cfg(feature = "storage-hdfs")]
        {
            let config = super::hdfs_config_parse(props)?;
            Ok(Self::HdfsNative {
                config: Box::new(config),
                op: Mutex::new(None),
            })
        }
        #[cfg(not(feature = "storage-hdfs"))]
        {
            let _ = props;
            Err(error::Error::ConfigInvalid {
                message:
                    "hdfs:// / viewfs:// require the paimon crate to be built with the storage-hdfs feature"
                        .to_string(),
            })
        }
    }

    pub(crate) fn create<'a>(&self, path: &'a str) -> crate::Result<(Operator, &'a str)> {
        match self {
            #[cfg(feature = "storage-memory")]
            Storage::Memory { op } => Ok((op.clone(), Self::memory_relative_path(path)?)),
            #[cfg(feature = "storage-fs")]
            Storage::LocalFs { op } => Ok((op.clone(), Self::fs_relative_path(path)?)),
            #[cfg(feature = "storage-oss")]
            Storage::Oss { config, operators } => {
                let (bucket, relative_path) =
                    Self::bucket_and_relative_path(path, "OSS", &["oss"])?;
                let op = Self::cached_oss_operator(config, operators, path, &bucket)?;
                Ok((op, relative_path))
            }
            #[cfg(feature = "storage-s3")]
            Storage::S3 { config, operators } => {
                let (bucket, relative_path) =
                    Self::bucket_and_relative_path(path, "S3", &["s3", "s3a"])?;
                let op = Self::cached_s3_operator(config, operators, path, &bucket)?;
                Ok((op, relative_path))
            }
            #[cfg(feature = "storage-cos")]
            Storage::Cos { config, operators } => {
                let (bucket, relative_path) =
                    Self::bucket_and_relative_path(path, "COS", &["cos", "cosn"])?;
                let op = Self::cached_operator(operators, "COS", &bucket, || {
                    super::cos_config_build(config, path)
                })?;
                Ok((op, relative_path))
            }
            #[cfg(feature = "storage-azdls")]
            Storage::Azdls { config, operators } => {
                let relative_path = super::azdls_relative_path(path)?;
                let cache_key = super::azdls_operator_cache_key(config, path)?;
                let op = Self::cached_operator(operators, "Azure", &cache_key, || {
                    super::azdls_config_build(config, path)
                })?;
                Ok((op, relative_path))
            }
            #[cfg(feature = "storage-obs")]
            Storage::Obs { config, operators } => {
                let (bucket, relative_path) =
                    Self::bucket_and_relative_path(path, "OBS", &["obs"])?;
                let op = Self::cached_operator(operators, "OBS", &bucket, || {
                    super::obs_config_build(config, path)
                })?;
                Ok((op, relative_path))
            }
            #[cfg(feature = "storage-gcs")]
            Storage::Gcs { config, operators } => {
                let (bucket, relative_path) =
                    Self::bucket_and_relative_path(path, "GCS", &["gcs", "gs"])?;
                let op = Self::cached_operator(operators, "GCS", &bucket, || {
                    super::gcs_config_build(config, path)
                })?;
                Ok((op, relative_path))
            }
            #[cfg(feature = "storage-hdfs")]
            Storage::HdfsNative { config, op } => {
                let relative_path = super::hdfs_relative_path(path)?;
                let mut guard = op.lock().map_err(|_| error::Error::UnexpectedError {
                    message: "Failed to lock HDFS operator".to_string(),
                    source: None,
                })?;
                // HDFS uses a single operator per Storage instance (unlike S3/OSS
                // which cache per bucket). The operator is lazily initialized from
                // the first path's NameNode if not set in config. One FileIO
                // instance should target exactly one HDFS cluster.
                if guard.is_none() {
                    *guard = Some(super::hdfs_config_build(config, path)?);
                }
                Ok((guard.as_ref().unwrap().clone(), relative_path))
            }
            #[cfg(feature = "storage-hdfs-jni")]
            Storage::HdfsJni { config, op } => {
                // Bleem-style scheme rewrite: hdfs://nn/p and viewfs://cluster/p
                // become alluxio://nn/p and alluxio://cluster/p before reaching
                // libhdfs. The Alluxio Hadoop FS SPI (alluxio.hadoop.FileSystem,
                // registered via classpath / HADOOP_CONF_DIR) takes over from
                // there. Idempotent for paths already in alluxio:// form.
                let rewritten = super::hdfs_to_alluxio_path(path)?;
                // The relative path must be derived from the rewritten path
                // so the substring offset returned to the caller is
                // self-consistent — but the caller only ever passes the
                // rewritten path's original on-wire form, so we also need to
                // resolve the original input back to its relative tail.
                let original_relative = super::hdfs_jni_relative_path(path)?;
                let mut guard = op.lock().map_err(|_| error::Error::UnexpectedError {
                    message: "Failed to lock HDFS JNI operator".to_string(),
                    source: None,
                })?;
                if guard.is_none() {
                    *guard = Some(super::hdfs_jni_config_build(config, &rewritten)?);
                }
                Ok((guard.as_ref().unwrap().clone(), original_relative))
            }
        }
    }

    #[cfg(feature = "storage-memory")]
    fn memory_relative_path(path: &str) -> crate::Result<&str> {
        if let Some(stripped) = path.strip_prefix("memory:/") {
            Ok(stripped)
        } else {
            path.get(1..).ok_or_else(|| error::Error::ConfigInvalid {
                message: format!("Invalid memory path: {path}"),
            })
        }
    }

    #[cfg(feature = "storage-fs")]
    fn fs_relative_path(path: &str) -> crate::Result<&str> {
        if let Some(stripped) = path.strip_prefix("file:/") {
            Ok(stripped)
        } else {
            path.get(1..).ok_or_else(|| error::Error::ConfigInvalid {
                message: format!("Invalid file path: {path}"),
            })
        }
    }

    #[cfg(any(
        feature = "storage-cos",
        feature = "storage-gcs",
        feature = "storage-obs",
        feature = "storage-oss",
        feature = "storage-s3"
    ))]
    fn bucket_and_relative_path<'a>(
        path: &'a str,
        storage_name: &str,
        allowed_schemes: &[&str],
    ) -> crate::Result<(String, &'a str)> {
        let url = Url::parse(path).map_err(|_| error::Error::ConfigInvalid {
            message: format!("Invalid {storage_name} url: {path}"),
        })?;
        let bucket = url
            .host_str()
            .ok_or_else(|| error::Error::ConfigInvalid {
                message: format!("Invalid {storage_name} url: {path}, missing bucket"),
            })?
            .to_string();
        let scheme = url.scheme();
        if !allowed_schemes.contains(&scheme) {
            return Err(error::Error::ConfigInvalid {
                message: format!("Invalid {storage_name} url: {path}, unsupported scheme {scheme}"),
            });
        }
        let prefix = format!("{scheme}://{bucket}/");
        let relative_path =
            path.strip_prefix(&prefix)
                .ok_or_else(|| error::Error::ConfigInvalid {
                    message: format!(
                        "Invalid {storage_name} url: {path}, should start with {prefix}"
                    ),
                })?;
        Ok((bucket, relative_path))
    }

    #[cfg(any(
        feature = "storage-azdls",
        feature = "storage-cos",
        feature = "storage-gcs",
        feature = "storage-oss",
        feature = "storage-obs",
        feature = "storage-s3"
    ))]
    fn lock_operator_cache<'a>(
        operators: &'a Mutex<HashMap<String, Operator>>,
        storage_name: &str,
    ) -> crate::Result<MutexGuard<'a, HashMap<String, Operator>>> {
        operators.lock().map_err(|_| error::Error::UnexpectedError {
            message: format!("Failed to lock {storage_name} operator cache"),
            source: None,
        })
    }

    #[cfg(any(
        feature = "storage-azdls",
        feature = "storage-cos",
        feature = "storage-gcs",
        feature = "storage-oss",
        feature = "storage-obs",
        feature = "storage-s3"
    ))]
    fn cached_operator(
        operators: &Mutex<HashMap<String, Operator>>,
        storage_name: &str,
        cache_key: &str,
        build: impl FnOnce() -> crate::Result<Operator>,
    ) -> crate::Result<Operator> {
        let mut operators = Self::lock_operator_cache(operators, storage_name)?;
        if let Some(op) = operators.get(cache_key) {
            return Ok(op.clone());
        }

        let op = build()?;
        operators.insert(cache_key.to_string(), op.clone());
        Ok(op)
    }

    #[cfg(feature = "storage-oss")]
    fn cached_oss_operator(
        config: &OssConfig,
        operators: &Mutex<HashMap<String, Operator>>,
        path: &str,
        bucket: &str,
    ) -> crate::Result<Operator> {
        Self::cached_operator(operators, "OSS", bucket, || {
            super::oss_config_build(config, path)
        })
    }

    #[cfg(feature = "storage-s3")]
    fn cached_s3_operator(
        config: &S3Config,
        operators: &Mutex<HashMap<String, Operator>>,
        path: &str,
        bucket: &str,
    ) -> crate::Result<Operator> {
        Self::cached_operator(operators, "S3", bucket, || {
            super::s3_config_build(config, path)
        })
    }

    fn parse_scheme(scheme: &str) -> crate::Result<Scheme> {
        match scheme {
            "memory" => Ok(Scheme::Memory),
            "file" | "" => Ok(Scheme::Fs),
            "s3" | "s3a" => Ok(Scheme::S3),
            "cosn" => Ok(Scheme::Cos),
            "abfs" | "abfss" | "az" | "azure" => Ok(Scheme::Azdls),
            "gs" => Ok(Scheme::Gcs),
            // `viewfs` is routed through the same hdfs-native backend: the
            // native client resolves the viewfs mount table (from Hadoop xml
            // discovered via HADOOP_CONF_DIR / HADOOP_HOME) to real clusters.
            // `alluxio` flows through opendal's libhdfs/JNI backend
            // (services-hdfs); it never enters this function for the native
            // path because the build() guard rejects it without
            // use_alluxio=true.
            "hdfs" | "viewfs" => Ok(Scheme::HdfsNative),
            "alluxio" => Ok(Scheme::Hdfs),
            s => Ok(s.parse::<Scheme>()?),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::FileIOBuilder;

    /// alluxio:// scheme without `use_alluxio=true` is rejected upfront —
    /// hdfs-native cannot talk to an alluxio master.
    #[test]
    fn build_rejects_alluxio_scheme_without_use_alluxio() {
        let err = FileIOBuilder::new("alluxio")
            .build()
            .expect_err("alluxio scheme without use_alluxio should be rejected");
        let msg = format!("{err}");
        assert!(
            msg.contains("alluxio") && msg.contains("with_alluxio"),
            "got {msg}"
        );
    }

    /// `use_alluxio=true` with a non-HDFS-family scheme is nonsense —
    /// alluxio caching only spans HDFS / ViewFS clusters. Should be
    /// rejected before any backend is constructed.
    #[test]
    fn build_rejects_use_alluxio_with_non_hdfs_scheme() {
        let err = FileIOBuilder::new("s3")
            .with_alluxio(true)
            .build()
            .expect_err("alluxio mode with s3:// should be rejected");
        let msg = format!("{err}");
        assert!(
            msg.contains("alluxio mode") && msg.contains("s3"),
            "got {msg}"
        );
    }

    /// `use_alluxio=true` with no JNI feature compiled in is a configuration
    /// mistake. The error must call out the missing feature so the deployer
    /// knows the binary needs a rebuild — silently falling back to native
    /// would defeat the whole point.
    #[cfg(all(feature = "storage-hdfs", not(feature = "storage-hdfs-jni")))]
    #[test]
    fn build_rejects_use_alluxio_when_jni_feature_disabled() {
        let err = FileIOBuilder::new("hdfs")
            .with_alluxio(true)
            .build()
            .expect_err("use_alluxio=true without JNI feature should be rejected");
        let msg = format!("{err}");
        assert!(msg.contains("storage-hdfs-jni"), "got {msg}");
    }

    /// hdfs:// without `use_alluxio` routes to the native backend on
    /// builds that have it; this is the path everyone has today.
    #[cfg(feature = "storage-hdfs")]
    #[test]
    fn build_routes_hdfs_to_native_backend() {
        let file_io = FileIOBuilder::new("hdfs")
            .with_prop("hdfs.name-node", "hdfs://nn:8020")
            .build()
            .unwrap();
        assert!(!file_io.use_alluxio());
    }

    /// hdfs:// with `use_alluxio=true` routes to the JNI backend on builds
    /// that have it. We can confirm via `FileIO::use_alluxio` without
    /// touching the (lazy) opendal operator — the JNI side won't try to
    /// initialise libhdfs until the first IO call.
    #[cfg(feature = "storage-hdfs-jni")]
    #[test]
    fn build_routes_alluxio_to_jni_backend() {
        let file_io = FileIOBuilder::new("hdfs")
            .with_alluxio(true)
            .with_prop("hdfs.name-node", "alluxio://master:19998")
            .build()
            .unwrap();
        assert!(file_io.use_alluxio());
    }

    /// `FileIO::with_alluxio` flips the flag without losing the props
    /// passed at builder time. Round-trips back to false so the test also
    /// covers the no-op fast path (returning self.clone() when the flag
    /// already matches).
    #[cfg(all(feature = "storage-hdfs", feature = "storage-hdfs-jni"))]
    #[test]
    fn with_alluxio_round_trip_preserves_props() {
        let file_io = FileIOBuilder::new("hdfs")
            .with_props([
                ("hdfs.name-node", "hdfs://nn:8020"),
                ("hdfs.enable-append", "true"),
                ("unrelated.key", "ignored-but-kept"),
            ])
            .build()
            .unwrap();
        assert!(!file_io.use_alluxio());

        let alluxio = file_io.with_alluxio(true).unwrap();
        assert!(alluxio.use_alluxio());

        let back = alluxio.with_alluxio(false).unwrap();
        assert!(!back.use_alluxio());
    }

    /// `with_alluxio` is a cheap no-op when the requested value already
    /// matches the current flag — covers the early-return branch.
    #[cfg(feature = "storage-hdfs")]
    #[test]
    fn with_alluxio_noop_when_flag_already_matches() {
        let file_io = FileIOBuilder::new("hdfs")
            .with_prop("hdfs.name-node", "hdfs://nn:8020")
            .build()
            .unwrap();
        let same = file_io.with_alluxio(false).unwrap();
        assert!(!same.use_alluxio());
    }
}
