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

//! Resolves the on-disk path of an index file recorded in an index manifest.
//!
//! An index file's location depends on how it was written:
//!   * an externally-stored file records its absolute path in the manifest and
//!     is read from exactly that path, wherever it lives;
//!   * a global index (data-evolution, row-id space) always lives under the
//!     table's `index/` directory;
//!   * a source-backed primary-key index lives beside its bucket's data files
//!     when the table stores index files in the data-file directory, and under
//!     the table `index/` directory otherwise.
//!
//! The bucket-local fallback resolves against the bucket directory captured on
//! the data split, never a path rebuilt from the table root, so custom data
//! directories and postpone-bucket layouts are honored.

const INDEX_DIR: &str = "index";

/// How to resolve an index file that carries no explicit external path.
pub(crate) enum IndexFileLocation<'a> {
    /// Global index files (data-evolution row-id space) always live under the
    /// table's `index/` directory.
    Global { table_path: &'a str },
    /// Source-backed primary-key index files live beside their bucket's data
    /// files when the table keeps index files in the data-file directory, and
    /// under the table `index/` directory otherwise.
    BucketLocal {
        table_path: &'a str,
        /// The bucket directory captured from the data split (e.g.
        /// `warehouse/db/tbl/bucket-3`). Used directly, not rebuilt.
        bucket_path: &'a str,
        /// Whether the table stores index files in the data-file (bucket)
        /// directory (`index-file-in-data-file-dir`).
        index_file_in_data_file_dir: bool,
    },
}

impl IndexFileLocation<'_> {
    /// The directory a file with no explicit external path resolves into. A
    /// writer needs it to create the directory it is about to write into, so it
    /// must come from here rather than be re-derived from the resolved path.
    pub(crate) fn directory(&self) -> String {
        match self {
            IndexFileLocation::Global { table_path } => format!("{table_path}/{INDEX_DIR}"),
            IndexFileLocation::BucketLocal {
                table_path,
                bucket_path,
                index_file_in_data_file_dir,
            } => {
                if *index_file_in_data_file_dir {
                    (*bucket_path).to_string()
                } else {
                    format!("{table_path}/{INDEX_DIR}")
                }
            }
        }
    }

    /// Resolve the full path of `file_name`, honoring an explicit
    /// `external_path` when present.
    pub(crate) fn resolve(&self, file_name: &str, external_path: Option<&str>) -> String {
        match external_path {
            Some(external) => external.to_string(),
            None => format!("{}/{file_name}", self.directory()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn external_path_wins_over_every_mode() {
        let external = "s3://other-bucket/abs/idx-0";
        let global = IndexFileLocation::Global {
            table_path: "warehouse/db/tbl",
        };
        assert_eq!(global.resolve("idx-0", Some(external)), external);

        let bucket_local = IndexFileLocation::BucketLocal {
            table_path: "warehouse/db/tbl",
            bucket_path: "warehouse/db/tbl/bucket-3",
            index_file_in_data_file_dir: true,
        };
        assert_eq!(bucket_local.resolve("idx-0", Some(external)), external);
    }

    #[test]
    fn global_uses_table_index_directory() {
        let loc = IndexFileLocation::Global {
            table_path: "warehouse/db/tbl",
        };
        assert_eq!(loc.resolve("idx-0", None), "warehouse/db/tbl/index/idx-0");
    }

    #[test]
    fn bucket_local_uses_bucket_directory_when_enabled() {
        let loc = IndexFileLocation::BucketLocal {
            table_path: "warehouse/db/tbl",
            bucket_path: "warehouse/db/tbl/bucket-3",
            index_file_in_data_file_dir: true,
        };
        assert_eq!(
            loc.resolve("idx-0", None),
            "warehouse/db/tbl/bucket-3/idx-0"
        );
    }

    #[test]
    fn bucket_local_falls_back_to_table_index_when_disabled() {
        let loc = IndexFileLocation::BucketLocal {
            table_path: "warehouse/db/tbl",
            bucket_path: "warehouse/db/tbl/bucket-3",
            index_file_in_data_file_dir: false,
        };
        assert_eq!(loc.resolve("idx-0", None), "warehouse/db/tbl/index/idx-0");
    }

    #[test]
    fn bucket_local_uses_captured_custom_bucket_path() {
        // A custom data directory / postpone-bucket layout must be honored via
        // the captured bucket path, not a path rebuilt from the table root.
        let loc = IndexFileLocation::BucketLocal {
            table_path: "warehouse/db/tbl",
            bucket_path: "s3://data-warehouse/custom/tbl/bucket-postpone",
            index_file_in_data_file_dir: true,
        };
        assert_eq!(
            loc.resolve("idx-0", None),
            "s3://data-warehouse/custom/tbl/bucket-postpone/idx-0"
        );
    }

    #[test]
    fn directory_is_the_parent_resolve_writes_into() {
        // A writer creates `directory()` and then writes `resolve()`; the two must
        // agree, or it creates one directory and writes into another.
        let locations = [
            IndexFileLocation::Global {
                table_path: "warehouse/db/tbl",
            },
            IndexFileLocation::BucketLocal {
                table_path: "warehouse/db/tbl",
                bucket_path: "warehouse/db/tbl/pt=1/bucket-3",
                index_file_in_data_file_dir: false,
            },
            IndexFileLocation::BucketLocal {
                table_path: "warehouse/db/tbl",
                bucket_path: "warehouse/db/tbl/pt=1/bucket-3",
                index_file_in_data_file_dir: true,
            },
        ];
        for loc in &locations {
            assert_eq!(
                loc.resolve("idx-0", None),
                format!("{}/idx-0", loc.directory())
            );
        }
    }
}
