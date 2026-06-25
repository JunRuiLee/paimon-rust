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

//! Table API for Apache Paimon

pub(crate) mod aggregator;
pub(crate) mod bin_pack;
mod blob_file_writer;
mod branch_manager;
mod bucket_assigner;
mod bucket_assigner_constant;
mod bucket_assigner_cross;
mod bucket_assigner_dynamic;
mod bucket_assigner_fixed;
mod bucket_filter;
mod commit_message;
pub(crate) mod cow_writer;
mod data_evolution_reader;
pub mod data_evolution_writer;
mod data_file_reader;
mod data_file_writer;
#[cfg(feature = "fulltext")]
mod full_text_search_builder;
pub(crate) mod global_index_scanner;
mod kv_file_reader;
mod kv_file_writer;
mod lumina_index_build_builder;
pub(crate) mod merge_tree_split_generator;
#[cfg(test)]
mod multi_level_mor_tests;
mod partition_filter;
mod postpone_file_writer;
mod prepared_files;
mod read_builder;
pub mod referenced_files;
pub(crate) mod rest_env;
pub(crate) mod row_id_predicate;
pub(crate) mod schema_manager;
pub(crate) mod snapshot_commit;
mod snapshot_manager;
mod sort_merge;
mod source;
pub mod split_serde;
mod stats_filter;
pub(crate) mod table_commit;
mod table_read;
mod table_scan;
mod table_update;
pub(crate) mod table_write;
mod tag_manager;
pub(crate) mod time_travel;
mod vector_search_builder;
mod versioned_partial_update;
mod write_builder;

use crate::Result;
use arrow_array::RecordBatch;
pub use branch_manager::BranchManager;
pub use commit_message::CommitMessage;
pub use cow_writer::{CopyOnWriteMergeWriter, FileInfo};
pub use data_evolution_writer::DataEvolutionWriter;
#[cfg(feature = "fulltext")]
pub use full_text_search_builder::FullTextSearchBuilder;
use futures::stream::BoxStream;
pub use lumina_index_build_builder::LuminaIndexBuildBuilder;
pub use read_builder::ReadBuilder;
pub use rest_env::RESTEnv;
pub use schema_manager::SchemaManager;
pub use snapshot_commit::{RESTSnapshotCommit, RenamingSnapshotCommit, SnapshotCommit};
pub use snapshot_manager::SnapshotManager;
pub use source::{
    merge_row_ranges, DataSplit, DataSplitBuilder, DeletionFile, PartitionBucket, Plan, RowRange,
};
pub use split_serde::{
    deserialize_data_split, deserialize_data_split_to_plan, serialize_data_split,
};
pub use table_commit::TableCommit;
pub use table_read::TableRead;
pub use table_scan::TableScan;
pub use table_update::TableUpdate;
pub use table_write::TableWrite;
pub use tag_manager::TagManager;
pub use vector_search_builder::VectorSearchBuilder;
pub use write_builder::WriteBuilder;

use crate::catalog::Identifier;
use crate::io::FileIO;
use crate::spec::{CoreOptions, DataField, Snapshot, TableSchema};
use std::collections::HashMap;

/// Table represents a table in the catalog.
#[derive(Debug, Clone)]
pub struct Table {
    file_io: FileIO,
    identifier: Identifier,
    location: String,
    schema: TableSchema,
    schema_manager: SchemaManager,
    rest_env: Option<RESTEnv>,
    /// True when this table copy was switched to a historical schema by
    /// [`Table::copy_with_time_travel`]. Such a copy is read-only.
    time_traveled: bool,
    /// Snapshot resolved by [`Table::copy_with_time_travel`] from this copy's
    /// options, so scans don't have to resolve the same selector again.
    /// Cleared when [`Table::copy_with_options`] changes the selector.
    travel_snapshot: Option<Snapshot>,
}

impl Table {
    /// Create a new table.
    pub fn new(
        file_io: FileIO,
        identifier: Identifier,
        location: String,
        schema: TableSchema,
        rest_env: Option<RESTEnv>,
    ) -> Self {
        let schema_manager = SchemaManager::new(file_io.clone(), location.clone());
        Self {
            file_io,
            identifier,
            location,
            schema,
            schema_manager,
            rest_env,
            time_traveled: false,
            travel_snapshot: None,
        }
    }

    /// Get the table's identifier.
    pub fn identifier(&self) -> &Identifier {
        &self.identifier
    }

    /// Get the table's location.
    pub fn location(&self) -> &str {
        &self.location
    }

    /// Get the table's schema.
    pub fn schema(&self) -> &TableSchema {
        &self.schema
    }

    /// Get the FileIO instance for this table.
    pub fn file_io(&self) -> &FileIO {
        &self.file_io
    }

    /// Get the SchemaManager for this table.
    pub fn schema_manager(&self) -> &SchemaManager {
        &self.schema_manager
    }

    /// Get the REST environment, if this table was loaded from a REST catalog.
    pub fn rest_env(&self) -> Option<&RESTEnv> {
        self.rest_env.as_ref()
    }

    /// Create a read builder for scan/read.
    ///
    /// Reference: [pypaimon FileStoreTable.new_read_builder](https://github.com/apache/paimon/blob/release-1.3/paimon-python/pypaimon/table/file_store_table.py).
    pub fn new_read_builder(&self) -> ReadBuilder<'_> {
        ReadBuilder::new(self)
    }

    /// Create a full-text search builder.
    ///
    /// Reference: [FullTextSearchBuilderImpl](https://github.com/apache/paimon/blob/master/paimon-core/src/main/java/org/apache/paimon/table/source/FullTextSearchBuilderImpl.java)
    #[cfg(feature = "fulltext")]
    pub fn new_full_text_search_builder(&self) -> FullTextSearchBuilder<'_> {
        FullTextSearchBuilder::new(self)
    }

    pub fn new_vector_search_builder(&self) -> VectorSearchBuilder<'_> {
        VectorSearchBuilder::new(self)
    }

    pub fn new_lumina_index_build_builder(&self) -> LuminaIndexBuildBuilder<'_> {
        LuminaIndexBuildBuilder::new(self)
    }

    /// Create a write builder for write/commit.
    ///
    /// Reference: [pypaimon FileStoreTable.new_write_builder](https://github.com/apache/paimon/blob/master/paimon-python/pypaimon/table/file_store_table.py).
    pub fn new_write_builder(&self) -> WriteBuilder<'_> {
        WriteBuilder::new(self)
    }

    /// Create a copy of this table with extra options merged into the schema.
    ///
    /// This never switches the schema version; it corresponds to Java
    /// `FileStoreTable.copyWithoutTimeTravel`. Use
    /// [`Table::copy_with_time_travel`] when the options may select a
    /// historical snapshot whose schema should be used for reading.
    pub fn copy_with_options(&self, extra: HashMap<String, String>) -> Self {
        // Changing the time-travel selector invalidates the resolved snapshot
        // (a time-travelled schema then has no matching snapshot anymore, and
        // scans of such a copy fail until `copy_with_time_travel` re-resolves
        // it). Unrelated options keep the snapshot/schema pair intact.
        let selector_changed = extra.keys().any(|k| {
            k == crate::spec::SCAN_VERSION_OPTION || k == crate::spec::SCAN_TIMESTAMP_MILLIS_OPTION
        });
        Self {
            file_io: self.file_io.clone(),
            identifier: self.identifier.clone(),
            location: self.location.clone(),
            schema: self.schema.copy_with_options(extra),
            schema_manager: self.schema_manager.clone(),
            rest_env: self.rest_env.clone(),
            time_traveled: self.time_traveled,
            travel_snapshot: if selector_changed {
                None
            } else {
                self.travel_snapshot.clone()
            },
        }
    }

    /// Create a copy of this table with extra options merged in, switching to
    /// the schema of the time-travelled snapshot when the merged options
    /// select one.
    ///
    /// Mirrors Java `AbstractFileStoreTable.copy(dynamicOptions)` →
    /// `tryTimeTravel`: if the merged options contain a time-travel selector
    /// (`scan.version` / `scan.timestamp-millis`) that resolves to a snapshot,
    /// the table's fields and keys come from that snapshot's schema while the
    /// options stay the merged ones (Java `TableSchema.copy(newOptions)`).
    /// Like Java, resolution failures fall back silently to the current
    /// schema (the `if let Ok` below swallows them); an invalid selector
    /// still fails later at scan planning.
    pub async fn copy_with_time_travel(&self, extra: HashMap<String, String>) -> Result<Self> {
        let mut table = self.copy_with_options(extra);
        // travel_to_snapshot returns Ok(None) without IO when the merged
        // options contain no selector.
        if let Ok(Some(snapshot)) =
            time_travel::travel_to_snapshot(&table.file_io, &table.location, table.schema.options())
                .await
        {
            if snapshot.schema_id() != table.schema.id() {
                let snapshot_schema = table.schema_manager.schema(snapshot.schema_id()).await?;
                table.schema =
                    snapshot_schema.copy_with_replaced_options(table.schema.options().clone());
                table.time_traveled = true;
            }
            table.travel_snapshot = Some(snapshot);
        }
        Ok(table)
    }

    /// Whether this table copy reads a historical snapshot with its
    /// historical schema (see [`Table::copy_with_time_travel`]).
    pub fn is_time_traveled(&self) -> bool {
        self.time_traveled
    }

    /// The snapshot resolved by [`Table::copy_with_time_travel`] from this
    /// copy's options, if any. Lets scans skip re-resolving the selector.
    pub(crate) fn travel_snapshot(&self) -> Option<&Snapshot> {
        self.travel_snapshot.as_ref()
    }

    /// Apply the session-level alluxio switch to this table's data FileIO.
    ///
    /// Two gates have to be true for the rebuild to actually happen:
    /// 1. `session_use_alluxio` — the caller's runtime choice (e.g. driven
    ///    by a C-binding parameter on `paimon_catalog_get_table` /
    ///    `paimon_table_open_path`).
    /// 2. `alluxio.cache-enabled` on the table's options — declaration that
    ///    this table's data files are actually covered by an alluxio cache.
    ///
    /// When both are true, `self.file_io` is rebuilt via
    /// [`FileIO::with_alluxio`] so subsequent `hdfs://` / `viewfs://` IO is
    /// routed through libhdfs to alluxio. `self.schema_manager` is
    /// deliberately left alone: catalog metadata (schema, snapshot,
    /// manifest) always reads through the native HDFS backend.
    ///
    /// Returns `Ok(self)` unchanged when either gate is false, so callers
    /// can pipe every `get_table` / `open_path` result through this method
    /// without branching.
    pub fn with_alluxio(mut self, session_use_alluxio: bool) -> Result<Self> {
        let effective =
            session_use_alluxio && CoreOptions::new(self.schema.options()).alluxio_cache_enabled();
        if !effective {
            return Ok(self);
        }
        self.file_io = self.file_io.with_alluxio(true)?;
        Ok(self)
    }
}

/// A stream of arrow [`RecordBatch`]es.
pub type ArrowRecordBatchStream = BoxStream<'static, Result<RecordBatch>>;

pub(crate) fn find_field_id_by_name(fields: &[DataField], name: &str) -> Option<i32> {
    fields.iter().find(|f| f.name() == name).map(|f| f.id())
}

#[cfg(all(test, feature = "storage-hdfs"))]
mod alluxio_tests {
    //! Coverage for the two-level (session ∧ table-option) gating in
    //! [`Table::with_alluxio`]. The non-flip cases run on any build that
    //! supplies `storage-hdfs`; the actual flip path requires
    //! `storage-hdfs-jni` so [`FileIO::with_alluxio(true)`] can resolve.

    use super::*;
    use crate::io::FileIOBuilder;
    use crate::spec::{DataType, IntType, Schema};

    /// Build a `Table` whose data FileIO is on the native HDFS backend.
    /// `with_table_flag=true` sets `alluxio.cache-enabled=true` on the
    /// table options; otherwise the option is absent (default false).
    fn make_table(with_table_flag: bool) -> Table {
        let mut builder = Schema::builder().column("id", DataType::Int(IntType::new()));
        if with_table_flag {
            builder = builder.option("alluxio.cache-enabled", "true");
        }
        let schema = builder.build().unwrap();
        let file_io = FileIOBuilder::new("hdfs")
            .with_prop("hdfs.name-node", "hdfs://nn:8020")
            .build()
            .unwrap();
        Table::new(
            file_io,
            Identifier::new("db", "t"),
            "hdfs://nn:8020/warehouse/db.db/t".to_string(),
            TableSchema::new(0, &schema),
            None,
        )
    }

    /// Session off + table off → no rebuild.
    #[test]
    fn with_alluxio_session_off_table_off_is_noop() {
        let table = make_table(false).with_alluxio(false).unwrap();
        assert!(!table.file_io().use_alluxio());
    }

    /// Session on but the table never opted in → no rebuild. Catches the
    /// regression where a global session flag accidentally rewrote every
    /// table.
    #[test]
    fn with_alluxio_session_on_table_off_is_noop() {
        let table = make_table(false).with_alluxio(true).unwrap();
        assert!(!table.file_io().use_alluxio());
    }

    /// Table declares cache-enabled but the session didn't opt in — caller
    /// wants a fresh read or doesn't trust the cache. No rebuild.
    #[test]
    fn with_alluxio_session_off_table_on_is_noop() {
        let table = make_table(true).with_alluxio(false).unwrap();
        assert!(!table.file_io().use_alluxio());
    }

    /// Both gates on → data FileIO rebuilt; the SchemaManager FileIO
    /// stays native, so catalog metadata (schema-* files) still goes
    /// through hdfs-native even after the switch. Requires the JNI
    /// feature because the rebuild itself needs services-hdfs to be in
    /// the binary.
    #[cfg(feature = "storage-hdfs-jni")]
    #[test]
    fn with_alluxio_session_on_table_on_flips_data_io_only() {
        let table = make_table(true).with_alluxio(true).unwrap();
        assert!(
            table.file_io().use_alluxio(),
            "data FileIO should be on the alluxio/JNI backend"
        );
        assert!(
            !table.schema_manager().file_io().use_alluxio(),
            "schema manager must keep reading metadata through the native backend"
        );
    }

    /// `copy_with_options` must preserve the alluxio state on the data
    /// FileIO and the native state on the schema manager. The clone
    /// shares the same Arc-wrapped storage; this just locks down that we
    /// don't accidentally reset either field in the copy.
    #[cfg(feature = "storage-hdfs-jni")]
    #[test]
    fn copy_with_options_preserves_alluxio_state() {
        let table = make_table(true).with_alluxio(true).unwrap();
        assert!(table.file_io().use_alluxio());

        let copy = table.copy_with_options(HashMap::new());
        assert!(
            copy.file_io().use_alluxio(),
            "copy should keep the alluxio data FileIO"
        );
        assert!(
            !copy.schema_manager().file_io().use_alluxio(),
            "copy's schema manager must still be native"
        );
    }
}
