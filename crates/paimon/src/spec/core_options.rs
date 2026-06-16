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

use std::collections::{HashMap, HashSet};

const DELETION_VECTORS_ENABLED_OPTION: &str = "deletion-vectors.enabled";
const DELETION_VECTORS_BITMAP64_OPTION: &str = "deletion-vectors.bitmap64";
const DELETION_VECTORS_READ_MODE_OPTION: &str = "deletion-vectors.read-mode";
const DATA_EVOLUTION_ENABLED_OPTION: &str = "data-evolution.enabled";
const GLOBAL_INDEX_ENABLED_OPTION: &str = "global-index.enabled";
const GLOBAL_INDEX_ROW_COUNT_PER_SHARD_OPTION: &str = "global-index.row-count-per-shard";
const SOURCE_SPLIT_TARGET_SIZE_OPTION: &str = "source.split.target-size";
const SOURCE_SPLIT_OPEN_FILE_COST_OPTION: &str = "source.split.open-file-cost";
const READ_BATCH_SIZE_OPTION: &str = "read.batch-size";
const PARQUET_PAGE_INDEX_ENABLED_OPTION: &str = "read.parquet.page-index.enabled";
const PARQUET_BLOOM_FILTER_ENABLED_OPTION: &str = "read.parquet.bloom-filter.enabled";
const PARTITION_DEFAULT_NAME_OPTION: &str = "partition.default-name";
const PARTITION_LEGACY_NAME_OPTION: &str = "partition.legacy-name";
const BUCKET_KEY_OPTION: &str = "bucket-key";
const BUCKET_FUNCTION_TYPE_OPTION: &str = "bucket-function.type";
const BUCKET_OPTION: &str = "bucket";
const DEFAULT_BUCKET: i32 = -1;
/// Postpone bucket mode: data is written to `bucket-postpone` directory
/// and is invisible to readers until compaction assigns real bucket numbers.
pub const POSTPONE_BUCKET: i32 = -2;
/// Directory name for postpone bucket files.
pub const POSTPONE_BUCKET_DIR: &str = "bucket-postpone";
const COMMIT_MAX_RETRIES_OPTION: &str = "commit.max-retries";
const COMMIT_TIMEOUT_OPTION: &str = "commit.timeout";
const COMMIT_MIN_RETRY_WAIT_OPTION: &str = "commit.min-retry-wait";
const COMMIT_MAX_RETRY_WAIT_OPTION: &str = "commit.max-retry-wait";
const FILE_COMPRESSION_OPTION: &str = "file.compression";
const FILE_COMPRESSION_ZSTD_LEVEL_OPTION: &str = "file.compression.zstd-level";
const FILE_FORMAT_OPTION: &str = "file.format";
const CHANGELOG_FILE_PREFIX_OPTION: &str = "changelog-file.prefix";
const CHANGELOG_FILE_FORMAT_OPTION: &str = "changelog-file.format";
const CHANGELOG_FILE_COMPRESSION_OPTION: &str = "changelog-file.compression";
const CHANGELOG_FILE_STATS_MODE_OPTION: &str = "changelog-file.stats-mode";
const ROW_TRACKING_ENABLED_OPTION: &str = "row-tracking.enabled";
const WRITE_PARQUET_BUFFER_SIZE_OPTION: &str = "write.parquet-buffer-size";
const SEQUENCE_FIELD_OPTION: &str = "sequence.field";
const MERGE_ENGINE_OPTION: &str = "merge-engine";
const VERSIONED_PARTIAL_UPDATE_MERGE_MODE_OPTION: &str =
    "versioned-partial-update.merge-mode";
const VERSIONED_PARTIAL_UPDATE_IGNORE_MODE_ENABLED_OPTION: &str =
    "versioned-partial-update.ignore-mode.enabled";
const CHANGELOG_PRODUCER_OPTION: &str = "changelog-producer";
const ROWKIND_FIELD_OPTION: &str = "rowkind.field";
const SNAPSHOT_SEQUENCE_ORDERING_OPTION: &str = "sequence.snapshot-ordering";
const FORCE_LOOKUP_OPTION: &str = "force-lookup";
const FIELDS_DEFAULT_AGG_FUNC_OPTION: &str = "fields.default-aggregate-function";
const DEFAULT_COMMIT_MAX_RETRIES: u32 = 10;
const DEFAULT_COMMIT_TIMEOUT_MS: u64 = 120_000;
const DEFAULT_COMMIT_MIN_RETRY_WAIT_MS: u64 = 1_000;
const DEFAULT_COMMIT_MAX_RETRY_WAIT_MS: u64 = 10_000;
pub const SCAN_TIMESTAMP_MILLIS_OPTION: &str = "scan.timestamp-millis";
pub const SCAN_VERSION_OPTION: &str = "scan.version";
const DEFAULT_SOURCE_SPLIT_TARGET_SIZE: i64 = 128 * 1024 * 1024;
const DEFAULT_SOURCE_SPLIT_OPEN_FILE_COST: i64 = 4 * 1024 * 1024;
/// Default rows per batch on the read path. Aligned with paimon-java's
/// `CoreOptions.READ_BATCH_SIZE` default (1024).
const DEFAULT_READ_BATCH_SIZE: usize = 1024;
const DEFAULT_PARTITION_DEFAULT_NAME: &str = "__DEFAULT_PARTITION__";
const DEFAULT_CHANGELOG_FILE_PREFIX: &str = "changelog-";
const DEFAULT_TARGET_FILE_SIZE: i64 = 256 * 1024 * 1024;
const DEFAULT_WRITE_PARQUET_BUFFER_SIZE: i64 = 256 * 1024 * 1024;
const DYNAMIC_BUCKET_TARGET_ROW_NUM_OPTION: &str = "dynamic-bucket.target-row-num";
const DEFAULT_DYNAMIC_BUCKET_TARGET_ROW_NUM: i64 = 200_000;
const DEFAULT_GLOBAL_INDEX_ROW_COUNT_PER_SHARD: i64 = 100_000;
const BLOB_AS_DESCRIPTOR_OPTION: &str = "blob-as-descriptor";
const BLOB_DESCRIPTOR_FIELD_OPTION: &str = "blob-descriptor-field";

/// Merge engine for primary-key tables.
///
/// Merge engine choice for a primary-key table — controls how same-key rows
/// are combined at read time (and during compaction).
///
/// Reference: Java `CoreOptions.MergeEngine`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergeEngine {
    /// Keep the row with the highest sequence number (default).
    Deduplicate,
    /// Merge same-key rows field-by-field, usually keeping non-null updates.
    PartialUpdate,
    /// Keep the first row for each key (ignore later updates).
    FirstRow,
    /// Versioned partial-update — same as `PartialUpdate` semantics but
    /// requires `(commit_snapshot_id, sequence_number)` ordering and
    /// supports per-file UPSERT/IGNORE merge mode + multi-version columns
    /// (`ROW<latest_version, latest_value, all_versioned_values MAP>`).
    /// See `docs/versioned-partial-update-impl-plan.md`.
    VersionedPartialUpdate,
    /// Apply per-field aggregate functions across rows sharing the same key.
    Aggregation,
}

/// Read-time policy for deletion vector tables, controlling whether the planner
/// strips L0 files for PK + DV-enabled tables and whether L0 entries skip
/// `value-stats` predicate pruning.
///
/// Mirrors Java `org.apache.paimon.CoreOptions.DvReadMode`:
/// - option declaration: `paimon-api/.../CoreOptions.java:1880-1889@e8938f347`
/// - enum definition: `paimon-api/.../CoreOptions.java:4144-4155@e8938f347`
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DvReadMode {
    /// Only read compacted data (level >= 1). Plan-time `skip_level_zero=true`.
    /// Best read performance; relies on the DV writer keeping L0 changes mirrored
    /// into L1+ via deletion vectors. **Default** (matches Java).
    Performance,
    /// Read all levels including level-0 for better data freshness.
    /// Plan-time `skip_level_zero=false`; `value-stats` predicate pruning is
    /// **skipped on L0 entries** to avoid the "stats pruning over PK-overlapping
    /// L0 files breaks sort-merge" trap (mirror Java
    /// `KeyValueFileStoreScan.java:154-162@e8938f347`).
    Freshness,
}

/// Per-file merge-mode flag carried on `DataFileMeta._MERGE_MODE` for
/// `versioned-partial-update` tables. Aligns with Java `VersionedMergeMode`
/// (`UPSERT=0` / `IGNORE=1`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VersionedMergeMode {
    /// Newer values overwrite existing ones (single-version columns) /
    /// MV map `put` always replaces (multi-version columns).
    Upsert,
    /// Newer values fill in only when current is null (single-version) /
    /// MV map `put` only when key absent (multi-version).
    Ignore,
}

impl VersionedMergeMode {
    /// Byte representation used in `DataFileMeta._MERGE_MODE`.
    pub fn to_byte(self) -> i8 {
        match self {
            VersionedMergeMode::Upsert => 0,
            VersionedMergeMode::Ignore => 1,
        }
    }

    /// Write-path helper: UPSERT serialises as `None` (Java's
    /// `DataFileMetaSerializer` null-encodes the default UPSERT case),
    /// IGNORE serialises as `Some(1)`. Pairs with [`Self::from_optional_byte`]
    /// so byte layout stays identical to Java.
    pub fn to_optional_byte(self) -> Option<i8> {
        match self {
            VersionedMergeMode::Upsert => None,
            VersionedMergeMode::Ignore => Some(self.to_byte()),
        }
    }

    /// Inverse of [`Self::to_byte`]. Unknown bytes are rejected so callers
    /// must opt into a fallback explicitly.
    pub fn from_byte(byte: i8) -> crate::Result<Self> {
        match byte {
            0 => Ok(VersionedMergeMode::Upsert),
            1 => Ok(VersionedMergeMode::Ignore),
            other => Err(crate::Error::Unsupported {
                message: format!("Unsupported versioned-partial-update merge mode byte: {other}"),
            }),
        }
    }

    /// Read-path helper: `None` means the on-disk value was absent (legacy
    /// or Java-side-omitted UPSERT) and falls back to [`Self::Upsert`];
    /// `Some(b)` is parsed strictly via [`Self::from_byte`] so unknown bytes
    /// become an integrity error rather than a silent UPSERT downgrade.
    /// Mirrors Java `DataFileMetaSerializer` which writes null for UPSERT
    /// and `VersionedMergeMode.fromByteValue` for any other value.
    pub fn from_optional_byte(byte: Option<i8>) -> crate::Result<Self> {
        match byte {
            None => Ok(VersionedMergeMode::Upsert),
            Some(b) => Self::from_byte(b),
        }
    }
}

impl std::str::FromStr for VersionedMergeMode {
    type Err = crate::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "upsert" => Ok(VersionedMergeMode::Upsert),
            "ignore" => Ok(VersionedMergeMode::Ignore),
            other => Err(crate::Error::Unsupported {
                message: format!(
                    "Unsupported versioned-partial-update.merge-mode: '{other}' (expected 'upsert' or 'ignore')"
                ),
            }),
        }
    }
}

/// Changelog producer for table writes.
///
/// Reference: Java `CoreOptions.ChangelogProducer`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangelogProducer {
    /// No changelog file.
    None,
    /// Double write input rows to changelog files.
    Input,
    /// Generate changelog files during full compaction.
    FullCompaction,
    /// Generate changelog files through lookup compaction.
    Lookup,
}

impl ChangelogProducer {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Input => "input",
            Self::FullCompaction => "full-compaction",
            Self::Lookup => "lookup",
        }
    }
}

pub(crate) fn first_row_supports_changelog_producer(producer: ChangelogProducer) -> bool {
    matches!(
        producer,
        ChangelogProducer::None | ChangelogProducer::Lookup
    )
}

/// Format the bucket directory name for a given bucket number.
/// Returns `"bucket-postpone"` for `POSTPONE_BUCKET` (-2), otherwise `"bucket-{N}"`.
pub fn bucket_dir_name(bucket: i32) -> String {
    if bucket == POSTPONE_BUCKET {
        POSTPONE_BUCKET_DIR.to_string()
    } else {
        format!("bucket-{bucket}")
    }
}

/// Typed accessors for common table options.
///
/// This mirrors pypaimon's `CoreOptions` pattern while staying lightweight.
#[derive(Debug, Clone, Copy)]
pub struct CoreOptions<'a> {
    options: &'a HashMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TimeTravelSelector<'a> {
    TimestampMillis(i64),
    /// Raw version string from `VERSION AS OF`. Resolved at scan time:
    /// tag name (if tag exists) → snapshot id (if parseable as i64) → error.
    Version(&'a str),
}

impl<'a> CoreOptions<'a> {
    pub fn new(options: &'a HashMap<String, String>) -> Self {
        Self { options }
    }

    pub fn deletion_vectors_enabled(&self) -> bool {
        self.options
            .get(DELETION_VECTORS_ENABLED_OPTION)
            .map(|value| value.eq_ignore_ascii_case("true"))
            .unwrap_or(false)
    }

    /// Whether the table is configured to write 64-bit deletion vectors
    /// (`Bitmap64DeletionVector` / `OptimizedRoaringBitmap64`). Required for
    /// row-tracking tables and Iceberg compatibility; the read side dispatches
    /// on the per-blob magic number regardless of this option, so it is
    /// currently consulted only for schema introspection and forward-compat
    /// with the future writer path. Default: `false` (matches Java).
    pub fn deletion_vectors_bitmap64(&self) -> bool {
        self.options
            .get(DELETION_VECTORS_BITMAP64_OPTION)
            .map(|value| value.eq_ignore_ascii_case("true"))
            .unwrap_or(false)
    }

    /// Read mode for deletion-vector tables (PERFORMANCE / FRESHNESS).
    ///
    /// Defaults to `Performance` to match Java
    /// (`paimon-api/.../CoreOptions.java:1880-1889@e8938f347`). PR-mode
    /// behavior is consumed by the planner: see [DvReadMode] for the contract
    /// each variant imposes on `skip_level_zero` and L0 value-stats pruning.
    pub fn deletion_vectors_read_mode(&self) -> crate::Result<DvReadMode> {
        match self.options.get(DELETION_VECTORS_READ_MODE_OPTION) {
            None => Ok(DvReadMode::Performance),
            Some(v) => match v.to_ascii_lowercase().as_str() {
                "performance" => Ok(DvReadMode::Performance),
                "freshness" => Ok(DvReadMode::Freshness),
                other => Err(crate::Error::Unsupported {
                    message: format!(
                        "Unsupported deletion-vectors.read-mode: '{other}' (expected 'performance' or 'freshness')"
                    ),
                }),
            },
        }
    }

    /// Returns the user-specified sequence field names, if configured.
    /// When set, the values of these columns are used as `_SEQUENCE_NUMBER` instead of auto-increment.
    /// Multiple fields can be comma-separated (e.g. `"col_a,col_b"`).
    pub fn sequence_fields(&self) -> Vec<&str> {
        self.options
            .get(SEQUENCE_FIELD_OPTION)
            .map(|s| s.split(',').map(str::trim).collect())
            .unwrap_or_default()
    }

    /// Merge engine for primary-key tables. Default is `Deduplicate`.
    pub fn merge_engine(&self) -> crate::Result<MergeEngine> {
        match self.options.get(MERGE_ENGINE_OPTION) {
            None => Ok(MergeEngine::Deduplicate),
            Some(v) => match v.to_ascii_lowercase().as_str() {
                "deduplicate" => Ok(MergeEngine::Deduplicate),
                "partial-update" => Ok(MergeEngine::PartialUpdate),
                "first-row" => Ok(MergeEngine::FirstRow),
                "versioned-partial-update" => Ok(MergeEngine::VersionedPartialUpdate),
                "aggregation" => Ok(MergeEngine::Aggregation),
                other => Err(crate::Error::Unsupported {
                    message: format!("Unsupported merge-engine: '{other}'"),
                }),
            },
        }
    }

    /// Raw changelog producer setting. Default is `"none"`.
    pub fn changelog_producer(&self) -> &str {
        self.options
            .get(CHANGELOG_PRODUCER_OPTION)
            .map(String::as_str)
            .unwrap_or("none")
    }

    /// Typed changelog producer setting. Default is `None`.
    pub fn try_changelog_producer(&self) -> crate::Result<ChangelogProducer> {
        match self.options.get(CHANGELOG_PRODUCER_OPTION) {
            None => Ok(ChangelogProducer::None),
            Some(v) => match v.to_ascii_lowercase().as_str() {
                "none" => Ok(ChangelogProducer::None),
                "input" => Ok(ChangelogProducer::Input),
                "full-compaction" => Ok(ChangelogProducer::FullCompaction),
                "lookup" => Ok(ChangelogProducer::Lookup),
                other => Err(crate::Error::Unsupported {
                    message: format!("Unsupported changelog-producer: '{other}'"),
                }),
            },
        }
    }

    /// The `rowkind.field` option: a user column whose value encodes the row kind.
    pub fn rowkind_field(&self) -> Option<&str> {
        self.options.get(ROWKIND_FIELD_OPTION).map(String::as_str)
    }

    pub fn data_evolution_enabled(&self) -> bool {
        self.options
            .get(DATA_EVOLUTION_ENABLED_OPTION)
            .map(|value| value.eq_ignore_ascii_case("true"))
            .unwrap_or(false)
    }

    pub fn global_index_enabled(&self) -> bool {
        self.options
            .get(GLOBAL_INDEX_ENABLED_OPTION)
            .map(|value| value.eq_ignore_ascii_case("true"))
            .unwrap_or(false)
    }

    pub fn global_index_row_count_per_shard(&self) -> crate::Result<i64> {
        let value = self
            .parse_i64_option(GLOBAL_INDEX_ROW_COUNT_PER_SHARD_OPTION)?
            .unwrap_or(DEFAULT_GLOBAL_INDEX_ROW_COUNT_PER_SHARD);
        if value <= 0 {
            return Err(crate::Error::DataInvalid {
                message: format!(
                    "Option '{}' must be greater than 0, got: {}",
                    GLOBAL_INDEX_ROW_COUNT_PER_SHARD_OPTION, value
                ),
                source: None,
            });
        }
        Ok(value)
    }

    pub fn source_split_target_size(&self) -> i64 {
        self.options
            .get(SOURCE_SPLIT_TARGET_SIZE_OPTION)
            .and_then(|value| parse_memory_size(value))
            .unwrap_or(DEFAULT_SOURCE_SPLIT_TARGET_SIZE)
    }

    pub fn source_split_open_file_cost(&self) -> i64 {
        self.options
            .get(SOURCE_SPLIT_OPEN_FILE_COST_OPTION)
            .and_then(|value| parse_memory_size(value))
            .unwrap_or(DEFAULT_SOURCE_SPLIT_OPEN_FILE_COST)
    }

    /// Rows per batch on the read path. Drives both the format reader's
    /// per-batch row count (parquet `with_batch_size`) and the sort-merge
    /// reader's output batch size. Corresponds to Java
    /// `CoreOptions.READ_BATCH_SIZE` (`read.batch-size`).
    pub fn read_batch_size(&self) -> usize {
        self.options
            .get(READ_BATCH_SIZE_OPTION)
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|&v| v > 0)
            .unwrap_or(DEFAULT_READ_BATCH_SIZE)
    }

    /// Whether to load parquet page index (ColumnIndex / OffsetIndex) and use
    /// page-level min/max for additional row-selection pruning. Default `true`
    /// to mirror the upstream parquet-hadoop behaviour Java relies on; set to
    /// `false` to opt out for files without page index where the metadata
    /// load cost outweighs the prune benefit. Corresponds to the Stage 1
    /// option in `docs/parquet-pushdown-plan.md`.
    pub fn parquet_page_index_enabled(&self) -> bool {
        match self.options.get(PARQUET_PAGE_INDEX_ENABLED_OPTION) {
            None => true,
            Some(value) if value.eq_ignore_ascii_case("true") => true,
            Some(value) if value.eq_ignore_ascii_case("false") => false,
            // Anything else is unparseable — fall back to the default rather
            // than silently flipping to off.
            Some(_) => true,
        }
    }

    /// Whether to evaluate parquet row-group bloom filters for `Eq` / `In`
    /// predicates and skip row groups that the bloom proves cannot match.
    /// Default `false`: paimon-rust's writer does not currently emit bloom
    /// filters (`ParquetFormatWriter::new` only sets compression on
    /// `WriterProperties`), so files without bloom would pay an async fetch
    /// per row group with no benefit. Set to `true` when reading files
    /// authored by a writer that does emit bloom filters. Corresponds to the
    /// Stage 2 option in `docs/parquet-pushdown-plan.md`.
    pub fn parquet_bloom_filter_enabled(&self) -> bool {
        match self.options.get(PARQUET_BLOOM_FILTER_ENABLED_OPTION) {
            None => false,
            Some(value) if value.eq_ignore_ascii_case("true") => true,
            Some(value) if value.eq_ignore_ascii_case("false") => false,
            Some(_) => false,
        }
    }

    /// `versioned-partial-update.merge-mode` — per-job intent for which
    /// merge mode newly written files should be stamped with. Persisted to
    /// each new file's `DataFileMeta._MERGE_MODE` at write time. Default
    /// `Upsert`. Aligns with Java
    /// `CoreOptions.VERSIONED_PARTIAL_UPDATE_MERGE_MODE`.
    pub fn versioned_partial_update_merge_mode(&self) -> crate::Result<VersionedMergeMode> {
        match self.options.get(VERSIONED_PARTIAL_UPDATE_MERGE_MODE_OPTION) {
            None => Ok(VersionedMergeMode::Upsert),
            Some(v) => v.parse::<VersionedMergeMode>(),
        }
    }

    /// `versioned-partial-update.ignore-mode.enabled` — whether IGNORE-mode
    /// files are *allowed* to be written/read against this table. When
    /// `true`, the table must have lookup capability (DV / force-lookup /
    /// changelog=lookup), enforced by [`Schema`] validation in stage 5.
    /// Default `true` (Java parity). Aligns with Java
    /// `CoreOptions.VERSIONED_PARTIAL_UPDATE_IGNORE_MODE_ENABLED`.
    pub fn versioned_partial_update_ignore_mode_enabled(&self) -> bool {
        self.options
            .get(VERSIONED_PARTIAL_UPDATE_IGNORE_MODE_ENABLED_OPTION)
            .map(|v| v.eq_ignore_ascii_case("true"))
            .unwrap_or(true)
    }

    /// `sequence.snapshot-ordering` — whether record precedence is determined
    /// by commit (snapshot) order. Default `false`. Required `true` by the
    /// `versioned-partial-update` merge engine.
    pub fn snapshot_sequence_ordering(&self) -> bool {
        self.options
            .get(SNAPSHOT_SEQUENCE_ORDERING_OPTION)
            .map(|v| v.eq_ignore_ascii_case("true"))
            .unwrap_or(false)
    }

    /// `force-lookup` — force the lookup pathway during compaction. Default
    /// `false`. Aligns with Java `CoreOptions.FORCE_LOOKUP`.
    pub fn force_lookup(&self) -> bool {
        self.options
            .get(FORCE_LOOKUP_OPTION)
            .map(|v| v.eq_ignore_ascii_case("true"))
            .unwrap_or(false)
    }

    /// Mirrors Java `LookupStrategy.needLookup` (paimon-api/.../lookup/
    /// LookupStrategy.java:40): true iff any of `produceChangelog`
    /// (`changelog-producer=lookup`), `deletionVector`
    /// (`deletion-vectors.enabled=true`), `isFirstRow`
    /// (`merge-engine=first-row`), or `forceLookup` (`force-lookup=true`)
    /// holds.
    pub fn need_lookup(&self) -> crate::Result<bool> {
        let produce_changelog =
            matches!(self.try_changelog_producer()?, ChangelogProducer::Lookup);
        let deletion_vector = self.deletion_vectors_enabled();
        let is_first_row = matches!(self.merge_engine()?, MergeEngine::FirstRow);
        let force_lookup = self.force_lookup();
        Ok(produce_changelog || deletion_vector || is_first_row || force_lookup)
    }

    /// `fields.default-aggregate-function` — the default aggregator applied
    /// to non-PK columns under the `aggregate` merge engine. Returns `None`
    /// when unset. Aligns with Java `CoreOptions.FIELDS_DEFAULT_AGG_FUNC`.
    pub fn fields_default_aggregate_function(&self) -> Option<&str> {
        self.options
            .get(FIELDS_DEFAULT_AGG_FUNC_OPTION)
            .map(String::as_str)
    }

    /// `fields.<field_name>.aggregate-function` — per-column aggregator
    /// override. Returns `None` when unset. Aligns with Java
    /// `CoreOptions.fieldAggFunc`.
    pub fn field_aggregate_function(&self, field_name: &str) -> Option<&str> {
        let key = format!("fields.{field_name}.aggregate-function");
        self.options.get(&key).map(String::as_str)
    }

    /// The default partition name for null/blank partition values.
    ///
    /// Corresponds to Java `CoreOptions.PARTITION_DEFAULT_NAME`.
    pub fn partition_default_name(&self) -> &str {
        self.options
            .get(PARTITION_DEFAULT_NAME_OPTION)
            .map(String::as_str)
            .unwrap_or(DEFAULT_PARTITION_DEFAULT_NAME)
    }

    /// Whether to use legacy partition name formatting (toString semantics).
    ///
    /// Corresponds to Java `CoreOptions.PARTITION_GENERATE_LEGACY_NAME`.
    /// Default: `true` to match Java Paimon.
    pub fn legacy_partition_name(&self) -> bool {
        self.options
            .get(PARTITION_LEGACY_NAME_OPTION)
            .map(|v| v.eq_ignore_ascii_case("true"))
            .unwrap_or(true)
    }

    fn parse_i64_option(&self, option_name: &'static str) -> crate::Result<Option<i64>> {
        match self.options.get(option_name) {
            Some(value) => value
                .parse::<i64>()
                .map(Some)
                .map_err(|e| crate::Error::DataInvalid {
                    message: format!("Invalid value for {option_name}: '{value}'"),
                    source: Some(Box::new(e)),
                }),
            None => Ok(None),
        }
    }

    /// Raw timestamp accessor for `scan.timestamp-millis`.
    ///
    /// This compatibility accessor is lossy: it returns `None` for absent or
    /// invalid values and does not validate selector conflicts. Internal
    /// time-travel planning should use `try_time_travel_selector`.
    pub fn scan_timestamp_millis(&self) -> Option<i64> {
        self.options
            .get(SCAN_TIMESTAMP_MILLIS_OPTION)
            .and_then(|v| v.parse().ok())
    }

    fn configured_time_travel_selectors(&self) -> Vec<&'static str> {
        let mut selectors = Vec::with_capacity(2);
        if self.options.contains_key(SCAN_TIMESTAMP_MILLIS_OPTION) {
            selectors.push(SCAN_TIMESTAMP_MILLIS_OPTION);
        }
        if self.options.contains_key(SCAN_VERSION_OPTION) {
            selectors.push(SCAN_VERSION_OPTION);
        }
        selectors
    }

    /// Validates and normalizes the internal time-travel selector.
    ///
    /// This is the semantic owner for selector mutual exclusion and strict
    /// numeric parsing.
    pub(crate) fn try_time_travel_selector(&self) -> crate::Result<Option<TimeTravelSelector<'a>>> {
        let selectors = self.configured_time_travel_selectors();
        if selectors.len() > 1 {
            return Err(crate::Error::DataInvalid {
                message: format!(
                    "Only one time-travel selector may be set, found: {}",
                    selectors.join(", ")
                ),
                source: None,
            });
        }

        if let Some(ts) = self.parse_i64_option(SCAN_TIMESTAMP_MILLIS_OPTION)? {
            Ok(Some(TimeTravelSelector::TimestampMillis(ts)))
        } else if let Some(version) = self.options.get(SCAN_VERSION_OPTION).map(String::as_str) {
            Ok(Some(TimeTravelSelector::Version(version)))
        } else {
            Ok(None)
        }
    }

    /// Explicit bucket key columns. If not set, defaults to primary keys for PK tables.
    pub fn bucket_key(&self) -> Option<Vec<String>> {
        self.options
            .get(BUCKET_KEY_OPTION)
            .map(|v| v.split(',').map(|s| s.trim().to_string()).collect())
    }

    pub fn commit_max_retries(&self) -> u32 {
        self.options
            .get(COMMIT_MAX_RETRIES_OPTION)
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_COMMIT_MAX_RETRIES)
    }

    pub fn commit_timeout_ms(&self) -> u64 {
        self.options
            .get(COMMIT_TIMEOUT_OPTION)
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_COMMIT_TIMEOUT_MS)
    }

    pub fn commit_min_retry_wait_ms(&self) -> u64 {
        self.options
            .get(COMMIT_MIN_RETRY_WAIT_OPTION)
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_COMMIT_MIN_RETRY_WAIT_MS)
    }

    pub fn commit_max_retry_wait_ms(&self) -> u64 {
        self.options
            .get(COMMIT_MAX_RETRY_WAIT_OPTION)
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_COMMIT_MAX_RETRY_WAIT_MS)
    }

    pub fn row_tracking_enabled(&self) -> bool {
        self.options
            .get(ROW_TRACKING_ENABLED_OPTION)
            .map(|v| v.eq_ignore_ascii_case("true"))
            .unwrap_or(false)
    }

    /// Number of buckets for the table. Default is 1.
    pub fn bucket(&self) -> i32 {
        self.options
            .get(BUCKET_OPTION)
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_BUCKET)
    }

    /// Whether the bucket function type is the default hash-based function.
    ///
    /// Only the default function (`Math.abs(hash % numBuckets)`) is supported
    /// for bucket predicate pruning. `mod` and `hive` use different algorithms.
    pub fn is_default_bucket_function(&self) -> bool {
        self.options
            .get(BUCKET_FUNCTION_TYPE_OPTION)
            .map(|v| v.eq_ignore_ascii_case("default"))
            .unwrap_or(true)
    }

    /// Target file size for data files. Default is 128MB.
    pub fn target_file_size(&self) -> i64 {
        self.options
            .get("target-file-size")
            .and_then(|v| parse_memory_size(v))
            .unwrap_or(DEFAULT_TARGET_FILE_SIZE)
    }

    pub fn blob_target_file_size(&self) -> i64 {
        self.options
            .get("blob.target-file-size")
            .and_then(|v| parse_memory_size(v))
            .unwrap_or_else(|| self.target_file_size())
    }

    /// File format for data files (e.g. "parquet", "orc", "avro", "vortex").
    /// Default is "parquet".
    pub fn file_format(&self) -> &str {
        self.options
            .get(FILE_FORMAT_OPTION)
            .map(String::as_str)
            .unwrap_or("parquet")
    }

    /// File compression codec (e.g. "lz4", "zstd", "snappy", "none").
    /// Default is "zstd".
    pub fn file_compression(&self) -> &str {
        self.options
            .get(FILE_COMPRESSION_OPTION)
            .map(String::as_str)
            .unwrap_or("zstd")
    }

    /// Zstd compression level. Only meaningful when `file.compression` is `"zstd"`.
    /// Default is 1 (matching Paimon Java).
    pub fn file_compression_zstd_level(&self) -> i32 {
        self.options
            .get(FILE_COMPRESSION_ZSTD_LEVEL_OPTION)
            .and_then(|v| v.parse().ok())
            .unwrap_or(1)
    }

    /// File name prefix for changelog files. Default is `"changelog-"`.
    pub fn changelog_file_prefix(&self) -> &str {
        self.options
            .get(CHANGELOG_FILE_PREFIX_OPTION)
            .map(String::as_str)
            .unwrap_or(DEFAULT_CHANGELOG_FILE_PREFIX)
    }

    /// Effective file format for changelog files.
    ///
    /// When `changelog-file.format` is not configured, Java Paimon falls back
    /// to the table `file.format`.
    pub fn changelog_file_format(&self) -> &str {
        self.options
            .get(CHANGELOG_FILE_FORMAT_OPTION)
            .map(String::as_str)
            .unwrap_or_else(|| self.file_format())
    }

    /// Effective compression codec for changelog files.
    ///
    /// When `changelog-file.compression` is not configured, Java Paimon falls
    /// back to the table `file.compression`.
    pub fn changelog_file_compression(&self) -> &str {
        self.options
            .get(CHANGELOG_FILE_COMPRESSION_OPTION)
            .map(String::as_str)
            .unwrap_or_else(|| self.file_compression())
    }

    /// Metadata stats collection mode for changelog files, if configured.
    pub fn changelog_file_stats_mode(&self) -> Option<&str> {
        self.options
            .get(CHANGELOG_FILE_STATS_MODE_OPTION)
            .map(String::as_str)
    }

    /// Parquet writer in-progress buffer size limit. Default is 256MB.
    /// When the buffered data exceeds this, the writer flushes the current row group.
    pub fn write_parquet_buffer_size(&self) -> i64 {
        self.options
            .get(WRITE_PARQUET_BUFFER_SIZE_OPTION)
            .and_then(|v| parse_memory_size(v))
            .unwrap_or(DEFAULT_WRITE_PARQUET_BUFFER_SIZE)
    }

    /// Target row number per bucket for dynamic bucket mode (bucket=-1).
    /// When a bucket reaches this number, a new bucket is created.
    /// Default is 200,000 (matching Java Paimon).
    pub fn dynamic_bucket_target_row_num(&self) -> i64 {
        self.options
            .get(DYNAMIC_BUCKET_TARGET_ROW_NUM_OPTION)
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_DYNAMIC_BUCKET_TARGET_ROW_NUM)
    }

    /// When true, blob field reads return serialized BlobDescriptor bytes
    /// instead of actual blob bytes. Default is false.
    pub fn blob_as_descriptor(&self) -> bool {
        self.options
            .get(BLOB_AS_DESCRIPTOR_OPTION)
            .map(|v| v.eq_ignore_ascii_case("true"))
            .unwrap_or(false)
    }

    /// Comma-separated BLOB field names stored as serialized BlobDescriptor
    /// bytes inline in normal data files (no .blob files for these fields).
    pub fn blob_descriptor_fields(&self) -> HashSet<String> {
        self.options
            .get(BLOB_DESCRIPTOR_FIELD_OPTION)
            .map(|s| s.split(',').map(|f| f.trim().to_string()).collect())
            .unwrap_or_default()
    }
}

/// Parse a memory size string to bytes using binary (1024-based) semantics.
///
/// Supports formats like `128 mb`, `128mb`, `4 gb`, `1024` (plain bytes).
/// Uses binary units: `kb` = 1024, `mb` = 1024², `gb` = 1024³, matching Java Paimon's `MemorySize`.
///
/// NOTE: Java Paimon's `MemorySize` also accepts long unit names such as `bytes`,
/// `kibibytes`, `mebibytes`, `gibibytes`, and `tebibytes`. This implementation
/// only supports short units (`b`, `kb`, `mb`, `gb`, `tb`), which covers all practical usage.
fn parse_memory_size(value: &str) -> Option<i64> {
    let value = value.trim();
    if value.is_empty() {
        return None;
    }

    let pos = value
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(value.len());
    let (num_str, unit_str) = value.split_at(pos);
    let num: i64 = num_str.trim().parse().ok()?;
    let multiplier = match unit_str.trim().to_ascii_lowercase().as_str() {
        "" | "b" => 1,
        "kb" | "k" => 1024,
        "mb" | "m" => 1024 * 1024,
        "gb" | "g" => 1024 * 1024 * 1024,
        "tb" | "t" => 1024 * 1024 * 1024 * 1024,
        _ => return None,
    };
    Some(num * multiplier)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_source_split_defaults() {
        let options = HashMap::new();
        let core_options = CoreOptions::new(&options);

        assert_eq!(core_options.source_split_target_size(), 128 * 1024 * 1024);
        assert_eq!(core_options.source_split_open_file_cost(), 4 * 1024 * 1024);
        assert_eq!(
            core_options.global_index_row_count_per_shard().unwrap(),
            100_000
        );
    }

    #[test]
    fn test_source_split_custom_values() {
        let options = HashMap::from([
            (
                SOURCE_SPLIT_TARGET_SIZE_OPTION.to_string(),
                "256 mb".to_string(),
            ),
            (
                SOURCE_SPLIT_OPEN_FILE_COST_OPTION.to_string(),
                "8 mb".to_string(),
            ),
            (
                GLOBAL_INDEX_ROW_COUNT_PER_SHARD_OPTION.to_string(),
                "2048".to_string(),
            ),
        ]);
        let core_options = CoreOptions::new(&options);

        assert_eq!(core_options.source_split_target_size(), 256 * 1024 * 1024);
        assert_eq!(core_options.source_split_open_file_cost(), 8 * 1024 * 1024);
        assert_eq!(
            core_options.global_index_row_count_per_shard().unwrap(),
            2048
        );
    }

    #[test]
    fn test_global_index_row_count_per_shard_rejects_invalid_values() {
        for value in ["0", "-1", "abc"] {
            let options = HashMap::from([(
                GLOBAL_INDEX_ROW_COUNT_PER_SHARD_OPTION.to_string(),
                value.to_string(),
            )]);
            let core = CoreOptions::new(&options);

            let err = core
                .global_index_row_count_per_shard()
                .expect_err("invalid rows-per-shard should fail");
            assert!(matches!(err, crate::Error::DataInvalid { message, .. }
                    if message.contains(GLOBAL_INDEX_ROW_COUNT_PER_SHARD_OPTION)));
        }
    }

    #[test]
    fn test_parse_memory_size() {
        assert_eq!(parse_memory_size("1024"), Some(1024));
        assert_eq!(parse_memory_size("128 mb"), Some(128 * 1024 * 1024));
        assert_eq!(parse_memory_size("128mb"), Some(128 * 1024 * 1024));
        assert_eq!(parse_memory_size("4MB"), Some(4 * 1024 * 1024));
        assert_eq!(parse_memory_size("1 gb"), Some(1024 * 1024 * 1024));
        assert_eq!(parse_memory_size("1024 kb"), Some(1024 * 1024));
        assert_eq!(parse_memory_size("100 b"), Some(100));
        assert_eq!(parse_memory_size(""), None);
        assert_eq!(parse_memory_size("abc"), None);
    }

    #[test]
    fn test_partition_options_defaults() {
        let options = HashMap::new();
        let core = CoreOptions::new(&options);
        assert_eq!(core.partition_default_name(), "__DEFAULT_PARTITION__");
        assert!(core.legacy_partition_name());
    }

    #[test]
    fn test_partition_options_custom() {
        let options = HashMap::from([
            (
                PARTITION_DEFAULT_NAME_OPTION.to_string(),
                "NULL_PART".to_string(),
            ),
            (
                PARTITION_LEGACY_NAME_OPTION.to_string(),
                "false".to_string(),
            ),
        ]);
        let core = CoreOptions::new(&options);
        assert_eq!(core.partition_default_name(), "NULL_PART");
        assert!(!core.legacy_partition_name());
    }

    #[test]
    fn test_try_time_travel_selector_rejects_conflicting_selectors() {
        let options = HashMap::from([
            (SCAN_VERSION_OPTION.to_string(), "tag1".to_string()),
            (SCAN_TIMESTAMP_MILLIS_OPTION.to_string(), "1234".to_string()),
        ]);
        let core = CoreOptions::new(&options);

        let err = core
            .try_time_travel_selector()
            .expect_err("conflicting selectors should fail");
        match err {
            crate::Error::DataInvalid { message, .. } => {
                assert!(message.contains("Only one time-travel selector may be set"));
                assert!(message.contains(SCAN_VERSION_OPTION));
                assert!(message.contains(SCAN_TIMESTAMP_MILLIS_OPTION));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn test_try_time_travel_selector_rejects_invalid_numeric_values() {
        let timestamp_options =
            HashMap::from([(SCAN_TIMESTAMP_MILLIS_OPTION.to_string(), "xyz".to_string())]);
        let timestamp_core = CoreOptions::new(&timestamp_options);

        let timestamp_err = timestamp_core
            .try_time_travel_selector()
            .expect_err("invalid timestamp millis should fail");
        match timestamp_err {
            crate::Error::DataInvalid { message, .. } => {
                assert!(message.contains(SCAN_TIMESTAMP_MILLIS_OPTION));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn test_merge_engine_accepts_partial_update() {
        let options = HashMap::from([(MERGE_ENGINE_OPTION.to_string(), "partial-update".into())]);
        let core = CoreOptions::new(&options);

        assert_eq!(core.merge_engine().unwrap(), MergeEngine::PartialUpdate);
    }

    #[test]
    fn test_merge_engine_accepts_versioned_partial_update() {
        let options = HashMap::from([(
            MERGE_ENGINE_OPTION.to_string(),
            "versioned-partial-update".into(),
        )]);
        let core = CoreOptions::new(&options);
        assert_eq!(
            core.merge_engine().unwrap(),
            MergeEngine::VersionedPartialUpdate
        );
    }

    #[test]
    fn test_deletion_vectors_read_mode_default_is_performance() {
        let opts = HashMap::new();
        let core = CoreOptions::new(&opts);
        assert_eq!(
            core.deletion_vectors_read_mode().unwrap(),
            DvReadMode::Performance
        );
    }

    #[test]
    fn test_deletion_vectors_read_mode_accepts_performance() {
        let opts = HashMap::from([(
            DELETION_VECTORS_READ_MODE_OPTION.to_string(),
            "performance".into(),
        )]);
        let core = CoreOptions::new(&opts);
        assert_eq!(
            core.deletion_vectors_read_mode().unwrap(),
            DvReadMode::Performance
        );
    }

    #[test]
    fn test_deletion_vectors_read_mode_accepts_freshness_case_insensitive() {
        let opts = HashMap::from([(
            DELETION_VECTORS_READ_MODE_OPTION.to_string(),
            "FRESHNESS".into(),
        )]);
        let core = CoreOptions::new(&opts);
        assert_eq!(
            core.deletion_vectors_read_mode().unwrap(),
            DvReadMode::Freshness
        );
    }

    #[test]
    fn test_deletion_vectors_read_mode_rejects_unknown() {
        let opts = HashMap::from([(
            DELETION_VECTORS_READ_MODE_OPTION.to_string(),
            "balanced".into(),
        )]);
        let core = CoreOptions::new(&opts);
        let err = core.deletion_vectors_read_mode().unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("Unsupported deletion-vectors.read-mode"),
            "expected unsupported-mode error, got: {msg}"
        );
    }

    #[test]
    fn test_versioned_merge_mode_default_is_upsert() {
        let opts = HashMap::new();
        let core = CoreOptions::new(&opts);
        assert_eq!(
            core.versioned_partial_update_merge_mode().unwrap(),
            VersionedMergeMode::Upsert
        );
        // Default `ignore-mode.enabled` mirrors Java parity (true);
        // Stage 5 validation will reject `true` until lookup capability lands.
        assert!(core.versioned_partial_update_ignore_mode_enabled());
    }

    #[test]
    fn test_versioned_merge_mode_parses_ignore_and_rejects_unknown() {
        let mut opts = HashMap::new();
        opts.insert(
            VERSIONED_PARTIAL_UPDATE_MERGE_MODE_OPTION.to_string(),
            "ignore".into(),
        );
        let core = CoreOptions::new(&opts);
        assert_eq!(
            core.versioned_partial_update_merge_mode().unwrap(),
            VersionedMergeMode::Ignore
        );

        let mut bad = HashMap::new();
        bad.insert(
            VERSIONED_PARTIAL_UPDATE_MERGE_MODE_OPTION.to_string(),
            "weird".into(),
        );
        let core = CoreOptions::new(&bad);
        assert!(core.versioned_partial_update_merge_mode().is_err());
    }

    #[test]
    fn test_versioned_merge_mode_byte_round_trip() {
        for mode in [VersionedMergeMode::Upsert, VersionedMergeMode::Ignore] {
            assert_eq!(VersionedMergeMode::from_byte(mode.to_byte()).unwrap(), mode);
        }
        assert!(VersionedMergeMode::from_byte(7).is_err());
    }

    /// Read-path parity with Java `DataFileMetaSerializer`: missing field
    /// (`None`) falls back to UPSERT; valid bytes parse strictly; unknown
    /// bytes are an error (no silent downgrade).
    #[test]
    fn test_versioned_merge_mode_from_optional_byte() {
        assert_eq!(
            VersionedMergeMode::from_optional_byte(None).unwrap(),
            VersionedMergeMode::Upsert
        );
        assert_eq!(
            VersionedMergeMode::from_optional_byte(Some(0)).unwrap(),
            VersionedMergeMode::Upsert
        );
        assert_eq!(
            VersionedMergeMode::from_optional_byte(Some(1)).unwrap(),
            VersionedMergeMode::Ignore
        );
        let err = VersionedMergeMode::from_optional_byte(Some(7)).unwrap_err();
        assert!(matches!(err, crate::Error::Unsupported { ref message }
                if message.contains('7')));
    }

    /// Write-path parity with Java `DataFileMetaSerializer`: UPSERT writes
    /// null, IGNORE writes the byte. Round-trips with `from_optional_byte`.
    #[test]
    fn test_versioned_merge_mode_to_optional_byte() {
        assert_eq!(VersionedMergeMode::Upsert.to_optional_byte(), None);
        assert_eq!(VersionedMergeMode::Ignore.to_optional_byte(), Some(1));
        for mode in [VersionedMergeMode::Upsert, VersionedMergeMode::Ignore] {
            let round_tripped =
                VersionedMergeMode::from_optional_byte(mode.to_optional_byte()).unwrap();
            assert_eq!(round_tripped, mode);
        }
    }

    #[test]
    fn test_ignore_mode_enabled_parses_false() {
        let opts = HashMap::from([(
            VERSIONED_PARTIAL_UPDATE_IGNORE_MODE_ENABLED_OPTION.to_string(),
            "false".into(),
        )]);
        let core = CoreOptions::new(&opts);
        assert!(!core.versioned_partial_update_ignore_mode_enabled());
    }

    #[test]
    fn test_merge_engine_accepts_aggregation() {
        let options = HashMap::from([(MERGE_ENGINE_OPTION.to_string(), "aggregation".into())]);
        let core = CoreOptions::new(&options);

        assert_eq!(core.merge_engine().unwrap(), MergeEngine::Aggregation);
    }

    #[test]
    fn test_changelog_producer_defaults_to_none() {
        let options = HashMap::new();
        let core = CoreOptions::new(&options);

        assert_eq!(core.changelog_producer(), "none");
        assert_eq!(
            core.try_changelog_producer().unwrap(),
            ChangelogProducer::None
        );
    }

    #[test]
    fn test_changelog_producer_accepts_known_values() {
        for (value, expected) in [
            ("none", ChangelogProducer::None),
            ("input", ChangelogProducer::Input),
            ("full-compaction", ChangelogProducer::FullCompaction),
            ("lookup", ChangelogProducer::Lookup),
            ("INPUT", ChangelogProducer::Input),
        ] {
            let options = HashMap::from([(CHANGELOG_PRODUCER_OPTION.to_string(), value.into())]);
            let core = CoreOptions::new(&options);

            assert_eq!(core.try_changelog_producer().unwrap(), expected);
        }
    }

    #[test]
    fn test_changelog_producer_rejects_unknown_values() {
        let options = HashMap::from([(CHANGELOG_PRODUCER_OPTION.to_string(), "other".into())]);
        let core = CoreOptions::new(&options);

        let err = core
            .try_changelog_producer()
            .expect_err("unknown producer should fail");
        assert!(
            matches!(err, crate::Error::Unsupported { message } if message.contains("Unsupported changelog-producer"))
        );
    }

    #[test]
    fn test_changelog_file_options_defaults_and_overrides() {
        let default_options = HashMap::from([
            (FILE_FORMAT_OPTION.to_string(), "avro".to_string()),
            (FILE_COMPRESSION_OPTION.to_string(), "snappy".to_string()),
        ]);
        let default_core = CoreOptions::new(&default_options);

        assert_eq!(default_core.changelog_file_prefix(), "changelog-");
        assert_eq!(default_core.changelog_file_format(), "avro");
        assert_eq!(default_core.changelog_file_compression(), "snappy");
        assert_eq!(default_core.changelog_file_stats_mode(), None);

        let custom_options = HashMap::from([
            (
                CHANGELOG_FILE_PREFIX_OPTION.to_string(),
                "custom-".to_string(),
            ),
            (
                CHANGELOG_FILE_FORMAT_OPTION.to_string(),
                "parquet".to_string(),
            ),
            (
                CHANGELOG_FILE_COMPRESSION_OPTION.to_string(),
                "zstd".to_string(),
            ),
            (
                CHANGELOG_FILE_STATS_MODE_OPTION.to_string(),
                "counts".to_string(),
            ),
        ]);
        let custom_core = CoreOptions::new(&custom_options);

        assert_eq!(custom_core.changelog_file_prefix(), "custom-");
        assert_eq!(custom_core.changelog_file_format(), "parquet");
        assert_eq!(custom_core.changelog_file_compression(), "zstd");
        assert_eq!(custom_core.changelog_file_stats_mode(), Some("counts"));
    }

    #[test]
    fn test_commit_options_defaults() {
        let options = HashMap::new();
        let core = CoreOptions::new(&options);
        assert_eq!(core.bucket(), -1);
        assert_eq!(core.commit_max_retries(), 10);
        assert_eq!(core.commit_timeout_ms(), 120_000);
        assert_eq!(core.commit_min_retry_wait_ms(), 1_000);
        assert_eq!(core.commit_max_retry_wait_ms(), 10_000);
        assert!(!core.row_tracking_enabled());
    }

    #[test]
    fn test_commit_options_custom() {
        let options = HashMap::from([
            (BUCKET_OPTION.to_string(), "4".to_string()),
            (COMMIT_MAX_RETRIES_OPTION.to_string(), "20".to_string()),
            (COMMIT_TIMEOUT_OPTION.to_string(), "60000".to_string()),
            (COMMIT_MIN_RETRY_WAIT_OPTION.to_string(), "500".to_string()),
            (COMMIT_MAX_RETRY_WAIT_OPTION.to_string(), "5000".to_string()),
            (ROW_TRACKING_ENABLED_OPTION.to_string(), "true".to_string()),
        ]);
        let core = CoreOptions::new(&options);
        assert_eq!(core.bucket(), 4);
        assert_eq!(core.commit_max_retries(), 20);
        assert_eq!(core.commit_timeout_ms(), 60_000);
        assert_eq!(core.commit_min_retry_wait_ms(), 500);
        assert_eq!(core.commit_max_retry_wait_ms(), 5_000);
        assert!(core.row_tracking_enabled());
    }

    #[test]
    fn test_try_time_travel_selector_normalizes_valid_selector() {
        let timestamp_options =
            HashMap::from([(SCAN_TIMESTAMP_MILLIS_OPTION.to_string(), "1234".to_string())]);
        let timestamp_core = CoreOptions::new(&timestamp_options);
        assert_eq!(
            timestamp_core
                .try_time_travel_selector()
                .expect("timestamp selector"),
            Some(TimeTravelSelector::TimestampMillis(1234))
        );

        let version_options =
            HashMap::from([(SCAN_VERSION_OPTION.to_string(), "my-tag".to_string())]);
        let version_core = CoreOptions::new(&version_options);
        assert_eq!(
            version_core
                .try_time_travel_selector()
                .expect("version selector"),
            Some(TimeTravelSelector::Version("my-tag"))
        );

        let version_num_options =
            HashMap::from([(SCAN_VERSION_OPTION.to_string(), "3".to_string())]);
        let version_num_core = CoreOptions::new(&version_num_options);
        assert_eq!(
            version_num_core
                .try_time_travel_selector()
                .expect("version numeric selector"),
            Some(TimeTravelSelector::Version("3"))
        );
    }

    #[test]
    fn test_write_options_defaults() {
        let options = HashMap::new();
        let core = CoreOptions::new(&options);
        assert_eq!(core.write_parquet_buffer_size(), 256 * 1024 * 1024);
    }

    #[test]
    fn test_write_options_custom() {
        let options = HashMap::from([(
            WRITE_PARQUET_BUFFER_SIZE_OPTION.to_string(),
            "32mb".to_string(),
        )]);
        let core = CoreOptions::new(&options);
        assert_eq!(core.write_parquet_buffer_size(), 32 * 1024 * 1024);
    }

    #[test]
    fn test_snapshot_sequence_ordering_default_false() {
        let opts = HashMap::new();
        let core = CoreOptions::new(&opts);
        assert!(!core.snapshot_sequence_ordering());
    }

    #[test]
    fn test_snapshot_sequence_ordering_true() {
        let options = HashMap::from([(
            SNAPSHOT_SEQUENCE_ORDERING_OPTION.to_string(),
            "true".to_string(),
        )]);
        let core = CoreOptions::new(&options);
        assert!(core.snapshot_sequence_ordering());
    }

    #[test]
    fn test_force_lookup_default_false() {
        let opts = HashMap::new();
        let core = CoreOptions::new(&opts);
        assert!(!core.force_lookup());
    }

    #[test]
    fn test_force_lookup_true() {
        let options = HashMap::from([(FORCE_LOOKUP_OPTION.to_string(), "true".to_string())]);
        let core = CoreOptions::new(&options);
        assert!(core.force_lookup());
    }

    #[test]
    fn test_need_lookup_false_by_default() {
        let opts = HashMap::new();
        let core = CoreOptions::new(&opts);
        assert!(!core.need_lookup().unwrap());
    }

    #[test]
    fn test_need_lookup_true_for_force_lookup() {
        let options = HashMap::from([(FORCE_LOOKUP_OPTION.to_string(), "true".to_string())]);
        let core = CoreOptions::new(&options);
        assert!(core.need_lookup().unwrap());
    }

    #[test]
    fn test_need_lookup_true_for_dv() {
        let options = HashMap::from([(
            DELETION_VECTORS_ENABLED_OPTION.to_string(),
            "true".to_string(),
        )]);
        let core = CoreOptions::new(&options);
        assert!(core.need_lookup().unwrap());
    }

    #[test]
    fn test_need_lookup_true_for_lookup_changelog() {
        let options =
            HashMap::from([(CHANGELOG_PRODUCER_OPTION.to_string(), "lookup".to_string())]);
        let core = CoreOptions::new(&options);
        assert!(core.need_lookup().unwrap());
    }

    #[test]
    fn test_need_lookup_true_for_first_row_engine() {
        let options =
            HashMap::from([(MERGE_ENGINE_OPTION.to_string(), "first-row".to_string())]);
        let core = CoreOptions::new(&options);
        assert!(core.need_lookup().unwrap());
    }

    #[test]
    fn test_fields_default_aggregate_function() {
        let opts = HashMap::new();
        let core = CoreOptions::new(&opts);
        assert_eq!(core.fields_default_aggregate_function(), None);

        let options = HashMap::from([(
            FIELDS_DEFAULT_AGG_FUNC_OPTION.to_string(),
            "sum".to_string(),
        )]);
        let core = CoreOptions::new(&options);
        assert_eq!(core.fields_default_aggregate_function(), Some("sum"));
    }

    #[test]
    fn test_field_aggregate_function() {
        let opts = HashMap::new();
        let core = CoreOptions::new(&opts);
        assert_eq!(core.field_aggregate_function("price"), None);

        let options = HashMap::from([(
            "fields.price.aggregate-function".to_string(),
            "max".to_string(),
        )]);
        let core = CoreOptions::new(&options);
        assert_eq!(core.field_aggregate_function("price"), Some("max"));
        assert_eq!(core.field_aggregate_function("other"), None);
    }

    #[test]
    fn test_parquet_page_index_default_is_enabled() {
        let opts = HashMap::new();
        let core = CoreOptions::new(&opts);
        assert!(core.parquet_page_index_enabled());
    }

    #[test]
    fn test_parquet_page_index_explicit_off() {
        let opts = HashMap::from([(
            PARQUET_PAGE_INDEX_ENABLED_OPTION.to_string(),
            "false".into(),
        )]);
        let core = CoreOptions::new(&opts);
        assert!(!core.parquet_page_index_enabled());
    }

    #[test]
    fn test_parquet_page_index_case_insensitive_true() {
        let opts = HashMap::from([(
            PARQUET_PAGE_INDEX_ENABLED_OPTION.to_string(),
            "TRUE".into(),
        )]);
        let core = CoreOptions::new(&opts);
        assert!(core.parquet_page_index_enabled());
    }

    #[test]
    fn test_parquet_page_index_unparseable_falls_back_to_default() {
        let opts = HashMap::from([(
            PARQUET_PAGE_INDEX_ENABLED_OPTION.to_string(),
            "yes".into(),
        )]);
        let core = CoreOptions::new(&opts);
        // Unparseable values fall back to the default (true), not silently
        // flipping the toggle off.
        assert!(core.parquet_page_index_enabled());
    }

    #[test]
    fn test_parquet_bloom_filter_default_is_disabled() {
        let opts = HashMap::new();
        let core = CoreOptions::new(&opts);
        assert!(!core.parquet_bloom_filter_enabled());
    }

    #[test]
    fn test_parquet_bloom_filter_explicit_on() {
        let opts = HashMap::from([(
            PARQUET_BLOOM_FILTER_ENABLED_OPTION.to_string(),
            "true".into(),
        )]);
        let core = CoreOptions::new(&opts);
        assert!(core.parquet_bloom_filter_enabled());
    }

    #[test]
    fn test_parquet_bloom_filter_case_insensitive_on() {
        let opts = HashMap::from([(
            PARQUET_BLOOM_FILTER_ENABLED_OPTION.to_string(),
            "TRUE".into(),
        )]);
        let core = CoreOptions::new(&opts);
        assert!(core.parquet_bloom_filter_enabled());
    }

    #[test]
    fn test_parquet_bloom_filter_unparseable_falls_back_to_default() {
        let opts = HashMap::from([(
            PARQUET_BLOOM_FILTER_ENABLED_OPTION.to_string(),
            "yes".into(),
        )]);
        let core = CoreOptions::new(&opts);
        // Unparseable values fall back to the default (false).
        assert!(!core.parquet_bloom_filter_enabled());
    }
}
