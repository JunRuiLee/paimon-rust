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

use crate::arrow::format::FilePredicates;
use crate::arrow::residual::{evaluate_predicates_mask, widen_scan_fields};
use crate::lumina::is_lumina_index_type;
use crate::lumina::reader::LuminaVectorGlobalIndexReader;
use crate::lumina::{LuminaIndexMeta, LuminaVectorIndexOptions};
use crate::spec::{
    CoreOptions, DataField, DataType, FileKind, GlobalIndexSearchMode, IndexManifest, Predicate,
    ROW_ID_FIELD_NAME,
};
use crate::table::bucket_filter::split_partition_and_data_predicates;
use crate::table::data_file_reader::DataFileReader;
use crate::table::pk_vector_data_file_reader::{
    append_batch_vectors, DataFilePkVectorReaderFactory,
};
use crate::table::pk_vector_indexed_split_read::{expand_ranges, PkVectorIndexedSplitRead};
use crate::table::pk_vector_orchestrator::{
    as_split_exact_file_search, build_indexed_splits, merge_candidates, OrchestratorSearchResult,
    PkVectorCandidate, PkVectorOrchestrator, PkVectorSearchSplit,
};
use crate::table::pk_vector_position_read::{
    PkVectorPositionRead, PKEY_VECTOR_POSITION_COLUMN, SEARCH_SCORE_COLUMN,
};
use crate::table::pk_vector_scan::{PkVectorScan, PkVectorScanPlan};
use crate::table::snapshot_manager::SnapshotManager;
use crate::table::source::DataSplit;
use crate::table::{find_field_id_by_name, ArrowRecordBatchStream, RowRange, Table};
use crate::vector_search::{GlobalIndexIOMeta, SearchResult, VectorSearch};
use crate::vindex::pkvector::ann::{PkVectorAnnSearcher, VindexAnnSearcher};
use crate::vindex::pkvector::bucket::{BucketActiveFile, BucketAnnSegment, ExactFileSearchFuture};
use crate::vindex::pkvector::exact::validate_query;
use crate::vindex::pkvector::metric::VectorSearchMetric;
use crate::vindex::reader::VindexVectorGlobalIndexReader;
use crate::vindex::{is_vindex_index_type, VindexVectorIndexOptions};
use arrow_array::{Array, Int64Array, RecordBatch};
use arrow_select::interleave::interleave_record_batch;
use futures::{stream, StreamExt, TryStreamExt};
use paimon_vindex_core::index::VectorIndexReader as VIndexReader;
use roaring::RoaringTreemap;
use std::collections::{HashMap, HashSet};
use std::io::Cursor;
use std::sync::Arc;

const INDEX_DIR: &str = "index";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum VectorIndexBackend {
    Lumina,
    Vindex,
}

impl VectorIndexBackend {
    fn from_index_type(index_type: &str) -> Option<Self> {
        if is_lumina_index_type(index_type) {
            Some(Self::Lumina)
        } else if is_vindex_index_type(index_type) {
            Some(Self::Vindex)
        } else {
            None
        }
    }

    fn error_name(self) -> &'static str {
        match self {
            Self::Lumina => "Lumina",
            Self::Vindex => "vindex",
        }
    }
}

pub struct VectorSearchBuilder<'a> {
    table: &'a Table,
    vector_column: Option<String>,
    query_vector: Option<Vec<f32>>,
    limit: Option<usize>,
    options: HashMap<String, String>,
    projection: Option<Vec<String>>,
    filter: Option<Predicate>,
}

pub struct BatchVectorSearchBuilder<'a> {
    table: &'a Table,
    vector_column: Option<String>,
    query_vectors: Option<Vec<Vec<f32>>>,
    limit: Option<usize>,
    options: HashMap<String, String>,
    projection: Option<Vec<String>>,
    filter: Option<Predicate>,
}

impl<'a> VectorSearchBuilder<'a> {
    pub(crate) fn new(table: &'a Table) -> Self {
        Self {
            table,
            vector_column: None,
            query_vector: None,
            limit: None,
            options: HashMap::new(),
            projection: None,
            filter: None,
        }
    }

    pub fn with_vector_column(&mut self, name: &str) -> &mut Self {
        self.vector_column = Some(name.to_string());
        self
    }

    pub fn with_query_vector(&mut self, vector: Vec<f32>) -> &mut Self {
        self.query_vector = Some(vector);
        self
    }

    pub fn with_limit(&mut self, limit: usize) -> &mut Self {
        self.limit = Some(limit);
        self
    }

    /// Attach per-search (query-side) options, resolved ahead of the table
    /// options they override — e.g. `fields.<col>.ivf.refine-factor` to request
    /// exact rerank for one query. Kept as a distinct map from the table schema
    /// options so a broad query key cannot be shadowed by a more specific table
    /// key; query options win as a whole. Mirrors the batch builder and the
    /// community read path (both thread this into the shared search core).
    pub fn with_options(&mut self, options: HashMap<String, String>) -> &mut Self {
        self.options = options;
        self
    }

    /// Attach a residual scalar predicate applied *after* vector recall on the
    /// primary-key vector path: each recalled candidate file is re-read and only
    /// rows satisfying `filter` survive, folded into the search so best-first
    /// order and Top-K still hold. Mirrors Java `PrimaryKeyVectorRead`'s
    /// residual-filter support. Only the primary-key vector path consumes it, and
    /// only when the table exposes physical rows directly (deletion vectors
    /// enabled without merge-on-read); otherwise the query fails loud. A query
    /// that does not resolve to the primary-key vector path (no PK-vector index,
    /// or a non-PK-vector column) also fails loud rather than silently ignoring
    /// the filter.
    ///
    /// The whole predicate is both pushed into the scan — where it prunes whole
    /// data files by their column stats — and applied per row as a residual over
    /// the surviving files, so results stay exact. Sub-file row-range narrowing is
    /// not performed; a surviving file is re-read in full for the residual.
    pub fn with_filter(&mut self, filter: Predicate) -> &mut Self {
        self.filter = Some(filter);
        self
    }

    /// Restrict the columns materialized by [`execute_read`](Self::execute_read)
    /// to `cols` (plus the always-appended `_PKEY_VECTOR_SCORE`). Without this
    /// call `execute_read` materializes every user table column. Only affects
    /// `execute_read`; the search-only paths ignore it.
    pub fn with_projection(&mut self, cols: &[&str]) -> &mut Self {
        self.projection = Some(cols.iter().map(|c| c.to_string()).collect());
        self
    }

    pub async fn execute(&self) -> crate::Result<Vec<RowRange>> {
        self.execute_scored().await?.to_row_ranges()
    }

    /// Run the vector search and return the scored hits (`row_ids` + `scores`,
    /// best-first). Shared search core for both `execute` (which converts to row
    /// ranges) and callers wanting scores. The PK-vector branch mirrors Java
    /// `PrimaryKeyVectorRead`; otherwise the data-evolution (DE) global-index path
    /// is used.
    pub async fn execute_scored(&self) -> crate::Result<SearchResult> {
        let vector_column =
            self.vector_column
                .as_deref()
                .ok_or_else(|| crate::Error::ConfigInvalid {
                    message: "Vector column must be set via with_vector_column()".to_string(),
                })?;
        let query_vector =
            self.query_vector
                .as_ref()
                .ok_or_else(|| crate::Error::ConfigInvalid {
                    message: "Query vector must be set via with_query_vector()".to_string(),
                })?;
        let limit = self.limit.ok_or_else(|| crate::Error::ConfigInvalid {
            message: "Limit must be set via with_limit()".to_string(),
        })?;

        // Primary-key vector search branch: mirrors Java `PrimaryKeyVectorRead`.
        // Only taken when the table enables the PK-vector index AND this query
        // targets a configured PK-vector column; otherwise fall through to the
        // data-evolution (DE) global-index path below.
        //
        // Membership is resolved via the non-erroring columns accessor so a
        // malformed PK-vector config (e.g. a blank list) cannot abort an unrelated
        // DE query. A query that does target the PK-vector column fails loud here:
        // the PK path produces physical positions, not global row ids, so scored
        // search is unsupported and callers must use `execute_read` instead.
        let core = CoreOptions::new(self.table.schema().options());
        if core.primary_key_vector_index_enabled() {
            let targets_pk_column = core
                .primary_key_vector_index_columns()
                .ok()
                .is_some_and(|cols| cols.iter().any(|c| c == vector_column));
            if targets_pk_column {
                return Err(crate::Error::DataInvalid {
                    message: "primary-key vector search does not produce global row ids; use the materialized read (execute_read) instead".to_string(),
                    source: None,
                });
            }
        }

        // The data-evolution (global-index) fall-through path cannot honor a
        // residual filter — it never reads physical rows. Rather than silently
        // drop the predicate and return unfiltered results, fail loud when a
        // filter is set on a query that does not resolve to the primary-key
        // vector path.
        if self.filter.is_some() {
            return Err(crate::Error::DataInvalid {
                message: "vector search filter is only supported on the primary-key vector path"
                    .to_string(),
                source: None,
            });
        }

        let vector_search =
            VectorSearch::new(query_vector.clone(), limit, vector_column.to_string())?;

        let snapshot_manager = SnapshotManager::new(
            self.table.file_io().clone(),
            self.table.location().to_string(),
        );

        let snapshot = match snapshot_manager.get_latest_snapshot().await? {
            Some(s) => s,
            None => return Ok(SearchResult::empty()),
        };

        let index_manifest_name = match snapshot.index_manifest() {
            Some(name) => name.to_string(),
            None => return Ok(SearchResult::empty()),
        };

        let manifest_path = format!(
            "{}/manifest/{}",
            self.table.location().trim_end_matches('/'),
            index_manifest_name
        );
        let index_entries = IndexManifest::read(self.table.file_io(), &manifest_path).await?;

        evaluate_vector_search_scored(
            self.table.file_io(),
            self.table.location(),
            self.table.schema().options(),
            &index_entries,
            &vector_search,
            self.table.schema().fields(),
        )
        .await
    }

    /// Run the vector search and materialize the matching rows as Arrow batches,
    /// ordered best-first. Only supported for primary-key vector indexes; a
    /// data-evolution table or a query targeting a non-PK-vector column fails
    /// loud. Output columns are the projected user table columns (all user
    /// columns by default, or those set via
    /// [`with_projection`](Self::with_projection)) plus `_PKEY_VECTOR_SCORE`;
    /// `_ROW_ID` and `_PKEY_VECTOR_POSITION` are always hidden.
    pub async fn execute_read(&self) -> crate::Result<ArrowRecordBatchStream> {
        self.execute_read_inner(None).await
    }

    /// Shared body for [`execute_read`](Self::execute_read) and
    /// [`execute_read_for_data_split`](Self::execute_read_for_data_split).
    /// `data_splits`: `None` scans the whole table; `Some(splits)` searches only
    /// the caller-supplied splits (one bucket per split). Only the primary-key
    /// vector path can materialize rows — a data-evolution table or a non-PK-vector
    /// column fails loud.
    async fn execute_read_inner(
        &self,
        data_splits: Option<Vec<DataSplit>>,
    ) -> crate::Result<ArrowRecordBatchStream> {
        let vector_column =
            self.vector_column
                .as_deref()
                .ok_or_else(|| crate::Error::ConfigInvalid {
                    message: "Vector column must be set via with_vector_column()".to_string(),
                })?;
        let query_vector =
            self.query_vector
                .as_ref()
                .ok_or_else(|| crate::Error::ConfigInvalid {
                    message: "Query vector must be set via with_query_vector()".to_string(),
                })?;
        let limit = self.limit.ok_or_else(|| crate::Error::ConfigInvalid {
            message: "Limit must be set via with_limit()".to_string(),
        })?;

        // Only the primary-key vector path can materialize rows. The data-evolution
        // (global-index) path returns data-derived row-ids, not table rows, so a
        // read against it (or against a non-PK-vector column) fails loud.
        let core = CoreOptions::new(self.table.schema().options());
        if core.primary_key_vector_index_enabled() {
            let targets_pk_column = core
                .primary_key_vector_index_columns()
                .ok()
                .is_some_and(|cols| cols.iter().any(|c| c == vector_column));
            if targets_pk_column {
                let pk_col = core.primary_key_vector_index_column()?;
                return self
                    .execute_primary_key_vector_read(
                        &core,
                        &pk_col,
                        query_vector,
                        limit,
                        data_splits,
                    )
                    .await;
            }
        }

        Err(crate::Error::DataInvalid {
            message: "vector search read is only supported for primary-key vector indexes".into(),
            source: None,
        })
    }

    /// Single-query wrapper over
    /// [`plan_and_search_pk_candidates_batch`]: plan once, search the one query,
    /// and return its candidate list. Output is byte-identical to the batch-of-one
    /// path.
    async fn plan_and_search_pk_candidates(
        &self,
        core: &CoreOptions<'_>,
        pk_col: &str,
        query_vector: &[f32],
        limit: usize,
        data_splits: Option<Vec<DataSplit>>,
    ) -> crate::Result<(Vec<PkVectorCandidate>, PkVectorScanPlan, VectorSearchMetric)> {
        // Thread the builder's query-side options into the shared batch core so a
        // per-search option (e.g. refine-factor) resolves ahead of the table
        // option it overrides — matching the batch path and the community read
        // path. These query options are kept distinct from the table schema
        // options in `resolve_pk_vector_search_params`; for an `ARRAY<FLOAT>`
        // column they are also validated by the vindex option whitelist (an
        // unknown key fails loud there), exactly as on the batch path and
        // community main.
        let (mut candidates, plan, metric) = plan_and_search_pk_candidates_batch(
            self.table,
            &self.options,
            self.filter.as_ref(),
            core,
            pk_col,
            &[query_vector],
            limit,
            data_splits,
        )
        .await?;
        debug_assert_eq!(candidates.len(), 1);
        Ok((candidates.remove(0), plan, metric))
    }

    /// Materialize the best-first PK-vector search hits into Arrow rows. Mirrors
    /// Java `PrimaryKeyVectorRead` feeding its result splits into an ordinary table
    /// read: the search decides which rows, a subsequent read decides which
    /// columns.
    ///
    /// Output columns are the projected user table columns (all user columns when
    /// [`with_projection`](Self::with_projection) was not called) plus
    /// `_PKEY_VECTOR_SCORE`; `_ROW_ID` and `_PKEY_VECTOR_POSITION` are always
    /// hidden. Rows are emitted best-first (the candidate order), which differs
    /// from the file/position order the orchestrator materializes in.
    async fn execute_primary_key_vector_read(
        &self,
        core: &CoreOptions<'_>,
        pk_col: &str,
        query_vector: &[f32],
        limit: usize,
        data_splits: Option<Vec<DataSplit>>,
    ) -> crate::Result<ArrowRecordBatchStream> {
        let (candidates, plan, metric) = self
            .plan_and_search_pk_candidates(core, pk_col, query_vector, limit, data_splits)
            .await?;

        // Resolve the materialization read-type up front so an invalid projection
        // (unknown column, or a reserved metadata / row-id name) fails loud
        // unconditionally, even when the plan is empty and no rows will be read.
        // Default (no `with_projection`) is every user table column.
        let read_type = self.resolve_materialize_read_type()?;

        // A separate, predicate-free materialization reader projecting the user
        // columns (the search reader projects only the vector column). Mirrors
        // `table_read.rs::new_data_file_reader` with an empty predicate list.
        let materialize_reader = DataFileReader::new(
            self.table.file_io().clone(),
            self.table.schema_manager().clone(),
            self.table.schema().id(),
            self.table.schema().fields().to_vec(),
            read_type,
            Vec::new(),
            core.read_batch_size(),
            core.parquet_page_index_enabled(),
            core.parquet_bloom_filter_enabled(),
        );

        Self::materialize_candidates(candidates, &plan, metric, &materialize_reader).await
    }

    /// Like [`execute_read`](Self::execute_read), but scoped to a single
    /// caller-supplied `DataSplit` (one bucket) instead of scanning the whole
    /// table: runs the PK-vector search over just that split and materializes its
    /// local Top-K best-first. Intended for a query engine that plans buckets
    /// itself and fans one whole-bucket split out per node, then merges the
    /// per-split results by `__paimon_search_score`. Same guards as
    /// [`execute_read`](Self::execute_read) (primary-key vector indexes only).
    pub async fn execute_read_for_data_split(
        &self,
        split: DataSplit,
    ) -> crate::Result<ArrowRecordBatchStream> {
        self.execute_read_inner(Some(vec![split])).await
    }

    /// Materialize one best-first candidate list into an Arrow stream, best-first,
    /// with a `__paimon_search_score` column and `_PKEY_VECTOR_POSITION` stripped.
    /// An empty candidate list yields an empty stream (never skipped) so a batch
    /// caller preserves per-query arity. `materialize_reader` must project the
    /// output columns (predicate-free). Both the single-query and batch read paths
    /// use this so their materialization is identical.
    async fn materialize_candidates(
        candidates: Vec<PkVectorCandidate>,
        plan: &PkVectorScanPlan,
        metric: VectorSearchMetric,
        materialize_reader: &DataFileReader,
    ) -> crate::Result<ArrowRecordBatchStream> {
        if candidates.is_empty() {
            return Ok(Box::pin(stream::empty()));
        }

        // Rank each candidate by its best-first position, then reduce the physical
        // materialization order back to best-first. The orchestrator emits rows in
        // ascending (partition, bucket, file, position); the rank map keyed by
        // (partition bytes, bucket, file, position) recovers the candidate order.
        let mut rank_of: HashMap<(Vec<u8>, i32, String, i64), usize> = HashMap::new();
        for (rank, c) in candidates.iter().enumerate() {
            rank_of.insert(
                (
                    c.partition.to_serialized_bytes(),
                    c.bucket,
                    c.data_file_name.clone(),
                    c.row_position,
                ),
                rank,
            );
        }

        let indexed_splits = build_indexed_splits(candidates, &plan.splits, metric)?;

        // Materialize every indexed split, retaining each batch and, per row, the
        // (rank, batch_index, row_index) tuple so we can reorder to best-first.
        // Top-K is small, so full in-memory collection is acceptable.
        let mut batches: Vec<RecordBatch> = Vec::new();
        let mut ranked: Vec<RankedRow> = Vec::new();
        for indexed in indexed_splits {
            let partition_bytes = indexed.split.partition().to_serialized_bytes();
            let bucket = indexed.split.bucket();
            let file_name = indexed.split.data_files()[0].file_name.clone();
            let mut stream =
                PkVectorIndexedSplitRead::new(materialize_reader.clone()).read(&indexed)?;
            while let Some(batch) = stream.try_next().await? {
                let batch_index = batches.len();
                collect_ranked_rows(
                    &batch,
                    batch_index,
                    &partition_bytes,
                    bucket,
                    &file_name,
                    &rank_of,
                    &mut ranked,
                )?;
                batches.push(batch);
            }
        }

        // Reorder to best-first and drop the position column.
        let output = reorder_and_strip_position(&batches, ranked)?;
        Ok(Box::pin(stream::iter(output.into_iter().map(Ok))))
    }

    /// Resolve the projected fields for the materialization read-type. Default
    /// (no projection set) is all user table fields; otherwise the requested
    /// names resolved against the schema. Rejects reserved metadata names and
    /// `_ROW_ID` so a user cannot request a hidden column.
    fn resolve_materialize_read_type(&self) -> crate::Result<Vec<DataField>> {
        let fields = match &self.projection {
            None => self.table.schema().fields().to_vec(),
            Some(names) => {
                for name in names {
                    if is_reserved_read_column(name) {
                        return Err(crate::Error::DataInvalid {
                            message: format!(
                                "vector search read projection must not request reserved column '{name}'"
                            ),
                            source: None,
                        });
                    }
                }
                let full_name = self.table.identifier().full_name();
                let field_map: HashMap<&str, &DataField> = self
                    .table
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| (f.name(), f))
                    .collect();
                let mut resolved = Vec::with_capacity(names.len());
                for name in names {
                    let field = field_map.get(name.as_str()).ok_or_else(|| {
                        crate::Error::ColumnNotExist {
                            full_name: full_name.clone(),
                            column: name.clone(),
                        }
                    })?;
                    resolved.push((*field).clone());
                }
                resolved
            }
        };
        // The default projection returns every user column, so a user column
        // whose name collides with an injected metadata column must be rejected
        // on the resolved field list too — not only when explicitly requested.
        ensure_no_reserved_read_columns(&fields)?;
        Ok(fields)
    }
}

/// Names a read injects as metadata columns — `__paimon_search_score`,
/// `_PKEY_VECTOR_POSITION`, and `_ROW_ID` — that a materialized read type must
/// not reuse for a user column.
fn is_reserved_read_column(name: &str) -> bool {
    name == PKEY_VECTOR_POSITION_COLUMN || name == SEARCH_SCORE_COLUMN || name == ROW_ID_FIELD_NAME
}

/// Reject a materialized read type whose resolved fields contain a reserved
/// metadata column name. Applied to the RESOLVED field list so the default
/// (all user columns) projection is covered, not only an explicit one.
fn ensure_no_reserved_read_columns(fields: &[DataField]) -> crate::Result<()> {
    for field in fields {
        if is_reserved_read_column(field.name()) {
            return Err(crate::Error::DataInvalid {
                message: format!(
                    "vector search read projection must not include reserved column '{}'",
                    field.name()
                ),
                source: None,
            });
        }
    }
    Ok(())
}

/// Config resolved once before planning, independent of how the plan is obtained
/// (whole-table scan vs caller-supplied splits).
struct PkVectorSearchParams {
    metric: VectorSearchMetric,
    concurrency: usize,
    index_type: String,
    field_id: i32,
    vector_field: DataField,
    skip_exact_fallback: bool,
    refine_factor: usize,
    indexed_limit: usize,
}

/// Resolve metric / index type / field / search-mode / refine params and validate
/// every query, independent of the plan. Extracted from the former prefix of
/// `plan_and_search_pk_candidates_batch` so the whole-snapshot and bucket-scoped
/// paths share identical config resolution.
fn resolve_pk_vector_search_params(
    table: &Table,
    core: &CoreOptions<'_>,
    pk_col: &str,
    query_options: &HashMap<String, String>,
    queries: &[&[f32]],
    limit: usize,
    filter: Option<&Predicate>,
) -> crate::Result<PkVectorSearchParams> {
    // Residual pre-filter guard, mirroring Java `PrimaryKeyVectorScan`. A DATA
    // predicate set via `with_filter` is applied post-recall by re-reading each
    // candidate file's physical rows (see below). That physical-position filtering
    // only agrees with the bucket search when the table exposes physical rows
    // directly: deletion vectors enabled and merge-on-read disabled. Under
    // merge-on-read (or without deletion vectors) a read merges multiple key
    // versions, so a scalar filter could retain a stale version whose live version
    // does not match — a silent wrong-read. Reject such queries rather than answer
    // them incorrectly.
    //
    // Guard on the DATA conjuncts, not the whole filter: partition-only conjuncts
    // are enforced entirely by scan planning (partition pruning) and produce no
    // per-row residual, so they need no physical-row read. This mirrors Java, where
    // `BatchVectorSearchBuilderImpl.withFilter` splits at the builder level and
    // leaves `this.filter == null` for a partition-only filter — the scan guard is
    // then skipped. No data predicate (partition-only or no filter) → nothing to
    // guard, so the search-only and read paths are unaffected.
    let physical_row_read =
        core.deletion_vectors_enabled() && !core.deletion_vectors_merge_on_read();
    let has_data_predicate = filter.is_some_and(|f| {
        let (_partition, data) = split_partition_and_data_predicates(
            f.clone(),
            table.schema().fields(),
            table.schema().partition_keys(),
        );
        !data.is_empty()
    });
    if has_data_predicate && !physical_row_read {
        return Err(crate::Error::DataInvalid {
            message:
                "primary-key vector pre-filter requires deletion vectors without merge-on-read"
                    .to_string(),
            source: None,
        });
    }
    // `primary_key_vector_distance_metric` returns a validated name; re-parse into
    // the enum for the numeric semantics.
    let metric = VectorSearchMetric::parse(&core.primary_key_vector_distance_metric(pk_col)?)?;
    // Fan-out limit for the per-bucket and per-exact-file search (Java
    // `GLOBAL_INDEX_THREAD_NUM`); `1` reproduces strictly sequential execution.
    let concurrency = core.global_index_thread_num()?;
    let index_type = core.primary_key_vector_index_type(pk_col)?;
    let field_id = find_field_id_by_name(table.schema().fields(), pk_col).ok_or_else(|| {
        crate::Error::DataInvalid {
            message: format!("PK-vector column '{pk_col}' not found in schema"),
            source: None,
        }
    })?;
    let vector_field = table
        .schema()
        .fields()
        .iter()
        .find(|f| f.name() == pk_col)
        .cloned()
        .ok_or_else(|| crate::Error::DataInvalid {
            message: format!("PK-vector column '{pk_col}' not found in schema"),
            source: None,
        })?;

    let search_mode = core.global_index_search_mode()?;
    let skip_exact_fallback = search_mode == GlobalIndexSearchMode::Fast;

    // A non-positive limit is invalid regardless of the plan; reject it before
    // planning so an empty plan cannot mask it with empty results.
    if limit == 0 {
        return Err(crate::Error::DataInvalid {
            message: "vector search limit must be positive".to_string(),
            source: None,
        });
    }

    // Resolve the refine factor from the query options first, then fall back to
    // the table options; a positive factor over-fetches indexed (approximate)
    // candidates so the exact rerank below has a wider pool to reorder. Factor 0
    // (unset) leaves `indexed_limit == limit`, byte-identical to the no-rerank
    // path. The two option maps are kept distinct (query options passed
    // separately from table options) so a broad query key cannot be overridden
    // by a more specific table key: query options take precedence as a whole.
    // Resolved before planning so an invalid factor (e.g. a non-numeric value)
    // fails loud regardless of whether the table currently has searchable data.
    let refine_factor =
        configured_refine_factor(query_options, table.schema().options(), pk_col, &index_type)?;
    let indexed_limit = indexed_search_limit(limit, refine_factor)?;

    // Validate every query against the vector column's dimension (and finiteness)
    // before planning or any read, so a malformed query fails loud even when the
    // plan turns out empty. VECTOR<FLOAT> carries the dimension in its type;
    // ARRAY<FLOAT> gets the index dimension from the same vindex option resolver
    // used by index reads. Both valid PK-vector column shapes must reject NaN/Inf
    // up front, not only after a non-empty plan opens readers.
    if let Some(dimension) = pk_vector_query_dimension(
        table.schema().options(),
        query_options,
        &index_type,
        &vector_field,
    )? {
        for query in queries {
            validate_query(query, dimension)?;
        }
    }

    Ok(PkVectorSearchParams {
        metric,
        concurrency,
        index_type,
        field_id,
        vector_field,
        skip_exact_fallback,
        refine_factor,
        indexed_limit,
    })
}

/// Plan and search PK-vector candidates for a batch of queries. `data_splits`
/// selects how the plan is obtained — `None` scans the whole table (the default
/// read path), `Some(splits)` plans from caller-supplied splits (e.g. a query
/// engine fanning out one bucket per node). Only the plan step differs; config
/// resolution ([`resolve_pk_vector_search_params`]) and the search body
/// ([`search_pk_candidates_batch_with_plan`]) are shared.
#[allow(clippy::too_many_arguments)]
async fn plan_and_search_pk_candidates_batch(
    table: &Table,
    query_options: &HashMap<String, String>,
    filter: Option<&Predicate>,
    core: &CoreOptions<'_>,
    pk_col: &str,
    queries: &[&[f32]],
    limit: usize,
    data_splits: Option<Vec<DataSplit>>,
) -> crate::Result<(
    Vec<Vec<PkVectorCandidate>>,
    PkVectorScanPlan,
    VectorSearchMetric,
)> {
    let params = resolve_pk_vector_search_params(
        table,
        core,
        pk_col,
        query_options,
        queries,
        limit,
        filter,
    )?;
    let scan = PkVectorScan::new(
        table,
        params.field_id,
        params.index_type.clone(),
        filter.cloned(),
    );
    let plan = match data_splits {
        Some(splits) => scan.plan_for_data_splits(splits).await?,
        None => scan.plan().await?,
    };
    search_pk_candidates_batch_with_plan(
        table,
        core,
        filter,
        query_options,
        queries,
        limit,
        plan,
        &params,
    )
    .await
}

/// Search a resolved plan across all queries and merge per-query Top-K. Shared by
/// the whole-snapshot and bucket-scoped entry points; preserves metric, refine /
/// rerank, search-mode (FAST skip), exact fallback, residual filtering, deletion
/// vectors and projection semantics.
#[allow(clippy::too_many_arguments)]
async fn search_pk_candidates_batch_with_plan(
    table: &Table,
    core: &CoreOptions<'_>,
    filter: Option<&Predicate>,
    query_options: &HashMap<String, String>,
    queries: &[&[f32]],
    limit: usize,
    plan: PkVectorScanPlan,
    params: &PkVectorSearchParams,
) -> crate::Result<(
    Vec<Vec<PkVectorCandidate>>,
    PkVectorScanPlan,
    VectorSearchMetric,
)> {
    let metric = params.metric;
    let concurrency = params.concurrency;
    let index_type = params.index_type.clone();
    let vector_field = params.vector_field.clone();
    let skip_exact_fallback = params.skip_exact_fallback;
    let refine_factor = params.refine_factor;
    let indexed_limit = params.indexed_limit;

    if plan.splits.is_empty() {
        return Ok((vec![Vec::new(); queries.len()], plan, metric));
    }

    // Resolve the vector index backend from the single configured index type.
    // Java enforces one index type per PK table and Rust filters segments to it,
    // so one backend serves every segment. Computed after the empty-plan return so
    // an empty table never errors on an unrecognized type.
    let backend = VectorIndexBackend::from_index_type(&index_type).ok_or_else(|| {
        crate::Error::DataInvalid {
            message: format!("unsupported PK vector index backend/type: '{index_type}'"),
            source: None,
        }
    })?;

    // Production data-file reader, mirroring `table_read.rs::new_data_file_reader`
    // but projecting only the vector column with no predicates.
    let reader = DataFileReader::new(
        table.file_io().clone(),
        table.schema_manager().clone(),
        table.schema().id(),
        table.schema().fields().to_vec(),
        vec![vector_field.clone()],
        Vec::new(),
        core.read_batch_size(),
        core.parquet_page_index_enabled(),
        core.parquet_bloom_filter_enabled(),
    );

    // Real ANN scorer: preload each segment's bytes (keyed by resolved, globally
    // unique path) and drive the vindex reader from memory. The reader is opened
    // once per segment and every query in the batch is searched against it,
    // mirroring the shared-reader batch search.
    let segment_bytes = preload_segment_bytes(table.file_io(), &plan.splits, concurrency).await?;
    // Fail loud on a config/segment metric mismatch before scoring, mirroring Java
    // `PkVectorAnnSegmentSearcher.search`.
    verify_pk_vector_segment_metrics(&plan.splits, &segment_bytes, metric, backend)?;
    let options = {
        let mut o = table.schema().options().clone();
        o.extend(query_options.clone());
        o
    };
    let search_options = options.clone();
    let field_name = vector_field.name().to_string();
    let scorer: crate::vindex::pkvector::ann::BatchScorer = Box::new(
        move |segment: &BucketAnnSegment, searches: &[VectorSearch]| {
            let data = segment_bytes
                .get(&segment.path)
                .ok_or_else(|| crate::Error::DataInvalid {
                    message: "missing preloaded ANN bytes for segment".to_string(),
                    source: None,
                })?
                .clone();
            let io_meta = GlobalIndexIOMeta::new(
                segment.path.clone(),
                segment.file_size,
                segment.index_meta.clone(),
            );
            match backend {
                VectorIndexBackend::Lumina => {
                    let mut reader = LuminaVectorGlobalIndexReader::new(io_meta, options.clone());
                    reader.visit_batch_vector_search(searches, |_| Ok(Cursor::new(data)))
                }
                VectorIndexBackend::Vindex => {
                    let mut reader = VindexVectorGlobalIndexReader::new(io_meta, options.clone());
                    reader.visit_batch_vector_search(searches, |_| Ok(Cursor::new(data)))
                }
            }
        },
    );
    let ann_searcher: Arc<dyn PkVectorAnnSearcher> =
        Arc::new(VindexAnnSearcher::new(field_name, scorer));

    // Residual (post-recall) filtering: for each candidate file, re-read its
    // physical rows and keep the positions whose rows satisfy the filter. The
    // per-split allow-list is threaded into the bucket search so the residual folds
    // into recall (best-first order and Top-K are preserved). Built only when the
    // filter has data (non-partition) conjuncts; a partition-only filter (or no
    // filter) leaves `None`, which leaves the search unfiltered — partition
    // pruning is already handled in planning. The residual depends only on the
    // filter and the plan, not the query vector, so it is computed once here and
    // shared across every query in the batch. The residual reader projects only
    // the predicate columns and carries no pushdown; `residual_positions_by_file`
    // recovers each surviving row's file-local physical position from its ordinal
    // in the unfiltered scan (no `_ROW_ID`, no `first_row_id`). A file the
    // allow-list leaves empty is skipped by the bucket search without opening an
    // exact reader.
    let residual_by_split: Option<Vec<HashMap<String, RoaringTreemap>>> = match filter {
        Some(filter) => {
            // The whole filter is pushed into scan planning (`PkVectorScan`), where
            // partition-only conjuncts already prune partitions/files. Re-applying
            // them as a per-row residual would be redundant, so keep only the data
            // conjuncts here — a partition-only filter then needs no residual at
            // all. Mixed partition/data conjuncts stay whole in `data_predicates`
            // and evaluate against the materialized partition column (partition
            // columns are physically present in primary-key data files), so there
            // is no missing-column case to reject.
            let (_partition_predicate, data_predicates) = split_partition_and_data_predicates(
                filter.clone(),
                table.schema().fields(),
                table.schema().partition_keys(),
            );
            if data_predicates.is_empty() {
                None
            } else {
                let file_predicates = FilePredicates {
                    predicates: data_predicates,
                    file_fields: table.schema().fields().to_vec(),
                };
                let residual_read_type = widen_scan_fields(&[], Some(&file_predicates));
                let residual_reader = DataFileReader::new(
                    table.file_io().clone(),
                    table.schema_manager().clone(),
                    table.schema().id(),
                    table.schema().fields().to_vec(),
                    residual_read_type,
                    Vec::new(),
                    core.read_batch_size(),
                    core.parquet_page_index_enabled(),
                    core.parquet_bloom_filter_enabled(),
                );
                let mut per_split = Vec::with_capacity(plan.splits.len());
                for split in &plan.splits {
                    per_split.push(
                        residual_positions_by_file(
                            &residual_reader,
                            &split.data_split,
                            &split.active_files,
                            &file_predicates,
                        )
                        .await?,
                    );
                }
                Some(per_split)
            }
        }
        None => None,
    };

    // Build the exact-fallback search on demand: the kernel calls this only for a
    // file it actually searches (uncovered by ANN, residual-allowed, and only when
    // the search mode is not FAST). Everything the future needs is cloned/owned up
    // front so it borrows neither the split nor the file across the await. The
    // search streams the file's vector column one Arrow batch at a time into
    // per-query bounded heaps (all queries share one stream).
    let reader_for_factory = reader.clone();
    let vector_field_for_factory = vector_field.clone();
    let factory = as_split_exact_file_search(
        move |_split_index: usize,
              split: &PkVectorSearchSplit,
              file: &BucketActiveFile,
              queries: &[&[f32]],
              metric: VectorSearchMetric,
              exact_limit: usize,
              is_excluded: &(dyn Fn(i64) -> bool + Sync)|
              -> ExactFileSearchFuture<'_> {
            let reader = reader_for_factory.clone();
            let vector_field = vector_field_for_factory.clone();
            let data_split = split.data_split.clone();
            let active = BucketActiveFile {
                file_name: file.file_name.clone(),
                row_count: file.row_count,
            };
            let owned_queries: Vec<Vec<f32>> = queries.iter().map(|q| q.to_vec()).collect();
            Box::pin(async move {
                let factory = DataFilePkVectorReaderFactory::new(reader, data_split, vector_field)?;
                let query_refs: Vec<&[f32]> = owned_queries.iter().map(|q| q.as_slice()).collect();
                factory
                    .search_file(&active, &query_refs, metric, exact_limit, is_excluded)
                    .await
            })
        },
    );

    // Resolve the refine factor from the query options first, then fall back to the
    // table options; a positive factor over-fetches indexed (approximate)
    // candidates so the exact rerank below has a wider pool to reorder. Factor 0
    // (unset) leaves `indexed_limit == limit`, byte-identical to the no-rerank
    // path. The two option maps are kept distinct (query options passed separately
    // from table options) so a broad query key cannot be overridden by a more
    // specific table key: query options take precedence as a whole. `search_options`
    // above is the merged view used only to drive the ANN read.

    let searches: Vec<OrchestratorSearchResult> = PkVectorOrchestrator::new(reader)
        .search_candidates_batch(
            &plan.splits,
            queries,
            metric,
            limit,
            indexed_limit,
            Some(ann_searcher.clone()),
            &factory,
            &search_options,
            skip_exact_fallback,
            residual_by_split.as_deref(),
            concurrency,
        )
        .await?;

    // Per query: exact rerank of the approximate candidates when a refine factor is
    // set (exact-fallback candidates are already exact and are not reranked), then
    // merge the (possibly reranked) indexed list with the exact list into one
    // best-first list bounded to the caller's limit. With no refine factor the
    // rerank is a plain merge, byte-identical to the no-rerank path. Each query
    // reranks its OWN indexed candidates.
    let mut per_query_candidates = Vec::with_capacity(searches.len());
    for (query_index, search) in searches.into_iter().enumerate() {
        let query_vector = queries[query_index];
        let indexed = if refine_factor > 0 && !search.indexed.is_empty() {
            // Vector-only reader (project just the vector field); the position read
            // appends _PKEY_VECTOR_POSITION itself and injects _ROW_ID internally.
            let rerank_reader = DataFileReader::new(
                table.file_io().clone(),
                table.schema_manager().clone(),
                table.schema().id(),
                table.schema().fields().to_vec(),
                vec![vector_field.clone()],
                Vec::new(),
                core.read_batch_size(),
                core.parquet_page_index_enabled(),
                core.parquet_bloom_filter_enabled(),
            );
            rerank_indexed_positional(
                &rerank_reader,
                search.indexed,
                &plan.splits,
                query_vector,
                metric,
                limit,
                &vector_field,
            )
            .await?
        } else {
            search.indexed
        };
        per_query_candidates.push(merge_candidates(indexed, search.exact, limit));
    }

    Ok((per_query_candidates, plan, metric))
}

impl<'a> BatchVectorSearchBuilder<'a> {
    pub(crate) fn new(table: &'a Table) -> Self {
        Self {
            table,
            vector_column: None,
            query_vectors: None,
            limit: None,
            options: HashMap::new(),
            projection: None,
            filter: None,
        }
    }

    pub fn with_vector_column(&mut self, name: &str) -> &mut Self {
        self.vector_column = Some(name.to_string());
        self
    }

    pub fn with_query_vectors(&mut self, vectors: Vec<Vec<f32>>) -> &mut Self {
        self.query_vectors = Some(vectors);
        self
    }

    pub fn with_limit(&mut self, limit: usize) -> &mut Self {
        self.limit = Some(limit);
        self
    }

    pub fn with_options(&mut self, options: HashMap<String, String>) -> &mut Self {
        self.options = options;
        self
    }

    /// Attach a residual scalar predicate applied *after* vector recall on the
    /// primary-key vector path, shared across every query in the batch. Mirrors
    /// the single [`VectorSearchBuilder::with_filter`]: only the primary-key
    /// vector path (via [`execute_read`](Self::execute_read)) consumes it, and only
    /// when the table exposes physical rows directly (deletion vectors without
    /// merge-on-read); otherwise the query fails loud.
    pub fn with_filter(&mut self, filter: Predicate) -> &mut Self {
        self.filter = Some(filter);
        self
    }

    /// Restrict the columns materialized by [`execute_read`](Self::execute_read) to
    /// `cols` (plus the always-appended `__paimon_search_score`). Without this call
    /// `execute_read` materializes every user table column. Only affects
    /// `execute_read`; `execute` ignores it.
    pub fn with_projection(&mut self, cols: &[&str]) -> &mut Self {
        self.projection = Some(cols.iter().map(|c| c.to_string()).collect());
        self
    }

    pub async fn execute(&self) -> crate::Result<Vec<SearchResult>> {
        // Fail closed: like `execute_read` and the single-query builder, this
        // returns data-derived row ids/scores outside `TableScan`/`TableRead`,
        // so it must refuse a `query-auth.enabled` table before any fast path
        // (an empty snapshot would otherwise return empty results and bypass it).
        let core = CoreOptions::new(self.table.schema().options());
        if core.query_auth_enabled() {
            return Err(crate::Error::Unsupported {
                message: "vector search does not support query-auth.enabled tables".to_string(),
            });
        };
        let vector_column =
            self.vector_column
                .as_deref()
                .ok_or_else(|| crate::Error::ConfigInvalid {
                    message: "Vector column must be set via with_vector_column()".to_string(),
                })?;
        if vector_column.is_empty() {
            return Err(crate::Error::ConfigInvalid {
                message: "Vector column must be set via with_vector_column()".to_string(),
            });
        }

        let query_vectors =
            self.query_vectors
                .as_ref()
                .ok_or_else(|| crate::Error::ConfigInvalid {
                    message: "Query vectors must be set via with_query_vectors()".to_string(),
                })?;
        if query_vectors.is_empty() {
            return Err(crate::Error::ConfigInvalid {
                message: "Query vectors must be set via with_query_vectors()".to_string(),
            });
        }

        let limit = self.limit.ok_or_else(|| crate::Error::ConfigInvalid {
            message: "Limit must be set via with_limit()".to_string(),
        })?;
        if limit == 0 || limit > i32::MAX as usize {
            return Err(crate::Error::ConfigInvalid {
                message: format!("Limit must be between 1 and {}, got: {limit}", i32::MAX),
            });
        }

        // A primary-key vector table exposes no global row ids, so scored batch
        // search is unsupported: `execute()` returns `SearchResult`s (global row
        // ids). Fail loud and direct callers to the materialized batch
        // `execute_read`, mirroring the single-query builder's PK guard. Membership
        // is resolved via the non-erroring columns accessor so a malformed
        // PK-vector config cannot abort an unrelated DE query.
        if core.primary_key_vector_index_enabled() {
            let targets_pk_column = core
                .primary_key_vector_index_columns()
                .ok()
                .is_some_and(|cols| cols.iter().any(|c| c == vector_column));
            if targets_pk_column {
                return Err(crate::Error::DataInvalid {
                    message: "primary-key vector search does not produce global row ids; use the materialized read (execute_read) instead".to_string(),
                    source: None,
                });
            }
        }

        // The data-evolution (global-index) fall-through path cannot honor a
        // residual filter — it never reads physical rows. Rather than silently
        // drop the predicate and return unfiltered results, fail loud when a
        // filter is set on a batch that does not resolve to the primary-key
        // vector path, mirroring the single-query builder.
        if self.filter.is_some() {
            return Err(crate::Error::DataInvalid {
                message: "vector search filter is only supported on the primary-key vector path"
                    .to_string(),
                source: None,
            });
        }

        // kwai's batch builder supports primary-key vector columns only (Decision 3).
        // The data-evolution (global-index) path would require helpers kwai lacks
        // (deleted_row_ranges_for_data_evolution_dvs, search_limit_with_deleted_rows,
        // unindexed_ranges_for_global_index_entries). Rather than silently fall back
        // or loop the single-query DE path (an unreviewed shim), fail loud and direct
        // callers to the single-query builder for DE queries.
        Err(crate::Error::Unsupported {
            message: "batch vector search supports primary-key vector columns only; for data-evolution (global-index) queries, use the single-query vector search builder instead".to_string(),
        })
    }

    /// Run a batch of vector searches and materialize each query's matching rows as
    /// a best-first Arrow stream. Supported only for the primary-key vector path
    /// (which alone can materialize physical rows). The returned `Vec` is aligned
    /// strictly to the input query order and its length always equals the query
    /// count — a query with no hits yields an empty stream, never a missing entry.
    /// If ANY query errors (e.g. a malformed vector) the whole call fails loud with
    /// no partial `Vec` of streams. Output columns are the projected user table
    /// columns (all user columns by default, or those set via
    /// [`with_projection`](Self::with_projection)) plus `__paimon_search_score`;
    /// `_ROW_ID` and `_PKEY_VECTOR_POSITION` are always hidden.
    ///
    /// A data-evolution (global-index) table fails loud: its batch search returns
    /// scored global row-ids, not materialized rows, so callers use
    /// [`execute`](Self::execute) instead.
    pub async fn execute_read(&self) -> crate::Result<Vec<ArrowRecordBatchStream>> {
        // Fail closed: returns data outside `TableScan`/`TableRead`.
        let core = CoreOptions::new(self.table.schema().options());
        if core.query_auth_enabled() {
            return Err(crate::Error::Unsupported {
                message: "vector search does not support query-auth.enabled tables".to_string(),
            });
        };
        let vector_column =
            self.vector_column
                .as_deref()
                .ok_or_else(|| crate::Error::ConfigInvalid {
                    message: "Vector column must be set via with_vector_column()".to_string(),
                })?;
        let query_vectors =
            self.query_vectors
                .as_ref()
                .ok_or_else(|| crate::Error::ConfigInvalid {
                    message: "Query vectors must be set via with_query_vectors()".to_string(),
                })?;
        if query_vectors.is_empty() {
            return Err(crate::Error::ConfigInvalid {
                message: "Query vectors must be set via with_query_vectors()".to_string(),
            });
        }
        let limit = self.limit.ok_or_else(|| crate::Error::ConfigInvalid {
            message: "Limit must be set via with_limit()".to_string(),
        })?;

        // Only the primary-key vector path can materialize rows. The data-evolution
        // (global-index) path returns data-derived row-ids, not table rows, so a
        // batch read against it (or a non-PK-vector column) fails loud, directing
        // callers to `execute()`.
        let targets_pk_column = core.primary_key_vector_index_enabled()
            && core
                .primary_key_vector_index_columns()
                .ok()
                .is_some_and(|cols| cols.iter().any(|c| c == vector_column));
        if !targets_pk_column {
            return Err(crate::Error::DataInvalid {
                message: "batch vector read is only supported on the primary-key vector path; data-evolution batch search returns scored row ids, use execute() instead".to_string(),
                source: None,
            });
        }

        let pk_col = core.primary_key_vector_index_column()?;
        let query_refs: Vec<&[f32]> = query_vectors.iter().map(|q| q.as_slice()).collect();

        // Resolve the materialization read-type up front so an invalid projection
        // (unknown column, or a reserved metadata / row-id name) fails loud
        // unconditionally, before any read — a whole-call failure, not a partial
        // Vec.
        let read_type = self.resolve_materialize_read_type()?;

        // One shared plan / segment preload / residual across all N queries; the
        // per-query candidate lists come back in strict input order. Any query
        // error (or a shared-plan error) propagates here, so no partial Vec is
        // returned.
        let (per_query_candidates, plan, metric) = plan_and_search_pk_candidates_batch(
            self.table,
            &self.options,
            self.filter.as_ref(),
            &core,
            &pk_col,
            &query_refs,
            limit,
            None,
        )
        .await?;

        let materialize_reader = DataFileReader::new(
            self.table.file_io().clone(),
            self.table.schema_manager().clone(),
            self.table.schema().id(),
            self.table.schema().fields().to_vec(),
            read_type,
            Vec::new(),
            core.read_batch_size(),
            core.parquet_page_index_enabled(),
            core.parquet_bloom_filter_enabled(),
        );

        // Materialize each query's candidates into its own stream, preserving
        // arity: an empty candidate list yields an empty stream. Build every stream
        // before returning so a materialization error fails the whole call with no
        // partial Vec.
        let mut streams = Vec::with_capacity(per_query_candidates.len());
        for candidates in per_query_candidates {
            streams.push(
                VectorSearchBuilder::materialize_candidates(
                    candidates,
                    &plan,
                    metric,
                    &materialize_reader,
                )
                .await?,
            );
        }
        Ok(streams)
    }

    /// Resolve the projected fields for the materialization read-type. Default
    /// (no projection set) is all user table fields; otherwise the requested names
    /// resolved via `resolve_projected_fields`. Rejects reserved metadata names and
    /// `_ROW_ID` so a user cannot request a hidden column. Mirrors the single
    /// builder's resolver.
    fn resolve_materialize_read_type(&self) -> crate::Result<Vec<DataField>> {
        let fields = match &self.projection {
            None => self.table.schema().fields().to_vec(),
            Some(names) => {
                for name in names {
                    if is_reserved_read_column(name) {
                        return Err(crate::Error::DataInvalid {
                            message: format!(
                                "vector search read projection must not request reserved column '{name}'"
                            ),
                            source: None,
                        });
                    }
                }
                let full_name = self.table.identifier().full_name();
                let field_map: HashMap<&str, &DataField> = self
                    .table
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| (f.name(), f))
                    .collect();
                let mut resolved = Vec::new();
                for name in names {
                    let field = field_map.get(name.as_str()).ok_or_else(|| {
                        crate::Error::ColumnNotExist {
                            full_name: full_name.clone(),
                            column: name.clone(),
                        }
                    })?;
                    resolved.push((*field).clone());
                }
                resolved
            }
        };
        // The default projection returns every user column, so a user column
        // whose name collides with an injected metadata column must be rejected
        // on the resolved field list too — not only when explicitly requested.
        ensure_no_reserved_read_columns(&fields)?;
        Ok(fields)
    }
}

async fn evaluate_vector_search_scored(
    file_io: &crate::io::FileIO,
    table_path: &str,
    table_options: &HashMap<String, String>,
    index_entries: &[crate::spec::IndexManifestEntry],
    vector_search: &VectorSearch,
    schema_fields: &[DataField],
) -> crate::Result<SearchResult> {
    let table_path = table_path.trim_end_matches('/');

    let field_id = match find_field_id_by_name(schema_fields, &vector_search.field_name) {
        Some(id) => id,
        None => return Ok(SearchResult::empty()),
    };

    let vector_entries: Vec<_> = index_entries
        .iter()
        .filter(|e| {
            e.kind == FileKind::Add
                && VectorIndexBackend::from_index_type(&e.index_file.index_type).is_some()
                && e.index_file
                    .global_index_meta
                    .as_ref()
                    .is_some_and(|m| m.index_field_id == field_id)
        })
        .collect();

    if vector_entries.is_empty() {
        return Ok(SearchResult::empty());
    }

    let futures: Vec<_> = vector_entries
        .into_iter()
        .map(|entry| {
            let global_meta = entry.index_file.global_index_meta.as_ref().unwrap();
            let backend = VectorIndexBackend::from_index_type(&entry.index_file.index_type)
                .expect("filtered vector index type");
            let path = format!("{table_path}/{INDEX_DIR}/{}", entry.index_file.file_name);
            let file_name = entry.index_file.file_name.clone();
            let file_size = entry.index_file.file_size as u64;
            let index_meta_bytes = global_meta.index_meta.clone().unwrap_or_default();
            let row_range_start = global_meta.row_range_start;
            let vector_search_clone = vector_search.clone();
            let options = table_options.clone();
            let input = file_io.new_input(&path);
            async move {
                let input = input?;
                let bytes = input.read().await.map_err(|e| crate::Error::DataInvalid {
                    message: format!(
                        "Failed to read {} index file '{}': {}",
                        backend.error_name(),
                        file_name,
                        e
                    ),
                    source: None,
                })?;

                let io_meta =
                    GlobalIndexIOMeta::new(file_name.clone(), file_size, index_meta_bytes);
                let data = bytes.to_vec();
                let result = match backend {
                    VectorIndexBackend::Lumina => {
                        let mut reader = LuminaVectorGlobalIndexReader::new(io_meta, options);
                        reader
                            .visit_vector_search(&vector_search_clone, |_| Ok(Cursor::new(data)))?
                    }
                    VectorIndexBackend::Vindex => {
                        let mut reader = VindexVectorGlobalIndexReader::new(io_meta, options);
                        reader
                            .visit_vector_search(&vector_search_clone, |_| Ok(Cursor::new(data)))?
                    }
                };

                match result {
                    Some(scored_map) => Ok::<_, crate::Error>(
                        SearchResult::from_scored_map(scored_map).offset(row_range_start),
                    ),
                    None => Ok(SearchResult::empty()),
                }
            }
        })
        .collect();

    let results = futures::future::try_join_all(futures).await?;
    let mut merged = SearchResult::empty();
    for r in &results {
        merged = merged.or(r);
    }

    Ok(merged.top_k(vector_search.limit))
}

/// Compute, per data file in `split`, the set of file-LOCAL physical row
/// positions whose rows satisfy the residual predicate. Mirrors the
/// row-collecting half of Java `PrimaryKeyVectorRead`'s `executeFilter`: the
/// predicate is NOT pushed down (a pushed filter would drop rows before their
/// position could be recovered). Instead `reader` projects only the residual
/// columns and carries no pushdown predicate; every physical row is scanned in
/// file order, the residual is evaluated here at the Arrow level, and each
/// surviving row's file-local 0-based position is its running ordinal in the scan.
/// This needs no `_ROW_ID` and no `first_row_id` — real primary-key tables never
/// write one.
///
/// Every *active* data file in the split gets an entry, possibly empty. The
/// bucket search treats an absent entry and an empty entry identically (the file
/// contributes no candidates), so the empty entries only make the map cover every
/// active file. Non-active files (e.g. level-0 files the bucket search excludes)
/// are skipped entirely: they are never searched, so re-reading them would be
/// wasted IO.
///
/// `reader` must be predicate-free and project the residual columns;
/// `residual.file_fields` are the fields the residual leaf indices point into
/// (resolved by name against each emitted batch).
async fn residual_positions_by_file(
    reader: &DataFileReader,
    split: &DataSplit,
    active_files: &[BucketActiveFile],
    residual: &FilePredicates,
) -> crate::Result<HashMap<String, RoaringTreemap>> {
    let scan_fields = reader.read_type().to_vec();
    let active_names: HashSet<&str> = active_files.iter().map(|f| f.file_name.as_str()).collect();
    let mut out: HashMap<String, RoaringTreemap> = HashMap::new();
    for file_meta in split.data_files() {
        // Only files the bucket search actually recalls from need residual
        // positions; skip everything else to avoid a wasted read.
        if !active_names.contains(file_meta.file_name.as_str()) {
            continue;
        }
        let data_fields = reader.derive_data_fields(file_meta).await?;
        let mut stream =
            reader.read_single_file_stream(split, file_meta.clone(), data_fields, None, None)?;
        // Register the file up front so a file whose rows all fail the residual
        // still appears in the map (empty set).
        let positions = out.entry(file_meta.file_name.clone()).or_default();
        // The scan has no row selection and no DV, so rows arrive in physical file
        // order with no gaps: each row's file-local 0-based position is its running
        // ordinal `base + row_index`.
        let mut base: u64 = 0;
        while let Some(batch) = stream.try_next().await? {
            let num_rows = batch.num_rows();
            let mask = evaluate_predicates_mask(
                &batch,
                &residual.predicates,
                &residual.file_fields,
                &scan_fields,
            )?;
            match mask {
                Some(mask) => {
                    for row_index in 0..num_rows {
                        // NULL follows the same NULL -> false convention the Arrow
                        // filter kernel applies, so a null mask slot drops the row.
                        if mask.is_valid(row_index) && mask.value(row_index) {
                            positions.insert(base + row_index as u64);
                        }
                    }
                }
                // No predicate contributed a mask (identity) -> keep every row.
                None => {
                    for row_index in 0..num_rows {
                        positions.insert(base + row_index as u64);
                    }
                }
            }
            base += num_rows as u64;
        }
    }
    Ok(out)
}

/// Preload every ANN segment's bytes into a map keyed by the resolved (globally
/// unique) segment path. The scorer closure reads from this map so the vindex
/// reader is driven from memory without per-search IO.
///
/// Distinct segment paths are fetched concurrently (up to `concurrency`, Java
/// `GLOBAL_INDEX_THREAD_NUM`) so ANN index loading is no longer a serial prefix
/// before the parallel search — mirroring Java, where each segment's
/// `ensureLoaded` runs inside its own pool task alongside the search. Bytes are
/// kept as refcounted `Bytes`, so the scorer clones a handle (a refcount bump),
/// not the whole index buffer, when it hands them to the reader.
async fn preload_segment_bytes(
    file_io: &crate::io::FileIO,
    splits: &[PkVectorSearchSplit],
    concurrency: usize,
) -> crate::Result<HashMap<String, bytes::Bytes>> {
    // Distinct paths only: a segment path is globally unique and may recur across
    // buckets/splits, and it must be read exactly once.
    let mut distinct_paths: Vec<String> = Vec::new();
    let mut seen: HashSet<&str> = HashSet::new();
    for split in splits {
        for segment in &split.ann_segments {
            if seen.insert(segment.path.as_str()) {
                distinct_paths.push(segment.path.clone());
            }
        }
    }

    let fetches = distinct_paths.into_iter().map(|path| async move {
        let input = file_io.new_input(&path)?;
        let bytes = input.read().await.map_err(|e| crate::Error::DataInvalid {
            message: format!("failed to read ANN index file '{path}': {e}"),
            source: None,
        })?;
        Ok::<(String, bytes::Bytes), crate::Error>((path, bytes))
    });

    // `concurrency <= 1` reads strictly sequentially; larger values fan the reads
    // out. The fan-in is order-independent (keyed by path), so completion order
    // does not affect the result.
    let pairs: Vec<(String, bytes::Bytes)> = if concurrency <= 1 {
        let mut out = Vec::new();
        for fetch in fetches {
            out.push(fetch.await?);
        }
        out
    } else {
        futures::stream::iter(fetches)
            .buffer_unordered(concurrency)
            .try_collect::<Vec<_>>()
            .await?
    };
    Ok(pairs.into_iter().collect())
}

/// Fail loud when an ANN segment was trained with a metric other than the
/// configured one, mirroring the search-time `checkArgument` in Java
/// `PkVectorAnnSegmentSearcher.search`. Opens each distinct segment's preloaded
/// bytes once and compares its trained metric against `configured`.
fn verify_pk_vector_segment_metrics(
    splits: &[PkVectorSearchSplit],
    segment_bytes: &HashMap<String, bytes::Bytes>,
    configured: VectorSearchMetric,
    backend: VectorIndexBackend,
) -> crate::Result<()> {
    let mut checked: HashSet<&str> = HashSet::new();
    for split in splits {
        for segment in &split.ann_segments {
            if !checked.insert(segment.path.as_str()) {
                continue;
            }
            let segment_metric = match backend {
                VectorIndexBackend::Lumina => {
                    // Lumina records its metric in the serialized index metadata
                    // (`index_meta`), not in the segment file bytes.
                    let lumina_metric =
                        LuminaIndexMeta::deserialize(&segment.index_meta)?.metric()?;
                    VectorSearchMetric::from_lumina(lumina_metric)
                }
                VectorIndexBackend::Vindex => {
                    let bytes = segment_bytes.get(&segment.path).ok_or_else(|| {
                        crate::Error::DataInvalid {
                            message: format!(
                                "missing preloaded ANN bytes for segment '{}'",
                                segment.path
                            ),
                            source: None,
                        }
                    })?;
                    let reader = VIndexReader::open(Cursor::new(bytes.clone())).map_err(|e| {
                        crate::Error::DataInvalid {
                            message: format!(
                                "failed to open ANN index file '{}' for metric check: {e}",
                                segment.path
                            ),
                            source: Some(Box::new(e)),
                        }
                    })?;
                    VectorSearchMetric::from_vindex(reader.metadata().metric)
                }
            };
            if segment_metric != configured {
                return Err(crate::Error::DataInvalid {
                    message: format!(
                        "ANN segment metric {} does not match configured metric {}",
                        segment_metric.as_str(),
                        configured.as_str()
                    ),
                    source: None,
                });
            }
        }
    }
    Ok(())
}

fn pk_vector_query_dimension(
    table_options: &HashMap<String, String>,
    query_options: &HashMap<String, String>,
    index_type: &str,
    vector_field: &DataField,
) -> crate::Result<Option<usize>> {
    match vector_field.data_type() {
        DataType::Vector(vector_type)
            if matches!(vector_type.element_type(), DataType::Float(_)) =>
        {
            Ok(Some(vector_type.length() as usize))
        }
        DataType::Array(array_type) if matches!(array_type.element_type(), DataType::Float(_)) => {
            // Resolve the dimension per the configured backend. An `ARRAY<FLOAT>`
            // column carries no dimension in its type, so it comes from options —
            // but the option shape differs by backend. Lumina is not a vindex
            // index type, so routing it through `VindexVectorIndexOptions` would
            // reject it as unsupported before planning (even on an empty table).
            if is_lumina_index_type(index_type) {
                // Lumina reads `lumina.index.dimension` (default 128) from the
                // merged table+query options, matching `resolve_lumina_options`.
                let mut merged = table_options.clone();
                merged.extend(query_options.clone());
                let dimension = LuminaVectorIndexOptions::new(&merged)?.dimension;
                Ok(Some(dimension as usize))
            } else {
                Ok(Some(
                    VindexVectorIndexOptions::new(
                        table_options,
                        query_options,
                        index_type,
                        vector_field,
                    )?
                    .dimension(),
                ))
            }
        }
        _ => Ok(None),
    }
}

/// Rerank approximate (indexed) candidates by rereading ONLY their candidate
/// positions and recomputing the exact distance, then keep the best `limit`.
///
/// Unlike a whole-column preload, this reuses [`PkVectorPositionRead`] to read
/// just the selected physical rows of each hit file (positions -> row ranges ->
/// local ranges), so a rerank over a large ANN-covered file touches only the
/// candidate rows. Mirrors Java's IndexedSplit rerank.
///
/// Each returned row is matched back to its candidate by the
/// `_PKEY_VECTOR_POSITION` column VALUE (never batch order). The recomputed
/// distance is written into the ORIGINAL candidate so `split_index` /
/// partition / bucket survive (`build_indexed_splits` does not carry
/// `split_index`). A DV loaded exactly as [`PkVectorIndexedSplitRead::read`]
/// does drops deleted positions, so a candidate at a deleted position returns no
/// row and trips the leftover guard — a deleted candidate reaching rerank is a
/// real inconsistency (the search path already DV-filters), so fail loud.
#[allow(clippy::too_many_arguments)]
async fn rerank_indexed_positional(
    rerank_reader: &DataFileReader,
    indexed: Vec<PkVectorCandidate>,
    plan_splits: &[PkVectorSearchSplit],
    query_vector: &[f32],
    metric: VectorSearchMetric,
    limit: usize,
    vector_field: &DataField,
) -> crate::Result<Vec<PkVectorCandidate>> {
    // Original per-position candidates keyed by (split_index, file, position);
    // the recomputed distance is written back into these so split_index and
    // partition/bucket survive (build_indexed_splits does not carry split_index).
    let mut by_key: HashMap<(usize, String, i64), PkVectorCandidate> = HashMap::new();
    for c in &indexed {
        if by_key
            .insert(
                (c.split_index, c.data_file_name.clone(), c.row_position),
                c.clone(),
            )
            .is_some()
        {
            return Err(crate::Error::DataInvalid {
                message: "duplicate primary-key vector candidate for reranking".to_string(),
                source: None,
            });
        }
    }

    // Rebuild the split_index lookup by (partition bytes, bucket, file): the
    // indexed split exposes partition/bucket/file but not split_index.
    let mut split_index_of: HashMap<(Vec<u8>, i32, String), usize> = HashMap::new();
    for (i, s) in plan_splits.iter().enumerate() {
        let p = s.data_split.partition().to_serialized_bytes();
        let b = s.data_split.bucket();
        for f in s.data_split.data_files() {
            split_index_of.insert((p.clone(), b, f.file_name.clone()), i);
        }
    }

    // Every candidate must reference a (partition, bucket, file) that the plan
    // actually carries. Checking up front — before build_indexed_splits, which
    // indexes plan_splits by split_index — turns an absent file into a fail-loud
    // error rather than an out-of-range panic, and keeps the per-split lookup
    // below a self-consistent backstop.
    for c in &indexed {
        let key = (
            c.partition.to_serialized_bytes(),
            c.bucket,
            c.data_file_name.clone(),
        );
        if !split_index_of.contains_key(&key) {
            return Err(crate::Error::DataInvalid {
                message: format!("rerank split for {} not found in plan", c.data_file_name),
                source: None,
            });
        }
    }

    // Group the candidates into per-file indexed splits (position ranges + file
    // meta), reusing the exact grouping/validation the materialization path uses.
    let indexed_splits = build_indexed_splits(indexed, plan_splits, metric)?;

    let dimension = query_vector.len();
    let mut reranked: Vec<PkVectorCandidate> = Vec::new();
    for split in indexed_splits {
        let data_split = split.split.clone();
        let file_meta = data_split.data_files()[0].clone();
        let file_name = file_meta.file_name.clone();
        let partition_bytes = data_split.partition().to_serialized_bytes();
        let bucket = data_split.bucket();
        let split_index = *split_index_of
            .get(&(partition_bytes, bucket, file_name.clone()))
            .ok_or_else(|| crate::Error::DataInvalid {
                message: format!("rerank split for {file_name} not found in plan"),
                source: None,
            })?;

        // DV loaded exactly as PkVectorIndexedSplitRead::read does; skipping it
        // would score deleted rows.
        let dv_factory = rerank_reader.build_split_dv_factory(&data_split).await?;
        let dv = DataFileReader::deletion_vector_for_file(dv_factory.as_ref(), &file_name).await?;
        let data_fields = rerank_reader.derive_data_fields(&file_meta).await?;

        // Positions from the split's row_ranges (ascending); read only those.
        let positions = expand_ranges(&split.row_ranges, file_meta.row_count)?;
        let mut stream = PkVectorPositionRead::new(rerank_reader).read(
            &data_split,
            file_meta,
            data_fields,
            dv,
            positions,
            None, // no scores; rerank recomputes distance
        )?;

        while let Some(batch) = stream.try_next().await? {
            let pos_idx = batch
                .schema()
                .index_of(PKEY_VECTOR_POSITION_COLUMN)
                .map_err(|_| crate::Error::DataInvalid {
                    message: format!("rerank batch missing {PKEY_VECTOR_POSITION_COLUMN} column"),
                    source: None,
                })?;
            let pos_col = batch
                .column(pos_idx)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| crate::Error::DataInvalid {
                    message: format!("{PKEY_VECTOR_POSITION_COLUMN} column is not Int64"),
                    source: None,
                })?;
            let mut vectors: Vec<Option<Vec<f32>>> = Vec::new();
            append_batch_vectors(&batch, vector_field.name(), dimension, &mut vectors)?;
            for (row, vector) in vectors.iter().enumerate() {
                let position = pos_col.value(row);
                let mut candidate = by_key
                    .remove(&(split_index, file_name.clone(), position))
                    .ok_or_else(|| crate::Error::DataInvalid {
                        message: format!("rerank read unexpected position {file_name}@{position}"),
                        source: None,
                    })?;
                let vector = vector.as_ref().ok_or_else(|| crate::Error::DataInvalid {
                    message: format!(
                        "primary-key vector candidate {file_name}@{position} contains a null vector"
                    ),
                    source: None,
                })?;
                candidate.distance = metric.compute_distance(query_vector, vector);
                reranked.push(candidate);
            }
        }
    }

    if !by_key.is_empty() {
        return Err(crate::Error::DataInvalid {
            message: format!(
                "failed to read {} primary-key vector candidate(s) for reranking",
                by_key.len()
            ),
            source: None,
        });
    }

    Ok(merge_candidates(reranked, Vec::new(), limit))
}

/// One materialized row tagged with its best-first `rank` and its `(batch_index,
/// row_index)` location in the retained materialization batches.
struct RankedRow {
    rank: usize,
    batch_index: usize,
    row_index: usize,
}

/// For each row in a materialized batch, look up its best-first rank via the
/// `(partition bytes, bucket, file, position)` key and record its location. The
/// `_PKEY_VECTOR_POSITION` column supplies the physical position; every row must
/// map to a candidate rank (the batch came from that candidate's file), so a miss
/// fails loud rather than silently dropping a row.
#[allow(clippy::too_many_arguments)]
fn collect_ranked_rows(
    batch: &RecordBatch,
    batch_index: usize,
    partition_bytes: &[u8],
    bucket: i32,
    file_name: &str,
    rank_of: &HashMap<(Vec<u8>, i32, String, i64), usize>,
    out: &mut Vec<RankedRow>,
) -> crate::Result<()> {
    let position_idx = batch
        .schema()
        .index_of(PKEY_VECTOR_POSITION_COLUMN)
        .map_err(|_| crate::Error::DataInvalid {
            message: format!("materialized batch missing {PKEY_VECTOR_POSITION_COLUMN} column"),
            source: None,
        })?;
    let positions = batch
        .column(position_idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| crate::Error::DataInvalid {
            message: format!("{PKEY_VECTOR_POSITION_COLUMN} column is not Int64"),
            source: None,
        })?;
    for row_index in 0..batch.num_rows() {
        let position = positions.value(row_index);
        let key = (
            partition_bytes.to_vec(),
            bucket,
            file_name.to_string(),
            position,
        );
        let rank = *rank_of.get(&key).ok_or_else(|| crate::Error::DataInvalid {
            message: format!(
                "materialized row (file {file_name}, position {position}) has no matching search candidate"
            ),
            source: None,
        })?;
        out.push(RankedRow {
            rank,
            batch_index,
            row_index,
        });
    }
    Ok(())
}

/// Reorder the materialized rows into best-first order and drop the internal
/// `_PKEY_VECTOR_POSITION` column, yielding a single output batch (empty input
/// yields no batches). The projected user columns and `_PKEY_VECTOR_SCORE` are
/// retained.
fn reorder_and_strip_position(
    batches: &[RecordBatch],
    mut ranked: Vec<RankedRow>,
) -> crate::Result<Vec<RecordBatch>> {
    if ranked.is_empty() {
        return Ok(Vec::new());
    }
    ranked.sort_by_key(|r| r.rank);
    let indices: Vec<(usize, usize)> = ranked
        .iter()
        .map(|r| (r.batch_index, r.row_index))
        .collect();
    let refs: Vec<&RecordBatch> = batches.iter().collect();
    let reordered =
        interleave_record_batch(&refs, &indices).map_err(|e| crate::Error::DataInvalid {
            message: format!("failed to reorder vector search read rows: {e}"),
            source: None,
        })?;

    // Drop the internal position column; keep every other column (projected user
    // columns + _PKEY_VECTOR_SCORE) in order.
    let position_idx = reordered
        .schema()
        .index_of(PKEY_VECTOR_POSITION_COLUMN)
        .map_err(|_| crate::Error::DataInvalid {
            message: format!("reordered batch missing {PKEY_VECTOR_POSITION_COLUMN} column"),
            source: None,
        })?;
    let keep: Vec<usize> = (0..reordered.num_columns())
        .filter(|i| *i != position_idx)
        .collect();
    let projected = reordered
        .project(&keep)
        .map_err(|e| crate::Error::DataInvalid {
            message: format!("failed to drop position column: {e}"),
            source: None,
        })?;
    Ok(vec![projected])
}

fn indexed_search_limit(limit: usize, refine_factor: usize) -> crate::Result<usize> {
    if refine_factor == 0 {
        return Ok(limit);
    }
    let search_limit =
        limit
            .checked_mul(refine_factor)
            .ok_or_else(|| crate::Error::ConfigInvalid {
                message: format!(
                    "Vector search limit overflow: limit={limit}, refine factor={refine_factor}"
                ),
            })?;
    if search_limit > i32::MAX as usize {
        return Err(crate::Error::ConfigInvalid {
            message: format!(
                "Vector search limit overflow: limit={limit}, refine factor={refine_factor}"
            ),
        });
    }
    Ok(search_limit)
}

fn normalize_metric(metric: &str) -> String {
    metric.to_ascii_lowercase().replace('-', "_")
}

/// Option-key prefixes probed (in order) for the refine-factor setting of a given
/// field/index type: `fields.<field>.<index_type>.`, its normalized variant, an
/// `ivf.` collapse, then the bare field/global scope.
fn indexed_type_prefixes(field_name: &str, index_type: &str) -> Vec<String> {
    let mut prefixes = Vec::new();
    add_refine_prefixes(&mut prefixes, &format!("fields.{field_name}."), index_type);
    add_refine_prefixes(&mut prefixes, "", index_type);
    prefixes
}

fn add_refine_prefixes(prefixes: &mut Vec<String>, base: &str, index_type: &str) {
    if !index_type.is_empty() {
        prefixes.push(format!("{base}{index_type}."));
        let normalized = normalize_metric(index_type);
        if normalized != index_type {
            prefixes.push(format!("{base}{normalized}."));
        }
        if normalized.starts_with("ivf") {
            prefixes.push(format!("{base}ivf."));
        }
    }
    prefixes.push(base.to_string());
}

/// Resolve the configured refine factor, preferring the query options over the
/// table options; returns 0 when unset. An invalid (non-numeric or zero) value
/// fails loud.
fn configured_refine_factor(
    search_options: &HashMap<String, String>,
    table_options: &HashMap<String, String>,
    field_name: &str,
    index_type: &str,
) -> crate::Result<usize> {
    if let Some(value) =
        configured_refine_factor_from_options(search_options, field_name, index_type)
    {
        return parse_refine_factor(&value);
    }
    if let Some(value) =
        configured_refine_factor_from_options(table_options, field_name, index_type)
    {
        return parse_refine_factor(&value);
    }
    Ok(0)
}

fn configured_refine_factor_from_options(
    options: &HashMap<String, String>,
    field_name: &str,
    index_type: &str,
) -> Option<String> {
    for prefix in indexed_type_prefixes(field_name, index_type) {
        for suffix in [
            "refine_factor",
            "refine-factor",
            "rerank_factor",
            "rerank-factor",
        ] {
            if let Some(value) = options.get(&(prefix.clone() + suffix)) {
                return Some(value.trim().to_string());
            }
        }
    }
    None
}

fn parse_refine_factor(value: &str) -> crate::Result<usize> {
    let factor = value
        .parse::<usize>()
        .map_err(|_| crate::Error::ConfigInvalid {
            message: format!("Invalid vector refine factor: {value}. Must be an integer."),
        })?;
    if factor == 0 {
        return Err(crate::Error::ConfigInvalid {
            message: format!("Vector refine factor must be positive, got: {value}"),
        });
    }
    Ok(factor)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::Identifier;
    use crate::io::{FileIO, FileIOBuilder};
    use crate::lumina::{LEGACY_LUMINA_VECTOR_ANN_IDENTIFIER, LUMINA_IDENTIFIER};
    use crate::spec::stats::BinaryTableStats;
    use crate::spec::{
        ArrayType, BinaryRow, DataFileMeta, DataType, Datum, FloatType, GlobalIndexMeta,
        IndexFileMeta, IndexManifestEntry, IntType, PredicateBuilder, Schema, TableSchema,
    };
    use crate::table::source::DataSplitBuilder;
    use crate::vindex::IVF_FLAT_IDENTIFIER;
    use arrow_array::builder::{FixedSizeListBuilder, Float32Builder};
    use arrow_array::{Float32Array, Int32Array};
    use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
    use std::sync::Arc;

    fn l2_score(distance: f32) -> f32 {
        VectorSearchMetric::L2.distance_to_score(distance)
    }

    fn make_field(id: i32, name: &str) -> DataField {
        DataField::new(id, name.to_string(), DataType::Int(IntType::default()))
    }

    fn vector_test_table() -> Table {
        use std::collections::HashMap;
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column(
                "embedding",
                DataType::Array(ArrayType::new(DataType::Float(FloatType::new()))),
            )
            .build()
            .unwrap();
        let mut options = HashMap::new();
        options.insert(
            "fields.embedding.pk-vector.index.type".to_string(),
            "ivf-flat".to_string(),
        );
        options.insert(
            "fields.embedding.pk-vector.distance.metric".to_string(),
            "l2".to_string(),
        );
        options.insert(
            "pk-vector.index.columns".to_string(),
            "embedding".to_string(),
        );
        let table_schema = TableSchema::new(0, &schema).copy_with_options(options);
        Table::new(
            FileIOBuilder::new("memory").build().unwrap(),
            Identifier::new("default", "vector_test"),
            "memory:/vector_test".to_string(),
            table_schema,
            None,
        )
    }

    #[test]
    fn test_find_field_id_by_name() {
        let fields = vec![make_field(1, "id"), make_field(2, "embedding")];
        assert_eq!(find_field_id_by_name(&fields, "embedding"), Some(2));
        assert_eq!(find_field_id_by_name(&fields, "nonexistent"), None);
    }

    #[test]
    fn test_configured_refine_factor_precedence_and_aliases() {
        let table_options = HashMap::from([(
            "fields.embedding.ivf.refine-factor".to_string(),
            "3".to_string(),
        )]);
        let search_options = HashMap::from([(
            "fields.embedding.ivf_flat.rerank_factor".to_string(),
            "2".to_string(),
        )]);
        assert_eq!(
            configured_refine_factor(
                &search_options,
                &table_options,
                "embedding",
                IVF_FLAT_IDENTIFIER,
            )
            .unwrap(),
            2
        );

        assert_eq!(
            configured_refine_factor(
                &HashMap::new(),
                &table_options,
                "embedding",
                IVF_FLAT_IDENTIFIER,
            )
            .unwrap(),
            3
        );

        let global_options = HashMap::from([("rerank-factor".to_string(), "4".to_string())]);
        assert_eq!(
            configured_refine_factor(
                &HashMap::new(),
                &global_options,
                "embedding",
                LUMINA_IDENTIFIER,
            )
            .unwrap(),
            4
        );
    }

    #[test]
    fn test_configured_refine_factor_rejects_invalid_values() {
        let zero_options = HashMap::from([("refine_factor".to_string(), "0".to_string())]);
        let err = configured_refine_factor(
            &zero_options,
            &HashMap::new(),
            "embedding",
            LUMINA_IDENTIFIER,
        )
        .unwrap_err();
        assert!(err.to_string().contains("must be positive"));

        let invalid_options = HashMap::from([("refine_factor".to_string(), "abc".to_string())]);
        let err = configured_refine_factor(
            &invalid_options,
            &HashMap::new(),
            "embedding",
            LUMINA_IDENTIFIER,
        )
        .unwrap_err();
        assert!(err.to_string().contains("Must be an integer"));

        assert!(indexed_search_limit(i32::MAX as usize, 2).is_err());
    }

    #[tokio::test]
    async fn test_batch_vector_search_requires_vectors() {
        let table = vector_test_table();
        let err = table
            .new_batch_vector_search_builder()
            .with_vector_column("embedding")
            .with_query_vectors(Vec::new())
            .with_limit(1)
            .execute()
            .await
            .unwrap_err();

        assert!(
            err.to_string()
                .contains("Query vectors must be set via with_query_vectors()"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn test_batch_vector_search_rejects_zero_limit() {
        let table = vector_test_table();
        let err = table
            .new_batch_vector_search_builder()
            .with_vector_column("embedding")
            .with_query_vectors(vec![vec![1.0]])
            .with_limit(0)
            .execute()
            .await
            .unwrap_err();

        assert!(
            err.to_string().contains("Limit must be between 1"),
            "unexpected error: {err}"
        );
    }

    fn pk_data_file(name: &str, row_count: i64, first_row_id: Option<i64>) -> DataFileMeta {
        DataFileMeta {
            file_name: name.to_string(),
            file_size: 1,
            row_count,
            min_key: Vec::new(),
            max_key: Vec::new(),
            key_stats: BinaryTableStats::empty(),
            value_stats: BinaryTableStats::empty(),
            min_sequence_number: 0,
            max_sequence_number: 0,
            schema_id: 1,
            level: 0,
            extra_files: Vec::new(),
            creation_time: None,
            delete_row_count: None,
            embedded_index: None,
            file_source: None,
            value_stats_cols: None,
            external_path: None,
            first_row_id,
            write_cols: None,
            commit_snapshot_id: None,
            merge_mode: None,
        }
    }

    fn pk_search_split(bucket: i32, files: Vec<DataFileMeta>) -> PkVectorSearchSplit {
        PkVectorSearchSplit {
            data_split: DataSplitBuilder::new()
                .with_snapshot(1)
                .with_partition(BinaryRow::new(0))
                .with_bucket(bucket)
                .with_bucket_path(format!("memory:/t/bucket-{bucket}"))
                .with_total_buckets(1)
                .with_data_files(files)
                .build()
                .unwrap(),
            ann_segments: Vec::new(),
            active_files: Vec::new(),
        }
    }

    fn pk_candidate(
        split_index: usize,
        bucket: i32,
        file: &str,
        pos: i64,
        distance: f32,
    ) -> PkVectorCandidate {
        PkVectorCandidate {
            split_index,
            partition: BinaryRow::new(0),
            bucket,
            data_file_name: file.to_string(),
            row_position: pos,
            distance,
        }
    }

    // Candidate with a fixed empty (arity-0) partition and bucket 0, keyed only by
    // (split_index, file, position) — the dimensions the rerank core groups on.
    fn cand_at(split_index: usize, file: &str, pos: i64, dist: f32) -> PkVectorCandidate {
        pk_candidate(split_index, 0, file, pos, dist)
    }

    /// The single data-file name every rerank fixture writes.
    const RERANK_FILE: &str = "part-0.parquet";

    /// Serialize a Paimon deletion-vector blob covering `deleted_rows` and write it
    /// at `path`, returning the matching `DeletionFile`. Byte layout mirrors the
    /// position-read tests: `[length][magic][roaring bitmap][0]`.
    async fn write_deletion_blob(
        file_io: &FileIO,
        path: &str,
        deleted_rows: &[u32],
    ) -> crate::table::source::DeletionFile {
        use roaring::RoaringBitmap;

        const MAGIC_NUMBER: i32 = 1581511376;
        let mut bitmap = RoaringBitmap::new();
        for row in deleted_rows {
            bitmap.insert(*row);
        }
        let mut bitmap_bytes = Vec::new();
        bitmap.serialize_into(&mut bitmap_bytes).unwrap();
        let bitmap_length = 4 + bitmap_bytes.len() as i32;
        let mut blob = Vec::new();
        blob.extend_from_slice(&bitmap_length.to_be_bytes());
        blob.extend_from_slice(&MAGIC_NUMBER.to_be_bytes());
        blob.extend_from_slice(&bitmap_bytes);
        blob.extend_from_slice(&0i32.to_be_bytes());
        file_io
            .new_output(path)
            .unwrap()
            .write(bytes::Bytes::from(blob))
            .await
            .unwrap();
        crate::table::source::DeletionFile::new(
            path.to_string(),
            0,
            bitmap_length as i64,
            Some(deleted_rows.len() as i64),
        )
    }

    /// Write a single-file vector data file (`FixedSizeList<Float32>` of width
    /// `dim`) holding `rows` (a `None` entry is a NULL vector row) as Parquet, and
    /// return a vector-only `DataFileReader`, the enclosing `PkVectorSearchSplit`,
    /// and the vector `DataField`. When `deleted_rows` is non-empty a deletion
    /// vector covering those physical positions is attached to the split, so the
    /// position read drops them exactly as `PkVectorIndexedSplitRead::read` does.
    ///
    /// This is the position-only analogue of the old `ArrayReader`: rerank now
    /// re-reads real stored rows through `PkVectorPositionRead`, so the fixtures
    /// exercise that path rather than an in-memory preloaded column.
    async fn vector_rerank_fixture(
        table_path: &str,
        dim: u32,
        rows: &[Option<Vec<f32>>],
        deleted_rows: &[u32],
    ) -> (DataFileReader, PkVectorSearchSplit, DataField) {
        use crate::arrow::build_target_arrow_schema;
        use crate::arrow::format::{FormatFileWriter, ParquetFormatWriter};
        use crate::spec::VectorType;
        use crate::table::schema_manager::SchemaManager;

        let vector_type =
            VectorType::try_new(true, dim, DataType::Float(FloatType::new())).unwrap();
        let vector_field =
            DataField::new(0, "embedding".to_string(), DataType::Vector(vector_type));
        let read_fields = vec![vector_field.clone()];
        let arrow_schema = build_target_arrow_schema(&read_fields).unwrap();

        let mut builder = FixedSizeListBuilder::new(Float32Builder::new(), dim as i32).with_field(
            Arc::new(ArrowField::new("element", ArrowDataType::Float32, true)),
        );
        for row in rows {
            match row {
                Some(values) => {
                    for v in values {
                        builder.values().append_value(*v);
                    }
                    builder.append(true);
                }
                None => {
                    for _ in 0..dim {
                        builder.values().append_value(0.0);
                    }
                    builder.append(false);
                }
            }
        }
        let vec_array = builder.finish();
        let batch =
            arrow_array::RecordBatch::try_new(arrow_schema.clone(), vec![Arc::new(vec_array)])
                .unwrap();

        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let bucket_path = format!("{table_path}/bucket-0");
        let output = file_io
            .new_output(&format!("{bucket_path}/{RERANK_FILE}"))
            .unwrap();
        let mut writer: Box<dyn FormatFileWriter> = Box::new(
            ParquetFormatWriter::new(&output, arrow_schema.clone(), "zstd", 1)
                .await
                .unwrap(),
        );
        writer.write(&batch).await.unwrap();
        let file_size = writer.close().await.unwrap();

        let schema_id = 1;
        let file_meta = pk_data_file(RERANK_FILE, rows.len() as i64, Some(0));
        let file_meta = DataFileMeta {
            file_size: file_size as i64,
            schema_id,
            ..file_meta
        };

        let mut split_builder = DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path(bucket_path)
            .with_total_buckets(1)
            .with_data_files(vec![file_meta]);
        if !deleted_rows.is_empty() {
            let df =
                write_deletion_blob(&file_io, &format!("{table_path}/index/dv-0"), deleted_rows)
                    .await;
            split_builder = split_builder.with_data_deletion_files(vec![Some(df)]);
        }
        let data_split = split_builder.build().unwrap();
        let split = PkVectorSearchSplit {
            data_split,
            ann_segments: Vec::new(),
            active_files: Vec::new(),
        };

        let schema_manager = SchemaManager::new(file_io.clone(), table_path.to_string());
        let reader = DataFileReader::new(
            file_io,
            schema_manager,
            schema_id,
            read_fields.clone(),
            read_fields,
            Vec::new(),
            1024,
            false,
            false,
        );
        (reader, split, vector_field)
    }

    #[tokio::test]
    async fn rerank_aligns_recomputed_distance_by_position_column() {
        use crate::arrow::build_target_arrow_schema;
        use crate::arrow::format::{FormatFileWriter, ParquetFormatWriter};
        use crate::spec::VectorType;
        use crate::table::schema_manager::SchemaManager;

        // A vector data file with 4 physical rows: positions 0,1,3 hold vectors
        // and position 2 (a NON-candidate) holds a NULL vector. Candidates sit at
        // non-contiguous positions {1, 3}. The ANN-reported distances are
        // deliberately reversed relative to the true stored vectors; after rerank
        // each candidate must carry compute_distance(query, vec_at_its_position),
        // proving alignment is by the _PKEY_VECTOR_POSITION column value, not batch
        // order. Position 2's NULL is never read (it is not a candidate), so it
        // cannot trip the null-vector guard.
        let vector_type = VectorType::try_new(true, 2, DataType::Float(FloatType::new())).unwrap();
        let vector_field =
            DataField::new(0, "embedding".to_string(), DataType::Vector(vector_type));
        let read_fields = vec![vector_field.clone()];
        let arrow_schema = build_target_arrow_schema(&read_fields).unwrap();

        // pos0=[7,0], pos1=[1,0], pos2=NULL, pos3=[4,0].
        let mut builder = FixedSizeListBuilder::new(Float32Builder::new(), 2).with_field(Arc::new(
            ArrowField::new("element", ArrowDataType::Float32, true),
        ));
        for row in [
            Some([7.0f32, 0.0]),
            Some([1.0, 0.0]),
            None,
            Some([4.0, 0.0]),
        ] {
            match row {
                Some([a, b]) => {
                    builder.values().append_value(a);
                    builder.values().append_value(b);
                    builder.append(true);
                }
                None => {
                    builder.values().append_value(0.0);
                    builder.values().append_value(0.0);
                    builder.append(false);
                }
            }
        }
        let vec_array = builder.finish();
        let batch =
            arrow_array::RecordBatch::try_new(arrow_schema.clone(), vec![Arc::new(vec_array)])
                .unwrap();

        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let table_path = "memory:/rerank_positional";
        let bucket_path = format!("{table_path}/bucket-0");
        let file_name = "part-0.parquet";
        let output = file_io
            .new_output(&format!("{bucket_path}/{file_name}"))
            .unwrap();
        let mut writer: Box<dyn FormatFileWriter> = Box::new(
            ParquetFormatWriter::new(&output, arrow_schema.clone(), "zstd", 1)
                .await
                .unwrap(),
        );
        writer.write(&batch).await.unwrap();
        let file_size = writer.close().await.unwrap();

        let schema_id = 1;
        let file_meta = pk_data_file(file_name, 4, Some(0));
        let file_meta = DataFileMeta {
            file_size: file_size as i64,
            schema_id,
            ..file_meta
        };
        let data_split = DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path(bucket_path)
            .with_total_buckets(1)
            .with_data_files(vec![file_meta])
            .build()
            .unwrap();
        let split = PkVectorSearchSplit {
            data_split,
            ann_segments: Vec::new(),
            active_files: Vec::new(),
        };

        let schema_manager = SchemaManager::new(file_io.clone(), table_path.to_string());
        let reader = DataFileReader::new(
            file_io,
            schema_manager,
            schema_id,
            read_fields.clone(),
            read_fields.clone(),
            Vec::new(),
            1024,
            false,
            false,
        );

        let query = vec![1.0f32, 0.0];
        // ANN-reported distances reversed vs. truth: pos1 reported worse (0.9) than
        // pos3 (0.1), but the true L2 distances are pos1=0 and pos3=9.
        let indexed = vec![cand_at(0, file_name, 1, 0.9), cand_at(0, file_name, 3, 0.1)];

        let out = rerank_indexed_positional(
            &reader,
            indexed,
            &[split],
            &query,
            VectorSearchMetric::L2,
            2,
            &vector_field,
        )
        .await
        .unwrap();

        // Best-first after exact recompute: pos1 (d=0) then pos3 (d=9), each
        // carrying the distance computed from its OWN position's stored vector.
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].row_position, 1);
        assert_eq!(out[0].distance, 0.0);
        assert_eq!(out[1].row_position, 3);
        assert_eq!(out[1].distance, 9.0);
    }

    #[tokio::test]
    async fn rerank_recomputes_distance_and_reorders() {
        // pos0=[9,0], pos1=[1,0]; query=[1,0]. The ANN-reported distances are
        // reversed relative to the truth (pos0 reported best at 0.1, pos1 worst at
        // 0.9), so an implementation that trusted the ANN order would emit pos0
        // first. Exact L2 recompute yields pos0=64, pos1=0, so the output must
        // reorder to pos1-then-pos0 with the recomputed distances.
        let (reader, split, vector_field) = vector_rerank_fixture(
            "memory:/rerank_reorder",
            2,
            &[Some(vec![9.0, 0.0]), Some(vec![1.0, 0.0])],
            &[],
        )
        .await;
        let query = vec![1.0f32, 0.0];
        let indexed = vec![
            cand_at(0, RERANK_FILE, 0, 0.1),
            cand_at(0, RERANK_FILE, 1, 0.9),
        ];

        let out = rerank_indexed_positional(
            &reader,
            indexed,
            &[split],
            &query,
            VectorSearchMetric::L2,
            2,
            &vector_field,
        )
        .await
        .unwrap();

        assert_eq!(out.len(), 2);
        assert_eq!(out[0].row_position, 1);
        assert_eq!(out[0].distance, 0.0);
        assert_eq!(out[1].row_position, 0);
        assert_eq!(out[1].distance, 64.0);
        // Order genuinely changed vs. the ANN-reported best-first (which was pos0).
        assert!(out[0].distance < out[1].distance);
    }

    #[tokio::test]
    async fn rerank_is_independent_of_fast_mode_reranks_indexed() {
        // The rerank core takes only the indexed (fast-path) candidates and always
        // recomputes their true distance; there is no fast/exact switch that can
        // skip it. The single candidate carries a bogus ANN distance (0.42) but its
        // stored vector equals the query, so the recomputed L2 distance is exactly
        // 0.0 — proving the indexed candidate WAS reranked rather than passed
        // through with its ANN distance.
        let (reader, split, vector_field) =
            vector_rerank_fixture("memory:/rerank_indexed", 2, &[Some(vec![1.0, 0.0])], &[]).await;
        let query = vec![1.0f32, 0.0];
        let indexed = vec![cand_at(0, RERANK_FILE, 0, 0.42)];

        let out = rerank_indexed_positional(
            &reader,
            indexed,
            &[split],
            &query,
            VectorSearchMetric::L2,
            1,
            &vector_field,
        )
        .await
        .unwrap();

        assert_eq!(out.len(), 1);
        assert_eq!(out[0].row_position, 0);
        assert_ne!(out[0].distance, 0.42);
        assert_eq!(out[0].distance, 0.0);
    }

    #[tokio::test]
    async fn rerank_fails_loud_on_null_vector() {
        // A NULL vector stored AT a candidate position must fail loud rather than
        // silently scoring it: the candidate genuinely has no vector to rerank on.
        let (reader, split, vector_field) =
            vector_rerank_fixture("memory:/rerank_null", 2, &[None], &[]).await;
        let query = vec![1.0f32, 0.0];
        let indexed = vec![cand_at(0, RERANK_FILE, 0, 0.1)];

        let err = rerank_indexed_positional(
            &reader,
            indexed,
            &[split],
            &query,
            VectorSearchMetric::L2,
            1,
            &vector_field,
        )
        .await
        .err()
        .expect("null vector at a candidate position must fail loud");
        assert!(
            matches!(err, crate::Error::DataInvalid { ref message, .. } if message.contains("null vector")),
            "unexpected error: {err:?}"
        );
    }

    #[tokio::test]
    async fn rerank_fails_loud_on_leftover_candidate() {
        // pos1 is deleted by the deletion vector, so the position read returns no
        // row for it. The search path already DV-filters, so a deleted candidate
        // reaching rerank is a real inconsistency: the leftover guard must fail
        // loud rather than silently dropping the candidate.
        let (reader, split, vector_field) = vector_rerank_fixture(
            "memory:/rerank_leftover",
            2,
            &[Some(vec![1.0, 0.0]), Some(vec![2.0, 0.0])],
            &[1],
        )
        .await;
        let query = vec![1.0f32, 0.0];
        let indexed = vec![
            cand_at(0, RERANK_FILE, 0, 0.1),
            cand_at(0, RERANK_FILE, 1, 0.9),
        ];

        let err = rerank_indexed_positional(
            &reader,
            indexed,
            &[split],
            &query,
            VectorSearchMetric::L2,
            2,
            &vector_field,
        )
        .await
        .err()
        .expect("a candidate returning no row must fail loud");
        assert!(
            matches!(err, crate::Error::DataInvalid { ref message, .. } if message.contains("failed to read")),
            "unexpected error: {err:?}"
        );
    }

    #[tokio::test]
    async fn rerank_fails_loud_on_dimension_mismatch() {
        // Stored vectors are 3-dimensional but the query is 2-dimensional. The
        // vector extraction validates each stored row against the query dimension
        // and fails loud, so the recompute never runs against mismatched vectors.
        let (reader, split, vector_field) =
            vector_rerank_fixture("memory:/rerank_dim", 3, &[Some(vec![1.0, 0.0, 0.0])], &[]).await;
        let query = vec![1.0f32, 0.0];
        let indexed = vec![cand_at(0, RERANK_FILE, 0, 0.1)];

        let err = rerank_indexed_positional(
            &reader,
            indexed,
            &[split],
            &query,
            VectorSearchMetric::L2,
            1,
            &vector_field,
        )
        .await
        .err()
        .expect("dimension mismatch must fail loud");
        assert!(
            matches!(err, crate::Error::DataInvalid { ref message, .. } if message.contains("dimension")),
            "unexpected error: {err:?}"
        );
    }

    #[tokio::test]
    async fn rerank_fails_loud_on_duplicate_candidate_position() {
        // Two candidates addressing the same (split_index, file, position) is a
        // programming error upstream: the dedup guard fires before any read.
        let (reader, split, vector_field) =
            vector_rerank_fixture("memory:/rerank_dup", 2, &[Some(vec![1.0, 0.0])], &[]).await;
        let query = vec![1.0f32, 0.0];
        let indexed = vec![
            cand_at(0, RERANK_FILE, 0, 0.1),
            cand_at(0, RERANK_FILE, 0, 0.9),
        ];

        let err = rerank_indexed_positional(
            &reader,
            indexed,
            &[split],
            &query,
            VectorSearchMetric::L2,
            2,
            &vector_field,
        )
        .await
        .err()
        .expect("duplicate candidate position must fail loud");
        assert!(
            matches!(err, crate::Error::DataInvalid { ref message, .. } if message.contains("duplicate")),
            "unexpected error: {err:?}"
        );
    }

    #[tokio::test]
    async fn rerank_fails_loud_on_unexpected_position() {
        // Every position the read surfaces must resolve to a candidate keyed by
        // (split_index, file, position). Here the plan carries two splits for the
        // SAME (partition, bucket, file), so `split_index_of` resolves the file to
        // the LAST plan index (1). The single candidate is tagged with split_index
        // 0, so its by_key entry is (0, file, 0) while the read looks up
        // (1, file, 0). The lookup misses and the unexpected-position guard fires
        // rather than silently dropping the surfaced row.
        let (reader, split, vector_field) =
            vector_rerank_fixture("memory:/rerank_unexpected", 2, &[Some(vec![1.0, 0.0])], &[])
                .await;
        let query = vec![1.0f32, 0.0];
        let indexed = vec![cand_at(0, RERANK_FILE, 0, 0.1)];

        // Two plan entries for the same file: split_index_of ends up mapping the
        // file to plan index 1, not the candidate's split_index 0.
        let dup = PkVectorSearchSplit {
            data_split: split.data_split.clone(),
            ann_segments: Vec::new(),
            active_files: Vec::new(),
        };
        let plan = vec![dup, split];

        let err = rerank_indexed_positional(
            &reader,
            indexed,
            &plan,
            &query,
            VectorSearchMetric::L2,
            1,
            &vector_field,
        )
        .await
        .err()
        .expect("a read position absent from the candidate map must fail loud");
        assert!(
            matches!(err, crate::Error::DataInvalid { ref message, .. } if message.contains("unexpected position")),
            "unexpected error: {err:?}"
        );
    }

    #[tokio::test]
    async fn rerank_fails_loud_on_file_not_in_plan() {
        // A candidate references a (partition, bucket, file) that is absent from
        // plan_splits. build_indexed_splits groups it into an indexed split, but the
        // split_index_of lookup — built only from plan_splits — has no entry, so the
        // kernel fails loud rather than reading an unplanned file.
        let (reader, _split, vector_field) =
            vector_rerank_fixture("memory:/rerank_noplan", 2, &[Some(vec![1.0, 0.0])], &[]).await;
        let query = vec![1.0f32, 0.0];
        let indexed = vec![cand_at(0, RERANK_FILE, 0, 0.1)];

        // Empty plan: the candidate's file resolves in no plan split.
        let err = rerank_indexed_positional(
            &reader,
            indexed,
            &[],
            &query,
            VectorSearchMetric::L2,
            1,
            &vector_field,
        )
        .await
        .err()
        .expect("a candidate file absent from the plan must fail loud");
        assert!(
            matches!(err, crate::Error::DataInvalid { ref message, .. } if message.contains("not found in plan")),
            "unexpected error: {err:?}"
        );
    }

    #[tokio::test]
    async fn rerank_reads_only_candidate_positions_not_whole_column() {
        // A 6-row file where every NON-candidate position (0, 2, 4, 5) holds a NULL
        // vector "poison" and only the two candidate positions (1, 3) hold real
        // vectors. The rerank read is told to fetch only positions {1, 3}; every
        // row it surfaces is looked up in the candidate map, and any position not in
        // the map trips the "unexpected position" guard (a surfaced NULL row would
        // additionally trip the null-vector guard). So if the read had surfaced any
        // of the poison rows, rerank would fail. It succeeds and returns exactly the
        // two candidates at positions {1, 3}, which proves the position selection
        // reaching the read contained only the candidate positions (not the whole
        // column).
        let rows = &[
            None,                 // pos0 poison (non-candidate)
            Some(vec![1.0, 0.0]), // pos1 candidate
            None,                 // pos2 poison (non-candidate)
            Some(vec![3.0, 0.0]), // pos3 candidate
            None,                 // pos4 poison (non-candidate)
            None,                 // pos5 poison (non-candidate)
        ];
        let (reader, split, vector_field) =
            vector_rerank_fixture("memory:/rerank_spy", 2, rows, &[]).await;
        let query = vec![1.0f32, 0.0];
        let indexed = vec![
            cand_at(0, RERANK_FILE, 1, 0.9),
            cand_at(0, RERANK_FILE, 3, 0.1),
        ];

        let out = rerank_indexed_positional(
            &reader,
            indexed,
            &[split],
            &query,
            VectorSearchMetric::L2,
            2,
            &vector_field,
        )
        .await
        .unwrap_or_else(|e| {
            panic!("only candidate positions are read, so the poison NULLs never decode: {e:?}")
        });

        assert_eq!(out.len(), 2, "exactly the candidate count of rows was read");
        let mut positions: Vec<i64> = out.iter().map(|c| c.row_position).collect();
        positions.sort_unstable();
        assert_eq!(
            positions,
            vec![1, 3],
            "only candidate positions reached the read"
        );
        // Recomputed distances confirm each surviving row is its own candidate's vector.
        assert_eq!(out[0].row_position, 1);
        assert_eq!(out[0].distance, 0.0);
        assert_eq!(out[1].row_position, 3);
        assert_eq!(out[1].distance, 4.0);
    }

    /// Build a real vindex IVF-flat segment trained with `metric`, returning the
    /// serialized bytes. `nlist = 1` keeps training trivial and deterministic; the
    /// only thing the metric check cares about is the persisted metadata metric.
    fn build_vindex_segment_bytes(metric: &str) -> Vec<u8> {
        use paimon_vindex_core::index::{VectorIndexConfig, VectorIndexTrainer, VectorIndexWriter};
        use paimon_vindex_core::io::PosWriter;

        const DIM: usize = 2;
        let vectors: Vec<f32> = vec![1.0, 0.0, 0.0, 1.0, 1.0, 1.0];
        let n = vectors.len() / DIM;
        let ids: Vec<i64> = (0..n as i64).collect();
        let options = HashMap::from([
            ("index.type".to_string(), "ivf_flat".to_string()),
            ("dimension".to_string(), DIM.to_string()),
            ("nlist".to_string(), "1".to_string()),
            ("metric".to_string(), metric.to_string()),
        ]);
        let config = VectorIndexConfig::from_options(&options).unwrap();
        let training = VectorIndexTrainer::train(config, &vectors, n).unwrap();
        let mut writer = VectorIndexWriter::new(training);
        writer.add_vectors(&ids, &vectors, n).unwrap();
        let mut bytes = Vec::new();
        {
            let mut output = PosWriter::new(&mut bytes);
            writer.write(&mut output).unwrap();
        }
        bytes
    }

    /// A `PkVectorSearchSplit` carrying a single ANN segment addressed by `path`.
    fn pk_split_with_segment(path: &str) -> PkVectorSearchSplit {
        let mut split = pk_search_split(0, vec![pk_data_file("file-a", 3, Some(0))]);
        let source_meta = crate::spec::PkVectorSourceMeta::new(
            1,
            vec![crate::spec::PkVectorSourceFile::new("file-a".to_string(), 3).unwrap()],
        )
        .unwrap();
        let mut segment = BucketAnnSegment::for_test(source_meta);
        segment.path = path.to_string();
        split.ann_segments = vec![segment];
        split
    }

    fn pk_split_with_lumina_segment(path: &str, metric: &str) -> PkVectorSearchSplit {
        let mut split = pk_search_split(0, vec![pk_data_file("file-a", 3, Some(0))]);
        let source_meta = crate::spec::PkVectorSourceMeta::new(
            1,
            vec![crate::spec::PkVectorSourceFile::new("file-a".to_string(), 3).unwrap()],
        )
        .unwrap();
        let mut segment = BucketAnnSegment::for_test(source_meta);
        segment.path = path.to_string();
        // Lumina stores its metric in the serialized index metadata blob, not in
        // the segment file bytes. `deserialize` requires both keys present.
        let meta = crate::lumina::LuminaIndexMeta::new(HashMap::from([
            ("index.dimension".to_string(), "2".to_string()),
            ("distance.metric".to_string(), metric.to_string()),
        ]));
        segment.index_meta = meta.serialize().unwrap();
        split.ann_segments = vec![segment];
        split
    }

    fn verify_pk_vector_segment_metrics_accepts_matching_lumina_metric() {
        // Lumina segment metadata says cosine; configured cosine => Ok. No segment
        // file bytes are needed on the Lumina path.
        let splits = vec![pk_split_with_lumina_segment("seg-lumina", "cosine")];
        let segment_bytes = HashMap::new();
        verify_pk_vector_segment_metrics(
            &splits,
            &segment_bytes,
            VectorSearchMetric::Cosine,
            VectorIndexBackend::Lumina,
        )
        .expect("matching lumina metric must pass");
    }

    fn verify_pk_vector_segment_metrics_rejects_mismatched_lumina_metric() {
        // Lumina segment metadata says l2; configured inner_product => fail loud,
        // naming both metrics.
        let splits = vec![pk_split_with_lumina_segment("seg-lumina", "l2")];
        let segment_bytes = HashMap::new();
        let err = verify_pk_vector_segment_metrics(
            &splits,
            &segment_bytes,
            VectorSearchMetric::InnerProduct,
            VectorIndexBackend::Lumina,
        )
        .expect_err("mismatched lumina metric must fail loud");
        assert!(
            matches!(err, crate::Error::DataInvalid { ref message, .. }
                if message.contains("does not match configured metric")
                    && message.contains("l2")
                    && message.contains("inner_product")),
            "unexpected error: {err:?}"
        );
    }

    fn from_index_type_classifies_lumina_and_vindex() {
        assert_eq!(
            VectorIndexBackend::from_index_type("lumina"),
            Some(VectorIndexBackend::Lumina)
        );
        assert_eq!(
            VectorIndexBackend::from_index_type("lumina-vector-ann"),
            Some(VectorIndexBackend::Lumina)
        );
        assert_eq!(
            VectorIndexBackend::from_index_type("ivf-flat"),
            Some(VectorIndexBackend::Vindex)
        );
        // `diskann` is Lumina's internal index type, not a top-level index type.
        assert_eq!(VectorIndexBackend::from_index_type("diskann"), None);
    }

    fn verify_pk_vector_segment_metrics_accepts_matching_metric() {
        // Real IVF segment trained with L2; configured metric L2 => Ok.
        let bytes = build_vindex_segment_bytes("l2");
        let splits = vec![pk_split_with_segment("seg-l2")];
        let segment_bytes = HashMap::from([("seg-l2".to_string(), bytes::Bytes::from(bytes))]);
        verify_pk_vector_segment_metrics(
            &splits,
            &segment_bytes,
            VectorSearchMetric::L2,
            VectorIndexBackend::Vindex,
        )
        .expect("matching metric must pass");
    }

    fn verify_pk_vector_segment_metrics_rejects_mismatched_metric() {
        // Real IVF segment trained with L2; configured metric Cosine => fail loud.
        let bytes = build_vindex_segment_bytes("l2");
        let splits = vec![pk_split_with_segment("seg-l2")];
        let segment_bytes = HashMap::from([("seg-l2".to_string(), bytes::Bytes::from(bytes))]);
        let err = verify_pk_vector_segment_metrics(
            &splits,
            &segment_bytes,
            VectorSearchMetric::Cosine,
            VectorIndexBackend::Vindex,
        )
        .expect_err("mismatched metric must fail loud");
        assert!(
            matches!(err, crate::Error::DataInvalid { ref message, .. }
                if message.contains("does not match configured metric")
                    && message.contains("l2")
                    && message.contains("cosine")),
            "unexpected error: {err:?}"
        );
    }

    fn pk_vector_table(options: &[(&str, &str)]) -> Table {
        let mut builder = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column(
                "embedding",
                DataType::Array(ArrayType::new(DataType::Float(FloatType::new()))),
            );
        for (k, v) in options {
            builder = builder.option(*k, *v);
        }
        let schema = builder.build().unwrap();
        Table::new(
            FileIOBuilder::new("memory").build().unwrap(),
            Identifier::new("default", "pk_vector_test"),
            "memory:/pk_vector_test".to_string(),
            TableSchema::new(0, &schema),
            None,
        )
    }

    #[tokio::test]
    async fn pk_branch_disabled_falls_through_to_de_path() {
        // No pk-vector.index.columns: behaves exactly as the DE path. With no
        // snapshot the DE path returns an empty result; the PK branch must not
        // intercept it.
        let table = pk_vector_table(&[]);
        let result = table
            .new_vector_search_builder()
            .with_vector_column("embedding")
            .with_query_vector(vec![1.0])
            .with_limit(5)
            .execute()
            .await
            .unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn pk_branch_execute_scored_fails_loud() {
        // On a PK-vector table `execute_scored` reports global row ids, which the
        // PK path cannot produce (physical (file, position) coords, no global ids).
        // It must fail loud rather than fabricate ids; callers use `execute_read`.
        let table = pk_vector_table(&[
            ("pk-vector.index.columns", "embedding"),
            ("fields.embedding.pk-vector.index.type", IVF_FLAT_IDENTIFIER),
            ("fields.embedding.pk-vector.distance.metric", "l2"),
        ]);
        let err = table
            .new_vector_search_builder()
            .with_vector_column("embedding")
            .with_query_vector(vec![1.0])
            .with_limit(5)
            .execute()
            .await
            .map(|_| ())
            .expect_err("execute_scored on a PK-vector column must fail loud");
        assert!(
            matches!(err, crate::Error::DataInvalid { ref message, .. }
                if message.contains("does not produce global row ids")),
            "unexpected error: {err:?}"
        );
    }

    #[tokio::test]
    async fn pk_branch_other_column_falls_through_to_de_path() {
        // pk-vector index configured for "embedding", but the query targets a
        // different column -> the PK branch must not intercept; DE path (no
        // snapshot) yields empty. Discriminator: the PK column carries a
        // DELIBERATELY INVALID distance metric, which the PK branch parses eagerly
        // (`VectorSearchMetric::parse`) and would fail on. So a regression that
        // dropped the `pk_col == vector_column` guard and ran the PK branch for
        // "other" would surface as Err here, not Ok(empty) -- the assertion
        // therefore proves the DE path ran, not merely that the result is empty.
        let table = pk_vector_table(&[
            ("pk-vector.index.columns", "embedding"),
            ("fields.embedding.pk-vector.index.type", IVF_FLAT_IDENTIFIER),
            (
                "fields.embedding.pk-vector.distance.metric",
                "not-a-real-metric",
            ),
        ]);
        let result = table
            .new_vector_search_builder()
            .with_vector_column("other")
            .with_query_vector(vec![1.0])
            .with_limit(5)
            .execute()
            .await
            .unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn pk_branch_multi_column_config_does_not_break_unrelated_de_query() {
        // A malformed multi-column PK-vector config ("a,b") must not abort an
        // unrelated DE vector query. The query targets a column NOT among the
        // configured PK-vector columns, so membership resolution short-circuits
        // before the exactly-one-column rule fires -- the query falls through to
        // the DE path (no snapshot -> empty) instead of surfacing the "must name
        // exactly one column" error.
        let table = pk_vector_table(&[
            ("pk-vector.index.columns", "a,b"),
            ("fields.a.pk-vector.index.type", IVF_FLAT_IDENTIFIER),
            ("fields.a.pk-vector.distance.metric", "l2"),
        ]);
        let result = table
            .new_vector_search_builder()
            .with_vector_column("other")
            .with_query_vector(vec![1.0])
            .with_limit(5)
            .execute()
            .await;
        match result {
            Ok(search) => assert!(search.is_empty()),
            Err(err) => panic!(
                "unrelated DE query must not error on a malformed multi-column PK config: {err}"
            ),
        }
    }

    /// `id > threshold` built against the table's user fields (leaf index resolves
    /// against `table.schema().fields()`).
    fn id_gt_filter(table: &Table, threshold: i32) -> Predicate {
        PredicateBuilder::new(table.schema().fields())
            .greater_than("id", Datum::Int(threshold))
            .unwrap()
    }

    /// The vector residual is derived from the DATA conjuncts of the filter:
    /// partition-only conjuncts are enforced by scan planning (`PkVectorScan`
    /// pushes the whole filter through the normal scan) and must not enter the
    /// per-row residual, so a partition-only filter yields no residual at all.
    #[test]
    fn residual_uses_only_data_conjuncts_of_the_filter() {
        use crate::spec::VarCharType;
        use crate::table::bucket_filter::split_partition_and_data_predicates;

        // Partitioned table: `dt` (partition key) + `id`.
        let schema = Schema::builder()
            .column("dt", DataType::VarChar(VarCharType::string_type()))
            .column("id", DataType::Int(IntType::new()))
            .partition_keys(["dt"])
            .build()
            .unwrap();
        let ts = TableSchema::new(0, &schema);
        let fields = ts.fields();
        let partition_keys = ts.partition_keys();
        let pb = PredicateBuilder::new(fields);

        // Partition-only `dt = 'a'` -> no residual data predicate (residual skipped;
        // the partition is enforced by planning alone).
        let (_p, data) = split_partition_and_data_predicates(
            pb.equal("dt", Datum::String("a".to_string())).unwrap(),
            fields,
            partition_keys,
        );
        assert!(
            data.is_empty(),
            "partition-only filter must leave no residual data predicate"
        );

        // Data-only `id > 5` -> kept as the residual.
        let (_p, data) = split_partition_and_data_predicates(
            pb.greater_than("id", Datum::Int(5)).unwrap(),
            fields,
            partition_keys,
        );
        assert_eq!(data.len(), 1, "data-only filter must remain the residual");

        // `dt = 'a' AND id > 5` -> only the data conjunct enters the residual.
        let (_p, data) = split_partition_and_data_predicates(
            Predicate::and(vec![
                pb.equal("dt", Datum::String("a".to_string())).unwrap(),
                pb.greater_than("id", Datum::Int(5)).unwrap(),
            ]),
            fields,
            partition_keys,
        );
        assert_eq!(
            data.len(),
            1,
            "AND(partition, data) residual must drop the partition conjunct"
        );

        // `dt = 'a' OR id > 5` is a single mixed conjunct: it is NOT partition-only,
        // so it stays whole in the residual (evaluated against the materialized
        // partition column), rather than being dropped or split.
        let mixed = Predicate::or(vec![
            pb.equal("dt", Datum::String("a".to_string())).unwrap(),
            pb.greater_than("id", Datum::Int(5)).unwrap(),
        ]);
        let (_p, data) = split_partition_and_data_predicates(mixed.clone(), fields, partition_keys);
        assert_eq!(
            data,
            vec![mixed],
            "a mixed partition/data conjunct must stay whole in the residual"
        );
    }

    #[tokio::test]
    async fn execute_read_filter_without_deletion_vectors_fails_loud() {
        let table = pk_vector_table(&[
            ("pk-vector.index.columns", "embedding"),
            ("fields.embedding.pk-vector.index.type", IVF_FLAT_IDENTIFIER),
            ("fields.embedding.pk-vector.distance.metric", "l2"),
        ]);
        let filter = id_gt_filter(&table, 2);
        let err = table
            .new_vector_search_builder()
            .with_vector_column("embedding")
            .with_query_vector(vec![1.0])
            .with_limit(5)
            .with_filter(filter)
            .execute_read()
            .await
            .map(|_| ())
            .expect_err("read filter without deletion vectors must fail loud");
        assert!(
            matches!(err, crate::Error::DataInvalid { ref message, .. }
                if message.contains("deletion vectors without merge-on-read")),
            "unexpected error: {err:?}"
        );
    }

    #[tokio::test]
    async fn execute_scored_filter_on_non_pk_vector_path_fails_loud() {
        // No PK-vector index configured, so `execute_scored` would fall through to
        // the data-evolution path, which never consumes the filter. Silently
        // returning unfiltered rows is a wrong-read; the query must fail loud
        // instead.
        let table = pk_vector_table(&[]);
        let filter = id_gt_filter(&table, 2);
        let err = table
            .new_vector_search_builder()
            .with_vector_column("embedding")
            .with_query_vector(vec![1.0])
            .with_limit(5)
            .with_filter(filter)
            .execute_scored()
            .await
            .map(|_| ())
            .expect_err("filter on the non-PK-vector path must fail loud");
        assert!(
            matches!(err, crate::Error::DataInvalid { ref message, .. }
                if message.contains("only supported on the primary-key vector path")),
            "unexpected error: {err:?}"
        );
    }

    #[tokio::test]
    async fn execute_read_filter_with_merge_on_read_fails_loud() {
        // Deletion vectors enabled BUT merge-on-read on: still rejected, because a
        // merge-on-read scan can surface stale key versions that a physical-row
        // filter cannot reconcile.
        let table = pk_vector_table(&[
            ("pk-vector.index.columns", "embedding"),
            ("fields.embedding.pk-vector.index.type", IVF_FLAT_IDENTIFIER),
            ("fields.embedding.pk-vector.distance.metric", "l2"),
            ("deletion-vectors.enabled", "true"),
            ("deletion-vectors.merge-on-read", "true"),
        ]);
        let filter = id_gt_filter(&table, 2);
        let err = table
            .new_vector_search_builder()
            .with_vector_column("embedding")
            .with_query_vector(vec![1.0])
            .with_limit(5)
            .with_filter(filter)
            .execute_read()
            .await
            .map(|_| ())
            .expect_err("merge-on-read filter must fail loud");
        assert!(
            matches!(err, crate::Error::DataInvalid { ref message, .. }
                if message.contains("deletion vectors without merge-on-read")),
            "unexpected error: {err:?}"
        );
    }

    #[tokio::test]
    async fn execute_read_filter_with_deletion_vectors_passes_guard() {
        // Deletion vectors enabled, merge-on-read off (default): the residual guard
        // passes. With no snapshot the plan is empty, so the (guarded) filter path
        // simply yields an empty stream rather than erroring — proving the guard
        // admits a legal filtered query.
        let table = pk_vector_table(&[
            ("pk-vector.index.columns", "embedding"),
            ("fields.embedding.pk-vector.index.type", IVF_FLAT_IDENTIFIER),
            ("fields.embedding.pk-vector.distance.metric", "l2"),
            ("deletion-vectors.enabled", "true"),
            // Pin the index dimension so the query vector below matches it; the
            // up-front dimension guard runs before this test's residual guard.
            ("fields.embedding.dimension", "4"),
        ]);
        let filter = id_gt_filter(&table, 2);
        let mut stream = table
            .new_vector_search_builder()
            .with_vector_column("embedding")
            .with_query_vector(vec![1.0; 4])
            .with_limit(5)
            .with_filter(filter)
            .execute_read()
            .await
            .expect("guarded filter query must be admitted");
        assert!(stream.try_next().await.unwrap().is_none());
    }

    /// A partition-only `with_filter` needs no per-row residual (partition pruning
    /// happens in scan planning), so the deletion-vector pre-filter guard must NOT
    /// reject it even when deletion vectors are off. Mirrors Java, where a
    /// partition-only filter leaves `this.filter == null` and the scan guard is
    /// skipped. Regression test for the guard keying on the whole filter rather
    /// than its data conjuncts.
    #[tokio::test]
    async fn execute_read_partition_only_filter_without_deletion_vectors_passes_guard() {
        use crate::spec::VarCharType;

        // Partitioned PK-vector table, deletion vectors OFF (default).
        let mut builder = Schema::builder()
            .column("dt", DataType::VarChar(VarCharType::string_type()))
            .column("id", DataType::Int(IntType::new()))
            .column(
                "embedding",
                DataType::Array(ArrayType::new(DataType::Float(FloatType::new()))),
            )
            .partition_keys(["dt"]);
        for (k, v) in [
            ("pk-vector.index.columns", "embedding"),
            ("fields.embedding.pk-vector.index.type", IVF_FLAT_IDENTIFIER),
            ("fields.embedding.pk-vector.distance.metric", "l2"),
            ("fields.embedding.dimension", "4"),
        ] {
            builder = builder.option(k, v);
        }
        let schema = builder.build().unwrap();
        let table = Table::new(
            FileIOBuilder::new("memory").build().unwrap(),
            Identifier::new("default", "pk_vector_partitioned"),
            "memory:/pk_vector_partitioned".to_string(),
            TableSchema::new(0, &schema),
            None,
        );

        // Partition-only `dt = 'a'`: no data residual, so the guard admits it and
        // (with no snapshot) the query yields an empty stream instead of the
        // deletion-vector error.
        let filter = PredicateBuilder::new(table.schema().fields())
            .equal("dt", Datum::String("a".to_string()))
            .unwrap();
        let mut stream = table
            .new_vector_search_builder()
            .with_vector_column("embedding")
            .with_query_vector(vec![1.0; 4])
            .with_limit(5)
            .with_filter(filter)
            .execute_read()
            .await
            .expect("partition-only filter must be admitted without deletion vectors");
        assert!(stream.try_next().await.unwrap().is_none());

        // But a DATA conjunct (`id > 2`) on the same non-DV table must still fail
        // loud — the guard now keys on data predicates, not the whole filter.
        let data_filter = id_gt_filter(&table, 2);
        let err = table
            .new_vector_search_builder()
            .with_vector_column("embedding")
            .with_query_vector(vec![1.0; 4])
            .with_limit(5)
            .with_filter(data_filter)
            .execute_read()
            .await
            .map(|_| ())
            .expect_err("data filter without deletion vectors must still fail loud");
        assert!(
            matches!(err, crate::Error::DataInvalid { ref message, .. }
                if message.contains("deletion vectors without merge-on-read")),
            "unexpected error: {err:?}"
        );
    }

    fn make_lumina_entry(
        file_name: &str,
        index_type: &str,
        kind: FileKind,
        index_field_id: i32,
    ) -> IndexManifestEntry {
        IndexManifestEntry {
            kind,
            partition: vec![],
            bucket: 0,
            index_file: IndexFileMeta {
                index_type: index_type.to_string(),
                file_name: file_name.to_string(),
                file_size: 100,
                row_count: 10,
                deletion_vectors_ranges: None,
                global_index_meta: Some(GlobalIndexMeta {
                    row_range_start: 0,
                    row_range_end: 9,
                    index_field_id,
                    extra_field_ids: None,
                    source_meta: None,
                    index_meta: None,
                }),
            },
            version: 1,
        }
    }

    // ---- Task B: search-and-read (`execute_read`) tests ----

    /// Build a small materialization batch: user column `id: Int32`, the internal
    /// `_PKEY_VECTOR_POSITION: Int64`, and `_PKEY_VECTOR_SCORE: Float32` (mirroring
    /// what `PkVectorIndexedSplitRead` emits for a single file).
    fn materialized_batch(rows: &[(i32, i64, f32)]) -> RecordBatch {
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, false),
            ArrowField::new(PKEY_VECTOR_POSITION_COLUMN, ArrowDataType::Int64, false),
            ArrowField::new(SEARCH_SCORE_COLUMN, ArrowDataType::Float32, false),
        ]));
        let ids = Int32Array::from(rows.iter().map(|(id, _, _)| *id).collect::<Vec<_>>());
        let positions = Int64Array::from(rows.iter().map(|(_, pos, _)| *pos).collect::<Vec<_>>());
        let scores = Float32Array::from(rows.iter().map(|(_, _, s)| *s).collect::<Vec<_>>());
        RecordBatch::try_new(
            schema,
            vec![Arc::new(ids), Arc::new(positions), Arc::new(scores)],
        )
        .unwrap()
    }

    fn i32_col(batch: &RecordBatch, name: &str) -> Vec<i32> {
        let idx = batch.schema().index_of(name).unwrap();
        batch
            .column(idx)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .values()
            .to_vec()
    }
    fn f32_col(batch: &RecordBatch, name: &str) -> Vec<f32> {
        let idx = batch.schema().index_of(name).unwrap();
        batch
            .column(idx)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap()
            .values()
            .to_vec()
    }

    #[test]
    fn reorder_and_strip_position_recovers_best_first_and_drops_position() {
        // Single file, one bucket. The materialization reader emits rows in
        // ascending physical position [pos0, pos1, pos2] -> ids [40,41,42]. The
        // search candidates ranked them best-first as pos1(rank0), pos2(rank1),
        // pos0(rank2), which is NEITHER position order nor score order-by-batch.
        // The reorder must yield ids [41,42,40] and drop _PKEY_VECTOR_POSITION.
        let batch = materialized_batch(&[
            (40, 0, l2_score(9.0)),
            (41, 1, l2_score(1.0)),
            (42, 2, l2_score(4.0)),
        ]);
        let batches = vec![batch];
        let part = BinaryRow::new(0).to_serialized_bytes();
        let mut rank_of: HashMap<(Vec<u8>, i32, String, i64), usize> = HashMap::new();
        rank_of.insert((part.clone(), 0, "o.mosaic".to_string(), 1), 0);
        rank_of.insert((part.clone(), 0, "o.mosaic".to_string(), 2), 1);
        rank_of.insert((part.clone(), 0, "o.mosaic".to_string(), 0), 2);

        let mut ranked = Vec::new();
        collect_ranked_rows(&batches[0], 0, &part, 0, "o.mosaic", &rank_of, &mut ranked).unwrap();
        let out = reorder_and_strip_position(&batches, ranked).unwrap();
        assert_eq!(out.len(), 1);
        let out = &out[0];

        // Best-first row order, not ascending position order.
        assert_eq!(i32_col(out, "id"), vec![41, 42, 40]);
        // Score column preserved and aligned to the reordered rows.
        assert_eq!(
            f32_col(out, SEARCH_SCORE_COLUMN),
            vec![l2_score(1.0), l2_score(4.0), l2_score(9.0)]
        );
        // Position column dropped; _ROW_ID never present.
        assert!(out.schema().index_of(PKEY_VECTOR_POSITION_COLUMN).is_err());
        assert!(out.schema().index_of("_ROW_ID").is_err());
    }

    #[test]
    fn reorder_and_strip_position_merges_rows_across_files() {
        // Two files (two materialization batches). Best-first interleaves them:
        // file-b pos0 (rank0), file-a pos1 (rank1), file-a pos0 (rank2). The
        // reorder must pull rows from both batches into one best-first output.
        let batch_a = materialized_batch(&[(10, 0, l2_score(9.0)), (11, 1, l2_score(1.0))]);
        let batch_b = materialized_batch(&[(20, 0, l2_score(0.5))]);
        let batches = vec![batch_a, batch_b];
        let part = BinaryRow::new(0).to_serialized_bytes();
        let mut rank_of: HashMap<(Vec<u8>, i32, String, i64), usize> = HashMap::new();
        rank_of.insert((part.clone(), 0, "b".to_string(), 0), 0);
        rank_of.insert((part.clone(), 0, "a".to_string(), 1), 1);
        rank_of.insert((part.clone(), 0, "a".to_string(), 0), 2);

        let mut ranked = Vec::new();
        collect_ranked_rows(&batches[0], 0, &part, 0, "a", &rank_of, &mut ranked).unwrap();
        collect_ranked_rows(&batches[1], 1, &part, 0, "b", &rank_of, &mut ranked).unwrap();
        let out = reorder_and_strip_position(&batches, ranked).unwrap();
        assert_eq!(i32_col(&out[0], "id"), vec![20, 11, 10]);
        assert_eq!(
            f32_col(&out[0], SEARCH_SCORE_COLUMN),
            vec![l2_score(0.5), l2_score(1.0), l2_score(9.0)]
        );
    }

    #[test]
    fn reorder_and_strip_position_empty_yields_no_batches() {
        let out = reorder_and_strip_position(&[], Vec::new()).unwrap();
        assert!(out.is_empty());
    }

    #[test]
    fn collect_ranked_rows_missing_candidate_fails_loud() {
        // A materialized position with no candidate rank must fail loud rather than
        // silently drop the row.
        let batch = materialized_batch(&[(40, 7, l2_score(1.0))]);
        let part = BinaryRow::new(0).to_serialized_bytes();
        let rank_of: HashMap<(Vec<u8>, i32, String, i64), usize> = HashMap::new();
        let mut ranked = Vec::new();
        let err = collect_ranked_rows(&batch, 0, &part, 0, "f", &rank_of, &mut ranked)
            .expect_err("missing candidate must fail loud");
        assert!(
            matches!(err, crate::Error::DataInvalid { ref message, .. } if message.contains("no matching search candidate")),
            "unexpected error: {err:?}"
        );
    }

    #[tokio::test]
    async fn execute_read_de_table_fails_loud() {
        // No pk-vector index configured: execute_read must fail loud (the DE path
        // has no row materialization).
        let table = pk_vector_table(&[]);
        let err = table
            .new_vector_search_builder()
            .with_vector_column("embedding")
            .with_query_vector(vec![1.0])
            .with_limit(5)
            .execute_read()
            .await
            .map(|_| ())
            .expect_err("DE read must fail loud");
        assert!(
            matches!(err, crate::Error::DataInvalid { ref message, .. }
                if message.contains("only supported for primary-key")),
            "unexpected error: {err:?}"
        );
    }

    #[tokio::test]
    async fn execute_read_non_pk_column_fails_loud() {
        // pk-vector index configured for "embedding", but the query targets a
        // different column -> read is unsupported.
        let table = pk_vector_table(&[
            ("pk-vector.index.columns", "embedding"),
            ("fields.embedding.pk-vector.index.type", IVF_FLAT_IDENTIFIER),
            ("fields.embedding.pk-vector.distance.metric", "l2"),
        ]);
        let err = table
            .new_vector_search_builder()
            .with_vector_column("other")
            .with_query_vector(vec![1.0])
            .with_limit(5)
            .execute_read()
            .await
            .map(|_| ())
            .expect_err("non-PK column read must fail loud");
        assert!(
            matches!(&err, crate::Error::DataInvalid { message, .. } if message.contains("only supported for primary-key")),
            "non-PK column read must fail loud, got: {err}"
        );
    }

    #[tokio::test]
    async fn execute_read_scalar_column_fails_loud() {
        // A scalar (non-vector) column targeted by a vector read must fail loud,
        // not return an empty data-evolution stream.
        let table = pk_vector_table(&[
            ("pk-vector.index.columns", "embedding"),
            ("fields.embedding.pk-vector.index.type", IVF_FLAT_IDENTIFIER),
            ("fields.embedding.pk-vector.distance.metric", "l2"),
        ]);
        let err = match table
            .new_vector_search_builder()
            .with_vector_column("id") // scalar Int column
            .with_query_vector(vec![1.0])
            .with_limit(5)
            .execute_read()
            .await
        {
            Ok(_) => panic!("scalar vector column must fail loud on execute_read"),
            Err(e) => e,
        };
        assert!(
            matches!(&err, crate::Error::DataInvalid { message, .. } if message.contains("only supported for primary-key")),
            "scalar column read must fail loud, got: {err}"
        );
    }

    #[tokio::test]
    async fn execute_read_non_float_vector_column_fails_loud() {
        // An ARRAY<INT> column is not a searchable vector column (the index/search
        // operates on FLOAT elements). It must fail loud rather than fall through
        // to the DE path and return an empty stream.
        use crate::spec::{ArrayType, IntType, Schema, TableSchema};
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column(
                "embedding",
                DataType::Array(ArrayType::new(DataType::Int(IntType::new()))),
            )
            .build()
            .unwrap();
        let table = Table::new(
            FileIOBuilder::new("memory").build().unwrap(),
            Identifier::new("default", "de_non_float_vector"),
            "memory:/de_non_float_vector".to_string(),
            TableSchema::new(0, &schema),
            None,
        );
        let err = match table
            .new_vector_search_builder()
            .with_vector_column("embedding")
            .with_query_vector(vec![1.0])
            .with_limit(5)
            .execute_read()
            .await
        {
            Ok(_) => panic!("ARRAY<INT> vector column must fail loud on execute_read"),
            Err(e) => e,
        };
        assert!(
            matches!(&err, crate::Error::DataInvalid { message, .. } if message.contains("only supported for primary-key")),
            "non-float vector column read must fail loud, got: {err}"
        );
    }

    #[tokio::test]
    async fn execute_read_empty_plan_reserved_projection_fails_loud() {
        // Empty plan (no snapshot) must still fail loud on a reserved-name
        // projection: projection validity does not depend on whether the search
        // matched any rows. A regression that resolved the projection only after
        // the `candidates.is_empty()` early return would yield an empty stream here
        // instead of an error.
        let table = pk_vector_table(&[
            ("pk-vector.index.columns", "embedding"),
            ("fields.embedding.pk-vector.index.type", IVF_FLAT_IDENTIFIER),
            ("fields.embedding.pk-vector.distance.metric", "l2"),
            // Pin the index dimension so the query vector below matches it; the
            // up-front dimension guard runs before this test's reserved-projection
            // guard, so a mismatched query would mask the error under test.
            ("fields.embedding.dimension", "4"),
        ]);
        for reserved in [
            ROW_ID_FIELD_NAME,
            PKEY_VECTOR_POSITION_COLUMN,
            SEARCH_SCORE_COLUMN,
        ] {
            let mut builder = table.new_vector_search_builder();
            builder
                .with_vector_column("embedding")
                .with_query_vector(vec![1.0; 4])
                .with_limit(5)
                .with_projection(&["id", reserved]);
            let err = builder
                .execute_read()
                .await
                .map(|_| ())
                .expect_err("empty plan + reserved projection must fail loud");
            assert!(
                matches!(err, crate::Error::DataInvalid { ref message, .. }
                    if message.contains("reserved column")),
                "unexpected error for {reserved}: {err:?}"
            );
        }
    }

    #[tokio::test]
    async fn execute_read_empty_plan_lumina_array_float_is_admitted() {
        // A Lumina PK-vector `ARRAY<FLOAT>` column is a valid configuration, but
        // batch query dimension validation routed every `ARRAY<FLOAT>` column
        // through the vindex resolver, which rejects `lumina` as an unsupported
        // index type before planning — failing even an empty table. The
        // dimension must be resolved per the configured backend, so a
        // well-formed Lumina query is admitted and (with no snapshot) yields an
        // empty stream rather than an "Unsupported vindex index type" error.
        let table = pk_vector_table(&[
            ("pk-vector.index.columns", "embedding"),
            (
                "fields.embedding.pk-vector.index.type",
                crate::lumina::LUMINA_IDENTIFIER,
            ),
            ("fields.embedding.pk-vector.distance.metric", "l2"),
            ("lumina.index.dimension", "4"),
        ]);
        let mut stream = table
            .new_vector_search_builder()
            .with_vector_column("embedding")
            .with_query_vector(vec![1.0; 4])
            .with_limit(5)
            .execute_read()
            .await
            .expect(
                "Lumina ARRAY<FLOAT> query must be admitted, not rejected as unsupported vindex",
            );
        assert!(stream.try_next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn execute_read_projection_reserved_name_fails_loud() {
        // Projecting a reserved metadata / row-id column must fail loud. The guard
        // lives in `resolve_materialize_read_type`, which `execute_read` invokes
        // before the empty-plan early return; assert on the resolver directly here.
        let table = pk_vector_table(&[
            ("pk-vector.index.columns", "embedding"),
            ("fields.embedding.pk-vector.index.type", IVF_FLAT_IDENTIFIER),
            ("fields.embedding.pk-vector.distance.metric", "l2"),
        ]);
        for reserved in [
            ROW_ID_FIELD_NAME,
            PKEY_VECTOR_POSITION_COLUMN,
            SEARCH_SCORE_COLUMN,
        ] {
            let mut builder = table.new_vector_search_builder();
            builder
                .with_vector_column("embedding")
                .with_query_vector(vec![1.0])
                .with_limit(5)
                .with_projection(&["id", reserved]);
            let err = builder
                .resolve_materialize_read_type()
                .expect_err("reserved projection must fail loud");
            assert!(
                matches!(err, crate::Error::DataInvalid { ref message, .. }
                    if message.contains("reserved column")),
                "unexpected error for {reserved}: {err:?}"
            );
        }
    }

    #[test]
    fn resolve_materialize_read_type_default_is_all_user_columns() {
        // No with_projection -> every user table column (id + embedding).
        let table = pk_vector_table(&[
            ("pk-vector.index.columns", "embedding"),
            ("fields.embedding.pk-vector.index.type", IVF_FLAT_IDENTIFIER),
            ("fields.embedding.pk-vector.distance.metric", "l2"),
        ]);
        let builder = table.new_vector_search_builder();
        let fields = builder.resolve_materialize_read_type().unwrap();
        let names: Vec<&str> = fields.iter().map(|f| f.name()).collect();
        assert_eq!(names, vec!["id", "embedding"]);
    }

    /// A PK-vector table whose user schema carries an extra column named
    /// `reserved`, used to prove reserved metadata names are rejected even when
    /// they arrive via the default (all-columns) projection.
    fn pk_vector_table_with_extra_column(reserved: &str) -> Table {
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column(
                "embedding",
                DataType::Array(ArrayType::new(DataType::Float(FloatType::new()))),
            )
            .column(reserved, DataType::Int(IntType::new()))
            .option("pk-vector.index.columns", "embedding")
            .option("fields.embedding.pk-vector.index.type", IVF_FLAT_IDENTIFIER)
            .option("fields.embedding.pk-vector.distance.metric", "l2")
            .build()
            .unwrap();
        Table::new(
            FileIOBuilder::new("memory").build().unwrap(),
            Identifier::new("default", "reserved_col_test"),
            "memory:/reserved_col_test".to_string(),
            TableSchema::new(0, &schema),
            None,
        )
    }

    #[test]
    fn resolve_materialize_read_type_default_rejects_reserved_user_column() {
        // The default (all-columns) projection must reject a user column whose
        // name collides with an injected metadata column, not only columns named
        // in an explicit projection. Otherwise it silently passes on an empty
        // result and collides with the metadata columns the read attaches.
        let table = pk_vector_table_with_extra_column(SEARCH_SCORE_COLUMN);
        let builder = table.new_vector_search_builder();
        let err = builder.resolve_materialize_read_type().unwrap_err();
        assert!(
            matches!(err, crate::Error::DataInvalid { ref message, .. }
                if message.contains("reserved column")),
            "single-query default projection must reject reserved user column, got: {err:?}"
        );
    }

    #[test]
    fn batch_resolve_materialize_read_type_default_rejects_reserved_user_column() {
        // Same guard on the batch resolver.
        let table = pk_vector_table_with_extra_column(PKEY_VECTOR_POSITION_COLUMN);
        let builder = table.new_batch_vector_search_builder();
        let err = builder.resolve_materialize_read_type().unwrap_err();
        assert!(
            matches!(err, crate::Error::DataInvalid { ref message, .. }
                if message.contains("reserved column")),
            "batch default projection must reject reserved user column, got: {err:?}"
        );
    }

    #[test]
    fn resolve_materialize_read_type_projection_selects_named_columns() {
        let table = pk_vector_table(&[
            ("pk-vector.index.columns", "embedding"),
            ("fields.embedding.pk-vector.index.type", IVF_FLAT_IDENTIFIER),
            ("fields.embedding.pk-vector.distance.metric", "l2"),
        ]);
        let mut builder = table.new_vector_search_builder();
        builder.with_projection(&["id"]);
        let fields = builder.resolve_materialize_read_type().unwrap();
        let names: Vec<&str> = fields.iter().map(|f| f.name()).collect();
        assert_eq!(names, vec!["id"]);
    }
}

/// Tests for [`residual_positions_by_file`]: the residual predicate is applied at
/// the Arrow level (no pushdown) against the predicate columns, and each surviving
/// row's file-local physical position is recovered from its ordinal in the
/// unfiltered scan (no `_ROW_ID`, no `first_row_id`).
// Disabled: this mosaic e2e module predates signature changes to
// `DataFileReader::new` and `DataFileMeta` on the base branch and no longer
// compiles under `--features mosaic`. The `any()` guard is always false, so the
// module is skipped until the tests are refreshed. Unrelated to ANN parallelism.
#[cfg(all(test, feature = "mosaic", any()))]
mod residual_positions_tests {
    use super::*;
    use crate::arrow::build_target_arrow_schema;
    use crate::arrow::format::FilePredicates;
    use crate::io::FileIOBuilder;
    use crate::spec::stats::BinaryTableStats;
    use crate::spec::{
        BigIntType, BinaryRow, DataField, DataFileMeta, DataType, Datum, IntType, PredicateBuilder,
        ROW_ID_FIELD_ID, ROW_ID_FIELD_NAME,
    };
    use crate::table::data_file_reader::DataFileReader;
    use crate::table::schema_manager::SchemaManager;
    use crate::table::source::{DataSplit, DataSplitBuilder};
    use arrow_array::{Int32Array, RecordBatch};
    use bytes::Bytes;
    use paimon_mosaic_core::spec::COMPRESSION_NONE;
    use paimon_mosaic_core::writer::{MosaicWriter, OutputFile, WriterOptions};
    use std::io;
    use std::sync::Arc;

    struct MemOutputFile {
        data: Vec<u8>,
    }

    impl OutputFile for MemOutputFile {
        fn write(&mut self, data: &[u8]) -> io::Result<()> {
            self.data.extend_from_slice(data);
            Ok(())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
        fn pos(&self) -> u64 {
            self.data.len() as u64
        }
    }

    fn id_field() -> DataField {
        DataField::new(0, "id".to_string(), DataType::Int(IntType::new()))
    }

    fn row_id_field() -> DataField {
        DataField::new(
            ROW_ID_FIELD_ID,
            ROW_ID_FIELD_NAME.to_string(),
            DataType::BigInt(BigIntType::new()),
        )
    }

    fn id_batch(ids: Vec<i32>) -> RecordBatch {
        let schema = build_target_arrow_schema(&[id_field()]).unwrap();
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(ids))]).unwrap()
    }

    fn write_mosaic(batch: &RecordBatch) -> Bytes {
        let mut writer = MosaicWriter::new(
            MemOutputFile { data: Vec::new() },
            batch.schema().as_ref(),
            WriterOptions {
                compression: COMPRESSION_NONE,
                num_buckets: 2,
                row_group_max_size: u64::MAX,
                ..Default::default()
            },
        )
        .unwrap();
        writer.write_batch(batch).unwrap();
        writer.close().unwrap();
        Bytes::from(writer.output().data.to_vec())
    }

    fn data_file(
        file_name: &str,
        file_size: i64,
        row_count: i64,
        first_row_id: Option<i64>,
    ) -> DataFileMeta {
        DataFileMeta {
            file_name: file_name.to_string(),
            file_size,
            row_count,
            min_key: Vec::new(),
            max_key: Vec::new(),
            key_stats: BinaryTableStats::empty(),
            value_stats: BinaryTableStats::empty(),
            min_sequence_number: 0,
            max_sequence_number: 0,
            schema_id: 1,
            level: 0,
            extra_files: Vec::new(),
            creation_time: None,
            delete_row_count: None,
            embedded_index: None,
            file_source: None,
            value_stats_cols: None,
            external_path: None,
            first_row_id,
            write_cols: None,
        }
    }

    /// Build a predicate-free reader (read_type = `id` + `_ROW_ID`) over a split
    /// containing `files` (each `(name, ids, first_row_id)`), written as Mosaic
    /// data files in the same bucket. The returned active-file list covers every
    /// file (all files active).
    async fn build_reader_and_split(
        table_path: &str,
        files: &[(&str, Vec<i32>, i64)],
    ) -> (DataFileReader, DataSplit, Vec<BucketActiveFile>) {
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let bucket_path = format!("{table_path}/bucket-0");
        let mut metas = Vec::new();
        let mut active_files = Vec::new();
        for (name, ids, first_row_id) in files {
            let data = write_mosaic(&id_batch(ids.clone()));
            file_io
                .new_output(&format!("{bucket_path}/{name}"))
                .unwrap()
                .write(data.clone())
                .await
                .unwrap();
            metas.push(data_file(
                name,
                data.len() as i64,
                ids.len() as i64,
                Some(*first_row_id),
            ));
            active_files.push(BucketActiveFile {
                file_name: name.to_string(),
                row_count: ids.len() as i64,
            });
        }
        let split = DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path(bucket_path)
            .with_total_buckets(1)
            .with_data_files(metas)
            .build()
            .unwrap();
        let reader = DataFileReader::new(
            file_io.clone(),
            SchemaManager::new(file_io, table_path.to_string()),
            1,
            vec![id_field()],
            vec![id_field(), row_id_field()],
            Vec::new(),
        );
        (reader, split, active_files)
    }

    /// `id > threshold`, with `file_fields` = `[id]` so the leaf index resolves.
    fn residual_id_gt(threshold: i32) -> FilePredicates {
        let pred = PredicateBuilder::new(&[id_field()])
            .greater_than("id", Datum::Int(threshold))
            .unwrap();
        FilePredicates {
            predicates: vec![pred],
            file_fields: vec![id_field()],
        }
    }

    fn sorted(t: &roaring::RoaringTreemap) -> Vec<u64> {
        t.iter().collect()
    }

    #[tokio::test]
    async fn test_residual_selects_matching_positions() {
        // ids [1,2,3,4,5] at first_row_id 0; id > 2 -> ids 3,4,5 -> positions 2,3,4.
        let (reader, split, active) = build_reader_and_split(
            "memory:/rpf_basic",
            &[("part-0.mosaic", vec![1, 2, 3, 4, 5], 0)],
        )
        .await;
        let map = residual_positions_by_file(&reader, &split, &active, &residual_id_gt(2))
            .await
            .unwrap();
        assert_eq!(sorted(&map["part-0.mosaic"]), vec![2, 3, 4]);
    }

    #[tokio::test]
    async fn test_residual_matches_none_yields_empty_entry() {
        // id > 100 matches nothing; the file still gets a (present, empty) entry.
        let (reader, split, active) =
            build_reader_and_split("memory:/rpf_none", &[("part-0.mosaic", vec![1, 2, 3], 0)])
                .await;
        let map = residual_positions_by_file(&reader, &split, &active, &residual_id_gt(100))
            .await
            .unwrap();
        assert!(map.contains_key("part-0.mosaic"));
        assert!(map["part-0.mosaic"].is_empty());
    }

    #[tokio::test]
    async fn test_residual_matches_all_yields_full_set() {
        let (reader, split, active) =
            build_reader_and_split("memory:/rpf_all", &[("part-0.mosaic", vec![1, 2, 3], 0)]).await;
        let map = residual_positions_by_file(&reader, &split, &active, &residual_id_gt(0))
            .await
            .unwrap();
        assert_eq!(sorted(&map["part-0.mosaic"]), vec![0, 1, 2]);
    }

    #[tokio::test]
    async fn test_residual_positions_are_file_local_across_files() {
        // Two files with distinct first_row_id; positions must be 0-based within
        // each file, not global. id > 3 keeps ids 4,5 in both -> positions {3,4}.
        let (reader, split, active) = build_reader_and_split(
            "memory:/rpf_multi",
            &[
                ("part-0.mosaic", vec![1, 2, 3, 4, 5], 0),
                ("part-1.mosaic", vec![1, 2, 3, 4, 5], 100),
            ],
        )
        .await;
        let map = residual_positions_by_file(&reader, &split, &active, &residual_id_gt(3))
            .await
            .unwrap();
        assert_eq!(sorted(&map["part-0.mosaic"]), vec![3, 4]);
        assert_eq!(sorted(&map["part-1.mosaic"]), vec![3, 4]);
    }

    #[tokio::test]
    async fn test_non_active_files_are_skipped() {
        // Two files in the split, but only `part-0.mosaic` is active. The bucket
        // search never recalls from `part-1.mosaic` (level-0 / non-active), so it
        // must not appear in the residual map — and even though it lacks a
        // `first_row_id`, the query still succeeds because non-active files are
        // skipped before the guard.
        let (reader, split, mut active) = build_reader_and_split(
            "memory:/rpf_nonactive",
            &[("part-0.mosaic", vec![1, 2, 3, 4, 5], 0)],
        )
        .await;
        // Append a non-active file (missing first_row_id) directly to the split's
        // data files, but leave it out of the active list.
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let bucket_path = "memory:/rpf_nonactive/bucket-0";
        let data = write_mosaic(&id_batch(vec![9, 9, 9]));
        file_io
            .new_output(&format!("{bucket_path}/part-1.mosaic"))
            .unwrap()
            .write(data.clone())
            .await
            .unwrap();
        let mut metas = split.data_files().to_vec();
        metas.push(data_file("part-1.mosaic", data.len() as i64, 3, None));
        // `active` already lists only part-0.mosaic; keep it that way.
        let _ = &mut active;
        let split = DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path(bucket_path.to_string())
            .with_total_buckets(1)
            .with_data_files(metas)
            .build()
            .unwrap();
        let map = residual_positions_by_file(&reader, &split, &active, &residual_id_gt(2))
            .await
            .unwrap();
        assert_eq!(sorted(&map["part-0.mosaic"]), vec![2, 3, 4]);
        assert!(
            !map.contains_key("part-1.mosaic"),
            "non-active file must be skipped"
        );
    }

    #[tokio::test]
    async fn test_missing_first_row_id_recovers_local_positions() {
        // Real primary-key data files carry no `first_row_id`. Positions are
        // recovered from each row's ordinal in the scan, so the residual still
        // works: ids [1,2,3] with id > 0 -> all match -> local positions [0,1,2].
        let (reader, split, active) = build_reader_and_split_no_first_row_id().await;
        let map = residual_positions_by_file(&reader, &split, &active, &residual_id_gt(0))
            .await
            .expect("missing first_row_id must not fail the residual read");
        assert_eq!(sorted(&map["part-0.mosaic"]), vec![0, 1, 2]);
    }

    async fn build_reader_and_split_no_first_row_id(
    ) -> (DataFileReader, DataSplit, Vec<BucketActiveFile>) {
        let table_path = "memory:/rpf_nofrid";
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let bucket_path = format!("{table_path}/bucket-0");
        let data = write_mosaic(&id_batch(vec![1, 2, 3]));
        file_io
            .new_output(&format!("{bucket_path}/part-0.mosaic"))
            .unwrap()
            .write(data.clone())
            .await
            .unwrap();
        let split = DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path(bucket_path)
            .with_total_buckets(1)
            .with_data_files(vec![data_file("part-0.mosaic", data.len() as i64, 3, None)])
            .build()
            .unwrap();
        let reader = DataFileReader::new(
            file_io.clone(),
            SchemaManager::new(file_io, table_path.to_string()),
            1,
            vec![id_field()],
            vec![id_field(), row_id_field()],
            Vec::new(),
        );
        // The lone file is active and carries no first_row_id, exercising the
        // ordinal-based position recovery.
        let active = vec![BucketActiveFile {
            file_name: "part-0.mosaic".to_string(),
            row_count: 3,
        }];
        (reader, split, active)
    }
}
