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
use crate::arrow::residual::{filter_record_batch_by_predicates, widen_scan_fields};
use crate::lumina::is_lumina_index_type;
use crate::lumina::reader::LuminaVectorGlobalIndexReader;
use crate::spec::{
    BigIntType, CoreOptions, DataField, DataType, FileKind, GlobalIndexSearchMode, IndexManifest,
    Predicate, ROW_ID_FIELD_ID, ROW_ID_FIELD_NAME,
};
use crate::table::data_file_reader::DataFileReader;
use crate::table::pk_vector_data_file_reader::DataFilePkVectorReaderFactory;
use crate::table::pk_vector_indexed_split_read::PkVectorIndexedSplitRead;
use crate::table::pk_vector_orchestrator::{
    build_indexed_splits, validate_row_position, PkVectorCandidate, PkVectorOrchestrator,
    PkVectorSearchSplit,
};
use crate::table::pk_vector_position_read::{
    PKEY_VECTOR_POSITION_COLUMN, PKEY_VECTOR_SCORE_COLUMN,
};
use crate::table::pk_vector_scan::{PkVectorScan, PkVectorScanPlan};
use crate::table::snapshot_manager::SnapshotManager;
use crate::table::source::DataSplit;
use crate::table::{
    find_field_id_by_name, merge_row_ranges, ArrowRecordBatchStream, RowRange, Table,
};
use crate::vector_search::{GlobalIndexIOMeta, SearchResult, VectorSearch};
use crate::vindex::is_vindex_index_type;
use crate::vindex::pkvector::ann::VindexAnnSearcher;
use crate::vindex::pkvector::bucket::{covered_source_files, BucketActiveFile, BucketAnnSegment};
use crate::vindex::pkvector::metric::VectorSearchMetric;
use crate::vindex::pkvector::reader::PkVectorReader;
use crate::vindex::reader::VindexVectorGlobalIndexReader;
use arrow_array::{Int64Array, RecordBatch};
use arrow_select::interleave::interleave_record_batch;
use futures::{stream, TryStreamExt};
use paimon_vindex_core::index::VectorIndexReader as VIndexReader;
use roaring::RoaringTreemap;
use std::collections::{HashMap, HashSet};
use std::io::Cursor;

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
        // Membership is resolved first via the non-erroring columns accessor so a
        // malformed PK-vector config (e.g. more than one column, or a blank list)
        // cannot abort an unrelated DE query. The exactly-one-column rule is
        // enforced only once this query is known to target a PK-vector column,
        // keeping fail-loud behavior for a genuinely-broken config on the path
        // where erroring is correct.
        //
        // This is the search-only path: the PK branch runs the search (not the
        // row-materialization path in `execute_read`) and returns its scored hits.
        let core = CoreOptions::new(self.table.schema().options());
        if core.primary_key_vector_index_enabled() {
            let targets_pk_column = core
                .primary_key_vector_index_columns()
                .ok()
                .is_some_and(|cols| cols.iter().any(|c| c == vector_column));
            if targets_pk_column {
                let pk_col = core.primary_key_vector_index_column()?;
                return self
                    .execute_primary_key_vector_search(&core, &pk_col, query_vector, limit)
                    .await;
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
                    .execute_primary_key_vector_read(&core, &pk_col, query_vector, limit)
                    .await;
            }
        }

        Err(crate::Error::DataInvalid {
            message: "vector search read is only supported for primary-key vector indexes".into(),
            source: None,
        })
    }

    /// Run the primary-key bucket-local vector search: plan the per-bucket splits,
    /// build the real vindex ANN scorer and (outside FAST mode) the exact-fallback
    /// readers, run the orchestrator, and convert the best-first candidates into a
    /// `SearchResult`. Mirrors Java `PrimaryKeyVectorRead`.
    async fn execute_primary_key_vector_search(
        &self,
        core: &CoreOptions<'_>,
        pk_col: &str,
        query_vector: &[f32],
        limit: usize,
    ) -> crate::Result<SearchResult> {
        let (candidates, plan, metric) = self
            .plan_and_search_pk_candidates(core, pk_col, query_vector, limit)
            .await?;
        candidates_to_search_result(&candidates, &plan.splits, metric)
    }

    /// Shared PK-vector search core for both the search-only and search-and-read
    /// paths: plan the per-bucket splits, verify the configured metric against each
    /// ANN segment, build the real vindex ANN scorer and (outside FAST mode) the
    /// exact-fallback readers, and run the orchestrator. Returns the best-first
    /// candidates together with the plan and resolved metric so the caller can
    /// either serialize them to a `SearchResult` or materialize their rows. An
    /// empty plan yields empty candidates.
    async fn plan_and_search_pk_candidates(
        &self,
        core: &CoreOptions<'_>,
        pk_col: &str,
        query_vector: &[f32],
        limit: usize,
    ) -> crate::Result<(Vec<PkVectorCandidate>, PkVectorScanPlan, VectorSearchMetric)> {
        // Residual pre-filter guard, mirroring Java `PrimaryKeyVectorScan`. A data
        // predicate set via `with_filter` is applied post-recall by re-reading
        // each candidate file's physical rows (see below). That physical-position
        // filtering only agrees with the bucket search when the table exposes
        // physical rows directly: deletion vectors enabled and merge-on-read
        // disabled. Under merge-on-read (or without deletion vectors) a read
        // merges multiple key versions, so a scalar filter could retain a stale
        // version whose live version does not match — a silent wrong-read. Reject
        // such queries rather than answer them incorrectly. No filter → nothing to
        // guard, so the search-only and read paths are unaffected.
        let physical_row_read =
            core.deletion_vectors_enabled() && !core.deletion_vectors_merge_on_read();
        if self.filter.is_some() && !physical_row_read {
            return Err(crate::Error::DataInvalid {
                message:
                    "primary-key vector pre-filter requires deletion vectors without merge-on-read"
                        .to_string(),
                source: None,
            });
        }
        // `primary_key_vector_distance_metric` returns a validated name; re-parse
        // into the enum for the numeric semantics.
        let metric = VectorSearchMetric::parse(&core.primary_key_vector_distance_metric(pk_col)?)?;
        let index_type = core.primary_key_vector_index_type(pk_col)?;
        let field_id =
            find_field_id_by_name(self.table.schema().fields(), pk_col).ok_or_else(|| {
                crate::Error::DataInvalid {
                    message: format!("PK-vector column '{pk_col}' not found in schema"),
                    source: None,
                }
            })?;
        let vector_field = self
            .table
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

        let plan = PkVectorScan::new(self.table, field_id, index_type, self.filter.clone())
            .plan()
            .await?;
        if plan.splits.is_empty() {
            return Ok((Vec::new(), plan, metric));
        }

        // Production data-file reader, mirroring `table_read.rs::new_data_file_reader`
        // but projecting only the vector column with no predicates.
        let reader = DataFileReader::new(
            self.table.file_io().clone(),
            self.table.schema_manager().clone(),
            self.table.schema().id(),
            self.table.schema().fields().to_vec(),
            vec![vector_field.clone()],
            Vec::new(),
            core.read_batch_size(),
            core.parquet_page_index_enabled(),
            core.parquet_bloom_filter_enabled(),
        );

        // Real ANN scorer: preload each segment's bytes (keyed by resolved,
        // globally unique path) and drive the vindex reader from memory.
        let segment_bytes = preload_segment_bytes(self.table.file_io(), &plan.splits).await?;
        // Fail loud on a config/segment metric mismatch before scoring, mirroring
        // Java `PkVectorAnnSegmentSearcher.search`.
        verify_pk_vector_segment_metrics(&plan.splits, &segment_bytes, metric)?;
        // kwai's builder carries no per-search options override, so the ANN reader
        // is driven with the table options directly.
        let options = self.table.schema().options().clone();
        let search_options = options.clone();
        let field_name = pk_col.to_string();
        let scorer: crate::vindex::pkvector::ann::Scorer =
            Box::new(move |segment: &BucketAnnSegment, search: &VectorSearch| {
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
                let mut reader = VindexVectorGlobalIndexReader::new(io_meta, options.clone());
                reader.visit_vector_search(search, |_| Ok(Cursor::new(data)))
            });
        let ann_searcher = VindexAnnSearcher::new(field_name, scorer);

        // Residual (post-recall) filtering: for each candidate file, re-read its
        // physical rows and keep the positions whose rows satisfy the filter. The
        // per-split allow-list is threaded into the bucket search so the residual
        // folds into recall (best-first order and Top-K are preserved). Built only
        // when a filter is set; otherwise `None` leaves the search unfiltered. The
        // residual reader projects the predicate columns plus `_ROW_ID` (used to
        // recover file-local physical positions) and carries no pushdown, matching
        // `residual_positions_by_file`. Computed before the exact-reader preload so
        // the preload can skip files the residual allow-list leaves empty.
        let residual_by_split: Option<Vec<HashMap<String, RoaringTreemap>>> = match &self.filter {
            Some(filter) => {
                let file_predicates = FilePredicates {
                    predicates: vec![filter.clone()],
                    file_fields: self.table.schema().fields().to_vec(),
                };
                let row_id_field = DataField::new(
                    ROW_ID_FIELD_ID,
                    ROW_ID_FIELD_NAME.to_string(),
                    DataType::BigInt(BigIntType::new()),
                );
                let residual_read_type =
                    widen_scan_fields(std::slice::from_ref(&row_id_field), Some(&file_predicates));
                let residual_reader = DataFileReader::new(
                    self.table.file_io().clone(),
                    self.table.schema_manager().clone(),
                    self.table.schema().id(),
                    self.table.schema().fields().to_vec(),
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
            None => None,
        };

        // Exact-fallback readers, keyed by (split_index, file_name). In FAST mode
        // the kernel never invokes the factory, so skip the in-memory column read
        // entirely. Otherwise preload only the *uncovered* active files: files an
        // ANN segment already covers never reach the exact fallback, so reading
        // their vector column here would be wasted IO/memory. Mirrors Java, which
        // creates a `PkVectorReader` lazily only for uncovered files. When a
        // residual filter leaves a file's allow-list empty (or absent) the bucket
        // search skips it, so its reader is not preloaded either.
        let mut exact_readers: HashMap<(usize, String), Box<dyn PkVectorReader>> = HashMap::new();
        if !skip_exact_fallback {
            for (split_index, split) in plan.splits.iter().enumerate() {
                let covered = covered_source_files(&split.ann_segments, &split.active_files);
                let factory = DataFilePkVectorReaderFactory::new(
                    reader.clone(),
                    split.data_split.clone(),
                    vector_field.clone(),
                )?;
                for active in &split.active_files {
                    if covered.contains(&active.file_name) {
                        continue;
                    }
                    if !should_preload_exact_reader(
                        residual_by_split.as_deref(),
                        split_index,
                        &active.file_name,
                    ) {
                        continue;
                    }
                    let r = factory.create(active).await?;
                    exact_readers.insert((split_index, active.file_name.clone()), r);
                }
            }
        }
        let mut factory = |split_index: usize,
                           _split: &PkVectorSearchSplit,
                           file: &BucketActiveFile|
         -> crate::Result<Box<dyn PkVectorReader>> {
            exact_readers
                .remove(&(split_index, file.file_name.clone()))
                .ok_or_else(|| crate::Error::DataInvalid {
                    message: format!("no preloaded exact reader for {}", file.file_name),
                    source: None,
                })
        };

        let candidates = PkVectorOrchestrator::new(reader)
            .search_candidates(
                &plan.splits,
                query_vector,
                metric,
                limit,
                Some(&ann_searcher),
                &mut factory,
                &search_options,
                skip_exact_fallback,
                residual_by_split.as_deref(),
            )
            .await?;

        Ok((candidates, plan, metric))
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
    ) -> crate::Result<ArrowRecordBatchStream> {
        let (candidates, plan, metric) = self
            .plan_and_search_pk_candidates(core, pk_col, query_vector, limit)
            .await?;

        // Resolve the materialization read-type up front so an invalid projection
        // (unknown column, or a reserved metadata / row-id name) fails loud
        // unconditionally, even when the plan is empty and no rows will be read.
        // Default (no `with_projection`) is every user table column.
        let read_type = self.resolve_materialize_read_type()?;

        if candidates.is_empty() {
            return Ok(Box::pin(stream::empty()));
        }

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
                    if name == PKEY_VECTOR_POSITION_COLUMN
                        || name == PKEY_VECTOR_SCORE_COLUMN
                        || name == ROW_ID_FIELD_NAME
                    {
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

/// Whether the exact-fallback reader for `file_name` in split `split_index`
/// should be preloaded. With a residual filter, a file absent from the split's
/// allow-list or with an empty allow-list has no candidate rows, so the bucket
/// search skips it and preloading its vector column would be wasted IO.
fn should_preload_exact_reader(
    residual_by_split: Option<&[HashMap<String, RoaringTreemap>]>,
    split_index: usize,
    file_name: &str,
) -> bool {
    match residual_by_split {
        None => true,
        Some(per_split) => per_split
            .get(split_index)
            .and_then(|m| m.get(file_name))
            .is_some_and(|allowed| !allowed.is_empty()),
    }
}

/// Compute, per data file in `split`, the set of physical row positions whose
/// rows satisfy the residual predicate. Mirrors the row-collecting half of Java
/// `PrimaryKeyVectorRead`'s `executeFilter`: because
/// [`DataFileReader::read_single_file_stream`] rejects projecting `_ROW_ID`
/// alongside a row-filtering predicate (the residual filter would drop rows
/// before `_ROW_ID` is assigned positionally, desyncing it), the predicate is
/// NOT pushed down. Instead `reader` projects the residual columns together with
/// `_ROW_ID` and carries no pushdown predicate; the residual is applied here at
/// the Arrow level, after `_ROW_ID` is materialized, and each surviving row's
/// `_ROW_ID - first_row_id` is the file-local physical position.
///
/// Every *active* data file in the split gets an entry, possibly empty. The
/// bucket search treats an absent entry and an empty entry identically (the file
/// contributes no candidates), so the empty entries only make the map cover every
/// active file. Non-active files (e.g. level-0 files the bucket search excludes)
/// are skipped entirely: they are never searched, so re-reading them would be
/// wasted IO and their possibly-absent `first_row_id` must not fail an otherwise
/// valid query.
///
/// `reader` must project `_ROW_ID` and be predicate-free; `residual.file_fields`
/// are the fields the residual leaf indices point into (resolved by name against
/// each emitted batch). A data file without `first_row_id` fails loud, matching
/// the position-read guard.
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
        // positions; skip everything else so a non-active file cannot trigger the
        // `first_row_id` guard below or incur a wasted read.
        if !active_names.contains(file_meta.file_name.as_str()) {
            continue;
        }
        let first_row_id = file_meta
            .first_row_id
            .ok_or_else(|| crate::Error::DataInvalid {
                message: format!(
                    "residual position read requires data file '{}' to have first_row_id",
                    file_meta.file_name
                ),
                source: None,
            })?;
        let data_fields = reader.derive_data_fields(file_meta).await?;
        let mut stream =
            reader.read_single_file_stream(split, file_meta.clone(), data_fields, None, None)?;
        // Register the file up front so a file whose rows all fail the residual
        // still appears in the map (empty set).
        let positions = out.entry(file_meta.file_name.clone()).or_default();
        while let Some(batch) = stream.try_next().await? {
            let filtered = filter_record_batch_by_predicates(batch, residual, &scan_fields)?;
            if filtered.num_rows() == 0 {
                continue;
            }
            let row_id_idx = filtered.schema().index_of(ROW_ID_FIELD_NAME).map_err(|_| {
                crate::Error::DataInvalid {
                    message: "residual position read batch is missing the _ROW_ID column"
                        .to_string(),
                    source: None,
                }
            })?;
            let row_ids = filtered
                .column(row_id_idx)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| crate::Error::DataInvalid {
                    message: "residual position read _ROW_ID column is not Int64".to_string(),
                    source: None,
                })?;
            for i in 0..row_ids.len() {
                let position = row_ids.value(i) - first_row_id;
                let position = u64::try_from(position).map_err(|_| crate::Error::DataInvalid {
                    message: format!(
                        "residual position {position} is negative for data file '{}'",
                        file_meta.file_name
                    ),
                    source: None,
                })?;
                positions.insert(position);
            }
        }
    }
    Ok(out)
}

/// Preload every ANN segment's bytes into a map keyed by the resolved (globally
/// unique) segment path. The scorer closure reads from this map so the vindex
/// reader is driven from memory without per-search IO.
async fn preload_segment_bytes(
    file_io: &crate::io::FileIO,
    splits: &[PkVectorSearchSplit],
) -> crate::Result<HashMap<String, Vec<u8>>> {
    let mut out = HashMap::new();
    for split in splits {
        for segment in &split.ann_segments {
            if out.contains_key(&segment.path) {
                continue;
            }
            let input = file_io.new_input(&segment.path)?;
            let bytes = input.read().await.map_err(|e| crate::Error::DataInvalid {
                message: format!("failed to read ANN index file '{}': {e}", segment.path),
                source: None,
            })?;
            out.insert(segment.path.clone(), bytes.to_vec());
        }
    }
    Ok(out)
}

/// Fail loud when an ANN segment was trained with a metric other than the
/// configured one, mirroring the search-time `checkArgument` in Java
/// `PkVectorAnnSegmentSearcher.search`. Opens each distinct segment's preloaded
/// bytes once and compares its trained metric against `configured`.
fn verify_pk_vector_segment_metrics(
    splits: &[PkVectorSearchSplit],
    segment_bytes: &HashMap<String, Vec<u8>>,
    configured: VectorSearchMetric,
) -> crate::Result<()> {
    let mut checked: HashSet<&str> = HashSet::new();
    for split in splits {
        for segment in &split.ann_segments {
            if !checked.insert(segment.path.as_str()) {
                continue;
            }
            let bytes =
                segment_bytes
                    .get(&segment.path)
                    .ok_or_else(|| crate::Error::DataInvalid {
                        message: format!(
                            "missing preloaded ANN bytes for segment '{}'",
                            segment.path
                        ),
                        source: None,
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
            let segment_metric = reader.metadata().metric;
            if VectorSearchMetric::from_vindex(segment_metric) != configured {
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

/// Convert best-first candidates to a `SearchResult`, preserving the input
/// candidate order (no re-sort). Each candidate's global row id is
/// `first_row_id + row_position` of the data file it references; the score is
/// derived from the raw distance via the metric. A candidate referencing a file
/// absent from its split, or a file with no `first_row_id`, fails loud.
fn candidates_to_search_result(
    candidates: &[PkVectorCandidate],
    splits: &[PkVectorSearchSplit],
    metric: VectorSearchMetric,
) -> crate::Result<SearchResult> {
    let mut row_ids = Vec::with_capacity(candidates.len());
    let mut scores = Vec::with_capacity(candidates.len());
    for c in candidates {
        let split = splits
            .get(c.split_index)
            .ok_or_else(|| crate::Error::DataInvalid {
                message: format!("candidate split_index {} out of range", c.split_index),
                source: None,
            })?;
        let file_meta = split
            .data_split
            .data_files()
            .iter()
            .find(|f| f.file_name == c.data_file_name)
            .ok_or_else(|| crate::Error::DataInvalid {
                message: format!(
                    "candidate references data file {} not present in its split",
                    c.data_file_name
                ),
                source: None,
            })?;
        let first_row_id = file_meta
            .first_row_id
            .ok_or_else(|| crate::Error::DataInvalid {
                message: format!("data file {} has no first_row_id", c.data_file_name),
                source: None,
            })?;
        validate_row_position(&c.data_file_name, c.row_position, file_meta.row_count)?;
        let global =
            first_row_id
                .checked_add(c.row_position)
                .ok_or_else(|| crate::Error::DataInvalid {
                    message: "global row id overflows i64".to_string(),
                    source: None,
                })?;
        row_ids.push(
            u64::try_from(global).map_err(|_| crate::Error::DataInvalid {
                message: format!("negative global row id {global}"),
                source: None,
            })?,
        );
        scores.push(metric.distance_to_score(c.distance));
    }
    // Order preserved: best-first, as produced by the orchestrator.
    Ok(SearchResult::new(row_ids, scores))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::Identifier;
    use crate::io::FileIOBuilder;
    use crate::lumina::{LEGACY_LUMINA_VECTOR_ANN_IDENTIFIER, LUMINA_IDENTIFIER};
    use crate::spec::stats::BinaryTableStats;
    use crate::spec::{
        ArrayType, BinaryRow, DataFileMeta, DataType, Datum, FloatType, GlobalIndexMeta,
        IndexFileMeta, IndexManifestEntry, IntType, PredicateBuilder, Schema, TableSchema,
    };
    use crate::table::source::DataSplitBuilder;
    use crate::vindex::IVF_FLAT_IDENTIFIER;
    use arrow_array::{Float32Array, Int32Array};
    use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
    use std::sync::Arc;

    fn l2_score(distance: f32) -> f32 {
        VectorSearchMetric::L2.distance_to_score(distance)
    }

    fn make_field(id: i32, name: &str) -> DataField {
        DataField::new(id, name.to_string(), DataType::Int(IntType::default()))
    }

    #[test]
    fn test_find_field_id_by_name() {
        let fields = vec![make_field(1, "id"), make_field(2, "embedding")];
        assert_eq!(find_field_id_by_name(&fields, "embedding"), Some(2));
        assert_eq!(find_field_id_by_name(&fields, "nonexistent"), None);
    }

    #[test]
    fn should_preload_skips_empty_or_absent_residual_files() {
        use roaring::RoaringTreemap;
        use std::collections::HashMap;
        // No filter -> always preload.
        assert!(should_preload_exact_reader(None, 0, "f0"));
        // Filter present: file with a non-empty allow-list -> preload.
        let mut m0: HashMap<String, RoaringTreemap> = HashMap::new();
        m0.insert("f0".to_string(), RoaringTreemap::from_iter([0u64]));
        let per_split = vec![m0];
        assert!(should_preload_exact_reader(Some(&per_split), 0, "f0"));
        // Filter present: file absent -> skip.
        assert!(!should_preload_exact_reader(Some(&per_split), 0, "missing"));
        // Filter present: file with empty allow-list -> skip.
        let mut m1: HashMap<String, RoaringTreemap> = HashMap::new();
        m1.insert("f1".to_string(), RoaringTreemap::new());
        let per_split2 = vec![m1];
        assert!(!should_preload_exact_reader(Some(&per_split2), 0, "f1"));
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

    #[test]
    fn candidates_to_search_result_global_row_id_and_best_first_order() {
        // Two files in one split with different first_row_id. The helper is a pure
        // order-preserving map: the orchestrator already established best-first
        // order upstream, so the candidate INPUT order here is deliberately NOT in
        // score order and NOT in (file, position) order. This proves the helper
        // preserves the given sequence rather than sorting.
        let splits = vec![pk_search_split(
            0,
            vec![
                pk_data_file("file-a", 100, Some(1000)),
                pk_data_file("file-b", 100, Some(5000)),
            ],
        )];
        // Input sequence (NOT sorted by score, NOT sorted by file/position):
        //   c0: file-b pos5 d=2.0  -> WORST distance, appears FIRST
        //   c1: file-b pos1 d=1.0  -> tie with c2
        //   c2: file-a pos2 d=1.0  -> tie with c1
        // A score-based best-first re-sort would produce [c1, c2, c0] (worst last);
        // a (file, position) re-sort would produce [c2 (file-a), c1, c0]. Both
        // differ from the input order, so the exact assertion below discriminates.
        let candidates = vec![
            pk_candidate(0, 0, "file-b", 5, 2.0),
            pk_candidate(0, 0, "file-b", 1, 1.0),
            pk_candidate(0, 0, "file-a", 2, 1.0),
        ];
        let result = candidates_to_search_result(&candidates, &splits, VectorSearchMetric::L2)
            .expect("conversion succeeds");
        // global_row_id = first_row_id + position; INPUT order preserved (not sorted).
        assert_eq!(result.row_ids, vec![5005, 5001, 1002]);
        assert_eq!(
            result.scores,
            vec![
                VectorSearchMetric::L2.distance_to_score(2.0),
                VectorSearchMetric::L2.distance_to_score(1.0),
                VectorSearchMetric::L2.distance_to_score(1.0),
            ]
        );
    }

    #[test]
    fn candidates_to_search_result_absent_first_row_id_fails_loud() {
        let splits = vec![pk_search_split(0, vec![pk_data_file("file-a", 100, None)])];
        let candidates = vec![pk_candidate(0, 0, "file-a", 0, 1.0)];
        let err = candidates_to_search_result(&candidates, &splits, VectorSearchMetric::L2)
            .expect_err("absent first_row_id must fail loud");
        assert!(
            matches!(err, crate::Error::DataInvalid { ref message, .. } if message.contains("first_row_id")),
            "unexpected error: {err:?}"
        );
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

    #[test]
    fn verify_pk_vector_segment_metrics_accepts_matching_metric() {
        // Real IVF segment trained with L2; configured metric L2 => Ok.
        let bytes = build_vindex_segment_bytes("l2");
        let splits = vec![pk_split_with_segment("seg-l2")];
        let segment_bytes = HashMap::from([("seg-l2".to_string(), bytes)]);
        verify_pk_vector_segment_metrics(&splits, &segment_bytes, VectorSearchMetric::L2)
            .expect("matching metric must pass");
    }

    #[test]
    fn verify_pk_vector_segment_metrics_rejects_mismatched_metric() {
        // Real IVF segment trained with L2; configured metric Cosine => fail loud.
        let bytes = build_vindex_segment_bytes("l2");
        let splits = vec![pk_split_with_segment("seg-l2")];
        let segment_bytes = HashMap::from([("seg-l2".to_string(), bytes)]);
        let err =
            verify_pk_vector_segment_metrics(&splits, &segment_bytes, VectorSearchMetric::Cosine)
                .expect_err("mismatched metric must fail loud");
        assert!(
            matches!(err, crate::Error::DataInvalid { ref message, .. }
                if message.contains("does not match configured metric")
                    && message.contains("l2")
                    && message.contains("cosine")),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn candidates_to_search_result_missing_file_fails_loud() {
        let splits = vec![pk_search_split(
            0,
            vec![pk_data_file("known", 100, Some(0))],
        )];
        let candidates = vec![pk_candidate(0, 0, "unknown", 0, 1.0)];
        let err = candidates_to_search_result(&candidates, &splits, VectorSearchMetric::L2)
            .expect_err("missing file must fail loud");
        assert!(matches!(err, crate::Error::DataInvalid { .. }));
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
    async fn pk_branch_enabled_empty_plan_returns_empty() {
        // pk-vector.index.columns set, but no snapshot -> empty plan -> empty result.
        let table = pk_vector_table(&[
            ("pk-vector.index.columns", "embedding"),
            ("fields.embedding.pk-vector.index.type", IVF_FLAT_IDENTIFIER),
            ("fields.embedding.pk-vector.distance.metric", "l2"),
        ]);
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

    #[tokio::test]
    async fn pk_branch_filter_without_deletion_vectors_fails_loud() {
        // A residual filter on a PK-vector table that does NOT enable deletion
        // vectors must be rejected (merge-on-read semantics would make physical
        // -position filtering unsound). Mirrors Java `PrimaryKeyVectorScan`.
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
            .execute_scored()
            .await
            .map(|_| ())
            .expect_err("filter without deletion vectors must fail loud");
        assert!(
            matches!(err, crate::Error::DataInvalid { ref message, .. }
                if message.contains("deletion vectors without merge-on-read")),
            "unexpected error: {err:?}"
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
    async fn pk_branch_filter_with_merge_on_read_fails_loud() {
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
            .execute_scored()
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
    async fn pk_branch_filter_with_deletion_vectors_passes_guard() {
        // Deletion vectors enabled, merge-on-read off (default): the residual guard
        // passes. With no snapshot the plan is empty, so the (guarded) filter path
        // simply yields an empty result rather than erroring — proving the guard
        // admits a legal filtered query.
        let table = pk_vector_table(&[
            ("pk-vector.index.columns", "embedding"),
            ("fields.embedding.pk-vector.index.type", IVF_FLAT_IDENTIFIER),
            ("fields.embedding.pk-vector.distance.metric", "l2"),
            ("deletion-vectors.enabled", "true"),
        ]);
        let filter = id_gt_filter(&table, 2);
        let result = table
            .new_vector_search_builder()
            .with_vector_column("embedding")
            .with_query_vector(vec![1.0])
            .with_limit(5)
            .with_filter(filter)
            .execute_scored()
            .await
            .expect("guarded filter query must be admitted");
        assert!(result.is_empty());
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
            ArrowField::new(PKEY_VECTOR_SCORE_COLUMN, ArrowDataType::Float32, false),
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
            f32_col(out, PKEY_VECTOR_SCORE_COLUMN),
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
            f32_col(&out[0], PKEY_VECTOR_SCORE_COLUMN),
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
            matches!(err, crate::Error::DataInvalid { ref message, .. }
                if message.contains("only supported for primary-key")),
            "unexpected error: {err:?}"
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
        ]);
        for reserved in [
            ROW_ID_FIELD_NAME,
            PKEY_VECTOR_POSITION_COLUMN,
            PKEY_VECTOR_SCORE_COLUMN,
        ] {
            let mut builder = table.new_vector_search_builder();
            builder
                .with_vector_column("embedding")
                .with_query_vector(vec![1.0])
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
            PKEY_VECTOR_SCORE_COLUMN,
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
/// the Arrow level (no pushdown) against the predicate columns plus `_ROW_ID`,
/// and each surviving row's `_ROW_ID` is converted back to a file-local physical
/// position.
#[cfg(all(test, feature = "mosaic"))]
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
    async fn test_missing_first_row_id_is_error() {
        let (reader, split, active) = build_reader_and_split_no_first_row_id().await;
        let err = residual_positions_by_file(&reader, &split, &active, &residual_id_gt(0))
            .await
            .expect_err("missing first_row_id must error");
        assert!(format!("{err:?}").contains("first_row_id"), "got: {err:?}");
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
        // The lone file is active, so the `first_row_id` guard applies to it.
        let active = vec![BucketActiveFile {
            file_name: "part-0.mosaic".to_string(),
            row_count: 3,
        }];
        (reader, split, active)
    }
}
