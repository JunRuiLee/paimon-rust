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

use crate::arrow::build_target_arrow_schema;
use crate::arrow::format::create_format_reader;
use crate::arrow::schema_evolution::{create_index_mapping, NULL_FIELD_INDEX};
use crate::deletion_vector::{DeletionVector, DeletionVectorFactory};
use crate::io::FileIO;
use crate::spec::{DataField, DataFileMeta, Predicate, RowKind, VALUE_KIND_FIELD_ID};
use crate::table::schema_manager::SchemaManager;
use crate::table::ArrowRecordBatchStream;
use crate::table::RowRange;
use crate::{DataSplit, Error};
use arrow_array::{Array, BooleanArray, Int64Array, Int8Array, RecordBatch};
use arrow_cast::cast;
use arrow_schema::Schema as ArrowSchema;
use arrow_select::filter::filter_record_batch;

use async_stream::try_stream;
use futures::StreamExt;
use std::sync::Arc;

/// Reads data from Parquet files.
#[derive(Clone)]
pub(crate) struct DataFileReader {
    file_io: FileIO,
    schema_manager: SchemaManager,
    table_schema_id: i64,
    table_fields: Vec<DataField>,
    read_type: Vec<DataField>,
    predicates: Vec<Predicate>,
    blob_as_descriptor: bool,
    /// When true, batches are post-filtered to keep only `RowKind::is_add()`
    /// rows (mirrors Java `DropDeleteReader`). Caller MUST include the
    /// `_VALUE_KIND` field in `read_type`; the column is consumed by the
    /// filter and dropped from the yielded batch. Defaults to `false`.
    drop_deletes: bool,
    /// Rows per batch passed to the format reader. Sourced from
    /// `CoreOptions::read_batch_size()` by callers.
    batch_size: usize,
    /// Whether the parquet format reader should load page index
    /// (ColumnIndex / OffsetIndex) and apply page-level pruning. Sourced
    /// from `CoreOptions::parquet_page_index_enabled()` by callers; ignored
    /// for non-parquet file formats.
    parquet_page_index_enabled: bool,
    /// Whether the parquet format reader should consult bloom filters for
    /// Eq / In leaf predicates. Sourced from
    /// `CoreOptions::parquet_bloom_filter_enabled()` by callers.
    parquet_bloom_filter_enabled: bool,
}

impl DataFileReader {
    pub(crate) fn new(
        file_io: FileIO,
        schema_manager: SchemaManager,
        table_schema_id: i64,
        table_fields: Vec<DataField>,
        read_type: Vec<DataField>,
        predicates: Vec<Predicate>,
        batch_size: usize,
        parquet_page_index_enabled: bool,
        parquet_bloom_filter_enabled: bool,
    ) -> Self {
        Self {
            file_io,
            schema_manager,
            table_schema_id,
            table_fields,
            read_type,
            predicates,
            blob_as_descriptor: false,
            drop_deletes: false,
            batch_size,
            parquet_page_index_enabled,
            parquet_bloom_filter_enabled,
        }
    }

    pub(crate) fn with_blob_as_descriptor(mut self, blob_as_descriptor: bool) -> Self {
        self.blob_as_descriptor = blob_as_descriptor;
        self
    }

    /// Enable post-decode `RowKind::is_add()` filter on yielded batches
    /// (mirrors Java `DropDeleteReader`).
    ///
    /// **Caller contract**: when `drop_deletes=true`, the `read_type` passed
    /// to [`new`] MUST include the `_VALUE_KIND` field
    /// ([`crate::spec::VALUE_KIND_FIELD_ID`], `TinyInt`); the column is
    /// consumed by the filter and dropped from the yielded batch. If the
    /// field is missing, [`read_single_file_stream`] returns an error
    /// instead of silently keeping every row.
    ///
    /// Used by Stage 4a of `dv-impl-plan.md` (PK raw-read short-circuit) to
    /// strip residual DELETE / UPDATE_BEFORE physical rows that the
    /// sort-merge path would otherwise drop via
    /// [`crate::spec::RowKind::is_add`]. Other DataFileReader callers
    /// (KV reader, data evolution, append, system tables) keep the default
    /// `false` so their `_VALUE_KIND` semantics stay untouched.
    pub(crate) fn with_drop_deletes(mut self, drop_deletes: bool) -> Self {
        self.drop_deletes = drop_deletes;
        self
    }

    /// Take a stream of DataSplits and read every data file in each split.
    /// Returns a stream of Arrow RecordBatches from all files.
    ///
    /// Uses SchemaManager to load the data file's schema (via `DataFileMeta.schema_id`)
    /// and computes field-ID-based index mapping for schema evolution (added columns,
    /// type promotion, column reordering).
    ///
    /// Matches [RawFileSplitRead.createReader](https://github.com/apache/paimon/blob/master/paimon-core/src/main/java/org/apache/paimon/operation/RawFileSplitRead.java).
    pub fn read(self, data_splits: &[DataSplit]) -> crate::Result<ArrowRecordBatchStream> {
        let splits: Vec<DataSplit> = data_splits.to_vec();
        let reader = self;
        Ok(try_stream! {
            for split in splits {
                // Build a per-split DV factory when any data file has a
                // DeletionFile attached. Construction is sync; the actual
                // DV blob IO + decode happens lazily inside the per-file
                // loop below so peak memory is one DV at a time.
                let dv_factory = if split
                    .data_deletion_files()
                    .is_some_and(|files| files.iter().any(Option::is_some))
                {
                    Some(DeletionVectorFactory::new(
                        &reader.file_io,
                        split.data_files(),
                        split.data_deletion_files(),
                    ))
                } else {
                    None
                };

                for file_meta in split.data_files().to_vec() {
                    // Lazy DV resolve — Arc moves into the file stream and
                    // drops when the stream ends.
                    let dv = match dv_factory.as_ref() {
                        Some(factory) => {
                            factory.get_deletion_vector(&file_meta.file_name).await?
                        }
                        None => None,
                    };

                    // Load data file's schema if it differs from the table schema.
                    let data_fields: Option<Vec<DataField>> = if file_meta.schema_id != reader.table_schema_id {
                        let data_schema = reader.schema_manager.schema(file_meta.schema_id).await?;
                        Some(data_schema.fields().to_vec())
                    } else {
                        None
                    };

                    let mut stream = reader.read_single_file_stream(
                        &split,
                        file_meta,
                        data_fields,
                        dv,
                        None,
                    )?;
                    while let Some(batch) = stream.next().await {
                        let batch = batch?;
                        // Explicit per-query accounting of the in-flight batch
                        // (the cdylib has no global allocator). append-only holds
                        // one batch at a time; charge it for the yield window and
                        // release when the next batch arrives or the stream ends.
                        // Runs on the tagged scanner thread; no-op without a tag.
                        // NOTE: only this append-only entry accounts here — MOR
                        // drives read_single_file_stream directly and accounts in
                        // sort_merge instead, so batches are never double-counted.
                        let _batch_mem = crate::mem_tag::ScopedBytes::new(batch.get_array_memory_size() as i64);
                        yield batch;
                    }
                }
            }
        }
        .boxed())
    }

    /// Read a single parquet file from a split, returning a lazy stream of batches.
    /// Optionally applies a deletion vector.
    ///
    /// Handles schema evolution using field-ID-based index mapping:
    /// - `data_fields`: if `Some`, the fields from the data file's schema (loaded via SchemaManager).
    ///   Used to compute index mapping between `read_type` and data fields by field ID.
    /// - Columns missing from the file are filled with null arrays.
    /// - Columns whose Arrow type differs from the target type are cast (type promotion).
    ///
    /// Reference: [RawFileSplitRead.createFileReader](https://github.com/apache/paimon/blob/release-1.3/paimon-core/src/main/java/org/apache/paimon/operation/RawFileSplitRead.java)
    pub(super) fn read_single_file_stream(
        &self,
        split: &DataSplit,
        file_meta: DataFileMeta,
        data_fields: Option<Vec<DataField>>,
        dv: Option<Arc<DeletionVector>>,
        row_ranges: Option<Vec<RowRange>>,
    ) -> crate::Result<ArrowRecordBatchStream> {
        let read_type = self.read_type.clone();
        let table_fields = self.table_fields.clone();
        let predicates = self.predicates.clone();
        let file_io = self.file_io.clone();
        let split = split.clone();
        let batch_size = self.batch_size;
        let blob_as_descriptor = self.blob_as_descriptor;
        let drop_deletes = self.drop_deletes;
        let parquet_page_index_enabled = self.parquet_page_index_enabled;
        let parquet_bloom_filter_enabled = self.parquet_bloom_filter_enabled;

        let target_schema = build_target_arrow_schema(&read_type)?;

        // When `drop_deletes` is enabled, find the `_VALUE_KIND` column once
        // up front and pre-build the user-visible output schema (without that
        // column). Caller contract: read_type MUST contain `_VALUE_KIND`.
        let drop_deletes_ctx: Option<(usize, Arc<ArrowSchema>)> = if drop_deletes {
            let vk_idx = read_type
                .iter()
                .position(|f| f.id() == VALUE_KIND_FIELD_ID)
                .ok_or_else(|| Error::DataInvalid {
                    message: "DataFileReader::with_drop_deletes(true) requires _VALUE_KIND in read_type"
                        .to_string(),
                    source: None,
                })?;
            let output_fields: Vec<arrow_schema::FieldRef> = target_schema
                .fields()
                .iter()
                .enumerate()
                .filter(|(i, _)| *i != vk_idx)
                .map(|(_, f)| f.clone())
                .collect();
            let output_schema = Arc::new(ArrowSchema::new(output_fields));
            Some((vk_idx, output_schema))
        } else {
            None
        };

        let file_fields = data_fields.clone().unwrap_or_else(|| table_fields.clone());

        // Compute index mapping and determine which columns to read from the file.
        let (projected_read_fields, index_mapping) = if let Some(ref df) = data_fields {
            let mapping = create_index_mapping(&read_type, df);
            match mapping {
                Some(ref idx_map) => {
                    let mut seen = std::collections::HashSet::new();
                    let fields_to_read: Vec<DataField> = idx_map
                        .iter()
                        .filter(|&&idx| idx != NULL_FIELD_INDEX && seen.insert(idx))
                        .map(|&idx| df[idx as usize].clone())
                        .collect();
                    (fields_to_read, Some(idx_map.clone()))
                }
                None => (df.clone(), None),
            }
        } else {
            (read_type.clone(), None)
        };

        // Remap predicates from table-level to file-level indices.
        let file_predicates = {
            let remapped = crate::arrow::filtering::remap_predicates_to_file(
                &predicates,
                &table_fields,
                &file_fields,
            );
            if remapped.is_empty() {
                None
            } else {
                Some(crate::arrow::format::FilePredicates {
                    predicates: remapped,
                    file_fields: file_fields.clone(),
                })
            }
        };

        Ok(try_stream! {
            let path_to_read = split.data_file_path(&file_meta);
            let format_reader = create_format_reader(
                &path_to_read,
                blob_as_descriptor,
                parquet_page_index_enabled,
                parquet_bloom_filter_enabled,
            )?;
            let input_file = file_io.new_input(&path_to_read)?;
            let file_reader = input_file.reader().await?;
            let local_ranges = row_ranges.as_ref().map(|ranges| {
                to_local_row_ranges(ranges, file_meta.first_row_id.unwrap_or(0), file_meta.row_count)
            });

            let row_selection = merge_row_selection(
                file_meta.row_count,
                dv.as_deref(),
                local_ranges.as_deref(),
            );

            let mut batch_stream = format_reader.read_batch_stream(
                Box::new(file_reader),
                file_meta.file_size as u64,
                &projected_read_fields,
                file_predicates.as_ref(),
                Some(batch_size),
                row_selection,
            ).await?;

            while let Some(batch) = batch_stream.next().await {
                let batch = batch?;
                let num_rows = batch.num_rows();
                let batch_schema = batch.schema();

                // Build output columns using index mapping (field-ID-based) or by name.
                let mut columns: Vec<Arc<dyn arrow_array::Array>> = Vec::with_capacity(target_schema.fields().len());
                for (i, target_field) in target_schema.fields().iter().enumerate() {
                    let source_col = if let Some(ref idx_map) = index_mapping {
                        let data_idx = idx_map[i];
                        if data_idx == NULL_FIELD_INDEX {
                            None
                        } else {
                            let data_field = &data_fields.as_ref().unwrap()[data_idx as usize];
                            batch_schema
                                .index_of(data_field.name())
                                .ok()
                                .map(|col_idx| batch.column(col_idx))
                        }
                    } else if let Some(ref df) = data_fields {
                        batch_schema
                            .index_of(df[i].name())
                            .ok()
                            .map(|col_idx| batch.column(col_idx))
                    } else {
                        batch_schema
                            .index_of(target_field.name())
                            .ok()
                            .map(|col_idx| batch.column(col_idx))
                    };

                    match source_col {
                        Some(col) => {
                            if col.data_type() == target_field.data_type() {
                                columns.push(col.clone());
                            } else {
                                let casted = cast(col, target_field.data_type()).map_err(|e| {
                                    Error::UnexpectedError {
                                        message: format!(
                                            "Failed to cast column '{}' from {:?} to {:?}: {e}",
                                            target_field.name(),
                                            col.data_type(),
                                            target_field.data_type()
                                        ),
                                        source: Some(Box::new(e)),
                                    }
                                })?;
                                columns.push(casted);
                            }
                        }
                        None => {
                            let null_array = arrow_array::new_null_array(target_field.data_type(), num_rows);
                            columns.push(null_array);
                        }
                    }
                }

                let result = if columns.is_empty() {
                    RecordBatch::try_new_with_options(
                        target_schema.clone(),
                        columns,
                        &arrow_array::RecordBatchOptions::new().with_row_count(Some(num_rows)),
                    )
                } else {
                    RecordBatch::try_new(target_schema.clone(), columns)
                }
                .map_err(|e| {
                    Error::UnexpectedError {
                        message: format!("Failed to build schema-evolved RecordBatch: {e}"),
                        source: Some(Box::new(e)),
                    }
                })?;

                // Stage 4a: drop_deletes filters out rows whose RowKind is not
                // `is_add()` (i.e. DELETE / UPDATE_BEFORE) and removes the
                // `_VALUE_KIND` column from the user-visible output. NULL
                // values fall back to INSERT (mirrors `sort_merge.rs:336-342`).
                let yielded = if let Some((vk_idx, ref output_schema)) = drop_deletes_ctx {
                    let vk_col = result.column(vk_idx);
                    let vk_array = vk_col.as_any().downcast_ref::<Int8Array>().ok_or_else(|| {
                        Error::DataInvalid {
                            message: format!(
                                "_VALUE_KIND column expected Int8, got {:?}",
                                vk_col.data_type()
                            ),
                            source: None,
                        }
                    })?;
                    let mask: BooleanArray = (0..vk_array.len())
                        .map(|i| {
                            if vk_array.is_null(i) {
                                Some(true)
                            } else {
                                let v = vk_array.value(i);
                                Some(RowKind::from_value(v).map(|rk| rk.is_add()).unwrap_or(true))
                            }
                        })
                        .collect();
                    let filtered = filter_record_batch(&result, &mask).map_err(|e| {
                        Error::UnexpectedError {
                            message: format!("Failed to filter RowKind from batch: {e}"),
                            source: Some(Box::new(e)),
                        }
                    })?;
                    let kept_columns: Vec<Arc<dyn Array>> = filtered
                        .columns()
                        .iter()
                        .enumerate()
                        .filter(|(i, _)| *i != vk_idx)
                        .map(|(_, c)| c.clone())
                        .collect();
                    let row_count = filtered.num_rows();
                    if kept_columns.is_empty() {
                        RecordBatch::try_new_with_options(
                            output_schema.clone(),
                            kept_columns,
                            &arrow_array::RecordBatchOptions::new().with_row_count(Some(row_count)),
                        )
                    } else {
                        RecordBatch::try_new(output_schema.clone(), kept_columns)
                    }
                    .map_err(|e| Error::UnexpectedError {
                        message: format!("Failed to drop _VALUE_KIND column: {e}"),
                        source: Some(Box::new(e)),
                    })?
                } else {
                    result
                };
                yield yielded;
            }
        }
        .boxed())
    }
}

/// Convert absolute RowRanges to file-local 0-based ranges.
fn to_local_row_ranges(
    row_ranges: &[RowRange],
    first_row_id: i64,
    row_count: i64,
) -> Vec<RowRange> {
    let file_end = first_row_id + row_count - 1;
    row_ranges
        .iter()
        .filter_map(|r| {
            if r.to() < first_row_id || r.from() > file_end {
                return None;
            }
            let local_from = (r.from() - first_row_id).max(0);
            let local_to = (r.to() - first_row_id).min(row_count - 1);
            Some(RowRange::new(local_from, local_to))
        })
        .collect()
}

/// Merge DV and row_ranges into a unified list of 0-based inclusive RowRanges.
/// Returns `None` if no filtering is needed (no DV and no ranges).
///
/// Complexity: O(D + R) where D = number of deleted rows, R = number of ranges.
fn merge_row_selection(
    row_count: i64,
    dv: Option<&DeletionVector>,
    row_ranges: Option<&[RowRange]>,
) -> Option<Vec<RowRange>> {
    let has_dv = dv.is_some_and(|d| !d.is_empty());
    let has_ranges = row_ranges.is_some();
    if !has_dv && !has_ranges {
        return None;
    }

    if !has_dv {
        return row_ranges.map(|r| r.to_vec());
    }

    let dv_ranges = dv_to_non_deleted_ranges(dv.unwrap(), row_count);

    match row_ranges {
        Some(ranges) => Some(intersect_sorted_ranges(&dv_ranges, ranges)),
        None => Some(dv_ranges),
    }
}

/// Convert a DeletionVector into sorted non-deleted inclusive RowRanges.
fn dv_to_non_deleted_ranges(dv: &DeletionVector, row_count: i64) -> Vec<RowRange> {
    let mut result = Vec::new();
    let mut cursor: i64 = 0;
    for deleted in dv.iter() {
        let del = deleted as i64;
        if del >= row_count {
            break;
        }
        if del > cursor {
            result.push(RowRange::new(cursor, del - 1));
        }
        cursor = del + 1;
    }
    if cursor < row_count {
        result.push(RowRange::new(cursor, row_count - 1));
    }
    result
}

/// Intersect two sorted lists of inclusive RowRanges using a merge-style scan.
fn intersect_sorted_ranges(a: &[RowRange], b: &[RowRange]) -> Vec<RowRange> {
    let mut result = Vec::new();
    let (mut i, mut j) = (0, 0);
    while i < a.len() && j < b.len() {
        let from = a[i].from().max(b[j].from());
        let to = a[i].to().min(b[j].to());
        if from <= to {
            result.push(RowRange::new(from, to));
        }
        if a[i].to() < b[j].to() {
            i += 1;
        } else {
            j += 1;
        }
    }
    result
}

/// Expand row_ranges into a flat sequence of selected row IDs for a file.
/// Intended for per-batch _ROW_ID attachment — callers should not pass
/// whole-file ranges with millions of rows, as this allocates a Vec<i64>
/// proportional to the selected range size.
pub(super) fn expand_selected_row_ids(
    first_row_id: i64,
    row_count: i64,
    row_ranges: &[RowRange],
) -> Vec<i64> {
    if row_count == 0 {
        return Vec::new();
    }
    let file_end = first_row_id + row_count - 1;
    let mut ids = Vec::new();
    for r in row_ranges {
        let from = r.from().max(first_row_id);
        let to = r.to().min(file_end);
        for id in from..=to {
            ids.push(id);
        }
    }
    ids
}

pub(super) fn attach_row_id(
    batch: RecordBatch,
    row_id_index: usize,
    selected_row_ids: &[i64],
    row_id_offset: &mut usize,
    output_schema: &Arc<arrow_schema::Schema>,
) -> crate::Result<RecordBatch> {
    let num_rows = batch.num_rows();
    let end = *row_id_offset + num_rows;
    if end > selected_row_ids.len() {
        return Err(Error::UnexpectedError {
            message: format!(
                "Row ID offset out of bounds: need {}..{} but selected_row_ids has {} entries",
                *row_id_offset,
                end,
                selected_row_ids.len()
            ),
            source: None,
        });
    }
    let batch_ids = &selected_row_ids[*row_id_offset..end];
    *row_id_offset = end;
    let array: Arc<dyn arrow_array::Array> = Arc::new(Int64Array::from(batch_ids.to_vec()));
    insert_column_at(batch, array, row_id_index, output_schema)
}

pub(super) fn insert_column_at(
    batch: RecordBatch,
    column: Arc<dyn arrow_array::Array>,
    insert_index: usize,
    output_schema: &Arc<arrow_schema::Schema>,
) -> crate::Result<RecordBatch> {
    let mut columns: Vec<Arc<dyn arrow_array::Array>> = Vec::with_capacity(batch.num_columns() + 1);
    for (i, col) in batch.columns().iter().enumerate() {
        if i == insert_index {
            columns.push(column.clone());
        }
        columns.push(col.clone());
    }
    if insert_index >= batch.num_columns() {
        columns.push(column);
    }
    RecordBatch::try_new(output_schema.clone(), columns).map_err(|e| Error::UnexpectedError {
        message: format!("Failed to insert column into RecordBatch: {e}"),
        source: Some(Box::new(e)),
    })
}

/// Append a null `_ROW_ID` column for files without `first_row_id`.
pub(super) fn append_null_row_id_column(
    batch: RecordBatch,
    insert_index: usize,
    output_schema: &Arc<arrow_schema::Schema>,
) -> crate::Result<RecordBatch> {
    let array: Arc<dyn arrow_array::Array> = Arc::new(Int64Array::new_null(batch.num_rows()));
    insert_column_at(batch, array, insert_index, output_schema)
}

#[cfg(all(test, feature = "mosaic"))]
mod tests {
    use super::*;
    use crate::arrow::build_target_arrow_schema;
    use crate::io::FileIOBuilder;
    use crate::spec::stats::BinaryTableStats;
    use crate::spec::{ArrayType, DataFileMeta, DataType, IntType, VarCharType};
    use crate::table::source::DataSplitBuilder;
    use arrow_array::{Int32Array, StringArray};
    use bytes::Bytes;
    use futures::TryStreamExt;
    use paimon_mosaic_core::spec::COMPRESSION_NONE;
    use paimon_mosaic_core::writer::{MosaicWriter, OutputFile, WriterOptions};
    use std::io;

    struct MemOutputFile {
        data: Vec<u8>,
    }

    impl MemOutputFile {
        fn new() -> Self {
            Self { data: Vec::new() }
        }
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

    fn data_field(id: i32, name: &str, data_type: DataType) -> DataField {
        DataField::new(id, name.to_string(), data_type)
    }

    fn data_file(file_name: &str, file_size: i64, row_count: i64, schema_id: i64) -> DataFileMeta {
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
            schema_id,
            level: 0,
            extra_files: Vec::new(),
            creation_time: None,
            delete_row_count: None,
            embedded_index: None,
            file_source: None,
            value_stats_cols: None,
            external_path: None,
            first_row_id: None,
            write_cols: None,
        }
    }

    fn write_mosaic(batch: &RecordBatch) -> Bytes {
        let out = MemOutputFile::new();
        let mut writer = MosaicWriter::new(
            out,
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

    #[tokio::test]
    async fn test_mosaic_physical_missing_column_is_null_filled() {
        let physical_fields = vec![
            data_field(0, "id", DataType::Int(IntType::with_nullable(false))),
            data_field(
                1,
                "name",
                DataType::VarChar(VarCharType::with_nullable(true, 20).unwrap()),
            ),
        ];
        let read_fields = vec![
            physical_fields[0].clone(),
            data_field(
                2,
                "items",
                DataType::Array(ArrayType::new(DataType::Int(IntType::new()))),
            ),
            physical_fields[1].clone(),
        ];

        let physical_arrow_schema = build_target_arrow_schema(&physical_fields).unwrap();
        let batch = RecordBatch::try_new(
            physical_arrow_schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap();
        let data = write_mosaic(&batch);

        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let table_path = "memory:/mosaic_schema_evolution";
        let bucket_path = format!("{table_path}/bucket-0");
        let file_name = "part-0.mosaic";
        let file_path = format!("{bucket_path}/{file_name}");
        file_io
            .new_output(&file_path)
            .unwrap()
            .write(data.clone())
            .await
            .unwrap();

        let table_schema_id = 1;
        let split = DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(crate::spec::BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path(bucket_path)
            .with_total_buckets(1)
            .with_data_files(vec![data_file(
                file_name,
                data.len() as i64,
                3,
                table_schema_id,
            )])
            .build()
            .unwrap();
        let schema_manager = SchemaManager::new(file_io.clone(), table_path.to_string());
        let reader = DataFileReader::new(
            file_io,
            schema_manager,
            table_schema_id,
            read_fields.clone(),
            read_fields.clone(),
            Vec::new(),
        );
        let stream = reader.read(&[split]).unwrap();
        let batches = stream.try_collect::<Vec<_>>().await.unwrap();

        assert_eq!(batches.len(), 1);
        let result = &batches[0];
        assert_eq!(result.num_rows(), 3);
        assert_eq!(result.num_columns(), 3);
        assert_eq!(result.schema().field(0).name(), "id");
        assert_eq!(result.schema().field(1).name(), "items");
        assert_eq!(result.schema().field(2).name(), "name");
        assert_eq!(result.column(1).null_count(), 3);

        let ids = result
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(ids.values(), &[1, 2, 3]);
        let names = result
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "a");
        assert_eq!(names.value(2), "c");
    }
}

/// Parquet-only end-to-end tests for the inline VECTOR (`FixedSizeList`) read path.
///
/// This module is deliberately NOT gated behind the `mosaic` feature: the vector
/// read capability is core parquet support, so these tests must run under a plain
/// `cargo test -p paimon`.
#[cfg(test)]
mod vector_parquet_tests {
    use super::*;
    use crate::arrow::format::FormatFileWriter;
    use crate::arrow::format::ParquetFormatWriter;
    use crate::io::FileIOBuilder;
    use crate::spec::stats::BinaryTableStats;
    use crate::spec::{DataFileMeta, DataType, FloatType, VectorType};
    use crate::table::source::DataSplitBuilder;
    use arrow_array::builder::{FixedSizeListBuilder, Float32Builder};
    use arrow_array::{FixedSizeListArray, Float32Array, RecordBatch};
    use arrow_schema::{DataType as ArrowDataType, Field as ArrowField};
    use futures::TryStreamExt;

    fn data_file(file_name: &str, file_size: i64, row_count: i64, schema_id: i64) -> DataFileMeta {
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
            schema_id,
            level: 0,
            extra_files: Vec::new(),
            creation_time: None,
            delete_row_count: None,
            embedded_index: None,
            file_source: None,
            value_stats_cols: None,
            external_path: None,
            first_row_id: None,
            write_cols: None,
            commit_snapshot_id: None,
            merge_mode: None,
        }
    }

    /// TRUE end-to-end: write a parquet data file containing a `FixedSizeList<Float32, 2>`
    /// column, then read it back through `DataFileReader` using a Paimon `read_type` whose
    /// field is `DataType::Vector`. This exercises `build_target_arrow_schema`, the parquet
    /// format dispatch (by `.parquet` extension), and the read path's pass-through/cast
    /// logic — not just a raw Arrow/parquet round-trip.
    #[tokio::test]
    async fn test_datafilereader_inline_vector_column_e2e() {
        // Paimon read schema: a single nullable VECTOR<FLOAT> column of length 2.
        let vector_type = VectorType::try_new(true, 2, DataType::Float(FloatType::new())).unwrap();
        let read_fields = vec![DataField::new(
            0,
            "embedding".to_string(),
            DataType::Vector(vector_type),
        )];

        // Build the physical Arrow data via the Paimon -> Arrow conversion under test,
        // so the parquet file matches what the read path expects to materialize.
        let arrow_schema = build_target_arrow_schema(&read_fields).unwrap();

        // Build a FixedSizeList<Float32, 2> column:
        //   row 0 = [1.0, 2.0]   (non-null)
        //   row 1 = null         (null vector row)
        //   row 2 = [3.0, 4.0]   (non-null)
        let mut builder = FixedSizeListBuilder::new(Float32Builder::new(), 2).with_field(Arc::new(
            ArrowField::new("element", ArrowDataType::Float32, true),
        ));
        builder.values().append_value(1.0);
        builder.values().append_value(2.0);
        builder.append(true);
        builder.values().append_value(0.0);
        builder.values().append_value(0.0);
        builder.append(false); // null vector row
        builder.values().append_value(3.0);
        builder.values().append_value(4.0);
        builder.append(true);
        let vec_array = builder.finish();
        let batch = RecordBatch::try_new(arrow_schema.clone(), vec![Arc::new(vec_array)]).unwrap();

        // Write the data file as parquet into the split's bucket path.
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let table_path = "memory:/vector_inline_e2e";
        let bucket_path = format!("{table_path}/bucket-0");
        let file_name = "part-0.parquet";
        let file_path = format!("{bucket_path}/{file_name}");
        let output = file_io.new_output(&file_path).unwrap();
        let mut writer: Box<dyn FormatFileWriter> = Box::new(
            ParquetFormatWriter::new(&output, arrow_schema.clone(), "zstd", 1)
                .await
                .unwrap(),
        );
        writer.write(&batch).await.unwrap();
        let file_size = writer.close().await.unwrap();

        // Build a split whose data file's schema_id matches the table schema_id, so the
        // read path uses `read_type` directly (no SchemaManager lookup needed).
        let table_schema_id = 1;
        let split = DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(crate::spec::BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path(bucket_path)
            .with_total_buckets(1)
            .with_data_files(vec![data_file(
                file_name,
                file_size as i64,
                3,
                table_schema_id,
            )])
            .build()
            .unwrap();

        let schema_manager = SchemaManager::new(file_io.clone(), table_path.to_string());
        let reader = DataFileReader::new(
            file_io,
            schema_manager,
            table_schema_id,
            read_fields.clone(),
            read_fields.clone(),
            Vec::new(),
            1024,
            false,
            false,
        );
        let batches = reader
            .read(&[split])
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);
        let result = &batches[0];
        assert_eq!(result.num_columns(), 1);
        assert_eq!(result.schema().field(0).name(), "embedding");

        // The materialized column must be a FixedSizeListArray with the right length,
        // child Float32 values, and null bitmap (one non-null and one null row).
        let fsl = result
            .column(0)
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .expect("column should materialize as FixedSizeListArray");
        assert_eq!(fsl.value_length(), 2);
        assert!(fsl.is_valid(0));
        assert!(fsl.is_null(1)); // null vector row preserved through the read path
        assert!(fsl.is_valid(2));

        let row0 = fsl.value(0);
        let floats0 = row0
            .as_any()
            .downcast_ref::<Float32Array>()
            .expect("child should be Float32Array");
        assert_eq!(floats0.values(), &[1.0, 2.0]);

        let row2 = fsl.value(2);
        let floats2 = row2
            .as_any()
            .downcast_ref::<Float32Array>()
            .expect("child should be Float32Array");
        assert_eq!(floats2.values(), &[3.0, 4.0]);
    }
}
