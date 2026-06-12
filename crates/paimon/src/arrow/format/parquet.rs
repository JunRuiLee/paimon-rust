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

use super::{FilePredicates, FormatFileReader, FormatFileWriter};
use crate::arrow::filtering::{predicates_may_match_with_schema, StatsAccessor};
use crate::io::{FileRead, OutputFile};
use crate::spec::{DataField, DataType, Datum, Predicate, PredicateOperator};
use crate::table::{ArrowRecordBatchStream, RowRange};
use crate::Error;
use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Datum as ArrowDatum, Date32Array, Decimal128Array,
    Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, RecordBatch, Scalar,
    StringArray,
};
use arrow_ord::cmp::{
    eq as arrow_eq, gt as arrow_gt, gt_eq as arrow_gt_eq, lt as arrow_lt, lt_eq as arrow_lt_eq,
    neq as arrow_neq,
};
use arrow_schema::ArrowError;
use arrow_string::like::{
    contains as arrow_contains, ends_with as arrow_ends_with, like as arrow_like,
    starts_with as arrow_starts_with,
};
use async_trait::async_trait;
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::{FutureExt, StreamExt, TryFutureExt, TryStreamExt};
use parquet::arrow::arrow_reader::{
    ArrowPredicate, ArrowPredicateFn, ArrowReaderOptions, RowFilter, RowSelection, RowSelector,
};
use parquet::arrow::async_reader::{AsyncFileReader, MetadataFetch};
use parquet::arrow::{AsyncArrowWriter, ParquetRecordBatchStreamBuilder, ProjectionMask};
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::metadata::ParquetMetaDataReader;
use parquet::file::metadata::{ParquetMetaData, PageIndexPolicy, RowGroupMetaData};
use parquet::file::page_index::column_index::ColumnIndexMetaData;
use parquet::file::page_index::offset_index::OffsetIndexMetaData;
use parquet::file::properties::WriterProperties;
use parquet::file::statistics::Statistics as ParquetStatistics;
use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;

/// Parquet implementation of [`FormatFileReader`].
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct ParquetFormatReader {
    /// Whether to load page index (ColumnIndex / OffsetIndex) and use
    /// page-level stats for additional `RowSelection` pruning. Sourced from
    /// `CoreOptions::parquet_page_index_enabled()` by callers.
    pub(crate) page_index_enabled: bool,
    /// Whether to consult parquet bloom filters for `Eq` / `In` leaf
    /// predicates and skip row groups proven absent. Sourced from
    /// `CoreOptions::parquet_bloom_filter_enabled()` by callers; default
    /// false because the writer does not currently emit bloom filters.
    pub(crate) bloom_filter_enabled: bool,
}

/// Parquet implementation of [`FormatFileWriter`].
/// Streams data directly to storage via `AsyncArrowWriter` + opendal.
pub(crate) struct ParquetFormatWriter {
    inner: AsyncArrowWriter<Box<dyn crate::io::AsyncFileWrite>>,
}

impl ParquetFormatWriter {
    pub(crate) async fn new(
        output: &OutputFile,
        schema: arrow_schema::SchemaRef,
        compression: &str,
        zstd_level: i32,
    ) -> crate::Result<Self> {
        let async_write = output.async_writer().await?;
        let codec = parse_compression(compression, zstd_level);
        let props = WriterProperties::builder().set_compression(codec).build();
        let inner = AsyncArrowWriter::try_new(async_write, schema, Some(props)).map_err(|e| {
            crate::Error::DataInvalid {
                message: format!("Failed to create parquet writer: {e}"),
                source: None,
            }
        })?;
        Ok(Self { inner })
    }
}

/// Map Paimon `file.compression` value to parquet [`Compression`].
fn parse_compression(codec: &str, zstd_level: i32) -> Compression {
    match codec.to_ascii_lowercase().as_str() {
        "zstd" => {
            let level = ZstdLevel::try_new(zstd_level).unwrap_or_default();
            Compression::ZSTD(level)
        }
        "lz4" => Compression::LZ4_RAW,
        "snappy" => Compression::SNAPPY,
        "gzip" | "gz" => Compression::GZIP(Default::default()),
        "none" | "uncompressed" => Compression::UNCOMPRESSED,
        _ => Compression::UNCOMPRESSED,
    }
}

#[async_trait]
impl FormatFileWriter for ParquetFormatWriter {
    async fn write(&mut self, batch: &RecordBatch) -> crate::Result<()> {
        self.inner
            .write(batch)
            .await
            .map_err(|e| crate::Error::DataInvalid {
                message: format!("Failed to write parquet batch: {e}"),
                source: None,
            })
    }

    fn num_bytes(&self) -> usize {
        self.inner.bytes_written() + self.inner.in_progress_size()
    }

    fn in_progress_size(&self) -> usize {
        self.inner.in_progress_size()
    }

    async fn flush(&mut self) -> crate::Result<()> {
        self.inner
            .flush()
            .await
            .map_err(|e| crate::Error::DataInvalid {
                message: format!("Failed to flush parquet writer: {e}"),
                source: None,
            })
    }

    async fn close(mut self: Box<Self>) -> crate::Result<u64> {
        self.inner
            .finish()
            .await
            .map_err(|e| crate::Error::DataInvalid {
                message: format!("Failed to close parquet writer: {e}"),
                source: None,
            })?;
        Ok(self.inner.bytes_written() as u64)
    }
}

#[async_trait]
impl FormatFileReader for ParquetFormatReader {
    async fn read_batch_stream(
        &self,
        reader: Box<dyn FileRead>,
        file_size: u64,
        read_fields: &[DataField],
        predicates: Option<&FilePredicates>,
        batch_size: Option<usize>,
        row_selection: Option<Vec<RowRange>>,
    ) -> crate::Result<ArrowRecordBatchStream> {
        let arrow_file_reader = ArrowFileReader::new(file_size, reader);

        // Page index is loaded lazily by parquet-58 only when the reader
        // options request it; when disabled we keep the previous footer-only
        // metadata read path. `Optional` lets older files without page index
        // fall through (Required would error, see
        // `parquet-58.3.0/src/file/metadata/reader.rs:85-94`).
        let mut batch_stream_builder = if self.page_index_enabled {
            let arrow_options = ArrowReaderOptions::new()
                .with_page_index_policy(PageIndexPolicy::Optional);
            ParquetRecordBatchStreamBuilder::new_with_options(arrow_file_reader, arrow_options)
                .await?
        } else {
            ParquetRecordBatchStreamBuilder::new(arrow_file_reader).await?
        };

        let parquet_schema = batch_stream_builder.parquet_schema().clone();
        let root_schema = parquet_schema.root_schema();
        let root_indices: Vec<usize> = read_fields
            .iter()
            .filter_map(|f| {
                root_schema
                    .get_fields()
                    .iter()
                    .position(|pf| pf.name() == f.name())
            })
            .collect();

        let mask = ProjectionMask::roots(&parquet_schema, root_indices);
        batch_stream_builder = batch_stream_builder.with_projection(mask);

        let empty_predicates = Vec::new();
        let (preds, file_fields): (&[Predicate], &[DataField]) = match predicates {
            Some(fp) => (&fp.predicates, &fp.file_fields),
            None => (&empty_predicates, &[]),
        };

        let parquet_row_filter = build_parquet_row_filter(&parquet_schema, preds, file_fields)?;
        if let Some(f) = parquet_row_filter {
            batch_stream_builder = batch_stream_builder.with_row_filter(f);
        }

        let predicate_row_selection = build_predicate_row_selection(
            batch_stream_builder.metadata().row_groups(),
            preds,
            file_fields,
        )?;
        let mut combined_selection = predicate_row_selection;

        // Page-level selection — only meaningful when page index was loaded
        // (controlled by `self.page_index_enabled`). The helper itself returns
        // `None` when ColumnIndex / OffsetIndex are missing, so a stale
        // `page_index_enabled=true` on a file without page index is harmless.
        if self.page_index_enabled {
            let page_selection = build_predicate_page_selection(
                batch_stream_builder.metadata(),
                preds,
                file_fields,
            )?;
            combined_selection = intersect_optional_row_selections(combined_selection, page_selection);
        }

        // Bloom-filter row-group prune (Eq / In leaves only). Off by default
        // because the writer does not currently emit bloom filters; readers
        // opt in when their data was authored with bloom support. Each
        // bloom fetch is one extra async I/O per row group, so when the
        // toggle is on we still skip the work for predicate-less reads.
        if self.bloom_filter_enabled && !preds.is_empty() {
            let skip = bloom_check_row_groups(&mut batch_stream_builder, preds, file_fields).await?;
            if let Some(bloom_selection) = bloom_skipped_row_groups_selection(
                batch_stream_builder.metadata().row_groups(),
                &skip,
            ) {
                combined_selection =
                    intersect_optional_row_selections(combined_selection, Some(bloom_selection));
            }
        }

        if let Some(ref ranges) = row_selection {
            let range_selection =
                build_row_ranges_selection(batch_stream_builder.metadata().row_groups(), ranges);
            combined_selection =
                intersect_optional_row_selections(combined_selection, Some(range_selection));
        }
        if let Some(sel) = combined_selection {
            batch_stream_builder = batch_stream_builder.with_row_selection(sel);
        }
        if let Some(size) = batch_size {
            batch_stream_builder = batch_stream_builder.with_batch_size(size);
        }

        let batch_stream = batch_stream_builder.build()?;
        Ok(batch_stream.map(|r| r.map_err(Error::from)).boxed())
    }
}

// ---------------------------------------------------------------------------
// Parquet row-filter helpers
// ---------------------------------------------------------------------------

fn build_parquet_row_filter(
    parquet_schema: &parquet::schema::types::SchemaDescriptor,
    predicates: &[Predicate],
    file_fields: &[DataField],
) -> crate::Result<Option<RowFilter>> {
    if predicates.is_empty() {
        return Ok(None);
    }

    let mut filters: Vec<Box<dyn ArrowPredicate>> = Vec::new();

    for predicate in predicates {
        if let Some(filter) = build_parquet_arrow_predicate(parquet_schema, predicate, file_fields)?
        {
            filters.push(filter);
        }
    }

    if filters.is_empty() {
        Ok(None)
    } else {
        Ok(Some(RowFilter::new(filters)))
    }
}

fn build_parquet_arrow_predicate(
    parquet_schema: &parquet::schema::types::SchemaDescriptor,
    predicate: &Predicate,
    file_fields: &[DataField],
) -> crate::Result<Option<Box<dyn ArrowPredicate>>> {
    let Predicate::Leaf {
        index,
        data_type: _,
        op,
        literals,
        ..
    } = predicate
    else {
        return Ok(None);
    };
    if !predicate_supported_for_parquet_row_filter(*op) {
        return Ok(None);
    }

    let Some(file_field) = file_fields.get(*index) else {
        return Ok(None);
    };
    let Some(root_index) = parquet_root_index(parquet_schema, file_field.name()) else {
        return Ok(None);
    };
    if !parquet_row_filter_literals_supported(*op, literals, file_field.data_type())? {
        return Ok(None);
    }

    let projection = ProjectionMask::roots(parquet_schema, [root_index]);
    let op = *op;
    let data_type = file_field.data_type().clone();
    let literals = literals.to_vec();
    Ok(Some(Box::new(ArrowPredicateFn::new(
        projection,
        move |batch: RecordBatch| {
            let Some(column) = batch.columns().first() else {
                return Ok(BooleanArray::new_null(batch.num_rows()));
            };
            evaluate_exact_leaf_predicate(column, &data_type, op, &literals)
        },
    ))))
}

fn predicate_supported_for_parquet_row_filter(op: PredicateOperator) -> bool {
    matches!(
        op,
        PredicateOperator::IsNull
            | PredicateOperator::IsNotNull
            | PredicateOperator::Eq
            | PredicateOperator::NotEq
            | PredicateOperator::Lt
            | PredicateOperator::LtEq
            | PredicateOperator::Gt
            | PredicateOperator::GtEq
            | PredicateOperator::In
            | PredicateOperator::NotIn
            | PredicateOperator::StartsWith
            | PredicateOperator::EndsWith
            | PredicateOperator::Contains
            | PredicateOperator::Like
            | PredicateOperator::Between
            | PredicateOperator::NotBetween
    )
}

fn parquet_row_filter_literals_supported(
    op: PredicateOperator,
    literals: &[Datum],
    file_data_type: &DataType,
) -> crate::Result<bool> {
    match op {
        PredicateOperator::IsNull | PredicateOperator::IsNotNull => Ok(true),
        PredicateOperator::Eq
        | PredicateOperator::NotEq
        | PredicateOperator::Lt
        | PredicateOperator::LtEq
        | PredicateOperator::Gt
        | PredicateOperator::GtEq => {
            let Some(literal) = literals.first() else {
                return Ok(false);
            };
            Ok(literal_scalar_for_parquet_filter(literal, file_data_type)?.is_some())
        }
        PredicateOperator::In | PredicateOperator::NotIn => {
            for literal in literals {
                if literal_scalar_for_parquet_filter(literal, file_data_type)?.is_none() {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        PredicateOperator::StartsWith
        | PredicateOperator::EndsWith
        | PredicateOperator::Contains
        | PredicateOperator::Like => {
            // Substring kernels only run against string-typed columns; reject
            // non-string file types early so the filter falls back to stats
            // pruning + residual evaluation.
            if !matches!(file_data_type, DataType::Char(_) | DataType::VarChar(_)) {
                return Ok(false);
            }
            let Some(literal) = literals.first() else {
                return Ok(false);
            };
            Ok(literal_scalar_for_parquet_filter(literal, file_data_type)?.is_some())
        }
        PredicateOperator::Between | PredicateOperator::NotBetween => {
            if literals.len() != 2 {
                return Ok(false);
            }
            for literal in literals {
                if literal_scalar_for_parquet_filter(literal, file_data_type)?.is_none() {
                    return Ok(false);
                }
            }
            Ok(true)
        }
    }
}

fn parquet_root_index(
    parquet_schema: &parquet::schema::types::SchemaDescriptor,
    root_name: &str,
) -> Option<usize> {
    parquet_schema
        .root_schema()
        .get_fields()
        .iter()
        .position(|field| field.name() == root_name)
}

// ---------------------------------------------------------------------------
// Predicate evaluation helpers
// ---------------------------------------------------------------------------

fn evaluate_exact_leaf_predicate(
    array: &ArrayRef,
    data_type: &DataType,
    op: PredicateOperator,
    literals: &[Datum],
) -> Result<BooleanArray, ArrowError> {
    match op {
        PredicateOperator::IsNull => Ok(boolean_mask_from_predicate(array.len(), |row_index| {
            array.is_null(row_index)
        })),
        PredicateOperator::IsNotNull => Ok(boolean_mask_from_predicate(array.len(), |row_index| {
            array.is_valid(row_index)
        })),
        PredicateOperator::In | PredicateOperator::NotIn => {
            evaluate_set_membership_predicate(array, data_type, op, literals)
        }
        PredicateOperator::Eq
        | PredicateOperator::NotEq
        | PredicateOperator::Lt
        | PredicateOperator::LtEq
        | PredicateOperator::Gt
        | PredicateOperator::GtEq
        | PredicateOperator::StartsWith
        | PredicateOperator::EndsWith
        | PredicateOperator::Contains
        | PredicateOperator::Like => {
            let Some(literal) = literals.first() else {
                return Ok(BooleanArray::from(vec![true; array.len()]));
            };
            let Some(scalar) = literal_scalar_for_parquet_filter(literal, data_type)
                .map_err(|e| ArrowError::ComputeError(e.to_string()))?
            else {
                return Ok(BooleanArray::from(vec![true; array.len()]));
            };
            let result = evaluate_column_predicate(array, &scalar, op)?;
            Ok(sanitize_filter_mask(result))
        }
        PredicateOperator::Between | PredicateOperator::NotBetween => {
            evaluate_between_predicate(array, data_type, op, literals)
        }
    }
}

/// `Between` / `NotBetween` translate to `gt_eq(col, low) & lt_eq(col, high)`
/// (and its negation). `arrow_ord::cmp` produces nullable masks: any null
/// row makes the comparison null, so a fully-built `Between` mask preserves
/// nulls. `NotBetween` then negates valid rows and leaves nulls null —
/// matching SQL three-valued logic; `sanitize_filter_mask` collapses nulls
/// into `false` to match the predicate evaluator's "NULL → false" rule.
fn evaluate_between_predicate(
    array: &ArrayRef,
    data_type: &DataType,
    op: PredicateOperator,
    literals: &[Datum],
) -> Result<BooleanArray, ArrowError> {
    let (Some(low), Some(high)) = (literals.first(), literals.get(1)) else {
        return Ok(BooleanArray::from(vec![true; array.len()]));
    };
    let Some(low_scalar) = literal_scalar_for_parquet_filter(low, data_type)
        .map_err(|e| ArrowError::ComputeError(e.to_string()))?
    else {
        return Ok(BooleanArray::from(vec![true; array.len()]));
    };
    let Some(high_scalar) = literal_scalar_for_parquet_filter(high, data_type)
        .map_err(|e| ArrowError::ComputeError(e.to_string()))?
    else {
        return Ok(BooleanArray::from(vec![true; array.len()]));
    };
    let lo_mask = arrow_gt_eq(array, &low_scalar)?;
    let hi_mask = arrow_lt_eq(array, &high_scalar)?;
    let between = arrow_arith::boolean::and_kleene(&lo_mask, &hi_mask)?;
    let result = match op {
        PredicateOperator::Between => between,
        PredicateOperator::NotBetween => arrow_arith::boolean::not(&between)?,
        _ => unreachable!(),
    };
    Ok(sanitize_filter_mask(result))
}

fn evaluate_set_membership_predicate(
    array: &ArrayRef,
    data_type: &DataType,
    op: PredicateOperator,
    literals: &[Datum],
) -> Result<BooleanArray, ArrowError> {
    if literals.is_empty() {
        return Ok(match op {
            PredicateOperator::In => BooleanArray::from(vec![false; array.len()]),
            PredicateOperator::NotIn => {
                boolean_mask_from_predicate(array.len(), |row_index| array.is_valid(row_index))
            }
            _ => unreachable!(),
        });
    }

    let mut combined = match op {
        PredicateOperator::In => BooleanArray::from(vec![false; array.len()]),
        PredicateOperator::NotIn => {
            boolean_mask_from_predicate(array.len(), |row_index| array.is_valid(row_index))
        }
        _ => unreachable!(),
    };

    for literal in literals {
        let Some(scalar) = literal_scalar_for_parquet_filter(literal, data_type)
            .map_err(|e| ArrowError::ComputeError(e.to_string()))?
        else {
            return Ok(BooleanArray::from(vec![true; array.len()]));
        };
        let comparison_op = match op {
            PredicateOperator::In => PredicateOperator::Eq,
            PredicateOperator::NotIn => PredicateOperator::NotEq,
            _ => unreachable!(),
        };
        let mask = sanitize_filter_mask(evaluate_column_predicate(array, &scalar, comparison_op)?);
        combined = combine_filter_masks(&combined, &mask, matches!(op, PredicateOperator::In));
    }

    Ok(combined)
}

fn evaluate_column_predicate(
    column: &ArrayRef,
    scalar: &Scalar<ArrayRef>,
    op: PredicateOperator,
) -> Result<BooleanArray, ArrowError> {
    match op {
        PredicateOperator::Eq => arrow_eq(column, scalar),
        PredicateOperator::NotEq => arrow_neq(column, scalar),
        PredicateOperator::Lt => arrow_lt(column, scalar),
        PredicateOperator::LtEq => arrow_lt_eq(column, scalar),
        PredicateOperator::Gt => arrow_gt(column, scalar),
        PredicateOperator::GtEq => arrow_gt_eq(column, scalar),
        PredicateOperator::StartsWith
        | PredicateOperator::EndsWith
        | PredicateOperator::Contains
        | PredicateOperator::Like => {
            let pattern = pattern_scalar_for_string_kernel(scalar, column.data_type())?;
            match op {
                PredicateOperator::StartsWith => arrow_starts_with(column, &pattern),
                PredicateOperator::EndsWith => arrow_ends_with(column, &pattern),
                PredicateOperator::Contains => arrow_contains(column, &pattern),
                PredicateOperator::Like => arrow_like(column, &pattern),
                _ => unreachable!(),
            }
        }
        PredicateOperator::IsNull
        | PredicateOperator::IsNotNull
        | PredicateOperator::In
        | PredicateOperator::NotIn
        | PredicateOperator::Between
        | PredicateOperator::NotBetween => Ok(BooleanArray::new_null(column.len())),
    }
}

/// `arrow_string::like::*` kernels reject mismatched string types — Utf8 column
/// against Utf8 pattern is fine, but a LargeUtf8 / Utf8View column needs a
/// pattern of the same flavour. The shared scalar built upstream is always
/// `StringArray` (Utf8); promote it to match the column when needed.
fn pattern_scalar_for_string_kernel(
    scalar: &Scalar<ArrayRef>,
    column_type: &arrow_schema::DataType,
) -> Result<Scalar<ArrayRef>, ArrowError> {
    use arrow_array::{LargeStringArray, StringArray, StringViewArray};
    use arrow_schema::DataType as ArrowDataType;

    let arr = scalar.get().0;
    let value = arr
        .as_any()
        .downcast_ref::<StringArray>()
        .and_then(|s| (s.len() == 1 && s.is_valid(0)).then(|| s.value(0).to_string()));
    let Some(value) = value else {
        return Ok(scalar.clone());
    };
    Ok(match column_type {
        ArrowDataType::Utf8 => Scalar::new(Arc::new(StringArray::from(vec![value])) as ArrayRef),
        ArrowDataType::LargeUtf8 => {
            Scalar::new(Arc::new(LargeStringArray::from(vec![value])) as ArrayRef)
        }
        ArrowDataType::Utf8View => {
            Scalar::new(Arc::new(StringViewArray::from(vec![value])) as ArrayRef)
        }
        ArrowDataType::Dictionary(_, value_type) if value_type.as_ref() == &ArrowDataType::Utf8 => {
            Scalar::new(Arc::new(StringArray::from(vec![value])) as ArrayRef)
        }
        other => {
            return Err(ArrowError::InvalidArgumentError(format!(
                "string predicate against non-string column type {other:?}"
            )))
        }
    })
}

fn sanitize_filter_mask(mask: BooleanArray) -> BooleanArray {
    if mask.null_count() == 0 {
        return mask;
    }

    boolean_mask_from_predicate(mask.len(), |row_index| {
        mask.is_valid(row_index) && mask.value(row_index)
    })
}

fn combine_filter_masks(left: &BooleanArray, right: &BooleanArray, use_or: bool) -> BooleanArray {
    debug_assert_eq!(left.len(), right.len());
    boolean_mask_from_predicate(left.len(), |row_index| {
        if use_or {
            left.value(row_index) || right.value(row_index)
        } else {
            left.value(row_index) && right.value(row_index)
        }
    })
}

fn boolean_mask_from_predicate(
    len: usize,
    mut predicate: impl FnMut(usize) -> bool,
) -> BooleanArray {
    BooleanArray::from((0..len).map(&mut predicate).collect::<Vec<_>>())
}

// ---------------------------------------------------------------------------
// Row-group statistics pruning
// ---------------------------------------------------------------------------

struct ParquetRowGroupStats<'a> {
    row_group: &'a RowGroupMetaData,
    column_indices: &'a [Option<usize>],
}

impl StatsAccessor for ParquetRowGroupStats<'_> {
    fn row_count(&self) -> i64 {
        self.row_group.num_rows()
    }

    fn null_count(&self, index: usize) -> Option<i64> {
        let _ = index;
        None
    }

    fn min_value(&self, index: usize, data_type: &DataType) -> Option<Datum> {
        let column_index = self.column_indices.get(index).copied().flatten()?;
        parquet_stats_to_datum(
            self.row_group.column(column_index).statistics()?,
            data_type,
            true,
        )
    }

    fn max_value(&self, index: usize, data_type: &DataType) -> Option<Datum> {
        let column_index = self.column_indices.get(index).copied().flatten()?;
        parquet_stats_to_datum(
            self.row_group.column(column_index).statistics()?,
            data_type,
            false,
        )
    }
}

fn build_predicate_row_selection(
    row_groups: &[RowGroupMetaData],
    predicates: &[Predicate],
    file_fields: &[DataField],
) -> crate::Result<Option<RowSelection>> {
    if predicates.is_empty() || row_groups.is_empty() {
        return Ok(None);
    }

    // Predicates have already been remapped to file-level indices by the caller
    // (remap_predicates_to_file in reader.rs), so we use an identity mapping here.
    let identity_mapping: Vec<Option<usize>> = (0..file_fields.len()).map(Some).collect();
    let column_indices = build_row_group_column_indices(row_groups[0].columns(), file_fields);
    let mut selectors = Vec::with_capacity(row_groups.len());
    let mut all_selected = true;

    for row_group in row_groups {
        let stats = ParquetRowGroupStats {
            row_group,
            column_indices: &column_indices,
        };
        let may_match =
            predicates_may_match_with_schema(predicates, &stats, &identity_mapping, file_fields);
        if !may_match {
            all_selected = false;
        }
        selectors.push(if may_match {
            RowSelector::select(row_group.num_rows() as usize)
        } else {
            RowSelector::skip(row_group.num_rows() as usize)
        });
    }

    if all_selected {
        Ok(None)
    } else {
        Ok(Some(selectors.into()))
    }
}

// ---------------------------------------------------------------------------
// Page-level pruning (Parquet ColumnIndex / OffsetIndex)
// ---------------------------------------------------------------------------

/// Stats accessor for a single page within a row group's column chunk. The
/// shared `predicate_stats::data_leaf_may_match` evaluator drives row-group,
/// page, and bloom-filter pruning the same way; this just plugs page-level
/// `ColumnIndex` + `OffsetIndex` metadata into the same `StatsAccessor` shape.
///
/// Each accessor instance is bound to one (row group, page) pair; callers
/// instantiate one per page index they want to prune.
struct ParquetPageStats<'a> {
    /// Per-column page-index metadata for this row group, indexed by
    /// `file_fields` order (entries are `None` when the column has no
    /// page index for this row group).
    column_indices: &'a [Option<&'a ColumnIndexMetaData>],
    page_idx: usize,
    /// Page row count, derived from `OffsetIndex.first_row_index` of this and
    /// the next page (or `row_group.num_rows()` for the last page).
    page_row_count: i64,
}

impl StatsAccessor for ParquetPageStats<'_> {
    fn row_count(&self) -> i64 {
        self.page_row_count
    }

    fn null_count(&self, index: usize) -> Option<i64> {
        let column_index = self.column_indices.get(index).copied().flatten()?;
        column_index.null_count(self.page_idx)
    }

    fn min_value(&self, index: usize, data_type: &DataType) -> Option<Datum> {
        let column_index = self.column_indices.get(index).copied().flatten()?;
        page_index_value_to_datum(column_index, self.page_idx, data_type, /* is_min */ true)
    }

    fn max_value(&self, index: usize, data_type: &DataType) -> Option<Datum> {
        let column_index = self.column_indices.get(index).copied().flatten()?;
        page_index_value_to_datum(column_index, self.page_idx, data_type, /* is_min */ false)
    }
}

/// Convert a single page's min or max from a [`ColumnIndexMetaData`] enum into
/// the matching paimon [`Datum`]. Returns `None` for the safe fail-open cases:
/// missing index data, null page, type mismatch, or any conversion that the
/// existing footer-side path already excludes (e.g. timestamps with
/// sub-millisecond precision, decimal stats).
fn page_index_value_to_datum(
    column_index: &ColumnIndexMetaData,
    page_idx: usize,
    data_type: &DataType,
    is_min: bool,
) -> Option<Datum> {
    if column_index.is_null_page(page_idx) {
        return None;
    }
    match (column_index, data_type) {
        (ColumnIndexMetaData::BOOLEAN(idx), DataType::Boolean(_)) => {
            let value = if is_min {
                idx.min_values().get(page_idx)
            } else {
                idx.max_values().get(page_idx)
            };
            value.copied().map(Datum::Bool)
        }
        (ColumnIndexMetaData::INT32(idx), DataType::TinyInt(_)) => {
            let value = if is_min { idx.min_values().get(page_idx) } else { idx.max_values().get(page_idx) };
            value.and_then(|v| i8::try_from(*v).ok()).map(Datum::TinyInt)
        }
        (ColumnIndexMetaData::INT32(idx), DataType::SmallInt(_)) => {
            let value = if is_min { idx.min_values().get(page_idx) } else { idx.max_values().get(page_idx) };
            value.and_then(|v| i16::try_from(*v).ok()).map(Datum::SmallInt)
        }
        (ColumnIndexMetaData::INT32(idx), DataType::Int(_)) => {
            let value = if is_min { idx.min_values().get(page_idx) } else { idx.max_values().get(page_idx) };
            value.copied().map(Datum::Int)
        }
        (ColumnIndexMetaData::INT32(idx), DataType::Date(_)) => {
            let value = if is_min { idx.min_values().get(page_idx) } else { idx.max_values().get(page_idx) };
            value.copied().map(Datum::Date)
        }
        (ColumnIndexMetaData::INT32(idx), DataType::Time(_)) => {
            let value = if is_min { idx.min_values().get(page_idx) } else { idx.max_values().get(page_idx) };
            value.copied().map(Datum::Time)
        }
        (ColumnIndexMetaData::INT64(idx), DataType::BigInt(_)) => {
            let value = if is_min { idx.min_values().get(page_idx) } else { idx.max_values().get(page_idx) };
            value.copied().map(Datum::Long)
        }
        (ColumnIndexMetaData::INT64(idx), DataType::Timestamp(ts)) if ts.precision() <= 3 => {
            let value = if is_min { idx.min_values().get(page_idx) } else { idx.max_values().get(page_idx) };
            value.copied().map(|millis| Datum::Timestamp { millis, nanos: 0 })
        }
        (ColumnIndexMetaData::INT64(idx), DataType::LocalZonedTimestamp(ts)) if ts.precision() <= 3 => {
            let value = if is_min { idx.min_values().get(page_idx) } else { idx.max_values().get(page_idx) };
            value.copied().map(|millis| Datum::LocalZonedTimestamp { millis, nanos: 0 })
        }
        (ColumnIndexMetaData::FLOAT(idx), DataType::Float(_)) => {
            let value = if is_min { idx.min_values().get(page_idx) } else { idx.max_values().get(page_idx) };
            value.copied().map(Datum::Float)
        }
        (ColumnIndexMetaData::DOUBLE(idx), DataType::Double(_)) => {
            let value = if is_min { idx.min_values().get(page_idx) } else { idx.max_values().get(page_idx) };
            value.copied().map(Datum::Double)
        }
        (ColumnIndexMetaData::BYTE_ARRAY(idx), DataType::Char(_))
        | (ColumnIndexMetaData::BYTE_ARRAY(idx), DataType::VarChar(_)) => {
            let value = if is_min { idx.min_value(page_idx) } else { idx.max_value(page_idx) };
            value
                .and_then(|bytes| std::str::from_utf8(bytes).ok())
                .map(|s| Datum::String(s.to_string()))
        }
        (ColumnIndexMetaData::BYTE_ARRAY(idx), DataType::Binary(_))
        | (ColumnIndexMetaData::BYTE_ARRAY(idx), DataType::VarBinary(_))
        | (ColumnIndexMetaData::FIXED_LEN_BYTE_ARRAY(idx), DataType::Binary(_))
        | (ColumnIndexMetaData::FIXED_LEN_BYTE_ARRAY(idx), DataType::VarBinary(_)) => {
            let value = if is_min { idx.min_value(page_idx) } else { idx.max_value(page_idx) };
            value.map(|bytes| Datum::Bytes(bytes.to_vec()))
        }
        _ => None,
    }
}

fn build_predicate_page_selection(
    metadata: &ParquetMetaData,
    predicates: &[Predicate],
    file_fields: &[DataField],
) -> crate::Result<Option<RowSelection>> {
    if predicates.is_empty() {
        return Ok(None);
    }
    // Page index / offset index are loaded lazily by the reader options. If
    // either is absent (older files, writer didn't emit them, page-index
    // disabled by config), fall through and let row-group + per-row filter
    // do their job.
    let column_index = metadata.column_index();
    let offset_index = metadata.offset_index();
    let (Some(column_index), Some(offset_index)) = (column_index, offset_index) else {
        return Ok(None);
    };

    let row_groups = metadata.row_groups();
    if row_groups.is_empty() {
        return Ok(None);
    }
    let identity_mapping: Vec<Option<usize>> = (0..file_fields.len()).map(Some).collect();
    let columns_in_first_rg = row_groups[0].columns();
    let column_index_lookup = build_row_group_column_indices(columns_in_first_rg, file_fields);

    let mut ranges: Vec<std::ops::Range<usize>> = Vec::new();
    let mut total_rows: usize = 0;
    let mut any_skipped = false;

    for (rg_idx, row_group) in row_groups.iter().enumerate() {
        let rg_base = total_rows;
        let rg_rows = row_group.num_rows() as usize;
        total_rows += rg_rows;

        let rg_column_index = column_index.get(rg_idx);
        let rg_offset_index = offset_index.get(rg_idx);
        let (Some(rg_column_index), Some(rg_offset_index)) = (rg_column_index, rg_offset_index)
        else {
            // No page index for this row group: keep every row in the group
            // (fail-open). Stats prune at row-group level still applied.
            ranges.push(rg_base..rg_base + rg_rows);
            continue;
        };

        // Collect a per-column-index ColumnIndex reference for the StatsAccessor.
        // `column_index_lookup[i]` is the file column index in this RG for
        // file_fields[i]; use it to fetch the ColumnIndexMetaData.
        let per_column: Vec<Option<&ColumnIndexMetaData>> = column_index_lookup
            .iter()
            .map(|opt| opt.and_then(|cidx| rg_column_index.get(cidx)))
            .collect();

        // Use the offset index of any column that exists to drive page row
        // boundaries. All columns have the same page row layout.
        let Some(driver_col) = column_index_lookup.iter().find_map(|opt| *opt) else {
            // No column resolved — keep group.
            ranges.push(rg_base..rg_base + rg_rows);
            continue;
        };
        let Some(driver_offset_index) = rg_offset_index.get(driver_col) else {
            ranges.push(rg_base..rg_base + rg_rows);
            continue;
        };
        let pages = match driver_offset_index {
            OffsetIndexMetaData { page_locations, .. } => page_locations,
        };
        if pages.is_empty() {
            ranges.push(rg_base..rg_base + rg_rows);
            continue;
        }

        for (page_idx, page) in pages.iter().enumerate() {
            let page_first_row = page.first_row_index as usize;
            let page_end = if page_idx + 1 < pages.len() {
                pages[page_idx + 1].first_row_index as usize
            } else {
                rg_rows
            };
            if page_end <= page_first_row {
                continue;
            }
            let page_row_count = (page_end - page_first_row) as i64;
            let stats = ParquetPageStats {
                column_indices: &per_column,
                page_idx,
                page_row_count,
            };
            let may_match = predicates_may_match_with_schema(
                predicates,
                &stats,
                &identity_mapping,
                file_fields,
            );
            if may_match {
                ranges.push(rg_base + page_first_row..rg_base + page_end);
            } else {
                any_skipped = true;
            }
        }
    }

    if !any_skipped {
        return Ok(None);
    }
    Ok(Some(RowSelection::from_consecutive_ranges(
        ranges.into_iter(),
        total_rows,
    )))
}

// ---------------------------------------------------------------------------
// Bloom filter row-group pruning (Stage 2 of P7)
// ---------------------------------------------------------------------------

/// Hash a paimon literal for `Sbbf::check` according to the parquet bloom
/// filter spec: hashes are taken over the column's physical-type bytes, so
/// each `(Datum, file DataType)` shape needs its own `check::<T>` call to
/// dispatch into the right `parquet::data_type::AsBytes` impl. Returns
/// `Some(true)` when the bloom filter says the value *may* be present (we
/// keep the row group), `Some(false)` when bloom proves it absent (we can
/// skip the row group), and `None` when the literal cannot be projected onto
/// the column's physical type — in that case the caller must fall open
/// (treat the row group as a possible match).
fn bloom_check_datum_against(
    sbbf: &parquet::bloom_filter::Sbbf,
    literal: &Datum,
    file_data_type: &DataType,
) -> Option<bool> {
    use parquet::data_type::ByteArray;
    match (literal, file_data_type) {
        (Datum::Bool(v), DataType::Boolean(_)) => Some(sbbf.check(v)),
        // Tiny / Small / Int / Date / Time all live in the parquet INT32
        // physical type, so widen the literal to i32 before hashing. Hash
        // bytes are explicitly little-endian to match the parquet bloom
        // spec (parquet-mr `BlockSplitBloomFilter` uses
        // `ByteBuffer.allocate(...).order(ByteOrder.LITTLE_ENDIAN)` before
        // calling `xxHash`); going through `Sbbf::check<i32>` would use
        // `data_type::gen_as_bytes!`'s native-memory layout, producing BE
        // bytes on big-endian CPUs and false-negatives against any file
        // authored by parquet-mr or by the Rust writer running on a LE
        // platform.
        (Datum::TinyInt(v), DataType::TinyInt(_)) => {
            Some(sbbf.check(&(*v as i32).to_le_bytes()[..]))
        }
        (Datum::SmallInt(v), DataType::SmallInt(_)) => {
            Some(sbbf.check(&(*v as i32).to_le_bytes()[..]))
        }
        (Datum::Int(v), DataType::Int(_)) => Some(sbbf.check(&v.to_le_bytes()[..])),
        (Datum::Date(v), DataType::Date(_)) => Some(sbbf.check(&v.to_le_bytes()[..])),
        (Datum::Time(v), DataType::Time(_)) => Some(sbbf.check(&v.to_le_bytes()[..])),
        (Datum::Long(v), DataType::BigInt(_)) => Some(sbbf.check(&v.to_le_bytes()[..])),
        (Datum::Timestamp { millis, .. }, DataType::Timestamp(ts)) if ts.precision() <= 3 => {
            Some(sbbf.check(&millis.to_le_bytes()[..]))
        }
        (Datum::LocalZonedTimestamp { millis, .. }, DataType::LocalZonedTimestamp(ts))
            if ts.precision() <= 3 =>
        {
            Some(sbbf.check(&millis.to_le_bytes()[..]))
        }
        (Datum::Float(v), DataType::Float(_)) => Some(sbbf.check(&v.to_le_bytes()[..])),
        (Datum::Double(v), DataType::Double(_)) => Some(sbbf.check(&v.to_le_bytes()[..])),
        (Datum::String(v), DataType::Char(_)) | (Datum::String(v), DataType::VarChar(_)) => {
            // Parquet hashes BYTE_ARRAY values via the raw byte slice.
            // `ByteArray::from(&str)` matches the writer-side encoding.
            let bytes = ByteArray::from(v.as_str());
            Some(sbbf.check(&bytes))
        }
        (Datum::Bytes(v), DataType::Binary(_)) | (Datum::Bytes(v), DataType::VarBinary(_)) => {
            let bytes = ByteArray::from(v.as_slice());
            Some(sbbf.check(&bytes))
        }
        // Decimal, sub-millisecond timestamps, and any cross-type combinations
        // (e.g. Datum::Int against a BigInt column) fall through to fail-open:
        // bloom filter encoding is physical-type sensitive and we don't want
        // to silently mis-hash.
        _ => None,
    }
}

/// Outcome of bloom-filtering one leaf predicate against one row group's
/// column bloom filter:
/// * `Skip` — bloom proves no row in this group can satisfy the leaf, the
///   caller can drop the entire row group.
/// * `Keep` — bloom says the value may be present (or bloom unavailable /
///   literal not encodable); leave the row group to downstream filters.
enum BloomVerdict {
    Skip,
    Keep,
}

async fn evaluate_bloom_for_leaf<'a, T: AsyncFileReader + Send + Sync + 'static>(
    builder: &mut ParquetRecordBatchStreamBuilder<T>,
    rg_idx: usize,
    column_idx: usize,
    op: PredicateOperator,
    literals: &[Datum],
    file_data_type: &DataType,
) -> crate::Result<BloomVerdict> {
    // Bloom only refutes equality. Any other op leaves the row group keep.
    if !matches!(op, PredicateOperator::Eq | PredicateOperator::In) {
        return Ok(BloomVerdict::Keep);
    }
    let sbbf = match builder
        .get_row_group_column_bloom_filter(rg_idx, column_idx)
        .await
    {
        Ok(Some(s)) => s,
        // No bloom filter for this column or fetch failed: fall open.
        Ok(None) => return Ok(BloomVerdict::Keep),
        Err(e) => {
            return Err(crate::Error::DataInvalid {
                message: format!("Failed to read parquet bloom filter: {e}"),
                source: Some(Box::new(e)),
            });
        }
    };

    let mut any_kept = false;
    let mut any_unencodable = false;
    for literal in literals {
        match bloom_check_datum_against(&sbbf, literal, file_data_type) {
            // Literal definitely absent — keep checking other literals (for
            // `In`, all of them must say absent before we can skip).
            Some(false) => continue,
            // Literal may be present. For `Eq` (a single literal) this means
            // keep; for `In` even one possible match means keep.
            Some(true) => {
                any_kept = true;
                break;
            }
            None => {
                // Couldn't encode the literal; we can't safely use bloom.
                any_unencodable = true;
                break;
            }
        }
    }
    if any_unencodable || any_kept {
        Ok(BloomVerdict::Keep)
    } else {
        // Eq path with no match, or In path where every literal said absent.
        Ok(BloomVerdict::Skip)
    }
}

/// Walk the predicates and for each `Eq` / `In` leaf consult the matching
/// column's bloom filter. A row group is skipped only when **some** leaf's
/// bloom proves no match (AND of leaves over a single row group ⇒ a single
/// proven-absent leaf is enough). All other ops, missing bloom filters, or
/// unencodable literals fall open.
///
/// Returns the set of row group indices that should be skipped. Empty set
/// means "no skips contributed by bloom" (caller leaves the existing
/// row-group selection alone).
async fn bloom_check_row_groups<T: AsyncFileReader + Send + Sync + 'static>(
    builder: &mut ParquetRecordBatchStreamBuilder<T>,
    predicates: &[Predicate],
    file_fields: &[DataField],
) -> crate::Result<std::collections::HashSet<usize>> {
    let mut skip = std::collections::HashSet::new();
    let row_group_count = builder.metadata().row_groups().len();
    if row_group_count == 0 || predicates.is_empty() || file_fields.is_empty() {
        return Ok(skip);
    }

    // Resolve each file_field to the column index inside the row group's
    // schema. The mapping is identical for every row group, so compute once.
    let columns = builder.metadata().row_groups()[0].columns();
    let column_indices = build_row_group_column_indices(columns, file_fields);

    // Each conjunct must hold for a row group to be relevant; we therefore
    // skip the row group as soon as *any* conjunct's bloom proves absent.
    for rg_idx in 0..row_group_count {
        for predicate in predicates {
            let Predicate::Leaf {
                index,
                op,
                literals,
                ..
            } = predicate
            else {
                continue;
            };
            let Some(column_idx) = column_indices.get(*index).copied().flatten() else {
                continue;
            };
            let Some(file_data_type) = file_fields.get(*index).map(|f| f.data_type()) else {
                continue;
            };
            match evaluate_bloom_for_leaf(
                builder,
                rg_idx,
                column_idx,
                *op,
                literals,
                file_data_type,
            )
            .await?
            {
                BloomVerdict::Skip => {
                    skip.insert(rg_idx);
                    break; // No need to check other leaves for this rg.
                }
                BloomVerdict::Keep => {}
            }
        }
    }
    Ok(skip)
}

/// Compose a `RowSelection` that skips exactly the row groups marked by
/// bloom filtering. Returns `None` when nothing was skipped, so the caller
/// can leave any existing selection alone.
fn bloom_skipped_row_groups_selection(
    row_groups: &[RowGroupMetaData],
    skip: &std::collections::HashSet<usize>,
) -> Option<RowSelection> {
    if skip.is_empty() {
        return None;
    }
    let mut selectors = Vec::with_capacity(row_groups.len());
    for (idx, rg) in row_groups.iter().enumerate() {
        let n = rg.num_rows() as usize;
        if skip.contains(&idx) {
            selectors.push(RowSelector::skip(n));
        } else {
            selectors.push(RowSelector::select(n));
        }
    }
    Some(selectors.into())
}

fn build_row_group_column_indices(
    columns: &[parquet::file::metadata::ColumnChunkMetaData],
    file_fields: &[DataField],
) -> Vec<Option<usize>> {
    let mut by_root_name: HashMap<&str, Option<usize>> = HashMap::new();
    for (column_index, column) in columns.iter().enumerate() {
        let Some(root_name) = column.column_path().parts().first() else {
            continue;
        };
        let entry = by_root_name
            .entry(root_name.as_str())
            .or_insert(Some(column_index));
        if entry.is_some() && *entry != Some(column_index) {
            *entry = None;
        }
    }

    file_fields
        .iter()
        .map(|field| by_root_name.get(field.name()).copied().flatten())
        .collect()
}

// ---------------------------------------------------------------------------
// Parquet statistics → Datum conversion
// ---------------------------------------------------------------------------

fn parquet_stats_to_datum(
    stats: &ParquetStatistics,
    data_type: &DataType,
    is_min: bool,
) -> Option<Datum> {
    let exact = if is_min {
        stats.min_is_exact()
    } else {
        stats.max_is_exact()
    };
    if !exact {
        return None;
    }

    match (stats, data_type) {
        (ParquetStatistics::Boolean(stats), DataType::Boolean(_)) => {
            exact_parquet_value(is_min, stats.min_opt(), stats.max_opt())
                .copied()
                .map(Datum::Bool)
        }
        (ParquetStatistics::Int32(stats), DataType::TinyInt(_)) => {
            exact_parquet_value(is_min, stats.min_opt(), stats.max_opt())
                .and_then(|value| i8::try_from(*value).ok())
                .map(Datum::TinyInt)
        }
        (ParquetStatistics::Int32(stats), DataType::SmallInt(_)) => {
            exact_parquet_value(is_min, stats.min_opt(), stats.max_opt())
                .and_then(|value| i16::try_from(*value).ok())
                .map(Datum::SmallInt)
        }
        (ParquetStatistics::Int32(stats), DataType::Int(_)) => {
            exact_parquet_value(is_min, stats.min_opt(), stats.max_opt())
                .copied()
                .map(Datum::Int)
        }
        (ParquetStatistics::Int32(stats), DataType::Date(_)) => {
            exact_parquet_value(is_min, stats.min_opt(), stats.max_opt())
                .copied()
                .map(Datum::Date)
        }
        (ParquetStatistics::Int32(stats), DataType::Time(_)) => {
            exact_parquet_value(is_min, stats.min_opt(), stats.max_opt())
                .copied()
                .map(Datum::Time)
        }
        (ParquetStatistics::Int64(stats), DataType::BigInt(_)) => {
            exact_parquet_value(is_min, stats.min_opt(), stats.max_opt())
                .copied()
                .map(Datum::Long)
        }
        (ParquetStatistics::Int64(stats), DataType::Timestamp(ts)) if ts.precision() <= 3 => {
            exact_parquet_value(is_min, stats.min_opt(), stats.max_opt())
                .copied()
                .map(|millis| Datum::Timestamp { millis, nanos: 0 })
        }
        (ParquetStatistics::Int64(stats), DataType::LocalZonedTimestamp(ts))
            if ts.precision() <= 3 =>
        {
            exact_parquet_value(is_min, stats.min_opt(), stats.max_opt())
                .copied()
                .map(|millis| Datum::LocalZonedTimestamp { millis, nanos: 0 })
        }
        (ParquetStatistics::Float(stats), DataType::Float(_)) => {
            exact_parquet_value(is_min, stats.min_opt(), stats.max_opt())
                .copied()
                .map(Datum::Float)
        }
        (ParquetStatistics::Double(stats), DataType::Double(_)) => {
            exact_parquet_value(is_min, stats.min_opt(), stats.max_opt())
                .copied()
                .map(Datum::Double)
        }
        (ParquetStatistics::ByteArray(stats), DataType::Char(_))
        | (ParquetStatistics::ByteArray(stats), DataType::VarChar(_)) => {
            exact_parquet_value(is_min, stats.min_opt(), stats.max_opt())
                .and_then(|value| std::str::from_utf8(value.data()).ok())
                .map(|value| Datum::String(value.to_string()))
        }
        (ParquetStatistics::ByteArray(stats), DataType::Binary(_))
        | (ParquetStatistics::ByteArray(stats), DataType::VarBinary(_)) => {
            exact_parquet_value(is_min, stats.min_opt(), stats.max_opt())
                .map(|value| Datum::Bytes(value.data().to_vec()))
        }
        (ParquetStatistics::FixedLenByteArray(stats), DataType::Binary(_))
        | (ParquetStatistics::FixedLenByteArray(stats), DataType::VarBinary(_)) => {
            exact_parquet_value(is_min, stats.min_opt(), stats.max_opt())
                .map(|value| Datum::Bytes(value.data().to_vec()))
        }
        _ => None,
    }
}

fn exact_parquet_value<'a, T>(
    is_min: bool,
    min: Option<&'a T>,
    max: Option<&'a T>,
) -> Option<&'a T> {
    if is_min {
        min
    } else {
        max
    }
}

// ---------------------------------------------------------------------------
// Literal → Arrow scalar conversion
// ---------------------------------------------------------------------------

fn literal_scalar_for_parquet_filter(
    literal: &Datum,
    file_data_type: &DataType,
) -> crate::Result<Option<Scalar<ArrayRef>>> {
    let array: ArrayRef = match file_data_type {
        DataType::Boolean(_) => match literal {
            Datum::Bool(value) => Arc::new(BooleanArray::new_scalar(*value).into_inner()),
            _ => return Ok(None),
        },
        DataType::TinyInt(_) => {
            match integer_literal(literal).and_then(|value| i8::try_from(value).ok()) {
                Some(value) => Arc::new(Int8Array::new_scalar(value).into_inner()),
                None => return Ok(None),
            }
        }
        DataType::SmallInt(_) => {
            match integer_literal(literal).and_then(|value| i16::try_from(value).ok()) {
                Some(value) => Arc::new(Int16Array::new_scalar(value).into_inner()),
                None => return Ok(None),
            }
        }
        DataType::Int(_) => {
            match integer_literal(literal).and_then(|value| i32::try_from(value).ok()) {
                Some(value) => Arc::new(Int32Array::new_scalar(value).into_inner()),
                None => return Ok(None),
            }
        }
        DataType::BigInt(_) => {
            match integer_literal(literal).and_then(|value| i64::try_from(value).ok()) {
                Some(value) => Arc::new(Int64Array::new_scalar(value).into_inner()),
                None => return Ok(None),
            }
        }
        DataType::Float(_) => match float32_literal(literal) {
            Some(value) => Arc::new(Float32Array::new_scalar(value).into_inner()),
            None => return Ok(None),
        },
        DataType::Double(_) => match float64_literal(literal) {
            Some(value) => Arc::new(Float64Array::new_scalar(value).into_inner()),
            None => return Ok(None),
        },
        DataType::Char(_) | DataType::VarChar(_) => match literal {
            Datum::String(value) => Arc::new(StringArray::new_scalar(value.as_str()).into_inner()),
            _ => return Ok(None),
        },
        DataType::Binary(_) | DataType::VarBinary(_) => match literal {
            Datum::Bytes(value) => Arc::new(BinaryArray::new_scalar(value.as_slice()).into_inner()),
            _ => return Ok(None),
        },
        DataType::Date(_) => match literal {
            Datum::Date(value) => Arc::new(Date32Array::new_scalar(*value).into_inner()),
            _ => return Ok(None),
        },
        DataType::Decimal(decimal) => match literal {
            Datum::Decimal {
                unscaled,
                precision,
                scale,
            } if *precision <= decimal.precision() && *scale == decimal.scale() => {
                let precision =
                    u8::try_from(decimal.precision()).map_err(|_| Error::Unsupported {
                        message: "Decimal precision exceeds Arrow decimal128 range".to_string(),
                    })?;
                let scale =
                    i8::try_from(decimal.scale() as i32).map_err(|_| Error::Unsupported {
                        message: "Decimal scale exceeds Arrow decimal128 range".to_string(),
                    })?;
                Arc::new(
                    Decimal128Array::new_scalar(*unscaled)
                        .into_inner()
                        .with_precision_and_scale(precision, scale)
                        .map_err(|e| Error::UnexpectedError {
                            message: format!(
                                "Failed to build decimal scalar for parquet row filter: {e}"
                            ),
                            source: Some(Box::new(e)),
                        })?,
                )
            }
            _ => return Ok(None),
        },
        DataType::Time(_)
        | DataType::Timestamp(_)
        | DataType::LocalZonedTimestamp(_)
        | DataType::Blob(_)
        | DataType::Array(_)
        | DataType::Map(_)
        | DataType::Multiset(_)
        | DataType::Row(_) => return Ok(None),
    };

    Ok(Some(Scalar::new(array)))
}

fn integer_literal(literal: &Datum) -> Option<i128> {
    match literal {
        Datum::TinyInt(value) => Some(i128::from(*value)),
        Datum::SmallInt(value) => Some(i128::from(*value)),
        Datum::Int(value) => Some(i128::from(*value)),
        Datum::Long(value) => Some(i128::from(*value)),
        _ => None,
    }
}

fn float32_literal(literal: &Datum) -> Option<f32> {
    match literal {
        Datum::Float(value) => Some(*value),
        Datum::Double(value) => {
            let casted = *value as f32;
            ((casted as f64) == *value).then_some(casted)
        }
        _ => None,
    }
}

fn float64_literal(literal: &Datum) -> Option<f64> {
    match literal {
        Datum::Float(value) => Some(f64::from(*value)),
        Datum::Double(value) => Some(*value),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Row selection helpers (DV, row ranges)
// ---------------------------------------------------------------------------

fn intersect_optional_row_selections(
    left: Option<RowSelection>,
    right: Option<RowSelection>,
) -> Option<RowSelection> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.intersection(&right)),
        (Some(selection), None) | (None, Some(selection)) => Some(selection),
        (None, None) => None,
    }
}

/// Build a Parquet [RowSelection] from inclusive `[from, to]` file-local row ranges (0-based).
fn build_row_ranges_selection(
    row_group_metadata_list: &[RowGroupMetaData],
    row_ranges: &[RowRange],
) -> RowSelection {
    let total_rows: i64 = row_group_metadata_list.iter().map(|rg| rg.num_rows()).sum();
    if total_rows == 0 {
        return vec![].into();
    }

    let file_end = total_rows - 1;
    let mut local_ranges: Vec<(usize, usize)> = row_ranges
        .iter()
        .filter_map(|r| {
            if r.to() < 0 || r.from() > file_end {
                return None;
            }
            let local_start = r.from().max(0) as usize;
            let local_end = (r.to().min(file_end) + 1) as usize;
            Some((local_start, local_end))
        })
        .collect();
    local_ranges.sort_by_key(|&(s, _)| s);

    let mut selectors: Vec<RowSelector> = Vec::new();
    let mut cursor: usize = 0;
    for (start, end) in &local_ranges {
        if *start > cursor {
            selectors.push(RowSelector::skip(*start - cursor));
        }
        let select_start = (*start).max(cursor);
        if *end > select_start {
            selectors.push(RowSelector::select(*end - select_start));
        }
        cursor = cursor.max(*end);
    }
    let total = total_rows as usize;
    if cursor < total {
        selectors.push(RowSelector::skip(total - cursor));
    }
    selectors.into()
}

// ---------------------------------------------------------------------------
// ArrowFileReader — async Parquet IO adapter
// ---------------------------------------------------------------------------

/// ArrowFileReader is a wrapper around a FileRead that impls parquets AsyncFileReader.
///
/// # TODO
///
/// [ParquetObjectReader](https://docs.rs/parquet/latest/src/parquet/arrow/async_reader/store.rs.html#64)
/// contains the following hints to speed up metadata loading, similar to iceberg, we can consider adding them to this struct:
///
/// - `metadata_size_hint`: Provide a hint as to the size of the parquet file's footer.
/// - `preload_column_index`: Load the Column Index  as part of [`Self::get_metadata`].
/// - `preload_offset_index`: Load the Offset Index as part of [`Self::get_metadata`].
struct ArrowFileReader {
    file_size: u64,
    r: Box<dyn FileRead>,
}

/// coalesce threshold: 1 MiB.
const RANGE_COALESCE_BYTES: u64 = 1024 * 1024;
/// concurrent range fetches.
const RANGE_FETCH_CONCURRENCY: usize = 10;
/// metadata prefetch hint: 512 KiB.
const METADATA_SIZE_HINT: usize = 512 * 1024;
/// Minimum range size for splitting: 4 MiB.
/// The block size used for split alignment and as the minimum split
/// granularity.  Ranges smaller than this will not be split further to
/// avoid excessive small IO requests whose per-request overhead dominates.
const IO_BLOCK_SIZE: u64 = 4 * 1024 * 1024;

impl ArrowFileReader {
    fn new(file_size: u64, r: Box<dyn FileRead>) -> Self {
        Self { file_size, r }
    }

    fn read_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        Box::pin(self.r.read(range.start..range.end).map_err(|err| {
            let err_msg = format!("{err}");
            parquet::errors::ParquetError::External(err_msg.into())
        }))
    }
}

impl MetadataFetch for ArrowFileReader {
    fn fetch(&mut self, range: Range<u64>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        self.read_bytes(range)
    }
}

impl AsyncFileReader for ArrowFileReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        self.read_bytes(range)
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'_, parquet::errors::Result<Vec<Bytes>>> {
        let coalesce_bytes = RANGE_COALESCE_BYTES;
        let concurrency = RANGE_FETCH_CONCURRENCY;

        async move {
            if ranges.is_empty() {
                return Ok(vec![]);
            }

            // Two-phase range optimization:
            // Phase 1: Merge nearby ranges based on coalesce threshold.
            let coalesced = merge_byte_ranges(&ranges, coalesce_bytes);
            // Phase 2: Split large merged ranges to utilize concurrency,
            // but only at original range boundaries.
            let fetch_ranges = split_ranges_for_concurrency(coalesced, concurrency);

            // Fetch merged ranges concurrently.
            let r = &self.r;
            let fetched: Vec<Bytes> = if fetch_ranges.len() <= concurrency {
                // All ranges fit within the concurrency limit — fire them all at once.
                futures::future::try_join_all(fetch_ranges.iter().map(|range| {
                    r.read(range.clone())
                        .map_err(|e| parquet::errors::ParquetError::External(format!("{e}").into()))
                }))
                .await?
            } else {
                // More ranges than concurrency slots — use buffered stream.
                futures::stream::iter(fetch_ranges.iter().cloned())
                    .map(|range| async move {
                        r.read(range).await.map_err(|e| {
                            parquet::errors::ParquetError::External(format!("{e}").into())
                        })
                    })
                    .buffered(concurrency)
                    .try_collect()
                    .await?
            };

            // Slice the fetched data back into the originally requested
            // ranges.  A single original range may span multiple fetch
            // chunks, so we copy from as many chunks as needed.
            let result: parquet::errors::Result<Vec<Bytes>> = ranges
                .iter()
                .map(|range| {
                    // Find the first fetch chunk whose end is past range.start.
                    let first = fetch_ranges.partition_point(|v| v.end <= range.start);
                    if first >= fetch_ranges.len() {
                        return Err(parquet::errors::ParquetError::General(format!(
                            "No fetch range covers requested range {}..{}",
                            range.start, range.end
                        )));
                    }

                    let need = (range.end - range.start) as usize;

                    // Fast path: the original range fits entirely within one
                    // fetch chunk — zero-copy slice.
                    let fr = &fetch_ranges[first];
                    if range.end <= fr.end {
                        let start = (range.start - fr.start) as usize;
                        let end = (range.end - fr.start) as usize;
                        return Ok(fetched[first].slice(start..end));
                    }

                    // Slow path: the original range spans multiple fetch
                    // chunks — copy pieces into a new buffer (mirrors Java's
                    // copyMultiBytesToBytes).
                    let mut buf = Vec::with_capacity(need);
                    let mut pos = range.start;
                    for i in first..fetch_ranges.len() {
                        if pos >= range.end {
                            break;
                        }
                        let fr = &fetch_ranges[i];
                        let chunk = &fetched[i];
                        let src_start = (pos - fr.start) as usize;
                        let src_end = ((range.end.min(fr.end)) - fr.start) as usize;
                        if src_end > chunk.len() {
                            return Err(parquet::errors::ParquetError::General(format!(
                                "Fetched data too short for range {}..{}: \
                                 chunk {}..{} has {} bytes, need up to offset {}",
                                range.start,
                                range.end,
                                fr.start,
                                fr.end,
                                chunk.len(),
                                src_end,
                            )));
                        }
                        buf.extend_from_slice(&chunk[src_start..src_end]);
                        pos = fr.end;
                    }
                    if buf.len() != need {
                        return Err(parquet::errors::ParquetError::General(format!(
                            "Assembled {} bytes for range {}..{}, expected {}",
                            buf.len(),
                            range.start,
                            range.end,
                            need,
                        )));
                    }
                    Ok(Bytes::from(buf))
                })
                .collect();
            result
        }
        .boxed()
    }

    fn get_metadata(
        &mut self,
        options: Option<&ArrowReaderOptions>,
    ) -> BoxFuture<'_, parquet::errors::Result<Arc<ParquetMetaData>>> {
        let metadata_opts = options.map(|o| o.metadata_options().clone());
        // Page index / offset index policies live on `ArrowReaderOptions`
        // directly (not inside `metadata_options`), so they have to be
        // forwarded explicitly — same as the upstream default
        // `AsyncFileReader::get_metadata` impl in
        // `parquet-58.3.0/src/arrow/async_reader/mod.rs:162-180`. Without
        // this, `with_page_index_policy(Optional)` would silently no-op.
        let column_index_policy = options.map(|o| o.column_index_policy());
        let offset_index_policy = options.map(|o| o.offset_index_policy());
        let prefetch_hint = Some(METADATA_SIZE_HINT);
        Box::pin(async move {
            let file_size = self.file_size;
            let mut reader = ParquetMetaDataReader::new()
                .with_prefetch_hint(prefetch_hint)
                .with_metadata_options(metadata_opts);
            if let Some(p) = column_index_policy {
                reader = reader.with_column_index_policy(p);
            }
            if let Some(p) = offset_index_policy {
                reader = reader.with_offset_index_policy(p);
            }
            let metadata = reader.load_and_finish(self, file_size).await?;
            Ok(Arc::new(metadata))
        })
    }
}

// ---------------------------------------------------------------------------
// Range coalescing
// ---------------------------------------------------------------------------

/// Merge nearby byte ranges to reduce the number of requests.
///
/// Ranges whose gap is ≤ `coalesce` bytes are merged into a single range.
/// The input does not need to be sorted.
fn merge_byte_ranges(ranges: &[Range<u64>], coalesce: u64) -> Vec<Range<u64>> {
    if ranges.is_empty() {
        return vec![];
    }

    let mut sorted = ranges.to_vec();
    sorted.sort_unstable_by_key(|r| r.start);

    let mut merged = Vec::with_capacity(sorted.len());
    let mut start_idx = 0;
    let mut end_idx = 1;

    while start_idx != sorted.len() {
        let mut range_end = sorted[start_idx].end;

        while end_idx != sorted.len()
            && sorted[end_idx]
                .start
                .checked_sub(range_end)
                .map(|delta| delta <= coalesce)
                .unwrap_or(true)
        {
            range_end = range_end.max(sorted[end_idx].end);
            end_idx += 1;
        }

        merged.push(sorted[start_idx].start..range_end);
        start_idx = end_idx;
        end_idx += 1;
    }

    merged
}

/// Split merged ranges into fixed-size batches to utilize concurrency,
/// Each merged range is divided into chunks of `expected_size`,
/// with the last chunk taking whatever remains.
/// Ranges smaller than `2 * IO_BLOCK_SIZE` are kept as-is to
/// avoid excessive small IO requests.
fn split_ranges_for_concurrency(merged: Vec<Range<u64>>, concurrency: usize) -> Vec<Range<u64>> {
    if merged.is_empty() || concurrency <= 1 {
        return merged;
    }

    let mut result = Vec::with_capacity(merged.len());

    for range in &merged {
        let length = range.end - range.start;
        let raw_size = IO_BLOCK_SIZE.max(length.div_ceil(concurrency as u64));
        // Round up to the nearest multiple of IO_BLOCK_SIZE (4 MB) so that
        // every split boundary is 4 MB-aligned relative to the range start.
        let expected_size = raw_size.div_ceil(IO_BLOCK_SIZE) * IO_BLOCK_SIZE;
        let min_tail_size = expected_size.max(IO_BLOCK_SIZE * 2);

        let mut offset = range.start;
        let end = range.end;

        // Align the first split boundary: if `offset` is not 4 MB-aligned,
        // emit a short head chunk so that all subsequent chunks start on a
        // 4 MB boundary.
        let misalign = offset % IO_BLOCK_SIZE;
        if misalign != 0 {
            let first_end = (offset - misalign + IO_BLOCK_SIZE).min(end);
            result.push(offset..first_end);
            offset = first_end;
        }

        loop {
            if offset >= end {
                break;
            }
            if end - offset < min_tail_size {
                result.push(offset..end);
                break;
            } else {
                result.push(offset..offset + expected_size);
                offset += expected_size;
            }
        }
    }

    result
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::build_parquet_row_filter;
    use super::ParquetFormatWriter;
    use crate::arrow::format::{FormatFileReader, FormatFileWriter};
    use crate::io::FileIOBuilder;
    use crate::spec::{DataField, DataType, Datum, IntType, PredicateBuilder};
    use arrow_array::{Int32Array, RecordBatch};
    use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
    use futures::StreamExt;
    use parquet::schema::{parser::parse_message_type, types::SchemaDescriptor};
    use std::sync::Arc;

    fn test_fields() -> Vec<DataField> {
        vec![
            DataField::new(0, "id".to_string(), DataType::Int(IntType::new())),
            DataField::new(1, "score".to_string(), DataType::Int(IntType::new())),
        ]
    }

    fn test_parquet_schema() -> SchemaDescriptor {
        SchemaDescriptor::new(Arc::new(
            parse_message_type(
                "
                message test_schema {
                  OPTIONAL INT32 id;
                  OPTIONAL INT32 score;
                }
                ",
            )
            .expect("test schema should parse"),
        ))
    }

    #[test]
    fn test_build_parquet_row_filter_supports_null_and_membership_predicates() {
        let fields = test_fields();
        let builder = PredicateBuilder::new(&fields);
        let predicates = vec![
            builder
                .is_null("id")
                .expect("is null predicate should build"),
            builder
                .is_in("score", vec![Datum::Int(7)])
                .expect("in predicate should build"),
            builder
                .is_not_in("score", vec![Datum::Int(9)])
                .expect("not in predicate should build"),
        ];

        let row_filter = build_parquet_row_filter(&test_parquet_schema(), &predicates, &fields)
            .expect("parquet row filter should build");

        assert!(row_filter.is_some());
    }

    // -----------------------------------------------------------------------
    // String predicate tests (StartsWith / EndsWith / Contains)
    // -----------------------------------------------------------------------

    fn run_string_op(
        op: super::PredicateOperator,
        column: arrow_array::ArrayRef,
        pattern: &str,
    ) -> arrow_array::BooleanArray {
        use crate::spec::VarCharType;
        let dt = DataType::VarChar(VarCharType::default());
        super::evaluate_exact_leaf_predicate(
            &column,
            &dt,
            op,
            &[Datum::String(pattern.to_string())],
        )
        .expect("string op should evaluate")
    }

    #[test]
    fn test_evaluate_starts_with_string_array() {
        use arrow_array::StringArray;
        let arr: arrow_array::ArrayRef = Arc::new(StringArray::from(vec![
            Some("foo"),
            Some("foobar"),
            Some("baz"),
            None,
        ]));
        let mask = run_string_op(super::PredicateOperator::StartsWith, arr, "foo");
        let expected = arrow_array::BooleanArray::from(vec![true, true, false, false]);
        assert_eq!(mask, expected);
    }

    #[test]
    fn test_evaluate_ends_with_large_string_array() {
        use arrow_array::LargeStringArray;
        let arr: arrow_array::ArrayRef = Arc::new(LargeStringArray::from(vec![
            Some("hello"),
            Some("world"),
            Some("ello"),
            None,
        ]));
        let mask = run_string_op(super::PredicateOperator::EndsWith, arr, "ello");
        let expected = arrow_array::BooleanArray::from(vec![true, false, true, false]);
        assert_eq!(mask, expected);
    }

    #[test]
    fn test_evaluate_contains_string_view_array() {
        use arrow_array::StringViewArray;
        let arr: arrow_array::ArrayRef = Arc::new(StringViewArray::from(vec![
            Some("apple pie"),
            Some("banana"),
            Some("crab apple"),
            None,
        ]));
        let mask = run_string_op(super::PredicateOperator::Contains, arr, "apple");
        let expected = arrow_array::BooleanArray::from(vec![true, false, true, false]);
        assert_eq!(mask, expected);
    }

    #[test]
    fn test_evaluate_like_pattern_with_underscore_and_percent() {
        use arrow_array::StringArray;
        let arr: arrow_array::ArrayRef = Arc::new(StringArray::from(vec![
            Some("foobar"),
            Some("foox"),
            Some("zoobar"),
            None,
        ]));
        // f_o% matches "foobar" (f-o-o then anything) and "foox" (f-o-o then x)
        // but not "zoobar".
        let mask = run_string_op(super::PredicateOperator::Like, arr, "f_o%");
        let expected = arrow_array::BooleanArray::from(vec![true, true, false, false]);
        assert_eq!(mask, expected);
    }

    #[test]
    fn test_evaluate_like_escaped_percent_treated_literally() {
        use arrow_array::StringArray;
        let arr: arrow_array::ArrayRef =
            Arc::new(StringArray::from(vec![Some("100%"), Some("1000"), None]));
        let mask = run_string_op(super::PredicateOperator::Like, arr, r"100\%");
        let expected = arrow_array::BooleanArray::from(vec![true, false, false]);
        assert_eq!(mask, expected);
    }

    // -----------------------------------------------------------------------
    // BETWEEN / NOT BETWEEN row-filter tests
    // -----------------------------------------------------------------------

    fn run_between(
        op: super::PredicateOperator,
        column: arrow_array::ArrayRef,
        low: i32,
        high: i32,
    ) -> arrow_array::BooleanArray {
        let dt = DataType::Int(IntType::new());
        super::evaluate_exact_leaf_predicate(
            &column,
            &dt,
            op,
            &[Datum::Int(low), Datum::Int(high)],
        )
        .expect("BETWEEN should evaluate")
    }

    #[test]
    fn test_evaluate_between_int_array() {
        let arr: arrow_array::ArrayRef =
            Arc::new(Int32Array::from(vec![Some(1), Some(5), Some(10), Some(11), None]));
        let mask = run_between(super::PredicateOperator::Between, arr, 5, 10);
        let expected =
            arrow_array::BooleanArray::from(vec![false, true, true, false, false]);
        assert_eq!(mask, expected);
    }

    #[test]
    fn test_evaluate_not_between_treats_null_as_false() {
        let arr: arrow_array::ArrayRef =
            Arc::new(Int32Array::from(vec![Some(1), Some(5), Some(10), Some(11), None]));
        let mask = run_between(super::PredicateOperator::NotBetween, arr, 5, 10);
        // NULL → false (residual filter convention; matches sanitize_filter_mask).
        let expected =
            arrow_array::BooleanArray::from(vec![true, false, false, true, false]);
        assert_eq!(mask, expected);
    }

    // -----------------------------------------------------------------------
    // merge_byte_ranges tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_merge_byte_ranges_empty() {
        assert_eq!(
            super::merge_byte_ranges(&[], 1024),
            Vec::<std::ops::Range<u64>>::new()
        );
    }

    #[test]
    fn test_merge_byte_ranges_no_coalesce() {
        // Ranges far apart should not be merged
        let ranges = vec![0..100, 1_000_000..1_000_100];
        let merged = super::merge_byte_ranges(&ranges, 1024);
        assert_eq!(merged, vec![0..100, 1_000_000..1_000_100]);
    }

    #[test]
    fn test_merge_byte_ranges_coalesce() {
        // Ranges within the gap threshold should be merged
        let ranges = vec![0..100, 200..300, 500..600];
        let merged = super::merge_byte_ranges(&ranges, 1024);
        assert_eq!(merged, vec![0..600]);
    }

    #[test]
    fn test_merge_byte_ranges_zero_coalesce_gap() {
        // With coalesce=0, ranges with a 1-byte gap should NOT merge
        let ranges = vec![0..100, 101..200];
        let merged = super::merge_byte_ranges(&ranges, 0);
        assert_eq!(merged, vec![0..100, 101..200]);
    }

    // -----------------------------------------------------------------------
    // split_ranges_for_concurrency tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_split_aligned_range_0_to_20mb() {
        // 0..20MB, concurrency=4:
        //   raw_size = max(4MB, 5MB+1) = 5MB+1
        //   expected_size = ceil((5MB+1)/4MB)*4MB = 8MB
        //   min_tail_size = max(8MB, 8MB) = 8MB
        //   No misalign. Chunks: [0..8, 8..16, 16..20]
        let mb = 1024 * 1024u64;
        #[allow(clippy::single_range_in_vec_init)]
        let merged = vec![0..20 * mb];
        let result = super::split_ranges_for_concurrency(merged, 4);
        assert_eq!(result, vec![0..8 * mb, 8 * mb..16 * mb, 16 * mb..20 * mb]);
    }

    #[test]
    fn test_split_unaligned_start_6_to_14mb() {
        // 6MB..14MB, concurrency=4:
        //   raw_size = max(4MB, 2MB+1) = 4MB
        //   expected_size = 4MB, min_tail_size = 8MB
        //   Head: 6..8MB. Loop: 8+8=16 > 14 → tail 8..14.
        //   Result: [6..8, 8..14]
        let mb = 1024 * 1024u64;
        #[allow(clippy::single_range_in_vec_init)]
        let merged = vec![6 * mb..14 * mb];
        let result = super::split_ranges_for_concurrency(merged, 4);
        assert_eq!(result, vec![6 * mb..8 * mb, 8 * mb..14 * mb]);
    }

    #[test]
    fn test_split_unaligned_start_6_to_22mb() {
        // 6MB..22MB, concurrency=4:
        //   raw_size = max(4MB, ceil(16MB/4)) = 4MB
        //   expected_size = ceil(4MB/4MB)*4MB = 4MB
        //   min_tail_size = max(4MB, 8MB) = 8MB
        //   Head: 6..8MB (misalign=2MB).
        //   Loop: 22-8=14≥8 → 8..12; 22-12=10≥8 → 12..16; 22-16=6<8 → tail 16..22.
        //   Result: [6..8, 8..12, 12..16, 16..22]
        let mb = 1024 * 1024u64;
        #[allow(clippy::single_range_in_vec_init)]
        let merged = vec![6 * mb..22 * mb];
        let result = super::split_ranges_for_concurrency(merged, 4);
        assert_eq!(
            result,
            vec![
                6 * mb..8 * mb,
                8 * mb..12 * mb,
                12 * mb..16 * mb,
                16 * mb..22 * mb,
            ]
        );
    }

    #[test]
    fn test_split_already_aligned_8_to_24mb() {
        // 8MB..24MB, concurrency=4:
        //   raw_size = max(4MB, ceil(16MB/4)) = 4MB
        //   expected_size = 4MB, min_tail_size = 8MB
        //   No misalign.
        //   Loop: 24-8=16≥8 → 8..12; 24-12=12≥8 → 12..16; 24-16=8≥8 → 16..20; 24-20=4<8 → tail 20..24.
        //   Result: [8..12, 12..16, 16..20, 20..24]
        let mb = 1024 * 1024u64;
        #[allow(clippy::single_range_in_vec_init)]
        let merged = vec![8 * mb..24 * mb];
        let result = super::split_ranges_for_concurrency(merged, 4);
        assert_eq!(
            result,
            vec![
                8 * mb..12 * mb,
                12 * mb..16 * mb,
                16 * mb..20 * mb,
                20 * mb..24 * mb,
            ]
        );
    }

    #[test]
    fn test_split_multiple_ranges() {
        // [0..20MB, 24..44MB], concurrency=4:
        //   Range 0..20MB → [0..8, 8..16, 16..20] (same as test above)
        //   Range 24..44MB (20MB): expected_size=8MB, min_tail_size=8MB, no misalign.
        //     24+8=32 ≤ 44 → 24..32; 32+8=40 ≤ 44 → 32..40; 40+8=48 > 44 → tail 40..44.
        //   Result: [0..8, 8..16, 16..20, 24..32, 32..40, 40..44]
        let mb = 1024 * 1024u64;
        let merged = vec![0..20 * mb, 24 * mb..44 * mb];
        let result = super::split_ranges_for_concurrency(merged, 4);
        assert_eq!(
            result,
            vec![
                0..8 * mb,
                8 * mb..16 * mb,
                16 * mb..20 * mb,
                24 * mb..32 * mb,
                32 * mb..40 * mb,
                40 * mb..44 * mb,
            ]
        );
    }

    #[test]
    fn test_split_empty() {
        let merged: Vec<std::ops::Range<u64>> = vec![];
        let result = super::split_ranges_for_concurrency(merged, 4);
        assert!(result.is_empty());
    }

    fn writer_arrow_schema() -> Arc<ArrowSchema> {
        Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, false),
            ArrowField::new("value", ArrowDataType::Int32, false),
        ]))
    }

    fn writer_test_batch(
        schema: &Arc<ArrowSchema>,
        ids: Vec<i32>,
        values: Vec<i32>,
    ) -> RecordBatch {
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(Int32Array::from(values)),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_parquet_writer_write_and_close() {
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let path = "memory:/test_parquet_writer_write_close.parquet";
        let output = file_io.new_output(path).unwrap();
        let schema = writer_arrow_schema();

        let mut writer: Box<dyn FormatFileWriter> = Box::new(
            ParquetFormatWriter::new(&output, schema.clone(), "zstd", 1)
                .await
                .unwrap(),
        );

        let batch = writer_test_batch(&schema, vec![1, 2, 3], vec![10, 20, 30]);
        writer.write(&batch).await.unwrap();
        writer.close().await.unwrap();

        // Verify valid parquet by reading back
        let bytes = file_io.new_input(path).unwrap().read().await.unwrap();
        let reader =
            parquet::arrow::arrow_reader::ParquetRecordBatchReader::try_new(bytes, 1024).unwrap();
        let total_rows: usize = reader.into_iter().map(|r| r.unwrap().num_rows()).sum();
        assert_eq!(total_rows, 3);
    }

    #[tokio::test]
    async fn test_parquet_writer_multiple_batches() {
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let path = "memory:/test_parquet_writer_multi.parquet";
        let output = file_io.new_output(path).unwrap();
        let schema = writer_arrow_schema();

        let mut writer: Box<dyn FormatFileWriter> = Box::new(
            ParquetFormatWriter::new(&output, schema.clone(), "zstd", 1)
                .await
                .unwrap(),
        );

        writer
            .write(&writer_test_batch(&schema, vec![1, 2], vec![10, 20]))
            .await
            .unwrap();
        writer
            .write(&writer_test_batch(&schema, vec![3, 4, 5], vec![30, 40, 50]))
            .await
            .unwrap();
        writer.close().await.unwrap();

        let bytes = file_io.new_input(path).unwrap().read().await.unwrap();
        let reader =
            parquet::arrow::arrow_reader::ParquetRecordBatchReader::try_new(bytes, 1024).unwrap();
        let total_rows: usize = reader.into_iter().map(|r| r.unwrap().num_rows()).sum();
        assert_eq!(total_rows, 5);
    }

    // -----------------------------------------------------------------------
    // Page-index page-level pruning tests (Stage 1 of P7)
    // -----------------------------------------------------------------------

    /// Write a parquet file with a single row group split into multiple data
    /// pages. `id` ranges 0..total_rows, `value` mirrors id*10. With
    /// `page_row_limit` rows per page, 80 rows / 10 rows-per-page = 8 pages.
    /// Returns the in-memory parquet bytes ready for `ParquetMetaDataReader`.
    async fn write_multi_page_parquet(page_row_limit: usize, total_rows: i32) -> Vec<u8> {
        use parquet::arrow::AsyncArrowWriter;
        let schema = writer_arrow_schema();
        let props = parquet::file::properties::WriterProperties::builder()
            .set_data_page_row_count_limit(page_row_limit)
            .set_write_batch_size(page_row_limit)
            .set_max_row_group_size(total_rows as usize)
            .build();
        let mut buf: Vec<u8> = Vec::new();
        {
            let mut writer = AsyncArrowWriter::try_new(&mut buf, schema.clone(), Some(props))
                .expect("create writer");
            let ids: Vec<i32> = (0..total_rows).collect();
            let values: Vec<i32> = ids.iter().map(|v| v * 10).collect();
            let batch = writer_test_batch(&schema, ids, values);
            writer.write(&batch).await.expect("write batch");
            writer.close().await.expect("close writer");
        }
        buf
    }

    /// Load metadata from in-memory parquet bytes with the page-index policy
    /// requested. Mirrors what `ParquetFormatReader::read_batch_stream` does
    /// when `page_index_enabled = true`.
    async fn load_metadata_with_page_index(
        bytes: &[u8],
        page_index: bool,
    ) -> Arc<parquet::file::metadata::ParquetMetaData> {
        use parquet::file::metadata::ParquetMetaDataReader;
        let mut reader = ParquetMetaDataReader::new();
        if page_index {
            reader = reader
                .with_column_index_policy(super::PageIndexPolicy::Optional)
                .with_offset_index_policy(super::PageIndexPolicy::Optional);
        }
        let bytes_owned: bytes::Bytes = bytes.to_vec().into();
        Arc::new(
            reader
                .parse_and_finish(&bytes_owned)
                .expect("parse metadata"),
        )
    }

    fn int_field(name: &str) -> DataField {
        DataField::new(0, name.to_string(), DataType::Int(IntType::new()))
    }

    fn build_int_eq_predicate(value: i32) -> super::Predicate {
        super::Predicate::Leaf {
            column: "id".to_string(),
            index: 0,
            data_type: DataType::Int(IntType::new()),
            op: super::PredicateOperator::Eq,
            literals: vec![Datum::Int(value)],
        }
    }

    #[tokio::test]
    async fn test_page_selection_eq_keeps_only_matching_page() {
        // 80 rows / 10 rows per page = 8 pages, each page covers id range
        // [page_idx*10 .. page_idx*10 + 10).
        let bytes = write_multi_page_parquet(10, 80).await;
        let metadata = load_metadata_with_page_index(&bytes, true).await;
        let fields = vec![int_field("id"), int_field("value")];

        // Eq(35) is in page 3 ([30, 40)) only.
        let predicates = vec![build_int_eq_predicate(35)];
        let sel = super::build_predicate_page_selection(&metadata, &predicates, &fields)
            .expect("page selection")
            .expect("must produce a selection");
        // Selection retains exactly one 10-row page.
        assert_eq!(sel.row_count(), 10);
    }

    #[tokio::test]
    async fn test_page_selection_eq_outside_all_pages_skips_everything() {
        let bytes = write_multi_page_parquet(10, 80).await;
        let metadata = load_metadata_with_page_index(&bytes, true).await;
        let fields = vec![int_field("id"), int_field("value")];

        // 1000 lies past every page's max (max == 79).
        let predicates = vec![build_int_eq_predicate(1000)];
        let sel = super::build_predicate_page_selection(&metadata, &predicates, &fields)
            .expect("page selection")
            .expect("must produce a selection");
        assert_eq!(sel.row_count(), 0);
    }

    #[tokio::test]
    async fn test_page_selection_between_keeps_overlapping_pages() {
        let bytes = write_multi_page_parquet(10, 80).await;
        let metadata = load_metadata_with_page_index(&bytes, true).await;
        let fields = vec![int_field("id"), int_field("value")];

        // Between [25, 44] overlaps page 2 ([20,30)), page 3 ([30,40)),
        // and page 4 ([40,50)) — 3 pages × 10 rows = 30 rows.
        let predicates = vec![super::Predicate::Leaf {
            column: "id".to_string(),
            index: 0,
            data_type: DataType::Int(IntType::new()),
            op: super::PredicateOperator::Between,
            literals: vec![Datum::Int(25), Datum::Int(44)],
        }];
        let sel = super::build_predicate_page_selection(&metadata, &predicates, &fields)
            .expect("page selection")
            .expect("must produce a selection");
        assert_eq!(sel.row_count(), 30);
    }

    #[tokio::test]
    async fn test_page_selection_neq_falls_open() {
        let bytes = write_multi_page_parquet(10, 80).await;
        let metadata = load_metadata_with_page_index(&bytes, true).await;
        let fields = vec![int_field("id"), int_field("value")];

        // NotEq is conservative under stats: every page contains other values
        // even if it contains the literal, so no page can ever be excluded.
        let predicates = vec![super::Predicate::Leaf {
            column: "id".to_string(),
            index: 0,
            data_type: DataType::Int(IntType::new()),
            op: super::PredicateOperator::NotEq,
            literals: vec![Datum::Int(35)],
        }];
        // No page is skipped → helper returns None to signal "selection
        // unchanged from full row group".
        let sel = super::build_predicate_page_selection(&metadata, &predicates, &fields)
            .expect("page selection");
        assert!(sel.is_none(), "NotEq must not skip any page (got {sel:?})");
    }

    #[tokio::test]
    async fn test_page_selection_returns_none_when_page_index_disabled() {
        let bytes = write_multi_page_parquet(10, 80).await;
        // Load metadata WITHOUT page-index policy, simulating a reader where
        // the toggle is off.
        let metadata = load_metadata_with_page_index(&bytes, false).await;
        let fields = vec![int_field("id"), int_field("value")];

        let predicates = vec![build_int_eq_predicate(35)];
        let sel = super::build_predicate_page_selection(&metadata, &predicates, &fields)
            .expect("page selection");
        assert!(
            sel.is_none(),
            "without page index loaded, helper must fall open (got {sel:?})"
        );
    }

    // -----------------------------------------------------------------------
    // Bloom filter row-group prune tests (Stage 2 of P7)
    // -----------------------------------------------------------------------

    /// Write a parquet file with bloom filters enabled on the `id` column.
    /// Returns (file_io, path, file_size). One row group, 3 rows so the
    /// helpers exercise per-row-group bloom checks.
    async fn write_parquet_with_bloom(
        ids: Vec<i32>,
        values: Vec<i32>,
    ) -> (crate::io::FileIO, String, u64) {
        use parquet::arrow::AsyncArrowWriter;
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let path = "memory:/test_parquet_bloom.parquet".to_string();
        let output = file_io.new_output(&path).unwrap();

        let schema = writer_arrow_schema();
        let props = parquet::file::properties::WriterProperties::builder()
            .set_bloom_filter_enabled(true)
            .build();

        let async_write = output.async_writer().await.unwrap();
        let mut writer = AsyncArrowWriter::try_new(async_write, schema.clone(), Some(props))
            .expect("create async writer with bloom");
        let batch = writer_test_batch(&schema, ids, values);
        writer.write(&batch).await.expect("write batch");
        writer.close().await.expect("close writer");

        let metadata = file_io.new_input(&path).unwrap().metadata().await.unwrap();
        (file_io, path, metadata.size)
    }

    fn int_eq_file_predicate(value: i32) -> super::FilePredicates {
        let fields = vec![int_field("id"), int_field("value")];
        super::FilePredicates {
            predicates: vec![build_int_eq_predicate(value)],
            file_fields: fields,
        }
    }

    fn int_in_file_predicate(values: Vec<i32>) -> super::FilePredicates {
        let fields = vec![int_field("id"), int_field("value")];
        let literals = values.into_iter().map(Datum::Int).collect();
        let pred = super::Predicate::Leaf {
            column: "id".to_string(),
            index: 0,
            data_type: DataType::Int(IntType::new()),
            op: super::PredicateOperator::In,
            literals,
        };
        super::FilePredicates {
            predicates: vec![pred],
            file_fields: fields,
        }
    }

    async fn read_count(
        reader: super::ParquetFormatReader,
        file_io: &crate::io::FileIO,
        path: &str,
        file_size: u64,
        predicates: super::FilePredicates,
    ) -> usize {
        let read_fields = vec![int_field("id"), int_field("value")];
        let input_reader = file_io.new_input(path).unwrap().reader().await.unwrap();
        let mut stream = reader
            .read_batch_stream(
                Box::new(input_reader),
                file_size,
                &read_fields,
                Some(&predicates),
                None,
                None,
            )
            .await
            .unwrap();
        let mut total = 0;
        while let Some(b) = stream.next().await {
            total += b.unwrap().num_rows();
        }
        total
    }

    #[tokio::test]
    async fn test_bloom_filter_skips_row_group_when_value_absent() {
        let (file_io, path, file_size) =
            write_parquet_with_bloom(vec![1, 2, 3], vec![10, 20, 30]).await;
        // Predicate Eq(999) cannot match — bloom should let us skip the
        // whole row group with `bloom_filter_enabled = true`.
        let bloom_on = super::ParquetFormatReader {
            page_index_enabled: false,
            bloom_filter_enabled: true,
        };
        let bloom_off = super::ParquetFormatReader {
            page_index_enabled: false,
            bloom_filter_enabled: false,
        };
        let count_on = read_count(bloom_on, &file_io, &path, file_size, int_eq_file_predicate(999))
            .await;
        let count_off = read_count(
            bloom_off,
            &file_io,
            &path,
            file_size,
            int_eq_file_predicate(999),
        )
        .await;
        // Bloom on must produce zero rows. Bloom off relies on per-row
        // RowFilter to drop the rows; the row-group itself is read but the
        // RowFilter rejects every row.
        assert_eq!(count_on, 0, "bloom on: row group must be skipped");
        assert_eq!(count_off, 0, "bloom off: row filter still rejects all rows");
    }

    #[tokio::test]
    async fn test_bloom_filter_keeps_row_group_when_value_present() {
        let (file_io, path, file_size) =
            write_parquet_with_bloom(vec![1, 2, 3], vec![10, 20, 30]).await;
        let bloom_on = super::ParquetFormatReader {
            page_index_enabled: false,
            bloom_filter_enabled: true,
        };
        // Eq(2) is in the data — bloom must say "may be present", and the
        // row filter then keeps exactly the matching row.
        let count = read_count(bloom_on, &file_io, &path, file_size, int_eq_file_predicate(2))
            .await;
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn test_bloom_filter_in_with_at_least_one_present_keeps_group() {
        let (file_io, path, file_size) =
            write_parquet_with_bloom(vec![1, 2, 3], vec![10, 20, 30]).await;
        let bloom_on = super::ParquetFormatReader {
            page_index_enabled: false,
            bloom_filter_enabled: true,
        };
        // In(999, 2) — 2 is in data, 999 is not. Bloom must keep the group
        // because at least one literal may be present.
        let count = read_count(
            bloom_on,
            &file_io,
            &path,
            file_size,
            int_in_file_predicate(vec![999, 2]),
        )
        .await;
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn test_bloom_filter_in_with_all_absent_skips_group() {
        let (file_io, path, file_size) =
            write_parquet_with_bloom(vec![1, 2, 3], vec![10, 20, 30]).await;
        let bloom_on = super::ParquetFormatReader {
            page_index_enabled: false,
            bloom_filter_enabled: true,
        };
        let count = read_count(
            bloom_on,
            &file_io,
            &path,
            file_size,
            int_in_file_predicate(vec![100, 200, 300]),
        )
        .await;
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn test_bloom_filter_lt_predicate_falls_open() {
        let (file_io, path, file_size) =
            write_parquet_with_bloom(vec![1, 2, 3], vec![10, 20, 30]).await;
        let bloom_on = super::ParquetFormatReader {
            page_index_enabled: false,
            bloom_filter_enabled: true,
        };
        let fields = vec![int_field("id"), int_field("value")];
        let pred = super::Predicate::Leaf {
            column: "id".to_string(),
            index: 0,
            data_type: DataType::Int(IntType::new()),
            op: super::PredicateOperator::Lt,
            literals: vec![Datum::Int(3)],
        };
        let fp = super::FilePredicates {
            predicates: vec![pred],
            file_fields: fields,
        };
        // Bloom can only refute Eq/In; for Lt the helper must fall open and
        // let the row filter handle the per-row check (id < 3 → 2 rows).
        let count = read_count(bloom_on, &file_io, &path, file_size, fp).await;
        assert_eq!(count, 2);
    }

    /// Endianness regression: numeric `Datum`s must be hashed using
    /// little-endian bytes so the read path matches what parquet-mr (and the
    /// Rust writer on LE platforms) inserted into the bloom filter.
    ///
    /// `parquet-mr/.../BlockSplitBloomFilter.java` builds a `ByteBuffer` with
    /// `ByteOrder.LITTLE_ENDIAN` before XXH64; `parquet-58.3.0` writer side
    /// uses `gen_as_bytes!` (raw memory) on `ParquetValueType` primitives —
    /// equivalent to LE on x86 / aarch64 and disagrees on big-endian CPUs.
    /// Going through `Sbbf::check::<i32>` reproduces the writer's bug on
    /// big-endian, so the read path now feeds explicit `to_le_bytes()` to
    /// `Sbbf::check::<[u8]>` and stays correct on every architecture as long
    /// as the writer stuck to spec / LE.
    ///
    /// We can't actually run on a BE CPU here, so the test pins the LE byte
    /// layout: build a bloom that contains the **LE bytes** of a known i32
    /// (the spec-correct encoding parquet-mr uses) and verify our helper
    /// produces a hit. If anyone ever reverts to `Sbbf::check::<i32>` this
    /// test still passes on LE but would fail on BE; the secondary assertion
    /// pins the helper to a specific byte sequence so a host-byte-order
    /// regression on LE is also caught.
    #[test]
    fn test_bloom_check_uses_little_endian_for_numeric_datums() {
        let mut sbbf = parquet::bloom_filter::Sbbf::new_with_num_of_bytes(1024);
        // Insert via the same byte path the writer uses on a LE platform:
        // raw-memory bytes of a primitive are equivalent to `to_le_bytes()`
        // on LE CPUs and equivalent to the parquet-format spec's required
        // LE encoding on every CPU.
        let value: i32 = 42;
        let value_bytes = value.to_le_bytes();
        sbbf.insert(&value_bytes[..]);

        let dt = DataType::Int(IntType::new());
        // Hit: `Datum::Int(42)` must hash the same byte sequence we inserted.
        let verdict = super::bloom_check_datum_against(&sbbf, &Datum::Int(42), &dt);
        assert_eq!(
            verdict,
            Some(true),
            "Datum::Int(42) must produce LE bytes [42, 0, 0, 0] and find the inserted entry"
        );
        // Miss: a different value must produce a different hash.
        let miss = super::bloom_check_datum_against(&sbbf, &Datum::Int(43), &dt);
        // Bloom can return false negatives only when the value is absent —
        // here `43` was never inserted, so `false` is the only deterministic
        // outcome (false-positive rate notwithstanding, the test bloom is
        // sparse enough at 1024 bytes / 1 entry).
        assert_eq!(miss, Some(false));

        // BigInt path: same shape but i64.
        let mut sbbf64 = parquet::bloom_filter::Sbbf::new_with_num_of_bytes(1024);
        let v64: i64 = 0x0102030405060708;
        sbbf64.insert(&v64.to_le_bytes()[..]);
        let dt64 = DataType::BigInt(crate::spec::BigIntType::new());
        assert_eq!(
            super::bloom_check_datum_against(&sbbf64, &Datum::Long(v64), &dt64),
            Some(true)
        );
    }
}
