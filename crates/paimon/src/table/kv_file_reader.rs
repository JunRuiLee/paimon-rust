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

//! Key-value file reader for primary-key tables using sort-merge with LoserTree.
//!
//! Each data file in a split is read as a separate sorted stream. The streams
//! are merged by primary key using a LoserTree, and rows with the same key are
//! deduplicated by keeping the one with the highest `_SEQUENCE_NUMBER`.
//!
//! Reference: Java Paimon `SortMergeReaderWithMinHeap`.

use super::data_file_reader::DataFileReader;
use super::sort_merge::{
    DeduplicateMergeFunction, PartialUpdateMergeFunction, SortMergeReaderBuilder,
};
use crate::arrow::build_target_arrow_schema;
use crate::io::FileIO;
use crate::spec::{
    BigIntType, DataField, DataType as PaimonDataType, MergeEngine, Predicate, TinyIntType,
    SEQUENCE_NUMBER_FIELD_ID, SEQUENCE_NUMBER_FIELD_NAME, VALUE_KIND_FIELD_ID,
    VALUE_KIND_FIELD_NAME,
};
use crate::table::schema_manager::SchemaManager;
use crate::table::ArrowRecordBatchStream;
use crate::{DataSplit, Error};
use arrow_array::RecordBatch;

use async_stream::try_stream;
use futures::StreamExt;
use std::collections::HashMap;

/// Reads primary-key table data files using sort-merge deduplication.
pub(crate) struct KeyValueFileReader {
    file_io: FileIO,
    config: KeyValueReadConfig,
}

/// Configuration for [`KeyValueFileReader`], grouping table schema and
/// key/predicate parameters.
pub(crate) struct KeyValueReadConfig {
    pub table_name: String,
    pub table_options: HashMap<String, String>,
    pub schema_manager: SchemaManager,
    pub table_schema_id: i64,
    pub table_fields: Vec<DataField>,
    pub read_type: Vec<DataField>,
    pub predicates: Vec<Predicate>,
    pub primary_keys: Vec<String>,
    pub merge_engine: MergeEngine,
    pub sequence_fields: Vec<String>,
    /// Rows per batch for both the inner parquet reader and the sort-merge
    /// output. Sourced from `CoreOptions::read_batch_size()` by callers.
    pub batch_size: usize,
}

impl KeyValueFileReader {
    pub(crate) fn new(file_io: FileIO, config: KeyValueReadConfig) -> Self {
        // Only keep predicates that reference primary key columns.
        // Non-PK predicates applied before merge can cause incorrect results.
        // Use project_field_index_inclusive: AND keeps PK children, OR requires all PK.
        let pk_set: std::collections::HashSet<&str> =
            config.primary_keys.iter().map(|s| s.as_str()).collect();
        let mapping: Vec<Option<usize>> = config
            .table_fields
            .iter()
            .enumerate()
            .map(|(i, f)| {
                if pk_set.contains(f.name()) {
                    Some(i)
                } else {
                    None
                }
            })
            .collect();
        let pk_predicates = config
            .predicates
            .into_iter()
            .filter_map(|p| p.project_field_index_inclusive(&mapping))
            .collect();

        Self {
            file_io,
            config: KeyValueReadConfig {
                predicates: pk_predicates,
                ..config
            },
        }
    }

    fn new_merge_function(
        merge_engine: MergeEngine,
        table_options: &HashMap<String, String>,
        table_name: &str,
    ) -> crate::Result<Box<dyn super::sort_merge::MergeFunction>> {
        match merge_engine {
            MergeEngine::Deduplicate => Ok(Box::new(DeduplicateMergeFunction)),
            MergeEngine::PartialUpdate => Ok(Box::new(PartialUpdateMergeFunction::new(
                table_options,
                table_name,
            )?)),
            MergeEngine::FirstRow => Err(Error::Unsupported {
                message: "KeyValueFileReader does not support merge-engine=first-row; first-row reads should use the non-KV path".to_string(),
            }),
            MergeEngine::VersionedPartialUpdate => Ok(Box::new(
                super::versioned_partial_update::VersionedPartialUpdateMergeFunction::new(
                    table_options,
                )?,
            )),
        }
    }

    pub fn read(self, data_splits: &[DataSplit]) -> crate::Result<ArrowRecordBatchStream> {
        // Build the internal read type for thin-mode files.
        // Physical file schema: [_SEQUENCE_NUMBER, _VALUE_KIND, all_user_cols...]
        // We need: _SEQ + _VK + union(read_type, primary_keys)
        let seq_field = DataField::new(
            SEQUENCE_NUMBER_FIELD_ID,
            SEQUENCE_NUMBER_FIELD_NAME.to_string(),
            PaimonDataType::BigInt(BigIntType::new()),
        );
        let value_kind_field = DataField::new(
            VALUE_KIND_FIELD_ID,
            VALUE_KIND_FIELD_NAME.to_string(),
            PaimonDataType::TinyInt(TinyIntType::new()),
        );

        let key_names: std::collections::HashSet<&str> = self
            .config
            .primary_keys
            .iter()
            .map(|s| s.as_str())
            .collect();

        // Collect key fields from table schema.
        let key_fields: Vec<DataField> = self
            .config
            .primary_keys
            .iter()
            .map(|pk| {
                self.config
                    .table_fields
                    .iter()
                    .find(|f| f.name() == pk)
                    .cloned()
                    .ok_or_else(|| Error::UnexpectedError {
                        message: format!("Primary key column '{pk}' not found in table schema"),
                        source: None,
                    })
            })
            .collect::<crate::Result<Vec<_>>>()?;

        // User columns = read_type fields + any key fields not already in read_type
        //              + (versioned-partial-update only) any multi-version fields
        //              + any sequence fields not already included.
        let user_fields = compute_merge_read_fields(
            &self.config.read_type,
            &key_fields,
            &self.config.table_fields,
            &self.config.sequence_fields,
            self.config.merge_engine,
        )?;

        // Internal read type: [_SEQ, _VK, user_fields...]
        let mut internal_read_type: Vec<DataField> = Vec::new();
        internal_read_type.push(seq_field);
        internal_read_type.push(value_kind_field);
        internal_read_type.extend(user_fields.clone());

        let internal_schema = build_target_arrow_schema(&internal_read_type)?;

        // Output schema: user's read_type order
        let output_schema = build_target_arrow_schema(&self.config.read_type)?;

        // Indices within internal_schema (offset 2 for _SEQ and _VK).
        let seq_index = 0;
        let value_kind_index = 1;
        let key_indices: Vec<usize> = self
            .config
            .primary_keys
            .iter()
            .map(|pk| {
                user_fields
                    .iter()
                    .position(|f| f.name() == pk)
                    .map(|p| p + 2)
                    .unwrap()
            })
            .collect();
        let value_fields: Vec<DataField> = user_fields
            .iter()
            .filter(|f| !key_names.contains(f.name()))
            .cloned()
            .collect();
        let value_indices: Vec<usize> = user_fields
            .iter()
            .enumerate()
            .filter(|(_, f)| !key_names.contains(f.name()))
            .map(|(i, _)| i + 2)
            .collect();

        // If sequence.field is configured, find each field's index in the internal schema.
        let user_sequence_indices: Vec<usize> = self
            .config
            .sequence_fields
            .iter()
            .filter_map(|sf| {
                user_fields
                    .iter()
                    .position(|f| f.name() == sf.as_str())
                    .map(|p| p + 2)
            })
            .collect();

        // Build the reorder mapping: merge output is [keys..., values...],
        // but user wants them in read_type order.
        let num_keys = key_fields.len();
        let mut reorder_map: Vec<usize> = vec![0; self.config.read_type.len()];
        for (out_idx, field) in self.config.read_type.iter().enumerate() {
            if key_names.contains(field.name()) {
                // Find position in key_fields
                let key_pos = key_fields
                    .iter()
                    .position(|kf| kf.name() == field.name())
                    .unwrap();
                reorder_map[out_idx] = key_pos;
            } else {
                // Find position in value_fields
                let val_pos = value_fields
                    .iter()
                    .position(|vf| vf.name() == field.name())
                    .unwrap();
                reorder_map[out_idx] = num_keys + val_pos;
            }
        }

        let splits: Vec<DataSplit> = data_splits.to_vec();
        let file_io = self.file_io;
        let merge_engine = self.config.merge_engine;
        let schema_manager = self.config.schema_manager;
        let table_schema_id = self.config.table_schema_id;
        let table_fields = self.config.table_fields;
        let table_name = self.config.table_name;
        let table_options = self.config.table_options;
        let predicates = self.config.predicates;
        let batch_size = self.config.batch_size;

        // Build the merge output schema (keys + values, no system columns).
        let mut merge_output_fields: Vec<DataField> = Vec::new();
        merge_output_fields.extend(key_fields);
        merge_output_fields.extend(value_fields);
        let merge_output_schema = build_target_arrow_schema(&merge_output_fields)?;

        Ok(try_stream! {
            for split in &splits {
                // DV mode should not reach KeyValueFileReader.
                if split
                    .data_deletion_files()
                    .is_some_and(|files| files.iter().any(Option::is_some))
                {
                    Err(Error::Unsupported {
                        message: "KeyValueFileReader does not support deletion vectors".to_string(),
                    })?;
                }

                // Create one stream per data file.
                let mut file_streams: Vec<ArrowRecordBatchStream> = Vec::new();
                // Per-stream metadata aligned with file_streams; consumed by
                // sort-merge to drive versioned-partial-update ordering and
                // per-file UPSERT/IGNORE mode dispatch. For non-versioned
                // engines the values pass through but the merge function
                // ignores them.
                let mut stream_metas: Vec<super::sort_merge::StreamMeta> = Vec::new();

                for file_meta in split.data_files().to_vec() {
                    let data_fields: Option<Vec<DataField>> = if file_meta.schema_id != table_schema_id {
                        let data_schema = schema_manager.schema(file_meta.schema_id).await?;
                        Some(data_schema.fields().to_vec())
                    } else {
                        None
                    };

                    // Snapshot id: legacy files lacking _COMMIT_SNAPSHOT_ID
                    // fall back to the read-side sentinel UNKNOWN, which is
                    // smaller than every real snapshot id so cross-file ordering
                    // still favours newer rows.
                    let snapshot_id = file_meta
                        .commit_snapshot_id
                        .unwrap_or(crate::spec::COMMIT_SNAPSHOT_ID_UNKNOWN);
                    // Merge mode: only `None` falls back to UPSERT (matches
                    // Java `DataFileMetaSerializer` which writes null for
                    // UPSERT and a valid byte otherwise). Unknown byte values
                    // are an integrity error and bubble up — silently
                    // downgrading to UPSERT would corrupt merge results.
                    let merge_mode = crate::spec::VersionedMergeMode::from_optional_byte(
                        file_meta.merge_mode,
                    )?;
                    stream_metas.push(super::sort_merge::StreamMeta {
                        snapshot_id,
                        merge_mode,
                    });

                    let reader = DataFileReader::new(
                        file_io.clone(),
                        schema_manager.clone(),
                        table_schema_id,
                        table_fields.clone(),
                        internal_read_type.clone(),
                        predicates.clone(),
                        batch_size,
                    );

                    let stream = reader.read_single_file_stream(
                        split,
                        file_meta,
                        data_fields,
                        None,
                        None,
                    )?;
                    file_streams.push(stream);
                }

                if file_streams.is_empty() {
                    continue;
                }

                // Always go through sort-merge even for single file,
                // because a single file may contain duplicate keys.
                let mut merge_stream = SortMergeReaderBuilder::new(
                    file_streams,
                    internal_schema.clone(),
                    key_indices.clone(),
                    seq_index,
                    value_kind_index,
                    user_sequence_indices.clone(),
                    value_indices.clone(),
                    merge_output_schema.clone(),
                    Self::new_merge_function(merge_engine, &table_options, &table_name)?,
                )
                .with_batch_size(batch_size)
                .with_stream_metas(stream_metas)
                .build()?;

                while let Some(batch) = merge_stream.next().await {
                    let batch = batch?;
                    // Reorder columns from [keys..., values...] to read_type order.
                    let columns: Vec<_> = reorder_map
                        .iter()
                        .map(|&src| batch.column(src).clone())
                        .collect();
                    let reordered = RecordBatch::try_new(output_schema.clone(), columns)
                        .map_err(|e| Error::UnexpectedError {
                            message: format!("Failed to reorder merged RecordBatch: {e}"),
                            source: Some(Box::new(e)),
                        })?;
                    yield reordered;
                }
            }
        }
        .boxed())
    }
}

/// Build the field list consumed by sort-merge for one read pass:
/// `requested read_type + missing PK + (VPU only) missing MV + missing
/// sequence fields`. Mirrors Java
/// `VersionedPartialUpdateMergeFunction.Factory.adjustReadType` for the
/// MV-completion step. The order is stable: user projection first, then
/// the implicit補齐 in PK → MV → sequence order.
fn compute_merge_read_fields(
    read_type: &[DataField],
    key_fields: &[DataField],
    table_fields: &[DataField],
    sequence_fields: &[String],
    merge_engine: MergeEngine,
) -> crate::Result<Vec<DataField>> {
    let read_type_names: std::collections::HashSet<&str> =
        read_type.iter().map(|f| f.name()).collect();
    let mut user_fields: Vec<DataField> = read_type.to_vec();

    for kf in key_fields {
        if !read_type_names.contains(kf.name()) {
            user_fields.push(kf.clone());
        }
    }

    if merge_engine == MergeEngine::VersionedPartialUpdate {
        for tf in table_fields {
            if user_fields.iter().any(|f| f.name() == tf.name()) {
                continue;
            }
            if crate::spec::is_multi_version_type(tf.data_type()) {
                user_fields.push(tf.clone());
            }
        }
    }

    for sf_name in sequence_fields {
        if user_fields.iter().all(|f| f.name() != sf_name.as_str()) {
            let sf = table_fields
                .iter()
                .find(|f| f.name() == sf_name.as_str())
                .cloned()
                .ok_or_else(|| Error::UnexpectedError {
                    message: format!("Sequence field '{sf_name}' not found in table schema"),
                    source: None,
                })?;
            user_fields.push(sf);
        }
    }

    Ok(user_fields)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{IntType, MapType, RowType, VarCharType};

    fn pk_field(id: i32, name: &str) -> DataField {
        DataField::new(id, name.to_string(), PaimonDataType::Int(IntType::with_nullable(false)))
    }

    fn int_field(id: i32, name: &str) -> DataField {
        DataField::new(id, name.to_string(), PaimonDataType::Int(IntType::new()))
    }

    fn mv_field(id: i32, name: &str) -> DataField {
        let varchar = PaimonDataType::VarChar(VarCharType::new(VarCharType::MAX_LENGTH).unwrap());
        let int_t = PaimonDataType::Int(IntType::new());
        DataField::new(
            id,
            name.to_string(),
            PaimonDataType::Row(RowType::new(vec![
                DataField::new(id + 100, "latest_version".to_string(), varchar.clone()),
                DataField::new(id + 101, "latest_value".to_string(), int_t.clone()),
                DataField::new(
                    id + 102,
                    "all_versioned_values".to_string(),
                    PaimonDataType::Map(MapType::new(varchar, int_t)),
                ),
            ])),
        )
    }

    /// User projects only the PK; merge schema must still carry the MV
    /// column so the accumulator is fed.
    #[test]
    fn test_compute_merge_read_fields_pk_only_supplements_mv_for_vpu() {
        let pk = pk_field(0, "id");
        let single = int_field(1, "val");
        let mv = mv_field(2, "mv");
        let table_fields = vec![pk.clone(), single, mv.clone()];
        let read_type = vec![pk.clone()];
        let key_fields = vec![pk.clone()];

        let merged = compute_merge_read_fields(
            &read_type,
            &key_fields,
            &table_fields,
            &[],
            MergeEngine::VersionedPartialUpdate,
        )
        .unwrap();
        let names: Vec<&str> = merged.iter().map(|f| f.name()).collect();
        // PK first (user projection), MV補齐 跟在 PK 之后。
        assert_eq!(names, vec!["id", "mv"]);
    }

    /// User projects a single-version column only; merge schema must補 PK
    /// and MV.
    #[test]
    fn test_compute_merge_read_fields_single_version_supplements_pk_and_mv() {
        let pk = pk_field(0, "id");
        let single = int_field(1, "val");
        let mv = mv_field(2, "mv");
        let table_fields = vec![pk.clone(), single.clone(), mv.clone()];
        let read_type = vec![single.clone()];
        let key_fields = vec![pk.clone()];

        let merged = compute_merge_read_fields(
            &read_type,
            &key_fields,
            &table_fields,
            &[],
            MergeEngine::VersionedPartialUpdate,
        )
        .unwrap();
        let names: Vec<&str> = merged.iter().map(|f| f.name()).collect();
        // val (user) → id (PK補齐) → mv (MV補齐).
        assert_eq!(names, vec!["val", "id", "mv"]);
    }

    /// User projects the MV column directly; nothing needs補.
    #[test]
    fn test_compute_merge_read_fields_user_already_projects_mv() {
        let pk = pk_field(0, "id");
        let mv = mv_field(2, "mv");
        let table_fields = vec![pk.clone(), mv.clone()];
        let read_type = vec![pk.clone(), mv.clone()];
        let key_fields = vec![pk.clone()];

        let merged = compute_merge_read_fields(
            &read_type,
            &key_fields,
            &table_fields,
            &[],
            MergeEngine::VersionedPartialUpdate,
        )
        .unwrap();
        let names: Vec<&str> = merged.iter().map(|f| f.name()).collect();
        assert_eq!(names, vec!["id", "mv"]);
    }

    /// Non-VPU engine: MV column shape exists in the schema but should not
    /// be auto-supplemented (the merge function does not maintain MV state).
    #[test]
    fn test_compute_merge_read_fields_non_vpu_skips_mv_supplement() {
        let pk = pk_field(0, "id");
        let mv = mv_field(2, "mv");
        let table_fields = vec![pk.clone(), mv];
        let read_type = vec![pk.clone()];
        let key_fields = vec![pk.clone()];

        let merged = compute_merge_read_fields(
            &read_type,
            &key_fields,
            &table_fields,
            &[],
            MergeEngine::Deduplicate,
        )
        .unwrap();
        let names: Vec<&str> = merged.iter().map(|f| f.name()).collect();
        assert_eq!(names, vec!["id"]);
    }

    /// Sequence fields are appended last regardless of MV補齐 order.
    #[test]
    fn test_compute_merge_read_fields_sequence_field_after_mv() {
        let pk = pk_field(0, "id");
        let seq = int_field(1, "seq");
        let mv = mv_field(2, "mv");
        let table_fields = vec![pk.clone(), seq.clone(), mv.clone()];
        let read_type = vec![pk.clone()];
        let key_fields = vec![pk.clone()];
        let sequence_fields = vec!["seq".to_string()];

        let merged = compute_merge_read_fields(
            &read_type,
            &key_fields,
            &table_fields,
            &sequence_fields,
            MergeEngine::VersionedPartialUpdate,
        )
        .unwrap();
        let names: Vec<&str> = merged.iter().map(|f| f.name()).collect();
        assert_eq!(names, vec!["id", "mv", "seq"]);
    }
}
