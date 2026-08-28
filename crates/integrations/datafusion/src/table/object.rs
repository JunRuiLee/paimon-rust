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

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{
    new_null_array, ArrayRef, Int64Array, RecordBatch, RecordBatchOptions, StringArray,
    StringViewArray,
};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::{PartitionStream, StreamingTableExec};
use datafusion::physical_plan::ExecutionPlan;
use futures::{StreamExt, TryStreamExt};
use paimon::table::{ObjectEntry, ObjectTable};

use crate::error::to_datafusion_error;

use super::datafusion_arrow_schema;

/// DataFusion provider for a native read-only Paimon object table.
#[derive(Debug, Clone)]
pub(crate) struct ObjectTableProvider {
    table: ObjectTable,
    schema: SchemaRef,
}

impl ObjectTableProvider {
    pub(crate) fn try_new(table: ObjectTable, schema_force_view_types: bool) -> DFResult<Self> {
        let schema = datafusion_arrow_schema(&ObjectTable::fields(), schema_force_view_types)?;
        Ok(Self { table, schema })
    }
}

#[derive(Debug, Clone)]
struct ObjectPartitionStream {
    table: ObjectTable,
    projection: Arc<[usize]>,
    schema: SchemaRef,
    limit: Option<usize>,
}

impl PartitionStream for ObjectPartitionStream {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let table = self.table.clone();
        let projection = Arc::clone(&self.projection);
        let schema = Arc::clone(&self.schema);
        let output_schema = Arc::clone(&self.schema);
        let limit = self.limit;
        let batch_size = ctx.session_config().batch_size().max(1);
        let future = async move {
            let entries = table
                .stream_objects(limit)
                .await
                .map_err(to_datafusion_error)?;
            let batch_schema = Arc::clone(&schema);
            let batches = entries
                .map(|entry| entry.map_err(to_datafusion_error))
                .chunks(batch_size)
                .map(move |chunk| {
                    let entries = chunk.into_iter().collect::<DFResult<Vec<_>>>()?;
                    object_entries_to_batch(&entries, &projection, &batch_schema)
                });
            Ok::<_, DataFusionError>(RecordBatchStreamAdapter::new(schema, Box::pin(batches)))
        };

        Box::pin(RecordBatchStreamAdapter::new(
            output_schema,
            futures::stream::once(future).try_flatten(),
        ))
    }
}

#[async_trait]
impl TableProvider for ObjectTableProvider {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let projection = projection
            .cloned()
            .unwrap_or_else(|| (0..self.schema.fields().len()).collect());
        let projected_schema = Arc::new(self.schema.project(&projection)?);
        let partition: Arc<dyn PartitionStream> = Arc::new(ObjectPartitionStream {
            table: self.table.clone(),
            projection: projection.into(),
            schema: Arc::clone(&projected_schema),
            limit,
        });

        Ok(Arc::new(StreamingTableExec::try_new(
            projected_schema,
            vec![partition],
            None,
            std::iter::empty(),
            false,
            limit,
        )?))
    }

    async fn insert_into(
        &self,
        _state: &dyn Session,
        _input: Arc<dyn ExecutionPlan>,
        _insert_op: InsertOp,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::NotImplemented(format!(
            "Object table '{}' is read-only",
            self.table.identifier().full_name()
        )))
    }
}

fn object_entries_to_batch(
    entries: &[ObjectEntry],
    projection: &[usize],
    schema: &SchemaRef,
) -> DFResult<RecordBatch> {
    let columns = projection
        .iter()
        .enumerate()
        .map(|(output_index, source_index)| -> DFResult<ArrayRef> {
            let data_type = schema.field(output_index).data_type();
            Ok(match source_index {
                0 => string_array(entries.iter().map(ObjectEntry::path), data_type),
                1 => string_array(entries.iter().map(ObjectEntry::name), data_type),
                2 => Arc::new(Int64Array::from_iter_values(
                    entries.iter().map(ObjectEntry::length),
                )),
                3 => Arc::new(Int64Array::from_iter_values(
                    entries.iter().map(ObjectEntry::mtime),
                )),
                4 => Arc::new(Int64Array::from_iter_values(
                    entries.iter().map(ObjectEntry::atime),
                )),
                5 => owner_array(entries, data_type),
                index => {
                    return Err(DataFusionError::Internal(format!(
                        "Object table projection index {index} is out of range"
                    )));
                }
            })
        })
        .collect::<DFResult<Vec<_>>>()?;
    let options = RecordBatchOptions::new().with_row_count(Some(entries.len()));
    Ok(RecordBatch::try_new_with_options(
        Arc::clone(schema),
        columns,
        &options,
    )?)
}

fn string_array<'a>(
    values: impl IntoIterator<Item = &'a str>,
    data_type: &datafusion::arrow::datatypes::DataType,
) -> ArrayRef {
    if matches!(data_type, datafusion::arrow::datatypes::DataType::Utf8View) {
        Arc::new(StringViewArray::from_iter_values(values))
    } else {
        Arc::new(StringArray::from_iter_values(values))
    }
}

fn owner_array(
    entries: &[ObjectEntry],
    data_type: &datafusion::arrow::datatypes::DataType,
) -> ArrayRef {
    let owners = entries.iter().map(ObjectEntry::owner).collect::<Vec<_>>();
    if owners.iter().all(Option::is_none) {
        new_null_array(data_type, entries.len())
    } else if matches!(data_type, datafusion::arrow::datatypes::DataType::Utf8View) {
        Arc::new(StringViewArray::from(owners))
    } else {
        Arc::new(StringArray::from(owners))
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use datafusion::execution::context::SessionConfig;
    use datafusion::physical_plan::streaming::StreamingTableExec;
    use datafusion::prelude::SessionContext;
    use futures::TryStreamExt;
    use paimon::catalog::Identifier;
    use paimon::io::FileIO;
    use paimon::spec::{Schema, TableSchema};

    use super::*;

    #[tokio::test]
    async fn scan_uses_a_projected_streaming_source() {
        let location = "memory:/objects";
        let file_io = FileIO::from_path(location).unwrap().build().unwrap();
        let schema = Schema::builder()
            .option("type", "object-table")
            .option("path", location)
            .build()
            .unwrap();
        let table = ObjectTable::try_new(
            file_io,
            Identifier::new("db", "objects"),
            &TableSchema::new(0, &schema),
        )
        .unwrap();
        let provider = ObjectTableProvider::try_new(table, false).unwrap();
        let projection = vec![0];
        let state = SessionContext::new().state();

        let plan = provider
            .scan(&state, Some(&projection), &[], None)
            .await
            .unwrap();
        let streaming = plan
            .downcast_ref::<StreamingTableExec>()
            .expect("object scans should use StreamingTableExec");

        assert_eq!(streaming.partition_schema().fields().len(), 1);
        assert_eq!(streaming.partition_schema().field(0).name(), "path");
    }

    #[tokio::test]
    async fn scan_streams_projected_batches_at_the_session_batch_size() {
        let location = "memory:/streaming-objects";
        let file_io = FileIO::from_path(location).unwrap().build().unwrap();
        for path in ["a.txt", "b.txt", "c.txt"] {
            file_io
                .new_output(&format!("{location}/{path}"))
                .unwrap()
                .write(Bytes::from_static(b"x"))
                .await
                .unwrap();
        }
        let schema = Schema::builder()
            .option("type", "object-table")
            .option("path", location)
            .build()
            .unwrap();
        let table = ObjectTable::try_new(
            file_io,
            Identifier::new("db", "objects"),
            &TableSchema::new(0, &schema),
        )
        .unwrap();
        let provider = ObjectTableProvider::try_new(table, false).unwrap();
        let projection = vec![0];
        let ctx = SessionContext::new_with_config(SessionConfig::new().with_batch_size(1));
        let state = ctx.state();

        let plan = provider
            .scan(&state, Some(&projection), &[], None)
            .await
            .unwrap();
        let batches = plan
            .execute(0, ctx.task_ctx())
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(batches.len(), 3);
        assert!(batches
            .iter()
            .all(|batch| batch.num_rows() == 1 && batch.num_columns() == 1));
        assert_eq!(batches[0].schema().field(0).name(), "path");
    }
}
