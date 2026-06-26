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

use arrow::pyarrow::FromPyArrow;
use arrow::record_batch::RecordBatch;
use paimon::table::{CommitMessage, Table, TableCommit, TableWrite};
use paimon_datafusion::runtime::runtime;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;

use crate::error::to_py_err;

/// Builder for the batch write loop, created via [`crate::table::PyTable::new_write_builder`].
///
/// Holds the owning table plus a single fixed `commit_user`, generated once and
/// shared by both `new_write()` and `new_commit()` so that writers and the
/// committer agree on the commit user (Paimon uses it for duplicate-commit
/// detection). Creating a fresh `WriteBuilder` per call would otherwise mint a
/// new random UUID each time.
#[pyclass(name = "WriteBuilder", module = "pypaimon_rust.datafusion")]
pub struct PyWriteBuilder {
    table: Arc<Table>,
    commit_user: String,
}

impl PyWriteBuilder {
    pub fn new(table: Arc<Table>) -> Self {
        let commit_user = table.new_write_builder().commit_user().to_string();
        Self { table, commit_user }
    }
}

#[pymethods]
impl PyWriteBuilder {
    /// Create a writer for accumulating Arrow batches.
    fn new_write(&self) -> PyResult<PyTableWrite> {
        let builder = self
            .table
            .new_write_builder()
            .with_commit_user(self.commit_user.clone())
            .map_err(to_py_err)?;
        Ok(PyTableWrite {
            inner: builder.new_write().map_err(to_py_err)?,
        })
    }

    /// Create a committer for persisting prepared commit messages.
    fn new_commit(&self) -> PyResult<PyTableCommit> {
        let builder = self
            .table
            .new_write_builder()
            .with_commit_user(self.commit_user.clone())
            .map_err(to_py_err)?;
        Ok(PyTableCommit {
            inner: builder.new_commit(),
        })
    }
}

/// A stateful writer that accumulates Arrow batches until `prepare_commit`.
///
/// Marked `unsendable`: the underlying `TableWrite` holds file writers that are
/// not `Sync`, so the object enforces single-thread access at runtime.
#[pyclass(name = "TableWrite", module = "pypaimon_rust.datafusion", unsendable)]
pub struct PyTableWrite {
    inner: TableWrite,
}

#[pymethods]
impl PyTableWrite {
    /// Write a single PyArrow RecordBatch into the table's writers.
    fn write_arrow(&mut self, py: Python<'_>, batch: &Bound<'_, PyAny>) -> PyResult<()> {
        let batch = RecordBatch::from_pyarrow_bound(batch)?;
        let rt = runtime();
        py.detach(|| rt.block_on(async { self.inner.write_arrow_batch(&batch).await }))
            .map_err(to_py_err)
    }

    /// Close writers and return the commit messages (opaque; pass to commit()).
    fn prepare_commit(&mut self, py: Python<'_>) -> PyResult<Vec<PyCommitMessage>> {
        let rt = runtime();
        let messages = py
            .detach(|| rt.block_on(async { self.inner.prepare_commit().await }))
            .map_err(to_py_err)?;
        Ok(messages
            .into_iter()
            .map(|inner| PyCommitMessage { inner })
            .collect())
    }
}

/// A committer that persists prepared commit messages as a snapshot.
#[pyclass(name = "TableCommit", module = "pypaimon_rust.datafusion")]
pub struct PyTableCommit {
    inner: TableCommit,
}

#[pymethods]
impl PyTableCommit {
    /// Commit the given commit messages. Empty input is a no-op success.
    fn commit(&self, py: Python<'_>, messages: &Bound<'_, PyAny>) -> PyResult<()> {
        let mut inner_messages = Vec::new();
        let iter = messages.try_iter().map_err(|_| {
            PyTypeError::new_err("commit() expects a sequence of CommitMessage objects")
        })?;
        for item in iter {
            let item = item?;
            let msg: PyRef<PyCommitMessage> = item.extract().map_err(|_| {
                PyTypeError::new_err("commit() expects a sequence of CommitMessage objects")
            })?;
            inner_messages.push(msg.inner.clone());
        }
        let rt = runtime();
        py.detach(|| rt.block_on(async { self.inner.commit(inner_messages).await }))
            .map_err(to_py_err)
    }
}

/// An opaque commit message produced by `prepare_commit`, consumed by `commit`.
/// PR1 supports same-process transfer only (no pickle/serialization).
#[pyclass(name = "CommitMessage", module = "pypaimon_rust.datafusion")]
pub struct PyCommitMessage {
    pub(crate) inner: CommitMessage,
}
