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

use paimon::table::{DataSplit, Table};
use paimon_datafusion::runtime::runtime;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

use crate::error::to_py_err;

#[pyclass(name = "ReadBuilder", module = "pypaimon_rust.datafusion")]
pub struct PyReadBuilder {
    table: Arc<Table>,
    projection: Option<Vec<String>>,
    limit: Option<usize>,
}

impl PyReadBuilder {
    pub fn new(table: Arc<Table>) -> Self {
        Self {
            table,
            projection: None,
            limit: None,
        }
    }
}

#[pymethods]
impl PyReadBuilder {
    fn with_projection(mut slf: PyRefMut<'_, Self>, columns: Vec<String>) -> PyRefMut<'_, Self> {
        slf.projection = Some(columns);
        slf
    }

    fn with_limit(mut slf: PyRefMut<'_, Self>, limit: usize) -> PyRefMut<'_, Self> {
        slf.limit = Some(limit);
        slf
    }

    fn new_scan(&self) -> PyTableScan {
        PyTableScan {
            table: Arc::clone(&self.table),
            projection: self.projection.clone(),
            limit: self.limit,
        }
    }
}

#[pyclass(name = "TableScan", module = "pypaimon_rust.datafusion")]
pub struct PyTableScan {
    table: Arc<Table>,
    projection: Option<Vec<String>>,
    limit: Option<usize>,
}

#[pymethods]
impl PyTableScan {
    fn plan(&self, py: Python<'_>) -> PyResult<PyPlan> {
        let rt = runtime();
        let splits = py.detach(|| {
            rt.block_on(async {
                let mut builder = self.table.new_read_builder();
                if let Some(projection) = &self.projection {
                    let cols: Vec<&str> = projection.iter().map(String::as_str).collect();
                    builder.with_projection(&cols);
                }
                if let Some(limit) = self.limit {
                    builder.with_limit(limit);
                }
                let plan = builder.new_scan().plan().await.map_err(to_py_err)?;
                Ok::<_, PyErr>(plan.splits().to_vec())
            })
        })?;
        Ok(PyPlan { splits })
    }
}

#[pyclass(name = "Plan", module = "pypaimon_rust.datafusion")]
pub struct PyPlan {
    splits: Vec<DataSplit>,
}

#[pymethods]
impl PyPlan {
    fn splits(&self) -> Vec<PySplit> {
        self.splits
            .iter()
            .cloned()
            .map(|inner| PySplit { inner })
            .collect()
    }

    fn __len__(&self) -> usize {
        self.splits.len()
    }
}

#[pyclass(name = "Split", module = "pypaimon_rust.datafusion")]
pub struct PySplit {
    pub(crate) inner: DataSplit,
}

impl PySplit {
    fn to_bytes(&self) -> PyResult<Vec<u8>> {
        serde_json::to_vec(&self.inner)
            .map_err(|e| PyValueError::new_err(format!("failed to serialize split: {e}")))
    }

    fn from_bytes(bytes: &[u8]) -> PyResult<DataSplit> {
        serde_json::from_slice(bytes)
            .map_err(|e| PyValueError::new_err(format!("failed to deserialize split: {e}")))
    }
}

#[pymethods]
impl PySplit {
    /// Physical row count: sum of data-file row counts (not a logical result count).
    fn row_count(&self) -> i64 {
        self.inner.row_count()
    }

    /// Reduce to `Split(bytes)` for pickle/copy. The bytes are an opaque,
    /// implementation-detail encoding; only same/compatible-version round-trip
    /// is guaranteed.
    fn __reduce__<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
    ) -> PyResult<(Py<PyAny>, (Py<PyBytes>,))> {
        let bytes = slf.borrow().to_bytes()?;
        let cls = slf.get_type().unbind().into_any();
        Ok((cls, (PyBytes::new(py, &bytes).unbind(),)))
    }

    /// Reconstruct a split from opaque bytes produced by pickling. Direct
    /// construction without those bytes is unsupported; obtain splits from
    /// `ReadBuilder.new_scan().plan()`.
    #[new]
    fn new(state: &Bound<'_, PyBytes>) -> PyResult<Self> {
        Ok(Self {
            inner: Self::from_bytes(state.as_bytes())?,
        })
    }
}
