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

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

const BYTE_PERMIT_UNIT: u64 = 1024 * 1024;
const DEFAULT_PARALLELISM: usize = 8;
const DEFAULT_MAX_INFLIGHT_BYTES: u64 = 256 * 1024 * 1024;

/// Shared resource budget for concurrent Parquet row-group reads.
#[derive(Debug)]
pub struct ParquetReadBudget {
    parallelism: usize,
    row_groups: Arc<Semaphore>,
    bytes: Arc<Semaphore>,
    byte_permits: u32,
}

impl ParquetReadBudget {
    pub fn new(parallelism: usize, max_inflight_bytes: u64) -> crate::Result<Self> {
        if parallelism == 0 || parallelism > Semaphore::MAX_PERMITS {
            return Err(crate::Error::DataInvalid {
                message: format!(
                    "Parquet row-group parallelism must be between 1 and {}, got {parallelism}",
                    Semaphore::MAX_PERMITS
                ),
                source: None,
            });
        }
        if max_inflight_bytes == 0 {
            return Err(crate::Error::DataInvalid {
                message: "Parquet row-group max in-flight bytes must be greater than 0".to_string(),
                source: None,
            });
        }
        let max_byte_permits = Semaphore::MAX_PERMITS.min(u32::MAX as usize) as u32;
        let byte_permits = max_inflight_bytes
            .div_ceil(BYTE_PERMIT_UNIT)
            .min(u64::from(max_byte_permits)) as u32;

        Ok(Self {
            parallelism,
            row_groups: Arc::new(Semaphore::new(parallelism)),
            bytes: Arc::new(Semaphore::new(byte_permits as usize)),
            byte_permits,
        })
    }

    pub fn parallelism(&self) -> usize {
        self.parallelism
    }

    pub(crate) async fn acquire(
        &self,
        projected_uncompressed_bytes: u64,
    ) -> crate::Result<ParquetReadPermit> {
        let row_group = Arc::clone(&self.row_groups)
            .acquire_owned()
            .await
            .map_err(|error| crate::Error::UnexpectedError {
                message: "Parquet row-group read budget was closed".to_string(),
                source: Some(Box::new(error)),
            })?;
        let requested = projected_uncompressed_bytes
            .max(1)
            .div_ceil(BYTE_PERMIT_UNIT)
            .min(u64::from(self.byte_permits)) as u32;
        let bytes = Arc::clone(&self.bytes)
            .acquire_many_owned(requested)
            .await
            .map_err(|error| crate::Error::UnexpectedError {
                message: "Parquet byte read budget was closed".to_string(),
                source: Some(Box::new(error)),
            })?;
        Ok(ParquetReadPermit {
            _row_group: row_group,
            _bytes: bytes,
        })
    }
}

impl Default for ParquetReadBudget {
    fn default() -> Self {
        Self::new(DEFAULT_PARALLELISM, DEFAULT_MAX_INFLIGHT_BYTES)
            .expect("default Parquet read budget is valid")
    }
}

#[derive(Debug)]
pub(crate) struct ParquetReadPermit {
    _row_group: OwnedSemaphorePermit,
    _bytes: OwnedSemaphorePermit,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn shared_budget_blocks_until_permits_are_released() {
        let budget = Arc::new(ParquetReadBudget::new(2, BYTE_PERMIT_UNIT).unwrap());
        let first = budget.acquire(2 * BYTE_PERMIT_UNIT).await.unwrap();

        assert!(
            tokio::time::timeout(Duration::from_millis(20), budget.acquire(1))
                .await
                .is_err(),
            "the projected-byte budget must be shared across readers"
        );

        drop(first);
        tokio::time::timeout(Duration::from_secs(1), budget.acquire(1))
            .await
            .expect("dropping a read must release its permits")
            .unwrap();
    }

    #[test]
    fn rejects_invalid_limits() {
        assert!(ParquetReadBudget::new(0, BYTE_PERMIT_UNIT).is_err());
        assert!(ParquetReadBudget::new(1, 0).is_err());
        assert!(
            ParquetReadBudget::new(Semaphore::MAX_PERMITS.saturating_add(1), BYTE_PERMIT_UNIT)
                .is_err()
        );
    }
}
