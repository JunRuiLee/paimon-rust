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

use crate::deletion_vector::core::DeletionVector;
use crate::io::{FileIO, FileRead};
use crate::spec::DataFileMeta;
use crate::Result;
use std::collections::HashMap;
use std::sync::Arc;

/// Factory for creating DeletionVector instances from files and metadata.
///
/// Corresponds to Java's [DeletionVector.Factory](https://github.com/apache/paimon/blob/master/paimon-core/src/main/java/org/apache/paimon/deletionvectors/DeletionVector.java)
/// (create(fileName) -> Optional<DeletionVector>). Can be built from split-level deletion files
/// ([create_from_deletion_files]) or from index manifest entries ([create]).
pub struct DeletionVectorFactory {
    /// Map from data file name to its deletion vector
    deletion_vectors: HashMap<String, Arc<DeletionVector>>,
}

impl DeletionVectorFactory {
    /// Create a DeletionVectorFactory from data file names and their optional deletion files.
    /// Same as Java's `DeletionVector.factory(fileIO, files, deletionFiles)`: for each file that
    /// has a DeletionFile, reads path/offset/length and loads the DV.
    pub async fn new(
        file_io: &FileIO,
        data_files: &[DataFileMeta],
        data_deletion_files: Option<&[Option<crate::DeletionFile>]>,
    ) -> Result<Self> {
        let mut deletion_vectors = HashMap::new();
        let Some(data_deletion_files) = data_deletion_files else {
            return Ok(DeletionVectorFactory { deletion_vectors });
        };

        for (data_file, opt_df) in data_files.iter().zip(data_deletion_files.iter()) {
            let Some(df) = opt_df.as_ref() else {
                continue;
            };
            let dv = Self::read(file_io, df).await?;
            deletion_vectors.insert(data_file.file_name.clone(), Arc::new(dv));
        }
        Ok(DeletionVectorFactory { deletion_vectors })
    }

    /// Get the deletion vector for a specific data file.
    pub fn get_deletion_vector(&self, data_file_name: &str) -> Option<&Arc<DeletionVector>> {
        self.deletion_vectors.get(data_file_name)
    }

    /// Read a single DeletionVector from storage using DeletionFile (path/offset/length).
    /// Same as Java's DeletionVector.read(FileIO, DeletionFile).
    ///
    /// Java's `DeletionVectorMeta.length` is the return value of
    /// `serializeTo(...)` and has different semantics for the two variants:
    /// - 32-bit: `length = magic + roaring32_bytes` — physical blob occupies
    ///   `length + 8` bytes on disk (outer length field + crc are extra)
    /// - 64-bit: `length = bytes.length` of the whole serialized buffer —
    ///   physical blob occupies exactly `length` bytes
    ///
    /// We can't dispatch on the magic before reading, so we always request
    /// `length + 8` bytes (the 32-bit upper bound) and clamp to the file size
    /// for the 64-bit case (where +8 over-reads). `read_from_bytes` ignores
    /// trailing bytes via the inner length field.
    async fn read(file_io: &FileIO, df: &crate::DeletionFile) -> Result<DeletionVector> {
        let input = file_io.new_input(df.path())?;
        let file_size = input.metadata().await?.size;
        let reader = input.reader().await?;
        let offset = df.offset() as u64;
        let len = df.length() as u64;
        // 32-bit: physical blob is len + 8 bytes (outer length + crc frame not counted in len).
        // 64-bit: physical blob is len bytes (outer length + crc frame already counted).
        // Clamp to file size so 64-bit reads don't over-read past EOF when the
        // DV is the last blob in the file.
        let end = offset
            .saturating_add(len)
            .saturating_add(8)
            .min(file_size);
        let bytes = reader.read(offset..end).await?;
        DeletionVector::read_from_bytes(&bytes, Some(len))
    }
}
