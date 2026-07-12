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

use crate::spec::PkVectorSourceMeta;

/// One ANN segment to be searched by the bucket kernel: the vindex index bytes
/// plus the source metadata resolving segment ordinals back to physical
/// `(data file, position)`. The index-byte identity (which physical index the
/// segment reads) is a PR4 concern — PR2 only needs `source_meta` for ordinal
/// mapping and live-row masking; the reader wiring is added in PR4.
pub(crate) struct BucketAnnSegment {
    pub source_meta: PkVectorSourceMeta,
}

/// A data file participating in the bucket search, with its row count. Used by
/// the bucket kernel (Task 6) to plan exact vs. ANN search over active files.
pub(crate) struct BucketActiveFile {
    pub file_name: String,
    pub row_count: i64,
}
