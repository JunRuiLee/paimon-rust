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

//! Primary-key vector (bucket-local ANN) search kernel.
//!
//! Mirrors Java PR apache/paimon#8569 (`[core] Add primary-key vector bucket
//! search`) at commit `a50a36ff8`. Read-only, synthetically tested; real ANN
//! index-byte validation is deferred to PR4 (Java-written fixtures).

// PR2 ports the PK-vector bucket-local search kernel before PR3/PR4 wires it
// into the read path. The kernel is crate-private with no production caller
// yet, so its items are unreachable outside their own tests. Suppress the
// resulting dead_code lint at the module boundary; remove this allow once
// PR3/PR4 lands production callers.
#![allow(dead_code)]

pub(crate) mod ann;
pub(crate) mod bucket;
pub(crate) mod exact;
pub(crate) mod metric;
pub(crate) mod reader;
pub(crate) mod result;

/// Shared constructor for validation failures in this module (mirrors Java
/// `checkArgument` / `IllegalArgumentException`).
pub(crate) fn data_invalid(message: impl Into<String>) -> crate::Error {
    crate::Error::DataInvalid {
        message: message.into(),
        source: None,
    }
}
