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
//
//! DataFileMeta ↔ BinaryRow conversion across the v8 and v9 split wire forms.
//!
//! The two wire forms agree on fields 0..=19 but diverge after that:
//!
//! | wire           | arity | field 20             | field 21              |
//! |----------------|-------|----------------------|-----------------------|
//! | v8 (Java legacy `DataFileMetaV11LegacySerializer`) | 20 | (absent)             | (absent)              |
//! | v8 (paimon-cpp `DataFileMetaSerializer`)            | 22 | `_MERGE_MODE` (i8)   | `_COMMIT_SNAPSHOT_ID` (i64) |
//! | v9 (Java `DataFileMetaSerializer`)                  | 22 | `_COMMIT_SNAPSHOT_ID` (i64) | `_MERGE_MODE` (i8) |
//!
//! Java v9 swapped the last two fields when adding versioned-partial-update,
//! so a wire-version-aware decoder is required. The encoder still emits the
//! paimon-cpp v8 22-field form for Bleem's C++ reader, since that's the only
//! consumer we ship today.
//!
//! Output byte form for a single meta (both v8/v9) is
//! `BinaryRowSerializer::Serialize` style: `i32 BE size_in_bytes | raw row bytes`.
//! The 22-field arity is implicit at this layer — only the outer `DataSplit`
//! wire carries an explicit version int.

use chrono::{DateTime, TimeZone, Utc};

use super::binary_array::{BinaryArray, MAX_FIX_PART_DATA_SIZE};
use super::binary_row::{BinaryRow, BinaryRowBuilder};
use super::data_file::DataFileMeta;
use super::stats::BinaryTableStats;

const DATA_FILE_META_V8_ARITY: i32 = 22;
const SIMPLE_STATS_ARITY: i32 = 3;

/// Wire-version selector for a DataFileMeta row. Determines:
///
/// - Which row arities are accepted (`V8` accepts 20 or 22; `V9` requires 22)
/// - The order of `commit_snapshot_id` and `merge_mode` at the tail of a
///   22-field row (V8 cpp-flavor vs. Java V9)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataFileMetaWireVersion {
    /// Outer split version 8. Inner row is either 20-field (Java legacy) or
    /// 22-field (paimon-cpp); when 22-field, `_MERGE_MODE` is at slot 20 and
    /// `_COMMIT_SNAPSHOT_ID` at slot 21 (matches `paimon-cpp`'s
    /// `DataFileMetaSerializer`).
    V8,
    /// Outer split version 9. Inner row is always 22-field with
    /// `_COMMIT_SNAPSHOT_ID` at slot 20 and `_MERGE_MODE` at slot 21
    /// (matches Java `DataFileMetaSerializer`).
    V9,
}

/// Serialize `DataFileMeta` to its on-wire bytes (used inside the `Split`
/// byte stream). Output format: `i32 BE size_in_bytes | raw row bytes`.
///
/// Writes the v8 paimon-cpp form (22 fields, `_MERGE_MODE` at 20,
/// `_COMMIT_SNAPSHOT_ID` at 21). That's the only consumer we currently feed.
pub fn data_file_meta_to_serialized_bytes(meta: &DataFileMeta) -> crate::Result<Vec<u8>> {
    let row = data_file_meta_to_row(meta)?;
    let raw = row.data();
    let size_in_bytes = raw.len() as i32;
    let mut out = Vec::with_capacity(4 + raw.len());
    out.extend_from_slice(&size_in_bytes.to_be_bytes());
    out.extend_from_slice(raw);
    Ok(out)
}

/// Deserialize a `DataFileMeta` from its on-wire bytes assuming the v8 wire
/// form. Use [`data_file_meta_from_serialized_bytes_versioned`] when the outer
/// split version is known and may be v9.
pub fn data_file_meta_from_serialized_bytes(buf: &[u8]) -> crate::Result<(DataFileMeta, usize)> {
    data_file_meta_from_serialized_bytes_versioned(buf, DataFileMetaWireVersion::V8)
}

/// Wire-version-aware deserialize. Splits decoded as Java v9 must use
/// [`DataFileMetaWireVersion::V9`] because the field 20/21 order changed in
/// that wire revision.
pub fn data_file_meta_from_serialized_bytes_versioned(
    buf: &[u8],
    version: DataFileMetaWireVersion,
) -> crate::Result<(DataFileMeta, usize)> {
    if buf.len() < 4 {
        return Err(crate::Error::DataInvalid {
            message: format!("DataFileMeta: buffer too short ({} bytes)", buf.len()),
            source: None,
        });
    }
    let size = i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
    if size < 0 {
        return Err(crate::Error::DataInvalid {
            message: format!("DataFileMeta: negative size_in_bytes {size}"),
            source: None,
        });
    }
    let body_len = size as usize;
    if buf.len() < 4 + body_len {
        return Err(crate::Error::DataInvalid {
            message: format!(
                "DataFileMeta: declared size {body_len} exceeds remaining {} bytes",
                buf.len() - 4
            ),
            source: None,
        });
    }
    let body = &buf[4..4 + body_len];
    let actual_arity = infer_actual_arity(body, version)?;
    let row = BinaryRow::from_bytes(actual_arity, body.to_vec());
    Ok((data_file_meta_from_row_versioned(&row, version)?, 4 + body_len))
}

const FIXED_PART_20_FIELDS: usize = 8 + 20 * 8;
const FIXED_PART_22_FIELDS: usize = 8 + 22 * 8;

/// Decide the row arity.
///
/// - V9 is strictly 22 fields — anything shorter is malformed.
/// - V8 may be 20-field (Java legacy) or 22-field (paimon-cpp). We use the
///   row body length first (sub-184 must be 20-field) and the file_name var-
///   offset as a tiebreaker when body length alone is ambiguous.
fn infer_actual_arity(
    body: &[u8],
    version: DataFileMetaWireVersion,
) -> crate::Result<i32> {
    if body.len() < FIXED_PART_20_FIELDS {
        return Err(crate::Error::DataInvalid {
            message: format!(
                "DataFileMeta row body too short: {} bytes (minimum {} for 20-field schema)",
                body.len(),
                FIXED_PART_20_FIELDS
            ),
            source: None,
        });
    }
    if matches!(version, DataFileMetaWireVersion::V9) {
        if body.len() < FIXED_PART_22_FIELDS {
            return Err(crate::Error::DataInvalid {
                message: format!(
                    "DataFileMeta v9 row body too short: {} bytes (minimum {} for 22-field schema)",
                    body.len(),
                    FIXED_PART_22_FIELDS
                ),
                source: None,
            });
        }
        return Ok(DATA_FILE_META_V8_ARITY);
    }
    if body.len() < FIXED_PART_22_FIELDS {
        return Ok(20);
    }
    // V8, body_len >= 184: ambiguous. Anchor on file_name's var-offset when
    // it's var-len encoded; otherwise (inline ≤ 7 bytes) default to 22.
    let slot0 = i64::from_le_bytes(body[8..16].try_into().unwrap()) as u64;
    if slot0 & (0x80 << 56) != 0 {
        return Ok(DATA_FILE_META_V8_ARITY);
    }
    let var_offset = (slot0 >> 32) as usize;
    let arity = if var_offset == FIXED_PART_22_FIELDS {
        DATA_FILE_META_V8_ARITY
    } else if var_offset == FIXED_PART_20_FIELDS {
        20
    } else {
        return Err(crate::Error::Unsupported {
            message: format!(
                "DataFileMeta v8 row file_name var-offset {var_offset} does not match 20-field ({FIXED_PART_20_FIELDS}) or 22-field ({FIXED_PART_22_FIELDS}) schema"
            ),
        });
    };
    Ok(arity)
}

/// Build a 22-field `BinaryRow` from a `DataFileMeta` (v8 paimon-cpp schema).
/// Mirrors `paimon-cpp/.../data_file_meta_serializer.cpp::ToRow`.
pub fn data_file_meta_to_row(meta: &DataFileMeta) -> crate::Result<BinaryRow> {
    let mut b = BinaryRowBuilder::new(DATA_FILE_META_V8_ARITY);

    write_string_auto(&mut b, 0, &meta.file_name);
    b.write_long(1, meta.file_size);
    b.write_long(2, meta.row_count);
    write_binary_auto(&mut b, 3, &meta.min_key);
    write_binary_auto(&mut b, 4, &meta.max_key);
    b.write_row(5, &simple_stats_to_row(&meta.key_stats));
    b.write_row(6, &simple_stats_to_row(&meta.value_stats));
    b.write_long(7, meta.min_sequence_number);
    b.write_long(8, meta.max_sequence_number);
    b.write_long(9, meta.schema_id);
    b.write_int(10, meta.level);

    let extra: Vec<Option<String>> = meta.extra_files.iter().cloned().map(Some).collect();
    b.write_array(11, &BinaryArray::from_str_array_nullable(&extra));

    match meta.creation_time {
        Some(t) => b.write_timestamp_compact(12, t.timestamp_millis()),
        None => b.set_null_at(12),
    }

    set_optional_long(&mut b, 13, meta.delete_row_count);

    match &meta.embedded_index {
        Some(bytes) => write_binary_auto(&mut b, 14, bytes),
        None => b.set_null_at(14),
    }

    match meta.file_source {
        Some(v) => b.write_byte(15, v as i8),
        None => b.set_null_at(15),
    }

    match &meta.value_stats_cols {
        Some(cols) => b.write_array(16, &BinaryArray::from_str_array_non_null(cols)),
        None => b.set_null_at(16),
    }

    match &meta.external_path {
        Some(s) => write_string_auto(&mut b, 17, s),
        None => b.set_null_at(17),
    }

    set_optional_long(&mut b, 18, meta.first_row_id);

    match &meta.write_cols {
        Some(cols) => b.write_array(19, &BinaryArray::from_str_array_non_null(cols)),
        None => b.set_null_at(19),
    }

    // v8 paimon-cpp schema: 20 = merge_mode, 21 = commit_snapshot_id.
    match meta.merge_mode {
        Some(v) => b.write_byte(20, v),
        None => b.set_null_at(20),
    }
    set_optional_long(&mut b, 21, meta.commit_snapshot_id);

    Ok(b.build())
}

/// Decode a v8 paimon-cpp-shaped row (the default this crate emits).
pub fn data_file_meta_from_row(row: &BinaryRow) -> crate::Result<DataFileMeta> {
    data_file_meta_from_row_versioned(row, DataFileMetaWireVersion::V8)
}

/// Wire-version-aware row decode. The first 20 fields are identical between
/// v8 and v9; only the trailing `merge_mode` / `commit_snapshot_id` slots are
/// swapped, and v9 requires arity 22 (no 20-field legacy form).
pub fn data_file_meta_from_row_versioned(
    row: &BinaryRow,
    version: DataFileMetaWireVersion,
) -> crate::Result<DataFileMeta> {
    let arity = row.arity();
    match version {
        DataFileMetaWireVersion::V8 => {
            if arity != DATA_FILE_META_V8_ARITY && arity != 20 {
                return Err(crate::Error::DataInvalid {
                    message: format!(
                        "DataFileMeta v8 expects arity 20 or {DATA_FILE_META_V8_ARITY}, got {arity}"
                    ),
                    source: None,
                });
            }
        }
        DataFileMetaWireVersion::V9 => {
            if arity != DATA_FILE_META_V8_ARITY {
                return Err(crate::Error::DataInvalid {
                    message: format!(
                        "DataFileMeta v9 expects arity {DATA_FILE_META_V8_ARITY}, got {arity}"
                    ),
                    source: None,
                });
            }
        }
    }

    let file_name = row.get_string(0)?.to_string();
    let file_size = row.get_long(1)?;
    let row_count = row.get_long(2)?;
    let min_key = row.get_binary(3)?.to_vec();
    let max_key = row.get_binary(4)?.to_vec();
    let key_stats = simple_stats_from_row(&row.get_row(5, SIMPLE_STATS_ARITY)?)?;
    let value_stats = simple_stats_from_row(&row.get_row(6, SIMPLE_STATS_ARITY)?)?;
    let min_sequence_number = row.get_long(7)?;
    let max_sequence_number = row.get_long(8)?;
    let schema_id = row.get_long(9)?;
    let level = row.get_int(10)?;

    let extra_arr = row.get_array(11)?;
    let mut extra_files = Vec::with_capacity(extra_arr.size() as usize);
    for i in 0..extra_arr.size() {
        if extra_arr.is_null_at(i) {
            return Err(crate::Error::Unsupported {
                message: format!(
                    "DataFileMeta::extra_files contains null at {i}; Rust type is Vec<String> and cannot represent null"
                ),
            });
        }
        extra_files.push(extra_arr.get_string(i)?.to_string());
    }

    let creation_time = if row.is_null_at(12) {
        None
    } else {
        let (millis, _nanos) = row.get_timestamp_raw(12, 3)?;
        Some(millis_to_utc(millis)?)
    };

    let delete_row_count = if row.is_null_at(13) {
        None
    } else {
        Some(row.get_long(13)?)
    };

    let embedded_index = if row.is_null_at(14) {
        None
    } else {
        Some(row.get_binary(14)?.to_vec())
    };

    let file_source = if row.is_null_at(15) {
        None
    } else {
        Some(row.get_byte(15)? as i32)
    };

    let value_stats_cols = if row.is_null_at(16) {
        None
    } else {
        Some(string_array_required_to_vec(&row.get_array(16)?)?)
    };

    let external_path = if row.is_null_at(17) {
        None
    } else {
        Some(row.get_string(17)?.to_string())
    };

    let first_row_id = if row.is_null_at(18) {
        None
    } else {
        Some(row.get_long(18)?)
    };

    let write_cols = if row.is_null_at(19) {
        None
    } else {
        Some(string_array_required_to_vec(&row.get_array(19)?)?)
    };

    // Slots 20/21 — order depends on wire version.
    let (merge_mode, commit_snapshot_id) = match version {
        DataFileMetaWireVersion::V8 => {
            // 20 = merge_mode, 21 = commit_snapshot_id (paimon-cpp layout;
            // the Java legacy 20-field form has neither).
            let mm = if arity > 20 && !row.is_null_at(20) {
                Some(row.get_byte(20)?)
            } else {
                None
            };
            let csi = if arity > 21 && !row.is_null_at(21) {
                Some(row.get_long(21)?)
            } else {
                None
            };
            (mm, csi)
        }
        DataFileMetaWireVersion::V9 => {
            // 20 = commit_snapshot_id, 21 = merge_mode (Java v9 layout).
            let csi = if !row.is_null_at(20) {
                Some(row.get_long(20)?)
            } else {
                None
            };
            let mm = if !row.is_null_at(21) {
                Some(row.get_byte(21)?)
            } else {
                None
            };
            (mm, csi)
        }
    };

    Ok(DataFileMeta {
        file_name,
        file_size,
        row_count,
        min_key,
        max_key,
        key_stats,
        value_stats,
        min_sequence_number,
        max_sequence_number,
        schema_id,
        level,
        extra_files,
        creation_time,
        delete_row_count,
        embedded_index,
        file_source,
        value_stats_cols,
        external_path,
        first_row_id,
        write_cols,
        merge_mode,
        commit_snapshot_id,
    })
}

/// Build a 3-field SimpleStats sub-row mirroring `paimon-cpp/.../simple_stats.cpp::ToRow`.
/// Note: `BinaryTableStats::min_values()` already includes the 4-byte BE arity
/// prefix that `SerializationUtils::SerializeBinaryRow` produces, so we write
/// it directly as a binary field — no further wrapping needed.
pub fn simple_stats_to_row(stats: &BinaryTableStats) -> BinaryRow {
    let mut b = BinaryRowBuilder::new(SIMPLE_STATS_ARITY);
    write_binary_auto_slice(&mut b, 0, stats.min_values());
    write_binary_auto_slice(&mut b, 1, stats.max_values());
    let null_counts_arr = BinaryArray::from_long_array_nullable(stats.null_counts());
    b.write_array(2, &null_counts_arr);
    b.build()
}

pub fn simple_stats_from_row(row: &BinaryRow) -> crate::Result<BinaryTableStats> {
    if row.arity() != SIMPLE_STATS_ARITY {
        return Err(crate::Error::DataInvalid {
            message: format!("SimpleStats expects arity 3, got {}", row.arity()),
            source: None,
        });
    }
    let min_values = row.get_binary(0)?.to_vec();
    let max_values = row.get_binary(1)?.to_vec();
    let arr = row.get_array(2)?;
    let mut null_counts: Vec<Option<i64>> = Vec::with_capacity(arr.size() as usize);
    for i in 0..arr.size() {
        if arr.is_null_at(i) {
            null_counts.push(None);
        } else {
            null_counts.push(Some(arr.get_long(i)?));
        }
    }
    Ok(BinaryTableStats::new(min_values, max_values, null_counts))
}

// ---------- helpers ----------

fn set_optional_long(b: &mut BinaryRowBuilder, pos: usize, v: Option<i64>) {
    match v {
        Some(x) => b.write_long(pos, x),
        None => b.set_null_at(pos),
    }
}

fn write_string_auto(b: &mut BinaryRowBuilder, pos: usize, s: &str) {
    if s.len() <= MAX_FIX_PART_DATA_SIZE {
        b.write_string_inline(pos, s);
    } else {
        b.write_string(pos, s);
    }
}

fn write_binary_auto(b: &mut BinaryRowBuilder, pos: usize, value: &[u8]) {
    if value.len() <= MAX_FIX_PART_DATA_SIZE {
        b.write_binary_inline(pos, value);
    } else {
        b.write_binary(pos, value);
    }
}

fn write_binary_auto_slice(b: &mut BinaryRowBuilder, pos: usize, value: &[u8]) {
    write_binary_auto(b, pos, value);
}

fn millis_to_utc(millis: i64) -> crate::Result<DateTime<Utc>> {
    Utc.timestamp_millis_opt(millis)
        .single()
        .ok_or_else(|| crate::Error::DataInvalid {
            message: format!("DataFileMeta: invalid creation_time millis {millis}"),
            source: None,
        })
}

fn string_array_required_to_vec(arr: &BinaryArray) -> crate::Result<Vec<String>> {
    let mut out = Vec::with_capacity(arr.size() as usize);
    for i in 0..arr.size() {
        if arr.is_null_at(i) {
            return Err(crate::Error::Unsupported {
                message: format!(
                    "DataFileMeta string array element {i} is null but field is non-nullable"
                ),
            });
        }
        out.push(arr.get_string(i)?.to_string());
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn empty_stats() -> BinaryTableStats {
        BinaryTableStats::empty()
    }

    fn make_meta_minimal() -> DataFileMeta {
        DataFileMeta {
            file_name: "data-0001.parquet".into(),
            file_size: 12345,
            row_count: 100,
            min_key: vec![0u8; 4],
            max_key: vec![0u8; 4],
            key_stats: empty_stats(),
            value_stats: empty_stats(),
            min_sequence_number: 0,
            max_sequence_number: 99,
            schema_id: 1,
            level: 0,
            extra_files: Vec::new(),
            creation_time: Some(Utc.timestamp_millis_opt(1_700_000_000_000).unwrap()),
            delete_row_count: None,
            embedded_index: None,
            file_source: None,
            value_stats_cols: None,
            external_path: None,
            first_row_id: None,
            write_cols: None,
            merge_mode: None,
            commit_snapshot_id: None,
        }
    }

    fn make_meta_full() -> DataFileMeta {
        DataFileMeta {
            file_name: "data-72b62a5f-d698-4db5-b51a-04c0dc027702-0.orc".into(),
            file_size: 961,
            row_count: 5,
            min_key: vec![0, 0, 0, 1, 1, 2, 3, 4],
            max_key: vec![0, 0, 0, 1, 5, 6, 7, 8],
            key_stats: empty_stats(),
            value_stats: empty_stats(),
            min_sequence_number: 0,
            max_sequence_number: 4,
            schema_id: 0,
            level: 5,
            extra_files: Vec::new(),
            creation_time: Some(Utc.timestamp_millis_opt(1_757_354_415_711).unwrap()),
            delete_row_count: Some(0),
            embedded_index: Some(vec![1u8, 2, 3, 4, 5, 6, 7, 8, 9]),
            file_source: Some(0), // APPEND
            value_stats_cols: Some(vec!["f0".into(), "f1".into()]),
            external_path: Some(
                "FILE:/tmp/external/f1=10/bucket-1/data-72b62a5f-d698-4db5-b51a-04c0dc027702-0.orc"
                    .into(),
            ),
            first_row_id: Some(42),
            write_cols: Some(vec!["a".into(), "b_long_field_name".into()]),
            merge_mode: Some(1),
            commit_snapshot_id: Some(7),
        }
    }

    #[test]
    fn round_trip_minimal() {
        let meta = make_meta_minimal();
        let bytes = data_file_meta_to_serialized_bytes(&meta).unwrap();
        let (decoded, consumed) = data_file_meta_from_serialized_bytes(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, meta);
    }

    #[test]
    fn round_trip_full() {
        let meta = make_meta_full();
        let bytes = data_file_meta_to_serialized_bytes(&meta).unwrap();
        let (decoded, consumed) = data_file_meta_from_serialized_bytes(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, meta);
    }

    #[test]
    fn simple_stats_round_trip_empty() {
        let stats = empty_stats();
        let row = simple_stats_to_row(&stats);
        let decoded = simple_stats_from_row(&row).unwrap();
        assert_eq!(decoded, stats);
    }

    #[test]
    fn simple_stats_round_trip_with_null_counts() {
        let stats = BinaryTableStats::new(
            vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            vec![Some(1), None, Some(3)],
        );
        let row = simple_stats_to_row(&stats);
        let decoded = simple_stats_from_row(&row).unwrap();
        assert_eq!(decoded, stats);
    }

    #[test]
    fn round_trip_short_strings_use_inline() {
        // Names ≤7 bytes must round-trip identically (and the wire form should
        // use inline encoding). We can't easily inspect the wire form here, but
        // round-trip equality + non-erroring decode is our primary correctness
        // check.
        let mut meta = make_meta_minimal();
        meta.file_name = "abc.orc".into(); // 7 bytes
        meta.external_path = Some("p".into()); // 1 byte
        meta.value_stats_cols = Some(vec!["a".into(), "b".into(), "c".into()]); // all ≤7

        let bytes = data_file_meta_to_serialized_bytes(&meta).unwrap();
        let (decoded, _) = data_file_meta_from_serialized_bytes(&bytes).unwrap();
        assert_eq!(decoded, meta);
    }

    #[test]
    fn rejects_wrong_arity() {
        let row = BinaryRow::from_bytes(3, vec![0u8; 32]);
        assert!(data_file_meta_from_row(&row).is_err());
    }

    #[test]
    fn rejects_truncated_buffer() {
        let bytes = vec![0u8, 0, 0, 1]; // declares size = 1, no body
        assert!(data_file_meta_from_serialized_bytes(&bytes).is_err());
    }

    #[test]
    fn rejects_body_shorter_than_20_field_fixed_part() {
        // size = 100 (< 168 = 20-field fixed part). Should be rejected before
        // we attempt any field access.
        let mut bytes = 100i32.to_be_bytes().to_vec();
        bytes.extend(std::iter::repeat_n(0u8, 100));
        let err = data_file_meta_from_serialized_bytes(&bytes).unwrap_err();
        assert!(matches!(err, crate::Error::DataInvalid { .. }));
    }

    /// A row body whose file_name var-offset doesn't match either canonical
    /// fixed-part length (168 / 184) is malformed — refuse rather than try to
    /// invent an arity that fits.
    #[test]
    fn rejects_unknown_file_name_var_offset() {
        // Build a row body of length >= 184 (so we hit the disambiguation
        // path) where slot0's var-offset is a bogus value (200).
        let body_len = 256usize;
        let mut body = vec![0u8; body_len];
        // null bitmap: zeros (no nulls).
        // slot 0 (offset 8..16): var-offset = 200, len = 0 → encoded LE.
        let slot0 = ((200u64) << 32) | 0u64;
        body[8..16].copy_from_slice(&slot0.to_le_bytes());

        let mut bytes = (body_len as i32).to_be_bytes().to_vec();
        bytes.extend(body);
        let err = data_file_meta_from_serialized_bytes(&bytes).unwrap_err();
        assert!(matches!(err, crate::Error::Unsupported { .. }));
    }
}
