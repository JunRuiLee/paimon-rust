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
//! Compatible serialization for `DataSplit` matching paimon-cpp's
//! `paimon::Split::Serialize` / `Split::Deserialize`. Used by Bleem to feed
//! splits into the C++ reader (`bleem/be/src/format/table/paimon_cpp_reader.cpp`).
//!
//! ## Wire format (v8, big-endian outer stream)
//! ```text
//! i64  magic = -2_394_839_472_490_812_314 (DataSplit)
//! i32  version = 8
//! i64  snapshot_id
//! i32  partition_total_len = 4 + sizeInBytes(partition)
//! i32  partition_arity
//! raw  partition_bytes[sizeInBytes]
//! i32  bucket
//! i16  bucket_path_len, raw utf8 bytes
//! i8   total_buckets_present (0 or 1)
//! [i32 total_buckets if present]
//! i32  before_files_n; for each: i32 size, raw row bytes (DataFileMetaSerializer form)
//! deletion_file_list                              (before_deletion_files)
//! i32  data_files_n; for each: i32 size, raw row bytes
//! deletion_file_list                              (data_deletion_files)
//! i8   is_streaming
//! i8   raw_convertible
//! ```
//!
//! Only DataSplit + DataFileMeta v8 are supported. Encountering an
//! `IndexedSplit` magic, a non-v8 version, or a trailing FallbackDataSplit byte
//! returns `Error::Unsupported` rather than silently dropping data.

use crate::spec::be_io::{BeReader, BeWriter};
use crate::spec::BinaryRow;
use crate::spec::DataFileMeta;
use crate::spec::{
    data_file_meta_from_serialized_bytes_versioned, data_file_meta_to_serialized_bytes,
    DataFileMetaWireVersion,
};
use crate::table::source::{DataSplit, DeletionFile, Plan};

/// `paimon::DataSplitImpl::MAGIC` from `paimon-cpp/.../data_split_impl.h`.
const DATA_SPLIT_MAGIC: i64 = -2_394_839_472_490_812_314;
/// `paimon::IndexedSplitImpl::MAGIC` from `paimon-cpp/.../indexed_split_impl.h`.
const INDEXED_SPLIT_MAGIC: i64 = -938_472_394_838_495_695;
/// Outer split versions we accept on deserialize. The encoder still emits v8
/// — that's what paimon-cpp's `Split::Deserialize` accepts. Java engines emit
/// v9, which is wire-compatible with v8 at the split layer but uses a
/// different DataFileMeta row schema.
const VERSION_8: i32 = 8;
const VERSION_9: i32 = 9;
/// Arity of the v8 paimon-cpp DataFileMeta `BinaryRow` schema. Used when
/// decoding a `DataFileMeta` row body whose arity is implicit on the wire.
const DATA_FILE_META_V8_ARITY: i32 = 22;

/// Serialize a `DataSplit` into the byte form that paimon-cpp's
/// `Split::Deserialize` accepts.
pub fn serialize_data_split(split: &DataSplit) -> crate::Result<Vec<u8>> {
    // The C++ DataSplit wire form has no slot for `row_ranges` — that field
    // only exists in `IndexedSplit`, which we don't support. Refuse rather
    // than silently dropping the row-id pruning a caller (e.g. global-index /
    // full-text / vector scan) baked into the split, which would otherwise
    // make the C++ reader return rows the Rust planner had filtered out.
    if split.row_ranges().is_some() {
        return Err(crate::Error::Unsupported {
            message: "DataSplit with row_ranges cannot be serialized: the paimon-cpp \
                      DataSplit wire format does not carry row_ranges (only IndexedSplit does, \
                      which paimon-rust does not yet support). Strip row_ranges or route the \
                      split through an IndexedSplit-aware path."
                .into(),
        });
    }

    let mut w = BeWriter::with_capacity(256);

    w.write_i64(DATA_SPLIT_MAGIC);
    w.write_i32(VERSION_8);
    w.write_i64(split.snapshot_id());

    // partition: SerializationUtils::SerializeBinaryRow form.
    let part = split.partition();
    let raw_len = part.data().len() as i32;
    w.write_i32(4 + raw_len); // total_len = arity_int(4) + raw_size
    w.write_i32(part.arity());
    w.write_bytes(part.data());

    w.write_i32(split.bucket());
    w.write_string(split.bucket_path())?;

    match split.total_buckets_opt() {
        None => w.write_bool(false),
        Some(v) => {
            w.write_bool(true);
            w.write_i32(v);
        }
    }

    write_data_file_list(&mut w, split.before_files())?;
    write_deletion_file_list(&mut w, split.before_deletion_files())?;
    write_data_file_list(&mut w, split.data_files())?;
    write_deletion_file_list(&mut w, split.data_deletion_files())?;

    w.write_bool(split.is_streaming());
    w.write_bool(split.raw_convertible());

    Ok(w.into_inner())
}

/// Deserialize a `DataSplit` from bytes produced by paimon-cpp's
/// `Split::Serialize` (or by [`serialize_data_split`]).
pub fn deserialize_data_split(buf: &[u8]) -> crate::Result<DataSplit> {
    let mut r = BeReader::new(buf);
    let magic = r.read_i64()?;
    if magic == INDEXED_SPLIT_MAGIC {
        return Err(crate::Error::Unsupported {
            message: "IndexedSplit not supported by paimon-rust split serde".into(),
        });
    }
    if magic != DATA_SPLIT_MAGIC {
        return Err(crate::Error::DataInvalid {
            message: format!("invalid split magic: {magic:#018x}"),
            source: None,
        });
    }
    let version = r.read_i32()?;
    let wire_version = match version {
        VERSION_8 => DataFileMetaWireVersion::V8,
        VERSION_9 => DataFileMetaWireVersion::V9,
        other => {
            return Err(crate::Error::Unsupported {
                message: format!(
                    "DataSplit version {other} not supported, only v{VERSION_8} or v{VERSION_9}"
                ),
            });
        }
    };
    let snapshot_id = r.read_i64()?;

    // partition (SerializationUtils form: i32 total_len + i32 arity + raw bytes).
    let total_len = r.read_i32()?;
    if total_len < 4 {
        return Err(crate::Error::DataInvalid {
            message: format!("partition total_len {total_len} < 4"),
            source: None,
        });
    }
    let arity = r.read_i32()?;
    let body_len = (total_len - 4) as usize;
    let body = r.read_bytes(body_len)?.to_vec();
    let partition = BinaryRow::from_bytes(arity, body);

    let bucket = r.read_i32()?;
    let bucket_path = r.read_string()?;

    let total_buckets_opt = if r.read_bool()? {
        Some(r.read_i32()?)
    } else {
        None
    };

    let before_files = read_data_file_list(&mut r, wire_version)?;
    let before_deletion_files = read_deletion_file_list(&mut r)?;
    let data_files = read_data_file_list(&mut r, wire_version)?;
    let data_deletion_files = read_deletion_file_list(&mut r)?;

    let is_streaming = r.read_bool()?;
    let raw_convertible = r.read_bool()?;

    if r.remaining() == 1 {
        return Err(crate::Error::Unsupported {
            message: "FallbackDataSplit not supported by paimon-rust split serde".into(),
        });
    }
    if r.remaining() != 0 {
        return Err(crate::Error::DataInvalid {
            message: format!(
                "trailing {} bytes after DataSplit at pos {}",
                r.remaining(),
                r.pos()
            ),
            source: None,
        });
    }

    let mut builder = DataSplit::builder()
        .with_snapshot(snapshot_id)
        .with_partition(partition)
        .with_bucket(bucket)
        .with_bucket_path(bucket_path)
        .with_total_buckets(total_buckets_opt.unwrap_or(-1))
        .with_data_files(data_files)
        .with_before_files(before_files)
        .with_is_streaming(is_streaming)
        .with_raw_convertible(raw_convertible);
    if let Some(b) = before_deletion_files {
        builder = builder.with_before_deletion_files(b);
    }
    if let Some(d) = data_deletion_files {
        builder = builder.with_data_deletion_files(d);
    }
    builder.build()
}

/// Convenience wrapper: deserialize a single split from bytes and wrap it in
/// a fresh [`Plan`]. Returned plans are equivalent to what
/// `TableScan::plan().await` would produce, so callers can hand them straight
/// to [`crate::table::TableRead::to_arrow`] without first re-running scan
/// planning.
///
/// Use case: a remote planner serialized splits over the wire (e.g. via the
/// Bleem coordinator), and the worker just wants to read them. This skips the
/// `paimon_plan` round-trip the on-box scan path goes through.
///
/// One byte buffer in → one-element plan out. Concatenating many serialized
/// splits is not supported here because the wire form is length-implicit
/// (each split's bytes consume the entire buffer); call this once per split
/// and merge the resulting plans on the caller's side, or extend with a
/// length-framed batch encoding if that pattern shows up repeatedly.
pub fn deserialize_data_split_to_plan(buf: &[u8]) -> crate::Result<Plan> {
    let split = deserialize_data_split(buf)?;
    Ok(Plan::new(vec![split]))
}

// --------------------- helpers ---------------------

fn write_data_file_list(w: &mut BeWriter, files: &[DataFileMeta]) -> crate::Result<()> {
    w.write_i32(files.len() as i32);
    for f in files {
        let bytes = data_file_meta_to_serialized_bytes(f)?;
        w.write_bytes(&bytes);
    }
    Ok(())
}

fn read_data_file_list(
    r: &mut BeReader,
    wire_version: DataFileMetaWireVersion,
) -> crate::Result<Vec<DataFileMeta>> {
    let n = r.read_i32()?;
    if n < 0 {
        return Err(crate::Error::DataInvalid {
            message: format!("DataFileMeta list size {n} < 0"),
            source: None,
        });
    }
    // Each entry needs at least the 4-byte BE size prefix; reject impossible
    // counts before they trigger a huge `Vec::with_capacity` on malformed
    // input. Real splits hold ~tens of files, so this is far above any
    // legitimate upper bound.
    let max_possible = r.remaining() / 4;
    if (n as usize) > max_possible {
        return Err(crate::Error::DataInvalid {
            message: format!(
                "DataFileMeta list size {n} exceeds {max_possible} (4 bytes/entry, {} bytes remaining)",
                r.remaining()
            ),
            source: None,
        });
    }
    let mut out = Vec::with_capacity(n as usize);
    for _ in 0..n {
        // We feed `data_file_meta_from_serialized_bytes_versioned` exactly the
        // bytes it needs to parse one entry by reading the i32 size first,
        // then handing over `[size_be_bytes][body]`.
        let size = r.read_i32()?;
        if size < 0 {
            return Err(crate::Error::DataInvalid {
                message: format!("DataFileMeta size {size} < 0"),
                source: None,
            });
        }
        let body = r.read_bytes(size as usize)?;
        let mut buf = Vec::with_capacity(4 + body.len());
        buf.extend_from_slice(&size.to_be_bytes());
        buf.extend_from_slice(body);
        let (meta, consumed) = data_file_meta_from_serialized_bytes_versioned(&buf, wire_version)?;
        debug_assert_eq!(consumed, buf.len());
        // Suppress unused-arity-constant warning when DEBUG_ASSERTIONS is off.
        let _ = DATA_FILE_META_V8_ARITY;
        out.push(meta);
    }
    Ok(out)
}

fn write_deletion_file_list(
    w: &mut BeWriter,
    files: Option<&[Option<DeletionFile>]>,
) -> crate::Result<()> {
    match files {
        None => w.write_i8(0),
        // Java/C++ encode "empty" the same way as "absent": i8 0. We match that
        // (so Some(empty) round-trips as None on read; the public API contract
        // already collapses these states).
        Some(list) if list.is_empty() => w.write_i8(0),
        Some(list) => {
            w.write_i8(1);
            w.write_i32(list.len() as i32);
            for entry in list {
                match entry {
                    None => w.write_i8(0),
                    Some(df) => {
                        w.write_i8(1);
                        w.write_string(df.path())?;
                        w.write_i64(df.offset());
                        w.write_i64(df.length());
                        w.write_i64(df.cardinality().unwrap_or(-1));
                    }
                }
            }
        }
    }
    Ok(())
}

fn read_deletion_file_list(r: &mut BeReader) -> crate::Result<Option<Vec<Option<DeletionFile>>>> {
    let has = r.read_i8()?;
    if has == 0 {
        return Ok(None);
    }
    if has != 1 {
        return Err(crate::Error::DataInvalid {
            message: format!("DeletionFile list flag must be 0 or 1, got {has}"),
            source: None,
        });
    }
    let n = r.read_i32()?;
    if n < 0 {
        return Err(crate::Error::DataInvalid {
            message: format!("DeletionFile list size {n} < 0"),
            source: None,
        });
    }
    // Each entry has at least a 1-byte presence flag; reject counts larger
    // than the bytes left in the stream so a malformed `n` cannot drive a
    // huge `Vec::with_capacity`.
    if (n as usize) > r.remaining() {
        return Err(crate::Error::DataInvalid {
            message: format!(
                "DeletionFile list size {n} exceeds {} bytes remaining (1 byte/entry minimum)",
                r.remaining()
            ),
            source: None,
        });
    }
    let mut out = Vec::with_capacity(n as usize);
    for _ in 0..n {
        let entry_flag = r.read_i8()?;
        if entry_flag == 0 {
            out.push(None);
        } else if entry_flag == 1 {
            let path = r.read_string()?;
            let offset = r.read_i64()?;
            let length = r.read_i64()?;
            let card = r.read_i64()?;
            let cardinality = if card == -1 { None } else { Some(card) };
            out.push(Some(DeletionFile::new(path, offset, length, cardinality)));
        } else {
            return Err(crate::Error::DataInvalid {
                message: format!("DeletionFile entry flag must be 0 or 1, got {entry_flag}"),
                source: None,
            });
        }
    }
    Ok(Some(out))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::stats::BinaryTableStats;
    use crate::spec::BinaryRowBuilder;
    use chrono::TimeZone;
    use chrono::Utc;

    fn empty_stats() -> BinaryTableStats {
        BinaryTableStats::empty()
    }

    fn make_partition_row() -> BinaryRow {
        let mut b = BinaryRowBuilder::new(1);
        b.write_int(0, 10);
        b.build()
    }

    fn make_meta(file_name: &str) -> DataFileMeta {
        DataFileMeta {
            file_name: file_name.into(),
            file_size: 1024,
            row_count: 7,
            min_key: vec![0u8; 4],
            max_key: vec![0u8; 4],
            key_stats: empty_stats(),
            value_stats: empty_stats(),
            min_sequence_number: 0,
            max_sequence_number: 6,
            schema_id: 0,
            level: 1,
            extra_files: Vec::new(),
            creation_time: Some(Utc.timestamp_millis_opt(1_700_000_000_000).unwrap()),
            delete_row_count: None,
            embedded_index: None,
            file_source: Some(0),
            value_stats_cols: None,
            external_path: None,
            first_row_id: None,
            write_cols: None,
            merge_mode: None,
            commit_snapshot_id: None,
        }
    }

    fn make_simple_split() -> DataSplit {
        DataSplit::builder()
            .with_snapshot(7)
            .with_partition(make_partition_row())
            .with_bucket(3)
            .with_bucket_path("data/some.db/some/f1=10/bucket-3".into())
            .with_total_buckets(4)
            .with_data_files(vec![make_meta("data-0001.parquet")])
            .with_raw_convertible(true)
            .build()
            .unwrap()
    }

    #[test]
    fn rust_self_round_trip_simple() {
        let split = make_simple_split();
        let bytes = serialize_data_split(&split).unwrap();
        let decoded = deserialize_data_split(&bytes).unwrap();

        assert_eq!(decoded.snapshot_id(), 7);
        assert_eq!(decoded.bucket(), 3);
        assert_eq!(decoded.bucket_path(), split.bucket_path());
        assert_eq!(decoded.total_buckets_opt(), Some(4));
        assert_eq!(decoded.is_streaming(), false);
        assert_eq!(decoded.raw_convertible(), true);
        assert_eq!(decoded.data_files().len(), 1);
        assert_eq!(decoded.data_files()[0].file_name, "data-0001.parquet");
        assert!(decoded.data_deletion_files().is_none());
        assert!(decoded.before_files().is_empty());

        // Idempotent re-serialize.
        let bytes2 = serialize_data_split(&decoded).unwrap();
        assert_eq!(bytes, bytes2);
    }

    #[test]
    fn rust_round_trip_with_deletion_files() {
        let dv = DeletionFile::new("FILE:/tmp/external/index-1".into(), 1, 22, Some(1));
        let split = DataSplit::builder()
            .with_snapshot(4)
            .with_partition(make_partition_row())
            .with_bucket(1)
            .with_bucket_path("data/x.db/x/f1=10/bucket-1".into())
            .with_total_buckets(2)
            .with_data_files(vec![make_meta("data-foo.orc")])
            .with_data_deletion_files(vec![Some(dv.clone())])
            .with_raw_convertible(true)
            .build()
            .unwrap();
        let bytes = serialize_data_split(&split).unwrap();
        let decoded = deserialize_data_split(&bytes).unwrap();
        let dvs = decoded.data_deletion_files().unwrap();
        assert_eq!(dvs.len(), 1);
        let got = dvs[0].as_ref().unwrap();
        assert_eq!(got.path(), dv.path());
        assert_eq!(got.offset(), 1);
        assert_eq!(got.length(), 22);
        assert_eq!(got.cardinality(), Some(1));

        let bytes2 = serialize_data_split(&decoded).unwrap();
        assert_eq!(bytes, bytes2);
    }

    #[test]
    fn rust_round_trip_total_buckets_absent() {
        let split = DataSplit::builder()
            .with_snapshot(1)
            .with_partition(make_partition_row())
            .with_bucket(0)
            .with_bucket_path("data/x.db/x/bucket-0".into())
            // total_buckets defaults to -1 => absent on the wire.
            .with_data_files(vec![make_meta("a.orc")])
            .build()
            .unwrap();
        let bytes = serialize_data_split(&split).unwrap();
        let decoded = deserialize_data_split(&bytes).unwrap();
        assert_eq!(decoded.total_buckets_opt(), None);
    }

    #[test]
    fn rejects_indexed_split_magic() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&INDEXED_SPLIT_MAGIC.to_be_bytes());
        bytes.extend_from_slice(&[0u8; 4]); // pretend version
        let err = deserialize_data_split(&bytes).unwrap_err();
        assert!(matches!(err, crate::Error::Unsupported { .. }));
    }

    #[test]
    fn rejects_unknown_magic() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&0i64.to_be_bytes());
        bytes.extend_from_slice(&[0u8; 4]);
        let err = deserialize_data_split(&bytes).unwrap_err();
        assert!(matches!(err, crate::Error::DataInvalid { .. }));
    }

    #[test]
    fn rejects_wrong_version() {
        let mut bytes = serialize_data_split(&make_simple_split()).unwrap();
        // Patch version (bytes 8..12) to 7.
        bytes[8..12].copy_from_slice(&7i32.to_be_bytes());
        let err = deserialize_data_split(&bytes).unwrap_err();
        assert!(matches!(err, crate::Error::Unsupported { .. }));
    }

    #[test]
    fn rejects_fallback_trailing_byte() {
        let mut bytes = serialize_data_split(&make_simple_split()).unwrap();
        bytes.push(1u8);
        let err = deserialize_data_split(&bytes).unwrap_err();
        assert!(matches!(err, crate::Error::Unsupported { .. }));
    }

    #[test]
    fn rejects_truncated_input() {
        let bytes = serialize_data_split(&make_simple_split()).unwrap();
        let truncated = &bytes[..bytes.len() - 5];
        let err = deserialize_data_split(truncated).unwrap_err();
        assert!(matches!(err, crate::Error::DataInvalid { .. }));
    }

    #[test]
    fn rejects_split_with_row_ranges() {
        use crate::table::source::RowRange;
        let split = DataSplit::builder()
            .with_snapshot(1)
            .with_partition(make_partition_row())
            .with_bucket(0)
            .with_bucket_path("data/x.db/x/bucket-0".into())
            .with_data_files(vec![make_meta("a.orc")])
            .with_row_ranges(vec![RowRange::new(0, 9)])
            .build()
            .unwrap();
        let err = serialize_data_split(&split).unwrap_err();
        assert!(
            matches!(err, crate::Error::Unsupported { .. }),
            "expected Unsupported, got {err:?}"
        );
    }

    #[test]
    fn rejects_bucket_path_too_long() {
        let mut split = make_simple_split();
        // Replace bucket_path with one that overflows i16 length.
        let huge = "a".repeat(i16::MAX as usize + 1);
        split = DataSplit::builder()
            .with_snapshot(split.snapshot_id())
            .with_partition(split.partition().clone())
            .with_bucket(split.bucket())
            .with_bucket_path(huge)
            .with_total_buckets(split.total_buckets())
            .with_data_files(split.data_files().to_vec())
            .build()
            .unwrap();
        let err = serialize_data_split(&split).unwrap_err();
        assert!(
            matches!(err, crate::Error::DataInvalid { .. }),
            "expected DataInvalid, got {err:?}"
        );
    }

    /// A malformed list count must not drive a huge `Vec::with_capacity`. We
    /// patch the data_files i32 count to a billion and expect a clean
    /// DataInvalid rather than an OOM-shaped allocation before reading.
    #[test]
    fn rejects_oversized_data_file_list_count() {
        let serialized = serialize_data_split(&make_simple_split()).unwrap();

        // Walk the framing to locate the data_files i32 count. Skip:
        //   8 (magic) + 4 (ver) + 8 (snap) = 20
        //   partition: i32 total_len + total_len bytes
        //   bucket i32, bucket_path (i16 len + bytes)
        //   total_buckets flag (1 byte) + i32 (since make_simple_split sets it)
        //   before_files i32 (=0)
        //   before_dv flag i8 (=0)
        let mut pos = 20;
        let part_total = i32::from_be_bytes(serialized[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4 + part_total;
        pos += 4; // bucket
        let bp_len = i16::from_be_bytes(serialized[pos..pos + 2].try_into().unwrap()) as usize;
        pos += 2 + bp_len;
        pos += 1 + 4; // total_buckets flag + value
        pos += 4; // before_files count
        pos += 1; // before_dv flag (0)
                  // pos now points to the data_files i32 count.

        let mut bytes = serialized.clone();
        bytes[pos..pos + 4].copy_from_slice(&1_000_000_000i32.to_be_bytes());
        let err = deserialize_data_split(&bytes).unwrap_err();
        assert!(
            matches!(err, crate::Error::DataInvalid { .. }),
            "expected DataInvalid, got {err:?}"
        );
    }

    // ---------------------------------------------------------------
    // Cross-language compatibility against paimon-cpp golden fixtures.
    // ---------------------------------------------------------------

    fn read_fixture(name: &str) -> Vec<u8> {
        let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("testdata/data_splits")
            .join(name);
        std::fs::read(&path)
            .unwrap_or_else(|e| panic!("failed to read fixture {}: {e}", path.display()))
    }

    /// Fixture: `pk_dv_index_in_data_with_external/data_split-02`
    /// Reference: paimon-cpp `data_split_test.cpp::TestDeserializeVersion8WithWriteColsAndExternalPath`.
    #[test]
    fn cpp_fixture_with_external_path_decodes() {
        let bytes = read_fixture("data_split-02_pk_dv_index_in_data_with_external");
        let split = deserialize_data_split(&bytes).expect("decode fixture");
        assert_eq!(split.snapshot_id(), 4);
        assert_eq!(split.bucket(), 1);
        assert_eq!(
            split.bucket_path(),
            "data/orc/pk_dv_index_in_data_with_external.db/pk_dv_index_in_data_with_external/f1=10/bucket-1"
        );
        assert_eq!(split.total_buckets_opt(), Some(2));
        assert!(!split.is_streaming());
        assert!(split.raw_convertible());
        assert!(split.before_files().is_empty());

        assert_eq!(split.data_files().len(), 1);
        let df = &split.data_files()[0];
        assert_eq!(
            df.file_name,
            "data-72b62a5f-d698-4db5-b51a-04c0dc027702-0.orc"
        );
        assert_eq!(df.file_size, 961);
        assert_eq!(df.row_count, 5);
        assert_eq!(df.min_sequence_number, 0);
        assert_eq!(df.max_sequence_number, 4);
        assert_eq!(df.schema_id, 0);
        assert_eq!(df.level, 5);
        assert_eq!(
            df.creation_time.unwrap().timestamp_millis(),
            1_757_354_415_711
        );
        assert_eq!(df.delete_row_count, Some(0));
        assert_eq!(df.file_source, Some(0)); // FileSource::Append() = 0
        assert_eq!(
            df.external_path.as_deref(),
            Some(
                "FILE:/tmp/external/f1=10/bucket-1/data-72b62a5f-d698-4db5-b51a-04c0dc027702-0.orc"
            )
        );

        let dvs = split.data_deletion_files().expect("has deletion list");
        assert_eq!(dvs.len(), 1);
        let dv = dvs[0].as_ref().expect("first deletion file present");
        assert_eq!(
            dv.path(),
            "FILE:/tmp/external/f1=10/bucket-1/index-419e7c6b-9cad-49e8-9cd2-6187471df954-1"
        );
        assert_eq!(dv.offset(), 1);
        assert_eq!(dv.length(), 22);
        assert_eq!(dv.cardinality(), Some(1));
    }

    /// Byte-stable Rust round-trip: encode → decode → encode must be deterministic.
    /// Critical for caching and idempotent transforms; weaker than byte-equality
    /// against a paimon-cpp fixture but does not depend on the paimon-cpp
    /// version that produced the fixture.
    #[test]
    fn rust_serialize_is_deterministic() {
        let split = make_simple_split();
        let bytes = serialize_data_split(&split).unwrap();
        let decoded = deserialize_data_split(&bytes).unwrap();
        let bytes2 = serialize_data_split(&decoded).unwrap();
        assert_eq!(bytes, bytes2);
    }

    /// Fixture: `pk_dv_index_not_in_data_no_external/data_split-02`
    /// Reference: paimon-cpp `data_split_test.cpp::TestDeserializeVersion8WithWriteCols`.
    /// Exercises `external_path = None` and a non-external deletion file path.
    /// Note: this fixture was produced by an earlier paimon-cpp build whose
    /// DataFileMeta v8 schema had 20 fields (no `_MERGE_MODE` /
    /// `_COMMIT_SNAPSHOT_ID`). Current paimon-cpp serializes 22 fields, so we
    /// can no longer assert byte-equality against this golden fixture; we still
    /// verify that the deserializer correctly recovers all populated fields.
    #[test]
    fn cpp_fixture_no_external_decodes() {
        let bytes = read_fixture("data_split-02_pk_dv_index_not_in_data_no_external");
        let split = deserialize_data_split(&bytes).expect("decode fixture");
        assert_eq!(split.snapshot_id(), 4);
        assert_eq!(split.bucket(), 1);
        assert_eq!(
            split.bucket_path(),
            "data/orc/pk_dv_index_not_in_data_no_external.db/pk_dv_index_not_in_data_no_external/f1=10/bucket-1"
        );
        assert_eq!(split.total_buckets_opt(), Some(2));
        assert_eq!(split.data_files().len(), 1);

        let df = &split.data_files()[0];
        assert_eq!(
            df.file_name,
            "data-aa87291d-2a90-4846-b106-1bb4c76d74db-0.orc"
        );
        assert_eq!(df.file_size, 961);
        assert_eq!(df.row_count, 5);
        assert!(df.external_path.is_none(), "fixture has no external_path");
        assert_eq!(
            df.creation_time.unwrap().timestamp_millis(),
            1_757_349_273_246
        );

        let dvs = split.data_deletion_files().expect("has deletion list");
        let dv = dvs[0].as_ref().unwrap();
        assert_eq!(
            dv.path(),
            "data/orc/pk_dv_index_not_in_data_no_external.db/pk_dv_index_not_in_data_no_external/index/index-aa60193d-d7cd-434f-bc1a-c1adb210e1f7-1"
        );
        assert_eq!(dv.cardinality(), Some(1));
    }

    /// Older-version fixtures must be rejected with `Error::Unsupported`. Bleem
    /// callers can then surface a clear "regenerate v8 split" message instead
    /// of silently mis-decoding fields.
    #[test]
    fn cpp_fixture_v3_append_rejected() {
        let bytes = read_fixture("data_split-01_append_10");
        let err = deserialize_data_split(&bytes).unwrap_err();
        assert!(
            matches!(err, crate::Error::Unsupported { .. }),
            "expected Unsupported, got {err:?}"
        );
    }

    /// Older-version fixtures must be rejected with `Error::Unsupported`.
    #[test]
    fn cpp_fixture_v6_pk_total_buckets_rejected() {
        let bytes = read_fixture("data_split-01_pk_table_with_total_buckets");
        let err = deserialize_data_split(&bytes).unwrap_err();
        assert!(
            matches!(err, crate::Error::Unsupported { .. }),
            "expected Unsupported, got {err:?}"
        );
    }

    /// Round-trip a Rust-built split through the plan-shaped helper. The
    /// returned plan must hold exactly the deserialized split, with all
    /// fields preserved.
    #[test]
    fn deserialize_to_plan_round_trips_simple_split() {
        let split = make_simple_split();
        let bytes = serialize_data_split(&split).unwrap();
        let plan = deserialize_data_split_to_plan(&bytes).unwrap();
        assert_eq!(plan.splits().len(), 1);
        let decoded = &plan.splits()[0];
        assert_eq!(decoded.snapshot_id(), split.snapshot_id());
        assert_eq!(decoded.bucket(), split.bucket());
        assert_eq!(decoded.bucket_path(), split.bucket_path());
        assert_eq!(decoded.total_buckets_opt(), split.total_buckets_opt());
        assert_eq!(decoded.is_streaming(), split.is_streaming());
        assert_eq!(decoded.raw_convertible(), split.raw_convertible());
        assert_eq!(decoded.data_files().len(), split.data_files().len());
    }

    /// The plan helper must propagate the same errors as the lower-level
    /// `deserialize_data_split`, not silently fall back to an empty plan.
    #[test]
    fn deserialize_to_plan_propagates_errors() {
        let mut bytes = vec![0u8; 12];
        bytes[0..8].copy_from_slice(&INDEXED_SPLIT_MAGIC.to_be_bytes());
        let err = deserialize_data_split_to_plan(&bytes).unwrap_err();
        assert!(matches!(err, crate::Error::Unsupported { .. }));
    }

    /// Plan from a paimon-cpp v8 fixture must be readable as a single-split
    /// plan with the expected file payload.
    #[test]
    fn deserialize_to_plan_decodes_cpp_fixture() {
        let bytes = read_fixture("data_split-02_pk_dv_index_in_data_with_external");
        let plan = deserialize_data_split_to_plan(&bytes).unwrap();
        assert_eq!(plan.splits().len(), 1);
        let split = &plan.splits()[0];
        assert_eq!(split.bucket(), 1);
        assert_eq!(split.data_files().len(), 1);
        assert_eq!(
            split.data_files()[0].file_name,
            "data-72b62a5f-d698-4db5-b51a-04c0dc027702-0.orc"
        );
    }

    // ---------------------------------------------------------------
    // v9 (Java DataSplit / DataFileMetaSerializer) wire compatibility.
    //
    // The Java side swapped the trailing two fields when adding versioned-
    // partial-update: v9 row has commit_snapshot_id at slot 20 and merge_mode
    // at slot 21, the opposite of paimon-cpp's v8 layout. We don't yet have a
    // Java-produced fixture, so we synthesize one by starting from our v8
    // encoder and mutating the wire bytes in place — patch the version int
    // and swap slots 20/21 in every DataFileMeta row body. If the decoder
    // routes through `DataFileMetaWireVersion::V9`, the original field values
    // round-trip exactly.
    // ---------------------------------------------------------------

    /// Locate every DataFileMeta row body in a serialized split and call
    /// `mutate` on each `&mut [u8]` (the 22-field row data, no size prefix).
    /// Walks the outer framing identically to `deserialize_data_split`.
    fn for_each_meta_row(bytes: &mut [u8], mut mutate: impl FnMut(&mut [u8])) {
        // magic(8) + ver(4) + snap(8) = 20
        let mut pos = 20;
        let part_total = i32::from_be_bytes(bytes[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4 + part_total;
        pos += 4; // bucket
        let bp_len = i16::from_be_bytes(bytes[pos..pos + 2].try_into().unwrap()) as usize;
        pos += 2 + bp_len;
        // total_buckets flag + optional i32
        let tb_flag = bytes[pos];
        pos += 1;
        if tb_flag != 0 {
            pos += 4;
        }

        // before_files i32 count, then n × (i32 size + body)
        let consume_list = |bytes: &mut [u8],
                            pos: &mut usize,
                            mutate: &mut dyn FnMut(&mut [u8])| {
            let n = i32::from_be_bytes(bytes[*pos..*pos + 4].try_into().unwrap()) as usize;
            *pos += 4;
            for _ in 0..n {
                let size = i32::from_be_bytes(bytes[*pos..*pos + 4].try_into().unwrap()) as usize;
                *pos += 4;
                let body = &mut bytes[*pos..*pos + size];
                mutate(body);
                *pos += size;
            }
        };
        consume_list(bytes, &mut pos, &mut mutate);

        // before_deletion_files: flag + (i32 n + entries)
        if bytes[pos] != 0 {
            pos += 1;
            let n = i32::from_be_bytes(bytes[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;
            for _ in 0..n {
                let entry_flag = bytes[pos];
                pos += 1;
                if entry_flag != 0 {
                    let p_len =
                        i16::from_be_bytes(bytes[pos..pos + 2].try_into().unwrap()) as usize;
                    pos += 2 + p_len + 8 + 8 + 8;
                }
            }
        } else {
            pos += 1;
        }

        consume_list(bytes, &mut pos, &mut mutate);
        // We don't need to walk past data_files for the v9-swap fixture; the
        // remaining bytes (data_deletion_files + flags) aren't touched.
    }

    /// Swap the 8-byte fixed slot 20 with slot 21 in a 22-field DataFileMeta
    /// row body, and swap the corresponding null bits. After this, a row
    /// originally written by `data_file_meta_to_row` (v8 layout) reads back
    /// correctly under the v9 decoder.
    fn swap_v9_layout(row_body: &mut [u8]) {
        const NULL_BITS_SIZE: usize = 8;
        const SLOT20: usize = NULL_BITS_SIZE + 20 * 8;
        const SLOT21: usize = NULL_BITS_SIZE + 21 * 8;
        // Swap 8-byte fixed slots.
        for i in 0..8 {
            row_body.swap(SLOT20 + i, SLOT21 + i);
        }
        // Swap null bits at field positions 20 and 21 (bit_index = pos + 8).
        let bit20 = 20 + 8;
        let bit21 = 21 + 8;
        let byte20 = bit20 / 8;
        let byte21 = bit21 / 8;
        let mask20 = 1u8 << (bit20 % 8);
        let mask21 = 1u8 << (bit21 % 8);
        let v20 = row_body[byte20] & mask20 != 0;
        let v21 = row_body[byte21] & mask21 != 0;
        if v20 {
            row_body[byte21] |= mask21;
        } else {
            row_body[byte21] &= !mask21;
        }
        if v21 {
            row_body[byte20] |= mask20;
        } else {
            row_body[byte20] &= !mask20;
        }
    }

    fn make_v9_bytes_from_meta_with_tail(commit_id: i64, merge_mode: i8) -> Vec<u8> {
        // Build a split whose only file carries commit_snapshot_id + merge_mode.
        let meta = DataFileMeta {
            file_name: "data-v9-fixture.orc".into(),
            file_size: 256,
            row_count: 5,
            min_key: vec![0u8; 4],
            max_key: vec![0u8; 4],
            key_stats: empty_stats(),
            value_stats: empty_stats(),
            min_sequence_number: 0,
            max_sequence_number: 4,
            schema_id: 0,
            level: 0,
            extra_files: Vec::new(),
            creation_time: Some(Utc.timestamp_millis_opt(1_700_000_000_000).unwrap()),
            delete_row_count: Some(0),
            embedded_index: None,
            file_source: Some(0),
            value_stats_cols: None,
            external_path: None,
            first_row_id: None,
            write_cols: None,
            merge_mode: Some(merge_mode),
            commit_snapshot_id: Some(commit_id),
        };
        let split = DataSplit::builder()
            .with_snapshot(7)
            .with_partition(make_partition_row())
            .with_bucket(0)
            .with_bucket_path("data/x.db/x/bucket-0".into())
            .with_total_buckets(1)
            .with_data_files(vec![meta])
            .build()
            .unwrap();

        let mut bytes = serialize_data_split(&split).unwrap();
        // Patch outer version 8 → 9.
        bytes[8..12].copy_from_slice(&9i32.to_be_bytes());
        // Swap field-20/21 in every row body so the wire matches Java v9
        // semantics (commit_snapshot_id at 20, merge_mode at 21).
        for_each_meta_row(&mut bytes, swap_v9_layout);
        bytes
    }

    #[test]
    fn v9_round_trip_decodes_commit_snapshot_id_and_merge_mode() {
        let bytes = make_v9_bytes_from_meta_with_tail(99, 1);
        let split = deserialize_data_split(&bytes).expect("v9 decode");
        assert_eq!(split.data_files().len(), 1);
        let df = &split.data_files()[0];
        assert_eq!(df.commit_snapshot_id, Some(99));
        assert_eq!(df.merge_mode, Some(1));
        assert_eq!(df.file_name, "data-v9-fixture.orc");
    }

    /// v9 with both tail fields null must round-trip the nulls (not silently
    /// fill them with the wrong slot's content).
    #[test]
    fn v9_round_trip_handles_null_tail_fields() {
        // Build a meta with nulls at both tail slots, run v8 encode, swap to
        // v9 layout, decode under v9 → expect both fields to come back as None.
        let meta = DataFileMeta {
            file_name: "data-v9-nulls.orc".into(),
            file_size: 100,
            row_count: 1,
            min_key: vec![0u8; 4],
            max_key: vec![0u8; 4],
            key_stats: empty_stats(),
            value_stats: empty_stats(),
            min_sequence_number: 0,
            max_sequence_number: 0,
            schema_id: 0,
            level: 0,
            extra_files: Vec::new(),
            creation_time: None,
            delete_row_count: None,
            embedded_index: None,
            file_source: None,
            value_stats_cols: None,
            external_path: None,
            first_row_id: None,
            write_cols: None,
            merge_mode: None,
            commit_snapshot_id: None,
        };
        let split = DataSplit::builder()
            .with_snapshot(1)
            .with_partition(make_partition_row())
            .with_bucket(0)
            .with_bucket_path("data/x.db/x/bucket-0".into())
            .with_data_files(vec![meta])
            .build()
            .unwrap();
        let mut bytes = serialize_data_split(&split).unwrap();
        bytes[8..12].copy_from_slice(&9i32.to_be_bytes());
        for_each_meta_row(&mut bytes, swap_v9_layout);

        let decoded = deserialize_data_split(&bytes).unwrap();
        let df = &decoded.data_files()[0];
        assert!(df.merge_mode.is_none());
        assert!(df.commit_snapshot_id.is_none());
    }

    /// A v10+ split (or any future version we haven't certified) must still
    /// be refused — silently routing it through the v9 path would risk
    /// mis-decoding fields that change shape again.
    #[test]
    fn rejects_v10_wire() {
        let mut bytes = serialize_data_split(&make_simple_split()).unwrap();
        bytes[8..12].copy_from_slice(&10i32.to_be_bytes());
        let err = deserialize_data_split(&bytes).unwrap_err();
        assert!(matches!(err, crate::Error::Unsupported { .. }));
    }
}
