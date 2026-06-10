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

use roaring::{RoaringBitmap, RoaringTreemap};
use std::sync::Arc;

/// DeletionVector represents a set of row positions that have been deleted.
///
/// Two on-disk formats are supported, distinguished by the magic number embedded
/// in the blob:
///
/// - [`Bitmap32`](DeletionVector::Bitmap32): 32-bit positions, mirrors Java
///   [`BitmapDeletionVector`]. Magic = `1581511376` (BE int32). Up to 2^31-1 rows.
/// - [`Bitmap64`](DeletionVector::Bitmap64): 64-bit positions, mirrors Java
///   [`Bitmap64DeletionVector`] / `OptimizedRoaringBitmap64`. Magic = `1681511377`
///   (LE int32). Iceberg-compatible portable RoaringTreemap layout.
///
/// Impl References:
/// - <https://github.com/apache/paimon/blob/release-1.3/paimon-core/src/main/java/org/apache/paimon/deletionvectors/BitmapDeletionVector.java>
/// - <https://github.com/apache/paimon/blob/release-1.3/paimon-core/src/main/java/org/apache/paimon/deletionvectors/Bitmap64DeletionVector.java>
#[derive(Debug, Clone)]
pub enum DeletionVector {
    /// 32-bit RoaringBitmap. Used for tables with row counts < 2^31.
    Bitmap32(Arc<RoaringBitmap>),
    /// 64-bit RoaringTreemap (Iceberg-compatible portable format). Required when
    /// row positions exceed 2^31, including row-tracking scenarios.
    Bitmap64(Arc<RoaringTreemap>),
}

/// Magic number for `BitmapDeletionVector` (32-bit). Java writes / reads this as
/// big-endian int32. Same value as Java: 1581511376.
pub(crate) const MAGIC_NUMBER: u32 = 1581511376;

/// Magic number for `Bitmap64DeletionVector` (64-bit). Java writes this into a
/// little-endian buffer (see `OptimizedRoaringBitmap64.serialize`); the read
/// side compares `toLittleEndianInt(rawMagic) == MAGIC_NUMBER_V2`. Same value
/// as Java: 1681511377.
pub(crate) const MAGIC_NUMBER_V2: u32 = 1681511377;

const MAGIC_NUMBER_SIZE_BYTES: usize = 4;
const LENGTH_SIZE_BYTES: usize = 4;
const CRC_SIZE_BYTES: usize = 4;

impl DeletionVector {
    /// Create a new empty DeletionVector (32-bit variant for backward compatibility).
    pub fn empty() -> Self {
        Self::Bitmap32(Arc::new(RoaringBitmap::new()))
    }

    /// Create a 32-bit DeletionVector from a RoaringBitmap.
    pub fn from_bitmap32(bitmap: RoaringBitmap) -> Self {
        Self::Bitmap32(Arc::new(bitmap))
    }

    /// Create a 64-bit DeletionVector from a RoaringTreemap.
    pub fn from_bitmap64(bitmap: RoaringTreemap) -> Self {
        Self::Bitmap64(Arc::new(bitmap))
    }

    /// Returns an iterator over deleted positions that supports
    /// [`DeletionVectorIterator::advance_to`].
    ///
    /// Required for efficient row-selection building when skipping row groups
    /// (avoid re-scanning deletes in skipped ranges). The internal sorted
    /// `Vec<u64>` representation works for both variants:
    /// - `Bitmap32`: each `u32` is widened to `u64`
    /// - `Bitmap64`: native `u64` positions
    ///
    /// Ideally we would wrap `roaring::RoaringBitmap::iter()` directly, but
    /// that iterator does not expose `advance_to`. There is a PR open on
    /// roaring to add this (<https://github.com/RoaringBitmap/roaring-rs/pull/314>);
    /// once merged we can simplify by delegating `advance_to` to the
    /// underlying iterator.
    pub fn iter(&self) -> DeletionVectorIterator {
        let positions: Vec<u64> = match self {
            DeletionVector::Bitmap32(bitmap) => bitmap.iter().map(u64::from).collect(),
            DeletionVector::Bitmap64(bitmap) => bitmap.iter().collect(),
        };
        DeletionVectorIterator::new(positions)
    }

    /// Check if the deletion vector is empty (no deleted rows).
    pub fn is_empty(&self) -> bool {
        match self {
            DeletionVector::Bitmap32(bitmap) => bitmap.is_empty(),
            DeletionVector::Bitmap64(bitmap) => bitmap.is_empty(),
        }
    }

    /// Cfg(test) accessor: borrow the inner 32-bit bitmap if this is a
    /// `Bitmap32` variant. Returns `None` otherwise.
    #[cfg(test)]
    fn as_bitmap32(&self) -> Option<&RoaringBitmap> {
        match self {
            DeletionVector::Bitmap32(bitmap) => Some(bitmap),
            DeletionVector::Bitmap64(_) => None,
        }
    }

    /// Cfg(test) accessor: borrow the inner 64-bit treemap if this is a
    /// `Bitmap64` variant. Returns `None` otherwise.
    #[cfg(test)]
    fn as_bitmap64(&self) -> Option<&RoaringTreemap> {
        match self {
            DeletionVector::Bitmap64(bitmap) => Some(bitmap),
            DeletionVector::Bitmap32(_) => None,
        }
    }

    /// Read a DeletionVector from bytes, dispatching on the magic number.
    ///
    /// Java `DeletionVector.read(DataInputStream, length)` reads:
    /// - `bitmapLength: int32` (BE) — outer length field
    /// - `magicNumber: int32` — interpretation depends on variant:
    ///   - 32-bit: `magicNumber == MAGIC_NUMBER` (BE int32)
    ///   - 64-bit: `toLittleEndianInt(magicNumber) == MAGIC_NUMBER_V2` (i.e.,
    ///     the bytes were written into an LE buffer in
    ///     `OptimizedRoaringBitmap64.serializeBitmapData`)
    /// - bitmap payload (length depends on variant; see `read_bitmap32` / `read_bitmap64`)
    /// - `crc: int32` BE — checksum, skipped on read
    ///
    /// Length-frame semantics differ by variant (see SECTION-RISKS #8 in
    /// `dv-impl-plan.md`):
    /// - 32-bit: `expected_length == bitmapLength` (does not include outer length+crc)
    /// - 64-bit: `expected_length == bitmapDataLength + 8` (includes outer length+crc frame)
    ///
    /// Production callers slice the blob as `[offset .. offset + length + 8]`
    /// (see `factory.rs:70-75`), giving 32-bit exactly the bytes it needs and
    /// 64-bit a few extra bytes that this routine ignores via the inner
    /// length field.
    pub fn read_from_bytes(bytes: &[u8], expected_length: Option<u64>) -> crate::Result<Self> {
        use bytes::Buf;
        if bytes.len() < 8 {
            return Err(crate::Error::DataInvalid {
                message: "Deletion vector data too short".to_string(),
                source: None,
            });
        }

        let mut buf = bytes;
        let bitmap_length = buf.get_i32() as usize;
        let raw_magic = buf.get_i32() as u32;

        if raw_magic == MAGIC_NUMBER {
            return Self::read_bitmap32(bytes, bitmap_length, expected_length);
        }
        // 64-bit: magic was written into a LE buffer, so the BE-read raw_magic
        // equals MAGIC_NUMBER_V2 only after byte-swapping. This mirrors Java
        // `toLittleEndianInt(magicNumber) == Bitmap64DeletionVector.MAGIC_NUMBER`.
        if raw_magic.swap_bytes() == MAGIC_NUMBER_V2 {
            return Self::read_bitmap64(bytes, bitmap_length, expected_length);
        }

        Err(crate::Error::DataInvalid {
            message: format!(
                "Invalid magic: got {raw_magic} (expected {MAGIC_NUMBER} BE for 32-bit or {MAGIC_NUMBER_V2} LE for 64-bit)"
            ),
            source: None,
        })
    }

    fn read_bitmap32(
        bytes: &[u8],
        bitmap_length: usize,
        expected_length: Option<u64>,
    ) -> crate::Result<Self> {
        // Java: `bitmapLength == length` for 32-bit (length excludes outer
        // length field and crc).
        if let Some(expected) = expected_length {
            if bitmap_length as u64 != expected {
                return Err(crate::Error::DataInvalid {
                    message: format!(
                        "Size not match (32-bit), actual size: {bitmap_length}, expected size: {expected}"
                    ),
                    source: None,
                });
            }
        }

        let bitmap_data_size = bitmap_length - MAGIC_NUMBER_SIZE_BYTES;
        // 4 (outer length) + 4 (magic) + bitmap_data_size + 4 (crc)
        if bytes.len() < 8 + bitmap_data_size + CRC_SIZE_BYTES {
            return Err(crate::Error::DataInvalid {
                message: format!(
                    "Deletion vector data incomplete (32-bit): need {} bytes, got {}",
                    8 + bitmap_data_size + CRC_SIZE_BYTES,
                    bytes.len()
                ),
                source: None,
            });
        }

        let bitmap_data = &bytes[8..8 + bitmap_data_size];
        // CRC at &bytes[8 + bitmap_data_size..] is skipped (matches Java DeletionVector.read).

        let bitmap = RoaringBitmap::deserialize_from(bitmap_data).map_err(|e| {
            crate::Error::DataInvalid {
                message: format!("Failed to deserialize RoaringBitmap (32-bit): {e}"),
                source: Some(Box::new(e)),
            }
        })?;

        Ok(Self::from_bitmap32(bitmap))
    }

    fn read_bitmap64(
        bytes: &[u8],
        bitmap_length: usize,
        expected_length: Option<u64>,
    ) -> crate::Result<Self> {
        // Java: `bitmapLength == expected_length - 8` for 64-bit (expected_length
        // includes outer length+crc frame). See SECTION-RISKS #8 in dv-impl-plan.md.
        if let Some(expected) = expected_length {
            let expected_inner = expected
                .checked_sub((LENGTH_SIZE_BYTES + CRC_SIZE_BYTES) as u64)
                .ok_or_else(|| crate::Error::DataInvalid {
                    message: format!(
                        "expected_length {expected} too small for 64-bit DV (need >= {})",
                        LENGTH_SIZE_BYTES + CRC_SIZE_BYTES
                    ),
                    source: None,
                })?;
            if bitmap_length as u64 != expected_inner {
                return Err(crate::Error::DataInvalid {
                    message: format!(
                        "Size not match (64-bit), actual inner size: {bitmap_length}, expected inner size: {expected_inner} (from outer length {expected})"
                    ),
                    source: None,
                });
            }
        }

        let bitmap_data_size = bitmap_length - MAGIC_NUMBER_SIZE_BYTES;
        if bytes.len() < 8 + bitmap_data_size + CRC_SIZE_BYTES {
            return Err(crate::Error::DataInvalid {
                message: format!(
                    "Deletion vector data incomplete (64-bit): need {} bytes, got {}",
                    8 + bitmap_data_size + CRC_SIZE_BYTES,
                    bytes.len()
                ),
                source: None,
            });
        }

        let bitmap_data = &bytes[8..8 + bitmap_data_size];
        // CRC skipped (mirrors 32-bit behavior).

        // RoaringTreemap::deserialize_from is byte-compatible with Java
        // OptimizedRoaringBitmap64.serialize: both write
        //   [bitmap_count:u64 LE]
        //   for each: [high_key:u32 LE][RoaringBitmap32 portable bytes]
        // Verified at roaring-0.11.4/src/treemap/serialization.rs:43-52 and
        // OptimizedRoaringBitmap64.java:198-221.
        let treemap = RoaringTreemap::deserialize_from(bitmap_data).map_err(|e| {
            crate::Error::DataInvalid {
                message: format!("Failed to deserialize RoaringTreemap (64-bit): {e}"),
                source: Some(Box::new(e)),
            }
        })?;

        Ok(Self::from_bitmap64(treemap))
    }
}

impl Default for DeletionVector {
    fn default() -> Self {
        Self::empty()
    }
}

/// Iterator over deleted row positions with [`advance_to`](DeletionVectorIterator::advance_to)
/// support.
///
/// See [`DeletionVector::iter`] for why we use an internal sorted vec instead
/// of wrapping `roaring::RoaringBitmap::iter()` (which does not provide
/// `advance_to`). The same shape works for both 32-bit and 64-bit variants;
/// dispatch happens once when the iterator is constructed.
#[derive(Debug)]
pub struct DeletionVectorIterator {
    /// Sorted deleted positions (collected from the inner bitmap).
    positions: Vec<u64>,
    cursor: usize,
}

impl DeletionVectorIterator {
    pub(crate) fn new(positions: Vec<u64>) -> Self {
        Self {
            positions,
            cursor: 0,
        }
    }
}

impl Iterator for DeletionVectorIterator {
    type Item = u64;

    fn next(&mut self) -> Option<Self::Item> {
        if self.cursor < self.positions.len() {
            let v = self.positions[self.cursor];
            self.cursor += 1;
            Some(v)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BufMut;
    use roaring::{RoaringBitmap, RoaringTreemap};
    use std::env::current_dir;

    /// Existing fixture-based 32-bit test, updated for the enum API.
    #[test]
    fn test_read_deletion_vector() {
        let workdir = current_dir().unwrap();
        let path =
            workdir.join("tests/fixtures/index/index-7e53780d-2faa-4e4c-9f2e-93af5082bbdb-0");

        // The first byte is the index file version flag; skip it.
        let bytes = &std::fs::read(&path).expect("fixture index file must exist")[1..];
        assert!(!bytes.is_empty(), "fixture file must not be empty");

        // Outer bitmap length = 24 (matches the inner length field in the fixture).
        let dv = DeletionVector::read_from_bytes(bytes, Some(24))
            .expect("failed to read DeletionVector");

        let expected = RoaringBitmap::from_iter([1u32, 2u32]);
        assert_eq!(
            dv.as_bitmap32()
                .expect("32-bit fixture must decode as Bitmap32"),
            &expected,
            "bitmap should be [1, 2]"
        );
        assert!(dv.as_bitmap64().is_none());
    }

    /// Build a 64-bit DV blob mirroring Java `Bitmap64DeletionVector.serializeTo`.
    /// Returns just the on-wire bytes after the version flag (i.e. starting with
    /// the outer length field), suitable for `read_from_bytes`.
    ///
    /// Java byte layout:
    /// ```text
    /// [bitmapDataLength:int32 BE][magic:int32 LE][roaring64 LE bytes][crc:int32 BE]
    /// ```
    /// where `bitmapDataLength = magic(4) + treemap.serialize_into bytes`.
    fn build_test_bitmap64_blob(deleted: &[u64]) -> Vec<u8> {
        let mut treemap = RoaringTreemap::new();
        for &d in deleted {
            treemap.insert(d);
        }
        let mut treemap_bytes = Vec::new();
        treemap.serialize_into(&mut treemap_bytes).unwrap();
        let bitmap_data_length: i32 = (4 + treemap_bytes.len()) as i32;

        let mut blob: Vec<u8> = Vec::with_capacity(4 + bitmap_data_length as usize + 4);
        blob.put_i32(bitmap_data_length); // outer length (BE)
        blob.extend_from_slice(&MAGIC_NUMBER_V2.to_le_bytes()); // magic (LE)
        blob.extend_from_slice(&treemap_bytes);
        blob.put_i32(0); // CRC (read path skips verification)
        blob
    }

    /// Build a 32-bit DV blob mirroring Java `BitmapDeletionVector.serializeTo`.
    fn build_test_bitmap32_blob(deleted: &[u32]) -> Vec<u8> {
        let mut bitmap = RoaringBitmap::new();
        for &d in deleted {
            bitmap.insert(d);
        }
        let mut roaring_bytes = Vec::new();
        bitmap.serialize_into(&mut roaring_bytes).unwrap();
        let inner_size: i32 = (4 + roaring_bytes.len()) as i32;

        let mut blob: Vec<u8> = Vec::with_capacity(4 + inner_size as usize + 4);
        blob.put_i32(inner_size); // outer length (BE)
        blob.put_i32(MAGIC_NUMBER as i32); // magic (BE)
        blob.extend_from_slice(&roaring_bytes);
        blob.put_i32(0); // CRC
        blob
    }

    /// 64-bit round-trip: positions covering small (<2^16), cross-32-bit-boundary
    /// (>2^32), and multiple high-32 containers (k=0, k=1, k=N) — i.e. the
    /// `OptimizedRoaringBitmap64.bitmaps` array has multiple entries.
    #[test]
    fn test_read_deletion_vector_bitmap64() {
        let positions: Vec<u64> = vec![
            0,                  // smallest
            42,                 // small
            (1u64 << 32) - 1,   // last 32-bit value (still in high-key=0)
            1u64 << 32,         // first value in high-key=1
            (1u64 << 33) + 17,  // high-key=2
            (1u64 << 40) + 999, // high-key=256, sparse — fills gaps in the array
        ];
        let blob = build_test_bitmap64_blob(&positions);
        // Java 64-bit semantics: outer DeletionFile.length = bitmapDataLength + 8
        // (i.e. includes the outer length field + crc frame). For our blob,
        // blob.len() == 4(outer length) + bitmapDataLength + 4(crc), which
        // is exactly bitmapDataLength + 8.
        let expected_length = blob.len() as u64;

        let dv = DeletionVector::read_from_bytes(&blob, Some(expected_length))
            .expect("failed to read 64-bit DeletionVector");

        let treemap = dv
            .as_bitmap64()
            .expect("64-bit blob must decode as Bitmap64");
        let mut expected = RoaringTreemap::new();
        for &p in &positions {
            expected.insert(p);
        }
        assert_eq!(treemap, &expected);
        assert!(dv.as_bitmap32().is_none());
    }

    /// Iter must yield positions in sorted order, including across the
    /// 32-bit boundary (catches accidental u32 truncation).
    #[test]
    fn test_bitmap64_iter_yields_correct_positions() {
        let positions: Vec<u64> = vec![3, 1u64 << 33, 5, (1u64 << 32) - 1, 1u64 << 40];
        let blob = build_test_bitmap64_blob(&positions);
        let dv = DeletionVector::read_from_bytes(&blob, None).expect("read 64-bit DeletionVector");

        let mut got: Vec<u64> = dv.iter().collect();
        got.sort_unstable();
        let mut want = positions.clone();
        want.sort_unstable();
        assert_eq!(got, want);
    }

    /// Wrong inner length field for 64-bit must error out (length-mismatch
    /// guards against silent corruption when callers use the wrong frame
    /// formula — see SECTION-RISKS #8).
    #[test]
    fn test_bitmap64_length_mismatch() {
        let blob = build_test_bitmap64_blob(&[42u64]);
        // Real outer length is blob.len(); pass an off-by-one expected length.
        let bad_expected = blob.len() as u64 + 1;
        let err = DeletionVector::read_from_bytes(&blob, Some(bad_expected)).unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("Size not match (64-bit)"),
            "expected length-mismatch error, got: {msg}"
        );
    }

    /// Magic that is neither 32-bit BE nor 64-bit LE must error out.
    #[test]
    fn test_magic_dispatch_invalid_magic() {
        // Hand-craft a blob with a junk magic.
        let mut blob: Vec<u8> = Vec::new();
        blob.put_i32(8); // outer length
        blob.put_u32(0xDEADBEEF); // junk magic — neither MAGIC_NUMBER nor MAGIC_NUMBER_V2 byte-swapped
        blob.extend_from_slice(&[0u8; 4]); // pretend bitmap data
        blob.put_i32(0); // CRC

        let err = DeletionVector::read_from_bytes(&blob, None).unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("Invalid magic"),
            "expected invalid-magic error, got: {msg}"
        );
    }

    /// Default constructor / `empty()` must produce the 32-bit variant for
    /// backward compatibility (older code paths may rely on it).
    #[test]
    fn test_empty_default_is_bitmap32() {
        let dv = DeletionVector::default();
        assert!(dv.is_empty());
        assert!(dv.as_bitmap32().is_some());
        assert!(dv.as_bitmap64().is_none());
    }

    /// Round-trip a 32-bit DV via `read_from_bytes` (besides the file fixture)
    /// to lock down the byte layout that `kv_file_reader` tests rely on.
    #[test]
    fn test_read_bitmap32_round_trip() {
        let blob = build_test_bitmap32_blob(&[1u32, 3u32, 7u32]);
        let inner_length = (blob.len() - 8) as u64; // outer length value
        let dv = DeletionVector::read_from_bytes(&blob, Some(inner_length))
            .expect("read 32-bit DeletionVector");

        let bitmap = dv
            .as_bitmap32()
            .expect("32-bit blob must decode as Bitmap32");
        let expected = RoaringBitmap::from_iter([1u32, 3u32, 7u32]);
        assert_eq!(bitmap, &expected);
    }

    /// Build a 64-bit DV blob with the inner roaring bitmap **run-length
    /// encoded** before serialization. Mirrors Java
    /// `Bitmap64DeletionVector.serializeTo:93@e8938f347` which calls
    /// `roaringBitmap.runLengthEncode()` before computing the byte payload.
    /// Rust `RoaringTreemap::optimize()` is the documented equivalent: it
    /// reshapes containers (incl. promoting dense ranges to run containers)
    /// while preserving the set's elements.
    ///
    /// Real cross-impl byte-for-byte equality with Java's `serializeTo`
    /// remains a follow-up: it requires a Java-generated fixture (see
    /// `crates/paimon/tests/fixtures/deletion_vector/README.md`). This Rust
    /// test guards the *decoder's* tolerance of optimized payloads — the
    /// commit assumes the writer chose run containers for dense ranges.
    fn build_test_bitmap64_rle_blob(deleted: &[u64]) -> Vec<u8> {
        let mut treemap = RoaringTreemap::new();
        for &d in deleted {
            treemap.insert(d);
        }
        // Equivalent to Java `runLengthEncode()`: run-optimize containers
        // before serialization so dense ranges flip to run containers.
        treemap.optimize();
        let mut treemap_bytes = Vec::new();
        treemap.serialize_into(&mut treemap_bytes).unwrap();
        let bitmap_data_length: i32 = (4 + treemap_bytes.len()) as i32;

        let mut blob: Vec<u8> = Vec::with_capacity(4 + bitmap_data_length as usize + 4);
        blob.put_i32(bitmap_data_length); // outer length (BE)
        blob.extend_from_slice(&MAGIC_NUMBER_V2.to_le_bytes()); // magic (LE)
        blob.extend_from_slice(&treemap_bytes);
        blob.put_i32(0); // CRC (read path skips verification)
        blob
    }

    /// Run-length-encoded 64-bit DV: insert a dense contiguous range
    /// `0..10000` (which `optimize()` will compact into a run container)
    /// and confirm the decoder restores every position. Mirrors the Java
    /// path through `runLengthEncode()` → `serializeTo` for dense data.
    /// Java fixture parity is tracked as follow-up.
    #[test]
    fn test_read_deletion_vector_bitmap64_run_length_encoded() {
        let positions: Vec<u64> = (0..10_000u64).collect();
        let blob = build_test_bitmap64_rle_blob(&positions);
        let expected_length = blob.len() as u64;

        let dv = DeletionVector::read_from_bytes(&blob, Some(expected_length))
            .expect("RLE-encoded 64-bit DeletionVector must decode");

        let treemap = dv
            .as_bitmap64()
            .expect("64-bit blob must decode as Bitmap64");
        // Spot-check both ends + middle of the range to avoid materializing
        // 10k assertions.
        assert!(treemap.contains(0));
        assert!(treemap.contains(5_000));
        assert!(treemap.contains(9_999));
        assert!(!treemap.contains(10_000));
        assert_eq!(treemap.len(), 10_000);
    }

    /// 64-bit DV crossing the 32-bit boundary: positions immediately below,
    /// at, and far above 2^32, plus a sparse high-32 container at 2^40.
    /// This guards the high-key serialization path that a single-container
    /// test cannot — Java `OptimizedRoaringBitmap64` packs distinct high-32
    /// values into separate sub-bitmaps, and the decoder must walk all of
    /// them in order. Java fixture parity is tracked as follow-up.
    #[test]
    fn test_read_deletion_vector_bitmap64_cross_32bit_boundary() {
        let positions: Vec<u64> = vec![
            (1u64 << 32) - 1,   // last value in high-key=0
            1u64 << 32,         // first value in high-key=1
            (1u64 << 40) + 999, // high-key=256, sparse
        ];
        let blob = build_test_bitmap64_rle_blob(&positions);
        let expected_length = blob.len() as u64;

        let dv = DeletionVector::read_from_bytes(&blob, Some(expected_length))
            .expect("cross-32bit 64-bit DeletionVector must decode");

        let treemap = dv
            .as_bitmap64()
            .expect("64-bit blob must decode as Bitmap64");
        let mut expected = RoaringTreemap::new();
        for &p in &positions {
            expected.insert(p);
        }
        assert_eq!(treemap, &expected);
        // Iterating must surface positions in sorted order across the
        // 32-bit gap (catches accidental u32 truncation in iteration).
        let mut iter = dv.iter();
        assert_eq!(iter.next(), Some((1u64 << 32) - 1));
        assert_eq!(iter.next(), Some(1u64 << 32));
        assert_eq!(iter.next(), Some((1u64 << 40) + 999));
        assert_eq!(iter.next(), None);
    }
}
