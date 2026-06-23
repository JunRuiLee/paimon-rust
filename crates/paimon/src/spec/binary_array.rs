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

//! BinaryArray: a minimal port of Java/C++ Paimon's BinaryArray for the v8
//! DataFileMeta serializer. Only the element types we need are supported:
//! - `array<long, nullable>` — used by `BinaryTableStats.null_counts`
//! - `array<string, nullable>` — used by `_EXTRA_FILES`
//! - `array<string, non-null>` — used by `_VALUE_STATS_COLS` / `_WRITE_COLS`
//!
//! Binary layout (little-endian inside the buffer; outer split stream is BE):
//! ```text
//! [size i32] [null bitmap, ((size+31)/32)*4 bytes] [fixed elements, element_size * size, 8B aligned] [variable area]
//! ```
//! - For 8-byte-element arrays (long, string) the fixed area is `8 * size`.
//! - String slot is `(offset << 32) | len` LE i64 pointing into the variable area,
//!   OR an inline encoding if `len <= 7` (high bit `0x80` set, length in the next 7 bits,
//!   bytes packed into the low 7 bytes).
//! - The total byte length of the array is `header_size + 8 * size + var_area_size`,
//!   rounded up to 8-byte boundaries on each var-area append.

const HIGHEST_FIRST_BIT: u64 = 0x80 << 56;
const HIGHEST_SECOND_TO_EIGHTH_BIT: u64 = 0x7F << 56;

/// Inline-encode threshold for variable-length elements (bytes). Identical to
/// Java's `BinarySection.MAX_FIX_PART_DATA_SIZE` and C++ `BinarySection::MAX_FIX_PART_DATA_SIZE`.
pub(crate) const MAX_FIX_PART_DATA_SIZE: usize = 7;

/// A binary array view backed by a single `Vec<u8>`.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct BinaryArray {
    size: i32,
    data: Vec<u8>,
}

impl BinaryArray {
    /// Header size in bytes: 4 (size i32) + null bitmap, padded to 4-byte word boundary
    /// (`((size + 31) / 32) * 4` for the bitmap).
    pub const fn cal_header_in_bytes(num_elements: i32) -> i32 {
        4 + ((num_elements + 31) / 32) * 4
    }

    /// Wrap raw bytes that already form a valid BinaryArray. Reads `size` from
    /// the first 4 bytes (LE i32). Performs minimal bounds checking.
    pub fn from_bytes(data: Vec<u8>) -> crate::Result<Self> {
        if data.len() < 4 {
            return Err(crate::Error::DataInvalid {
                message: format!(
                    "BinaryArray: buffer too short ({} bytes) to hold size prefix",
                    data.len()
                ),
                source: None,
            });
        }
        let size = i32::from_le_bytes([data[0], data[1], data[2], data[3]]);
        if size < 0 {
            return Err(crate::Error::DataInvalid {
                message: format!("BinaryArray: negative size {size}"),
                source: None,
            });
        }
        let header = Self::cal_header_in_bytes(size) as usize;
        if data.len() < header {
            return Err(crate::Error::DataInvalid {
                message: format!(
                    "BinaryArray: buffer ({} bytes) shorter than header ({header})",
                    data.len()
                ),
                source: None,
            });
        }
        Ok(Self { size, data })
    }

    pub fn size(&self) -> i32 {
        self.size
    }

    pub fn data(&self) -> &[u8] {
        &self.data
    }

    /// Bit `pos` in the bitmap that lives at `data[4 .. header]`.
    /// Mirrors C++ `BinaryArray::IsNullAt`: `BitGet({segment}, offset+4, pos)`.
    pub fn is_null_at(&self, pos: i32) -> bool {
        let byte_index = 4 + (pos as usize) / 8;
        let bit_offset = (pos as usize) % 8;
        match self.data.get(byte_index) {
            Some(b) => (b & (1 << bit_offset)) != 0,
            None => false,
        }
    }

    fn header_size(&self) -> usize {
        Self::cal_header_in_bytes(self.size) as usize
    }

    fn assert_pos(&self, pos: i32) -> crate::Result<()> {
        if pos < 0 || pos >= self.size {
            return Err(crate::Error::DataInvalid {
                message: format!("BinaryArray: index {pos} out of bounds (size={})", self.size),
                source: None,
            });
        }
        Ok(())
    }

    fn read_i64_at(&self, offset: usize) -> crate::Result<i64> {
        self.data
            .get(offset..offset + 8)
            .and_then(|s| s.try_into().ok())
            .map(i64::from_le_bytes)
            .ok_or_else(|| crate::Error::DataInvalid {
                message: format!(
                    "BinaryArray: read 8 bytes at offset {offset} exceeds data length {}",
                    self.data.len()
                ),
                source: None,
            })
    }

    pub fn get_long(&self, pos: i32) -> crate::Result<i64> {
        self.assert_pos(pos)?;
        let element_offset = self.header_size() + (pos as usize) * 8;
        self.read_i64_at(element_offset)
    }

    /// Resolve a variable-length element (string or binary) at element slot `pos`.
    /// Returns `(offset_in_data, len)` for the bytes; if the element is encoded
    /// inline (len <= 7, high bit set), returns `(slot_offset, len)` pointing at
    /// the slot itself — caller reads the inline bytes from there.
    fn resolve_var_at(&self, pos: i32) -> crate::Result<(usize, usize)> {
        self.assert_pos(pos)?;
        let slot_offset = self.header_size() + (pos as usize) * 8;
        let raw = self.read_i64_at(slot_offset)? as u64;
        let (start, len) = if raw & HIGHEST_FIRST_BIT == 0 {
            let offset = (raw >> 32) as usize;
            let len = (raw & 0xFFFF_FFFF) as usize;
            (offset, len)
        } else {
            let len = ((raw & HIGHEST_SECOND_TO_EIGHTH_BIT) >> 56) as usize;
            (slot_offset, len)
        };
        let end = start
            .checked_add(len)
            .ok_or_else(|| crate::Error::DataInvalid {
                message: format!(
                    "BinaryArray: var-len slot {pos}: offset {start} + len {len} overflows"
                ),
                source: None,
            })?;
        if end > self.data.len() {
            return Err(crate::Error::DataInvalid {
                message: format!(
                    "BinaryArray: var-len slot {pos}: range [{start}..{end}) exceeds data length {}",
                    self.data.len()
                ),
                source: None,
            });
        }
        Ok((start, len))
    }

    pub fn get_binary(&self, pos: i32) -> crate::Result<&[u8]> {
        let (start, len) = self.resolve_var_at(pos)?;
        Ok(&self.data[start..start + len])
    }

    pub fn get_string(&self, pos: i32) -> crate::Result<&str> {
        let bytes = self.get_binary(pos)?;
        std::str::from_utf8(bytes).map_err(|e| crate::Error::DataInvalid {
            message: format!("BinaryArray: invalid UTF-8 at slot {pos}: {e}"),
            source: Some(Box::new(e)),
        })
    }

    /// Build an array of nullable longs (element_size = 8). Null entries set
    /// the bitmap bit and store 0 in the slot.
    pub fn from_long_array_nullable(values: &[Option<i64>]) -> Self {
        let mut b = BinaryArrayBuilder::new(values.len() as i32, 8);
        for (i, v) in values.iter().enumerate() {
            match v {
                Some(x) => b.write_long(i as i32, *x),
                None => b.set_null_at(i as i32, 8),
            }
        }
        b.build()
    }

    /// Build an array of strings, every element non-null (matches Java's
    /// `InternalRowUtils::ToNotNullStringArrayData`).
    pub fn from_str_array_non_null(values: &[String]) -> Self {
        let mut b = BinaryArrayBuilder::new(values.len() as i32, 8);
        for (i, s) in values.iter().enumerate() {
            b.write_string_auto(i as i32, s);
        }
        b.build()
    }

    /// Build an array of nullable strings (matches Java's
    /// `InternalRowUtils::ToStringArrayData`).
    pub fn from_str_array_nullable(values: &[Option<String>]) -> Self {
        let mut b = BinaryArrayBuilder::new(values.len() as i32, 8);
        for (i, s) in values.iter().enumerate() {
            match s {
                Some(s) => b.write_string_auto(i as i32, s),
                None => b.set_null_at(i as i32, 8),
            }
        }
        b.build()
    }
}

/// Builder mirroring C++ `BinaryArrayWriter`. Only supports the element types
/// `BinaryArray` exposes for reading.
pub(crate) struct BinaryArrayBuilder {
    size: i32,
    null_bits_size: usize, // = cal_header_in_bytes(size); INCLUDES the 4-byte size prefix
    data: Vec<u8>,
}

impl BinaryArrayBuilder {
    /// `element_size` is the size of an entry in the fixed area: 8 for long /
    /// string / binary, 4 for int, etc. For the v8 DataFileMeta path we always
    /// use 8. The fixed buffer is sized + zero-filled up to the 8-byte-aligned
    /// header+elements length.
    pub fn new(size: i32, element_size: usize) -> Self {
        assert!(size >= 0);
        let null_bits_size = BinaryArray::cal_header_in_bytes(size) as usize;
        let raw = null_bits_size + element_size * (size as usize);
        let fixed_size = round_up_to_word(raw);
        let mut data = vec![0u8; fixed_size];
        // Write `size` LE i32 into the first 4 bytes (the rest of the header is the null bitmap).
        data[0..4].copy_from_slice(&size.to_le_bytes());
        Self {
            size,
            null_bits_size,
            data,
        }
    }

    fn element_offset(&self, pos: i32, element_size: usize) -> usize {
        self.null_bits_size + (pos as usize) * element_size
    }

    /// Set the null bit for `pos` and zero out its element slot.
    pub fn set_null_at(&mut self, pos: i32, element_size: usize) {
        // Bit set inside data[4 .. null_bits_size]: byte = 4 + pos/8, bit = pos%8.
        let byte_index = 4 + (pos as usize) / 8;
        let bit_offset = (pos as usize) % 8;
        self.data[byte_index] |= 1 << bit_offset;
        let off = self.element_offset(pos, element_size);
        self.data[off..off + element_size].fill(0);
    }

    pub fn write_long(&mut self, pos: i32, value: i64) {
        let off = self.element_offset(pos, 8);
        self.data[off..off + 8].copy_from_slice(&value.to_le_bytes());
    }

    /// Write a string at element `pos`. If `s.len() <= 7`, encode inline; else
    /// append to the variable area and store offset|len in the slot. Element
    /// size is 8 for var-length elements (matches C++/Java behavior).
    pub fn write_string_auto(&mut self, pos: i32, s: &str) {
        self.write_binary_auto(pos, s.as_bytes());
    }

    pub fn write_binary_auto(&mut self, pos: i32, value: &[u8]) {
        if value.len() <= MAX_FIX_PART_DATA_SIZE {
            self.write_inline(pos, value);
        } else {
            self.write_var_len(pos, value);
        }
    }

    fn write_inline(&mut self, pos: i32, value: &[u8]) {
        debug_assert!(value.len() <= MAX_FIX_PART_DATA_SIZE);
        let off = self.element_offset(pos, 8);
        // Layout matches C++ `WriteBytesToFixLenPart` on a little-endian system:
        // first byte (highest in i64 LE) = 0x80 | len; low 7 bytes = value.
        self.data[off..off + 8].fill(0);
        self.data[off..off + value.len()].copy_from_slice(value);
        self.data[off + 7] = 0x80 | (value.len() as u8);
    }

    fn write_var_len(&mut self, pos: i32, value: &[u8]) {
        let var_offset = self.data.len();
        self.data.extend_from_slice(value);
        let padding = (8 - (value.len() % 8)) % 8;
        self.data.extend(std::iter::repeat_n(0u8, padding));
        let encoded = ((var_offset as u64) << 32) | (value.len() as u64);
        let off = self.element_offset(pos, 8);
        self.data[off..off + 8].copy_from_slice(&encoded.to_le_bytes());
    }

    pub fn build(self) -> BinaryArray {
        BinaryArray {
            size: self.size,
            data: self.data,
        }
    }
}

fn round_up_to_word(n: usize) -> usize {
    let rem = n & 0x07;
    if rem == 0 {
        n
    } else {
        n + (8 - rem)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn long_array_with_nulls_round_trip() {
        let arr = BinaryArray::from_long_array_nullable(&[Some(1), None, Some(-5), None]);
        assert_eq!(arr.size(), 4);
        assert!(!arr.is_null_at(0));
        assert_eq!(arr.get_long(0).unwrap(), 1);
        assert!(arr.is_null_at(1));
        assert!(!arr.is_null_at(2));
        assert_eq!(arr.get_long(2).unwrap(), -5);
        assert!(arr.is_null_at(3));
    }

    #[test]
    fn long_array_round_trip_through_bytes() {
        let arr = BinaryArray::from_long_array_nullable(&[Some(7), Some(11), Some(13)]);
        let bytes = arr.data().to_vec();
        let arr2 = BinaryArray::from_bytes(bytes).unwrap();
        assert_eq!(arr2.size(), 3);
        assert_eq!(arr2.get_long(0).unwrap(), 7);
        assert_eq!(arr2.get_long(1).unwrap(), 11);
        assert_eq!(arr2.get_long(2).unwrap(), 13);
    }

    #[test]
    fn string_array_inline_and_varlen() {
        let arr =
            BinaryArray::from_str_array_non_null(&["f0".into(), "longer_field_name".into()]);
        assert_eq!(arr.size(), 2);
        assert_eq!(arr.get_string(0).unwrap(), "f0"); // inline (≤7)
        assert_eq!(arr.get_string(1).unwrap(), "longer_field_name"); // var-len
    }

    #[test]
    fn nullable_string_array() {
        let arr = BinaryArray::from_str_array_nullable(&[
            Some("abc".into()),
            None,
            Some("xyz_long".into()),
        ]);
        assert_eq!(arr.size(), 3);
        assert!(!arr.is_null_at(0));
        assert_eq!(arr.get_string(0).unwrap(), "abc");
        assert!(arr.is_null_at(1));
        assert!(!arr.is_null_at(2));
        assert_eq!(arr.get_string(2).unwrap(), "xyz_long");
    }

    #[test]
    fn empty_array() {
        let arr = BinaryArray::from_long_array_nullable(&[]);
        assert_eq!(arr.size(), 0);
        let arr2 = BinaryArray::from_bytes(arr.data().to_vec()).unwrap();
        assert_eq!(arr2.size(), 0);
    }

    #[test]
    fn from_bytes_rejects_short_buffer() {
        assert!(BinaryArray::from_bytes(vec![0u8, 0]).is_err());
    }

    #[test]
    fn from_bytes_rejects_negative_size() {
        let mut bytes = vec![0xFFu8, 0xFF, 0xFF, 0xFF];
        bytes.resize(16, 0);
        assert!(BinaryArray::from_bytes(bytes).is_err());
    }

    #[test]
    fn header_in_bytes_matches_cpp() {
        // C++: 4 + ((n + 31) / 32) * 4
        assert_eq!(BinaryArray::cal_header_in_bytes(0), 4 + 0);
        assert_eq!(BinaryArray::cal_header_in_bytes(1), 4 + 4);
        assert_eq!(BinaryArray::cal_header_in_bytes(32), 4 + 4);
        assert_eq!(BinaryArray::cal_header_in_bytes(33), 4 + 8);
    }
}
