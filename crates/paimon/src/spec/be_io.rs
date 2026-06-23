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
//! Big-endian framed reader/writer used by `table::split_serde`.
//!
//! Mirrors paimon-cpp's `MemorySegmentOutputStream` / `DataInputStream` outer
//! framing: all multi-byte integers are big-endian; `WriteString(s)` emits
//! `i16 BE length` followed by raw UTF-8 bytes; `WriteValue<bool>` /
//! `WriteValue<char>` emit a single byte. Internal `BinaryRow` / `BinaryArray`
//! payloads remain little-endian and are written as opaque byte slices.

/// Append-only big-endian writer over an internal `Vec<u8>`. Used to build the
/// outer split byte stream.
pub(crate) struct BeWriter {
    buf: Vec<u8>,
}

impl BeWriter {
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            buf: Vec::with_capacity(cap),
        }
    }

    pub fn into_inner(self) -> Vec<u8> {
        self.buf
    }

    pub fn write_i8(&mut self, v: i8) {
        self.buf.push(v as u8);
    }

    pub fn write_bool(&mut self, v: bool) {
        self.buf.push(u8::from(v));
    }

    pub fn write_i16(&mut self, v: i16) {
        self.buf.extend_from_slice(&v.to_be_bytes());
    }

    pub fn write_i32(&mut self, v: i32) {
        self.buf.extend_from_slice(&v.to_be_bytes());
    }

    pub fn write_i64(&mut self, v: i64) {
        self.buf.extend_from_slice(&v.to_be_bytes());
    }

    /// Write UTF-8 string as `i16 BE length` + raw bytes (mirrors C++ `WriteString`).
    /// Length must fit `i16`; debug builds assert, release builds truncate the
    /// length cast (matching Java's `(short) length` semantics).
    pub fn write_string(&mut self, s: &str) {
        debug_assert!(
            s.len() <= i16::MAX as usize,
            "BeWriter::write_string: length {} exceeds i16::MAX",
            s.len()
        );
        self.write_i16(s.len() as i16);
        self.buf.extend_from_slice(s.as_bytes());
    }

    pub fn write_bytes(&mut self, b: &[u8]) {
        self.buf.extend_from_slice(b);
    }
}

/// Big-endian reader borrowing an input slice. Used to parse the outer split
/// byte stream.
pub(crate) struct BeReader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> BeReader<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    pub fn pos(&self) -> usize {
        self.pos
    }

    pub fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    fn need(&self, n: usize) -> crate::Result<()> {
        if self.remaining() < n {
            return Err(crate::Error::DataInvalid {
                message: format!(
                    "BeReader: need {n} bytes at pos {} but only {} remaining",
                    self.pos,
                    self.remaining()
                ),
                source: None,
            });
        }
        Ok(())
    }

    pub fn read_i8(&mut self) -> crate::Result<i8> {
        self.need(1)?;
        let v = self.buf[self.pos] as i8;
        self.pos += 1;
        Ok(v)
    }

    pub fn read_bool(&mut self) -> crate::Result<bool> {
        Ok(self.read_i8()? != 0)
    }

    pub fn read_i16(&mut self) -> crate::Result<i16> {
        self.need(2)?;
        let v = i16::from_be_bytes([self.buf[self.pos], self.buf[self.pos + 1]]);
        self.pos += 2;
        Ok(v)
    }

    pub fn read_i32(&mut self) -> crate::Result<i32> {
        self.need(4)?;
        let v = i32::from_be_bytes([
            self.buf[self.pos],
            self.buf[self.pos + 1],
            self.buf[self.pos + 2],
            self.buf[self.pos + 3],
        ]);
        self.pos += 4;
        Ok(v)
    }

    pub fn read_i64(&mut self) -> crate::Result<i64> {
        self.need(8)?;
        let mut bs = [0u8; 8];
        bs.copy_from_slice(&self.buf[self.pos..self.pos + 8]);
        self.pos += 8;
        Ok(i64::from_be_bytes(bs))
    }

    /// Read `i16 BE length` + raw UTF-8 bytes; mirrors C++ `ReadString`.
    pub fn read_string(&mut self) -> crate::Result<String> {
        let len = self.read_i16()?;
        if len < 0 {
            return Err(crate::Error::DataInvalid {
                message: format!("BeReader: negative string length {len}"),
                source: None,
            });
        }
        let len = len as usize;
        self.need(len)?;
        let s = std::str::from_utf8(&self.buf[self.pos..self.pos + len])
            .map_err(|e| crate::Error::DataInvalid {
                message: format!("BeReader: invalid UTF-8 in string: {e}"),
                source: Some(Box::new(e)),
            })?
            .to_string();
        self.pos += len;
        Ok(s)
    }

    pub fn read_bytes(&mut self, n: usize) -> crate::Result<&'a [u8]> {
        self.need(n)?;
        let s = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_read_primitives_round_trip() {
        let mut w = BeWriter::with_capacity(0);
        w.write_i8(-1);
        w.write_i8(42);
        w.write_bool(true);
        w.write_bool(false);
        w.write_i16(-30000);
        w.write_i32(0x01020304);
        w.write_i64(-1);
        w.write_string("hello");
        w.write_bytes(b"\xDE\xAD\xBE\xEF");
        let buf = w.into_inner();

        let mut r = BeReader::new(&buf);
        assert_eq!(r.read_i8().unwrap(), -1);
        assert_eq!(r.read_i8().unwrap(), 42);
        assert!(r.read_bool().unwrap());
        assert!(!r.read_bool().unwrap());
        assert_eq!(r.read_i16().unwrap(), -30000);
        assert_eq!(r.read_i32().unwrap(), 0x01020304);
        assert_eq!(r.read_i64().unwrap(), -1);
        assert_eq!(r.read_string().unwrap(), "hello");
        assert_eq!(r.read_bytes(4).unwrap(), b"\xDE\xAD\xBE\xEF");
        assert_eq!(r.remaining(), 0);
    }

    #[test]
    fn write_i32_is_big_endian() {
        let mut w = BeWriter::with_capacity(0);
        w.write_i32(1);
        let buf = w.into_inner();
        assert_eq!(buf, vec![0, 0, 0, 1]);
    }

    #[test]
    fn write_i64_is_big_endian() {
        let mut w = BeWriter::with_capacity(0);
        w.write_i64(1);
        let buf = w.into_inner();
        assert_eq!(buf, vec![0, 0, 0, 0, 0, 0, 0, 1]);
    }

    #[test]
    fn write_string_uses_i16_length() {
        let mut w = BeWriter::with_capacity(0);
        w.write_string("ab");
        let buf = w.into_inner();
        // i16 BE length 2, then ASCII "ab".
        assert_eq!(buf, vec![0, 2, b'a', b'b']);
    }

    #[test]
    fn read_string_rejects_negative_length() {
        let buf = vec![0xFFu8, 0xFF, b'x'];
        let mut r = BeReader::new(&buf);
        assert!(r.read_string().is_err());
    }

    #[test]
    fn read_overruns_return_error() {
        let buf = vec![0u8, 0];
        let mut r = BeReader::new(&buf);
        assert!(r.read_i32().is_err());
    }

    #[test]
    fn empty_string_round_trip() {
        let mut w = BeWriter::with_capacity(0);
        w.write_string("");
        let buf = w.into_inner();
        assert_eq!(buf, vec![0, 0]);
        let mut r = BeReader::new(&buf);
        assert_eq!(r.read_string().unwrap(), "");
    }
}
