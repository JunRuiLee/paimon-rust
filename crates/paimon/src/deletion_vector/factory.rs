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

/// Factory that resolves a [`DeletionVector`] for each data file in a split,
/// reading on demand.
///
/// Mirrors Java's `DeletionVector.Factory` (`create(fileName) ->
/// Optional<DeletionVector>`), but the Rust version is **lazy**:
/// [`Self::new`] only records the per-file `DeletionFile` index — no IO
/// happens until [`Self::get_deletion_vector`] is called. This caps
/// per-split peak memory at the size of a single decoded DV (the one held
/// by the file currently being streamed) instead of the sum of every DV
/// in the split.
///
/// The factory does not cache decoded vectors. Each split iterates
/// `data_files()` exactly once, so caching would only re-pin peak memory
/// to the eager-load behaviour we are removing. Callers hold the returned
/// `Arc<DeletionVector>` for their data file's stream lifetime; once the
/// stream ends, the Arc drops and the bitmap is freed before the next
/// file's DV is read.
pub struct DeletionVectorFactory {
    file_io: FileIO,
    /// Map from data file name to the source `DeletionFile` (path / offset
    /// / length / cardinality). Cheap to clone; held until the factory
    /// itself is dropped at end-of-split.
    pending: HashMap<String, crate::DeletionFile>,
}

impl DeletionVectorFactory {
    /// Build a factory by recording which data files have an associated
    /// deletion file. Synchronous — does not read or decode any DV.
    ///
    /// `data_deletion_files` aligns positionally with `data_files`; entries
    /// that are `None` are skipped (no deletion vector for that data file).
    pub fn new(
        file_io: &FileIO,
        data_files: &[DataFileMeta],
        data_deletion_files: Option<&[Option<crate::DeletionFile>]>,
    ) -> Self {
        let mut pending = HashMap::new();
        if let Some(deletion_files) = data_deletion_files {
            for (data_file, opt_df) in data_files.iter().zip(deletion_files.iter()) {
                if let Some(df) = opt_df.as_ref() {
                    pending.insert(data_file.file_name.clone(), df.clone());
                }
            }
        }
        Self {
            file_io: file_io.clone(),
            pending,
        }
    }

    /// Resolve the `DeletionVector` for a data file, reading and decoding
    /// it on demand. Returns `Ok(None)` if the data file has no deletion
    /// vector attached (the common case).
    ///
    /// Each call performs IO; results are not cached. Callers normally
    /// invoke this once per file and hold the returned `Arc` for the
    /// lifetime of that file's record-batch stream.
    pub async fn get_deletion_vector(
        &self,
        data_file_name: &str,
    ) -> Result<Option<Arc<DeletionVector>>> {
        let Some(df) = self.pending.get(data_file_name) else {
            return Ok(None);
        };
        let dv = Self::read(&self.file_io, df).await?;
        Ok(Some(Arc::new(dv)))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::deletion_vector::core::DeletionVector;
    use crate::io::FileIOBuilder;
    use crate::spec::stats::BinaryTableStats;
    use bytes::BufMut;
    use roaring::RoaringBitmap;
    use std::sync::Weak;

    /// Magic int for the 32-bit DV header (mirrors Java
    /// `BitmapDeletionVector.MAGIC_NUMBER`). Kept local rather than reaching
    /// into `core::MAGIC_NUMBER` (private to the module).
    const MAGIC_NUMBER_32: i32 = 1581511376;

    /// Build a minimal `DataFileMeta` whose only meaningful field for the
    /// factory is `file_name`. The factory only uses `file_name` to look up
    /// per-file `DeletionFile` entries; every other field can take a
    /// throwaway value.
    fn data_file(name: &str) -> DataFileMeta {
        DataFileMeta {
            file_name: name.to_string(),
            file_size: 0,
            row_count: 0,
            min_key: vec![],
            max_key: vec![],
            key_stats: BinaryTableStats::new(vec![], vec![], vec![]),
            value_stats: BinaryTableStats::new(vec![], vec![], vec![]),
            min_sequence_number: 0,
            max_sequence_number: 0,
            schema_id: 0,
            level: 0,
            extra_files: vec![],
            creation_time: None,
            delete_row_count: None,
            embedded_index: None,
            first_row_id: None,
            write_cols: None,
            external_path: None,
            file_source: None,
            value_stats_cols: None,
            commit_snapshot_id: None,
            merge_mode: None,
        }
    }

    /// Encode a 32-bit DV blob in the on-disk layout
    /// `[outer_length:i32 BE][magic:i32 BE][roaring_bytes][crc:i32 BE]`
    /// (matches `kv_file_reader::tests::write_test_dv_blob`). Returns the
    /// full byte buffer to be written to a `memory:` path. The returned
    /// `(offset, length)` are what `DeletionFile::new` should receive: the
    /// physical blob starts at byte 0 (no leading version byte here), and
    /// `length = MAGIC + roaring_bytes` excludes the outer length+crc
    /// frame, matching Java's 32-bit `DeletionVectorMeta.length` semantics.
    fn build_dv_blob(deleted: &[u32]) -> (Vec<u8>, i64, i64) {
        let mut bitmap = RoaringBitmap::new();
        for &r in deleted {
            bitmap.insert(r);
        }
        let mut roaring_bytes = Vec::new();
        bitmap.serialize_into(&mut roaring_bytes).unwrap();
        let inner_size: i32 = (4 + roaring_bytes.len()) as i32;

        let mut blob: Vec<u8> = Vec::with_capacity(4 + inner_size as usize + 4);
        blob.put_i32(inner_size); // outer length field (BE)
        blob.put_i32(MAGIC_NUMBER_32); // magic (BE)
        blob.extend_from_slice(&roaring_bytes);
        blob.put_i32(0); // crc (skipped on read)
        (blob, 0, inner_size as i64)
    }

    async fn write_blob(file_io: &FileIO, path: &str, blob: Vec<u8>) {
        let out = file_io.new_output(path).unwrap();
        let mut writer = out.writer().await.unwrap();
        writer.write(bytes::Bytes::from(blob)).await.unwrap();
        writer.close().await.unwrap();
    }

    /// `new` must not perform any IO. Building a factory with a deletion
    /// file pointing at a non-existent path succeeds; only an explicit
    /// `get_deletion_vector` for that file triggers IO and surfaces the
    /// error.
    #[tokio::test]
    async fn test_lazy_factory_new_does_no_io() {
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let data_files = vec![data_file("file_a"), data_file("file_b")];
        let deletion_files = vec![
            Some(crate::DeletionFile::new(
                "memory:/never_exists/dv.bin".to_string(),
                0,
                4,
                Some(0),
            )),
            None,
        ];
        // Construction itself is sync and does not touch the FS — the bad
        // path above does NOT panic here.
        let factory = DeletionVectorFactory::new(&file_io, &data_files, Some(&deletion_files));

        // file_b has no DeletionFile entry → Ok(None) without IO.
        assert!(factory
            .get_deletion_vector("file_b")
            .await
            .expect("file_b lookup")
            .is_none());

        // file_a points at a missing path → IO error surfaces here, not at
        // construction time. This is the lazy contract: a caller who never
        // touches a missing/corrupt DV pays nothing.
        let err = factory
            .get_deletion_vector("file_a")
            .await
            .expect_err("missing path must surface as error from get_deletion_vector");
        let msg = format!("{err}");
        assert!(
            msg.contains("never_exists")
                || msg.contains("not found")
                || msg.contains("NotFound"),
            "expected NotFound-style error, got: {msg}"
        );
    }

    /// Round-trip a real DV through the factory and verify the decoded
    /// bitmap matches what we wrote. Two successive lookups for the same
    /// file each return a fresh `Arc` (no caching) but contain the same
    /// rows — so callers that do retain previous Arcs see consistent data.
    #[tokio::test]
    async fn test_lazy_factory_returns_decoded_dv() {
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let dv_path = "memory:/dvs/index_blob.bin";
        let (blob, off, len) = build_dv_blob(&[1, 5, 1024]);
        write_blob(&file_io, dv_path, blob).await;

        let data_files = vec![data_file("data_xyz.parquet")];
        let deletion_files = vec![Some(crate::DeletionFile::new(
            dv_path.to_string(),
            off,
            len,
            Some(3),
        ))];
        let factory = DeletionVectorFactory::new(&file_io, &data_files, Some(&deletion_files));

        let dv1 = factory
            .get_deletion_vector("data_xyz.parquet")
            .await
            .expect("get_deletion_vector")
            .expect("DV present");
        let dv2 = factory
            .get_deletion_vector("data_xyz.parquet")
            .await
            .expect("get_deletion_vector second")
            .expect("DV present second");

        // Both Arcs decode to the same set of deleted rows.
        let collect = |dv: &Arc<DeletionVector>| -> Vec<u64> {
            let mut iter = dv.iter();
            std::iter::from_fn(move || iter.next()).collect()
        };
        assert_eq!(collect(&dv1), vec![1u64, 5, 1024]);
        assert_eq!(collect(&dv2), vec![1u64, 5, 1024]);

        // No caching: the two Arcs are distinct allocations.
        assert!(
            !Arc::ptr_eq(&dv1, &dv2),
            "factory must not cache decoded DVs"
        );
    }

    /// The lazy contract: with each Arc held only for the duration of one
    /// data file's stream, the previous DV's strong refs go to zero before
    /// the next DV is decoded. This bounds per-split peak memory at the
    /// size of one DV instead of the sum of all DVs in the split.
    #[tokio::test]
    async fn test_lazy_factory_per_file_lifetime() {
        let file_io = FileIOBuilder::new("memory").build().unwrap();

        let (blob_a, off_a, len_a) = build_dv_blob(&[10, 20]);
        write_blob(&file_io, "memory:/dvs/a.bin", blob_a).await;
        let (blob_b, off_b, len_b) = build_dv_blob(&[30, 40]);
        write_blob(&file_io, "memory:/dvs/b.bin", blob_b).await;

        let data_files = vec![data_file("a"), data_file("b")];
        let deletion_files = vec![
            Some(crate::DeletionFile::new(
                "memory:/dvs/a.bin".to_string(),
                off_a,
                len_a,
                Some(2),
            )),
            Some(crate::DeletionFile::new(
                "memory:/dvs/b.bin".to_string(),
                off_b,
                len_b,
                Some(2),
            )),
        ];
        let factory = DeletionVectorFactory::new(&file_io, &data_files, Some(&deletion_files));

        // Acquire `a`, take a Weak to it, then drop the strong reference and
        // immediately acquire `b`. The eager factory would hold both DVs
        // alive for the split's lifetime; the lazy factory holds only the
        // currently-requested one.
        let weak_a: Weak<DeletionVector>;
        {
            let dv_a = factory
                .get_deletion_vector("a")
                .await
                .expect("get a")
                .expect("a present");
            weak_a = Arc::downgrade(&dv_a);
            // dv_a dropped here, mirroring stream-end behaviour in the reader.
        }
        let _dv_b = factory
            .get_deletion_vector("b")
            .await
            .expect("get b")
            .expect("b present");

        assert!(
            weak_a.upgrade().is_none(),
            "after dropping the previous file's DV, no eager-cached copy must remain alive"
        );
    }
}
