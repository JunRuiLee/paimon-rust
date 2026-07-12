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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use super::ann::PkVectorAnnSearcher;
use super::data_invalid;
use super::exact::exact_search;
use super::metric::VectorSearchMetric;
use super::reader::PkVectorReader;
use super::result::PkVectorSearchResult;
use crate::deletion_vector::DeletionVector;
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

/// True if `candidate` ranks strictly better (BEST_FIRST) than `weakest`:
/// distance ASC, then data_file_name ASC, then row_position ASC.
fn is_better_than(candidate: &PkVectorSearchResult, weakest: &PkVectorSearchResult) -> bool {
    candidate
        .distance
        .total_cmp(&weakest.distance)
        .then_with(|| candidate.data_file_name.cmp(&weakest.data_file_name))
        .then_with(|| candidate.row_position.cmp(&weakest.row_position))
        == std::cmp::Ordering::Less
}

fn add_candidate(
    heap: &mut Vec<PkVectorSearchResult>,
    candidate: PkVectorSearchResult,
    limit: usize,
) {
    if heap.len() < limit {
        heap.push(candidate);
        return;
    }
    // Find the current worst (BEST_FIRST-largest) and replace if the candidate beats it.
    let worst_idx = heap
        .iter()
        .enumerate()
        .max_by(|(_, a), (_, b)| {
            a.distance
                .total_cmp(&b.distance)
                .then_with(|| a.data_file_name.cmp(&b.data_file_name))
                .then_with(|| a.row_position.cmp(&b.row_position))
        })
        .map(|(i, _)| i);
    if let Some(i) = worst_idx {
        if is_better_than(&candidate, &heap[i]) {
            heap[i] = candidate;
        }
    }
}

/// ANN + exact data-file fallback search for one snapshot bucket. Mirrors Java
/// `org.apache.paimon.index.pkvector.PrimaryKeyVectorBucketSearch.search`.
///
/// `ann_searcher` may be `None` only when there are no ANN segments; segments
/// present with `None` is an error.
#[allow(clippy::too_many_arguments)]
pub(crate) fn bucket_search(
    ann_searcher: Option<&dyn PkVectorAnnSearcher>,
    ann_segments: &[BucketAnnSegment],
    active_files: &[BucketActiveFile],
    deletion_vectors: &HashMap<String, Arc<DeletionVector>>,
    exact_reader_factory: &mut dyn FnMut(
        &BucketActiveFile,
    ) -> crate::Result<Box<dyn PkVectorReader>>,
    query: &[f32],
    metric: VectorSearchMetric,
    limit: usize,
    search_options: &HashMap<String, String>,
) -> crate::Result<Vec<PkVectorSearchResult>> {
    if limit == 0 {
        return Err(data_invalid("vector search limit must be positive"));
    }

    let mut files_by_name: HashMap<&str, &BucketActiveFile> = HashMap::new();
    for file in active_files {
        if file.row_count < 0 {
            return Err(data_invalid(format!(
                "active data file {} row count must not be negative: {}",
                file.file_name, file.row_count
            )));
        }
        if files_by_name
            .insert(file.file_name.as_str(), file)
            .is_some()
        {
            return Err(data_invalid(format!(
                "duplicate data file: {}",
                file.file_name
            )));
        }
    }

    let mut heap: Vec<PkVectorSearchResult> = Vec::with_capacity(limit + 1);
    let mut covered: HashSet<String> = HashSet::new();

    for segment in ann_segments {
        for source in segment.source_meta.source_files() {
            match files_by_name.get(source.file_name()) {
                Some(active) if active.row_count == source.row_count() => {
                    covered.insert(source.file_name().to_string());
                }
                _ => {
                    return Err(data_invalid(format!(
                        "ANN source {} does not match the active data file",
                        source.file_name()
                    )));
                }
            }
        }
        let searcher = ann_searcher.ok_or_else(|| data_invalid("ANN search is not configured"))?;
        for result in searcher.search(
            segment,
            query,
            metric,
            limit,
            deletion_vectors,
            search_options,
        )? {
            add_candidate(&mut heap, result, limit);
        }
    }

    for file in active_files {
        if covered.contains(&file.file_name) {
            continue;
        }
        let dv = deletion_vectors.get(&file.file_name).cloned();
        let is_excluded = move |position: i64| -> bool {
            match &dv {
                Some(dv) => u64::try_from(position)
                    .map(|p| dv.is_deleted(p))
                    .unwrap_or(false),
                None => false,
            }
        };
        let mut reader = exact_reader_factory(file)?;
        for result in exact_search(
            &file.file_name,
            reader.as_mut(),
            query,
            metric,
            limit,
            &is_excluded,
        )? {
            add_candidate(&mut heap, result, limit);
        }
    }

    heap.sort_by(|a, b| {
        a.distance
            .total_cmp(&b.distance)
            .then_with(|| a.data_file_name.cmp(&b.data_file_name))
            .then_with(|| a.row_position.cmp(&b.row_position))
    });
    Ok(heap)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::PkVectorSourceFile;
    use crate::vindex::pkvector::ann::PkVectorAnnSearcher;
    use crate::vindex::pkvector::reader::test_support::ArrayReader;
    use roaring::RoaringBitmap;
    use std::cell::RefCell;

    fn meta(files: &[(&str, i64)]) -> PkVectorSourceMeta {
        PkVectorSourceMeta::new(
            files
                .iter()
                .map(|(n, r)| PkVectorSourceFile::new((*n).into(), *r).unwrap())
                .collect(),
        )
        .unwrap()
    }

    fn active(name: &str, rows: i64) -> BucketActiveFile {
        BucketActiveFile {
            file_name: name.into(),
            row_count: rows,
        }
    }

    /// Fake ANN searcher returning preset results and recording calls.
    struct FakeAnnSearcher {
        result: Vec<PkVectorSearchResult>,
    }
    impl PkVectorAnnSearcher for FakeAnnSearcher {
        fn search(
            &self,
            _segment: &BucketAnnSegment,
            _query: &[f32],
            _metric: VectorSearchMetric,
            _limit: usize,
            _dvs: &HashMap<String, Arc<DeletionVector>>,
            _opts: &HashMap<String, String>,
        ) -> crate::Result<Vec<PkVectorSearchResult>> {
            Ok(self.result.clone())
        }
    }

    #[test]
    fn test_rejects_non_positive_limit() {
        let mut factory =
            |_: &BucketActiveFile| -> crate::Result<Box<dyn PkVectorReader>> { unreachable!() };
        let err = bucket_search(
            None,
            &[],
            &[],
            &HashMap::new(),
            &mut factory,
            &[0.0, 0.0],
            VectorSearchMetric::L2,
            0,
            &HashMap::new(),
        )
        .unwrap_err();
        assert!(err.to_string().contains("positive"));
    }

    #[test]
    fn test_merges_ann_and_exact_without_rescanning_covered_files() {
        // data-1 is ANN-covered; data-2 is exact fallback. Factory must never be
        // called for data-1.
        let segment = BucketAnnSegment {
            source_meta: meta(&[("data-1", 2)]),
        };
        let ann = FakeAnnSearcher {
            result: vec![PkVectorSearchResult {
                data_file_name: "data-1".into(),
                row_position: 1,
                distance: 0.5,
            }],
        };
        let calls = RefCell::new(Vec::<String>::new());
        let mut factory = |f: &BucketActiveFile| -> crate::Result<Box<dyn PkVectorReader>> {
            calls.borrow_mut().push(f.file_name.clone());
            // data-2 vectors: pos0 {1,0} dist 1.0, pos1 {3,0} dist 9.0
            Ok(Box::new(ArrayReader::new(
                2,
                vec![Some(vec![1.0, 0.0]), Some(vec![3.0, 0.0])],
            )))
        };
        let results = bucket_search(
            Some(&ann),
            &[segment],
            &[active("data-1", 2), active("data-2", 2)],
            &HashMap::new(),
            &mut factory,
            &[0.0, 0.0],
            VectorSearchMetric::L2,
            2,
            &HashMap::new(),
        )
        .unwrap();
        assert_eq!(
            results,
            vec![
                PkVectorSearchResult {
                    data_file_name: "data-1".into(),
                    row_position: 1,
                    distance: 0.5
                },
                PkVectorSearchResult {
                    data_file_name: "data-2".into(),
                    row_position: 0,
                    distance: 1.0
                },
            ]
        );
        assert_eq!(calls.borrow().as_slice(), &["data-2".to_string()]);
    }

    #[test]
    fn test_exact_fallback_merges_files_and_applies_deletion_vectors() {
        // No ANN. data-1 pos0 {0,0} deleted; remaining candidates merge across files.
        let calls = RefCell::new(0);
        let mut factory = |f: &BucketActiveFile| -> crate::Result<Box<dyn PkVectorReader>> {
            *calls.borrow_mut() += 1;
            let vectors = match f.file_name.as_str() {
                "data-1" => vec![Some(vec![0.0, 0.0]), Some(vec![2.0, 0.0])],
                "data-2" => vec![Some(vec![1.0, 0.0]), None],
                _ => unreachable!(),
            };
            Ok(Box::new(ArrayReader::new(2, vectors)))
        };
        let mut dvs: HashMap<String, Arc<DeletionVector>> = HashMap::new();
        let mut bm = RoaringBitmap::new();
        bm.insert(0); // data-1 position 0 deleted
        dvs.insert("data-1".into(), Arc::new(DeletionVector::from_bitmap(bm)));

        let results = bucket_search(
            None,
            &[],
            &[active("data-1", 2), active("data-2", 2)],
            &dvs,
            &mut factory,
            &[0.0, 0.0],
            VectorSearchMetric::L2,
            2,
            &HashMap::new(),
        )
        .unwrap();
        // Candidates: data-2 pos0 {1,0} dist 1.0; data-1 pos1 {2,0} dist 4.0.
        // (data-1 pos0 deleted, data-2 pos1 null.)
        assert_eq!(
            results,
            vec![
                PkVectorSearchResult {
                    data_file_name: "data-2".into(),
                    row_position: 0,
                    distance: 1.0
                },
                PkVectorSearchResult {
                    data_file_name: "data-1".into(),
                    row_position: 1,
                    distance: 4.0
                },
            ]
        );
    }

    #[test]
    fn test_rejects_duplicate_active_file_name() {
        let mut factory =
            |_: &BucketActiveFile| -> crate::Result<Box<dyn PkVectorReader>> { unreachable!() };
        let err = bucket_search(
            None,
            &[],
            &[active("dup", 1), active("dup", 1)],
            &HashMap::new(),
            &mut factory,
            &[0.0, 0.0],
            VectorSearchMetric::L2,
            1,
            &HashMap::new(),
        )
        .unwrap_err();
        assert!(err.to_string().contains("duplicate") || err.to_string().contains("Duplicate"));
    }

    #[test]
    fn test_rejects_ann_source_missing_or_mismatched_active_file() {
        let ann = FakeAnnSearcher { result: vec![] };
        // Segment references data-1 with 2 rows, but active file has 3 rows.
        let segment = BucketAnnSegment {
            source_meta: meta(&[("data-1", 2)]),
        };
        let mut factory =
            |_: &BucketActiveFile| -> crate::Result<Box<dyn PkVectorReader>> { unreachable!() };
        let err = bucket_search(
            Some(&ann),
            &[segment],
            &[active("data-1", 3)],
            &HashMap::new(),
            &mut factory,
            &[0.0, 0.0],
            VectorSearchMetric::L2,
            1,
            &HashMap::new(),
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("does not match") || err.to_string().contains("ANN source")
        );
    }

    #[test]
    fn test_rejects_segments_without_ann_searcher() {
        let segment = BucketAnnSegment {
            source_meta: meta(&[("data-1", 2)]),
        };
        let mut factory =
            |_: &BucketActiveFile| -> crate::Result<Box<dyn PkVectorReader>> { unreachable!() };
        let err = bucket_search(
            None,
            &[segment],
            &[active("data-1", 2)],
            &HashMap::new(),
            &mut factory,
            &[0.0, 0.0],
            VectorSearchMetric::L2,
            1,
            &HashMap::new(),
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("ANN search is not configured")
                || err.to_string().contains("not configured")
        );
    }

    #[test]
    fn test_negative_active_row_count_rejected() {
        let mut factory =
            |_: &BucketActiveFile| -> crate::Result<Box<dyn PkVectorReader>> { unreachable!() };
        let err = bucket_search(
            None,
            &[],
            &[active("data-1", -1)],
            &HashMap::new(),
            &mut factory,
            &[0.0, 0.0],
            VectorSearchMetric::L2,
            1,
            &HashMap::new(),
        )
        .unwrap_err();
        assert!(err.to_string().contains("row count") || err.to_string().contains("-1"));
    }
}
