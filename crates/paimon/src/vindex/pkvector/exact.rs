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

use std::cmp::Ordering;
use std::collections::BinaryHeap;

use super::data_invalid;
use super::metric::VectorSearchMetric;
use super::reader::PkVectorReader;
use super::result::PkVectorSearchResult;

/// A candidate wrapped so a max-heap keeps the WORST candidate on top:
/// worst = largest distance, ties broken by largest row_position. Popping the
/// top therefore evicts the least-wanted candidate. Uses `total_cmp` for a
/// deterministic total order over f32 (NaN-safe, no panic).
struct WorstFirst(PkVectorSearchResult);

impl PartialEq for WorstFirst {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}
impl Eq for WorstFirst {}
impl PartialOrd for WorstFirst {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for WorstFirst {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0
            .distance
            .total_cmp(&other.0.distance)
            .then_with(|| self.0.row_position.cmp(&other.0.row_position))
    }
}

/// True if `candidate` ranks strictly better (BEST_FIRST) than the current
/// worst-on-heap `weakest`: smaller distance, ties broken by smaller position.
fn is_better_than(candidate: &PkVectorSearchResult, weakest: &PkVectorSearchResult) -> bool {
    candidate
        .distance
        .total_cmp(&weakest.distance)
        .then_with(|| candidate.row_position.cmp(&weakest.row_position))
        == Ordering::Less
}

/// Exact Top-K over one sequential physical-row vector source. Mirrors Java
/// `PkVectorExactSearcher.search`. Results are sorted BEST_FIRST: distance ASC,
/// then row_position ASC (single file, so data_file_name is constant).
pub(crate) fn exact_search(
    data_file_name: &str,
    reader: &mut dyn PkVectorReader,
    query: &[f32],
    metric: VectorSearchMetric,
    limit: usize,
    is_excluded: &dyn Fn(i64) -> bool,
) -> crate::Result<Vec<PkVectorSearchResult>> {
    if query.len() != reader.dimension() {
        return Err(data_invalid(format!(
            "query vector dimension does not match: index expects {}, got {}",
            reader.dimension(),
            query.len()
        )));
    }
    if limit == 0 {
        return Err(data_invalid("vector search limit must be positive"));
    }
    if let Some(i) = query.iter().position(|v| !v.is_finite()) {
        return Err(data_invalid(format!(
            "query vector element at position {i} must be finite"
        )));
    }
    let row_count = reader.row_count();
    if row_count < 0 {
        return Err(data_invalid(format!(
            "vector reader row count must not be negative: {row_count}"
        )));
    }

    let mut reuse = vec![0.0f32; reader.dimension()];
    let mut heap: BinaryHeap<WorstFirst> = BinaryHeap::with_capacity(limit + 1);
    for position in 0..row_count {
        let has_vector = reader.read_next_vector(&mut reuse)?;
        if !has_vector || is_excluded(position) {
            continue;
        }
        let candidate = PkVectorSearchResult {
            data_file_name: data_file_name.to_string(),
            row_position: position,
            distance: metric.compute_distance(query, &reuse),
        };
        if heap.len() < limit {
            heap.push(WorstFirst(candidate));
        } else if heap
            .peek()
            .is_some_and(|worst| is_better_than(&candidate, &worst.0))
        {
            heap.pop();
            heap.push(WorstFirst(candidate));
        }
    }

    let mut results: Vec<PkVectorSearchResult> = heap.into_iter().map(|w| w.0).collect();
    results.sort_by(|a, b| {
        a.distance
            .total_cmp(&b.distance)
            .then_with(|| a.row_position.cmp(&b.row_position))
    });
    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vindex::pkvector::metric::VectorSearchMetric;
    use crate::vindex::pkvector::reader::test_support::ArrayReader;

    fn no_exclusion() -> impl Fn(i64) -> bool {
        |_| false
    }

    #[test]
    fn test_distances_for_supported_metrics() {
        // Java testDistancesForSupportedMetrics: q=[2,0] over stored [1,0].
        for (metric, expected) in [
            (VectorSearchMetric::L2, 1.0f32),
            (VectorSearchMetric::Cosine, 0.0),
            (VectorSearchMetric::InnerProduct, -2.0),
        ] {
            let mut reader = ArrayReader::new(2, vec![Some(vec![1.0, 0.0])]);
            let results = exact_search(
                "data-file",
                &mut reader,
                &[2.0, 0.0],
                metric,
                1,
                &no_exclusion(),
            )
            .unwrap();
            assert_eq!(results[0].distance, expected);
        }
    }

    #[test]
    fn test_rejects_dimension_mismatch() {
        let mut reader = ArrayReader::new(2, vec![Some(vec![1.0, 0.0])]);
        let err = exact_search(
            "data-file",
            &mut reader,
            &[1.0],
            VectorSearchMetric::L2,
            1,
            &no_exclusion(),
        )
        .unwrap_err();
        assert!(err.to_string().contains("dimension"));
    }

    #[test]
    fn test_rejects_non_positive_limit() {
        let mut reader = ArrayReader::new(2, vec![Some(vec![1.0, 0.0])]);
        let err = exact_search(
            "data-file",
            &mut reader,
            &[1.0, 0.0],
            VectorSearchMetric::L2,
            0,
            &no_exclusion(),
        )
        .unwrap_err();
        assert!(err.to_string().contains("positive"));
    }

    #[test]
    fn test_rejects_non_finite_query() {
        let mut reader = ArrayReader::new(2, vec![Some(vec![1.0, 0.0])]);
        let err = exact_search(
            "data-file",
            &mut reader,
            &[f32::NAN, 0.0],
            VectorSearchMetric::L2,
            1,
            &no_exclusion(),
        )
        .unwrap_err();
        assert!(err.to_string().contains("finite"));
    }

    #[test]
    fn test_rejects_negative_row_count() {
        struct NegativeReader;
        impl PkVectorReader for NegativeReader {
            fn dimension(&self) -> usize {
                2
            }
            fn row_count(&self) -> i64 {
                -1
            }
            fn read_next_vector(&mut self, _reuse: &mut [f32]) -> crate::Result<bool> {
                unreachable!()
            }
        }
        let mut reader = NegativeReader;
        let err = exact_search(
            "data-file",
            &mut reader,
            &[1.0, 0.0],
            VectorSearchMetric::L2,
            1,
            &no_exclusion(),
        )
        .unwrap_err();
        assert!(err.to_string().contains("row count") || err.to_string().contains("-1"));
    }

    #[test]
    fn test_preserves_null_and_excluded_physical_positions() {
        // Java testPreservesNullAndDeletedPhysicalPositions:
        // vectors [{3,0}, null, {1,0}, {2,0}], q=[0,0], l2, limit 2, exclude pos==2.
        let mut reader = ArrayReader::new(
            2,
            vec![
                Some(vec![3.0, 0.0]),
                None,
                Some(vec![1.0, 0.0]),
                Some(vec![2.0, 0.0]),
            ],
        );
        let results = exact_search(
            "data-file",
            &mut reader,
            &[0.0, 0.0],
            VectorSearchMetric::L2,
            2,
            &|pos| pos == 2,
        )
        .unwrap();
        assert_eq!(
            results,
            vec![
                PkVectorSearchResult {
                    data_file_name: "data-file".into(),
                    row_position: 3,
                    distance: 4.0
                },
                PkVectorSearchResult {
                    data_file_name: "data-file".into(),
                    row_position: 0,
                    distance: 9.0
                },
            ]
        );
    }

    #[test]
    fn test_tie_break_prefers_smaller_row_position() {
        // Equal distances -> smaller row_position ranks first.
        let mut reader =
            ArrayReader::new(1, vec![Some(vec![1.0]), Some(vec![1.0]), Some(vec![1.0])]);
        let results = exact_search(
            "data-file",
            &mut reader,
            &[0.0],
            VectorSearchMetric::L2,
            2,
            &no_exclusion(),
        )
        .unwrap();
        assert_eq!(
            results.iter().map(|r| r.row_position).collect::<Vec<_>>(),
            vec![0, 1]
        );
    }
}
