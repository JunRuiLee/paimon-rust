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

use std::collections::HashMap;
use std::sync::Arc;

use super::data_invalid;
use super::metric::VectorSearchMetric;
use super::result::PkVectorSearchResult;
use crate::deletion_vector::DeletionVector;
use crate::spec::{PkVectorSourceFile, PkVectorSourceMeta};

/// Build the live-row-id mask for the ANN reader's `include_row_ids` filter, in
/// segment-ordinal space (source files concatenated in order). Mirrors Java
/// `PkVectorAnnSegmentSearcher.liveRowPositions`.
///
/// Returns `None` when no deletion vector is relevant to these source files
/// (empty map, or no source file has a matching DV) — nothing to mask. When at
/// least one source file has a matching DV, returns the masked live ids.
pub(crate) fn build_live_row_ids(
    source_files: &[PkVectorSourceFile],
    deletion_vectors: &HashMap<String, Arc<DeletionVector>>,
) -> crate::Result<Option<roaring::RoaringTreemap>> {
    let has_relevant_dv = source_files
        .iter()
        .any(|f| deletion_vectors.contains_key(f.file_name()));
    if !has_relevant_dv {
        return Ok(None);
    }

    let mut live = roaring::RoaringTreemap::new();
    let mut deleted = roaring::RoaringTreemap::new();
    let mut file_offset: u64 = 0;
    for source_file in source_files {
        let row_count = u64::try_from(source_file.row_count())
            .map_err(|_| data_invalid("vector source row count must not be negative"))?;
        if row_count > 0 {
            live.insert_range(file_offset..file_offset + row_count);
        }
        if let Some(dv) = deletion_vectors.get(source_file.file_name()) {
            for position in dv.iter() {
                deleted.insert(file_offset + position);
            }
        }
        file_offset = file_offset
            .checked_add(row_count)
            .ok_or_else(|| data_invalid("vector source row counts overflow u64"))?;
    }
    live -= deleted;
    Ok(Some(live))
}

/// Map ANN `(ordinal, score)` pairs to physical `(data file, position)` results,
/// validating ordinals against source metadata and rejecting hits on
/// snapshot-deleted rows. Mirrors the post-processing loop of Java
/// `PkVectorAnnSegmentSearcher.search`. Results are sorted BEST_FIRST.
pub(crate) fn map_ann_results(
    scored: &[(u64, f32)],
    source_meta: &PkVectorSourceMeta,
    deletion_vectors: &HashMap<String, Arc<DeletionVector>>,
    metric: VectorSearchMetric,
) -> crate::Result<Vec<PkVectorSearchResult>> {
    let mut results = Vec::with_capacity(scored.len());
    for &(ordinal, score) in scored {
        let ordinal_i64 = i64::try_from(ordinal)
            .map_err(|_| data_invalid(format!("ANN ordinal {ordinal} exceeds i64::MAX")))?;
        let (data_file_name, row_position) = source_meta.resolve(ordinal_i64)?;
        if let Some(dv) = deletion_vectors.get(&data_file_name) {
            let pos = u64::try_from(row_position)
                .map_err(|_| data_invalid("resolved row position must not be negative"))?;
            if dv.is_deleted(pos) {
                return Err(data_invalid(format!(
                    "ANN segment returned snapshot-deleted row position {row_position} in {data_file_name}"
                )));
            }
        }
        results.push(PkVectorSearchResult {
            data_file_name,
            row_position,
            distance: metric.score_to_distance(score),
        });
    }
    results.sort_by(|a, b| {
        a.distance
            .total_cmp(&b.distance)
            .then_with(|| a.data_file_name.cmp(&b.data_file_name))
            .then_with(|| a.row_position.cmp(&b.row_position))
    });
    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;
    use roaring::RoaringBitmap;

    fn source_meta(files: &[(&str, i64)]) -> PkVectorSourceMeta {
        let files = files
            .iter()
            .map(|(name, rows)| PkVectorSourceFile::new((*name).to_string(), *rows).unwrap())
            .collect();
        PkVectorSourceMeta::new(files).unwrap()
    }

    fn dv(deleted: &[u32]) -> Arc<DeletionVector> {
        let mut bitmap = RoaringBitmap::new();
        for &p in deleted {
            bitmap.insert(p);
        }
        Arc::new(DeletionVector::from_bitmap(bitmap))
    }

    #[test]
    fn test_build_live_row_ids_none_when_no_relevant_dv() {
        let files = [PkVectorSourceFile::new("f0".into(), 3).unwrap()];
        // Empty map -> None.
        assert!(build_live_row_ids(&files, &HashMap::new())
            .unwrap()
            .is_none());
        // Non-empty map but no matching file name -> None.
        let mut dvs = HashMap::new();
        dvs.insert("other".to_string(), dv(&[0]));
        assert!(build_live_row_ids(&files, &dvs).unwrap().is_none());
    }

    #[test]
    fn test_build_live_row_ids_masks_deleted_positions_with_file_offsets() {
        // f0 rows 0..3 (global 0,1,2), f1 rows 0..2 (global 3,4).
        let files = vec![
            PkVectorSourceFile::new("f0".into(), 3).unwrap(),
            PkVectorSourceFile::new("f1".into(), 2).unwrap(),
        ];
        let mut dvs = HashMap::new();
        dvs.insert("f0".to_string(), dv(&[1])); // deletes global 1
        dvs.insert("f1".to_string(), dv(&[0])); // deletes global 3
        let live = build_live_row_ids(&files, &dvs).unwrap().unwrap();
        assert_eq!(live.iter().collect::<Vec<u64>>(), vec![0, 2, 4]);
    }

    #[test]
    fn test_map_ann_results_maps_ordinals_to_positions_and_scores() {
        let meta = source_meta(&[("f0", 3), ("f1", 5)]);
        // ordinal 3 -> (f1, 0); ordinal 0 -> (f0, 0). l2 score_to_distance(0.5)=1.0.
        let scored = [(3u64, 0.5f32), (0u64, 0.5f32)];
        let results =
            map_ann_results(&scored, &meta, &HashMap::new(), VectorSearchMetric::L2).unwrap();
        assert_eq!(
            results,
            vec![
                PkVectorSearchResult {
                    data_file_name: "f0".into(),
                    row_position: 0,
                    distance: 1.0
                },
                PkVectorSearchResult {
                    data_file_name: "f1".into(),
                    row_position: 0,
                    distance: 1.0
                },
            ]
        );
    }

    #[test]
    fn test_map_ann_results_rejects_out_of_range_ordinal() {
        let meta = source_meta(&[("f0", 3)]);
        let err = map_ann_results(
            &[(3u64, 0.5)],
            &meta,
            &HashMap::new(),
            VectorSearchMetric::L2,
        )
        .unwrap_err();
        assert!(err.to_string().contains("out of range") || err.to_string().contains("ordinal"));
    }

    #[test]
    fn test_map_ann_results_rejects_hit_on_deleted_position() {
        let meta = source_meta(&[("f0", 3)]);
        let mut dvs = HashMap::new();
        dvs.insert("f0".to_string(), dv(&[1])); // position 1 deleted
        let err = map_ann_results(&[(1u64, 0.5)], &meta, &dvs, VectorSearchMetric::L2).unwrap_err();
        assert!(err.to_string().contains("deleted"));
    }
}
