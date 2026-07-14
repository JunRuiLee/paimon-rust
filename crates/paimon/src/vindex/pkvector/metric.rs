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

use super::data_invalid;

/// Normalize a metric name: lowercase and `-` → `_`. NO trim (deliberately
/// stricter than the build-side `vindex::normalize_metric`, to match Java
/// `VectorSearchMetric.normalize`).
pub(crate) fn normalize_metric(metric: &str) -> String {
    metric.to_ascii_lowercase().replace('-', "_")
}

/// True if the (normalized) metric is one of the three supported metrics.
pub(crate) fn is_supported_metric(metric: &str) -> bool {
    matches!(
        normalize_metric(metric).as_str(),
        "l2" | "cosine" | "inner_product"
    )
}

/// Numeric semantics for a supported vector search metric. Mirrors Java
/// `org.apache.paimon.globalindex.VectorSearchMetric`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum VectorSearchMetric {
    L2,
    Cosine,
    InnerProduct,
}

impl VectorSearchMetric {
    /// Normalize, validate, and map to the enum. Errors on an unsupported metric.
    pub(crate) fn parse(metric: &str) -> crate::Result<Self> {
        match normalize_metric(metric).as_str() {
            "l2" => Ok(Self::L2),
            "cosine" => Ok(Self::Cosine),
            "inner_product" => Ok(Self::InnerProduct),
            other => Err(data_invalid(format!(
                "unsupported vector distance metric: {other}"
            ))),
        }
    }

    /// Higher-is-better score for exact vector search.
    pub(crate) fn compute_score(&self, query: &[f32], stored: &[f32]) -> f32 {
        match self {
            Self::L2 => 1.0 / (1.0 + squared_l2(query, stored)),
            Self::Cosine => cosine_similarity(query, stored),
            Self::InnerProduct => inner_product(query, stored),
        }
    }

    /// Lower-is-better distance for exact vector search.
    pub(crate) fn compute_distance(&self, query: &[f32], stored: &[f32]) -> f32 {
        match self {
            Self::L2 => squared_l2(query, stored),
            Self::Cosine => cosine_distance(cosine_similarity(query, stored)),
            Self::InnerProduct => -inner_product(query, stored),
        }
    }

    /// Convert a higher-is-better standardized index score to a lower-is-better
    /// distance. For L2 with `score == 0.0` this yields `inf` (natural f32
    /// behavior, matching Java — no clamp).
    pub(crate) fn score_to_distance(&self, score: f32) -> f32 {
        match self {
            Self::L2 => 1.0 / score - 1.0,
            Self::Cosine => cosine_distance(score),
            Self::InnerProduct => -score,
        }
    }
}

fn squared_l2(query: &[f32], stored: &[f32]) -> f32 {
    let mut squared = 0.0f32;
    for i in 0..query.len() {
        let delta = query[i] - stored[i];
        squared += delta * delta;
    }
    squared
}

fn cosine_similarity(query: &[f32], stored: &[f32]) -> f32 {
    let mut dot = 0.0f32;
    let mut query_norm = 0.0f32;
    let mut stored_norm = 0.0f32;
    for i in 0..query.len() {
        dot += query[i] * stored[i];
        query_norm += query[i] * query[i];
        stored_norm += stored[i] * stored[i];
    }
    let denominator = ((query_norm as f64).sqrt() * (stored_norm as f64).sqrt()) as f32;
    if denominator == 0.0 {
        0.0
    } else {
        dot / denominator
    }
}

fn inner_product(query: &[f32], stored: &[f32]) -> f32 {
    let mut dot = 0.0f32;
    for i in 0..query.len() {
        dot += query[i] * stored[i];
    }
    dot
}

fn cosine_distance(similarity: f32) -> f32 {
    1.0 - similarity.clamp(-1.0, 1.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_lowercases_and_replaces_hyphens_without_trimming() {
        assert_eq!(normalize_metric("Inner-Product"), "inner_product");
        assert_eq!(normalize_metric("L2"), "l2");
        // No trim: surrounding whitespace is preserved (unlike the build-side helper).
        assert_eq!(normalize_metric(" l2 "), " l2 ");
    }

    #[test]
    fn test_is_supported_only_for_three_metrics() {
        assert!(is_supported_metric("L2"));
        assert!(is_supported_metric("cosine"));
        assert!(is_supported_metric("inner-product"));
        assert!(!is_supported_metric("manhattan"));
        assert!(!is_supported_metric(" l2 "));
    }

    #[test]
    fn test_parse_rejects_unsupported_metric() {
        assert!(VectorSearchMetric::parse("l2").is_ok());
        assert!(VectorSearchMetric::parse("cosine").is_ok());
        assert!(VectorSearchMetric::parse("inner_product").is_ok());
        assert!(VectorSearchMetric::parse("manhattan").is_err());
    }

    #[test]
    fn test_compute_distance_matches_java_anchor() {
        // Java PkVectorExactSearcherTest.testDistancesForSupportedMetrics:
        // q=[2,0], s=[1,0] -> l2=1.0, cosine=0.0, inner_product=-2.0
        let q = [2.0f32, 0.0];
        let s = [1.0f32, 0.0];
        assert_eq!(VectorSearchMetric::L2.compute_distance(&q, &s), 1.0);
        assert_eq!(VectorSearchMetric::Cosine.compute_distance(&q, &s), 0.0);
        assert_eq!(
            VectorSearchMetric::InnerProduct.compute_distance(&q, &s),
            -2.0
        );
    }

    #[test]
    fn test_compute_score_higher_is_better() {
        let q = [2.0f32, 0.0];
        let s = [1.0f32, 0.0];
        assert_eq!(VectorSearchMetric::L2.compute_score(&q, &s), 0.5); // 1/(1+1)
        assert_eq!(VectorSearchMetric::Cosine.compute_score(&q, &s), 1.0); // parallel
        assert_eq!(VectorSearchMetric::InnerProduct.compute_score(&q, &s), 2.0);
    }

    #[test]
    fn test_cosine_zero_norm_similarity_is_zero() {
        let zero = [0.0f32, 0.0];
        let s = [1.0f32, 0.0];
        assert_eq!(VectorSearchMetric::Cosine.compute_score(&zero, &s), 0.0);
        assert_eq!(VectorSearchMetric::Cosine.compute_distance(&zero, &s), 1.0);
    }

    #[test]
    fn test_cosine_non_perfect_square_norm_uses_f64_sqrt() {
        // query [0,3] -> norm 9, stored [1,2] -> norm 5; sqrt(5) is irrational
        // so the f32-sqrt path (each sqrt taken in f32, then widened) and the
        // f64-sqrt path (widen first, sqrt in f64) produce different f32 bits:
        // buggy score 0.8944271 vs correct 0.8944272. Encode the f64 contract in
        // the expected value (dot / (sqrt(9.0f64) * sqrt(5.0f64)) as f32) rather
        // than a magic literal, so this pins the Java-matching f64 arithmetic.
        let q = [0.0f32, 3.0];
        let s = [1.0f32, 2.0];
        let dot = 6.0f32;
        let denominator = ((9.0f64).sqrt() * (5.0f64).sqrt()) as f32;
        let expected = dot / denominator;
        assert_eq!(VectorSearchMetric::Cosine.compute_score(&q, &s), expected);
    }

    #[test]
    fn test_score_to_distance_l2_zero_score_is_infinite() {
        assert!(VectorSearchMetric::L2.score_to_distance(0.0).is_infinite());
        assert_eq!(VectorSearchMetric::L2.score_to_distance(0.5), 1.0); // 1/0.5 - 1
        assert_eq!(
            VectorSearchMetric::InnerProduct.score_to_distance(2.0),
            -2.0
        );
        assert_eq!(VectorSearchMetric::Cosine.score_to_distance(1.0), 0.0);
    }
}
