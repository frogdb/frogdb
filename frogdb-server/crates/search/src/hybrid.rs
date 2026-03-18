//! Hybrid search fusion logic for FT.HYBRID.
//!
//! Combines BM25 text search results with vector similarity results
//! using Reciprocal Rank Fusion (RRF) or Linear weighted combination.

use std::collections::HashMap;

use crate::index::{KnnHit, SearchHit};
use crate::schema::VectorDistanceMetric;

/// Fusion strategy for combining text and vector search results.
#[derive(Debug, Clone)]
pub enum FusionStrategy {
    /// Reciprocal Rank Fusion: score = 1/(C + rank_text) + 1/(C + rank_vec)
    Rrf { constant: f32 },
    /// Linear weighted combination: score = alpha * norm_text + beta * norm_vec
    Linear { alpha: f32, beta: f32 },
}

/// A hybrid search result entry after fusion.
#[derive(Debug, Clone)]
pub struct HybridHit {
    /// The Redis key of the matching document.
    pub key: String,
    /// Fused score (higher = better).
    pub fused_score: f32,
    /// Original BM25 text score (None if not in text results).
    pub text_score: Option<f32>,
    /// Original vector distance (None if not in vector results).
    pub vector_distance: Option<f32>,
}

/// Convert a vector distance to a similarity score (higher = more similar).
fn distance_to_similarity(distance: f32, metric: VectorDistanceMetric) -> f32 {
    match metric {
        VectorDistanceMetric::Cosine => 1.0 - distance,
        VectorDistanceMetric::L2 => 1.0 / (1.0 + distance),
        VectorDistanceMetric::IP => 1.0 - distance,
    }
}

/// Min-max normalize a slice of values to [0, 1].
/// If all values are identical, returns 0.5 for all.
fn min_max_normalize(values: &[f32]) -> Vec<f32> {
    if values.is_empty() {
        return Vec::new();
    }
    let min = values.iter().copied().fold(f32::INFINITY, f32::min);
    let max = values.iter().copied().fold(f32::NEG_INFINITY, f32::max);
    let range = max - min;
    if range == 0.0 {
        vec![0.5; values.len()]
    } else {
        values.iter().map(|v| (v - min) / range).collect()
    }
}

/// Fuse text search hits and vector search hits using the given strategy.
///
/// Returns up to `count` results sorted by fused score descending.
pub fn hybrid_fuse(
    text_hits: &[SearchHit],
    knn_hits: &[KnnHit],
    strategy: &FusionStrategy,
    distance_metric: VectorDistanceMetric,
    count: usize,
) -> Vec<HybridHit> {
    match strategy {
        FusionStrategy::Rrf { constant } => fuse_rrf(text_hits, knn_hits, *constant, count),
        FusionStrategy::Linear { alpha, beta } => {
            fuse_linear(text_hits, knn_hits, *alpha, *beta, distance_metric, count)
        }
    }
}

/// Reciprocal Rank Fusion.
///
/// Text hits are ranked by BM25 score descending (rank 1 = best).
/// Vector hits are ranked by distance ascending (rank 1 = closest).
/// For each unique key: score = sum(1/(C + rank)) for each list containing the key.
fn fuse_rrf(
    text_hits: &[SearchHit],
    knn_hits: &[KnnHit],
    constant: f32,
    count: usize,
) -> Vec<HybridHit> {
    let mut scores: HashMap<&str, (f32, Option<f32>, Option<f32>)> = HashMap::new();

    // Text hits are already sorted by BM25 desc from the search engine
    for (rank, hit) in text_hits.iter().enumerate() {
        let rrf_score = 1.0 / (constant + (rank + 1) as f32);
        let entry = scores.entry(&hit.key).or_insert((0.0, None, None));
        entry.0 += rrf_score;
        entry.1 = Some(hit.score);
    }

    // KNN hits are already sorted by distance asc from usearch
    for (rank, hit) in knn_hits.iter().enumerate() {
        let rrf_score = 1.0 / (constant + (rank + 1) as f32);
        let entry = scores.entry(&hit.key).or_insert((0.0, None, None));
        entry.0 += rrf_score;
        entry.2 = Some(hit.distance);
    }

    let mut results: Vec<HybridHit> = scores
        .into_iter()
        .map(|(key, (fused, text, vec_dist))| HybridHit {
            key: key.to_string(),
            fused_score: fused,
            text_score: text,
            vector_distance: vec_dist,
        })
        .collect();

    results.sort_by(|a, b| {
        b.fused_score
            .partial_cmp(&a.fused_score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    results.truncate(count);
    results
}

/// Linear weighted fusion.
///
/// Normalizes BM25 scores and vector similarities to [0, 1] using min-max,
/// then: score = alpha * norm_text + beta * norm_vec.
/// Keys in only one list get 0 for the missing component.
fn fuse_linear(
    text_hits: &[SearchHit],
    knn_hits: &[KnnHit],
    alpha: f32,
    beta: f32,
    distance_metric: VectorDistanceMetric,
    count: usize,
) -> Vec<HybridHit> {
    // Collect raw scores
    let text_scores: Vec<f32> = text_hits.iter().map(|h| h.score).collect();
    let vec_similarities: Vec<f32> = knn_hits
        .iter()
        .map(|h| distance_to_similarity(h.distance, distance_metric))
        .collect();

    // Normalize
    let text_norm = min_max_normalize(&text_scores);
    let vec_norm = min_max_normalize(&vec_similarities);

    // Build per-key entries
    // (text_norm, vec_norm, raw_text_score, raw_vec_distance)
    type LinearEntry = (f32, f32, Option<f32>, Option<f32>);
    let mut entries: HashMap<&str, LinearEntry> = HashMap::new();

    for (i, hit) in text_hits.iter().enumerate() {
        let entry = entries.entry(&hit.key).or_insert((0.0, 0.0, None, None));
        entry.0 = text_norm[i];
        entry.2 = Some(hit.score);
    }

    for (i, hit) in knn_hits.iter().enumerate() {
        let entry = entries.entry(&hit.key).or_insert((0.0, 0.0, None, None));
        entry.1 = vec_norm[i];
        entry.3 = Some(hit.distance);
    }

    let mut results: Vec<HybridHit> = entries
        .into_iter()
        .map(|(key, (t_norm, v_norm, text_score, vec_dist))| HybridHit {
            key: key.to_string(),
            fused_score: alpha * t_norm + beta * v_norm,
            text_score,
            vector_distance: vec_dist,
        })
        .collect();

    results.sort_by(|a, b| {
        b.fused_score
            .partial_cmp(&a.fused_score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    results.truncate(count);
    results
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_text_hits(keys_scores: &[(&str, f32)]) -> Vec<SearchHit> {
        keys_scores
            .iter()
            .map(|(k, s)| SearchHit {
                key: k.to_string(),
                score: *s,
                fields: Vec::new(),
                sort_value: None,
            })
            .collect()
    }

    fn make_knn_hits(keys_distances: &[(&str, f32)]) -> Vec<KnnHit> {
        keys_distances
            .iter()
            .map(|(k, d)| KnnHit {
                key: k.to_string(),
                distance: *d,
                fields: Vec::new(),
            })
            .collect()
    }

    #[test]
    fn test_rrf_basic() {
        let text = make_text_hits(&[("a", 10.0), ("b", 5.0), ("c", 1.0)]);
        let knn = make_knn_hits(&[("b", 0.1), ("d", 0.2), ("a", 0.5)]);

        let results = hybrid_fuse(
            &text,
            &knn,
            &FusionStrategy::Rrf { constant: 60.0 },
            VectorDistanceMetric::Cosine,
            10,
        );

        // "b" appears in both lists at good ranks, should be top
        assert_eq!(results[0].key, "b");
        assert!(results[0].text_score.is_some());
        assert!(results[0].vector_distance.is_some());
        // All 4 unique keys should be present
        assert_eq!(results.len(), 4);
    }

    #[test]
    fn test_rrf_disjoint() {
        let text = make_text_hits(&[("a", 10.0)]);
        let knn = make_knn_hits(&[("b", 0.1)]);

        let results = hybrid_fuse(
            &text,
            &knn,
            &FusionStrategy::Rrf { constant: 60.0 },
            VectorDistanceMetric::Cosine,
            10,
        );

        assert_eq!(results.len(), 2);
        // Both should have equal RRF scores (rank 1 in their respective lists)
        assert!((results[0].fused_score - results[1].fused_score).abs() < 1e-6);
    }

    #[test]
    fn test_linear_basic() {
        let text = make_text_hits(&[("a", 10.0), ("b", 5.0)]);
        let knn = make_knn_hits(&[("b", 0.1), ("a", 0.9)]);

        let results = hybrid_fuse(
            &text,
            &knn,
            &FusionStrategy::Linear {
                alpha: 0.5,
                beta: 0.5,
            },
            VectorDistanceMetric::Cosine,
            10,
        );

        assert_eq!(results.len(), 2);
        // Both keys should have scores
        for r in &results {
            assert!(r.text_score.is_some());
            assert!(r.vector_distance.is_some());
        }
    }

    #[test]
    fn test_linear_single_scores() {
        // All identical BM25 scores -> normalized to 0.5
        let text = make_text_hits(&[("a", 5.0), ("b", 5.0)]);
        let knn = make_knn_hits(&[("a", 0.1), ("b", 0.1)]);

        let results = hybrid_fuse(
            &text,
            &knn,
            &FusionStrategy::Linear {
                alpha: 0.5,
                beta: 0.5,
            },
            VectorDistanceMetric::Cosine,
            10,
        );

        // All identical -> all fused scores should be equal
        assert!((results[0].fused_score - results[1].fused_score).abs() < 1e-6);
    }

    #[test]
    fn test_count_limits_results() {
        let text = make_text_hits(&[("a", 10.0), ("b", 5.0), ("c", 1.0)]);
        let knn = make_knn_hits(&[("d", 0.1), ("e", 0.2)]);

        let results = hybrid_fuse(
            &text,
            &knn,
            &FusionStrategy::Rrf { constant: 60.0 },
            VectorDistanceMetric::Cosine,
            3,
        );

        assert_eq!(results.len(), 3);
    }

    #[test]
    fn test_empty_text_results() {
        let text: Vec<SearchHit> = Vec::new();
        let knn = make_knn_hits(&[("a", 0.1), ("b", 0.2)]);

        let results = hybrid_fuse(
            &text,
            &knn,
            &FusionStrategy::Rrf { constant: 60.0 },
            VectorDistanceMetric::Cosine,
            10,
        );

        assert_eq!(results.len(), 2);
        for r in &results {
            assert!(r.text_score.is_none());
            assert!(r.vector_distance.is_some());
        }
    }

    #[test]
    fn test_empty_vector_results() {
        let text = make_text_hits(&[("a", 10.0), ("b", 5.0)]);
        let knn: Vec<KnnHit> = Vec::new();

        let results = hybrid_fuse(
            &text,
            &knn,
            &FusionStrategy::Rrf { constant: 60.0 },
            VectorDistanceMetric::Cosine,
            10,
        );

        assert_eq!(results.len(), 2);
        for r in &results {
            assert!(r.text_score.is_some());
            assert!(r.vector_distance.is_none());
        }
    }
}
