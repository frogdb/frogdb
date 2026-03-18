//! Vector Set data type for approximate nearest neighbor (ANN) search.
//!
//! Implements Redis 8.0 vector sets backed by USearch HNSW graphs. Each vector
//! set stores named elements with associated vectors, optional JSON attributes,
//! and supports similarity search with filtering.

use bytes::Bytes;
use rand::Rng;
use rand::SeedableRng;
use rand::rngs::StdRng;
use std::collections::{BTreeMap, HashMap};

/// Distance metric for vector comparison.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VectorDistanceMetric {
    Cosine,
    L2,
    InnerProduct,
}

/// Vector quantization level.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VectorQuantization {
    NoQuant,
    Q8,
    Bin,
}

/// A vector set value backed by a USearch HNSW index.
pub struct VectorSetValue {
    /// The HNSW index for approximate nearest neighbor search.
    index: usearch::Index,
    /// Distance metric used for this vector set.
    metric: VectorDistanceMetric,
    /// Quantization level.
    quantization: VectorQuantization,
    /// Stored dimension (after REDUCE projection, if any).
    dim: usize,
    /// Original dimension before REDUCE (0 if no REDUCE).
    original_dim: usize,
    /// HNSW M parameter (max edges per node).
    m: usize,
    /// HNSW EF construction parameter.
    ef_construction: usize,
    /// Next internal ID to assign.
    next_id: u64,
    /// Map from element name to internal ID.
    name_to_id: HashMap<Bytes, u64>,
    /// Map from internal ID to element name.
    id_to_name: HashMap<u64, Bytes>,
    /// Stored f32 vectors (the vectors actually inserted into the index).
    vectors: HashMap<u64, Vec<f32>>,
    /// JSON attributes per element.
    attributes: HashMap<Bytes, serde_json::Value>,
    /// Lexicographic index for VRANGE.
    lex_index: BTreeMap<Bytes, u64>,
    /// Random projection matrix for REDUCE (empty if unused).
    /// Layout: original_dim rows × dim columns, stored row-major.
    projection_matrix: Vec<f32>,
    /// Unique ID for this vector set (seeds projection matrix).
    uid: u64,
}

impl std::fmt::Debug for VectorSetValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VectorSetValue")
            .field("metric", &self.metric)
            .field("quantization", &self.quantization)
            .field("dim", &self.dim)
            .field("original_dim", &self.original_dim)
            .field("m", &self.m)
            .field("ef_construction", &self.ef_construction)
            .field("count", &self.name_to_id.len())
            .finish()
    }
}

impl Clone for VectorSetValue {
    fn clone(&self) -> Self {
        let mut cloned =
            Self::new_inner(self.metric, self.quantization, self.dim, self.m, self.ef_construction)
                .expect("failed to create index for clone");
        cloned.original_dim = self.original_dim;
        cloned.next_id = self.next_id;
        cloned.name_to_id = self.name_to_id.clone();
        cloned.id_to_name = self.id_to_name.clone();
        cloned.vectors = self.vectors.clone();
        cloned.attributes = self.attributes.clone();
        cloned.lex_index = self.lex_index.clone();
        cloned.projection_matrix = self.projection_matrix.clone();
        cloned.uid = self.uid;

        // Rebuild the HNSW index from stored vectors.
        if !cloned.vectors.is_empty() {
            let count = cloned.vectors.len();
            let _ = cloned.index.reserve(count);
            for (&id, vec) in &cloned.vectors {
                let _ = cloned.index.add(id, vec);
            }
        }

        cloned
    }
}

/// Result from a similarity search.
#[derive(Debug, Clone)]
pub struct VectorSearchResult {
    pub name: Bytes,
    pub score: f64,
}

impl VectorSetValue {
    /// Create a new vector set with the given parameters.
    pub fn new(
        metric: VectorDistanceMetric,
        quantization: VectorQuantization,
        dim: usize,
        m: usize,
        ef_construction: usize,
    ) -> Result<Self, String> {
        let mut vs = Self::new_inner(metric, quantization, dim, m, ef_construction)?;
        vs.uid = rand::random();
        Ok(vs)
    }

    fn new_inner(
        metric: VectorDistanceMetric,
        quantization: VectorQuantization,
        dim: usize,
        m: usize,
        ef_construction: usize,
    ) -> Result<Self, String> {
        let usearch_metric = match metric {
            VectorDistanceMetric::Cosine => usearch::MetricKind::Cos,
            VectorDistanceMetric::L2 => usearch::MetricKind::L2sq,
            VectorDistanceMetric::InnerProduct => usearch::MetricKind::IP,
        };

        let usearch_quant = match quantization {
            VectorQuantization::NoQuant => usearch::ScalarKind::F32,
            VectorQuantization::Q8 => usearch::ScalarKind::I8,
            VectorQuantization::Bin => usearch::ScalarKind::B1,
        };

        let opts = usearch::IndexOptions {
            dimensions: dim,
            metric: usearch_metric,
            quantization: usearch_quant,
            connectivity: m,
            expansion_add: ef_construction,
            expansion_search: ef_construction,
            ..Default::default()
        };

        let index =
            usearch::Index::new(&opts).map_err(|e| format!("Failed to create index: {e}"))?;
        let _ = index.reserve(64);

        Ok(Self {
            index,
            metric,
            quantization,
            dim,
            original_dim: 0,
            m,
            ef_construction,
            next_id: 0,
            name_to_id: HashMap::new(),
            id_to_name: HashMap::new(),
            vectors: HashMap::new(),
            attributes: HashMap::new(),
            lex_index: BTreeMap::new(),
            projection_matrix: Vec::new(),
            uid: 0,
        })
    }

    /// Reconstruct from serialized parts (for deserialization).
    #[allow(clippy::too_many_arguments)]
    pub fn from_parts(
        metric: VectorDistanceMetric,
        quantization: VectorQuantization,
        dim: usize,
        original_dim: usize,
        m: usize,
        ef_construction: usize,
        next_id: u64,
        uid: u64,
        projection_matrix: Vec<f32>,
        elements: Vec<(Bytes, u64, Vec<f32>, Option<serde_json::Value>)>,
    ) -> Result<Self, String> {
        let mut vs = Self::new_inner(metric, quantization, dim, m, ef_construction)?;
        vs.original_dim = original_dim;
        vs.next_id = next_id;
        vs.uid = uid;
        vs.projection_matrix = projection_matrix;

        if !elements.is_empty() {
            let _ = vs.index.reserve(elements.len());
        }

        for (name, id, vector, attr) in elements {
            let _ = vs.index.add(id, &vector);
            vs.name_to_id.insert(name.clone(), id);
            vs.id_to_name.insert(id, name.clone());
            vs.vectors.insert(id, vector);
            vs.lex_index.insert(name.clone(), id);
            if let Some(a) = attr {
                vs.attributes.insert(name, a);
            }
        }

        Ok(vs)
    }

    /// Add or update an element. Returns true if the element is new.
    pub fn add(&mut self, name: Bytes, vector: Vec<f32>) -> Result<bool, String> {
        let is_new;

        if let Some(&existing_id) = self.name_to_id.get(&name) {
            // Update: remove old vector and re-insert.
            let _ = self.index.remove(existing_id);
            self.vectors.remove(&existing_id);
            self.id_to_name.remove(&existing_id);
            self.lex_index.remove(&name);

            // Ensure capacity.
            let size = self.index.size();
            let capacity = self.index.capacity();
            if size >= capacity {
                let _ = self.index.reserve(capacity.max(64) * 2);
            }

            let new_id = self.next_id;
            self.next_id += 1;
            self.index
                .add(new_id, &vector)
                .map_err(|e| format!("Failed to add vector: {e}"))?;
            self.name_to_id.insert(name.clone(), new_id);
            self.id_to_name.insert(new_id, name.clone());
            self.vectors.insert(new_id, vector);
            self.lex_index.insert(name, new_id);
            is_new = false;
        } else {
            // Ensure capacity.
            let size = self.index.size();
            let capacity = self.index.capacity();
            if size >= capacity {
                let _ = self.index.reserve(capacity.max(64) * 2);
            }

            let id = self.next_id;
            self.next_id += 1;
            self.index
                .add(id, &vector)
                .map_err(|e| format!("Failed to add vector: {e}"))?;
            self.name_to_id.insert(name.clone(), id);
            self.id_to_name.insert(id, name.clone());
            self.vectors.insert(id, vector);
            self.lex_index.insert(name, id);
            is_new = true;
        }

        Ok(is_new)
    }

    /// Remove an element. Returns true if it existed.
    pub fn remove(&mut self, name: &[u8]) -> bool {
        if let Some(id) = self.name_to_id.remove(name) {
            let _ = self.index.remove(id);
            self.id_to_name.remove(&id);
            self.vectors.remove(&id);
            self.attributes.remove(name);
            self.lex_index.remove(name);
            true
        } else {
            false
        }
    }

    /// Search for the nearest neighbors of a query vector.
    pub fn search(&self, query: &[f32], count: usize) -> Vec<VectorSearchResult> {
        if self.name_to_id.is_empty() || count == 0 {
            return Vec::new();
        }

        let actual_count = count.min(self.name_to_id.len());

        match self.index.search(query, actual_count) {
            Ok(results) => {
                let mut hits = Vec::with_capacity(results.keys.len());
                for i in 0..results.keys.len() {
                    let id = results.keys[i];
                    let distance = results.distances[i] as f64;
                    if let Some(name) = self.id_to_name.get(&id) {
                        hits.push(VectorSearchResult {
                            name: name.clone(),
                            score: self.distance_to_similarity(distance),
                        });
                    }
                }
                hits
            }
            Err(_) => Vec::new(),
        }
    }

    /// Brute-force linear scan search (for TRUTH mode).
    pub fn brute_force_search(&self, query: &[f32], count: usize) -> Vec<VectorSearchResult> {
        if self.name_to_id.is_empty() || count == 0 {
            return Vec::new();
        }

        let mut all: Vec<(Bytes, f64)> = self
            .vectors
            .iter()
            .filter_map(|(id, vec)| {
                let name = self.id_to_name.get(id)?;
                let distance = compute_distance(query, vec, self.metric);
                Some((name.clone(), self.distance_to_similarity(distance)))
            })
            .collect();

        // Sort by similarity descending (highest first).
        all.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        all.truncate(count);

        all.into_iter()
            .map(|(name, score)| VectorSearchResult { name, score })
            .collect()
    }

    /// Convert a raw distance to a similarity score.
    fn distance_to_similarity(&self, distance: f64) -> f64 {
        match self.metric {
            VectorDistanceMetric::Cosine => 1.0 - distance,
            VectorDistanceMetric::L2 => 1.0 / (1.0 + distance),
            VectorDistanceMetric::InnerProduct => 1.0 - distance,
        }
    }

    /// Get the stored vector for an element.
    pub fn get_vector(&self, name: &[u8]) -> Option<&[f32]> {
        let id = self.name_to_id.get(name)?;
        self.vectors.get(id).map(|v| v.as_slice())
    }

    /// Set a JSON attribute on an element.
    pub fn set_attr(&mut self, name: &[u8], attr: serde_json::Value) -> bool {
        if !self.name_to_id.contains_key(name) {
            return false;
        }
        if attr.is_null() {
            self.attributes.remove(name);
        } else {
            self.attributes.insert(Bytes::copy_from_slice(name), attr);
        }
        true
    }

    /// Get the JSON attribute of an element.
    pub fn get_attr(&self, name: &[u8]) -> Option<&serde_json::Value> {
        self.attributes.get(name)
    }

    /// Number of elements.
    pub fn card(&self) -> usize {
        self.name_to_id.len()
    }

    /// Stored dimension.
    pub fn dim(&self) -> usize {
        self.dim
    }

    /// Original dimension (before REDUCE), or 0 if no REDUCE.
    pub fn original_dim(&self) -> usize {
        self.original_dim
    }

    /// Distance metric.
    pub fn metric(&self) -> VectorDistanceMetric {
        self.metric
    }

    /// Quantization level.
    pub fn quantization(&self) -> VectorQuantization {
        self.quantization
    }

    /// HNSW M parameter.
    pub fn m(&self) -> usize {
        self.m
    }

    /// HNSW EF construction parameter.
    pub fn ef_construction(&self) -> usize {
        self.ef_construction
    }

    /// Unique ID for this vector set.
    pub fn uid(&self) -> u64 {
        self.uid
    }

    /// Next internal ID.
    pub fn next_id(&self) -> u64 {
        self.next_id
    }

    /// Projection matrix (empty if no REDUCE).
    pub fn projection_matrix(&self) -> &[f32] {
        &self.projection_matrix
    }

    /// Check if an element exists.
    pub fn contains(&self, name: &[u8]) -> bool {
        self.name_to_id.contains_key(name)
    }

    /// Get random element(s). If count > 0, return that many unique elements.
    /// If count < 0, return |count| elements allowing duplicates.
    pub fn rand_member(&self, count: i64) -> Vec<Bytes> {
        if self.lex_index.is_empty() {
            return Vec::new();
        }

        let names: Vec<&Bytes> = self.lex_index.keys().collect();
        let mut rng = rand::thread_rng();

        if count == 0 {
            return Vec::new();
        }

        if count > 0 {
            // Unique elements.
            let n = (count as usize).min(names.len());
            let mut indices: Vec<usize> = (0..names.len()).collect();
            // Fisher-Yates partial shuffle.
            for i in 0..n {
                let j = rng.gen_range(i..names.len());
                indices.swap(i, j);
            }
            indices[..n].iter().map(|&i| names[i].clone()).collect()
        } else {
            // Allow duplicates.
            let n = (-count) as usize;
            (0..n)
                .map(|_| {
                    let i = rng.gen_range(0..names.len());
                    names[i].clone()
                })
                .collect()
        }
    }

    /// Lexicographic range query for VRANGE.
    /// Returns elements in lex order starting after `cursor`, up to `count` elements.
    pub fn range(&self, cursor: &[u8], count: usize) -> Vec<Bytes> {
        use std::ops::Bound;

        let iter = if cursor.is_empty() {
            self.lex_index.range::<Bytes, _>(..)
        } else {
            let start = Bytes::copy_from_slice(cursor);
            self.lex_index
                .range::<Bytes, _>((Bound::Excluded(start), Bound::Unbounded))
        };

        iter.take(count).map(|(name, _)| name.clone()).collect()
    }

    /// Get HNSW neighbor links for an element (approximation via search).
    /// USearch doesn't expose per-layer neighbor lists, so we approximate
    /// by searching from the element's own vector with high EF.
    pub fn links(&self, name: &[u8], layer: Option<usize>) -> Option<Vec<Bytes>> {
        let vec = self.get_vector(name)?;
        let vec_owned = vec.to_vec();

        // Return neighbors at the requested layer. Since usearch doesn't expose
        // layer info, we use search with M neighbors as approximation.
        let neighbor_count = if layer.unwrap_or(0) == 0 {
            self.m * 2 // Layer 0 has 2M neighbors
        } else {
            self.m
        };

        let count = neighbor_count.min(self.name_to_id.len().saturating_sub(1));
        if count == 0 {
            return Some(Vec::new());
        }

        // Search for count+1 to account for self-match.
        let results = self.search(&vec_owned, count + 1);
        Some(
            results
                .into_iter()
                .filter(|r| r.name.as_ref() != name)
                .take(count)
                .map(|r| r.name)
                .collect(),
        )
    }

    /// Set up REDUCE projection from original_dim → dim.
    pub fn setup_reduce(&mut self, original_dim: usize) {
        self.original_dim = original_dim;
        self.projection_matrix = generate_projection_matrix(self.uid, original_dim, self.dim);
    }

    /// Project a vector through the REDUCE projection matrix.
    /// Input: original_dim-length vector. Output: dim-length vector.
    pub fn project_vector(&self, input: &[f32]) -> Vec<f32> {
        if self.projection_matrix.is_empty() || self.original_dim == 0 {
            return input.to_vec();
        }

        let mut output = vec![0.0f32; self.dim];
        for (j, out_val) in output.iter_mut().enumerate() {
            let mut sum = 0.0f32;
            for (i, &in_val) in input.iter().enumerate() {
                sum += in_val * self.projection_matrix[i * self.dim + j];
            }
            *out_val = sum;
        }

        // Normalize the projected vector.
        let norm: f32 = output.iter().map(|x| x * x).sum::<f32>().sqrt();
        if norm > 0.0 {
            for v in &mut output {
                *v /= norm;
            }
        }

        output
    }

    /// Iterator over all elements (name, id, vector, attribute).
    pub fn elements(
        &self,
    ) -> impl Iterator<Item = (&Bytes, u64, &[f32], Option<&serde_json::Value>)> {
        self.name_to_id.iter().map(move |(name, &id)| {
            let vec = self.vectors.get(&id).map(|v| v.as_slice()).unwrap_or(&[]);
            let attr = self.attributes.get(name);
            (name, id, vec, attr)
        })
    }

    /// Info response data.
    pub fn info(&self) -> VectorSetInfo {
        VectorSetInfo {
            count: self.name_to_id.len(),
            dim: self.dim,
            original_dim: self.original_dim,
            m: self.m,
            ef_construction: self.ef_construction,
            metric: self.metric,
            quantization: self.quantization,
        }
    }

    /// Calculate approximate memory size.
    pub fn memory_size(&self) -> usize {
        let base = std::mem::size_of::<Self>();
        let vectors_size: usize = self
            .vectors
            .values()
            .map(|v| v.len() * std::mem::size_of::<f32>() + std::mem::size_of::<Vec<f32>>())
            .sum();
        let names_size: usize = self.name_to_id.keys().map(|k| k.len()).sum::<usize>() * 2; // in both maps
        let attr_size: usize = self
            .attributes
            .values()
            .map(|v| v.to_string().len())
            .sum();
        let proj_size = self.projection_matrix.len() * std::mem::size_of::<f32>();
        // Rough estimate for usearch index: ~(M * 2 * 8) bytes per vector
        let index_size = self.name_to_id.len() * self.m * 2 * 8;

        base + vectors_size + names_size + attr_size + proj_size + index_size
    }
}

/// Info about a vector set.
#[derive(Debug)]
pub struct VectorSetInfo {
    pub count: usize,
    pub dim: usize,
    pub original_dim: usize,
    pub m: usize,
    pub ef_construction: usize,
    pub metric: VectorDistanceMetric,
    pub quantization: VectorQuantization,
}

/// Compute distance between two vectors.
fn compute_distance(a: &[f32], b: &[f32], metric: VectorDistanceMetric) -> f64 {
    match metric {
        VectorDistanceMetric::Cosine => {
            let mut dot = 0.0f64;
            let mut norm_a = 0.0f64;
            let mut norm_b = 0.0f64;
            for i in 0..a.len() {
                let ai = a[i] as f64;
                let bi = b[i] as f64;
                dot += ai * bi;
                norm_a += ai * ai;
                norm_b += bi * bi;
            }
            let denom = norm_a.sqrt() * norm_b.sqrt();
            if denom == 0.0 {
                1.0
            } else {
                1.0 - (dot / denom)
            }
        }
        VectorDistanceMetric::L2 => {
            let mut sum = 0.0f64;
            for i in 0..a.len() {
                let d = (a[i] as f64) - (b[i] as f64);
                sum += d * d;
            }
            sum
        }
        VectorDistanceMetric::InnerProduct => {
            let mut dot = 0.0f64;
            for i in 0..a.len() {
                dot += (a[i] as f64) * (b[i] as f64);
            }
            1.0 - dot
        }
    }
}

/// Generate a deterministic random projection matrix seeded by uid.
fn generate_projection_matrix(uid: u64, input_dim: usize, output_dim: usize) -> Vec<f32> {
    let mut rng = StdRng::seed_from_u64(uid);
    let scale = 1.0 / (output_dim as f32).sqrt();
    (0..input_dim * output_dim)
        .map(|_| {
            // Gaussian-like via simple approximation: sum of uniforms.
            let u1: f32 = rng.r#gen();
            let u2: f32 = rng.r#gen();
            (u1 + u2 - 1.0) * scale
        })
        .collect()
}

// --- Filter expression parser for VSIM FILTER ---

/// Comparison operator for filter expressions.
#[derive(Debug, Clone, PartialEq)]
pub enum CompareOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

/// A value in a filter expression.
#[derive(Debug, Clone, PartialEq)]
pub enum FilterValue {
    Number(f64),
    String(String),
    Bool(bool),
    Null,
}

/// A filter expression for VSIM FILTER.
#[derive(Debug, Clone)]
pub enum FilterExpr {
    Compare {
        field: String,
        op: CompareOp,
        value: FilterValue,
    },
    And(Box<FilterExpr>, Box<FilterExpr>),
    Or(Box<FilterExpr>, Box<FilterExpr>),
    Not(Box<FilterExpr>),
}

impl FilterExpr {
    /// Parse a filter expression string.
    pub fn parse(input: &str) -> Result<Self, String> {
        let tokens = tokenize(input)?;
        let mut pos = 0;
        let expr = parse_or(&tokens, &mut pos)?;
        if pos < tokens.len() {
            return Err(format!("Unexpected token at position {pos}"));
        }
        Ok(expr)
    }

    /// Evaluate the filter against a JSON attribute value.
    pub fn evaluate(&self, attrs: Option<&serde_json::Value>) -> bool {
        match self {
            FilterExpr::Compare { field, op, value } => {
                let json_val = attrs
                    .and_then(|a| resolve_field(a, field));
                compare_json_value(json_val, op, value)
            }
            FilterExpr::And(a, b) => a.evaluate(attrs) && b.evaluate(attrs),
            FilterExpr::Or(a, b) => a.evaluate(attrs) || b.evaluate(attrs),
            FilterExpr::Not(e) => !e.evaluate(attrs),
        }
    }
}

/// Resolve a dotted field path in a JSON value.
fn resolve_field<'a>(val: &'a serde_json::Value, field: &str) -> Option<&'a serde_json::Value> {
    let mut current = val;
    for part in field.split('.') {
        current = current.get(part)?;
    }
    Some(current)
}

/// Compare a JSON value against a filter value.
fn compare_json_value(
    json_val: Option<&serde_json::Value>,
    op: &CompareOp,
    filter_val: &FilterValue,
) -> bool {
    match (json_val, filter_val) {
        (None, FilterValue::Null) => matches!(op, CompareOp::Eq),
        (None, _) => matches!(op, CompareOp::Ne),
        (Some(serde_json::Value::Null), FilterValue::Null) => matches!(op, CompareOp::Eq),
        (Some(serde_json::Value::Number(n)), FilterValue::Number(fv)) => {
            let nf = n.as_f64().unwrap_or(0.0);
            match op {
                CompareOp::Eq => (nf - fv).abs() < f64::EPSILON,
                CompareOp::Ne => (nf - fv).abs() >= f64::EPSILON,
                CompareOp::Lt => nf < *fv,
                CompareOp::Le => nf <= *fv,
                CompareOp::Gt => nf > *fv,
                CompareOp::Ge => nf >= *fv,
            }
        }
        (Some(serde_json::Value::String(s)), FilterValue::String(fv)) => match op {
            CompareOp::Eq => s == fv,
            CompareOp::Ne => s != fv,
            CompareOp::Lt => s.as_str() < fv.as_str(),
            CompareOp::Le => s.as_str() <= fv.as_str(),
            CompareOp::Gt => s.as_str() > fv.as_str(),
            CompareOp::Ge => s.as_str() >= fv.as_str(),
        },
        (Some(serde_json::Value::Bool(b)), FilterValue::Bool(fv)) => match op {
            CompareOp::Eq => b == fv,
            CompareOp::Ne => b != fv,
            _ => false,
        },
        _ => matches!(op, CompareOp::Ne),
    }
}

// --- Tokenizer ---

#[derive(Debug, Clone, PartialEq)]
enum Token {
    Field(String),
    Op(CompareOp),
    Value(FilterValue),
    And,
    Or,
    Not,
    LParen,
    RParen,
}

fn tokenize(input: &str) -> Result<Vec<Token>, String> {
    let mut tokens = Vec::new();
    let chars: Vec<char> = input.chars().collect();
    let mut i = 0;

    while i < chars.len() {
        match chars[i] {
            ' ' | '\t' | '\r' | '\n' => i += 1,
            '(' => {
                tokens.push(Token::LParen);
                i += 1;
            }
            ')' => {
                tokens.push(Token::RParen);
                i += 1;
            }
            '!' if i + 1 < chars.len() && chars[i + 1] == '=' => {
                tokens.push(Token::Op(CompareOp::Ne));
                i += 2;
            }
            '!' => {
                tokens.push(Token::Not);
                i += 1;
            }
            '<' if i + 1 < chars.len() && chars[i + 1] == '=' => {
                tokens.push(Token::Op(CompareOp::Le));
                i += 2;
            }
            '<' => {
                tokens.push(Token::Op(CompareOp::Lt));
                i += 1;
            }
            '>' if i + 1 < chars.len() && chars[i + 1] == '=' => {
                tokens.push(Token::Op(CompareOp::Ge));
                i += 2;
            }
            '>' => {
                tokens.push(Token::Op(CompareOp::Gt));
                i += 1;
            }
            '=' if i + 1 < chars.len() && chars[i + 1] == '=' => {
                tokens.push(Token::Op(CompareOp::Eq));
                i += 2;
            }
            '&' if i + 1 < chars.len() && chars[i + 1] == '&' => {
                tokens.push(Token::And);
                i += 2;
            }
            '|' if i + 1 < chars.len() && chars[i + 1] == '|' => {
                tokens.push(Token::Or);
                i += 2;
            }
            '"' | '\'' => {
                let quote = chars[i];
                i += 1;
                let mut s = String::new();
                while i < chars.len() && chars[i] != quote {
                    if chars[i] == '\\' && i + 1 < chars.len() {
                        i += 1;
                    }
                    s.push(chars[i]);
                    i += 1;
                }
                if i >= chars.len() {
                    return Err("Unterminated string".to_string());
                }
                i += 1; // skip closing quote
                tokens.push(Token::Value(FilterValue::String(s)));
            }
            '.' if i + 1 < chars.len() && chars[i + 1].is_ascii_alphabetic() => {
                // Field reference starting with .
                i += 1; // skip the dot
                let start = i;
                while i < chars.len()
                    && (chars[i].is_ascii_alphanumeric() || chars[i] == '_' || chars[i] == '.')
                {
                    i += 1;
                }
                tokens.push(Token::Field(input[start..i].to_string()));
            }
            c if c.is_ascii_alphabetic() || c == '_' => {
                let start = i;
                while i < chars.len()
                    && (chars[i].is_ascii_alphanumeric() || chars[i] == '_' || chars[i] == '.')
                {
                    i += 1;
                }
                let word = &input[start..i];
                match word {
                    "true" => tokens.push(Token::Value(FilterValue::Bool(true))),
                    "false" => tokens.push(Token::Value(FilterValue::Bool(false))),
                    "null" => tokens.push(Token::Value(FilterValue::Null)),
                    "and" | "AND" => tokens.push(Token::And),
                    "or" | "OR" => tokens.push(Token::Or),
                    "not" | "NOT" => tokens.push(Token::Not),
                    _ => tokens.push(Token::Field(word.to_string())),
                }
            }
            c if c.is_ascii_digit() || c == '-' => {
                let start = i;
                if c == '-' {
                    i += 1;
                }
                while i < chars.len() && (chars[i].is_ascii_digit() || chars[i] == '.') {
                    i += 1;
                }
                let num_str = &input[start..i];
                let num: f64 = num_str
                    .parse()
                    .map_err(|_| format!("Invalid number: {num_str}"))?;
                tokens.push(Token::Value(FilterValue::Number(num)));
            }
            c => return Err(format!("Unexpected character: {c}")),
        }
    }

    Ok(tokens)
}

// --- Recursive descent parser ---

fn parse_or(tokens: &[Token], pos: &mut usize) -> Result<FilterExpr, String> {
    let mut left = parse_and(tokens, pos)?;
    while *pos < tokens.len() && tokens[*pos] == Token::Or {
        *pos += 1;
        let right = parse_and(tokens, pos)?;
        left = FilterExpr::Or(Box::new(left), Box::new(right));
    }
    Ok(left)
}

fn parse_and(tokens: &[Token], pos: &mut usize) -> Result<FilterExpr, String> {
    let mut left = parse_not(tokens, pos)?;
    while *pos < tokens.len() && tokens[*pos] == Token::And {
        *pos += 1;
        let right = parse_not(tokens, pos)?;
        left = FilterExpr::And(Box::new(left), Box::new(right));
    }
    Ok(left)
}

fn parse_not(tokens: &[Token], pos: &mut usize) -> Result<FilterExpr, String> {
    if *pos < tokens.len() && tokens[*pos] == Token::Not {
        *pos += 1;
        let expr = parse_primary(tokens, pos)?;
        Ok(FilterExpr::Not(Box::new(expr)))
    } else {
        parse_primary(tokens, pos)
    }
}

fn parse_primary(tokens: &[Token], pos: &mut usize) -> Result<FilterExpr, String> {
    if *pos >= tokens.len() {
        return Err("Unexpected end of expression".to_string());
    }

    if tokens[*pos] == Token::LParen {
        *pos += 1;
        let expr = parse_or(tokens, pos)?;
        if *pos >= tokens.len() || tokens[*pos] != Token::RParen {
            return Err("Expected closing parenthesis".to_string());
        }
        *pos += 1;
        return Ok(expr);
    }

    // Expect: field op value
    let field = match &tokens[*pos] {
        Token::Field(f) => f.clone(),
        other => return Err(format!("Expected field name, got {other:?}")),
    };
    *pos += 1;

    if *pos >= tokens.len() {
        return Err("Expected operator".to_string());
    }
    let op = match &tokens[*pos] {
        Token::Op(o) => o.clone(),
        other => return Err(format!("Expected operator, got {other:?}")),
    };
    *pos += 1;

    if *pos >= tokens.len() {
        return Err("Expected value".to_string());
    }
    let value = match &tokens[*pos] {
        Token::Value(v) => v.clone(),
        other => return Err(format!("Expected value, got {other:?}")),
    };
    *pos += 1;

    Ok(FilterExpr::Compare { field, op, value })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vectorset_add_and_card() {
        let mut vs = VectorSetValue::new(
            VectorDistanceMetric::Cosine,
            VectorQuantization::NoQuant,
            3,
            16,
            200,
        )
        .unwrap();

        assert_eq!(vs.card(), 0);

        let added = vs.add(Bytes::from("a"), vec![1.0, 0.0, 0.0]).unwrap();
        assert!(added);
        assert_eq!(vs.card(), 1);

        let added = vs.add(Bytes::from("b"), vec![0.0, 1.0, 0.0]).unwrap();
        assert!(added);
        assert_eq!(vs.card(), 2);

        // Update existing element.
        let added = vs.add(Bytes::from("a"), vec![0.0, 0.0, 1.0]).unwrap();
        assert!(!added);
        assert_eq!(vs.card(), 2);
    }

    #[test]
    fn test_vectorset_remove() {
        let mut vs = VectorSetValue::new(
            VectorDistanceMetric::Cosine,
            VectorQuantization::NoQuant,
            3,
            16,
            200,
        )
        .unwrap();

        vs.add(Bytes::from("a"), vec![1.0, 0.0, 0.0]).unwrap();
        vs.add(Bytes::from("b"), vec![0.0, 1.0, 0.0]).unwrap();

        assert!(vs.remove(b"a"));
        assert_eq!(vs.card(), 1);
        assert!(!vs.remove(b"a"));
        assert!(!vs.contains(b"a"));
        assert!(vs.contains(b"b"));
    }

    #[test]
    fn test_vectorset_search() {
        let mut vs = VectorSetValue::new(
            VectorDistanceMetric::Cosine,
            VectorQuantization::NoQuant,
            3,
            16,
            200,
        )
        .unwrap();

        vs.add(Bytes::from("x"), vec![1.0, 0.0, 0.0]).unwrap();
        vs.add(Bytes::from("y"), vec![0.0, 1.0, 0.0]).unwrap();
        vs.add(Bytes::from("z"), vec![0.99, 0.1, 0.0]).unwrap();

        let results = vs.search(&[1.0, 0.0, 0.0], 2);
        assert_eq!(results.len(), 2);
        // "x" should be the closest match (exact match).
        assert_eq!(results[0].name, Bytes::from("x"));
    }

    #[test]
    fn test_vectorset_brute_force_search() {
        let mut vs = VectorSetValue::new(
            VectorDistanceMetric::Cosine,
            VectorQuantization::NoQuant,
            3,
            16,
            200,
        )
        .unwrap();

        vs.add(Bytes::from("x"), vec![1.0, 0.0, 0.0]).unwrap();
        vs.add(Bytes::from("y"), vec![0.0, 1.0, 0.0]).unwrap();
        vs.add(Bytes::from("z"), vec![0.99, 0.1, 0.0]).unwrap();

        let results = vs.brute_force_search(&[1.0, 0.0, 0.0], 2);
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].name, Bytes::from("x"));
    }

    #[test]
    fn test_vectorset_get_vector() {
        let mut vs = VectorSetValue::new(
            VectorDistanceMetric::Cosine,
            VectorQuantization::NoQuant,
            3,
            16,
            200,
        )
        .unwrap();

        vs.add(Bytes::from("a"), vec![1.0, 2.0, 3.0]).unwrap();

        let vec = vs.get_vector(b"a").unwrap();
        assert_eq!(vec, &[1.0, 2.0, 3.0]);
        assert!(vs.get_vector(b"nonexistent").is_none());
    }

    #[test]
    fn test_vectorset_attributes() {
        let mut vs = VectorSetValue::new(
            VectorDistanceMetric::Cosine,
            VectorQuantization::NoQuant,
            3,
            16,
            200,
        )
        .unwrap();

        vs.add(Bytes::from("a"), vec![1.0, 0.0, 0.0]).unwrap();

        let attr = serde_json::json!({"color": "red", "size": 42});
        assert!(vs.set_attr(b"a", attr.clone()));
        assert_eq!(vs.get_attr(b"a"), Some(&attr));

        // Set to null removes attribute.
        assert!(vs.set_attr(b"a", serde_json::Value::Null));
        assert!(vs.get_attr(b"a").is_none());

        // Setting attr on non-existent element returns false.
        assert!(!vs.set_attr(b"nonexistent", serde_json::json!({})));
    }

    #[test]
    fn test_vectorset_range() {
        let mut vs = VectorSetValue::new(
            VectorDistanceMetric::Cosine,
            VectorQuantization::NoQuant,
            3,
            16,
            200,
        )
        .unwrap();

        vs.add(Bytes::from("alpha"), vec![1.0, 0.0, 0.0]).unwrap();
        vs.add(Bytes::from("beta"), vec![0.0, 1.0, 0.0]).unwrap();
        vs.add(Bytes::from("gamma"), vec![0.0, 0.0, 1.0]).unwrap();

        // From start, get 2.
        let result = vs.range(b"", 2);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], Bytes::from("alpha"));
        assert_eq!(result[1], Bytes::from("beta"));

        // After "beta", get 10.
        let result = vs.range(b"beta", 10);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], Bytes::from("gamma"));
    }

    #[test]
    fn test_vectorset_rand_member() {
        let mut vs = VectorSetValue::new(
            VectorDistanceMetric::Cosine,
            VectorQuantization::NoQuant,
            3,
            16,
            200,
        )
        .unwrap();

        vs.add(Bytes::from("a"), vec![1.0, 0.0, 0.0]).unwrap();
        vs.add(Bytes::from("b"), vec![0.0, 1.0, 0.0]).unwrap();
        vs.add(Bytes::from("c"), vec![0.0, 0.0, 1.0]).unwrap();

        // Positive count: unique.
        let result = vs.rand_member(2);
        assert_eq!(result.len(), 2);
        // All results should be from the set.
        for r in &result {
            assert!(vs.contains(r));
        }

        // Negative count: may have duplicates, return exactly |count|.
        let result = vs.rand_member(-5);
        assert_eq!(result.len(), 5);

        // Zero returns empty.
        assert!(vs.rand_member(0).is_empty());
    }

    #[test]
    fn test_vectorset_clone() {
        let mut vs = VectorSetValue::new(
            VectorDistanceMetric::Cosine,
            VectorQuantization::NoQuant,
            3,
            16,
            200,
        )
        .unwrap();

        vs.add(Bytes::from("a"), vec![1.0, 0.0, 0.0]).unwrap();
        vs.add(Bytes::from("b"), vec![0.0, 1.0, 0.0]).unwrap();

        let cloned = vs.clone();
        assert_eq!(cloned.card(), 2);
        assert!(cloned.contains(b"a"));
        assert!(cloned.contains(b"b"));

        // Search should still work on clone.
        let results = cloned.search(&[1.0, 0.0, 0.0], 1);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].name, Bytes::from("a"));
    }

    #[test]
    fn test_vectorset_reduce() {
        let mut vs = VectorSetValue::new(
            VectorDistanceMetric::Cosine,
            VectorQuantization::NoQuant,
            4, // reduced dim
            16,
            200,
        )
        .unwrap();

        vs.setup_reduce(8); // original dim = 8

        let input = vec![1.0, 0.0, 0.5, 0.0, 0.0, 0.5, 0.0, 1.0];
        let projected = vs.project_vector(&input);
        assert_eq!(projected.len(), 4);

        // Projected vector should be normalized.
        let norm: f32 = projected.iter().map(|x| x * x).sum::<f32>().sqrt();
        assert!((norm - 1.0).abs() < 0.01);
    }

    #[test]
    fn test_filter_parser_simple() {
        let expr = FilterExpr::parse(".color == \"red\"").unwrap();
        let attrs = serde_json::json!({"color": "red"});
        assert!(expr.evaluate(Some(&attrs)));

        let attrs2 = serde_json::json!({"color": "blue"});
        assert!(!expr.evaluate(Some(&attrs2)));
    }

    #[test]
    fn test_filter_parser_numeric() {
        let expr = FilterExpr::parse(".age > 18").unwrap();
        let attrs = serde_json::json!({"age": 25});
        assert!(expr.evaluate(Some(&attrs)));

        let attrs2 = serde_json::json!({"age": 15});
        assert!(!expr.evaluate(Some(&attrs2)));
    }

    #[test]
    fn test_filter_parser_and_or() {
        let expr = FilterExpr::parse(".a == 1 && .b == 2").unwrap();
        assert!(expr.evaluate(Some(&serde_json::json!({"a": 1, "b": 2}))));
        assert!(!expr.evaluate(Some(&serde_json::json!({"a": 1, "b": 3}))));

        let expr = FilterExpr::parse(".a == 1 || .b == 2").unwrap();
        assert!(expr.evaluate(Some(&serde_json::json!({"a": 1, "b": 3}))));
        assert!(expr.evaluate(Some(&serde_json::json!({"a": 0, "b": 2}))));
        assert!(!expr.evaluate(Some(&serde_json::json!({"a": 0, "b": 3}))));
    }

    #[test]
    fn test_filter_parser_not() {
        let expr = FilterExpr::parse("!(.color == \"red\")").unwrap();
        let attrs = serde_json::json!({"color": "blue"});
        assert!(expr.evaluate(Some(&attrs)));
    }

    #[test]
    fn test_vectorset_memory_size() {
        let vs = VectorSetValue::new(
            VectorDistanceMetric::Cosine,
            VectorQuantization::NoQuant,
            3,
            16,
            200,
        )
        .unwrap();

        assert!(vs.memory_size() > 0);
    }

    #[test]
    fn test_vectorset_info() {
        let vs = VectorSetValue::new(
            VectorDistanceMetric::Cosine,
            VectorQuantization::NoQuant,
            128,
            16,
            200,
        )
        .unwrap();

        let info = vs.info();
        assert_eq!(info.dim, 128);
        assert_eq!(info.m, 16);
        assert_eq!(info.ef_construction, 200);
        assert_eq!(info.metric, VectorDistanceMetric::Cosine);
    }

    #[test]
    fn test_vectorset_l2_search() {
        let mut vs = VectorSetValue::new(
            VectorDistanceMetric::L2,
            VectorQuantization::NoQuant,
            3,
            16,
            200,
        )
        .unwrap();

        vs.add(Bytes::from("near"), vec![1.0, 0.0, 0.0]).unwrap();
        vs.add(Bytes::from("far"), vec![10.0, 10.0, 10.0]).unwrap();

        let results = vs.search(&[1.0, 0.1, 0.0], 2);
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].name, Bytes::from("near"));
    }
}
