//! RediSearch query parser using nom combinators.
//!
//! Parses RediSearch query syntax into tantivy Query objects.
//!
//! Grammar:
//! ```text
//! query      = or_expr
//! or_expr    = and_expr ("|" and_expr)*
//! and_expr   = unary_expr (space unary_expr)*
//! unary_expr = "-" atom | atom
//! atom       = "(" or_expr ")" | "@" field ":" field_query | phrase | fuzzy | term
//! field_query = "{" tag ("|" tag)* "}" | "[" num num "]" | phrase | term ["*"]
//! phrase     = '"' ... '"'
//! fuzzy      = "%" term "%" | "%%" term "%%" | "%%%" term "%%%"
//! ```

use std::collections::HashMap;

use nom::{
    IResult,
    bytes::complete::{tag, take_while1},
    character::complete::{char, multispace0, multispace1},
    combinator::opt,
    multi::separated_list1,
    sequence::delimited,
};
use tantivy::query::{
    AllQuery, BooleanQuery, BoostQuery, EmptyQuery, FuzzyTermQuery, Occur, PhraseQuery, Query,
    RangeQuery, TermQuery, TermSetQuery,
};
use tantivy::schema::{Field, Schema};

use crate::error::SearchError;
use crate::schema::{FieldType, SearchIndexDef};

/// A geo radius filter extracted from the query AST for post-filtering.
#[derive(Debug, Clone)]
pub struct GeoFilter {
    pub field: String,
    pub lon: f64,
    pub lat: f64,
    pub radius_m: f64,
}

/// Intermediate AST node.
#[derive(Debug, Clone)]
enum QueryNode {
    /// A simple term (searched across all text fields).
    Term(String),
    /// A field-specific term: @field:term
    FieldTerm { field: String, term: String },
    /// A tag query: @field:{tag1|tag2}
    TagQuery { field: String, tags: Vec<String> },
    /// A numeric range: @field:[min max]
    NumericRange {
        field: String,
        min: f64,
        max: f64,
        min_exclusive: bool,
        max_exclusive: bool,
    },
    /// AND of multiple nodes (implicit).
    And(Vec<QueryNode>),
    /// OR of multiple nodes.
    Or(Vec<QueryNode>),
    /// Negation.
    Not(Box<QueryNode>),
    /// Match all documents.
    MatchAll,
    /// A phrase query: "exact phrase"
    Phrase {
        field: Option<String>,
        terms: Vec<String>,
    },
    /// A prefix query: hel*
    Prefix {
        field: Option<String>,
        prefix: String,
    },
    /// A fuzzy query: %term%, %%term%%, %%%term%%%
    Fuzzy {
        field: Option<String>,
        term: String,
        distance: u8,
    },
    /// A geo radius query: @field:[lon lat radius unit]
    GeoRadius {
        field: String,
        lon: f64,
        lat: f64,
        radius_m: f64,
    },
}

/// Query parser that converts RediSearch syntax to tantivy queries.
pub struct QueryParser<'a> {
    schema: &'a Schema,
    field_map: &'a HashMap<String, Field>,
    def: &'a SearchIndexDef,
    key_field: Field,
    infields: Option<Vec<String>>,
    /// Reverse synonym lookup: lowercased term -> list of all synonyms in its group.
    synonym_lookup: HashMap<String, Vec<String>>,
    /// Slop for phrase queries (max number of intervening terms allowed).
    slop: Option<u32>,
    /// When true, disable stemming (use "simple" tokenizer instead of default).
    verbatim: bool,
}

impl<'a> QueryParser<'a> {
    pub fn new(
        schema: &'a Schema,
        field_map: &'a HashMap<String, Field>,
        def: &'a SearchIndexDef,
        key_field: Field,
    ) -> Self {
        // Build reverse synonym lookup: for each term in any group,
        // map it to the full list of terms in that group.
        let mut synonym_lookup: HashMap<String, Vec<String>> = HashMap::new();
        for terms in def.synonym_groups.values() {
            let lowered: Vec<String> = terms.iter().map(|t| t.to_lowercase()).collect();
            for term in &lowered {
                synonym_lookup.insert(term.clone(), lowered.clone());
            }
        }
        Self {
            schema,
            field_map,
            def,
            key_field,
            infields: None,
            synonym_lookup,
            slop: None,
            verbatim: false,
        }
    }

    /// Set INFIELDS to restrict default text field expansion.
    pub fn with_infields(mut self, infields: Vec<String>) -> Self {
        self.infields = Some(infields);
        self
    }

    /// Set slop for phrase queries.
    pub fn with_slop(mut self, slop: Option<u32>) -> Self {
        self.slop = slop;
        self
    }

    /// Set verbatim mode (disable stemming).
    pub fn with_verbatim(mut self, v: bool) -> Self {
        self.verbatim = v;
        self
    }

    /// Return the key field handle (for composing INKEYS filters externally).
    pub fn key_field(&self) -> Field {
        self.key_field
    }

    /// Parse a query string into a tantivy Query.
    pub fn parse(&self, input: &str) -> Result<Box<dyn Query>, SearchError> {
        let (query, _) = self.parse_with_geo_filters(input)?;
        Ok(query)
    }

    /// Parse a query string, returning the tantivy Query and any geo radius filters.
    pub fn parse_with_geo_filters(
        &self,
        input: &str,
    ) -> Result<(Box<dyn Query>, Vec<GeoFilter>), SearchError> {
        let input = input.trim();
        if input.is_empty() || input == "*" {
            return Ok((Box::new(AllQuery), vec![]));
        }

        let (remaining, ast) =
            parse_query(input).map_err(|e| SearchError::QueryParseError(format!("{}", e)))?;

        if !remaining.trim().is_empty() {
            return Err(SearchError::QueryParseError(format!(
                "Unexpected trailing input: '{}'",
                remaining.trim()
            )));
        }

        let mut geo_filters = Vec::new();
        Self::extract_geo_filters(&ast, &mut geo_filters);
        let query = self.ast_to_query(&ast)?;
        Ok((query, geo_filters))
    }

    /// Recursively extract GeoRadius filters from the AST.
    fn extract_geo_filters(node: &QueryNode, out: &mut Vec<GeoFilter>) {
        match node {
            QueryNode::GeoRadius {
                field,
                lon,
                lat,
                radius_m,
            } => {
                out.push(GeoFilter {
                    field: field.clone(),
                    lon: *lon,
                    lat: *lat,
                    radius_m: *radius_m,
                });
            }
            QueryNode::And(nodes) | QueryNode::Or(nodes) => {
                for n in nodes {
                    Self::extract_geo_filters(n, out);
                }
            }
            QueryNode::Not(inner) => Self::extract_geo_filters(inner, out),
            _ => {}
        }
    }

    fn ast_to_query(&self, node: &QueryNode) -> Result<Box<dyn Query>, SearchError> {
        match node {
            QueryNode::MatchAll => Ok(Box::new(AllQuery)),
            QueryNode::Term(term) => {
                // Check for synonym expansion
                if let Some(synonyms) = self.synonym_lookup.get(&term.to_lowercase())
                    && synonyms.len() > 1
                {
                    let mut sub = Vec::new();
                    for syn in synonyms {
                        sub.push((Occur::Should, self.build_default_term_query(syn)?));
                    }
                    return Ok(Box::new(BooleanQuery::new(sub)));
                }
                self.build_default_term_query(term)
            }
            QueryNode::FieldTerm { field, term } => {
                let tantivy_field = self.resolve_field(field)?;
                // Check for synonym expansion
                if let Some(synonyms) = self.synonym_lookup.get(&term.to_lowercase())
                    && synonyms.len() > 1
                {
                    let mut sub = Vec::new();
                    for syn in synonyms {
                        sub.push((Occur::Should, self.make_text_query(tantivy_field, syn)));
                    }
                    return Ok(Box::new(BooleanQuery::new(sub)));
                }
                Ok(self.make_text_query(tantivy_field, term))
            }
            QueryNode::TagQuery { field, tags } => {
                let tantivy_field = self.resolve_field(field)?;
                // Check if this TAG field is case-sensitive
                let casesensitive = self
                    .def
                    .fields
                    .iter()
                    .find(|f| f.name == *field)
                    .is_some_and(|f| f.casesensitive);
                let normalize = |t: &str| -> String {
                    if casesensitive {
                        t.to_string()
                    } else {
                        t.to_lowercase()
                    }
                };
                if tags.len() == 1 {
                    let term =
                        tantivy::Term::from_field_text(tantivy_field, &normalize(&tags[0]));
                    return Ok(Box::new(TermQuery::new(term, Default::default())));
                }
                let terms: Vec<tantivy::Term> = tags
                    .iter()
                    .map(|t| tantivy::Term::from_field_text(tantivy_field, &normalize(t)))
                    .collect();
                Ok(Box::new(TermSetQuery::new(terms)))
            }
            QueryNode::NumericRange {
                field,
                min,
                max,
                min_exclusive,
                max_exclusive,
            } => {
                let _tantivy_field = self.resolve_field(field)?;
                let min_bound = if *min_exclusive {
                    std::ops::Bound::Excluded(*min)
                } else {
                    std::ops::Bound::Included(*min)
                };
                let max_bound = if *max_exclusive {
                    std::ops::Bound::Excluded(*max)
                } else {
                    std::ops::Bound::Included(*max)
                };
                let range_query = RangeQuery::new_f64_bounds(field.clone(), min_bound, max_bound);
                Ok(Box::new(range_query))
            }
            QueryNode::And(nodes) => {
                if nodes.len() == 1 {
                    return self.ast_to_query(&nodes[0]);
                }
                let mut subqueries: Vec<(Occur, Box<dyn Query>)> = Vec::new();
                for n in nodes {
                    let q = self.ast_to_query(n)?;
                    subqueries.push((Occur::Must, q));
                }
                Ok(Box::new(BooleanQuery::new(subqueries)))
            }
            QueryNode::Or(nodes) => {
                if nodes.len() == 1 {
                    return self.ast_to_query(&nodes[0]);
                }
                let mut subqueries: Vec<(Occur, Box<dyn Query>)> = Vec::new();
                for n in nodes {
                    let q = self.ast_to_query(n)?;
                    subqueries.push((Occur::Should, q));
                }
                Ok(Box::new(BooleanQuery::new(subqueries)))
            }
            QueryNode::Not(inner) => {
                let q = self.ast_to_query(inner)?;
                Ok(Box::new(BooleanQuery::new(vec![
                    (Occur::Must, Box::new(AllQuery) as Box<dyn Query>),
                    (Occur::MustNot, q),
                ])))
            }
            QueryNode::Phrase { field, terms } => {
                if terms.is_empty() {
                    return Ok(Box::new(EmptyQuery));
                }
                match field {
                    Some(f) => {
                        let tantivy_field = self.resolve_field(f)?;
                        Ok(self.make_phrase_query(tantivy_field, terms))
                    }
                    None => {
                        let text_fields = self.default_text_fields();
                        if text_fields.is_empty() {
                            return Ok(Box::new(EmptyQuery));
                        }
                        if text_fields.len() == 1 {
                            let (field, weight) = text_fields[0];
                            let q = self.make_phrase_query(field, terms);
                            return Ok(self.maybe_boost(q, weight));
                        }
                        let subqueries: Vec<(Occur, Box<dyn Query>)> = text_fields
                            .into_iter()
                            .map(|(field, weight)| {
                                let q = self.make_phrase_query(field, terms);
                                (Occur::Should, self.maybe_boost(q, weight))
                            })
                            .collect();
                        Ok(Box::new(BooleanQuery::new(subqueries)))
                    }
                }
            }
            QueryNode::Prefix { field, prefix } => match field {
                Some(f) => {
                    let tantivy_field = self.resolve_field(f)?;
                    Ok(self.make_prefix_query(tantivy_field, prefix))
                }
                None => {
                    let text_fields = self.default_text_fields();
                    if text_fields.is_empty() {
                        return Ok(Box::new(EmptyQuery));
                    }
                    if text_fields.len() == 1 {
                        let (field, weight) = text_fields[0];
                        let q = self.make_prefix_query(field, prefix);
                        return Ok(self.maybe_boost(q, weight));
                    }
                    let subqueries: Vec<(Occur, Box<dyn Query>)> = text_fields
                        .into_iter()
                        .map(|(field, weight)| {
                            let q = self.make_prefix_query(field, prefix);
                            (Occur::Should, self.maybe_boost(q, weight))
                        })
                        .collect();
                    Ok(Box::new(BooleanQuery::new(subqueries)))
                }
            },
            QueryNode::Fuzzy {
                field,
                term,
                distance,
            } => match field {
                Some(f) => {
                    let tantivy_field = self.resolve_field(f)?;
                    Ok(self.make_fuzzy_query(tantivy_field, term, *distance))
                }
                None => {
                    let text_fields = self.default_text_fields();
                    if text_fields.is_empty() {
                        return Ok(Box::new(EmptyQuery));
                    }
                    if text_fields.len() == 1 {
                        let (field, weight) = text_fields[0];
                        let q = self.make_fuzzy_query(field, term, *distance);
                        return Ok(self.maybe_boost(q, weight));
                    }
                    let subqueries: Vec<(Occur, Box<dyn Query>)> = text_fields
                        .into_iter()
                        .map(|(field, weight)| {
                            let q = self.make_fuzzy_query(field, term, *distance);
                            (Occur::Should, self.maybe_boost(q, weight))
                        })
                        .collect();
                    Ok(Box::new(BooleanQuery::new(subqueries)))
                }
            },
            QueryNode::GeoRadius { .. } => {
                // Geo radius filtering is handled as post-filtering in search_inner.
                // Return AllQuery here; the filter is applied after retrieval.
                Ok(Box::new(AllQuery))
            }
        }
    }

    /// Returns (Field, weight) pairs for default text fields, respecting INFIELDS.
    fn default_text_fields(&self) -> Vec<(Field, f64)> {
        self.def
            .fields
            .iter()
            .filter(|f| matches!(f.field_type, FieldType::Text { .. }) && !f.noindex)
            .filter(|f| {
                self.infields
                    .as_ref()
                    .is_none_or(|inf| inf.contains(&f.name))
            })
            .filter_map(|f| {
                let weight = match &f.field_type {
                    FieldType::Text { weight } => *weight,
                    _ => 1.0,
                };
                self.field_map.get(&f.name).map(|&field| (field, weight))
            })
            .collect()
    }

    /// Build a query for a bare term across all default text fields.
    fn build_default_term_query(&self, term: &str) -> Result<Box<dyn Query>, SearchError> {
        let text_fields = self.default_text_fields();
        if text_fields.is_empty() {
            return Ok(Box::new(EmptyQuery));
        }
        if text_fields.len() == 1 {
            let (field, weight) = text_fields[0];
            let q = self.make_text_query(field, term);
            return Ok(self.maybe_boost(q, weight));
        }
        let subqueries: Vec<(Occur, Box<dyn Query>)> = text_fields
            .into_iter()
            .map(|(field, weight)| {
                let q = self.make_text_query(field, term);
                (Occur::Should, self.maybe_boost(q, weight))
            })
            .collect();
        Ok(Box::new(BooleanQuery::new(subqueries)))
    }

    fn resolve_field(&self, name: &str) -> Result<Field, SearchError> {
        self.field_map
            .get(name)
            .copied()
            .ok_or_else(|| SearchError::QueryParseError(format!("Unknown field: {}", name)))
    }

    fn maybe_boost(&self, query: Box<dyn Query>, weight: f64) -> Box<dyn Query> {
        if (weight - 1.0).abs() < f64::EPSILON {
            query
        } else {
            Box::new(BoostQuery::new(query, weight as f32))
        }
    }

    fn make_text_query(&self, field: Field, term: &str) -> Box<dyn Query> {
        let field_entry = self.schema.get_field_entry(field);
        if field_entry.field_type().is_indexed() {
            // Check if STRING (raw tokenizer) or TEXT (tokenized)
            if let tantivy::schema::FieldType::Str(text_options) = field_entry.field_type()
                && let Some(indexing_options) = text_options.get_indexing_options()
                && indexing_options.tokenizer() == "raw"
            {
                // STRING field — exact match
                let t = tantivy::Term::from_field_text(field, term);
                return Box::new(TermQuery::new(t, Default::default()));
            }
            // TEXT field — use tantivy query parser for proper tokenization
            let index = tantivy::Index::create_in_ram(self.schema.clone());
            crate::index::register_custom_tokenizers(&index);
            // When verbatim, use "simple" tokenizer (lowercase only, no stemming)
            if self.verbatim {
                let lowered = term.to_lowercase();
                let t = tantivy::Term::from_field_text(field, &lowered);
                return Box::new(TermQuery::new(t, Default::default()));
            }
            let mut parser = tantivy::query::QueryParser::for_index(&index, vec![field]);
            parser.set_conjunction_by_default();
            match parser.parse_query(term) {
                Ok(q) => q,
                Err(_) => {
                    let t = tantivy::Term::from_field_text(field, term);
                    Box::new(TermQuery::new(t, Default::default()))
                }
            }
        } else {
            Box::new(EmptyQuery)
        }
    }

    fn make_phrase_query(&self, field: Field, terms: &[String]) -> Box<dyn Query> {
        // Lowercase terms to match tantivy's default tokenizer behavior
        let tantivy_terms: Vec<tantivy::Term> = terms
            .iter()
            .map(|t| tantivy::Term::from_field_text(field, &t.to_lowercase()))
            .collect();
        if tantivy_terms.len() == 1 {
            return Box::new(TermQuery::new(
                tantivy_terms.into_iter().next().unwrap(),
                Default::default(),
            ));
        }
        let offset_terms: Vec<(usize, tantivy::Term)> = tantivy_terms
            .into_iter()
            .enumerate()
            .collect();
        Box::new(PhraseQuery::new_with_offset_and_slop(
            offset_terms,
            self.slop.unwrap_or(0),
        ))
    }

    fn make_prefix_query(&self, field: Field, prefix: &str) -> Box<dyn Query> {
        let pattern = format!("{}.*", regex_escape(&prefix.to_lowercase()));
        match tantivy::query::RegexQuery::from_pattern(&pattern, field) {
            Ok(q) => Box::new(q),
            Err(_) => Box::new(EmptyQuery),
        }
    }

    fn make_fuzzy_query(&self, field: Field, term: &str, distance: u8) -> Box<dyn Query> {
        let t = tantivy::Term::from_field_text(field, &term.to_lowercase());
        Box::new(FuzzyTermQuery::new(t, distance, true))
    }

    /// Format a query string as a human-readable execution plan.
    pub fn explain(&self, input: &str) -> Result<String, SearchError> {
        let input = input.trim();
        if input.is_empty() || input == "*" {
            return Ok("WILDCARD *\n".to_string());
        }
        let (_, ast) =
            parse_query(input).map_err(|e| SearchError::QueryParseError(format!("{}", e)))?;
        Ok(format_node(&ast, 0))
    }
}

fn format_node(node: &QueryNode, indent: usize) -> String {
    let pad = "  ".repeat(indent);
    match node {
        QueryNode::MatchAll => format!("{pad}WILDCARD *\n"),
        QueryNode::Term(t) => format!("{pad}UNION {{\n{pad}  {t}\n{pad}}}\n"),
        QueryNode::FieldTerm { field, term } => format!("{pad}@{field} TERM {term}\n"),
        QueryNode::TagQuery { field, tags } => {
            let vals = tags.join(" | ");
            format!("{pad}@{field} TAG {{{vals}}}\n")
        }
        QueryNode::NumericRange {
            field,
            min,
            max,
            min_exclusive,
            max_exclusive,
        } => {
            let lb = if *min_exclusive { "(" } else { "[" };
            let rb = if *max_exclusive { ")" } else { "]" };
            format!("{pad}@{field} NUMERIC {lb}{min} {max}{rb}\n")
        }
        QueryNode::And(nodes) => {
            let mut s = format!("{pad}INTERSECT {{\n");
            for n in nodes {
                s.push_str(&format_node(n, indent + 1));
            }
            s.push_str(&format!("{pad}}}\n"));
            s
        }
        QueryNode::Or(nodes) => {
            let mut s = format!("{pad}UNION {{\n");
            for n in nodes {
                s.push_str(&format_node(n, indent + 1));
            }
            s.push_str(&format!("{pad}}}\n"));
            s
        }
        QueryNode::Not(inner) => {
            let mut s = format!("{pad}NOT {{\n");
            s.push_str(&format_node(inner, indent + 1));
            s.push_str(&format!("{pad}}}\n"));
            s
        }
        QueryNode::Phrase { field, terms } => {
            let phrase = terms.join(" ");
            match field {
                Some(f) => format!("{pad}@{f} PHRASE \"{phrase}\"\n"),
                None => format!("{pad}PHRASE \"{phrase}\"\n"),
            }
        }
        QueryNode::Prefix { field, prefix } => match field {
            Some(f) => format!("{pad}@{f} PREFIX {prefix}*\n"),
            None => format!("{pad}PREFIX {prefix}*\n"),
        },
        QueryNode::Fuzzy {
            field,
            term,
            distance,
        } => {
            let pct = "%".repeat(*distance as usize);
            match field {
                Some(f) => format!("{pad}@{f} FUZZY {pct}{term}{pct}\n"),
                None => format!("{pad}FUZZY {pct}{term}{pct}\n"),
            }
        }
        QueryNode::GeoRadius {
            field,
            lon,
            lat,
            radius_m,
        } => format!("{pad}@{field} GEO [{lon} {lat} {radius_m} m]\n"),
    }
}

/// Escape special regex characters.
fn regex_escape(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    for c in s.chars() {
        if "\\.*+?()[]{}|^$".contains(c) {
            result.push('\\');
        }
        result.push(c);
    }
    result
}

// ============================================================================
// nom parsers
// ============================================================================

fn is_term_char(c: char) -> bool {
    c.is_alphanumeric() || c == '_' || c == '-' || c == '.' || c == '\'' || c == '/' || c == '\\'
}

fn is_field_name_char(c: char) -> bool {
    c.is_alphanumeric() || c == '_' || c == '-' || c == '.'
}

fn is_tag_char(c: char) -> bool {
    !matches!(c, '|' | '{' | '}')
}

fn parse_term(input: &str) -> IResult<&str, QueryNode> {
    let (input, _) = multispace0(input)?;
    if input.starts_with('*') {
        let (input, _) = tag("*")(input)?;
        return Ok((input, QueryNode::MatchAll));
    }
    let (input, word) = take_while1(is_term_char)(input)?;
    // Check for trailing * (prefix query)
    if input.starts_with('*') {
        let (input, _) = tag("*")(input)?;
        return Ok((
            input,
            QueryNode::Prefix {
                field: None,
                prefix: word.to_string(),
            },
        ));
    }
    Ok((input, QueryNode::Term(word.to_string())))
}

fn parse_field_name(input: &str) -> IResult<&str, String> {
    let (input, name) = take_while1(is_field_name_char)(input)?;
    Ok((input, name.to_string()))
}

fn parse_tag_value(input: &str) -> IResult<&str, String> {
    let (input, val) = take_while1(is_tag_char)(input)?;
    Ok((input, val.trim().to_string()))
}

fn parse_tag_query(input: &str) -> IResult<&str, Vec<String>> {
    delimited(
        char('{'),
        separated_list1(char('|'), parse_tag_value),
        char('}'),
    )(input)
}

fn parse_num(input: &str) -> IResult<&str, (f64, bool)> {
    let (input, _) = multispace0(input)?;
    if let Some(rest) = input.strip_prefix("+inf") {
        return Ok((rest, (f64::INFINITY, false)));
    }
    if let Some(rest) = input.strip_prefix("inf") {
        return Ok((rest, (f64::INFINITY, false)));
    }
    if let Some(rest) = input.strip_prefix("-inf") {
        return Ok((rest, (f64::NEG_INFINITY, false)));
    }
    let (input, exclusive) = opt(char('('))(input)?;
    let (input, num_str) =
        take_while1(|c: char| c.is_ascii_digit() || c == '.' || c == '-' || c == '+')(input)?;
    let val: f64 = num_str.parse().map_err(|_| {
        nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Float))
    })?;
    Ok((input, (val, exclusive.is_some())))
}

fn parse_numeric_range(input: &str) -> IResult<&str, (f64, bool, f64, bool)> {
    let (input, _) = char('[')(input)?;
    let (input, (min, min_exclusive)) = parse_num(input)?;
    let (input, _) = multispace1(input)?;
    let (input, (max, max_exclusive)) = parse_num(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = char(']')(input)?;
    Ok((input, (min, min_exclusive, max, max_exclusive)))
}

/// Parse a quoted phrase: "hello world"
fn parse_phrase(input: &str) -> IResult<&str, Vec<String>> {
    let (input, _) = char('"')(input)?;
    let mut terms = Vec::new();
    let mut remaining = input;
    loop {
        // Skip whitespace
        let (r, _) = multispace0(remaining)?;
        remaining = r;
        if remaining.starts_with('"') {
            let (r, _) = char('"')(remaining)?;
            return Ok((r, terms));
        }
        if remaining.is_empty() {
            // Unterminated quote — treat what we have as the phrase
            return Ok((remaining, terms));
        }
        // Consume a word
        let (r, word) = take_while1(|c: char| c != '"' && !c.is_whitespace())(remaining)?;
        terms.push(word.to_string());
        remaining = r;
    }
}

/// Parse a fuzzy query: %term%, %%term%%, %%%term%%%
fn parse_fuzzy(input: &str) -> IResult<&str, QueryNode> {
    let (input, _) = multispace0(input)?;
    // Count leading % characters (1-3)
    let mut distance: u8 = 0;
    let mut remaining = input;
    while remaining.starts_with('%') && distance < 3 {
        remaining = &remaining[1..];
        distance += 1;
    }
    if distance == 0 {
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Tag,
        )));
    }
    // Parse the term
    let (mut remaining, word) = take_while1(|c: char| c != '%' && !c.is_whitespace())(remaining)?;
    // Consume matching trailing % characters
    for _ in 0..distance {
        if remaining.starts_with('%') {
            remaining = &remaining[1..];
        }
    }
    Ok((
        remaining,
        QueryNode::Fuzzy {
            field: None,
            term: word.to_string(),
            distance,
        },
    ))
}

/// Parse a geo radius: [lon lat radius unit]
/// Returns (lon, lat, radius_in_meters).
fn parse_geo_radius(input: &str) -> IResult<&str, (f64, f64, f64)> {
    let (input, _) = char('[')(input)?;
    let (input, _) = multispace0(input)?;
    // Parse lon
    let (input, lon_str) =
        take_while1(|c: char| c.is_ascii_digit() || c == '.' || c == '-' || c == '+')(input)?;
    let lon: f64 = lon_str.parse().map_err(|_| {
        nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Float))
    })?;
    let (input, _) = multispace1(input)?;
    // Parse lat
    let (input, lat_str) =
        take_while1(|c: char| c.is_ascii_digit() || c == '.' || c == '-' || c == '+')(input)?;
    let lat: f64 = lat_str.parse().map_err(|_| {
        nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Float))
    })?;
    let (input, _) = multispace1(input)?;
    // Parse radius
    let (input, rad_str) =
        take_while1(|c: char| c.is_ascii_digit() || c == '.' || c == '-' || c == '+')(input)?;
    let radius: f64 = rad_str.parse().map_err(|_| {
        nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Float))
    })?;
    let (input, _) = multispace1(input)?;
    // Parse unit
    let (input, unit) = take_while1(|c: char| c.is_alphabetic())(input)?;
    let radius_m = match unit.to_lowercase().as_str() {
        "m" => radius,
        "km" => radius * 1000.0,
        "mi" => radius * 1609.344,
        "ft" => radius * 0.3048,
        _ => {
            return Err(nom::Err::Error(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Tag,
            )));
        }
    };
    let (input, _) = multispace0(input)?;
    let (input, _) = char(']')(input)?;
    Ok((input, (lon, lat, radius_m)))
}

fn parse_field_query(input: &str) -> IResult<&str, (String, QueryNode)> {
    let (input, _) = multispace0(input)?;
    let (input, _) = char('@')(input)?;
    let (input, field) = parse_field_name(input)?;
    let (input, _) = char(':')(input)?;

    if let Ok((input, tags)) = parse_tag_query(input) {
        return Ok((input, (field.clone(), QueryNode::TagQuery { field, tags })));
    }

    // Try geo radius before numeric range (geo has 4 tokens: lon lat radius unit)
    if let Ok((input, (lon, lat, radius_m))) = parse_geo_radius(input) {
        return Ok((
            input,
            (
                field.clone(),
                QueryNode::GeoRadius {
                    field,
                    lon,
                    lat,
                    radius_m,
                },
            ),
        ));
    }

    if let Ok((input, (min, min_exclusive, max, max_exclusive))) = parse_numeric_range(input) {
        return Ok((
            input,
            (
                field.clone(),
                QueryNode::NumericRange {
                    field,
                    min,
                    max,
                    min_exclusive,
                    max_exclusive,
                },
            ),
        ));
    }

    // Try phrase query: @field:"exact phrase"
    if input.starts_with('"') {
        let (input, terms) = parse_phrase(input)?;
        return Ok((
            input,
            (
                field.clone(),
                QueryNode::Phrase {
                    field: Some(field),
                    terms,
                },
            ),
        ));
    }

    let (input, word) = take_while1(is_term_char)(input)?;
    // Check for trailing * (field-specific prefix query)
    if input.starts_with('*') {
        let (input, _) = tag("*")(input)?;
        return Ok((
            input,
            (
                field.clone(),
                QueryNode::Prefix {
                    field: Some(field),
                    prefix: word.to_string(),
                },
            ),
        ));
    }
    Ok((
        input,
        (
            field.clone(),
            QueryNode::FieldTerm {
                field,
                term: word.to_string(),
            },
        ),
    ))
}

fn parse_atom(input: &str) -> IResult<&str, QueryNode> {
    let (input, _) = multispace0(input)?;

    if input.starts_with('(') {
        let (input, _) = char('(')(input)?;
        let (input, node) = parse_or_expr(input)?;
        let (input, _) = multispace0(input)?;
        let (input, _) = char(')')(input)?;
        return Ok((input, node));
    }

    if input.starts_with('@') {
        let (input, (_, node)) = parse_field_query(input)?;
        return Ok((input, node));
    }

    // Try phrase query: "exact phrase"
    if input.starts_with('"') {
        let (input, terms) = parse_phrase(input)?;
        return Ok((input, QueryNode::Phrase { field: None, terms }));
    }

    // Try fuzzy query: %term%
    if input.starts_with('%') {
        return parse_fuzzy(input);
    }

    parse_term(input)
}

fn parse_unary(input: &str) -> IResult<&str, QueryNode> {
    let (input, _) = multispace0(input)?;
    if input.starts_with('-') {
        let (input, _) = char('-')(input)?;
        let (input, node) = parse_atom(input)?;
        return Ok((input, QueryNode::Not(Box::new(node))));
    }
    parse_atom(input)
}

fn parse_and_expr(input: &str) -> IResult<&str, QueryNode> {
    let (mut input, first) = parse_unary(input)?;
    let mut nodes = vec![first];

    loop {
        let (rest, _) = multispace0(input)?;
        if rest.is_empty() || rest.starts_with('|') || rest.starts_with(')') {
            input = rest;
            break;
        }
        match parse_unary(rest) {
            Ok((rest, node)) => {
                nodes.push(node);
                input = rest;
            }
            Err(_) => {
                input = rest;
                break;
            }
        }
    }

    if nodes.len() == 1 {
        Ok((input, nodes.into_iter().next().unwrap()))
    } else {
        Ok((input, QueryNode::And(nodes)))
    }
}

fn parse_or_expr(input: &str) -> IResult<&str, QueryNode> {
    let (mut input, first) = parse_and_expr(input)?;
    let mut nodes = vec![first];

    loop {
        let (rest, _) = multispace0(input)?;
        if !rest.starts_with('|') {
            input = rest;
            break;
        }
        let (rest, _) = char('|')(rest)?;
        let (rest, _) = multispace0(rest)?;
        let (rest, node) = parse_and_expr(rest)?;
        nodes.push(node);
        input = rest;
    }

    if nodes.len() == 1 {
        Ok((input, nodes.into_iter().next().unwrap()))
    } else {
        Ok((input, QueryNode::Or(nodes)))
    }
}

fn parse_query(input: &str) -> IResult<&str, QueryNode> {
    let (input, _) = multispace0(input)?;
    parse_or_expr(input)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_ast(input: &str) -> QueryNode {
        let (remaining, node) = parse_query(input).unwrap();
        assert!(remaining.trim().is_empty(), "Unexpected: '{}'", remaining);
        node
    }

    #[test]
    fn test_simple_term() {
        let node = parse_ast("hello");
        assert!(matches!(node, QueryNode::Term(ref s) if s == "hello"));
    }

    #[test]
    fn test_and_terms() {
        let node = parse_ast("hello world");
        assert!(matches!(node, QueryNode::And(ref v) if v.len() == 2));
    }

    #[test]
    fn test_or_terms() {
        let node = parse_ast("a | b");
        assert!(matches!(node, QueryNode::Or(ref v) if v.len() == 2));
    }

    #[test]
    fn test_field_term() {
        let node = parse_ast("@title:hello");
        assert!(
            matches!(node, QueryNode::FieldTerm { ref field, ref term } if field == "title" && term == "hello")
        );
    }

    #[test]
    fn test_tag_query() {
        let node = parse_ast("@tags:{redis|search}");
        match node {
            QueryNode::TagQuery { field, tags } => {
                assert_eq!(field, "tags");
                assert_eq!(tags, vec!["redis", "search"]);
            }
            _ => panic!("Expected TagQuery, got {:?}", node),
        }
    }

    #[test]
    fn test_single_tag() {
        let node = parse_ast("@tags:{redis}");
        match node {
            QueryNode::TagQuery { field, tags } => {
                assert_eq!(field, "tags");
                assert_eq!(tags, vec!["redis"]);
            }
            _ => panic!("Expected TagQuery"),
        }
    }

    #[test]
    fn test_numeric_range() {
        let node = parse_ast("@price:[10 100]");
        match node {
            QueryNode::NumericRange {
                field,
                min,
                max,
                min_exclusive,
                max_exclusive,
            } => {
                assert_eq!(field, "price");
                assert_eq!(min, 10.0);
                assert_eq!(max, 100.0);
                assert!(!min_exclusive);
                assert!(!max_exclusive);
            }
            _ => panic!("Expected NumericRange"),
        }
    }

    #[test]
    fn test_exclusive_numeric_range() {
        let node = parse_ast("@price:[(10 (100]");
        match node {
            QueryNode::NumericRange {
                field,
                min,
                max,
                min_exclusive,
                max_exclusive,
            } => {
                assert_eq!(field, "price");
                assert_eq!(min, 10.0);
                assert_eq!(max, 100.0);
                assert!(min_exclusive);
                assert!(max_exclusive);
            }
            _ => panic!("Expected NumericRange"),
        }
    }

    #[test]
    fn test_negation() {
        let node = parse_ast("-@field:value");
        assert!(matches!(node, QueryNode::Not(_)));
    }

    #[test]
    fn test_boolean_or_parens() {
        let node = parse_ast("(a | b)");
        assert!(matches!(node, QueryNode::Or(ref v) if v.len() == 2));
    }

    #[test]
    fn test_mixed_query() {
        let node = parse_ast("@title:redis @tags:{fast} @price:[0 100]");
        assert!(matches!(node, QueryNode::And(ref v) if v.len() == 3));
    }

    #[test]
    fn test_nested_parens() {
        let node = parse_ast("(a | b) (c | d)");
        assert!(matches!(node, QueryNode::And(ref v) if v.len() == 2));
    }

    #[test]
    fn test_negation_with_group() {
        let node = parse_ast("hello -world");
        match node {
            QueryNode::And(ref v) => {
                assert_eq!(v.len(), 2);
                assert!(matches!(&v[1], QueryNode::Not(_)));
            }
            _ => panic!("Expected And"),
        }
    }

    #[test]
    fn test_match_all() {
        let node = parse_ast("*");
        assert!(matches!(node, QueryNode::MatchAll));
    }

    #[test]
    fn test_phrase_query() {
        let node = parse_ast("\"hello world\"");
        match node {
            QueryNode::Phrase { field, terms } => {
                assert!(field.is_none());
                assert_eq!(terms, vec!["hello", "world"]);
            }
            _ => panic!("Expected Phrase, got {:?}", node),
        }
    }

    #[test]
    fn test_field_specific_phrase() {
        let node = parse_ast("@title:\"hello world\"");
        match node {
            QueryNode::Phrase { field, terms } => {
                assert_eq!(field.as_deref(), Some("title"));
                assert_eq!(terms, vec!["hello", "world"]);
            }
            _ => panic!("Expected Phrase, got {:?}", node),
        }
    }

    #[test]
    fn test_phrase_in_boolean() {
        let node = parse_ast("\"hello world\" @tags:{fast}");
        assert!(matches!(node, QueryNode::And(ref v) if v.len() == 2));
    }

    #[test]
    fn test_prefix_query() {
        let node = parse_ast("hel*");
        match node {
            QueryNode::Prefix { field, prefix } => {
                assert!(field.is_none());
                assert_eq!(prefix, "hel");
            }
            _ => panic!("Expected Prefix, got {:?}", node),
        }
    }

    #[test]
    fn test_field_prefix_query() {
        let node = parse_ast("@title:hel*");
        match node {
            QueryNode::Prefix { field, prefix } => {
                assert_eq!(field.as_deref(), Some("title"));
                assert_eq!(prefix, "hel");
            }
            _ => panic!("Expected Prefix, got {:?}", node),
        }
    }

    #[test]
    fn test_fuzzy_query_distance_1() {
        let node = parse_ast("%hello%");
        match node {
            QueryNode::Fuzzy {
                field,
                term,
                distance,
            } => {
                assert!(field.is_none());
                assert_eq!(term, "hello");
                assert_eq!(distance, 1);
            }
            _ => panic!("Expected Fuzzy, got {:?}", node),
        }
    }

    #[test]
    fn test_fuzzy_query_distance_2() {
        let node = parse_ast("%%hello%%");
        match node {
            QueryNode::Fuzzy {
                field,
                term,
                distance,
            } => {
                assert!(field.is_none());
                assert_eq!(term, "hello");
                assert_eq!(distance, 2);
            }
            _ => panic!("Expected Fuzzy, got {:?}", node),
        }
    }

    #[test]
    fn test_fuzzy_query_distance_3() {
        let node = parse_ast("%%%hello%%%");
        match node {
            QueryNode::Fuzzy {
                field,
                term,
                distance,
            } => {
                assert!(field.is_none());
                assert_eq!(term, "hello");
                assert_eq!(distance, 3);
            }
            _ => panic!("Expected Fuzzy, got {:?}", node),
        }
    }

    #[test]
    fn test_geo_radius_query() {
        let node = parse_ast("@location:[-73.9654 40.7829 10 km]");
        match node {
            QueryNode::GeoRadius {
                field,
                lon,
                lat,
                radius_m,
            } => {
                assert_eq!(field, "location");
                assert!((lon - (-73.9654)).abs() < 1e-6);
                assert!((lat - 40.7829).abs() < 1e-6);
                assert!((radius_m - 10_000.0).abs() < 0.1);
            }
            _ => panic!("Expected GeoRadius, got {:?}", node),
        }
    }

    #[test]
    fn test_geo_radius_miles() {
        let node = parse_ast("@loc:[-122.4194 37.7749 5 mi]");
        match node {
            QueryNode::GeoRadius {
                field, radius_m, ..
            } => {
                assert_eq!(field, "loc");
                assert!((radius_m - 5.0 * 1609.344).abs() < 0.1);
            }
            _ => panic!("Expected GeoRadius, got {:?}", node),
        }
    }

    #[test]
    fn test_geo_combined_with_text() {
        let node = parse_ast("hello @location:[-73.0 40.0 1 km]");
        assert!(matches!(node, QueryNode::And(ref v) if v.len() == 2));
    }
}
