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
//! atom       = "(" or_expr ")" | "@" field ":" field_query | term
//! field_query = "{" tag ("|" tag)* "}" | "[" num num "]" | term
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
    AllQuery, BooleanQuery, EmptyQuery, Occur, Query, RangeQuery, TermQuery, TermSetQuery,
};
use tantivy::schema::{Field, Schema};

use crate::error::SearchError;
use crate::schema::{FieldType, SearchIndexDef};

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
    NumericRange { field: String, min: f64, max: f64 },
    /// AND of multiple nodes (implicit).
    And(Vec<QueryNode>),
    /// OR of multiple nodes.
    Or(Vec<QueryNode>),
    /// Negation.
    Not(Box<QueryNode>),
    /// Match all documents.
    MatchAll,
}

/// Query parser that converts RediSearch syntax to tantivy queries.
pub struct QueryParser<'a> {
    schema: &'a Schema,
    field_map: &'a HashMap<String, Field>,
    def: &'a SearchIndexDef,
    #[allow(dead_code)]
    key_field: Field,
}

impl<'a> QueryParser<'a> {
    pub fn new(
        schema: &'a Schema,
        field_map: &'a HashMap<String, Field>,
        def: &'a SearchIndexDef,
        key_field: Field,
    ) -> Self {
        Self {
            schema,
            field_map,
            def,
            key_field,
        }
    }

    /// Parse a query string into a tantivy Query.
    pub fn parse(&self, input: &str) -> Result<Box<dyn Query>, SearchError> {
        let input = input.trim();
        if input.is_empty() || input == "*" {
            return Ok(Box::new(AllQuery));
        }

        let (remaining, ast) =
            parse_query(input).map_err(|e| SearchError::QueryParseError(format!("{}", e)))?;

        if !remaining.trim().is_empty() {
            return Err(SearchError::QueryParseError(format!(
                "Unexpected trailing input: '{}'",
                remaining.trim()
            )));
        }

        self.ast_to_query(&ast)
    }

    fn ast_to_query(&self, node: &QueryNode) -> Result<Box<dyn Query>, SearchError> {
        match node {
            QueryNode::MatchAll => Ok(Box::new(AllQuery)),
            QueryNode::Term(term) => {
                let text_fields = self.default_text_fields();
                if text_fields.is_empty() {
                    return Ok(Box::new(EmptyQuery));
                }
                if text_fields.len() == 1 {
                    return Ok(self.make_text_query(text_fields[0], term));
                }
                let subqueries: Vec<(Occur, Box<dyn Query>)> = text_fields
                    .into_iter()
                    .map(|field| {
                        let q = self.make_text_query(field, term);
                        (Occur::Should, q)
                    })
                    .collect();
                Ok(Box::new(BooleanQuery::new(subqueries)))
            }
            QueryNode::FieldTerm { field, term } => {
                let tantivy_field = self.resolve_field(field)?;
                Ok(self.make_text_query(tantivy_field, term))
            }
            QueryNode::TagQuery { field, tags } => {
                let tantivy_field = self.resolve_field(field)?;
                if tags.len() == 1 {
                    let term = tantivy::Term::from_field_text(tantivy_field, &tags[0]);
                    return Ok(Box::new(TermQuery::new(term, Default::default())));
                }
                let terms: Vec<tantivy::Term> = tags
                    .iter()
                    .map(|t| tantivy::Term::from_field_text(tantivy_field, t))
                    .collect();
                Ok(Box::new(TermSetQuery::new(terms)))
            }
            QueryNode::NumericRange { field, min, max } => {
                let _tantivy_field = self.resolve_field(field)?;
                let range_query = RangeQuery::new_f64_bounds(
                    field.clone(),
                    std::ops::Bound::Included(*min),
                    std::ops::Bound::Included(*max),
                );
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
        }
    }

    fn default_text_fields(&self) -> Vec<Field> {
        self.def
            .fields
            .iter()
            .filter(|f| matches!(f.field_type, FieldType::Text { .. }) && !f.noindex)
            .filter_map(|f| self.field_map.get(&f.name).copied())
            .collect()
    }

    fn resolve_field(&self, name: &str) -> Result<Field, SearchError> {
        self.field_map
            .get(name)
            .copied()
            .ok_or_else(|| SearchError::QueryParseError(format!("Unknown field: {}", name)))
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

fn parse_num(input: &str) -> IResult<&str, f64> {
    let (input, _) = multispace0(input)?;
    if let Some(rest) = input.strip_prefix("+inf") {
        return Ok((rest, f64::INFINITY));
    }
    if let Some(rest) = input.strip_prefix("inf") {
        return Ok((rest, f64::INFINITY));
    }
    if let Some(rest) = input.strip_prefix("-inf") {
        return Ok((rest, f64::NEG_INFINITY));
    }
    let (input, _exclusive) = opt(char('('))(input)?;
    let (input, num_str) =
        take_while1(|c: char| c.is_ascii_digit() || c == '.' || c == '-' || c == '+')(input)?;
    let val: f64 = num_str.parse().map_err(|_| {
        nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Float))
    })?;
    Ok((input, val))
}

fn parse_numeric_range(input: &str) -> IResult<&str, (f64, f64)> {
    let (input, _) = char('[')(input)?;
    let (input, min) = parse_num(input)?;
    let (input, _) = multispace1(input)?;
    let (input, max) = parse_num(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = char(']')(input)?;
    Ok((input, (min, max)))
}

fn parse_field_query(input: &str) -> IResult<&str, (String, QueryNode)> {
    let (input, _) = multispace0(input)?;
    let (input, _) = char('@')(input)?;
    let (input, field) = parse_field_name(input)?;
    let (input, _) = char(':')(input)?;

    if let Ok((input, tags)) = parse_tag_query(input) {
        return Ok((input, (field.clone(), QueryNode::TagQuery { field, tags })));
    }

    if let Ok((input, (min, max))) = parse_numeric_range(input) {
        return Ok((
            input,
            (field.clone(), QueryNode::NumericRange { field, min, max }),
        ));
    }

    let (input, word) = take_while1(is_term_char)(input)?;
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
            QueryNode::NumericRange { field, min, max } => {
                assert_eq!(field, "price");
                assert_eq!(min, 10.0);
                assert_eq!(max, 100.0);
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
}
