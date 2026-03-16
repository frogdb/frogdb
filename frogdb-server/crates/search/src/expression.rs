//! Expression parser and evaluator for APPLY/FILTER pipeline steps.
//!
//! Implements a Pratt parser (recursive descent with precedence climbing) for
//! arithmetic, comparison, logical, and function-call expressions.

use crate::aggregate::Row;

// =============================================================================
// AST
// =============================================================================

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Field(String),
    Literal(ExprValue),
    BinaryOp {
        op: BinOp,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    UnaryOp {
        op: UnOp,
        operand: Box<Expr>,
    },
    FnCall {
        name: String,
        args: Vec<Expr>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum ExprValue {
    Number(f64),
    Str(String),
    Bool(bool),
    Null,
}

impl ExprValue {
    pub fn is_truthy(&self) -> bool {
        match self {
            ExprValue::Bool(b) => *b,
            ExprValue::Number(n) => *n != 0.0,
            ExprValue::Str(s) => !s.is_empty(),
            ExprValue::Null => false,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            ExprValue::Number(n) => Some(*n),
            ExprValue::Str(s) => s.parse().ok(),
            ExprValue::Bool(b) => Some(if *b { 1.0 } else { 0.0 }),
            ExprValue::Null => None,
        }
    }

    pub fn as_string(&self) -> String {
        match self {
            ExprValue::Number(n) => {
                if *n == n.trunc() && n.abs() < 1e15 {
                    format!("{}", *n as i64)
                } else {
                    format!("{n}")
                }
            }
            ExprValue::Str(s) => s.clone(),
            ExprValue::Bool(b) => if *b { "1" } else { "0" }.to_string(),
            ExprValue::Null => String::new(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Eq,
    Ne,
    Lt,
    Gt,
    Le,
    Ge,
    And,
    Or,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnOp {
    Neg,
    Not,
}

// =============================================================================
// Tokenizer
// =============================================================================

#[derive(Debug, Clone, PartialEq)]
enum Token {
    Field(String), // @field_name
    Number(f64),   // 123, 3.14
    Str(String),   // "hello" or 'hello'
    Ident(String), // function names, true/false
    Plus,
    Minus,
    Star,
    Slash,
    Percent,
    EqEq,
    BangEq,
    Lt,
    Gt,
    Le,
    Ge,
    AmpAmp,
    PipePipe,
    Bang,
    LParen,
    RParen,
    Comma,
    Eof,
}

struct Tokenizer<'a> {
    input: &'a [u8],
    pos: usize,
}

impl<'a> Tokenizer<'a> {
    fn new(input: &'a str) -> Self {
        Self {
            input: input.as_bytes(),
            pos: 0,
        }
    }

    fn skip_whitespace(&mut self) {
        while self.pos < self.input.len() && self.input[self.pos].is_ascii_whitespace() {
            self.pos += 1;
        }
    }

    fn peek_byte(&self) -> Option<u8> {
        self.input.get(self.pos).copied()
    }

    fn next_token(&mut self) -> Result<Token, String> {
        self.skip_whitespace();
        let Some(ch) = self.peek_byte() else {
            return Ok(Token::Eof);
        };

        match ch {
            b'@' => {
                self.pos += 1;
                let start = self.pos;
                while self.pos < self.input.len()
                    && (self.input[self.pos].is_ascii_alphanumeric()
                        || self.input[self.pos] == b'_')
                {
                    self.pos += 1;
                }
                let name = std::str::from_utf8(&self.input[start..self.pos])
                    .map_err(|_| "invalid field name")?;
                Ok(Token::Field(name.to_string()))
            }
            b'"' | b'\'' => {
                let quote = ch;
                self.pos += 1;
                let start = self.pos;
                while self.pos < self.input.len() && self.input[self.pos] != quote {
                    if self.input[self.pos] == b'\\' {
                        self.pos += 1; // skip escaped char
                    }
                    self.pos += 1;
                }
                let s = std::str::from_utf8(&self.input[start..self.pos])
                    .map_err(|_| "invalid string")?
                    .to_string();
                if self.pos < self.input.len() {
                    self.pos += 1; // skip closing quote
                }
                Ok(Token::Str(s))
            }
            b'0'..=b'9' | b'.'
                if ch == b'.'
                    && self.pos + 1 < self.input.len()
                    && self.input[self.pos + 1].is_ascii_digit()
                    || ch.is_ascii_digit() =>
            {
                let start = self.pos;
                while self.pos < self.input.len()
                    && (self.input[self.pos].is_ascii_digit() || self.input[self.pos] == b'.')
                {
                    self.pos += 1;
                }
                let s = std::str::from_utf8(&self.input[start..self.pos])
                    .map_err(|_| "invalid number")?;
                let n: f64 = s.parse().map_err(|_| format!("invalid number: {s}"))?;
                Ok(Token::Number(n))
            }
            b'+' => {
                self.pos += 1;
                Ok(Token::Plus)
            }
            b'-' => {
                self.pos += 1;
                Ok(Token::Minus)
            }
            b'*' => {
                self.pos += 1;
                Ok(Token::Star)
            }
            b'/' => {
                self.pos += 1;
                Ok(Token::Slash)
            }
            b'%' => {
                self.pos += 1;
                Ok(Token::Percent)
            }
            b'=' => {
                self.pos += 1;
                if self.peek_byte() == Some(b'=') {
                    self.pos += 1;
                }
                Ok(Token::EqEq)
            }
            b'!' => {
                self.pos += 1;
                if self.peek_byte() == Some(b'=') {
                    self.pos += 1;
                    Ok(Token::BangEq)
                } else {
                    Ok(Token::Bang)
                }
            }
            b'<' => {
                self.pos += 1;
                if self.peek_byte() == Some(b'=') {
                    self.pos += 1;
                    Ok(Token::Le)
                } else {
                    Ok(Token::Lt)
                }
            }
            b'>' => {
                self.pos += 1;
                if self.peek_byte() == Some(b'=') {
                    self.pos += 1;
                    Ok(Token::Ge)
                } else {
                    Ok(Token::Gt)
                }
            }
            b'&' => {
                self.pos += 1;
                if self.peek_byte() == Some(b'&') {
                    self.pos += 1;
                }
                Ok(Token::AmpAmp)
            }
            b'|' => {
                self.pos += 1;
                if self.peek_byte() == Some(b'|') {
                    self.pos += 1;
                }
                Ok(Token::PipePipe)
            }
            b'(' => {
                self.pos += 1;
                Ok(Token::LParen)
            }
            b')' => {
                self.pos += 1;
                Ok(Token::RParen)
            }
            b',' => {
                self.pos += 1;
                Ok(Token::Comma)
            }
            _ if ch.is_ascii_alphabetic() || ch == b'_' => {
                let start = self.pos;
                while self.pos < self.input.len()
                    && (self.input[self.pos].is_ascii_alphanumeric()
                        || self.input[self.pos] == b'_')
                {
                    self.pos += 1;
                }
                let s = std::str::from_utf8(&self.input[start..self.pos])
                    .map_err(|_| "invalid identifier")?;
                Ok(Token::Ident(s.to_string()))
            }
            _ => Err(format!("unexpected character: '{}'", ch as char)),
        }
    }
}

// =============================================================================
// Parser (Pratt / precedence climbing)
// =============================================================================

struct Parser<'a> {
    tokens: Vec<Token>,
    pos: usize,
    _phantom: std::marker::PhantomData<&'a ()>,
}

impl Parser<'_> {
    fn from_str(input: &str) -> Result<Self, String> {
        let mut tokenizer = Tokenizer::new(input);
        let mut tokens = Vec::new();
        loop {
            let tok = tokenizer.next_token()?;
            let is_eof = tok == Token::Eof;
            tokens.push(tok);
            if is_eof {
                break;
            }
        }
        Ok(Self {
            tokens,
            pos: 0,
            _phantom: std::marker::PhantomData,
        })
    }

    fn peek(&self) -> &Token {
        self.tokens.get(self.pos).unwrap_or(&Token::Eof)
    }

    fn advance(&mut self) -> Token {
        let tok = self.tokens.get(self.pos).cloned().unwrap_or(Token::Eof);
        self.pos += 1;
        tok
    }

    fn expect_rparen(&mut self) -> Result<(), String> {
        if matches!(self.peek(), Token::RParen) {
            self.advance();
            Ok(())
        } else {
            Err("expected ')'".to_string())
        }
    }

    fn parse_expr(&mut self) -> Result<Expr, String> {
        self.parse_prec(0)
    }

    fn parse_prec(&mut self, min_prec: u8) -> Result<Expr, String> {
        let mut lhs = self.parse_unary()?;

        loop {
            let (op, prec) = match self.peek() {
                Token::PipePipe => (BinOp::Or, 1),
                Token::AmpAmp => (BinOp::And, 2),
                Token::EqEq => (BinOp::Eq, 3),
                Token::BangEq => (BinOp::Ne, 3),
                Token::Lt => (BinOp::Lt, 4),
                Token::Gt => (BinOp::Gt, 4),
                Token::Le => (BinOp::Le, 4),
                Token::Ge => (BinOp::Ge, 4),
                Token::Plus => (BinOp::Add, 5),
                Token::Minus => (BinOp::Sub, 5),
                Token::Star => (BinOp::Mul, 6),
                Token::Slash => (BinOp::Div, 6),
                Token::Percent => (BinOp::Mod, 6),
                _ => break,
            };

            if prec < min_prec {
                break;
            }

            self.advance();
            // Left-associative: use prec + 1 for right side
            let rhs = self.parse_prec(prec + 1)?;
            lhs = Expr::BinaryOp {
                op,
                left: Box::new(lhs),
                right: Box::new(rhs),
            };
        }

        Ok(lhs)
    }

    fn parse_unary(&mut self) -> Result<Expr, String> {
        match self.peek().clone() {
            Token::Minus => {
                self.advance();
                let operand = self.parse_unary()?;
                Ok(Expr::UnaryOp {
                    op: UnOp::Neg,
                    operand: Box::new(operand),
                })
            }
            Token::Bang => {
                self.advance();
                let operand = self.parse_unary()?;
                Ok(Expr::UnaryOp {
                    op: UnOp::Not,
                    operand: Box::new(operand),
                })
            }
            _ => self.parse_primary(),
        }
    }

    fn parse_primary(&mut self) -> Result<Expr, String> {
        match self.advance() {
            Token::Number(n) => Ok(Expr::Literal(ExprValue::Number(n))),
            Token::Str(s) => Ok(Expr::Literal(ExprValue::Str(s))),
            Token::Field(name) => Ok(Expr::Field(name)),
            Token::Ident(name) => {
                match name.as_str() {
                    "true" => return Ok(Expr::Literal(ExprValue::Bool(true))),
                    "false" => return Ok(Expr::Literal(ExprValue::Bool(false))),
                    "null" | "NULL" => return Ok(Expr::Literal(ExprValue::Null)),
                    _ => {}
                }
                // Function call
                if matches!(self.peek(), Token::LParen) {
                    self.advance(); // consume '('
                    let mut args = Vec::new();
                    if !matches!(self.peek(), Token::RParen) {
                        args.push(self.parse_expr()?);
                        while matches!(self.peek(), Token::Comma) {
                            self.advance();
                            args.push(self.parse_expr()?);
                        }
                    }
                    self.expect_rparen()?;
                    Ok(Expr::FnCall { name, args })
                } else {
                    // Bare identifier treated as a string literal
                    Ok(Expr::Literal(ExprValue::Str(name)))
                }
            }
            Token::LParen => {
                let expr = self.parse_expr()?;
                self.expect_rparen()?;
                Ok(expr)
            }
            tok => Err(format!("unexpected token: {tok:?}")),
        }
    }
}

/// Parse an expression string into an AST.
pub fn parse_expression(input: &str) -> Result<Expr, String> {
    let mut parser = Parser::from_str(input)?;
    let expr = parser.parse_expr()?;
    if !matches!(parser.peek(), Token::Eof) {
        return Err(format!(
            "unexpected trailing tokens at position {}",
            parser.pos
        ));
    }
    Ok(expr)
}

// =============================================================================
// Evaluator
// =============================================================================

/// Evaluate an expression against a row of field/value pairs.
pub fn evaluate(expr: &Expr, row: &Row) -> ExprValue {
    match expr {
        Expr::Field(name) => {
            row.iter()
                .find(|(k, _)| k == name)
                .map(|(_, v)| {
                    // Try to parse as number first
                    if let Ok(n) = v.parse::<f64>() {
                        ExprValue::Number(n)
                    } else {
                        ExprValue::Str(v.clone())
                    }
                })
                .unwrap_or(ExprValue::Null)
        }
        Expr::Literal(val) => val.clone(),
        Expr::BinaryOp { op, left, right } => {
            let lv = evaluate(left, row);
            let rv = evaluate(right, row);
            eval_binop(*op, &lv, &rv)
        }
        Expr::UnaryOp { op, operand } => {
            let v = evaluate(operand, row);
            match op {
                UnOp::Neg => match v.as_f64() {
                    Some(n) => ExprValue::Number(-n),
                    None => ExprValue::Null,
                },
                UnOp::Not => ExprValue::Bool(!v.is_truthy()),
            }
        }
        Expr::FnCall { name, args } => eval_function(name, args, row),
    }
}

fn eval_binop(op: BinOp, lv: &ExprValue, rv: &ExprValue) -> ExprValue {
    match op {
        BinOp::And => ExprValue::Bool(lv.is_truthy() && rv.is_truthy()),
        BinOp::Or => ExprValue::Bool(lv.is_truthy() || rv.is_truthy()),
        BinOp::Eq => ExprValue::Bool(values_equal(lv, rv)),
        BinOp::Ne => ExprValue::Bool(!values_equal(lv, rv)),
        BinOp::Add | BinOp::Sub | BinOp::Mul | BinOp::Div | BinOp::Mod => {
            // Try string concatenation for Add when either side is a non-numeric string
            if op == BinOp::Add
                && let (None, _) | (_, None) = (lv.as_f64(), rv.as_f64())
            {
                return ExprValue::Str(format!("{}{}", lv.as_string(), rv.as_string()));
            }
            match (lv.as_f64(), rv.as_f64()) {
                (Some(a), Some(b)) => {
                    let result = match op {
                        BinOp::Add => a + b,
                        BinOp::Sub => a - b,
                        BinOp::Mul => a * b,
                        BinOp::Div => {
                            if b == 0.0 {
                                return ExprValue::Null;
                            }
                            a / b
                        }
                        BinOp::Mod => {
                            if b == 0.0 {
                                return ExprValue::Null;
                            }
                            a % b
                        }
                        _ => unreachable!(),
                    };
                    ExprValue::Number(result)
                }
                _ => ExprValue::Null,
            }
        }
        BinOp::Lt | BinOp::Gt | BinOp::Le | BinOp::Ge => {
            match (lv.as_f64(), rv.as_f64()) {
                (Some(a), Some(b)) => {
                    let result = match op {
                        BinOp::Lt => a < b,
                        BinOp::Gt => a > b,
                        BinOp::Le => a <= b,
                        BinOp::Ge => a >= b,
                        _ => unreachable!(),
                    };
                    ExprValue::Bool(result)
                }
                _ => {
                    // String comparison fallback
                    let a = lv.as_string();
                    let b = rv.as_string();
                    let result = match op {
                        BinOp::Lt => a < b,
                        BinOp::Gt => a > b,
                        BinOp::Le => a <= b,
                        BinOp::Ge => a >= b,
                        _ => unreachable!(),
                    };
                    ExprValue::Bool(result)
                }
            }
        }
    }
}

fn values_equal(a: &ExprValue, b: &ExprValue) -> bool {
    match (a.as_f64(), b.as_f64()) {
        (Some(fa), Some(fb)) => (fa - fb).abs() < f64::EPSILON,
        _ => a.as_string() == b.as_string(),
    }
}

fn eval_function(name: &str, args: &[Expr], row: &Row) -> ExprValue {
    let evaled: Vec<ExprValue> = args.iter().map(|a| evaluate(a, row)).collect();
    match name.to_lowercase().as_str() {
        // String functions
        "upper" => evaled
            .first()
            .map(|v| ExprValue::Str(v.as_string().to_uppercase()))
            .unwrap_or(ExprValue::Null),
        "lower" => evaled
            .first()
            .map(|v| ExprValue::Str(v.as_string().to_lowercase()))
            .unwrap_or(ExprValue::Null),
        "strlen" => evaled
            .first()
            .map(|v| ExprValue::Number(v.as_string().len() as f64))
            .unwrap_or(ExprValue::Null),
        "substr" => {
            if evaled.len() < 3 {
                return ExprValue::Null;
            }
            let s = evaled[0].as_string();
            let offset = evaled[1].as_f64().unwrap_or(0.0) as usize;
            let count = evaled[2].as_f64().unwrap_or(0.0) as usize;
            let chars: Vec<char> = s.chars().collect();
            let end = (offset + count).min(chars.len());
            let start = offset.min(chars.len());
            ExprValue::Str(chars[start..end].iter().collect())
        }
        "contains" => {
            if evaled.len() < 2 {
                return ExprValue::Bool(false);
            }
            ExprValue::Bool(evaled[0].as_string().contains(&evaled[1].as_string()))
        }
        "startswith" => {
            if evaled.len() < 2 {
                return ExprValue::Bool(false);
            }
            ExprValue::Bool(evaled[0].as_string().starts_with(&evaled[1].as_string()))
        }
        "format" => {
            // Simple format: format("%s-%s", a, b)
            if evaled.is_empty() {
                return ExprValue::Null;
            }
            let fmt = evaled[0].as_string();
            let mut result = fmt;
            for arg in &evaled[1..] {
                if let Some(pos) = result.find("%s") {
                    result = format!(
                        "{}{}{}",
                        &result[..pos],
                        arg.as_string(),
                        &result[pos + 2..]
                    );
                }
            }
            ExprValue::Str(result)
        }
        "split" => {
            // split(str, sep) -> returns first element (no array type)
            if evaled.len() < 2 {
                return ExprValue::Null;
            }
            let s = evaled[0].as_string();
            let sep = evaled[1].as_string();
            ExprValue::Str(s.split(&sep).next().unwrap_or("").to_string())
        }
        // Math functions
        "log" => evaled
            .first()
            .and_then(|v| v.as_f64())
            .map(|n| ExprValue::Number(n.ln()))
            .unwrap_or(ExprValue::Null),
        "log2" => evaled
            .first()
            .and_then(|v| v.as_f64())
            .map(|n| ExprValue::Number(n.log2()))
            .unwrap_or(ExprValue::Null),
        "exp" => evaled
            .first()
            .and_then(|v| v.as_f64())
            .map(|n| ExprValue::Number(n.exp()))
            .unwrap_or(ExprValue::Null),
        "ceil" => evaled
            .first()
            .and_then(|v| v.as_f64())
            .map(|n| ExprValue::Number(n.ceil()))
            .unwrap_or(ExprValue::Null),
        "floor" => evaled
            .first()
            .and_then(|v| v.as_f64())
            .map(|n| ExprValue::Number(n.floor()))
            .unwrap_or(ExprValue::Null),
        "abs" => evaled
            .first()
            .and_then(|v| v.as_f64())
            .map(|n| ExprValue::Number(n.abs()))
            .unwrap_or(ExprValue::Null),
        "sqrt" => evaled
            .first()
            .and_then(|v| v.as_f64())
            .map(|n| ExprValue::Number(n.sqrt()))
            .unwrap_or(ExprValue::Null),
        // Utility functions
        "exists" => {
            // exists(@field) — check if field is present in row
            if let Some(Expr::Field(field_name)) = args.first() {
                ExprValue::Bool(row.iter().any(|(k, _)| k == field_name))
            } else {
                // Fallback: check if the value is non-null
                evaled
                    .first()
                    .map(|v| ExprValue::Bool(!matches!(v, ExprValue::Null)))
                    .unwrap_or(ExprValue::Bool(false))
            }
        }
        "timefmt" => {
            // timefmt(unix_timestamp) -> formatted string
            evaled
                .first()
                .and_then(|v| v.as_f64())
                .map(|ts| ExprValue::Str(format!("{}", ts as i64)))
                .unwrap_or(ExprValue::Null)
        }
        "parsetime" => {
            // parsetime(str) -> unix timestamp (best effort)
            evaled
                .first()
                .and_then(|v| v.as_string().parse::<f64>().ok())
                .map(ExprValue::Number)
                .unwrap_or(ExprValue::Null)
        }
        _ => ExprValue::Null,
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn make_row(fields: &[(&str, &str)]) -> Row {
        fields
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn test_parse_field() {
        let expr = parse_expression("@name").unwrap();
        assert_eq!(expr, Expr::Field("name".into()));
    }

    #[test]
    fn test_parse_number() {
        let expr = parse_expression("42").unwrap();
        assert_eq!(expr, Expr::Literal(ExprValue::Number(42.0)));
    }

    #[test]
    fn test_parse_string() {
        let expr = parse_expression("\"hello\"").unwrap();
        assert_eq!(expr, Expr::Literal(ExprValue::Str("hello".into())));
    }

    #[test]
    fn test_parse_bool() {
        assert_eq!(
            parse_expression("true").unwrap(),
            Expr::Literal(ExprValue::Bool(true))
        );
        assert_eq!(
            parse_expression("false").unwrap(),
            Expr::Literal(ExprValue::Bool(false))
        );
    }

    #[test]
    fn test_parse_arithmetic() {
        let expr = parse_expression("@a + @b * 2").unwrap();
        // Should be @a + (@b * 2) due to precedence
        assert!(matches!(expr, Expr::BinaryOp { op: BinOp::Add, .. }));
        if let Expr::BinaryOp { right, .. } = &expr {
            assert!(matches!(
                right.as_ref(),
                Expr::BinaryOp { op: BinOp::Mul, .. }
            ));
        }
    }

    #[test]
    fn test_parse_comparison() {
        let expr = parse_expression("@age >= 18").unwrap();
        assert!(matches!(expr, Expr::BinaryOp { op: BinOp::Ge, .. }));
    }

    #[test]
    fn test_parse_logical() {
        let expr = parse_expression("@a > 0 && @b > 0").unwrap();
        assert!(matches!(expr, Expr::BinaryOp { op: BinOp::And, .. }));
    }

    #[test]
    fn test_parse_function_call() {
        let expr = parse_expression("upper(@name)").unwrap();
        assert!(matches!(expr, Expr::FnCall { .. }));
    }

    #[test]
    fn test_parse_nested_parens() {
        let expr = parse_expression("(@a + @b) * @c").unwrap();
        assert!(matches!(expr, Expr::BinaryOp { op: BinOp::Mul, .. }));
    }

    #[test]
    fn test_parse_unary_neg() {
        let expr = parse_expression("-@a").unwrap();
        assert!(matches!(expr, Expr::UnaryOp { op: UnOp::Neg, .. }));
    }

    #[test]
    fn test_parse_unary_not() {
        let expr = parse_expression("!@flag").unwrap();
        assert!(matches!(expr, Expr::UnaryOp { op: UnOp::Not, .. }));
    }

    #[test]
    fn test_eval_field_lookup() {
        let row = make_row(&[("name", "Alice"), ("age", "30")]);
        assert_eq!(
            evaluate(&Expr::Field("name".into()), &row),
            ExprValue::Str("Alice".into())
        );
        assert_eq!(
            evaluate(&Expr::Field("age".into()), &row),
            ExprValue::Number(30.0)
        );
        assert_eq!(
            evaluate(&Expr::Field("missing".into()), &row),
            ExprValue::Null
        );
    }

    #[test]
    fn test_eval_arithmetic() {
        let row = make_row(&[("a", "10"), ("b", "3")]);
        let expr = parse_expression("@a + @b").unwrap();
        assert_eq!(evaluate(&expr, &row), ExprValue::Number(13.0));

        let expr = parse_expression("@a * @b - 5").unwrap();
        assert_eq!(evaluate(&expr, &row), ExprValue::Number(25.0));

        let expr = parse_expression("@a / @b").unwrap();
        let result = evaluate(&expr, &row);
        if let ExprValue::Number(n) = result {
            assert!((n - 3.333333333).abs() < 0.001);
        } else {
            panic!("expected number");
        }

        let expr = parse_expression("@a % @b").unwrap();
        assert_eq!(evaluate(&expr, &row), ExprValue::Number(1.0));
    }

    #[test]
    fn test_eval_comparison() {
        let row = make_row(&[("age", "25")]);
        assert_eq!(
            evaluate(&parse_expression("@age >= 18").unwrap(), &row),
            ExprValue::Bool(true)
        );
        assert_eq!(
            evaluate(&parse_expression("@age < 18").unwrap(), &row),
            ExprValue::Bool(false)
        );
        assert_eq!(
            evaluate(&parse_expression("@age == 25").unwrap(), &row),
            ExprValue::Bool(true)
        );
        assert_eq!(
            evaluate(&parse_expression("@age != 25").unwrap(), &row),
            ExprValue::Bool(false)
        );
    }

    #[test]
    fn test_eval_logical() {
        let row = make_row(&[("a", "1"), ("b", "0")]);
        assert_eq!(
            evaluate(&parse_expression("@a && @b").unwrap(), &row),
            ExprValue::Bool(false)
        );
        assert_eq!(
            evaluate(&parse_expression("@a || @b").unwrap(), &row),
            ExprValue::Bool(true)
        );
    }

    #[test]
    fn test_eval_function_upper_lower() {
        let row = make_row(&[("name", "Alice")]);
        assert_eq!(
            evaluate(&parse_expression("upper(@name)").unwrap(), &row),
            ExprValue::Str("ALICE".into())
        );
        assert_eq!(
            evaluate(&parse_expression("lower(@name)").unwrap(), &row),
            ExprValue::Str("alice".into())
        );
    }

    #[test]
    fn test_eval_function_strlen() {
        let row = make_row(&[("name", "Alice")]);
        assert_eq!(
            evaluate(&parse_expression("strlen(@name)").unwrap(), &row),
            ExprValue::Number(5.0)
        );
    }

    #[test]
    fn test_eval_function_substr() {
        let row = make_row(&[("s", "hello world")]);
        assert_eq!(
            evaluate(&parse_expression("substr(@s, 0, 5)").unwrap(), &row),
            ExprValue::Str("hello".into())
        );
    }

    #[test]
    fn test_eval_function_contains() {
        let row = make_row(&[("s", "hello world")]);
        assert_eq!(
            evaluate(&parse_expression("contains(@s, \"world\")").unwrap(), &row),
            ExprValue::Bool(true)
        );
        assert_eq!(
            evaluate(&parse_expression("contains(@s, \"xyz\")").unwrap(), &row),
            ExprValue::Bool(false)
        );
    }

    #[test]
    fn test_eval_function_math() {
        let row = make_row(&[("x", "4")]);
        assert_eq!(
            evaluate(&parse_expression("sqrt(@x)").unwrap(), &row),
            ExprValue::Number(2.0)
        );
        assert_eq!(
            evaluate(&parse_expression("abs(-5)").unwrap(), &row),
            ExprValue::Number(5.0)
        );
        assert_eq!(
            evaluate(&parse_expression("ceil(3.2)").unwrap(), &row),
            ExprValue::Number(4.0)
        );
        assert_eq!(
            evaluate(&parse_expression("floor(3.8)").unwrap(), &row),
            ExprValue::Number(3.0)
        );
    }

    #[test]
    fn test_eval_exists() {
        let row = make_row(&[("name", "Alice")]);
        let expr = parse_expression("exists(@name)").unwrap();
        assert_eq!(evaluate(&expr, &row), ExprValue::Bool(true));
        let expr = parse_expression("exists(@missing)").unwrap();
        assert_eq!(evaluate(&expr, &row), ExprValue::Bool(false));
    }

    #[test]
    fn test_eval_division_by_zero() {
        let row = make_row(&[("a", "10"), ("b", "0")]);
        assert_eq!(
            evaluate(&parse_expression("@a / @b").unwrap(), &row),
            ExprValue::Null
        );
        assert_eq!(
            evaluate(&parse_expression("@a % @b").unwrap(), &row),
            ExprValue::Null
        );
    }

    #[test]
    fn test_eval_null_propagation() {
        let row = make_row(&[("a", "10")]);
        // Missing field in arithmetic: Null.as_f64() = None, so falls through to string concat
        // @missing = Null, Null.as_string() = "", so "" + "5" = "5"
        assert_eq!(
            evaluate(&parse_expression("@missing + 5").unwrap(), &row),
            ExprValue::Str("5".into())
        );
        // Subtraction with null returns Null (no string fallback)
        assert_eq!(
            evaluate(&parse_expression("@missing - 5").unwrap(), &row),
            ExprValue::Null
        );
    }

    #[test]
    fn test_eval_string_concat() {
        let row = make_row(&[("first", "hello"), ("last", "world")]);
        assert_eq!(
            evaluate(&parse_expression("@first + @last").unwrap(), &row),
            ExprValue::Str("helloworld".into())
        );
    }

    #[test]
    fn test_precedence_chain() {
        let row = make_row(&[]);
        // 2 + 3 * 4 = 14 (not 20)
        assert_eq!(
            evaluate(&parse_expression("2 + 3 * 4").unwrap(), &row),
            ExprValue::Number(14.0)
        );
        // (2 + 3) * 4 = 20
        assert_eq!(
            evaluate(&parse_expression("(2 + 3) * 4").unwrap(), &row),
            ExprValue::Number(20.0)
        );
    }

    #[test]
    fn test_complex_expression() {
        let row = make_row(&[("price", "100"), ("qty", "5"), ("tax", "0.1")]);
        let expr = parse_expression("@price * @qty * (1 + @tax)").unwrap();
        let result = evaluate(&expr, &row);
        if let ExprValue::Number(n) = result {
            assert!((n - 550.0).abs() < 0.001);
        } else {
            panic!("expected number");
        }
    }

    #[test]
    fn test_format_function() {
        let row = make_row(&[("first", "John"), ("last", "Doe")]);
        let expr = parse_expression("format(\"%s %s\", @first, @last)").unwrap();
        assert_eq!(evaluate(&expr, &row), ExprValue::Str("John Doe".into()));
    }
}
