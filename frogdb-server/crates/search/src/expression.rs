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
                    if self.input[self.pos] == b'\\' && self.pos + 1 < self.input.len() {
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

const MAX_EXPR_DEPTH: usize = 64;

struct Parser<'a> {
    tokens: Vec<Token>,
    pos: usize,
    depth: usize,
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
            depth: 0,
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
        self.depth += 1;
        if self.depth > MAX_EXPR_DEPTH {
            return Err("Expression exceeds maximum nesting depth".to_string());
        }
        let result = self.parse_prec(0);
        self.depth -= 1;
        result
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
            let ts = evaled.first().and_then(|v| v.as_f64());
            match (ts, evaled.get(1)) {
                (Some(ts), None) => {
                    // Default: ISO-8601 format
                    let (y, m, d, h, min, s) = unix_to_components(ts);
                    ExprValue::Str(format!("{y:04}-{m:02}-{d:02}T{h:02}:{min:02}:{s:02}Z"))
                }
                (Some(ts), Some(fmt_val)) => {
                    ExprValue::Str(format_strftime(ts, &fmt_val.as_string()))
                }
                _ => ExprValue::Null,
            }
        }
        "parsetime" => {
            // parsetime(str) -> unix timestamp (best effort)
            evaled
                .first()
                .and_then(|v| v.as_string().parse::<f64>().ok())
                .map(ExprValue::Number)
                .unwrap_or(ExprValue::Null)
        }
        // Date/time extraction functions (take unix timestamp, return component)
        "year" => evaled
            .first()
            .and_then(|v| v.as_f64())
            .map(|ts| ExprValue::Number(unix_to_components(ts).0 as f64))
            .unwrap_or(ExprValue::Null),
        "month" | "monthofyear" => evaled
            .first()
            .and_then(|v| v.as_f64())
            .map(|ts| ExprValue::Number(unix_to_components(ts).1 as f64))
            .unwrap_or(ExprValue::Null),
        "day" | "dayofmonth" => evaled
            .first()
            .and_then(|v| v.as_f64())
            .map(|ts| ExprValue::Number(unix_to_components(ts).2 as f64))
            .unwrap_or(ExprValue::Null),
        "hour" => evaled
            .first()
            .and_then(|v| v.as_f64())
            .map(|ts| ExprValue::Number(unix_to_components(ts).3 as f64))
            .unwrap_or(ExprValue::Null),
        "minute" => evaled
            .first()
            .and_then(|v| v.as_f64())
            .map(|ts| ExprValue::Number(unix_to_components(ts).4 as f64))
            .unwrap_or(ExprValue::Null),
        "dayofweek" => evaled
            .first()
            .and_then(|v| v.as_f64())
            .map(|ts| ExprValue::Number(day_of_week(ts) as f64))
            .unwrap_or(ExprValue::Null),
        "dayofyear" => evaled
            .first()
            .and_then(|v| v.as_f64())
            .map(|ts| ExprValue::Number(day_of_year(ts) as f64))
            .unwrap_or(ExprValue::Null),
        // Type conversion functions
        "to_number" => evaled
            .first()
            .and_then(|v| v.as_f64())
            .map(ExprValue::Number)
            .unwrap_or(ExprValue::Null),
        "to_str" => evaled
            .first()
            .map(|v| ExprValue::Str(v.as_string()))
            .unwrap_or(ExprValue::Null),
        // Geo distance (haversine)
        "geodistance" => {
            match evaled.len() {
                2 => {
                    // Two string args: "lon1,lat1" "lon2,lat2"
                    let parse_coord = |v: &ExprValue| -> Option<(f64, f64)> {
                        let s = v.as_string();
                        let parts: Vec<&str> = s.split(',').collect();
                        if parts.len() == 2 {
                            Some((parts[0].trim().parse().ok()?, parts[1].trim().parse().ok()?))
                        } else {
                            None
                        }
                    };
                    match (parse_coord(&evaled[0]), parse_coord(&evaled[1])) {
                        (Some((lon1, lat1)), Some((lon2, lat2))) => {
                            ExprValue::Number(haversine_distance(lon1, lat1, lon2, lat2))
                        }
                        _ => ExprValue::Null,
                    }
                }
                4 => {
                    // Four numeric args: lon1 lat1 lon2 lat2
                    match (
                        evaled[0].as_f64(),
                        evaled[1].as_f64(),
                        evaled[2].as_f64(),
                        evaled[3].as_f64(),
                    ) {
                        (Some(lon1), Some(lat1), Some(lon2), Some(lat2)) => {
                            ExprValue::Number(haversine_distance(lon1, lat1, lon2, lat2))
                        }
                        _ => ExprValue::Null,
                    }
                }
                _ => ExprValue::Null,
            }
        }
        _ => ExprValue::Null,
    }
}

/// Decompose a Unix timestamp (seconds since epoch) into civil date/time components.
/// Uses Howard Hinnant's algorithm for days → civil date.
fn unix_to_components(ts: f64) -> (i32, u32, u32, u32, u32, u32) {
    let secs = ts as i64;
    let day_secs = secs.rem_euclid(86400);
    let hour = (day_secs / 3600) as u32;
    let minute = ((day_secs % 3600) / 60) as u32;
    let second = (day_secs % 60) as u32;
    let days = secs.div_euclid(86400);
    let (year, month, day) = civil_from_days(days);
    (year, month, day, hour, minute, second)
}

/// Howard Hinnant's civil_from_days algorithm.
/// Converts days since Unix epoch (1970-01-01) to (year, month, day).
fn civil_from_days(days: i64) -> (i32, u32, u32) {
    let z = days + 719468;
    let era = (if z >= 0 { z } else { z - 146096 }) / 146097;
    let doe = (z - era * 146097) as u32; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365; // [0, 399]
    let y = (yoe as i64 + era * 400) as i32;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = doy - (153 * mp + 2) / 5 + 1; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 }; // [1, 12]
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

/// Day of week: 0=Sunday..6=Saturday.
fn day_of_week(ts: f64) -> u32 {
    // 1970-01-01 was a Thursday (4)
    let days = (ts as i64).div_euclid(86400);
    ((days + 4).rem_euclid(7)) as u32
}

/// Day of year: 1-366.
fn day_of_year(ts: f64) -> u32 {
    let (year, month, day) = civil_from_days((ts as i64).div_euclid(86400));
    let is_leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    let month_days = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    let mut doy: u32 = day;
    for &md in &month_days[1..month as usize] {
        doy += md;
    }
    if is_leap && month > 2 {
        doy += 1;
    }
    doy
}

/// Week of year, Sunday-based (0–53). Week 0 contains days before the first Sunday.
fn week_of_year_sunday(ts: f64) -> u32 {
    let doy = day_of_year(ts); // 1-based
    let dow = day_of_week(ts); // 0=Sunday
    (doy + 6 - dow) / 7
}

/// Week of year, Monday-based (0–53). Week 0 contains days before the first Monday.
fn week_of_year_monday(ts: f64) -> u32 {
    let doy = day_of_year(ts); // 1-based
    let dow = day_of_week(ts); // 0=Sunday
    let dow_mon = if dow == 0 { 6 } else { dow - 1 }; // 0=Monday
    (doy + 6 - dow_mon) / 7
}

/// Format a Unix timestamp using strftime-style format codes.
fn format_strftime(ts: f64, fmt: &str) -> String {
    let (y, mo, d, h, min, s) = unix_to_components(ts);
    let dow = day_of_week(ts);
    let doy = day_of_year(ts);

    let day_abbr = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
    let day_full = [
        "Sunday",
        "Monday",
        "Tuesday",
        "Wednesday",
        "Thursday",
        "Friday",
        "Saturday",
    ];
    let mon_abbr = [
        "", "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ];
    let mon_full = [
        "",
        "January",
        "February",
        "March",
        "April",
        "May",
        "June",
        "July",
        "August",
        "September",
        "October",
        "November",
        "December",
    ];

    let bytes = fmt.as_bytes();
    let mut result = String::with_capacity(fmt.len() * 2);
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 1 < bytes.len() {
            i += 1;
            match bytes[i] {
                b'%' => result.push('%'),
                b'Y' => result.push_str(&format!("{y:04}")),
                b'y' => result.push_str(&format!("{:02}", (y % 100).unsigned_abs())),
                b'm' => result.push_str(&format!("{mo:02}")),
                b'd' => result.push_str(&format!("{d:02}")),
                b'e' => result.push_str(&format!("{d:2}")),
                b'H' => result.push_str(&format!("{h:02}")),
                b'I' => {
                    let h12 = match h % 12 {
                        0 => 12,
                        other => other,
                    };
                    result.push_str(&format!("{h12:02}"));
                }
                b'M' => result.push_str(&format!("{min:02}")),
                b'S' => result.push_str(&format!("{s:02}")),
                b'a' => result.push_str(day_abbr[dow as usize]),
                b'A' => result.push_str(day_full[dow as usize]),
                b'b' => result.push_str(mon_abbr[mo as usize]),
                b'B' => result.push_str(mon_full[mo as usize]),
                b'j' => result.push_str(&format!("{doy:03}")),
                b'p' => result.push_str(if h < 12 { "AM" } else { "PM" }),
                b'P' => result.push_str(if h < 12 { "am" } else { "pm" }),
                b'w' => result.push_str(&format!("{dow}")),
                b'u' => {
                    let iso_dow = if dow == 0 { 7 } else { dow };
                    result.push_str(&format!("{iso_dow}"));
                }
                b'U' => result.push_str(&format!("{:02}", week_of_year_sunday(ts))),
                b'W' => result.push_str(&format!("{:02}", week_of_year_monday(ts))),
                b'D' => {
                    // %m/%d/%y
                    result.push_str(&format!("{mo:02}/{d:02}/{:02}", (y % 100).unsigned_abs()));
                }
                b'F' => {
                    // %Y-%m-%d
                    result.push_str(&format!("{y:04}-{mo:02}-{d:02}"));
                }
                b'T' => {
                    // %H:%M:%S
                    result.push_str(&format!("{h:02}:{min:02}:{s:02}"));
                }
                b'R' => {
                    // %H:%M
                    result.push_str(&format!("{h:02}:{min:02}"));
                }
                b's' => {
                    result.push_str(&format!("{}", ts as i64));
                }
                b'n' => result.push('\n'),
                b't' => result.push('\t'),
                b'z' => result.push_str("+0000"),
                b'Z' => result.push_str("UTC"),
                other => {
                    // Unknown code: pass through as-is
                    result.push('%');
                    result.push(other as char);
                }
            }
        } else {
            result.push(bytes[i] as char);
        }
        i += 1;
    }
    result
}

/// Haversine distance in meters between two (lon, lat) points.
fn haversine_distance(lon1: f64, lat1: f64, lon2: f64, lat2: f64) -> f64 {
    const R: f64 = 6_371_000.0;
    let dlat = (lat2 - lat1).to_radians();
    let dlon = (lon2 - lon1).to_radians();
    let lat1_r = lat1.to_radians();
    let lat2_r = lat2.to_radians();
    let a = (dlat / 2.0).sin().powi(2) + lat1_r.cos() * lat2_r.cos() * (dlon / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().asin();
    R * c
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

    #[test]
    fn test_timefmt_strftime_basic() {
        // 2024-01-15 09:30:45 UTC = 1705311045
        let ts = 1705311045.0;
        let row = make_row(&[("ts", "1705311045")]);

        // %F = %Y-%m-%d
        let expr = parse_expression("timefmt(@ts, \"%F\")").unwrap();
        assert_eq!(evaluate(&expr, &row), ExprValue::Str("2024-01-15".into()));

        // %T = %H:%M:%S
        let expr = parse_expression("timefmt(@ts, \"%T\")").unwrap();
        assert_eq!(evaluate(&expr, &row), ExprValue::Str("09:30:45".into()));

        // %R = %H:%M
        let expr = parse_expression("timefmt(@ts, \"%R\")").unwrap();
        assert_eq!(evaluate(&expr, &row), ExprValue::Str("09:30".into()));

        // %D = %m/%d/%y
        let expr = parse_expression("timefmt(@ts, \"%D\")").unwrap();
        assert_eq!(evaluate(&expr, &row), ExprValue::Str("01/15/24".into()));

        // Verify format_strftime directly for more codes
        assert_eq!(format_strftime(ts, "%y"), "24");
        assert_eq!(format_strftime(ts, "%e"), "15");
        assert_eq!(format_strftime(ts, "%I"), "09");
        assert_eq!(format_strftime(ts, "%p"), "AM");
        assert_eq!(format_strftime(ts, "%P"), "am");
        assert_eq!(format_strftime(ts, "%A"), "Monday");
        assert_eq!(format_strftime(ts, "%B"), "January");
        assert_eq!(format_strftime(ts, "%w"), "1"); // Monday
        assert_eq!(format_strftime(ts, "%u"), "1"); // Monday (ISO)
        assert_eq!(format_strftime(ts, "%z"), "+0000");
        assert_eq!(format_strftime(ts, "%Z"), "UTC");
        assert_eq!(format_strftime(ts, "%n"), "\n");
        assert_eq!(format_strftime(ts, "%t"), "\t");
        assert_eq!(format_strftime(ts, "%s"), "1705311045");
    }

    #[test]
    fn test_timefmt_percent_escape() {
        // %% should produce a literal %
        assert_eq!(format_strftime(0.0, "100%%"), "100%");
        // %%Y should produce %Y (not the year)
        assert_eq!(format_strftime(0.0, "%%Y"), "%Y");
    }

    #[test]
    fn test_timefmt_12hour_clock() {
        // Midnight = hour 0 => 12 AM
        assert_eq!(format_strftime(0.0, "%I %p"), "12 AM");
        // 13:00 => 01 PM
        assert_eq!(format_strftime(46800.0, "%I %p"), "01 PM");
    }

    #[test]
    fn test_timefmt_week_of_year() {
        // 2024-01-01 is a Monday
        let jan1 = 1704067200.0; // 2024-01-01 00:00:00 UTC
        assert_eq!(format_strftime(jan1, "%U"), "00"); // Sunday-based: before first Sunday
        assert_eq!(format_strftime(jan1, "%W"), "01"); // Monday-based: is a Monday
    }

    #[test]
    fn test_timefmt_sunday_weekday() {
        // 2024-01-07 is a Sunday
        let sun = 1704585600.0; // 2024-01-07 00:00:00 UTC
        assert_eq!(format_strftime(sun, "%w"), "0"); // Sunday = 0
        assert_eq!(format_strftime(sun, "%u"), "7"); // Sunday = 7 (ISO)
    }

    #[test]
    fn test_expr_max_depth_parens() {
        // 65+ nested parentheses should return an error, not stack overflow
        let mut input = String::new();
        for _ in 0..65 {
            input.push('(');
        }
        input.push('1');
        for _ in 0..65 {
            input.push(')');
        }
        let result = parse_expression(&input);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("maximum nesting depth"));
    }

    #[test]
    fn test_expr_max_depth_unary() {
        // 65+ nested unary NOT should return an error, not stack overflow
        let mut input = String::new();
        for _ in 0..65 {
            input.push_str("!(");
        }
        input.push_str("true");
        for _ in 0..65 {
            input.push(')');
        }
        let result = parse_expression(&input);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("maximum nesting depth"));
    }

    #[test]
    fn test_trailing_backslash_in_quoted_string_no_panic() {
        // Regression test for fuzzer crash: backslash as the last byte inside
        // an unterminated quoted string caused a slice index out-of-bounds.
        let input = "TBYFI'SORTBYR-\\";
        let result = parse_expression(input);
        // Should not panic; the result (Ok or Err) doesn't matter.
        let _ = result;
    }
}
