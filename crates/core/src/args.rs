//! Argument parser for Redis-style command arguments.
//!
//! Provides a declarative API for parsing command arguments, replacing manual
//! `while i < args.len()` loops with a cleaner, more maintainable abstraction.
//!
//! # Example
//!
//! ```rust,ignore
//! use frogdb_core::args::ArgParser;
//!
//! // Parse SCAN command: SCAN cursor [MATCH pattern] [COUNT count] [TYPE type]
//! let mut parser = ArgParser::new(&args[1..]); // Skip command key
//!
//! let mut pattern = None;
//! let mut count = 10usize;
//!
//! while let Some(opt) = parser.try_option()? {
//!     match opt.as_slice() {
//!         b"MATCH" => pattern = Some(parser.next_arg()?),
//!         b"COUNT" => count = parser.next_parsed()?,
//!         _ => return Err(CommandError::SyntaxError),
//!     }
//! }
//! ```

use bytes::Bytes;
use std::str::FromStr;

use crate::CommandError;

/// Parser for Redis-style command arguments.
///
/// Provides methods for consuming arguments sequentially, checking for flags,
/// and parsing typed values.
#[derive(Debug)]
pub struct ArgParser<'a> {
    args: &'a [Bytes],
    pos: usize,
}

impl<'a> ArgParser<'a> {
    /// Create a new argument parser starting at the beginning of the slice.
    pub fn new(args: &'a [Bytes]) -> Self {
        Self { args, pos: 0 }
    }

    /// Create a parser starting at a specific position.
    pub fn from_position(args: &'a [Bytes], pos: usize) -> Self {
        Self { args, pos }
    }

    /// Returns true if there are more arguments.
    #[inline]
    pub fn has_more(&self) -> bool {
        self.pos < self.args.len()
    }

    /// Returns the number of remaining arguments.
    #[inline]
    pub fn remaining_count(&self) -> usize {
        self.args.len().saturating_sub(self.pos)
    }

    /// Returns the current position in the argument list.
    #[inline]
    pub fn position(&self) -> usize {
        self.pos
    }

    /// Peek at the current argument without consuming it.
    pub fn peek(&self) -> Option<&'a Bytes> {
        self.args.get(self.pos)
    }

    /// Get the next argument, consuming it.
    ///
    /// Returns `None` if no more arguments.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Option<&'a Bytes> {
        if self.pos < self.args.len() {
            let arg = &self.args[self.pos];
            self.pos += 1;
            Some(arg)
        } else {
            None
        }
    }

    /// Get the next argument, returning a syntax error if missing.
    pub fn next_arg(&mut self) -> Result<&'a Bytes, CommandError> {
        self.next().ok_or(CommandError::SyntaxError)
    }

    /// Get the next argument as a parsed type.
    ///
    /// Returns `CommandError::SyntaxError` if missing, or the parse error.
    pub fn next_parsed<T>(&mut self) -> Result<T, CommandError>
    where
        T: FromStr,
        T::Err: std::fmt::Display,
    {
        let arg = self.next_arg()?;
        parse_from_bytes(arg)
    }

    /// Get the next argument as a u64.
    pub fn next_u64(&mut self) -> Result<u64, CommandError> {
        let arg = self.next_arg()?;
        parse_u64(arg)
    }

    /// Get the next argument as an i64.
    pub fn next_i64(&mut self) -> Result<i64, CommandError> {
        let arg = self.next_arg()?;
        parse_i64(arg)
    }

    /// Get the next argument as a usize.
    pub fn next_usize(&mut self) -> Result<usize, CommandError> {
        let arg = self.next_arg()?;
        parse_usize(arg)
    }

    /// Get the next argument as a f64.
    pub fn next_f64(&mut self) -> Result<f64, CommandError> {
        let arg = self.next_arg()?;
        parse_f64(arg)
    }

    /// Peek at the current argument as an uppercase option (case-insensitive).
    ///
    /// Returns the uppercase version of the current argument if it looks like
    /// an option (starts with a letter), without consuming it.
    ///
    /// Use this with `skip(1)` to consume after matching:
    /// ```rust,ignore
    /// while let Some(opt) = parser.peek_option() {
    ///     match opt.as_slice() {
    ///         b"NX" => { parser.skip(1); nx = true; }
    ///         b"EX" => { parser.skip(1); expire_secs = Some(parser.next_u64()?); }
    ///         _ => break, // Unknown option, don't consume
    ///     }
    /// }
    /// ```
    ///
    /// For simpler cases, prefer using `try_flag()` and `try_flag_value()` directly:
    /// ```rust,ignore
    /// while parser.has_more() {
    ///     if parser.try_flag(b"NX") {
    ///         nx = true;
    ///     } else if let Some(secs) = parser.try_flag_u64(b"EX")? {
    ///         expire_secs = Some(secs);
    ///     } else {
    ///         break; // Unknown option or start of data
    ///     }
    /// }
    /// ```
    pub fn peek_option(&self) -> Option<Bytes> {
        if let Some(arg) = self.peek() {
            let upper = arg.to_ascii_uppercase();
            // Check if it looks like an option (not a number or data)
            // Options typically start with a letter
            if !upper.is_empty() && upper[0].is_ascii_alphabetic() {
                return Some(Bytes::from(upper));
            }
        }
        None
    }

    /// Try to consume a specific flag (case-insensitive).
    ///
    /// Returns `true` and consumes the argument if it matches.
    pub fn try_flag(&mut self, flag: &[u8]) -> bool {
        if let Some(arg) = self.peek()
            && arg.eq_ignore_ascii_case(flag)
        {
            self.pos += 1;
            return true;
        }
        false
    }

    /// Try to consume a flag and its following value.
    ///
    /// Returns `Ok(Some(value))` if the flag is present, `Ok(None)` if not.
    /// Returns `Err(SyntaxError)` if the flag is present but value is missing.
    pub fn try_flag_value(&mut self, flag: &[u8]) -> Result<Option<&'a Bytes>, CommandError> {
        if self.try_flag(flag) {
            Ok(Some(self.next_arg()?))
        } else {
            Ok(None)
        }
    }

    /// Try to consume a flag and parse its following value.
    ///
    /// Returns `Ok(Some(value))` if the flag is present, `Ok(None)` if not.
    pub fn try_flag_parsed<T>(&mut self, flag: &[u8]) -> Result<Option<T>, CommandError>
    where
        T: FromStr,
        T::Err: std::fmt::Display,
    {
        if self.try_flag(flag) {
            Ok(Some(self.next_parsed()?))
        } else {
            Ok(None)
        }
    }

    /// Try to consume a flag and parse its value as u64.
    pub fn try_flag_u64(&mut self, flag: &[u8]) -> Result<Option<u64>, CommandError> {
        if self.try_flag(flag) {
            Ok(Some(self.next_u64()?))
        } else {
            Ok(None)
        }
    }

    /// Try to consume a flag and parse its value as i64.
    pub fn try_flag_i64(&mut self, flag: &[u8]) -> Result<Option<i64>, CommandError> {
        if self.try_flag(flag) {
            Ok(Some(self.next_i64()?))
        } else {
            Ok(None)
        }
    }

    /// Try to consume a flag and parse its value as usize.
    pub fn try_flag_usize(&mut self, flag: &[u8]) -> Result<Option<usize>, CommandError> {
        if self.try_flag(flag) {
            Ok(Some(self.next_usize()?))
        } else {
            Ok(None)
        }
    }

    /// Get all remaining arguments as a slice.
    pub fn remaining(&self) -> &'a [Bytes] {
        &self.args[self.pos..]
    }

    /// Consume all remaining arguments and return them.
    pub fn take_remaining(&mut self) -> &'a [Bytes] {
        let remaining = &self.args[self.pos..];
        self.pos = self.args.len();
        remaining
    }

    /// Skip the next n arguments.
    pub fn skip(&mut self, n: usize) {
        self.pos = (self.pos + n).min(self.args.len());
    }

    /// Reset the parser to a specific position.
    pub fn reset_to(&mut self, pos: usize) {
        self.pos = pos.min(self.args.len());
    }
}

/// Parse bytes as a string and then as a type.
///
/// Works with any type that can be viewed as a byte slice, including:
/// - `&[u8]`
/// - `&Bytes`
/// - `&Vec<u8>`
pub fn parse_from_bytes<T, B: AsRef<[u8]>>(bytes: B) -> Result<T, CommandError>
where
    T: FromStr,
    T::Err: std::fmt::Display,
{
    let s = std::str::from_utf8(bytes.as_ref()).map_err(|_| CommandError::SyntaxError)?;
    s.parse()
        .map_err(|e: T::Err| CommandError::InvalidArgument {
            message: e.to_string(),
        })
}

/// Parse bytes as u64.
///
/// Works with any type that can be viewed as a byte slice.
pub fn parse_u64<B: AsRef<[u8]>>(bytes: B) -> Result<u64, CommandError> {
    let s = std::str::from_utf8(bytes.as_ref()).map_err(|_| CommandError::NotInteger)?;
    s.parse().map_err(|_| CommandError::NotInteger)
}

/// Parse bytes as i64.
///
/// Works with any type that can be viewed as a byte slice.
pub fn parse_i64<B: AsRef<[u8]>>(bytes: B) -> Result<i64, CommandError> {
    let s = std::str::from_utf8(bytes.as_ref()).map_err(|_| CommandError::NotInteger)?;
    s.parse().map_err(|_| CommandError::NotInteger)
}

/// Parse bytes as usize.
///
/// Works with any type that can be viewed as a byte slice.
pub fn parse_usize<B: AsRef<[u8]>>(bytes: B) -> Result<usize, CommandError> {
    let s = std::str::from_utf8(bytes.as_ref()).map_err(|_| CommandError::NotInteger)?;
    s.parse().map_err(|_| CommandError::NotInteger)
}

/// Parse bytes as f64.
///
/// Works with any type that can be viewed as a byte slice.
///
/// Supports special values:
/// - "inf" or "+inf" -> f64::INFINITY
/// - "-inf" -> f64::NEG_INFINITY
pub fn parse_f64<B: AsRef<[u8]>>(bytes: B) -> Result<f64, CommandError> {
    let s = std::str::from_utf8(bytes.as_ref()).map_err(|_| CommandError::NotFloat)?;
    if s.eq_ignore_ascii_case("inf") || s.eq_ignore_ascii_case("+inf") {
        Ok(f64::INFINITY)
    } else if s.eq_ignore_ascii_case("-inf") {
        Ok(f64::NEG_INFINITY)
    } else {
        s.parse().map_err(|_| CommandError::NotFloat)
    }
}

/// Helper for MATCH/COUNT style scan options.
#[derive(Debug, Default)]
pub struct ScanOptions<'a> {
    pub pattern: Option<&'a [u8]>,
    pub count: usize,
}

impl<'a> ScanOptions<'a> {
    /// Parse common scan options (MATCH, COUNT) from a parser.
    ///
    /// Returns the options and whether parsing succeeded.
    pub fn parse(parser: &mut ArgParser<'a>) -> Result<Self, CommandError> {
        let mut opts = Self {
            pattern: None,
            count: 10, // Redis default
        };

        while parser.has_more() {
            if let Some(value) = parser.try_flag_value(b"MATCH")? {
                opts.pattern = Some(value.as_ref());
            } else if let Some(value) = parser.try_flag_usize(b"COUNT")? {
                opts.count = value;
            } else {
                // Unknown option
                return Err(CommandError::SyntaxError);
            }
        }

        Ok(opts)
    }
}

/// Helper for expiration options (EX, PX, EXAT, PXAT, KEEPTTL).
#[derive(Debug, Clone, PartialEq)]
pub enum ExpiryOption {
    /// Expiration in seconds from now.
    Ex(u64),
    /// Expiration in milliseconds from now.
    Px(u64),
    /// Expiration at Unix timestamp (seconds).
    ExAt(u64),
    /// Expiration at Unix timestamp (milliseconds).
    PxAt(u64),
    /// Keep existing TTL.
    KeepTtl,
    /// Persist (remove expiration).
    Persist,
}

impl ExpiryOption {
    /// Try to parse an expiration option from the parser.
    ///
    /// Returns `Ok(Some(option))` if an expiration flag was found and parsed,
    /// `Ok(None)` if no expiration flag was found, or an error if parsing failed.
    pub fn try_parse(parser: &mut ArgParser<'_>) -> Result<Option<Self>, CommandError> {
        if let Some(secs) = parser.try_flag_u64(b"EX")? {
            Ok(Some(Self::Ex(secs)))
        } else if let Some(ms) = parser.try_flag_u64(b"PX")? {
            Ok(Some(Self::Px(ms)))
        } else if let Some(ts) = parser.try_flag_u64(b"EXAT")? {
            Ok(Some(Self::ExAt(ts)))
        } else if let Some(ts) = parser.try_flag_u64(b"PXAT")? {
            Ok(Some(Self::PxAt(ts)))
        } else if parser.try_flag(b"KEEPTTL") {
            Ok(Some(Self::KeepTtl))
        } else if parser.try_flag(b"PERSIST") {
            Ok(Some(Self::Persist))
        } else {
            Ok(None)
        }
    }
}

/// Helper for conditional flags (NX, XX).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SetCondition {
    /// Set regardless of existence.
    #[default]
    Always,
    /// Only set if key does not exist.
    Nx,
    /// Only set if key exists.
    Xx,
}

impl SetCondition {
    /// Try to parse NX or XX from the parser.
    pub fn try_parse(parser: &mut ArgParser<'_>) -> Result<Self, CommandError> {
        if parser.try_flag(b"NX") {
            Ok(Self::Nx)
        } else if parser.try_flag(b"XX") {
            Ok(Self::Xx)
        } else {
            Ok(Self::Always)
        }
    }

    /// Try to parse NX or XX with conflict detection.
    ///
    /// Returns an error if both NX and XX are specified.
    pub fn try_parse_strict(
        parser: &mut ArgParser<'_>,
        existing: &mut Option<Self>,
    ) -> Result<bool, CommandError> {
        if parser.try_flag(b"NX") {
            if *existing == Some(Self::Xx) {
                return Err(CommandError::InvalidArgument {
                    message: "XX and NX options at the same time are not compatible".to_string(),
                });
            }
            *existing = Some(Self::Nx);
            Ok(true)
        } else if parser.try_flag(b"XX") {
            if *existing == Some(Self::Nx) {
                return Err(CommandError::InvalidArgument {
                    message: "XX and NX options at the same time are not compatible".to_string(),
                });
            }
            *existing = Some(Self::Xx);
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

/// Helper for comparison flags (GT, LT).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CompareCondition {
    /// No comparison constraint.
    #[default]
    None,
    /// Only update if new value is greater.
    Gt,
    /// Only update if new value is less.
    Lt,
}

impl CompareCondition {
    /// Try to parse GT or LT with conflict detection.
    pub fn try_parse_strict(
        parser: &mut ArgParser<'_>,
        existing: &mut Option<Self>,
    ) -> Result<bool, CommandError> {
        if parser.try_flag(b"GT") {
            if *existing == Some(Self::Lt) {
                return Err(CommandError::InvalidArgument {
                    message: "GT, LT, and NX options at the same time are not compatible"
                        .to_string(),
                });
            }
            *existing = Some(Self::Gt);
            Ok(true)
        } else if parser.try_flag(b"LT") {
            if *existing == Some(Self::Gt) {
                return Err(CommandError::InvalidArgument {
                    message: "GT, LT, and NX options at the same time are not compatible"
                        .to_string(),
                });
            }
            *existing = Some(Self::Lt);
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bytes_vec(strs: &[&'static str]) -> Vec<Bytes> {
        strs.iter()
            .map(|s| Bytes::from_static(s.as_bytes()))
            .collect()
    }

    #[test]
    fn test_basic_parsing() {
        let args = bytes_vec(&["key1", "key2", "value"]);
        let mut parser = ArgParser::new(&args);

        assert!(parser.has_more());
        assert_eq!(parser.remaining_count(), 3);

        assert_eq!(parser.next_arg().unwrap().as_ref(), b"key1");
        assert_eq!(parser.next_arg().unwrap().as_ref(), b"key2");
        assert_eq!(parser.remaining_count(), 1);

        let remaining = parser.take_remaining();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].as_ref(), b"value");

        assert!(!parser.has_more());
    }

    #[test]
    fn test_flag_parsing() {
        let args = bytes_vec(&["NX", "EX", "100", "other"]);
        let mut parser = ArgParser::new(&args);

        assert!(parser.try_flag(b"NX"));
        assert!(!parser.try_flag(b"XX")); // Not present
        assert!(parser.try_flag(b"ex")); // Case insensitive

        assert_eq!(parser.next_u64().unwrap(), 100);
        assert_eq!(parser.next_arg().unwrap().as_ref(), b"other");
    }

    #[test]
    fn test_flag_value_parsing() {
        let args = bytes_vec(&["MATCH", "user:*", "COUNT", "20"]);
        let mut parser = ArgParser::new(&args);

        let pattern = parser.try_flag_value(b"MATCH").unwrap();
        assert_eq!(pattern.unwrap().as_ref(), b"user:*");

        let count = parser.try_flag_usize(b"COUNT").unwrap();
        assert_eq!(count, Some(20));

        assert!(!parser.has_more());
    }

    #[test]
    fn test_peek_option_loop() {
        let args = bytes_vec(&["NX", "EX", "100", "myvalue"]);
        let mut parser = ArgParser::new(&args);

        let mut nx = false;
        let mut expire_secs = None;

        while let Some(opt) = parser.peek_option() {
            match opt.as_ref() {
                b"NX" => {
                    parser.skip(1);
                    nx = true;
                }
                b"EX" => {
                    parser.skip(1);
                    expire_secs = Some(parser.next_u64().unwrap());
                }
                _ => break, // Unknown option, don't consume
            }
        }

        assert!(nx);
        assert_eq!(expire_secs, Some(100));
        // "myvalue" should not be consumed since it's an unknown option
        assert_eq!(parser.remaining_count(), 1);
    }

    #[test]
    fn test_try_flag_loop() {
        // Alternative pattern using try_flag directly - recommended for most cases
        let args = bytes_vec(&["NX", "EX", "100", "myvalue"]);
        let mut parser = ArgParser::new(&args);

        let mut nx = false;
        let mut expire_secs = None;

        while parser.has_more() {
            if parser.try_flag(b"NX") {
                nx = true;
            } else if let Some(secs) = parser.try_flag_u64(b"EX").unwrap() {
                expire_secs = Some(secs);
            } else {
                break; // Unknown option or start of data
            }
        }

        assert!(nx);
        assert_eq!(expire_secs, Some(100));
        assert_eq!(parser.remaining_count(), 1);
    }

    #[test]
    fn test_scan_options() {
        let args = bytes_vec(&["MATCH", "user:*", "COUNT", "50"]);
        let mut parser = ArgParser::new(&args);

        let opts = ScanOptions::parse(&mut parser).unwrap();
        assert_eq!(opts.pattern, Some(b"user:*".as_slice()));
        assert_eq!(opts.count, 50);
    }

    #[test]
    fn test_expiry_options() {
        let args = bytes_vec(&["EX", "3600"]);
        let mut parser = ArgParser::new(&args);

        let expiry = ExpiryOption::try_parse(&mut parser).unwrap();
        assert_eq!(expiry, Some(ExpiryOption::Ex(3600)));
    }

    #[test]
    fn test_set_condition_conflict() {
        let args = bytes_vec(&["NX", "XX"]);
        let mut parser = ArgParser::new(&args);
        let mut cond = None;

        assert!(SetCondition::try_parse_strict(&mut parser, &mut cond).unwrap());
        assert_eq!(cond, Some(SetCondition::Nx));

        // Should error on XX after NX
        let result = SetCondition::try_parse_strict(&mut parser, &mut cond);
        assert!(result.is_err());
    }

    #[test]
    fn test_numeric_parsing() {
        let args = bytes_vec(&["42", "-10", "3.15"]);
        let mut parser = ArgParser::new(&args);

        assert_eq!(parser.next_u64().unwrap(), 42);
        assert_eq!(parser.next_i64().unwrap(), -10);
<<<<<<< HEAD
        assert!((parser.next_f64().unwrap() - 3.15_f64).abs() < 0.001);
||||||| parent of 670778b (more fixing stuff?)
        assert!((parser.next_f64().unwrap() - 3.14_f64).abs() < 0.001);
=======
        assert!((parser.next_f64().unwrap() - std::f64::consts::PI).abs() < 0.001);
>>>>>>> 670778b (more fixing stuff?)
    }

    #[test]
    fn test_syntax_error_on_missing() {
        let args: Vec<Bytes> = vec![];
        let mut parser = ArgParser::new(&args);

        let result = parser.next_arg();
        assert!(matches!(result, Err(CommandError::SyntaxError)));
    }

    #[test]
    fn test_position_tracking() {
        let args = bytes_vec(&["a", "b", "c"]);
        let mut parser = ArgParser::new(&args);

        assert_eq!(parser.position(), 0);
        parser.next();
        assert_eq!(parser.position(), 1);
        parser.skip(1);
        assert_eq!(parser.position(), 2);
        parser.reset_to(0);
        assert_eq!(parser.position(), 0);
    }
}
