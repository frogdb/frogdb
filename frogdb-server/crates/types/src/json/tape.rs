//! Flat tape encoding for stored JSON documents.
//!
//! A document is two contiguous allocations instead of one boxed node per value:
//!
//! - `words`: the tape proper, a `Vec<u64>` of nodes in document order.
//! - `text`: a side buffer holding string and object-key bytes, each run stored
//!   as a LEB128 length followed by its UTF-8 bytes.
//!
//! Every node is a single word laid out as `aux << 8 | tag`. Only numbers too
//! wide for the 56-bit `aux` field spill into a second word:
//!
//! | tag      | words | aux                                |
//! |----------|-------|------------------------------------|
//! | null     | 1     | -                                  |
//! | false    | 1     | -                                  |
//! | true     | 1     | -                                  |
//! | uint     | 1     | the value, when it fits in 56 bits |
//! | int      | 1     | the negative value, bias-encoded   |
//! | number   | 2     | which kind; the raw bits follow    |
//! | string   | 1     | offset into `text`                 |
//! | key      | 1     | offset into `text`                 |
//! | array    | 1     | word index just past the array     |
//! | object   | 1     | word index just past the object    |
//!
//! Array elements follow the header in order; object members follow it as a
//! `key` node then a value node, repeated. A container's `aux` is its end, so
//! "step over this subtree" is O(1) and reaching the *n*-th child is O(children)
//! with no pointer chasing. Element and member counts are *not* stored — they
//! fall out of that same walk, which keeps every node down to one word.
//!
//! `aux` is 56 bits wide, so a text offset or a tape end would have to exceed
//! 64 petabytes to overflow it: strings and containers have no wide form, and
//! the only spill case is a number outside the inline integer range.
//!
//! Encoding numbers as `uint` / `int` / `number`-with-kind mirrors
//! `serde_json::Number`'s own `PosInt`/`NegInt`/`Float` split, so round-tripping
//! a document through the tape preserves both its rendered text and
//! `serde_json` value equality exactly.
//!
//! Text is interned as it is written, so the repeated object keys that dominate
//! record-shaped documents are stored once per document rather than once per
//! occurrence.

use serde_json::Value as JsonData;
use std::collections::HashMap;
use std::fmt;

use super::JsonType;

const TAG_NULL: u64 = 0;
const TAG_FALSE: u64 = 1;
const TAG_TRUE: u64 = 2;
const TAG_UINT: u64 = 3;
const TAG_INT: u64 = 4;
const TAG_NUMBER: u64 = 5;
const TAG_STRING: u64 = 6;
const TAG_KEY: u64 = 7;
const TAG_ARRAY: u64 = 8;
const TAG_OBJECT: u64 = 9;

const TAG_MASK: u64 = 0xFF;
const AUX_BITS: u32 = 56;
/// Largest value the header's `aux` field can hold.
const AUX_MAX: u64 = (1 << AUX_BITS) - 1;
/// Offset applied to an inline negative integer so `aux` stays unsigned.
const INT_BIAS: i64 = 1 << (AUX_BITS - 1);

/// Which flavour of number a [`TAG_NUMBER`] node's payload word holds.
const NUM_U64: u64 = 0;
const NUM_I64: u64 = 1;
const NUM_F64: u64 = 2;

#[inline]
fn header(tag: u64, aux: u64) -> u64 {
    debug_assert!(aux <= AUX_MAX);
    (aux << 8) | tag
}

#[inline]
fn tag_of(word: u64) -> u64 {
    word & TAG_MASK
}

#[inline]
fn aux_of(word: u64) -> u64 {
    word >> 8
}

/// A number read off the tape, kept in the same three flavours
/// `serde_json::Number` distinguishes.
#[derive(Clone, Copy, PartialEq)]
enum Num {
    U(u64),
    I(i64),
    F(f64),
}

fn push_varint(buf: &mut Vec<u8>, mut n: u64) {
    while n >= 0x80 {
        buf.push(n as u8 | 0x80);
        n >>= 7;
    }
    buf.push(n as u8);
}

/// Read a LEB128 length at `at`, returning it and the index of the bytes it
/// prefixes.
fn read_varint(buf: &[u8], at: usize) -> (usize, usize) {
    let mut value = 0u64;
    let mut shift = 0;
    let mut i = at;
    loop {
        let byte = buf[i];
        i += 1;
        value |= u64::from(byte & 0x7F) << shift;
        if byte < 0x80 {
            return (value as usize, i);
        }
        shift += 7;
    }
}

/// A JSON document encoded as a flat tape plus a string side buffer.
#[derive(Debug, Clone)]
pub struct JsonTape {
    words: Vec<u64>,
    text: Vec<u8>,
}

impl Default for JsonTape {
    fn default() -> Self {
        Self::from_value(&JsonData::Null)
    }
}

impl JsonTape {
    /// Encode a `serde_json` value onto a fresh tape.
    pub fn from_value(value: &JsonData) -> Self {
        let mut builder = TapeBuilder::new();
        builder.append_value(value);
        builder.finish()
    }

    /// The document root.
    pub fn root(&self) -> TapeRef<'_> {
        TapeRef { tape: self, at: 0 }
    }

    /// A cursor at the node starting at word index `at`, as returned by
    /// [`TapeRef::offset`].
    pub(super) fn node_at(&self, at: usize) -> TapeRef<'_> {
        debug_assert!(at < self.words.len());
        TapeRef { tape: self, at }
    }

    /// Bytes held by the tape itself (words plus string buffer).
    ///
    /// [`TapeBuilder::finish`] shrinks both allocations, so this is the whole
    /// heap footprint and not just the used prefix of it.
    pub fn byte_len(&self) -> usize {
        self.words.len() * std::mem::size_of::<u64>() + self.text.len()
    }

    /// The word index just past the node starting at `at`.
    fn node_end(&self, at: usize) -> usize {
        let word = self.words[at];
        match tag_of(word) {
            TAG_ARRAY | TAG_OBJECT => aux_of(word) as usize,
            TAG_NUMBER => at + 2,
            _ => at + 1,
        }
    }

    fn text_at(&self, at: usize) -> &str {
        let (len, start) = read_varint(&self.text, aux_of(self.words[at]) as usize);
        // Only `&str` bytes are ever appended to `text`, so this never fails.
        std::str::from_utf8(&self.text[start..start + len]).unwrap_or("")
    }

    fn number_at(&self, at: usize) -> Option<Num> {
        let word = self.words[at];
        match tag_of(word) {
            TAG_UINT => Some(Num::U(aux_of(word))),
            TAG_INT => Some(Num::I(aux_of(word) as i64 - INT_BIAS)),
            TAG_NUMBER => {
                let bits = self.words[at + 1];
                Some(match aux_of(word) {
                    NUM_U64 => Num::U(bits),
                    NUM_I64 => Num::I(bits as i64),
                    _ => Num::F(f64::from_bits(bits)),
                })
            }
            _ => None,
        }
    }
}

/// Writes a [`JsonTape`], interning text as it goes.
///
/// Separate from the finished tape so the intern table — pure build-time
/// scratch — never becomes part of what a stored document costs.
pub(super) struct TapeBuilder {
    words: Vec<u64>,
    text: Vec<u8>,
    interned: HashMap<Box<str>, u64>,
}

impl TapeBuilder {
    pub(super) fn new() -> Self {
        Self {
            words: Vec::new(),
            text: Vec::new(),
            interned: HashMap::new(),
        }
    }

    /// Hand back the finished tape, trimmed to the bytes it actually holds.
    pub(super) fn finish(self) -> JsonTape {
        let mut words = self.words;
        let mut text = self.text;
        words.shrink_to_fit();
        text.shrink_to_fit();
        JsonTape { words, text }
    }

    /// Encode `value` as the next node on the tape.
    pub(super) fn append_value(&mut self, value: &JsonData) {
        match value {
            JsonData::Null => self.words.push(header(TAG_NULL, 0)),
            JsonData::Bool(false) => self.words.push(header(TAG_FALSE, 0)),
            JsonData::Bool(true) => self.words.push(header(TAG_TRUE, 0)),
            JsonData::Number(n) => self.push_number(n),
            JsonData::String(s) => self.push_text(TAG_STRING, s),
            JsonData::Array(items) => {
                let at = self.begin_array();
                for item in items {
                    self.append_value(item);
                }
                self.end_container(at);
            }
            JsonData::Object(members) => {
                let at = self.begin_object();
                for (key, value) in members {
                    self.push_key(key);
                    self.append_value(value);
                }
                self.end_container(at);
            }
        }
    }

    /// Copy `node` and its whole subtree onto the end of this tape.
    pub(super) fn append_subtree(&mut self, node: TapeRef<'_>) {
        match tag_of(node.word()) {
            TAG_ARRAY => {
                let at = self.begin_array();
                for child in node.elements() {
                    self.append_subtree(child);
                }
                self.end_container(at);
            }
            TAG_OBJECT => {
                let at = self.begin_object();
                for (key, value) in node.members() {
                    self.push_key(key);
                    self.append_subtree(value);
                }
                self.end_container(at);
            }
            // A string's aux is an offset into the *source* text buffer, so it
            // has to be re-interned rather than copied.
            TAG_STRING => self.push_text(TAG_STRING, node.as_str().unwrap_or("")),
            _ => {
                // Scalars carry no tape-relative payload, so their words copy verbatim.
                let end = node.tape.node_end(node.at);
                self.words.extend_from_slice(&node.tape.words[node.at..end]);
            }
        }
    }

    pub(super) fn push_key(&mut self, key: &str) {
        self.push_text(TAG_KEY, key);
    }

    pub(super) fn begin_array(&mut self) -> usize {
        self.begin_container(TAG_ARRAY)
    }

    pub(super) fn begin_object(&mut self) -> usize {
        self.begin_container(TAG_OBJECT)
    }

    /// Close the container opened at `at`, recording where its subtree ends.
    pub(super) fn end_container(&mut self, at: usize) {
        self.words[at] = header(tag_of(self.words[at]), self.words.len() as u64);
    }

    fn begin_container(&mut self, tag: u64) -> usize {
        let at = self.words.len();
        self.words.push(header(tag, 0));
        at
    }

    fn push_number(&mut self, n: &serde_json::Number) {
        // Classify exactly as `serde_json::Number` does — PosInt before NegInt
        // before Float — so a round trip preserves both the rendered text and
        // value equality.
        if let Some(u) = n.as_u64() {
            if u <= AUX_MAX {
                self.words.push(header(TAG_UINT, u));
            } else {
                self.words.push(header(TAG_NUMBER, NUM_U64));
                self.words.push(u);
            }
        } else if let Some(i) = n.as_i64() {
            if i >= -INT_BIAS {
                self.words.push(header(TAG_INT, (i + INT_BIAS) as u64));
            } else {
                self.words.push(header(TAG_NUMBER, NUM_I64));
                self.words.push(i as u64);
            }
        } else {
            self.words.push(header(TAG_NUMBER, NUM_F64));
            self.words.push(n.as_f64().unwrap_or(0.0).to_bits());
        }
    }

    fn push_text(&mut self, tag: u64, s: &str) {
        let offset = self.intern(s);
        self.words.push(header(tag, offset));
    }

    /// The offset of `s` in the text buffer, appending it only if this document
    /// has not stored that exact run before.
    fn intern(&mut self, s: &str) -> u64 {
        if let Some(&offset) = self.interned.get(s) {
            return offset;
        }
        let offset = self.text.len() as u64;
        push_varint(&mut self.text, s.len() as u64);
        self.text.extend_from_slice(s.as_bytes());
        self.interned.insert(s.into(), offset);
        offset
    }
}

/// A cursor pointing at one node of a [`JsonTape`].
///
/// Cheap to copy, and the only way command code reads a stored document: child
/// iteration walks the tape in place and container end-offsets step over
/// subtrees without materializing them.
#[derive(Clone, Copy)]
pub struct TapeRef<'a> {
    tape: &'a JsonTape,
    at: usize,
}

impl<'a> TapeRef<'a> {
    /// Word index of this node, used to key edits during a tape rebuild.
    pub(crate) fn offset(&self) -> usize {
        self.at
    }

    fn word(&self) -> u64 {
        self.tape.words[self.at]
    }

    fn number(&self) -> Option<Num> {
        self.tape.number_at(self.at)
    }

    /// The JSON type as `JSON.TYPE` reports it.
    pub fn json_type(&self) -> JsonType {
        match tag_of(self.word()) {
            TAG_NULL => JsonType::Null,
            TAG_FALSE | TAG_TRUE => JsonType::Boolean,
            TAG_UINT | TAG_INT => JsonType::Integer,
            TAG_NUMBER => match self.number() {
                Some(Num::F(_)) => JsonType::Number,
                _ => JsonType::Integer,
            },
            TAG_STRING => JsonType::String,
            TAG_ARRAY => JsonType::Array,
            _ => JsonType::Object,
        }
    }

    /// True when this node is a JSON `null`.
    pub fn is_null(&self) -> bool {
        tag_of(self.word()) == TAG_NULL
    }

    /// The boolean this node holds, if it is one.
    pub fn as_bool(&self) -> Option<bool> {
        match tag_of(self.word()) {
            TAG_TRUE => Some(true),
            TAG_FALSE => Some(false),
            _ => None,
        }
    }

    /// The string this node holds, if it is one.
    pub fn as_str(&self) -> Option<&'a str> {
        (tag_of(self.word()) == TAG_STRING).then(|| self.tape.text_at(self.at))
    }

    /// The numeric value of this node as `f64`, if it is a number.
    pub fn as_f64(&self) -> Option<f64> {
        self.number().map(|n| match n {
            Num::U(u) => u as f64,
            Num::I(i) => i as f64,
            Num::F(f) => f,
        })
    }

    /// True when this node is a JSON array.
    pub fn is_array(&self) -> bool {
        tag_of(self.word()) == TAG_ARRAY
    }

    /// True when this node is a JSON object.
    pub fn is_object(&self) -> bool {
        tag_of(self.word()) == TAG_OBJECT
    }

    /// True when this node is a JSON number.
    pub fn is_number(&self) -> bool {
        self.number().is_some()
    }

    /// Element or member count for a container; `None` for anything else.
    ///
    /// Counts are derived from a walk of the container's children rather than
    /// stored, so this is O(children).
    pub fn container_len(&self) -> Option<usize> {
        match tag_of(self.word()) {
            TAG_ARRAY => Some(self.elements().count()),
            TAG_OBJECT => Some(self.members().count()),
            _ => None,
        }
    }

    /// Iterate an array's elements. Empty for non-arrays.
    pub fn elements(&self) -> Elements<'a> {
        let (at, end) = self.children_span(TAG_ARRAY);
        Elements {
            tape: self.tape,
            at,
            end,
        }
    }

    /// Iterate an object's members in stored order. Empty for non-objects.
    pub fn members(&self) -> Members<'a> {
        let (at, end) = self.children_span(TAG_OBJECT);
        Members {
            tape: self.tape,
            at,
            end,
        }
    }

    /// The half-open word range holding this container's children, or an empty
    /// range when the node is not a container of the expected kind.
    fn children_span(&self, tag: u64) -> (usize, usize) {
        if tag_of(self.word()) == tag {
            (self.at + 1, aux_of(self.word()) as usize)
        } else {
            (0, 0)
        }
    }

    /// The value stored under `key`, if this node is an object holding it.
    pub fn member(&self, key: &str) -> Option<TapeRef<'a>> {
        self.members().find(|(k, _)| *k == key).map(|(_, v)| v)
    }

    /// The element at `index`, if this node is an array holding it.
    pub fn element(&self, index: usize) -> Option<TapeRef<'a>> {
        self.elements().nth(index)
    }

    /// Bytes this subtree occupies on the tape (words plus its string bytes).
    pub fn subtree_bytes(&self) -> usize {
        let end = self.tape.node_end(self.at);
        let words = (end - self.at) * std::mem::size_of::<u64>();
        words + self.subtree_text_bytes()
    }

    fn subtree_text_bytes(&self) -> usize {
        match tag_of(self.word()) {
            TAG_STRING => self.as_str().unwrap_or("").len(),
            TAG_ARRAY => self.elements().map(|e| e.subtree_text_bytes()).sum(),
            TAG_OBJECT => self
                .members()
                .map(|(k, v)| k.len() + v.subtree_text_bytes())
                .sum(),
            _ => 0,
        }
    }

    /// Container nesting depth of this subtree; scalars are depth 0.
    pub fn depth(&self) -> usize {
        match tag_of(self.word()) {
            TAG_ARRAY => 1 + self.elements().map(|e| e.depth()).max().unwrap_or(0),
            TAG_OBJECT => 1 + self.members().map(|(_, v)| v.depth()).max().unwrap_or(0),
            _ => 0,
        }
    }

    /// Materialize this subtree as a `serde_json` value.
    ///
    /// Only for the seams that still speak `serde_json` (search-index field
    /// extraction, mutation edit buffers, values echoed back to clients) — reads
    /// that only inspect or serialize should stay on the cursor.
    pub fn to_json_data(&self) -> JsonData {
        match tag_of(self.word()) {
            TAG_NULL => JsonData::Null,
            TAG_TRUE => JsonData::Bool(true),
            TAG_FALSE => JsonData::Bool(false),
            TAG_UINT | TAG_INT | TAG_NUMBER => match self.number() {
                Some(Num::U(u)) => JsonData::Number(u.into()),
                Some(Num::I(i)) => JsonData::Number(i.into()),
                Some(Num::F(f)) => serde_json::Number::from_f64(f)
                    .map(JsonData::Number)
                    .unwrap_or(JsonData::Null),
                None => JsonData::Null,
            },
            TAG_STRING => JsonData::String(self.as_str().unwrap_or("").to_string()),
            TAG_ARRAY => JsonData::Array(self.elements().map(|e| e.to_json_data()).collect()),
            _ => JsonData::Object(
                self.members()
                    .map(|(k, v)| (k.to_string(), v.to_json_data()))
                    .collect(),
            ),
        }
    }

    /// True when this subtree equals `other` under `serde_json`'s own value
    /// equality (which distinguishes `1` from `1.0`).
    pub fn equals_json(&self, other: &JsonData) -> bool {
        match (tag_of(self.word()), other) {
            (TAG_NULL, JsonData::Null) => true,
            (TAG_TRUE, JsonData::Bool(true)) => true,
            (TAG_FALSE, JsonData::Bool(false)) => true,
            (TAG_UINT | TAG_INT | TAG_NUMBER, JsonData::Number(n)) => self.number() == number_of(n),
            (TAG_STRING, JsonData::String(s)) => self.as_str() == Some(s.as_str()),
            (TAG_ARRAY, JsonData::Array(items)) => {
                let mut elements = self.elements();
                items
                    .iter()
                    .all(|item| elements.next().is_some_and(|e| e.equals_json(item)))
                    && elements.next().is_none()
            }
            (TAG_OBJECT, JsonData::Object(members)) => {
                let mut stored = self.members();
                members.iter().all(|(key, value)| {
                    stored
                        .next()
                        .is_some_and(|(k, v)| k == key && v.equals_json(value))
                }) && stored.next().is_none()
            }
            _ => false,
        }
    }

    /// Append this subtree to `out` as compact JSON, byte-identical to
    /// `serde_json::to_string`.
    pub fn write_json(&self, out: &mut String) {
        match tag_of(self.word()) {
            TAG_NULL => out.push_str("null"),
            TAG_TRUE => out.push_str("true"),
            TAG_FALSE => out.push_str("false"),
            TAG_UINT | TAG_INT | TAG_NUMBER => out.push_str(&self.number_text()),
            TAG_STRING => write_escaped(out, self.tape.text_at(self.at)),
            TAG_ARRAY => {
                out.push('[');
                for (i, element) in self.elements().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    element.write_json(out);
                }
                out.push(']');
            }
            _ => {
                out.push('{');
                for (i, (key, value)) in self.members().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    write_escaped(out, key);
                    out.push(':');
                    value.write_json(out);
                }
                out.push('}');
            }
        }
    }

    /// This subtree as compact JSON text.
    pub fn to_json_string(&self) -> String {
        let mut out = String::new();
        self.write_json(&mut out);
        out
    }

    /// The rendered text of a number node, matching `serde_json`'s formatting.
    pub fn number_text(&self) -> String {
        match self.number() {
            Some(Num::U(u)) => u.to_string(),
            Some(Num::I(i)) => i.to_string(),
            Some(Num::F(f)) => serde_json::Number::from_f64(f)
                .map(|n| n.to_string())
                .unwrap_or_else(|| "null".to_string()),
            None => String::new(),
        }
    }
}

/// Classify a `serde_json` number the same way the tape does, so the two can be
/// compared without going through `f64`.
fn number_of(n: &serde_json::Number) -> Option<Num> {
    if let Some(u) = n.as_u64() {
        Some(Num::U(u))
    } else if let Some(i) = n.as_i64() {
        Some(Num::I(i))
    } else {
        n.as_f64().map(Num::F)
    }
}

impl fmt::Debug for TapeRef<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_json_string())
    }
}

/// Iterator over an array node's elements.
pub struct Elements<'a> {
    tape: &'a JsonTape,
    at: usize,
    end: usize,
}

impl<'a> Iterator for Elements<'a> {
    type Item = TapeRef<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.at >= self.end {
            return None;
        }
        let at = self.at;
        self.at = self.tape.node_end(at);
        Some(TapeRef {
            tape: self.tape,
            at,
        })
    }
}

/// Iterator over an object node's members, in stored order.
pub struct Members<'a> {
    tape: &'a JsonTape,
    at: usize,
    end: usize,
}

impl<'a> Iterator for Members<'a> {
    type Item = (&'a str, TapeRef<'a>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.at >= self.end {
            return None;
        }
        let key = self.tape.text_at(self.at);
        let value_at = self.at + 1;
        self.at = self.tape.node_end(value_at);
        Some((
            key,
            TapeRef {
                tape: self.tape,
                at: value_at,
            },
        ))
    }
}

/// The escape `serde_json` applies to a byte, or 0 when it emits the byte as-is.
///
/// Mirrors `serde_json`'s own ESCAPE table so compact output stays byte-identical:
/// only `"`, `\` and the C0 controls are escaped, and non-ASCII passes through.
fn escape_of(byte: u8) -> u8 {
    match byte {
        0x08 => b'b',
        0x09 => b't',
        0x0A => b'n',
        0x0C => b'f',
        0x0D => b'r',
        b'"' => b'"',
        b'\\' => b'\\',
        0x00..=0x1F => b'u',
        _ => 0,
    }
}

/// Write `s` as a quoted JSON string, escaping exactly as `serde_json` does.
fn write_escaped(out: &mut String, s: &str) {
    const HEX: &[u8; 16] = b"0123456789abcdef";

    out.push('"');
    let bytes = s.as_bytes();
    let mut start = 0;
    for (i, &byte) in bytes.iter().enumerate() {
        let escape = escape_of(byte);
        if escape == 0 {
            continue;
        }
        // Escapes are all ASCII, so `i` is always a char boundary.
        out.push_str(&s[start..i]);
        if escape == b'u' {
            out.push_str("\\u00");
            out.push(HEX[(byte >> 4) as usize] as char);
            out.push(HEX[(byte & 0x0F) as usize] as char);
        } else {
            out.push('\\');
            out.push(escape as char);
        }
        start = i + 1;
    }
    out.push_str(&s[start..]);
    out.push('"');
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn roundtrip(text: &str) {
        let value: JsonData = serde_json::from_str(text).unwrap();
        let tape = JsonTape::from_value(&value);
        assert_eq!(
            tape.root().to_json_string(),
            serde_json::to_string(&value).unwrap(),
            "tape serialization diverged for {text}"
        );
        assert_eq!(tape.root().to_json_data(), value, "materialize diverged");
        assert!(tape.root().equals_json(&value), "equality diverged");
    }

    #[test]
    fn scalars_round_trip() {
        for text in [
            "null",
            "true",
            "false",
            "0",
            "-1",
            "42",
            "9007199254740992",
            "18446744073709551615",
            "-9223372036854775808",
            "1.5",
            "-0.0",
            "1.23e10",
            "99999999999999999999999999999",
            r#""""#,
            r#""hello""#,
            r#""🦊""#,
        ] {
            roundtrip(text);
        }
    }

    /// Integers on both sides of the 56-bit inline window, which is where the
    /// one-word and two-word number encodings meet.
    #[test]
    fn numbers_spill_out_of_the_inline_window() {
        for text in [
            "72057594037927935",  // AUX_MAX, the largest inline uint
            "72057594037927936",  // one past it: spills to a payload word
            "-36028797018963968", // -INT_BIAS, the smallest inline int
            "-36028797018963969", // one past it
            "9223372036854775807",
            "18446744073709551615",
        ] {
            roundtrip(text);
        }
    }

    #[test]
    fn containers_round_trip() {
        for text in [
            "[]",
            "{}",
            "[1,2,3]",
            r#"{"a":1,"b":[1,{"c":null}],"d":{"e":"f"}}"#,
            r#"[[[[1]]]]"#,
            r#"{"":0}"#,
        ] {
            roundtrip(text);
        }
    }

    #[test]
    fn control_characters_escape_like_serde_json() {
        let value = json!({ "k\u{0}\u{8}\u{9}\u{a}\u{b}\u{c}\u{d}\u{1f}\"\\/": "v\u{7}" });
        let tape = JsonTape::from_value(&value);
        assert_eq!(
            tape.root().to_json_string(),
            serde_json::to_string(&value).unwrap()
        );
    }

    #[test]
    fn integer_and_float_stay_distinct() {
        let ints = JsonTape::from_value(&json!(1));
        let floats = JsonTape::from_value(&json!(1.0));
        assert_eq!(ints.root().to_json_string(), "1");
        assert_eq!(floats.root().to_json_string(), "1.0");
        assert_eq!(ints.root().json_type(), JsonType::Integer);
        assert_eq!(floats.root().json_type(), JsonType::Number);
        assert!(!ints.root().equals_json(&json!(1.0)));
        assert!(ints.root().equals_json(&json!(1)));
    }

    #[test]
    fn skip_offsets_step_over_subtrees() {
        let tape = JsonTape::from_value(&json!({"a": [1, [2, 3], {"b": 4}], "z": 9}));
        let root = tape.root();
        assert_eq!(root.container_len(), Some(2));
        let a = root.member("a").unwrap();
        assert_eq!(a.container_len(), Some(3));
        assert_eq!(a.element(0).unwrap().as_f64(), Some(1.0));
        assert_eq!(a.element(1).unwrap().container_len(), Some(2));
        assert_eq!(
            a.element(2).unwrap().member("b").unwrap().as_f64(),
            Some(4.0)
        );
        assert_eq!(root.member("z").unwrap().as_f64(), Some(9.0));
        assert!(root.member("missing").is_none());
    }

    #[test]
    fn subtree_copy_rebases_string_offsets() {
        let source = JsonTape::from_value(&json!({"a": ["x", {"b": "y"}], "c": "z"}));
        let mut copy = TapeBuilder::new();
        copy.append_subtree(source.root().member("a").unwrap());
        let copy = copy.finish();
        assert_eq!(copy.root().to_json_string(), r#"["x",{"b":"y"}]"#);
    }

    #[test]
    fn depth_counts_container_nesting() {
        assert_eq!(JsonTape::from_value(&json!(1)).root().depth(), 0);
        assert_eq!(JsonTape::from_value(&json!([])).root().depth(), 1);
        assert_eq!(
            JsonTape::from_value(&json!({"a": {"b": [1]}}))
                .root()
                .depth(),
            3
        );
    }

    /// Repeated keys and repeated string values are stored once per document.
    #[test]
    fn repeated_text_is_interned_once() {
        let repeated = JsonTape::from_value(&json!([
            {"key": "value"},
            {"key": "value"},
            {"key": "value"},
        ]));
        let single = JsonTape::from_value(&json!([{"key": "value"}]));
        assert_eq!(repeated.text.len(), single.text.len());
    }
}
