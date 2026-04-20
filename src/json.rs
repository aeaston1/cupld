use std::fmt;

use crate::runtime::{QueryResult, RuntimeValue};

#[derive(Clone, Debug, PartialEq)]
pub enum JsonNumber {
    Int(i64),
    Unsigned(u64),
    Float(f64),
}

impl JsonNumber {
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Self::Int(value) => Some(*value),
            _ => None,
        }
    }

    pub fn as_u64(&self) -> Option<u64> {
        match self {
            Self::Unsigned(value) => Some(*value),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Self::Int(value) => Some(*value as f64),
            Self::Unsigned(value) => Some(*value as f64),
            Self::Float(value) => Some(*value),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum JsonValue {
    Null,
    Bool(bool),
    Number(JsonNumber),
    String(String),
    Array(Vec<JsonValue>),
    Object(Vec<(String, JsonValue)>),
}

impl JsonValue {
    pub fn object<I, K>(fields: I) -> Self
    where
        I: IntoIterator<Item = (K, JsonValue)>,
        K: Into<String>,
    {
        Self::Object(
            fields
                .into_iter()
                .map(|(key, value)| (key.into(), value))
                .collect(),
        )
    }

    pub fn array<I>(values: I) -> Self
    where
        I: IntoIterator<Item = JsonValue>,
    {
        Self::Array(values.into_iter().collect())
    }

    pub fn as_object(&self) -> Option<&[(String, JsonValue)]> {
        match self {
            Self::Object(entries) => Some(entries),
            _ => None,
        }
    }

    pub fn as_array(&self) -> Option<&[JsonValue]> {
        match self {
            Self::Array(values) => Some(values),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::String(value) => Some(value),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Self::Bool(value) => Some(*value),
            _ => None,
        }
    }

    pub fn as_number(&self) -> Option<&JsonNumber> {
        match self {
            Self::Number(value) => Some(value),
            _ => None,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        self.as_number().and_then(JsonNumber::as_i64)
    }

    pub fn as_u64(&self) -> Option<u64> {
        self.as_number().and_then(JsonNumber::as_u64)
    }

    pub fn as_f64(&self) -> Option<f64> {
        self.as_number().and_then(JsonNumber::as_f64)
    }

    pub fn is_number(&self) -> bool {
        matches!(self, Self::Number(_))
    }

    pub fn get(&self, key: &str) -> Option<&JsonValue> {
        match self {
            Self::Object(entries) => entries
                .iter()
                .rev()
                .find_map(|(entry_key, value)| (entry_key == key).then_some(value)),
            _ => None,
        }
    }
}

impl From<bool> for JsonValue {
    fn from(value: bool) -> Self {
        Self::Bool(value)
    }
}

impl From<i64> for JsonValue {
    fn from(value: i64) -> Self {
        Self::Number(JsonNumber::Int(value))
    }
}

impl From<u64> for JsonValue {
    fn from(value: u64) -> Self {
        Self::Number(JsonNumber::Unsigned(value))
    }
}

impl From<usize> for JsonValue {
    fn from(value: usize) -> Self {
        Self::Number(JsonNumber::Unsigned(value as u64))
    }
}

impl From<f64> for JsonValue {
    fn from(value: f64) -> Self {
        Self::Number(JsonNumber::Float(value))
    }
}

impl From<String> for JsonValue {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl From<&str> for JsonValue {
    fn from(value: &str) -> Self {
        Self::String(value.to_owned())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JsonError {
    message: String,
}

impl JsonError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for JsonError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for JsonError {}

pub fn parse(input: &str) -> Result<JsonValue, JsonError> {
    let mut parser = Parser::new(input);
    parser.parse()
}

pub fn stringify(value: &JsonValue) -> String {
    let mut output = String::new();
    write_to(&mut output, value);
    output
}

pub fn write_to(output: &mut String, value: &JsonValue) {
    match value {
        JsonValue::Null => output.push_str("null"),
        JsonValue::Bool(value) => output.push_str(if *value { "true" } else { "false" }),
        JsonValue::Number(JsonNumber::Int(value)) => output.push_str(&value.to_string()),
        JsonValue::Number(JsonNumber::Unsigned(value)) => output.push_str(&value.to_string()),
        JsonValue::Number(JsonNumber::Float(value)) => {
            if value.is_finite() {
                output.push_str(&value.to_string());
            } else {
                write_quoted_string(output, &value.to_string());
            }
        }
        JsonValue::String(value) => write_quoted_string(output, value),
        JsonValue::Array(values) => {
            output.push('[');
            let mut first = true;
            for value in values {
                if !first {
                    output.push(',');
                }
                first = false;
                write_to(output, value);
            }
            output.push(']');
        }
        JsonValue::Object(entries) => {
            output.push('{');
            let mut first = true;
            for (key, value) in entries {
                if !first {
                    output.push(',');
                }
                first = false;
                write_quoted_string(output, key);
                output.push(':');
                write_to(output, value);
            }
            output.push('}');
        }
    }
}

pub fn write_quoted_string(output: &mut String, input: &str) {
    output.push('"');
    for ch in input.chars() {
        match ch {
            '"' => output.push_str("\\\""),
            '\\' => output.push_str("\\\\"),
            '\u{08}' => output.push_str("\\b"),
            '\u{0C}' => output.push_str("\\f"),
            '\n' => output.push_str("\\n"),
            '\r' => output.push_str("\\r"),
            '\t' => output.push_str("\\t"),
            ch if ch < '\u{20}' => {
                output.push_str("\\u");
                output.push_str(&format!("{:04x}", ch as u32));
            }
            _ => output.push(ch),
        }
    }
    output.push('"');
}

pub fn row_to_json_object(columns: &[String], row: &[RuntimeValue]) -> JsonValue {
    JsonValue::object(
        columns
            .iter()
            .zip(row.iter())
            .map(|(column, value)| (column.clone(), runtime_value_to_json(value))),
    )
}

pub fn query_result_rows_to_json(result: &QueryResult) -> JsonValue {
    JsonValue::array(
        result
            .rows
            .iter()
            .map(|row| row_to_json_object(&result.columns, row)),
    )
}

pub fn runtime_value_to_json(value: &RuntimeValue) -> JsonValue {
    match value {
        RuntimeValue::Null => JsonValue::Null,
        RuntimeValue::Bool(value) => JsonValue::Bool(*value),
        RuntimeValue::Int(value) => JsonValue::from(*value),
        RuntimeValue::Float(value) => {
            if value.is_finite() {
                JsonValue::from(*value)
            } else {
                JsonValue::from(value.to_string())
            }
        }
        RuntimeValue::String(value) => JsonValue::from(value.clone()),
        RuntimeValue::Bytes(value) => JsonValue::from(format!("{value:?}")),
        RuntimeValue::Datetime(value) => JsonValue::from(format!("{value:?}")),
        RuntimeValue::List(values) => JsonValue::array(values.iter().map(runtime_value_to_json)),
        RuntimeValue::Map(entries) => JsonValue::object(
            entries
                .iter()
                .map(|(key, value)| (key.clone(), runtime_value_to_json(value))),
        ),
        RuntimeValue::Node(node_id) => JsonValue::from(format!("n{}", node_id.get())),
        RuntimeValue::Edge(edge_id) => JsonValue::from(format!("e{}", edge_id.get())),
    }
}

struct Parser<'a> {
    input: &'a str,
    offset: usize,
}

impl<'a> Parser<'a> {
    fn new(input: &'a str) -> Self {
        Self { input, offset: 0 }
    }

    fn parse(&mut self) -> Result<JsonValue, JsonError> {
        self.skip_ws();
        let value = self.parse_value()?;
        self.skip_ws();
        if self.offset != self.input.len() {
            return Err(self.error("unexpected trailing characters"));
        }
        Ok(value)
    }

    fn parse_value(&mut self) -> Result<JsonValue, JsonError> {
        self.skip_ws();
        match self.peek_char() {
            Some('n') => {
                self.expect_keyword("null")?;
                Ok(JsonValue::Null)
            }
            Some('t') => {
                self.expect_keyword("true")?;
                Ok(JsonValue::Bool(true))
            }
            Some('f') => {
                self.expect_keyword("false")?;
                Ok(JsonValue::Bool(false))
            }
            Some('"') => self.parse_string().map(JsonValue::String),
            Some('[') => self.parse_array(),
            Some('{') => self.parse_object(),
            Some('-' | '0'..='9') => self.parse_number().map(JsonValue::Number),
            Some(other) => Err(self.error(format!("unexpected character `{other}`"))),
            None => Err(self.error("unexpected end of input")),
        }
    }

    fn parse_array(&mut self) -> Result<JsonValue, JsonError> {
        self.expect_char('[')?;
        self.skip_ws();
        let mut values = Vec::new();
        if self.peek_char() == Some(']') {
            self.bump_char();
            return Ok(JsonValue::Array(values));
        }
        loop {
            values.push(self.parse_value()?);
            self.skip_ws();
            match self.peek_char() {
                Some(',') => {
                    self.bump_char();
                    self.skip_ws();
                }
                Some(']') => {
                    self.bump_char();
                    break;
                }
                Some(other) => {
                    return Err(self.error(format!("expected `,` or `]`, found `{other}`")));
                }
                None => return Err(self.error("unexpected end of input in array")),
            }
        }
        Ok(JsonValue::Array(values))
    }

    fn parse_object(&mut self) -> Result<JsonValue, JsonError> {
        self.expect_char('{')?;
        self.skip_ws();
        let mut entries = Vec::new();
        if self.peek_char() == Some('}') {
            self.bump_char();
            return Ok(JsonValue::Object(entries));
        }
        loop {
            let key = self.parse_string()?;
            self.skip_ws();
            self.expect_char(':')?;
            self.skip_ws();
            let value = self.parse_value()?;
            entries.push((key, value));
            self.skip_ws();
            match self.peek_char() {
                Some(',') => {
                    self.bump_char();
                    self.skip_ws();
                }
                Some('}') => {
                    self.bump_char();
                    break;
                }
                Some(other) => {
                    return Err(self.error(format!("expected `,` or `}}`, found `{other}`")));
                }
                None => return Err(self.error("unexpected end of input in object")),
            }
        }
        Ok(JsonValue::Object(entries))
    }

    fn parse_string(&mut self) -> Result<String, JsonError> {
        self.expect_char('"')?;
        let mut output = String::new();
        loop {
            let Some(ch) = self.bump_char() else {
                return Err(self.error("unexpected end of input in string"));
            };
            match ch {
                '"' => return Ok(output),
                '\\' => {
                    let Some(escaped) = self.bump_char() else {
                        return Err(self.error("unexpected end of input in escape"));
                    };
                    match escaped {
                        '"' => output.push('"'),
                        '\\' => output.push('\\'),
                        '/' => output.push('/'),
                        'b' => output.push('\u{08}'),
                        'f' => output.push('\u{0C}'),
                        'n' => output.push('\n'),
                        'r' => output.push('\r'),
                        't' => output.push('\t'),
                        'u' => output.push(self.parse_unicode_escape()?),
                        other => {
                            return Err(self.error(format!("invalid escape sequence `\\{other}`")));
                        }
                    }
                }
                ch if ch < '\u{20}' => return Err(self.error("control characters must be escaped")),
                _ => output.push(ch),
            }
        }
    }

    fn parse_unicode_escape(&mut self) -> Result<char, JsonError> {
        let code = self.parse_hex_codepoint()?;
        if !(0xD800..=0xDBFF).contains(&code) {
            return char::from_u32(code).ok_or_else(|| self.error("invalid unicode escape"));
        }
        if self.bump_char() != Some('\\') || self.bump_char() != Some('u') {
            return Err(self.error("expected low surrogate after high surrogate"));
        }
        let low = self.parse_hex_codepoint()?;
        if !(0xDC00..=0xDFFF).contains(&low) {
            return Err(self.error("invalid low surrogate"));
        }
        let high_ten = code - 0xD800;
        let low_ten = low - 0xDC00;
        let scalar = 0x10000 + ((high_ten << 10) | low_ten);
        char::from_u32(scalar).ok_or_else(|| self.error("invalid unicode escape"))
    }

    fn parse_hex_codepoint(&mut self) -> Result<u32, JsonError> {
        let start = self.offset;
        for _ in 0..4 {
            let Some(ch) = self.bump_char() else {
                return Err(self.error("unexpected end of input in unicode escape"));
            };
            if !ch.is_ascii_hexdigit() {
                return Err(self.error(format!("invalid unicode escape at byte {start}")));
            }
        }
        u32::from_str_radix(&self.input[start..self.offset], 16)
            .map_err(|_| self.error("invalid unicode escape"))
    }

    fn parse_number(&mut self) -> Result<JsonNumber, JsonError> {
        let start = self.offset;
        if self.peek_char() == Some('-') {
            self.bump_char();
        }
        match self.peek_char() {
            Some('0') => {
                self.bump_char();
            }
            Some('1'..='9') => {
                self.bump_char();
                while matches!(self.peek_char(), Some('0'..='9')) {
                    self.bump_char();
                }
            }
            _ => return Err(self.error("invalid number")),
        }
        let mut is_float = false;
        if self.peek_char() == Some('.') {
            is_float = true;
            self.bump_char();
            if !matches!(self.peek_char(), Some('0'..='9')) {
                return Err(self.error("invalid number"));
            }
            while matches!(self.peek_char(), Some('0'..='9')) {
                self.bump_char();
            }
        }
        if matches!(self.peek_char(), Some('e' | 'E')) {
            is_float = true;
            self.bump_char();
            if matches!(self.peek_char(), Some('+' | '-')) {
                self.bump_char();
            }
            if !matches!(self.peek_char(), Some('0'..='9')) {
                return Err(self.error("invalid exponent"));
            }
            while matches!(self.peek_char(), Some('0'..='9')) {
                self.bump_char();
            }
        }
        let literal = &self.input[start..self.offset];
        if is_float {
            let value = literal
                .parse::<f64>()
                .map_err(|_| self.error("invalid number"))?;
            return Ok(JsonNumber::Float(value));
        }
        if literal.starts_with('-') {
            return literal
                .parse::<i64>()
                .map(JsonNumber::Int)
                .map_err(|_| self.error("integer is outside the supported range"));
        }
        if let Ok(value) = literal.parse::<i64>() {
            return Ok(JsonNumber::Int(value));
        }
        literal
            .parse::<u64>()
            .map(JsonNumber::Unsigned)
            .map_err(|_| self.error("integer is outside the supported range"))
    }

    fn expect_keyword(&mut self, keyword: &str) -> Result<(), JsonError> {
        if self.input[self.offset..].starts_with(keyword) {
            self.offset += keyword.len();
            Ok(())
        } else {
            Err(self.error(format!("expected `{keyword}`")))
        }
    }

    fn expect_char(&mut self, expected: char) -> Result<(), JsonError> {
        match self.bump_char() {
            Some(ch) if ch == expected => Ok(()),
            Some(ch) => Err(self.error(format!("expected `{expected}`, found `{ch}`"))),
            None => Err(self.error(format!("expected `{expected}`, found end of input"))),
        }
    }

    fn skip_ws(&mut self) {
        while matches!(self.peek_char(), Some(' ' | '\n' | '\r' | '\t')) {
            self.bump_char();
        }
    }

    fn peek_char(&self) -> Option<char> {
        self.input[self.offset..].chars().next()
    }

    fn bump_char(&mut self) -> Option<char> {
        let ch = self.peek_char()?;
        self.offset += ch.len_utf8();
        Some(ch)
    }

    fn error(&self, message: impl Into<String>) -> JsonError {
        JsonError::new(format!(
            "invalid json at byte {}: {}",
            self.offset,
            message.into()
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::{
        JsonNumber, JsonValue, parse, query_result_rows_to_json, runtime_value_to_json, stringify,
        write_quoted_string,
    };
    use crate::{QueryResult, RuntimeValue};

    #[test]
    fn parses_nested_values_and_unicode() {
        let parsed =
            parse("{\"name\":\"Ada\",\"tags\":[\"rust\",null],\"emoji\":\"\\uD83D\\uDE80\"}")
                .unwrap();

        assert_eq!(parsed.get("name").and_then(JsonValue::as_str), Some("Ada"));
        assert_eq!(
            parsed
                .get("tags")
                .and_then(JsonValue::as_array)
                .map(|values| values.len()),
            Some(2)
        );
        assert_eq!(parsed.get("emoji").and_then(JsonValue::as_str), Some("🚀"));
    }

    #[test]
    fn parses_unsigned_and_float_numbers() {
        let parsed = parse("{\"large\":9223372036854775808,\"ratio\":1.5e2}").unwrap();

        assert_eq!(
            parsed.get("large").and_then(JsonValue::as_number),
            Some(&JsonNumber::Unsigned(9_223_372_036_854_775_808))
        );
        assert_eq!(parsed.get("ratio").and_then(JsonValue::as_f64), Some(150.0));
    }

    #[test]
    fn stringifies_runtime_values_as_valid_json() {
        let result = QueryResult {
            columns: vec!["name".to_owned(), "score".to_owned()],
            rows: vec![vec![
                RuntimeValue::String("Ada".to_owned()),
                RuntimeValue::Float(f64::INFINITY),
            ]],
        };

        assert_eq!(
            stringify(&query_result_rows_to_json(&result)),
            r#"[{"name":"Ada","score":"inf"}]"#
        );
    }

    #[test]
    fn quotes_strings_with_control_characters() {
        let mut output = String::new();
        write_quoted_string(&mut output, "\"line\"\n");
        assert_eq!(output, r#""\"line\"\n""#);
    }

    #[test]
    fn rejects_invalid_json_inputs() {
        assert!(
            parse("{bad")
                .unwrap_err()
                .to_string()
                .contains("invalid json")
        );
        assert!(
            parse("[1,]")
                .unwrap_err()
                .to_string()
                .contains("invalid json")
        );
    }

    #[test]
    fn serializes_runtime_maps_and_lists() {
        let value = RuntimeValue::Map(vec![
            ("name".to_owned(), RuntimeValue::String("Ada".to_owned())),
            (
                "tags".to_owned(),
                RuntimeValue::List(vec![RuntimeValue::String("graph".to_owned())]),
            ),
        ]);

        assert_eq!(
            stringify(&runtime_value_to_json(&value)),
            r#"{"name":"Ada","tags":["graph"]}"#
        );
    }
}
