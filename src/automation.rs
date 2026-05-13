use std::collections::BTreeMap;

use crate::json::{self, JsonNumber, JsonValue};
use crate::{ExecutionError, QueryResult, SourceError, Value};

pub const DEFAULT_CONTEXT_MAX_PAYLOAD_BYTES: usize = 64 * 1024;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AutomationError {
    code: String,
    message: String,
}

impl AutomationError {
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
        }
    }

    pub fn code(&self) -> &str {
        &self.code
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl std::fmt::Display for AutomationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for AutomationError {}

impl From<ExecutionError> for AutomationError {
    fn from(value: ExecutionError) -> Self {
        Self::new(value.code(), value.message())
    }
}

impl From<SourceError> for AutomationError {
    fn from(value: SourceError) -> Self {
        Self::new(value.code(), value.to_string())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExecutionMode {
    Interactive,
    AutomationReadOnly,
    AutomationReadWrite,
}

impl ExecutionMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Interactive => "interactive",
            Self::AutomationReadOnly => "automation_read_only",
            Self::AutomationReadWrite => "automation_read_write",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RetrievalBudget {
    pub nodes: usize,
    pub edges: usize,
    pub snippet_bytes: usize,
    pub total_payload_bytes: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AutomationPolicy {
    pub execution_mode: ExecutionMode,
    pub max_rows: Option<usize>,
    pub retrieval_budget: Option<RetrievalBudget>,
}

impl AutomationPolicy {
    pub fn query(max_rows: usize) -> Self {
        Self {
            execution_mode: ExecutionMode::AutomationReadWrite,
            max_rows: Some(max_rows),
            retrieval_budget: None,
        }
    }

    pub fn context(max_nodes: usize, max_edges: usize) -> Self {
        Self {
            execution_mode: ExecutionMode::AutomationReadOnly,
            max_rows: None,
            retrieval_budget: Some(RetrievalBudget {
                nodes: max_nodes,
                edges: max_edges,
                snippet_bytes: 0,
                total_payload_bytes: DEFAULT_CONTEXT_MAX_PAYLOAD_BYTES,
            }),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RetrievalUsage {
    pub nodes: usize,
    pub edges: usize,
    pub snippet_bytes: usize,
    pub total_payload_bytes: usize,
    pub truncated: bool,
}

#[derive(Clone, Debug, PartialEq)]
struct QueryResultEnvelope {
    columns: Vec<String>,
    row_count: usize,
    truncated: bool,
    rows: Vec<JsonValue>,
}

pub fn parse_params_json(input: &str) -> Result<BTreeMap<String, Value>, AutomationError> {
    let parsed = json::parse(input).map_err(|error| {
        AutomationError::new("params_json_parse", format!("invalid params json: {error}"))
    })?;
    match parsed {
        JsonValue::Object(entries) => entries
            .into_iter()
            .map(|(key, value)| json_to_graph_value(value).map(|value| (key, value)))
            .collect(),
        _ => Err(AutomationError::new(
            "params_json_type",
            "params json must be an object mapping parameter names to values",
        )),
    }
}

pub fn format_error_json(code: &str, message: &str) -> String {
    json::stringify(&JsonValue::object([
        ("ok", JsonValue::Bool(false)),
        (
            "error",
            JsonValue::object([
                ("code", JsonValue::from(code)),
                ("message", JsonValue::from(message)),
            ]),
        ),
    ]))
}

pub fn query_as_json(results: &[QueryResult], policy: AutomationPolicy) -> String {
    let result_values = results
        .iter()
        .map(|result| query_result_envelope(result, policy.max_rows.unwrap_or(usize::MAX)))
        .map(|result| query_result_json_value(&result))
        .collect::<Vec<_>>();

    json::stringify(&JsonValue::object([
        ("ok", JsonValue::Bool(true)),
        ("command", JsonValue::from("query")),
        ("policy", automation_policy_json_value(&policy)),
        ("results", JsonValue::Array(result_values)),
    ]))
}

pub fn query_as_ndjson(results: &[QueryResult], policy: AutomationPolicy) -> Vec<String> {
    let max_rows = policy.max_rows.unwrap_or(usize::MAX);
    let mut lines = vec![json::stringify(&JsonValue::object([
        ("kind", JsonValue::from("query_meta")),
        ("ok", JsonValue::Bool(true)),
        ("command", JsonValue::from("query")),
        ("policy", automation_policy_json_value(&policy)),
        ("result_count", JsonValue::from(results.len())),
    ]))];

    for (result_index, result) in results.iter().enumerate() {
        let limited = query_result_envelope(result, max_rows);
        lines.push(json::stringify(&JsonValue::object([
            ("kind", JsonValue::from("query_result")),
            ("result_index", JsonValue::from(result_index)),
            (
                "columns",
                JsonValue::array(limited.columns.iter().cloned().map(JsonValue::from)),
            ),
            ("row_count", JsonValue::from(limited.row_count)),
            ("truncated", JsonValue::Bool(limited.truncated)),
        ])));
        for (row_index, row) in limited.rows.into_iter().enumerate() {
            lines.push(json::stringify(&JsonValue::object([
                ("kind", JsonValue::from("query_row")),
                ("result_index", JsonValue::from(result_index)),
                ("row_index", JsonValue::from(row_index)),
                ("row", row),
            ])));
        }
    }

    lines
}

fn json_to_graph_value(value: JsonValue) -> Result<Value, AutomationError> {
    match value {
        JsonValue::Null => Ok(Value::Null),
        JsonValue::Bool(value) => Ok(Value::Bool(value)),
        JsonValue::Number(JsonNumber::Int(value)) => Ok(Value::Int(value)),
        JsonValue::Number(JsonNumber::Unsigned(value)) => {
            i64::try_from(value).map(Value::Int).map_err(|_| {
                AutomationError::new(
                    "params_json_number_range",
                    format!("integer `{value}` is outside the supported i64 range"),
                )
            })
        }
        JsonValue::Number(JsonNumber::Float(value)) => Ok(Value::Float(value)),
        JsonValue::String(value) => Ok(Value::String(value)),
        JsonValue::Array(values) => values
            .into_iter()
            .map(json_to_graph_value)
            .collect::<Result<Vec<_>, _>>()
            .map(Value::List),
        JsonValue::Object(entries) => entries
            .into_iter()
            .map(|(key, value)| json_to_graph_value(value).map(|value| (key, value)))
            .collect::<Result<Vec<_>, _>>()
            .map(Value::Map),
    }
}

fn query_result_envelope(result: &QueryResult, max_rows: usize) -> QueryResultEnvelope {
    QueryResultEnvelope {
        columns: result.columns.clone(),
        row_count: result.rows.len().min(max_rows),
        truncated: result.rows.len() > max_rows,
        rows: result
            .rows
            .iter()
            .take(max_rows)
            .map(|row| json::row_to_json_object(&result.columns, row))
            .collect(),
    }
}

fn query_result_json_value(result: &QueryResultEnvelope) -> JsonValue {
    JsonValue::object([
        (
            "columns",
            JsonValue::array(result.columns.iter().cloned().map(JsonValue::from)),
        ),
        ("row_count", JsonValue::from(result.row_count)),
        ("truncated", JsonValue::Bool(result.truncated)),
        ("rows", JsonValue::Array(result.rows.clone())),
    ])
}

pub(crate) fn automation_policy_json_value(policy: &AutomationPolicy) -> JsonValue {
    let mut fields = vec![(
        "execution_mode".to_owned(),
        JsonValue::from(policy.execution_mode.as_str()),
    )];
    if let Some(max_rows) = policy.max_rows {
        fields.push(("max_rows".to_owned(), JsonValue::from(max_rows)));
    }
    if let Some(retrieval_budget) = policy.retrieval_budget {
        fields.push((
            "retrieval_budget".to_owned(),
            retrieval_budget_json_value(&retrieval_budget),
        ));
    }
    JsonValue::Object(fields)
}

fn retrieval_budget_json_value(budget: &RetrievalBudget) -> JsonValue {
    JsonValue::object([
        ("nodes", JsonValue::from(budget.nodes)),
        ("edges", JsonValue::from(budget.edges)),
        ("snippet_bytes", JsonValue::from(budget.snippet_bytes)),
        (
            "total_payload_bytes",
            JsonValue::from(budget.total_payload_bytes),
        ),
    ])
}

pub(crate) fn retrieval_usage_json_value(usage: &RetrievalUsage) -> JsonValue {
    JsonValue::object([
        ("nodes", JsonValue::from(usage.nodes)),
        ("edges", JsonValue::from(usage.edges)),
        ("snippet_bytes", JsonValue::from(usage.snippet_bytes)),
        (
            "total_payload_bytes",
            JsonValue::from(usage.total_payload_bytes),
        ),
        ("truncated", JsonValue::Bool(usage.truncated)),
    ])
}

#[cfg(test)]
mod tests {
    use super::{AutomationPolicy, format_error_json, parse_params_json, query_as_json};
    use crate::json;
    use crate::{QueryResult, RuntimeValue, Value};

    #[test]
    fn parses_params_json_with_standard_json_layer() {
        let params =
            parse_params_json("{\"name\":\"Ada\",\"tags\":[\"a\"],\"meta\":{\"team\":\"graph\"}}")
                .unwrap();

        assert_eq!(params.get("name"), Some(&Value::String("Ada".to_owned())));
        assert_eq!(
            params.get("meta"),
            Some(&Value::Map(vec![(
                "team".to_owned(),
                Value::String("graph".to_owned())
            )]))
        );
    }

    #[test]
    fn formats_machine_error_envelope() {
        assert_eq!(
            format_error_json("constraint_unique_violation", "duplicate email"),
            "{\"ok\":false,\"error\":{\"code\":\"constraint_unique_violation\",\"message\":\"duplicate email\"}}"
        );
    }

    #[test]
    fn renders_query_json_envelope() {
        let result = QueryResult {
            columns: vec!["name".to_owned()],
            rows: vec![vec![RuntimeValue::String("Ada".to_owned())]],
        };
        let json = query_as_json(&[result], AutomationPolicy::query(100));
        let parsed = json::parse(&json).unwrap();

        assert_eq!(
            parsed.get("ok").and_then(json::JsonValue::as_bool),
            Some(true)
        );
        assert_eq!(
            parsed.get("command").and_then(json::JsonValue::as_str),
            Some("query")
        );
        assert_eq!(
            parsed
                .get("results")
                .and_then(json::JsonValue::as_array)
                .and_then(|results| results[0].get("rows"))
                .and_then(json::JsonValue::as_array)
                .and_then(|rows| rows[0].get("name"))
                .and_then(json::JsonValue::as_str),
            Some("Ada")
        );
    }
}
