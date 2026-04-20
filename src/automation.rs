use std::collections::BTreeMap;
use std::path::Path;

use crate::json::{self, JsonNumber, JsonValue};
use crate::{ExecutionError, QueryResult, RuntimeValue, SourceError, Value};

const DEFAULT_CONTEXT_MAX_PAYLOAD_BYTES: usize = 64 * 1024;

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

    pub fn context(max_nodes: usize) -> Self {
        Self {
            execution_mode: ExecutionMode::AutomationReadOnly,
            max_rows: None,
            retrieval_budget: Some(RetrievalBudget {
                nodes: max_nodes,
                edges: 0,
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ContextEvidence {
    pub field: String,
    pub value: String,
    pub source: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ContextItem {
    pub node_id: i64,
    pub labels: Vec<String>,
    pub name: Option<String>,
    pub title: Option<String>,
    pub display: Option<String>,
    pub evidence: Vec<ContextEvidence>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ContextProvenance {
    pub db_path: String,
    pub source: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ContextEnvelope {
    pub ok: bool,
    pub command: String,
    pub policy: AutomationPolicy,
    pub retrieval_usage: RetrievalUsage,
    pub provenance: ContextProvenance,
    pub items: Vec<ContextItem>,
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

pub fn build_context_response(
    db_path: &Path,
    top_k: usize,
    result: &QueryResult,
) -> Result<ContextEnvelope, AutomationError> {
    let policy = AutomationPolicy::context(top_k);
    let Some(retrieval_budget) = policy.retrieval_budget else {
        unreachable!("context policy should include retrieval budget");
    };
    let mut items = result
        .rows
        .iter()
        .map(|row| parse_context_item(&result.columns, row))
        .collect::<Result<Vec<_>, _>>()?;
    let mut truncated = items.len() > retrieval_budget.nodes;
    items.truncate(retrieval_budget.nodes);
    let mut buffer = String::new();

    loop {
        let mut response = ContextEnvelope {
            ok: true,
            command: "context".to_owned(),
            policy,
            retrieval_usage: RetrievalUsage {
                nodes: items.len(),
                edges: 0,
                snippet_bytes: 0,
                total_payload_bytes: 0,
                truncated,
            },
            provenance: ContextProvenance {
                db_path: db_path.display().to_string(),
                source: "cupld.context".to_owned(),
            },
            items: items.clone(),
        };
        let payload_bytes = context_payload_bytes(&mut response, &mut buffer);
        if payload_bytes <= retrieval_budget.total_payload_bytes || response.items.is_empty() {
            return Ok(response);
        }
        response.items.pop();
        items = response.items;
        truncated = true;
    }
}

pub fn context_as_json(response: &ContextEnvelope) -> String {
    json::stringify(&context_json_value(response))
}

pub fn context_as_ndjson(response: &ContextEnvelope) -> Vec<String> {
    let mut lines = vec![json::stringify(&JsonValue::object([
        ("kind", JsonValue::from("context_meta")),
        ("ok", JsonValue::Bool(response.ok)),
        ("command", JsonValue::from(response.command.clone())),
        ("policy", automation_policy_json_value(&response.policy)),
        (
            "retrieval_usage",
            retrieval_usage_json_value(&response.retrieval_usage),
        ),
        (
            "provenance",
            context_provenance_json_value(&response.provenance),
        ),
    ]))];

    for (item_index, item) in response.items.iter().enumerate() {
        lines.push(json::stringify(&JsonValue::object([
            ("kind", JsonValue::from("context_item")),
            ("item_index", JsonValue::from(item_index)),
            ("item", context_item_json_value(item)),
        ])));
    }

    lines
}

fn context_payload_bytes(response: &mut ContextEnvelope, buffer: &mut String) -> usize {
    loop {
        buffer.clear();
        json::write_to(buffer, &context_json_value(response));
        let payload_bytes = buffer.len();
        if response.retrieval_usage.total_payload_bytes == payload_bytes {
            return payload_bytes;
        }
        response.retrieval_usage.total_payload_bytes = payload_bytes;
    }
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

fn automation_policy_json_value(policy: &AutomationPolicy) -> JsonValue {
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

fn retrieval_usage_json_value(usage: &RetrievalUsage) -> JsonValue {
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

fn context_evidence_json_value(evidence: &ContextEvidence) -> JsonValue {
    JsonValue::object([
        ("field", JsonValue::from(evidence.field.clone())),
        ("value", JsonValue::from(evidence.value.clone())),
        ("source", JsonValue::from(evidence.source.clone())),
    ])
}

fn context_item_json_value(item: &ContextItem) -> JsonValue {
    let mut fields = vec![
        ("node_id".to_owned(), JsonValue::from(item.node_id)),
        (
            "labels".to_owned(),
            JsonValue::array(item.labels.iter().cloned().map(JsonValue::from)),
        ),
    ];
    if let Some(name) = &item.name {
        fields.push(("name".to_owned(), JsonValue::from(name.clone())));
    }
    if let Some(title) = &item.title {
        fields.push(("title".to_owned(), JsonValue::from(title.clone())));
    }
    if let Some(display) = &item.display {
        fields.push(("display".to_owned(), JsonValue::from(display.clone())));
    }
    fields.push((
        "evidence".to_owned(),
        JsonValue::array(item.evidence.iter().map(context_evidence_json_value)),
    ));
    JsonValue::Object(fields)
}

fn context_provenance_json_value(provenance: &ContextProvenance) -> JsonValue {
    JsonValue::object([
        ("db_path", JsonValue::from(provenance.db_path.clone())),
        ("source", JsonValue::from(provenance.source.clone())),
    ])
}

fn context_json_value(response: &ContextEnvelope) -> JsonValue {
    JsonValue::object([
        ("ok", JsonValue::Bool(response.ok)),
        ("command", JsonValue::from(response.command.clone())),
        ("policy", automation_policy_json_value(&response.policy)),
        (
            "retrieval_usage",
            retrieval_usage_json_value(&response.retrieval_usage),
        ),
        (
            "provenance",
            context_provenance_json_value(&response.provenance),
        ),
        (
            "items",
            JsonValue::array(response.items.iter().map(context_item_json_value)),
        ),
    ])
}

fn parse_context_item(
    columns: &[String],
    row: &[RuntimeValue],
) -> Result<ContextItem, AutomationError> {
    let node_id = expect_int(columns, row, "node_id")?;
    let labels = expect_string_list(columns, row, "labels")?;
    let name = optional_string(columns, row, "name")?;
    let title = optional_string(columns, row, "title")?;
    let display = name.clone().or_else(|| title.clone());
    let mut evidence = Vec::new();
    if let Some(name) = &name {
        evidence.push(ContextEvidence {
            field: "name".to_owned(),
            value: name.clone(),
            source: "property:name".to_owned(),
        });
    }
    if let Some(title) = &title {
        evidence.push(ContextEvidence {
            field: "title".to_owned(),
            value: title.clone(),
            source: "property:title".to_owned(),
        });
    }
    if !labels.is_empty() {
        evidence.push(ContextEvidence {
            field: "labels".to_owned(),
            value: labels.join(","),
            source: "labels(n)".to_owned(),
        });
    }

    Ok(ContextItem {
        node_id,
        labels,
        name,
        title,
        display,
        evidence,
    })
}

fn column_index(columns: &[String], expected: &str) -> Result<usize, AutomationError> {
    columns
        .iter()
        .position(|column| column == expected)
        .ok_or_else(|| {
            AutomationError::new(
                "context_contract",
                format!("missing expected `{expected}` column in context result"),
            )
        })
}

fn expect_int(
    columns: &[String],
    row: &[RuntimeValue],
    column: &str,
) -> Result<i64, AutomationError> {
    let index = column_index(columns, column)?;
    match row.get(index) {
        Some(RuntimeValue::Int(value)) => Ok(*value),
        Some(other) => Err(AutomationError::new(
            "context_contract",
            format!("expected `{column}` to be an integer, found {other:?}"),
        )),
        None => Err(AutomationError::new(
            "context_contract",
            format!("missing value for `{column}` in context result row"),
        )),
    }
}

fn expect_string_list(
    columns: &[String],
    row: &[RuntimeValue],
    column: &str,
) -> Result<Vec<String>, AutomationError> {
    let index = column_index(columns, column)?;
    match row.get(index) {
        Some(RuntimeValue::List(values)) => values
            .iter()
            .map(|value| match value {
                RuntimeValue::String(value) => Ok(value.clone()),
                other => Err(AutomationError::new(
                    "context_contract",
                    format!("expected `{column}` items to be strings, found {other:?}"),
                )),
            })
            .collect(),
        Some(other) => Err(AutomationError::new(
            "context_contract",
            format!("expected `{column}` to be a list, found {other:?}"),
        )),
        None => Err(AutomationError::new(
            "context_contract",
            format!("missing value for `{column}` in context result row"),
        )),
    }
}

fn optional_string(
    columns: &[String],
    row: &[RuntimeValue],
    column: &str,
) -> Result<Option<String>, AutomationError> {
    let index = column_index(columns, column)?;
    match row.get(index) {
        Some(RuntimeValue::Null) => Ok(None),
        Some(RuntimeValue::String(value)) => Ok(Some(value.clone())),
        Some(other) => Err(AutomationError::new(
            "context_contract",
            format!("expected `{column}` to be a string or null, found {other:?}"),
        )),
        None => Err(AutomationError::new(
            "context_contract",
            format!("missing value for `{column}` in context result row"),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AutomationPolicy, build_context_response, context_as_json, format_error_json,
        parse_params_json, query_as_json,
    };
    use crate::json;
    use crate::{QueryResult, RuntimeValue, Value};
    use std::path::Path;

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

    #[test]
    fn builds_context_envelope_with_budgets_and_evidence() {
        let result = QueryResult {
            columns: vec![
                "node_id".to_owned(),
                "labels".to_owned(),
                "name".to_owned(),
                "title".to_owned(),
            ],
            rows: vec![vec![
                RuntimeValue::Int(7),
                RuntimeValue::List(vec![RuntimeValue::String("Person".to_owned())]),
                RuntimeValue::String("Ada".to_owned()),
                RuntimeValue::Null,
            ]],
        };

        let envelope = build_context_response(Path::new("/tmp/test.cupld"), 5, &result).unwrap();

        assert_eq!(envelope.policy.retrieval_budget.unwrap().nodes, 5);
        assert_eq!(envelope.items[0].display.as_deref(), Some("Ada"));
        assert!(
            envelope.items[0]
                .evidence
                .iter()
                .any(|evidence| evidence.field == "name")
        );

        let parsed = json::parse(&context_as_json(&envelope)).unwrap();
        assert_eq!(
            parsed.get("items").unwrap().as_array().unwrap()[0]
                .get("node_id")
                .unwrap()
                .as_i64(),
            Some(7)
        );
    }
}
