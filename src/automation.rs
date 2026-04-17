use std::collections::BTreeMap;
use std::path::Path;

use serde::Serialize;
use serde_json::{Map as JsonMap, Number, Value as JsonValue};

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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionMode {
    Interactive,
    AutomationReadOnly,
    AutomationReadWrite,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub struct RetrievalBudget {
    pub nodes: usize,
    pub edges: usize,
    pub snippet_bytes: usize,
    pub total_payload_bytes: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub struct AutomationPolicy {
    pub execution_mode: ExecutionMode,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_rows: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct RetrievalUsage {
    pub nodes: usize,
    pub edges: usize,
    pub snippet_bytes: usize,
    pub total_payload_bytes: usize,
    pub truncated: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ContextEvidence {
    pub field: String,
    pub value: String,
    pub source: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ContextItem {
    pub node_id: i64,
    pub labels: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub display: Option<String>,
    pub evidence: Vec<ContextEvidence>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ContextProvenance {
    pub db_path: String,
    pub source: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ContextEnvelope {
    pub ok: bool,
    pub command: String,
    pub policy: AutomationPolicy,
    pub retrieval_usage: RetrievalUsage,
    pub provenance: ContextProvenance,
    pub items: Vec<ContextItem>,
}

#[derive(Serialize)]
struct ErrorEnvelope<'a> {
    ok: bool,
    error: ErrorBody<'a>,
}

#[derive(Serialize)]
struct ErrorBody<'a> {
    code: &'a str,
    message: &'a str,
}

#[derive(Serialize)]
struct QueryEnvelope {
    ok: bool,
    command: String,
    policy: AutomationPolicy,
    results: Vec<QueryResultEnvelope>,
}

#[derive(Serialize)]
struct QueryResultEnvelope {
    columns: Vec<String>,
    row_count: usize,
    truncated: bool,
    rows: Vec<JsonValue>,
}

#[derive(Serialize)]
struct QueryMetaLine {
    kind: String,
    ok: bool,
    command: String,
    policy: AutomationPolicy,
    result_count: usize,
}

#[derive(Serialize)]
struct QueryResultLine {
    kind: String,
    result_index: usize,
    columns: Vec<String>,
    row_count: usize,
    truncated: bool,
}

#[derive(Serialize)]
struct QueryRowLine {
    kind: String,
    result_index: usize,
    row_index: usize,
    row: JsonValue,
}

#[derive(Serialize)]
struct ContextMetaLine<'a> {
    kind: String,
    ok: bool,
    command: &'a str,
    policy: AutomationPolicy,
    retrieval_usage: &'a RetrievalUsage,
    provenance: &'a ContextProvenance,
}

#[derive(Serialize)]
struct ContextItemLine<'a> {
    kind: String,
    item_index: usize,
    item: &'a ContextItem,
}

pub fn parse_params_json(input: &str) -> Result<BTreeMap<String, Value>, AutomationError> {
    let parsed: JsonValue = serde_json::from_str(input).map_err(|error| {
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
    serde_json::to_string(&ErrorEnvelope {
        ok: false,
        error: ErrorBody { code, message },
    })
    .expect("error envelope should serialize")
}

pub fn query_as_json(results: &[QueryResult], policy: AutomationPolicy) -> String {
    serde_json::to_string(&QueryEnvelope {
        ok: true,
        command: "query".to_owned(),
        policy,
        results: results
            .iter()
            .map(|result| query_result_envelope(result, policy.max_rows.unwrap_or(usize::MAX)))
            .collect(),
    })
    .expect("query envelope should serialize")
}

pub fn query_as_ndjson(results: &[QueryResult], policy: AutomationPolicy) -> Vec<String> {
    let max_rows = policy.max_rows.unwrap_or(usize::MAX);
    let mut lines = vec![
        serde_json::to_string(&QueryMetaLine {
            kind: "query_meta".to_owned(),
            ok: true,
            command: "query".to_owned(),
            policy,
            result_count: results.len(),
        })
        .expect("query meta should serialize"),
    ];

    for (result_index, result) in results.iter().enumerate() {
        let limited = query_result_envelope(result, max_rows);
        lines.push(
            serde_json::to_string(&QueryResultLine {
                kind: "query_result".to_owned(),
                result_index,
                columns: limited.columns.clone(),
                row_count: limited.row_count,
                truncated: limited.truncated,
            })
            .expect("query result line should serialize"),
        );
        for (row_index, row) in limited.rows.into_iter().enumerate() {
            lines.push(
                serde_json::to_string(&QueryRowLine {
                    kind: "query_row".to_owned(),
                    result_index,
                    row_index,
                    row,
                })
                .expect("query row line should serialize"),
            );
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
        let payload_bytes = serde_json::to_vec(&response)
            .expect("context response should serialize")
            .len();
        if payload_bytes <= retrieval_budget.total_payload_bytes || response.items.is_empty() {
            response.retrieval_usage.total_payload_bytes = payload_bytes;
            return Ok(response);
        }
        response.items.pop();
        items = response.items;
        truncated = true;
    }
}

pub fn context_as_json(response: &ContextEnvelope) -> String {
    serde_json::to_string(response).expect("context envelope should serialize")
}

pub fn context_as_ndjson(response: &ContextEnvelope) -> Vec<String> {
    let mut lines = vec![
        serde_json::to_string(&ContextMetaLine {
            kind: "context_meta".to_owned(),
            ok: response.ok,
            command: &response.command,
            policy: response.policy,
            retrieval_usage: &response.retrieval_usage,
            provenance: &response.provenance,
        })
        .expect("context meta should serialize"),
    ];

    for (item_index, item) in response.items.iter().enumerate() {
        lines.push(
            serde_json::to_string(&ContextItemLine {
                kind: "context_item".to_owned(),
                item_index,
                item,
            })
            .expect("context item should serialize"),
        );
    }

    lines
}

fn json_to_graph_value(value: JsonValue) -> Result<Value, AutomationError> {
    match value {
        JsonValue::Null => Ok(Value::Null),
        JsonValue::Bool(value) => Ok(Value::Bool(value)),
        JsonValue::Number(value) => {
            if let Some(value) = value.as_i64() {
                Ok(Value::Int(value))
            } else if let Some(value) = value.as_u64() {
                i64::try_from(value).map(Value::Int).map_err(|_| {
                    AutomationError::new(
                        "params_json_number_range",
                        format!("integer `{value}` is outside the supported i64 range"),
                    )
                })
            } else if let Some(value) = value.as_f64() {
                Ok(Value::Float(value))
            } else {
                Err(AutomationError::new(
                    "params_json_number_parse",
                    "unsupported numeric value in params json",
                ))
            }
        }
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
            .map(|row| row_as_json(&result.columns, row))
            .collect(),
    }
}

fn row_as_json(columns: &[String], row: &[RuntimeValue]) -> JsonValue {
    let mut object = JsonMap::with_capacity(columns.len());
    for (column, value) in columns.iter().zip(row.iter()) {
        object.insert(column.clone(), runtime_value_as_json(value));
    }
    JsonValue::Object(object)
}

fn runtime_value_as_json(value: &RuntimeValue) -> JsonValue {
    match value {
        RuntimeValue::Null => JsonValue::Null,
        RuntimeValue::Bool(value) => JsonValue::Bool(*value),
        RuntimeValue::Int(value) => JsonValue::Number((*value).into()),
        RuntimeValue::Float(value) => Number::from_f64(*value)
            .map(JsonValue::Number)
            .unwrap_or_else(|| JsonValue::String(value.to_string())),
        RuntimeValue::String(value) => JsonValue::String(value.clone()),
        RuntimeValue::Bytes(value) => JsonValue::String(format!("{value:?}")),
        RuntimeValue::Datetime(value) => JsonValue::String(format!("{value:?}")),
        RuntimeValue::List(values) => {
            JsonValue::Array(values.iter().map(runtime_value_as_json).collect())
        }
        RuntimeValue::Map(entries) => {
            let mut object = JsonMap::with_capacity(entries.len());
            for (key, value) in entries {
                object.insert(key.clone(), runtime_value_as_json(value));
            }
            JsonValue::Object(object)
        }
        RuntimeValue::Node(node_id) => JsonValue::String(format!("n{}", node_id.get())),
        RuntimeValue::Edge(edge_id) => JsonValue::String(format!("e{}", edge_id.get())),
    }
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
    use crate::{QueryResult, RuntimeValue, Value};
    use serde_json::Value as JsonValue;
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
        let parsed: JsonValue = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed["ok"], JsonValue::Bool(true));
        assert_eq!(parsed["command"], JsonValue::String("query".to_owned()));
        assert_eq!(parsed["results"][0]["rows"][0]["name"], "Ada");
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

        let parsed: JsonValue = serde_json::from_str(&context_as_json(&envelope)).unwrap();
        assert_eq!(parsed["items"][0]["node_id"], JsonValue::Number(7.into()));
    }
}
