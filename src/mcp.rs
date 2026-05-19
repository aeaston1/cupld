use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::{BufRead, Write};
use std::path::{Component, Path, PathBuf};

use crate::context::{ContextDirection, ContextRequest, ContextSeedRequest, context_as_json};
use crate::json::{self, JsonValue};
use crate::package::WorkspacePackage;
use crate::runtime::{RuntimeValue, Session};
use crate::source::{
    MD_IN_DIRECTORY, MD_PARENT_DIRECTORY, configured_markdown_root, sync_markdown_root,
};
use crate::{MarkdownSyncReport, Value};

const MAX_LIMIT: usize = 50;
const DEFAULT_LIMIT: usize = 10;
const DEFAULT_SNIPPET_CHARS: usize = 500;
const DEFAULT_BODY_CHARS: usize = 4000;
const SYNC_VISIBILITY_MESSAGE: &str = "MCP reads are DB-backed; run memory_sync after markdown changes before memory_search or memory_get can see them.";

#[derive(Clone, Debug)]
pub struct McpConfig {
    pub db_path: PathBuf,
    pub root_override: Option<PathBuf>,
    pub read_only: bool,
}

pub fn serve_stdio(
    config: McpConfig,
    input: impl BufRead,
    mut output: impl Write,
) -> Result<(), String> {
    for line in input.lines() {
        let line = line.map_err(|error| error.to_string())?;
        if line.trim().is_empty() {
            continue;
        }
        let response = handle_json_line(&config, &line);
        if let Some(response) = response {
            writeln!(output, "{response}").map_err(|error| error.to_string())?;
            output.flush().map_err(|error| error.to_string())?;
        }
    }
    Ok(())
}

pub fn handle_json_line(config: &McpConfig, input: &str) -> Option<String> {
    let value = match json::parse(input) {
        Ok(value) => value,
        Err(error) => {
            return Some(json::stringify(&json_rpc_error(
                JsonValue::Null,
                -32700,
                "parse_error",
                &error.to_string(),
            )));
        }
    };
    handle_request(config, &value).map(|value| json::stringify(&value))
}

fn handle_request(config: &McpConfig, request: &JsonValue) -> Option<JsonValue> {
    let id = request.get("id").cloned().unwrap_or(JsonValue::Null);
    let Some(method) = request.get("method").and_then(JsonValue::as_str) else {
        return Some(json_rpc_error(
            id,
            -32600,
            "invalid_request",
            "expected JSON-RPC method",
        ));
    };
    if request.get("id").is_none() && method == "notifications/initialized" {
        return None;
    }
    match method {
        "initialize" => Some(json_rpc_result(
            id,
            JsonValue::object([
                ("protocolVersion", JsonValue::from("2025-06-18")),
                (
                    "capabilities",
                    JsonValue::object([("tools", empty_object()), ("resources", empty_object())]),
                ),
                (
                    "serverInfo",
                    JsonValue::object([
                        ("name", JsonValue::from("cupld-memory")),
                        ("version", JsonValue::from(env!("CARGO_PKG_VERSION"))),
                    ]),
                ),
            ]),
        )),
        "tools/list" => Some(json_rpc_result(
            id,
            JsonValue::object([("tools", JsonValue::array(tool_definitions()))]),
        )),
        "tools/call" => {
            let params = request.get("params").unwrap_or(&JsonValue::Null);
            let result = call_tool(config, params);
            Some(json_rpc_result(id, tool_content(result)))
        }
        "resources/list" => Some(json_rpc_result(
            id,
            JsonValue::object([("resources", JsonValue::array(resource_definitions()))]),
        )),
        "resources/read" => {
            let params = request.get("params").unwrap_or(&JsonValue::Null);
            let result = read_resource(config, params);
            Some(json_rpc_result(id, result))
        }
        _ => Some(json_rpc_error(
            id,
            -32601,
            "method_not_found",
            "unknown MCP method",
        )),
    }
}

fn json_rpc_result(id: JsonValue, result: JsonValue) -> JsonValue {
    JsonValue::object([
        ("jsonrpc", JsonValue::from("2.0")),
        ("id", id),
        ("result", result),
    ])
}

fn empty_object() -> JsonValue {
    JsonValue::Object(Vec::new())
}

fn json_rpc_error(id: JsonValue, rpc_code: i64, code: &str, message: &str) -> JsonValue {
    JsonValue::object([
        ("jsonrpc", JsonValue::from("2.0")),
        ("id", id),
        (
            "error",
            JsonValue::object([
                ("code", JsonValue::from(rpc_code)),
                ("message", JsonValue::from(message)),
                ("data", error_payload(code, message)),
            ]),
        ),
    ])
}

#[derive(Clone, Debug)]
struct McpToolError {
    code: String,
    message: String,
    details: Option<JsonValue>,
}

impl McpToolError {
    fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            details: None,
        }
    }

    fn with_details(
        code: impl Into<String>,
        message: impl Into<String>,
        details: JsonValue,
    ) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            details: Some(details),
        }
    }

    fn validation(message: impl Into<String>) -> Self {
        Self::new("validation_error", message)
    }

    fn payload(&self) -> JsonValue {
        error_payload_with_details(&self.code, &self.message, self.details.clone())
    }

    fn error_object(&self) -> JsonValue {
        let mut error = vec![
            ("code".to_owned(), JsonValue::from(self.code.clone())),
            ("message".to_owned(), JsonValue::from(self.message.clone())),
        ];
        if let Some(details) = &self.details {
            error.push(("details".to_owned(), details.clone()));
        }
        JsonValue::Object(error)
    }
}

fn error_payload(code: &str, message: &str) -> JsonValue {
    error_payload_with_details(code, message, None)
}

fn error_payload_with_details(code: &str, message: &str, details: Option<JsonValue>) -> JsonValue {
    let mut error = vec![
        ("code".to_owned(), JsonValue::from(code)),
        ("message".to_owned(), JsonValue::from(message)),
    ];
    if let Some(details) = details {
        error.push(("details".to_owned(), details));
    }
    JsonValue::object([
        ("ok", JsonValue::from(false)),
        ("error", JsonValue::Object(error)),
    ])
}

fn tool_content(payload: JsonValue) -> JsonValue {
    JsonValue::object([(
        "content",
        JsonValue::array([JsonValue::object([
            ("type", JsonValue::from("text")),
            ("text", JsonValue::from(json::stringify(&payload))),
        ])]),
    )])
}

#[derive(Clone, Copy)]
struct ToolSpec {
    name: &'static str,
    description: &'static str,
    input_schema: fn() -> JsonValue,
}

const TOOL_SPECS: &[ToolSpec] = &[
    ToolSpec {
        name: "memory_health",
        description: "Report cupld memory DB and markdown root status.",
        input_schema: empty_schema,
    },
    ToolSpec {
        name: "memory_get",
        description: "Get one DB-backed memory note by URI, path, id, or title.",
        input_schema: memory_get_schema,
    },
    ToolSpec {
        name: "memory_list",
        description: "List DB-backed memory notes.",
        input_schema: memory_list_schema,
    },
    ToolSpec {
        name: "memory_search",
        description: "Deterministically search DB-backed memory notes with local lexical matching.",
        input_schema: memory_search_schema,
    },
    ToolSpec {
        name: "memory_context",
        description: "Expand from memory search hits or explicit seeds into bounded DB-backed graph context.",
        input_schema: memory_context_schema,
    },
    ToolSpec {
        name: "memory_sync",
        description: "Sync configured markdown memory into the cupld DB.",
        input_schema: empty_schema,
    },
    ToolSpec {
        name: "memory_add",
        description: "Write a markdown memory note and sync it into the DB.",
        input_schema: memory_add_schema,
    },
    ToolSpec {
        name: "memory_doctor",
        description: "Diagnose cupld memory harness readiness without mutating DB or markdown state.",
        input_schema: memory_doctor_schema,
    },
];

fn tool_definitions() -> Vec<JsonValue> {
    TOOL_SPECS
        .iter()
        .map(|spec| {
            JsonValue::object([
                ("name", JsonValue::from(spec.name)),
                ("description", JsonValue::from(spec.description)),
                ("inputSchema", (spec.input_schema)()),
            ])
        })
        .collect()
}

fn empty_schema() -> JsonValue {
    object_schema([], [])
}

fn memory_doctor_schema() -> JsonValue {
    object_schema([("deep", bool_schema())], [])
}

fn memory_get_schema() -> JsonValue {
    object_schema(
        [
            ("id_or_uri", string_schema()),
            ("max_chars", integer_schema(Some(1), Some(20_000))),
        ],
        ["id_or_uri"],
    )
}

fn memory_list_schema() -> JsonValue {
    object_schema(
        [
            ("limit", integer_schema(Some(1), Some(MAX_LIMIT))),
            ("tags", string_array_schema()),
        ],
        [],
    )
}

fn memory_search_schema() -> JsonValue {
    let retrieval = enum_schema(["lexical", "semantic", "vector"]);
    object_schema(
        [
            ("query", string_schema()),
            ("limit", integer_schema(Some(1), Some(MAX_LIMIT))),
            ("tags", string_array_schema()),
            ("retrieval_mode", retrieval.clone()),
            ("mode", retrieval.clone()),
            ("retrieval", retrieval),
        ],
        ["query"],
    )
}

fn memory_context_schema() -> JsonValue {
    object_schema(
        [
            ("id_or_uri", string_schema()),
            ("path", string_schema()),
            ("paths", string_array_schema()),
            ("node", integer_schema(Some(0), None)),
            ("nodes", integer_array_schema(Some(0), None)),
            (
                "depth",
                integer_schema(Some(0), Some(crate::MAX_TRAVERSAL_DEPTH as usize)),
            ),
            ("direction", enum_schema(["in", "out", "both"])),
            ("edge_types", string_array_schema()),
            ("labels", string_array_schema()),
            ("max_nodes", integer_schema(Some(1), Some(250))),
            ("max_edges", integer_schema(Some(1), Some(1_000))),
        ],
        [],
    )
}

fn memory_add_schema() -> JsonValue {
    object_schema(
        [
            ("content", string_schema()),
            ("title", string_schema()),
            ("tags", string_array_schema()),
            ("path_hint", string_schema()),
            ("source", string_schema()),
        ],
        ["content"],
    )
}

fn object_schema<const P: usize, const R: usize>(
    properties: [(&'static str, JsonValue); P],
    required: [&'static str; R],
) -> JsonValue {
    JsonValue::object([
        ("type", JsonValue::from("object")),
        (
            "properties",
            JsonValue::object(
                properties
                    .into_iter()
                    .map(|(key, value)| (key.to_owned(), value)),
            ),
        ),
        (
            "required",
            JsonValue::array(required.into_iter().map(JsonValue::from)),
        ),
        ("additionalProperties", JsonValue::from(false)),
    ])
}

fn string_schema() -> JsonValue {
    JsonValue::object([("type", JsonValue::from("string"))])
}

fn bool_schema() -> JsonValue {
    JsonValue::object([("type", JsonValue::from("boolean"))])
}

fn integer_schema(minimum: Option<usize>, maximum: Option<usize>) -> JsonValue {
    let mut fields = vec![("type".to_owned(), JsonValue::from("integer"))];
    if let Some(minimum) = minimum {
        fields.push(("minimum".to_owned(), JsonValue::from(minimum)));
    }
    if let Some(maximum) = maximum {
        fields.push(("maximum".to_owned(), JsonValue::from(maximum)));
    }
    JsonValue::Object(fields)
}

fn string_array_schema() -> JsonValue {
    JsonValue::object([
        ("type", JsonValue::from("array")),
        ("items", string_schema()),
    ])
}

fn integer_array_schema(minimum: Option<usize>, maximum: Option<usize>) -> JsonValue {
    JsonValue::object([
        ("type", JsonValue::from("array")),
        ("items", integer_schema(minimum, maximum)),
    ])
}

fn enum_schema<const N: usize>(values: [&'static str; N]) -> JsonValue {
    JsonValue::object([
        ("type", JsonValue::from("string")),
        (
            "enum",
            JsonValue::array(values.into_iter().map(JsonValue::from)),
        ),
    ])
}

struct ToolArgs<'a> {
    value: &'a JsonValue,
}

impl<'a> ToolArgs<'a> {
    fn new(value: &'a JsonValue) -> Result<Self, McpToolError> {
        match value {
            JsonValue::Null | JsonValue::Object(_) => Ok(Self { value }),
            _ => Err(McpToolError::validation("expected arguments object")),
        }
    }

    fn reject_unknown(&self, allowed: &[&str]) -> Result<(), McpToolError> {
        let Some(fields) = self.value.as_object() else {
            return Ok(());
        };
        for (key, _) in fields {
            if !allowed.contains(&key.as_str()) {
                return Err(McpToolError::validation(format!(
                    "unexpected argument {key}"
                )));
            }
        }
        Ok(())
    }

    fn required_string(&self, key: &str) -> Result<String, McpToolError> {
        self.optional_string(key)?
            .ok_or_else(|| McpToolError::validation(format!("expected {key}")))
    }

    fn optional_string(&self, key: &str) -> Result<Option<String>, McpToolError> {
        match self.value.get(key) {
            None | Some(JsonValue::Null) => Ok(None),
            Some(value) => value
                .as_str()
                .map(|value| Some(value.to_owned()))
                .ok_or_else(|| McpToolError::validation(format!("expected {key}"))),
        }
    }

    fn optional_string_alias(
        &self,
        keys: &[&str],
        canonical: &str,
    ) -> Result<Option<String>, McpToolError> {
        for key in keys {
            if self.value.get(key).is_some() {
                return match self.value.get(key) {
                    None | Some(JsonValue::Null) => Ok(None),
                    Some(value) => value
                        .as_str()
                        .map(|value| Some(value.to_owned()))
                        .ok_or_else(|| McpToolError::validation(format!("expected {canonical}"))),
                };
            }
        }
        Ok(None)
    }

    fn optional_bool(&self, key: &str) -> Result<Option<bool>, McpToolError> {
        match self.value.get(key) {
            None | Some(JsonValue::Null) => Ok(None),
            Some(value) => value
                .as_bool()
                .map(Some)
                .ok_or_else(|| McpToolError::validation(format!("expected {key}"))),
        }
    }

    fn optional_i64(&self, key: &str) -> Result<Option<i64>, McpToolError> {
        match self.value.get(key) {
            None | Some(JsonValue::Null) => Ok(None),
            Some(value) => integer_value(value)
                .map(Some)
                .ok_or_else(|| McpToolError::validation(format!("expected {key}"))),
        }
    }

    fn optional_usize(&self, key: &str, default: usize, max: usize) -> Result<usize, McpToolError> {
        let Some(value) = self.value.get(key) else {
            return Ok(default);
        };
        if matches!(value, JsonValue::Null) {
            return Ok(default);
        }
        let value = usize_value(value)
            .ok_or_else(|| McpToolError::validation(format!("expected {key}")))?;
        if value < 1 {
            return Err(McpToolError::validation(format!(
                "expected {key} between 1 and {max}"
            )));
        }
        Ok(value.min(max))
    }

    fn optional_u8(&self, key: &str, default: u8, max: u8) -> Result<u8, McpToolError> {
        let Some(value) = self.value.get(key) else {
            return Ok(default);
        };
        if matches!(value, JsonValue::Null) {
            return Ok(default);
        }
        let value = usize_value(value)
            .ok_or_else(|| McpToolError::validation(format!("expected {key}")))?;
        if value > max as usize {
            return Err(McpToolError::validation(format!(
                "expected {key} to be at most {max}"
            )));
        }
        Ok(value as u8)
    }

    fn optional_string_array(&self, key: &str) -> Result<Vec<String>, McpToolError> {
        let Some(value) = self.value.get(key) else {
            return Ok(Vec::new());
        };
        if matches!(value, JsonValue::Null) {
            return Ok(Vec::new());
        }
        let Some(values) = value.as_array() else {
            return Err(McpToolError::validation(format!(
                "expected {key} to be an array of strings"
            )));
        };
        values
            .iter()
            .map(|value| {
                value.as_str().map(str::to_owned).ok_or_else(|| {
                    McpToolError::validation(format!("expected {key} entries to be strings"))
                })
            })
            .collect()
    }

    fn optional_i64_array(&self, key: &str) -> Result<Vec<i64>, McpToolError> {
        let Some(value) = self.value.get(key) else {
            return Ok(Vec::new());
        };
        if matches!(value, JsonValue::Null) {
            return Ok(Vec::new());
        }
        let Some(values) = value.as_array() else {
            return Err(McpToolError::validation(format!(
                "expected {key} to be an array of integers"
            )));
        };
        values
            .iter()
            .map(|value| {
                integer_value(value).ok_or_else(|| {
                    McpToolError::validation(format!("expected {key} entries to be integers"))
                })
            })
            .collect()
    }
}

fn integer_value(value: &JsonValue) -> Option<i64> {
    value
        .as_i64()
        .or_else(|| value.as_u64().and_then(|value| i64::try_from(value).ok()))
}

fn usize_value(value: &JsonValue) -> Option<usize> {
    match integer_value(value)? {
        value if value >= 0 => usize::try_from(value).ok(),
        _ => None,
    }
}

fn resource_definitions() -> Vec<JsonValue> {
    [
        ("memory://index", "Memory index"),
        ("memory://recent", "Recent memory notes"),
        ("memory://config", "Memory server config"),
    ]
    .into_iter()
    .map(|(uri, name)| {
        JsonValue::object([
            ("uri", JsonValue::from(uri)),
            ("name", JsonValue::from(name)),
            ("mimeType", JsonValue::from("application/json")),
        ])
    })
    .collect()
}

fn call_tool(config: &McpConfig, params: &JsonValue) -> JsonValue {
    let Some(name) = params.get("name").and_then(JsonValue::as_str) else {
        return McpToolError::validation("expected tool name").payload();
    };
    let args = params.get("arguments").unwrap_or(&JsonValue::Null);
    match match name {
        "memory_health" => memory_health(config, args),
        "memory_doctor" => memory_doctor(config, args),
        "memory_get" => memory_get(config, args),
        "memory_list" => memory_list(config, args),
        "memory_search" => memory_search(config, args),
        "memory_context" => memory_context(config, args),
        "memory_sync" => memory_sync(config, args),
        "memory_add" => memory_add(config, args),
        _ => Err(McpToolError::new("unknown_tool", "unknown memory tool")),
    } {
        Ok(payload) => payload,
        Err(error) => error.payload(),
    }
}

fn read_resource(config: &McpConfig, params: &JsonValue) -> JsonValue {
    let Some(uri) = params.get("uri").and_then(JsonValue::as_str) else {
        return resource_text(
            "memory://error",
            McpToolError::validation("expected uri").payload(),
        );
    };
    let payload = if uri == "memory://index" || uri == "memory://recent" {
        memory_list(
            config,
            &JsonValue::object([("limit", JsonValue::from(10usize))]),
        )
        .unwrap_or_else(|error| error.payload())
    } else if uri == "memory://config" {
        memory_health(config, &JsonValue::Null).unwrap_or_else(|error| error.payload())
    } else if let Some(path) = uri.strip_prefix("memory://note/") {
        memory_get(
            config,
            &JsonValue::object([("id_or_uri", JsonValue::from(path))]),
        )
        .unwrap_or_else(|error| error.payload())
    } else if let Some(tag) = uri.strip_prefix("memory://tag/") {
        memory_list(
            config,
            &JsonValue::object([("tags", JsonValue::array([tag.into()]))]),
        )
        .unwrap_or_else(|error| error.payload())
    } else {
        McpToolError::new("resource_not_found", "unknown memory resource").payload()
    };
    resource_text(uri, payload)
}

fn resource_text(uri: &str, payload: JsonValue) -> JsonValue {
    JsonValue::object([(
        "contents",
        JsonValue::array([JsonValue::object([
            ("uri", JsonValue::from(uri)),
            ("mimeType", JsonValue::from("application/json")),
            ("text", JsonValue::from(json::stringify(&payload))),
        ])]),
    )])
}

#[derive(Clone, Debug)]
struct HarnessDiagnostics {
    deep: bool,
    db_path: PathBuf,
    db_exists: bool,
    markdown_root: Option<PathBuf>,
    markdown_root_exists: bool,
    read_only: bool,
    safe_for_writes: bool,
    write_status: &'static str,
    db_last_tx_id: Option<u64>,
    db_open_error: Option<String>,
    storage_recovered_tail: Option<bool>,
}

impl HarnessDiagnostics {
    fn status(&self) -> &'static str {
        if self.db_open_error.is_some() {
            "fail"
        } else if !self.markdown_root_exists || self.storage_recovered_tail == Some(true) {
            "warn"
        } else {
            "pass"
        }
    }

    fn health_payload(&self) -> JsonValue {
        JsonValue::object([
            ("ok", true.into()),
            ("db_path", path_json(&self.db_path)),
            ("db_exists", self.db_exists.into()),
            (
                "markdown_root",
                self.markdown_root
                    .as_ref()
                    .map(path_json)
                    .unwrap_or(JsonValue::Null),
            ),
            ("markdown_root_exists", self.markdown_root_exists.into()),
            ("read_only", self.read_only.into()),
            ("safe_for_writes", self.safe_for_writes.into()),
            ("write_status", JsonValue::from(self.write_status)),
            ("sync_visibility", JsonValue::from(SYNC_VISIBILITY_MESSAGE)),
            (
                "db_last_tx_id",
                self.db_last_tx_id
                    .map(JsonValue::from)
                    .unwrap_or(JsonValue::Null),
            ),
        ])
    }

    fn doctor_payload(&self) -> JsonValue {
        let mut fields = vec![
            ("ok", JsonValue::from(self.status() != "fail")),
            ("tool", JsonValue::from("memory_doctor")),
            ("status", JsonValue::from(self.status())),
            (
                "diagnostic_mode",
                JsonValue::from(if self.deep { "deep" } else { "light" }),
            ),
            (
                "db",
                JsonValue::object([
                    ("path", path_json(&self.db_path)),
                    ("exists", self.db_exists.into()),
                    (
                        "last_tx_id",
                        self.db_last_tx_id
                            .map(JsonValue::from)
                            .unwrap_or(JsonValue::Null),
                    ),
                ]),
            ),
            (
                "markdown_root",
                JsonValue::object([
                    (
                        "path",
                        self.markdown_root
                            .as_ref()
                            .map(path_json)
                            .unwrap_or(JsonValue::Null),
                    ),
                    ("exists", self.markdown_root_exists.into()),
                ]),
            ),
            (
                "server",
                JsonValue::object([
                    ("read_only", self.read_only.into()),
                    ("safe_for_writes", self.safe_for_writes.into()),
                    ("write_status", JsonValue::from(self.write_status)),
                    ("sync_visibility", JsonValue::from(SYNC_VISIBILITY_MESSAGE)),
                ]),
            ),
            ("checks", JsonValue::array(self.doctor_checks())),
            ("next_actions", JsonValue::array(self.next_actions())),
            ("provenance", provenance()),
        ];
        if let Some(recovered_tail) = self.storage_recovered_tail {
            fields.push((
                "storage",
                JsonValue::object([("recovered_tail", recovered_tail.into())]),
            ));
        }
        JsonValue::object(fields)
    }

    fn next_actions(&self) -> Vec<JsonValue> {
        let mut actions = Vec::new();
        if self.db_open_error.is_some() {
            actions.push(JsonValue::from(
                "verify --db points at a readable local cupld database",
            ));
        }
        if !self.markdown_root_exists {
            actions.push(JsonValue::from(
                "create the markdown root, pass --root, or run cupld source set-root",
            ));
        }
        if !self.safe_for_writes && !self.read_only {
            actions.push(JsonValue::from(
                "create the markdown root before calling memory_add or memory_sync",
            ));
        }
        actions.push(JsonValue::from(
            "run memory_sync after markdown changes before expecting DB-backed MCP reads to see them",
        ));
        if self.storage_recovered_tail == Some(true) {
            actions.push(JsonValue::from(
                "run cupld check --db and consider compacting the database",
            ));
        }
        actions
    }

    fn doctor_checks(&self) -> Vec<JsonValue> {
        let mut checks = Vec::new();
        checks.push(doctor_check(
            "db_open",
            if self.db_open_error.is_some() {
                "fail"
            } else {
                "pass"
            },
            self.db_open_error
                .as_deref()
                .unwrap_or("database opened successfully"),
            self.db_open_error
                .as_ref()
                .map(|_| "verify --db points at a readable .cupld file"),
        ));
        checks.push(doctor_check(
            "markdown_root_exists",
            if self.markdown_root_exists {
                "pass"
            } else {
                "warn"
            },
            if self.markdown_root_exists {
                "configured markdown root exists"
            } else {
                "configured markdown root does not exist"
            },
            (!self.markdown_root_exists)
                .then_some("create the root directory, pass --root, or run cupld source set-root"),
        ));
        checks.push(doctor_check(
            "write_readiness",
            if self.safe_for_writes || self.read_only {
                "pass"
            } else {
                "warn"
            },
            match self.write_status {
                "ready" => "memory_add and memory_sync are available",
                "read_only" => "write tools are disabled by --read-only",
                _ => "write tools need an existing markdown root",
            },
            (!self.safe_for_writes && !self.read_only)
                .then_some("create the markdown root before calling memory_add or memory_sync"),
        ));
        checks.push(doctor_check(
            "sync_visibility",
            "pass",
            SYNC_VISIBILITY_MESSAGE,
            None,
        ));
        if let Some(recovered_tail) = self.storage_recovered_tail {
            checks.push(doctor_check(
                "storage_integrity",
                if recovered_tail { "warn" } else { "pass" },
                if recovered_tail {
                    "database check recovered WAL tail records"
                } else {
                    "database storage check passed without recovery"
                },
                recovered_tail
                    .then_some("run cupld check --db and consider compacting the database"),
            ));
        }
        checks
    }
}

fn collect_harness_diagnostics(config: &McpConfig, deep: bool) -> HarnessDiagnostics {
    let db_path = config.db_path.clone();
    let db_exists = db_path.exists();
    let session = open_session(config);
    let (markdown_root, db_last_tx_id, db_open_error) = match session {
        Ok(session) => (
            resolve_markdown_root(config, Some(&session)),
            Some(session.transaction_info().last_tx_id),
            None,
        ),
        Err(error) => (resolve_markdown_root(config, None), None, Some(error)),
    };
    let markdown_root_exists = markdown_root.as_ref().is_some_and(|path| path.exists());
    let safe_for_writes = !config.read_only && markdown_root_exists && db_open_error.is_none();
    let write_status = if config.read_only {
        "read_only"
    } else if markdown_root_exists {
        "ready"
    } else {
        "markdown_root_missing"
    };
    let storage_recovered_tail = if deep && db_open_error.is_none() {
        Session::check(&db_path)
            .ok()
            .map(|report| report.recovered_tail)
    } else {
        None
    };
    HarnessDiagnostics {
        deep,
        db_path,
        db_exists,
        markdown_root,
        markdown_root_exists,
        read_only: config.read_only,
        safe_for_writes,
        write_status,
        db_last_tx_id,
        db_open_error,
        storage_recovered_tail,
    }
}

fn doctor_check(code: &str, status: &str, message: &str, remediation: Option<&str>) -> JsonValue {
    let mut fields = vec![
        ("code".to_owned(), JsonValue::from(code)),
        ("status".to_owned(), JsonValue::from(status)),
        ("message".to_owned(), JsonValue::from(message)),
    ];
    if let Some(remediation) = remediation {
        fields.push(("remediation".to_owned(), JsonValue::from(remediation)));
    }
    JsonValue::Object(fields)
}

fn memory_health(config: &McpConfig, args: &JsonValue) -> Result<JsonValue, McpToolError> {
    let args = ToolArgs::new(args)?;
    args.reject_unknown(&[])?;
    let diagnostics = collect_harness_diagnostics(config, false);
    if let Some(error) = diagnostics.db_open_error.as_deref() {
        return Err(McpToolError::new("db_open_failed", error));
    }
    Ok(diagnostics.health_payload())
}

fn memory_doctor(config: &McpConfig, args: &JsonValue) -> Result<JsonValue, McpToolError> {
    let args = ToolArgs::new(args)?;
    args.reject_unknown(&["deep"])?;
    let deep = args.optional_bool("deep")?.unwrap_or(false);
    Ok(collect_harness_diagnostics(config, deep).doctor_payload())
}

fn memory_context(config: &McpConfig, args: &JsonValue) -> Result<JsonValue, McpToolError> {
    let args = ToolArgs::new(args)?;
    args.reject_unknown(&[
        "id_or_uri",
        "path",
        "paths",
        "node",
        "nodes",
        "depth",
        "direction",
        "edge_types",
        "labels",
        "max_nodes",
        "max_edges",
    ])?;
    let mut seeds = Vec::new();
    for node in args.optional_i64_array("nodes")? {
        seeds.push(ContextSeedRequest::Node(usize::try_from(node).map_err(
            |_| McpToolError::validation("expected nodes entries to be non-negative integers"),
        )?));
    }
    for path in args.optional_string_array("paths")? {
        seeds.push(ContextSeedRequest::Path(path));
    }
    if let Some(node) = args.optional_i64("node")? {
        seeds.push(ContextSeedRequest::Node(usize::try_from(node).map_err(
            |_| McpToolError::validation("expected node to be a non-negative integer"),
        )?));
    }
    if let Some(path) = args.optional_string("path")? {
        seeds.push(ContextSeedRequest::Path(path));
    }
    if let Some(uri) = args.optional_string("id_or_uri")? {
        let doc = find_one_doc(config, &uri).map_err(|error| match error.code.as_str() {
            "not_found" => {
                McpToolError::new("not_found", "expected exactly one matching memory note")
            }
            _ => error,
        })?;
        seeds.push(ContextSeedRequest::Path(doc.path));
    }
    if seeds.is_empty() {
        return Err(McpToolError::validation(
            "expected node, nodes, path, paths, or id_or_uri",
        ));
    }

    let depth = args.optional_u8("depth", 1, crate::MAX_TRAVERSAL_DEPTH)?;
    let direction = match args
        .optional_string("direction")?
        .unwrap_or_else(|| "both".to_owned())
        .trim()
    {
        "in" => ContextDirection::In,
        "out" => ContextDirection::Out,
        "both" | "" => ContextDirection::Both,
        _ => {
            return Err(McpToolError::validation(
                "expected direction to be in, out, or both",
            ));
        }
    };
    let request = ContextRequest {
        db_path: config.db_path.clone(),
        nodes: seeds
            .iter()
            .filter_map(|seed| match seed {
                ContextSeedRequest::Node(node) => Some(*node),
                ContextSeedRequest::Path(_) => None,
            })
            .collect(),
        paths: seeds
            .iter()
            .filter_map(|seed| match seed {
                ContextSeedRequest::Node(_) => None,
                ContextSeedRequest::Path(path) => Some(path.clone()),
            })
            .collect(),
        seeds,
        depth,
        direction,
        edge_types: args.optional_string_array("edge_types")?,
        labels: args.optional_string_array("labels")?,
        max_nodes: args.optional_usize("max_nodes", 25, 250)?,
        max_edges: args.optional_usize("max_edges", 100, 1_000)?,
    };
    match request.run() {
        Ok(envelope) => json::parse(&context_as_json(&envelope))
            .map_err(|error| McpToolError::new("context_serialization_failed", error.to_string())),
        Err(error) => Err(McpToolError::with_details(
            error.code(),
            error.message(),
            JsonValue::object([("source", JsonValue::from("context"))]),
        )),
    }
}

fn memory_get(config: &McpConfig, args: &JsonValue) -> Result<JsonValue, McpToolError> {
    let args = ToolArgs::new(args)?;
    args.reject_unknown(&["id_or_uri", "max_chars"])?;
    let needle = args.required_string("id_or_uri")?;
    let max_chars = args.optional_usize("max_chars", DEFAULT_BODY_CHARS, 20_000)?;
    let doc = find_one_doc(config, &needle)?;
    let (body, truncated) = truncate(&doc.body, max_chars);
    Ok(JsonValue::object([
        ("ok", true.into()),
        ("item", doc.to_json_with_body(&body)),
        ("truncated", truncated.into()),
        ("provenance", provenance()),
    ]))
}

fn memory_list(config: &McpConfig, args: &JsonValue) -> Result<JsonValue, McpToolError> {
    let args = ToolArgs::new(args)?;
    args.reject_unknown(&["limit", "tags"])?;
    let limit = args.optional_usize("limit", DEFAULT_LIMIT, MAX_LIMIT)?;
    let tags = args.optional_string_array("tags")?;
    match load_docs(config) {
        Ok(docs) => Ok(list_payload(filter_tags(docs, &tags), limit)),
        Err(error) => Err(McpToolError::new("db_query_failed", error)),
    }
}

fn memory_search(config: &McpConfig, args: &JsonValue) -> Result<JsonValue, McpToolError> {
    let args = ToolArgs::new(args)?;
    args.reject_unknown(&[
        "query",
        "limit",
        "tags",
        "retrieval_mode",
        "mode",
        "retrieval",
    ])?;
    let query = args.required_string("query")?;
    let query = query.trim();
    if query.is_empty() {
        return Err(McpToolError::validation("expected non-empty query"));
    }
    let limit = args.optional_usize("limit", DEFAULT_LIMIT, MAX_LIMIT)?;
    let tags = args.optional_string_array("tags")?;
    let retrieval_mode = SearchRetrievalMode::from_args(&args)?;
    if retrieval_mode.is_semantic() {
        return Ok(SemanticSearchBackend::unconfigured().search(retrieval_mode, query, limit));
    }
    let search_query = SearchQuery::new(query);
    match load_search_docs(config, query, &tags) {
        Ok((docs, structural_index, index_used)) => Ok(search_payload(
            query,
            score_search_docs(docs, &search_query, &tags, &structural_index),
            limit,
            index_used,
            structural_index.has_filesystem_graph_data(),
        )),
        Err(error) => Err(McpToolError::new("db_query_failed", error)),
    }
}

pub fn memory_search_payload_for_db(
    db_path: PathBuf,
    query: &str,
    tags: &[String],
    limit: usize,
) -> Result<JsonValue, String> {
    let query = query.trim();
    if query.is_empty() {
        return Err("expected non-empty query".to_owned());
    }
    let search_query = SearchQuery::new(query);
    let config = McpConfig {
        db_path,
        root_override: None,
        read_only: true,
    };
    let (docs, structural_index, index_used) = load_search_docs(&config, query, tags)?;
    Ok(search_payload(
        query,
        score_search_docs(docs, &search_query, tags, &structural_index),
        limit.min(MAX_LIMIT),
        index_used,
        structural_index.has_filesystem_graph_data(),
    ))
}

fn list_payload(docs: Vec<MemoryDoc>, limit: usize) -> JsonValue {
    let truncated = docs.len() > limit;
    JsonValue::object([
        ("ok", true.into()),
        (
            "items",
            JsonValue::array(
                docs.into_iter()
                    .take(limit)
                    .map(|doc| doc.to_item_json(DEFAULT_SNIPPET_CHARS)),
            ),
        ),
        ("truncated", truncated.into()),
        ("provenance", provenance()),
    ])
}

fn search_payload(
    query: &str,
    docs: Vec<(SearchMatch, MemoryDoc)>,
    limit: usize,
    index_used: bool,
    structural_signal_available: bool,
) -> JsonValue {
    let truncated = docs.len() > limit;
    JsonValue::object([
        ("ok", true.into()),
        ("query", JsonValue::from(query.to_owned())),
        (
            "retrieval",
            JsonValue::object([
                ("mode", JsonValue::from("lexical")),
                ("deterministic", true.into()),
                ("semantic", false.into()),
                ("index_used", index_used.into()),
                (
                    "ranking_policy",
                    JsonValue::from(
                        "ascending deterministic lexical score, weak filesystem structural signal for lexical ties, then ascending path",
                    ),
                ),
                (
                    "score_policy",
                    JsonValue::from(
                        "lower score is more relevant: exact title/path, partial title/path, alias/tag/heading, body, with multi-term coverage bonuses inside each tier",
                    ),
                ),
                (
                    "structural_signal_available",
                    structural_signal_available.into(),
                ),
                (
                    "structural_signal_policy",
                    JsonValue::from(
                        "opt-in filesystem graph edges can provide a weak deterministic tie-break signal; lexical score remains primary",
                    ),
                ),
            ]),
        ),
        (
            "items",
            JsonValue::array(docs.into_iter().take(limit).enumerate().map(
                |(index, (search_match, doc))| {
                    doc.to_search_item_json(&search_match, index + 1, DEFAULT_SNIPPET_CHARS)
                },
            )),
        ),
        ("truncated", truncated.into()),
        ("provenance", provenance()),
    ])
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SearchRetrievalMode {
    Lexical,
    Semantic,
    Vector,
}

impl SearchRetrievalMode {
    fn from_args(args: &ToolArgs<'_>) -> Result<Self, McpToolError> {
        let Some(mode) =
            args.optional_string_alias(&["retrieval_mode", "mode", "retrieval"], "retrieval_mode")?
        else {
            return Ok(Self::Lexical);
        };
        match mode.trim().to_ascii_lowercase().as_str() {
            "" | "lexical" => Ok(Self::Lexical),
            "semantic" => Ok(Self::Semantic),
            "vector" => Ok(Self::Vector),
            _ => Err(McpToolError::validation(
                "expected retrieval_mode to be lexical, semantic, or vector",
            )),
        }
    }

    fn is_semantic(self) -> bool {
        matches!(self, Self::Semantic | Self::Vector)
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Lexical => "lexical",
            Self::Semantic => "semantic",
            Self::Vector => "vector",
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum SemanticSearchBackend {
    Unconfigured,
}

impl SemanticSearchBackend {
    fn unconfigured() -> Self {
        Self::Unconfigured
    }

    fn search(self, mode: SearchRetrievalMode, query: &str, _limit: usize) -> JsonValue {
        match self {
            Self::Unconfigured => semantic_unconfigured_payload(mode, query),
        }
    }
}

fn semantic_unconfigured_payload(mode: SearchRetrievalMode, query: &str) -> JsonValue {
    JsonValue::object([
        ("ok", false.into()),
        ("query", JsonValue::from(query.to_owned())),
        (
            "retrieval",
            JsonValue::object([
                ("mode", JsonValue::from(mode.as_str())),
                ("deterministic", true.into()),
                ("semantic", true.into()),
                ("index_used", false.into()),
                ("backend", JsonValue::from("unconfigured")),
                ("network_used", false.into()),
                (
                    "ranking_policy",
                    JsonValue::from(
                        "semantic retrieval is opt-in and requires precomputed local vector data",
                    ),
                ),
                (
                    "score_policy",
                    JsonValue::from(
                        "semantic_score and blended_score are unavailable until a local semantic backend is configured",
                    ),
                ),
            ]),
        ),
        ("items", JsonValue::array([])),
        ("truncated", false.into()),
        ("provenance", provenance()),
        (
            "error",
            McpToolError::new(
                "unconfigured",
                "semantic memory_search requires an explicitly configured local vector backend",
            )
            .error_object(),
        ),
    ])
}

fn memory_sync(config: &McpConfig, args: &JsonValue) -> Result<JsonValue, McpToolError> {
    let args = ToolArgs::new(args)?;
    args.reject_unknown(&[])?;
    if config.read_only {
        return Err(McpToolError::new(
            "read_only",
            "memory_sync is disabled in read-only mode",
        ));
    }
    match sync_configured_root(config) {
        Ok(report) => Ok(JsonValue::object([
            ("ok", true.into()),
            ("report", report_json(&report)),
            ("db_updated", true.into()),
        ])),
        Err(error) => Err(McpToolError::new("sync_failed", error)),
    }
}

fn memory_add(config: &McpConfig, args: &JsonValue) -> Result<JsonValue, McpToolError> {
    let args = ToolArgs::new(args)?;
    args.reject_unknown(&["content", "title", "tags", "path_hint", "source"])?;
    if config.read_only {
        return Err(McpToolError::new(
            "read_only",
            "memory_add is disabled in read-only mode",
        ));
    }
    let content = args.required_string("content")?;
    let session = match open_session(config) {
        Ok(session) => session,
        Err(error) => return Err(McpToolError::new("db_open_failed", error)),
    };
    let root = match resolve_markdown_root(config, Some(&session)) {
        Some(root) => root,
        None => {
            return Err(McpToolError::new(
                "markdown_root_missing",
                "markdown root is not configured",
            ));
        }
    };
    let title = args
        .optional_string("title")?
        .unwrap_or_else(|| "Memory Note".to_owned());
    let path_hint = args.optional_string("path_hint")?;
    let relative_path = match safe_relative_path(&title, path_hint.as_deref()) {
        Ok(path) => path,
        Err(error) => return Err(McpToolError::new("invalid_path", error)),
    };
    let note_path = root.join(&relative_path);
    if let Some(parent) = note_path.parent()
        && let Err(error) = fs::create_dir_all(parent)
    {
        return Err(McpToolError::new(
            "markdown_write_failed",
            error.to_string(),
        ));
    }
    if let Err(error) = ensure_confined_write(&root, &note_path) {
        return Err(McpToolError::new("invalid_path", error));
    }
    let tags = args.optional_string_array("tags")?;
    let source = args.optional_string("source")?;
    let markdown = note_markdown(&content, &title, &tags, source.as_deref());
    if let Err(error) = fs::write(&note_path, markdown) {
        return Err(McpToolError::new(
            "markdown_write_failed",
            error.to_string(),
        ));
    }
    match sync_configured_root(config) {
        Ok(report) => Ok(JsonValue::object([
            ("ok", true.into()),
            ("note_path", JsonValue::from(path_to_string(&relative_path))),
            (
                "uri",
                JsonValue::from(format!("memory://note/{}", path_to_string(&relative_path))),
            ),
            ("sync_report", report_json(&report)),
            ("db_updated", true.into()),
            ("status", JsonValue::from("ok")),
        ])),
        Err(error) => Ok(JsonValue::object([
            ("ok", false.into()),
            ("note_path", JsonValue::from(path_to_string(&relative_path))),
            ("db_updated", false.into()),
            ("status", JsonValue::from("markdown_written_sync_failed")),
            (
                "error",
                McpToolError::new("sync_failed", error).error_object(),
            ),
        ])),
    }
}

fn sync_configured_root(config: &McpConfig) -> Result<MarkdownSyncReport, String> {
    let mut session = open_session(config)?;
    let root = resolve_markdown_root(config, Some(&session))
        .ok_or_else(|| "markdown root is not configured".to_owned())?;
    let mut engine = session.engine().clone();
    let report = sync_markdown_root(&mut engine, &root).map_err(|error| error.to_string())?;
    engine.commit().map_err(|error| error.to_string())?;
    session
        .replace_engine(engine)
        .map_err(|error| error.to_string())?;
    session.save().map_err(|error| error.to_string())?;
    Ok(report)
}

fn open_session(config: &McpConfig) -> Result<Session, String> {
    Session::open(&config.db_path).map_err(|error| error.to_string())
}

fn resolve_markdown_root(config: &McpConfig, session: Option<&Session>) -> Option<PathBuf> {
    if let Some(root) = &config.root_override {
        return Some(root.clone());
    }
    let package = WorkspacePackage::discover_current().ok()?;
    if let Some(root) = package.configured_markdown_root() {
        return Some(root);
    }
    if let Some(session) = session
        && let Some(root) = configured_markdown_root(session.engine())
    {
        return Some(root);
    }
    Some(package.default_markdown_root())
}

fn load_docs(config: &McpConfig) -> Result<Vec<MemoryDoc>, String> {
    let mut session = open_session(config)?;
    load_docs_from_session(&mut session)
}

fn find_one_doc(config: &McpConfig, id_or_uri: &str) -> Result<MemoryDoc, McpToolError> {
    let matches = find_docs_by_identity(config, id_or_uri)
        .map_err(|error| McpToolError::new("db_query_failed", error))?;
    if matches.len() == 1 {
        Ok(matches.into_iter().next().expect("single match"))
    } else {
        Err(McpToolError::with_details(
            "not_found",
            "expected exactly one matching memory note",
            JsonValue::object([
                ("id_or_uri", JsonValue::from(id_or_uri.to_owned())),
                ("matches", JsonValue::from(matches.len())),
            ]),
        ))
    }
}

fn find_docs_by_identity(config: &McpConfig, id_or_uri: &str) -> Result<Vec<MemoryDoc>, String> {
    let lookup = normalize_memory_lookup(id_or_uri);
    let mut session = open_session(config)?;
    let mut docs = Vec::new();
    let mut seen_ids = BTreeSet::new();

    if let Ok(id) = lookup.parse::<i64>()
        && id >= 0
    {
        extend_unique_docs(
            &mut docs,
            &mut seen_ids,
            query_memory_docs(&mut session, &format!("WHERE id(d) = {id}"))?,
        );
    }

    extend_unique_docs(
        &mut docs,
        &mut seen_ids,
        query_memory_docs(
            &mut session,
            &format!("WHERE d.`src.path` = {}", cypher_string(&lookup)),
        )?,
    );
    extend_unique_docs(
        &mut docs,
        &mut seen_ids,
        query_memory_docs(
            &mut session,
            &format!("WHERE d.`md.title` = {}", cypher_string(&lookup)),
        )?,
    );

    if !docs
        .iter()
        .any(|doc| doc.title.eq_ignore_ascii_case(&lookup))
    {
        let matching_title_ids = query_memory_doc_identities(&mut session)?
            .into_iter()
            .filter_map(|(id, title)| title.eq_ignore_ascii_case(&lookup).then_some(id))
            .collect::<Vec<_>>();
        for id in matching_title_ids {
            extend_unique_docs(
                &mut docs,
                &mut seen_ids,
                query_memory_docs(&mut session, &format!("WHERE id(d) = {id}"))?,
            );
        }
    }

    Ok(docs)
}

fn normalize_memory_lookup(id_or_uri: &str) -> String {
    id_or_uri
        .strip_prefix("memory://note/")
        .unwrap_or(id_or_uri)
        .to_owned()
}

fn extend_unique_docs(
    docs: &mut Vec<MemoryDoc>,
    seen_ids: &mut BTreeSet<i64>,
    more: Vec<MemoryDoc>,
) {
    for doc in more {
        if seen_ids.insert(doc.id) {
            docs.push(doc);
        }
    }
}

fn load_search_docs(
    config: &McpConfig,
    query: &str,
    tags: &[String],
) -> Result<(Vec<MemoryDoc>, StructuralIndex, bool), String> {
    let mut session = open_session(config)?;
    let indexes = markdown_search_indexes(&session);
    let structural_index = load_structural_index(&mut session)?;
    let search_query = SearchQuery::new(query);
    if tags.is_empty() && indexes.body_fulltext && search_query.terms.len() == 1 {
        let (docs, index_used) = load_indexed_body_candidates(&mut session, query)?;
        return Ok((docs, structural_index, index_used));
    }
    if !tags.is_empty() && indexes.tags_list {
        let (docs, index_used) = load_indexed_tag_candidates(&mut session, tags)?;
        return Ok((docs, structural_index, index_used));
    }
    Ok((
        load_docs_from_session(&mut session)?,
        structural_index,
        false,
    ))
}

fn load_docs_from_session(session: &mut Session) -> Result<Vec<MemoryDoc>, String> {
    let result = session
        .execute_script(
            "MATCH (d:MarkdownDocument)
             RETURN id(d), d.`src.path`, d.`md.title`, d.`md.tags`, d.`md.aliases`, d.`md.headings`, d.`md.body`, d.`md.raw`, d.`src.status`
             ORDER BY d.`src.path`",
            &BTreeMap::new(),
        )
        .map_err(|error| error.to_string())?
        .remove(0);
    Ok(result.rows.into_iter().map(MemoryDoc::from_row).collect())
}

fn query_memory_docs(session: &mut Session, where_clause: &str) -> Result<Vec<MemoryDoc>, String> {
    let result = session
        .execute_script(
            &format!(
                "MATCH (d:MarkdownDocument)
                 {where_clause}
                 RETURN id(d), d.`src.path`, d.`md.title`, d.`md.tags`, d.`md.aliases`, d.`md.headings`, d.`md.body`, d.`md.raw`, d.`src.status`
                 ORDER BY d.`src.path`"
            ),
            &BTreeMap::new(),
        )
        .map_err(|error| error.to_string())?
        .remove(0);
    Ok(result.rows.into_iter().map(MemoryDoc::from_row).collect())
}

fn query_memory_doc_identities(session: &mut Session) -> Result<Vec<(i64, String)>, String> {
    let result = session
        .execute_script(
            "MATCH (d:MarkdownDocument)
             RETURN id(d), d.`md.title`
             ORDER BY d.`src.path`",
            &BTreeMap::new(),
        )
        .map_err(|error| error.to_string())?
        .remove(0);
    Ok(result
        .rows
        .into_iter()
        .map(|row| (int_at(&row, 0), string_at(&row, 1)))
        .collect())
}

fn load_indexed_body_candidates(
    session: &mut Session,
    query: &str,
) -> Result<(Vec<MemoryDoc>, bool), String> {
    let indexed = session
        .execute_script(
            &format!(
                "MATCH (d:MarkdownDocument)
                 WHERE d.`md.body` CONTAINS {}
                 RETURN id(d), d.`src.path`, d.`md.title`, d.`md.tags`, d.`md.aliases`, d.`md.headings`, d.`md.body`, d.`md.raw`, d.`src.status`
                 ORDER BY d.`src.path`",
                cypher_string(query)
            ),
            &BTreeMap::new(),
        )
        .map_err(|error| error.to_string())?
        .remove(0)
        .rows
        .into_iter()
        .map(MemoryDoc::from_row)
        .collect::<Vec<_>>();

    let indexed_ids = indexed.iter().map(|doc| doc.id).collect::<BTreeSet<_>>();
    let mut docs = indexed;
    for doc in load_docs_from_session(session)? {
        if indexed_ids.contains(&doc.id) {
            continue;
        }
        if doc.metadata_match(&query.to_ascii_lowercase()) {
            docs.push(doc);
        }
    }
    Ok((docs, true))
}

fn load_indexed_tag_candidates(
    session: &mut Session,
    tags: &[String],
) -> Result<(Vec<MemoryDoc>, bool), String> {
    let mut clauses = Vec::new();
    for tag in tags {
        clauses.push(format!("{} IN d.`md.tags`", cypher_string(tag)));
    }
    let result = session
        .execute_script(
            &format!(
                "MATCH (d:MarkdownDocument)
                 WHERE {}
                 RETURN id(d), d.`src.path`, d.`md.title`, d.`md.tags`, d.`md.aliases`, d.`md.headings`, d.`md.body`, d.`md.raw`, d.`src.status`
                 ORDER BY d.`src.path`",
                clauses.join(" AND ")
            ),
            &BTreeMap::new(),
        )
        .map_err(|error| error.to_string())?
        .remove(0);
    Ok((
        result.rows.into_iter().map(MemoryDoc::from_row).collect(),
        true,
    ))
}

#[derive(Clone, Copy, Debug, Default)]
struct MarkdownSearchIndexes {
    body_fulltext: bool,
    tags_list: bool,
}

fn markdown_search_indexes(session: &Session) -> MarkdownSearchIndexes {
    let mut indexes = MarkdownSearchIndexes::default();
    for index in session.engine().show_indexes(None) {
        if index.target_kind == "label"
            && index.target_name == "MarkdownDocument"
            && index.status == "ready"
        {
            if index.property == "md.body" && index.kind == "fulltext" {
                indexes.body_fulltext = true;
            }
            if index.property == "md.tags" && index.kind == "list" {
                indexes.tags_list = true;
            }
        }
    }
    indexes
}

fn score_search_docs(
    docs: Vec<MemoryDoc>,
    search_query: &SearchQuery,
    tags: &[String],
    structural_index: &StructuralIndex,
) -> Vec<(SearchMatch, MemoryDoc)> {
    let mut scored = filter_tags(docs, tags)
        .into_iter()
        .filter_map(|doc| {
            doc.search_match(search_query)
                .map(|search_match| (search_match, doc))
        })
        .collect::<Vec<_>>();
    structural_index.apply(&mut scored);
    scored.sort_by(|(left_match, left), (right_match, right)| {
        left_match
            .score
            .cmp(&right_match.score)
            .then_with(|| {
                right_match
                    .structural_signal
                    .score
                    .cmp(&left_match.structural_signal.score)
            })
            .then_with(|| left.path.cmp(&right.path))
    });
    scored
}

fn cypher_string(input: &str) -> String {
    format!("'{}'", input.replace('\\', "\\\\").replace('\'', "\\'"))
}

#[derive(Clone, Debug)]
struct MemoryDoc {
    id: i64,
    path: String,
    title: String,
    tags: Vec<String>,
    aliases: Vec<String>,
    headings: Vec<String>,
    body: String,
    raw: String,
}

impl MemoryDoc {
    fn from_row(row: Vec<RuntimeValue>) -> Self {
        Self {
            id: int_at(&row, 0),
            path: string_at(&row, 1),
            title: string_at(&row, 2),
            tags: string_list_at(&row, 3),
            aliases: string_list_at(&row, 4),
            headings: string_list_at(&row, 5),
            body: string_at(&row, 6),
            raw: string_at(&row, 7),
        }
    }

    fn to_item_json(&self, max_chars: usize) -> JsonValue {
        let (snippet, _) = self.snippet(max_chars);
        self.to_json_with_body(&snippet)
    }

    fn to_search_item_json(
        &self,
        search_match: &SearchMatch,
        rank: usize,
        max_chars: usize,
    ) -> JsonValue {
        let (snippet, truncated, snippet_source) = self.search_snippet(search_match, max_chars);
        let mut fields = match self.to_json_with_body(&snippet) {
            JsonValue::Object(fields) => fields,
            _ => Vec::new(),
        };
        fields.push(("rank".to_owned(), JsonValue::from(rank)));
        fields.push((
            "score".to_owned(),
            JsonValue::from(search_match.score as usize),
        ));
        fields.push((
            "lexical_score".to_owned(),
            JsonValue::from(search_match.score as usize),
        ));
        fields.push(("semantic_score".to_owned(), JsonValue::Null));
        fields.push(("blended_score".to_owned(), JsonValue::Null));
        fields.push((
            "matched_fields".to_owned(),
            JsonValue::array(
                search_match
                    .fields
                    .iter()
                    .map(|field| JsonValue::from(*field)),
            ),
        ));
        fields.push((
            "matched_category".to_owned(),
            JsonValue::from(search_match.category),
        ));
        fields.push((
            "structural_signal".to_owned(),
            search_match.structural_signal.to_json(),
        ));
        fields.push((
            "snippet_metadata".to_owned(),
            JsonValue::object([
                ("source", JsonValue::from(snippet_source)),
                ("max_chars", JsonValue::from(max_chars)),
                ("truncated", truncated.into()),
                ("empty_body_fallback", self.body.is_empty().into()),
            ]),
        ));
        JsonValue::Object(fields)
    }

    fn snippet(&self, max_chars: usize) -> (String, bool) {
        truncate(
            if self.body.is_empty() {
                &self.raw
            } else {
                &self.body
            },
            max_chars,
        )
    }

    fn search_snippet(
        &self,
        search_match: &SearchMatch,
        max_chars: usize,
    ) -> (String, bool, &'static str) {
        if search_match.fields.contains(&"headings")
            && let Some(heading) = best_matching_text(&self.headings, &search_match.terms)
        {
            let (snippet, truncated) = truncate(heading, max_chars);
            return (snippet, truncated, "headings");
        }
        if search_match.fields.contains(&"body")
            && let Some(line) = best_matching_line(&self.body, &search_match.terms)
        {
            let (snippet, truncated) = truncate(line, max_chars);
            return (snippet, truncated, "body");
        }
        let (snippet, truncated) = self.snippet(max_chars);
        let source = if self.body.is_empty() { "raw" } else { "body" };
        (snippet, truncated, source)
    }

    fn to_json_with_body(&self, body: &str) -> JsonValue {
        JsonValue::object([
            ("id", JsonValue::from(self.id)),
            (
                "uri",
                JsonValue::from(format!("memory://note/{}", self.path)),
            ),
            ("path", JsonValue::from(self.path.clone())),
            ("title", JsonValue::from(self.title.clone())),
            (
                "tags",
                JsonValue::array(self.tags.iter().cloned().map(JsonValue::from)),
            ),
            ("snippet", JsonValue::from(body.to_owned())),
            ("updated_at", JsonValue::Null),
        ])
    }

    fn search_match(&self, query: &SearchQuery) -> Option<SearchMatch> {
        let title = self.title.to_ascii_lowercase();
        let path = self.path.to_ascii_lowercase();
        let exact_title = title == query.normalized;
        let exact_path = path == query.normalized;
        if exact_title || exact_path {
            return Some(SearchMatch::new(
                0,
                "exact_title_or_path",
                matched_fields([("title", exact_title), ("path", exact_path)]),
                query.terms.clone(),
            ));
        }
        let mut score = LexicalScore::new();
        score.add_field("title", 100, &title, query);
        score.add_field("path", 110, &path, query);
        score.add_values("aliases", 200, &self.aliases, query);
        score.add_values("tags", 210, &self.tags, query);
        score.add_values("headings", 220, &self.headings, query);
        score.add_field("body", 300, &self.body.to_ascii_lowercase(), query);
        score.into_match()
    }

    fn metadata_match(&self, query: &str) -> bool {
        let title = self.title.to_ascii_lowercase();
        let path = self.path.to_ascii_lowercase();
        title == query
            || path == query
            || title.contains(query)
            || path.contains(query)
            || self
                .tags
                .iter()
                .any(|value| value.to_ascii_lowercase().contains(query))
            || self
                .aliases
                .iter()
                .any(|value| value.to_ascii_lowercase().contains(query))
            || self
                .headings
                .iter()
                .any(|value| value.to_ascii_lowercase().contains(query))
    }
}

#[derive(Clone, Debug)]
struct SearchMatch {
    score: usize,
    category: &'static str,
    fields: Vec<&'static str>,
    terms: Vec<String>,
    structural_signal: StructuralSignal,
}

impl SearchMatch {
    fn new(
        score: usize,
        category: &'static str,
        fields: Vec<&'static str>,
        terms: Vec<String>,
    ) -> Self {
        Self {
            score,
            category,
            fields,
            terms,
            structural_signal: StructuralSignal::default(),
        }
    }
}

#[derive(Clone, Debug, Default)]
struct StructuralSignal {
    score: usize,
    evidence: Vec<StructuralEvidence>,
}

impl StructuralSignal {
    fn to_json(&self) -> JsonValue {
        JsonValue::object([
            ("score", JsonValue::from(self.score)),
            (
                "evidence",
                JsonValue::array(self.evidence.iter().map(StructuralEvidence::to_json)),
            ),
        ])
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct StructuralEvidence {
    kind: &'static str,
    via: String,
    edge_types: Vec<&'static str>,
    edge_weight: usize,
}

impl Ord for StructuralEvidence {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.kind
            .cmp(other.kind)
            .then_with(|| self.via.cmp(&other.via))
            .then_with(|| self.edge_types.cmp(&other.edge_types))
    }
}

impl PartialOrd for StructuralEvidence {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl StructuralEvidence {
    fn to_json(&self) -> JsonValue {
        JsonValue::object([
            ("kind", JsonValue::from(self.kind)),
            ("via", JsonValue::from(self.via.clone())),
            (
                "edge_types",
                JsonValue::array(
                    self.edge_types
                        .iter()
                        .map(|edge_type| JsonValue::from(*edge_type)),
                ),
            ),
            ("edge_weight", JsonValue::from(self.edge_weight)),
        ])
    }
}

#[derive(Clone, Debug, Default)]
struct StructuralIndex {
    doc_directories: BTreeMap<i64, Vec<DirectoryEdge>>,
    directory_parents: BTreeMap<String, Vec<DirectoryEdge>>,
}

impl StructuralIndex {
    fn has_filesystem_graph_data(&self) -> bool {
        !self.doc_directories.is_empty() || !self.directory_parents.is_empty()
    }

    fn apply(&self, scored: &mut [(SearchMatch, MemoryDoc)]) {
        if !self.has_filesystem_graph_data() || scored.len() < 2 {
            return;
        }
        let Some(best_score) = scored
            .iter()
            .map(|(search_match, _)| search_match.score)
            .min()
        else {
            return;
        };
        let anchor_ids = scored
            .iter()
            .filter_map(|(search_match, doc)| (search_match.score == best_score).then_some(doc.id))
            .collect::<BTreeSet<_>>();
        for (search_match, doc) in scored.iter_mut() {
            search_match.structural_signal = self.signal_for(doc.id, &anchor_ids);
        }
    }

    fn signal_for(&self, doc_id: i64, anchor_ids: &BTreeSet<i64>) -> StructuralSignal {
        let mut evidence = BTreeSet::new();
        let Some(doc_dirs) = self.doc_directories.get(&doc_id) else {
            return StructuralSignal::default();
        };
        for anchor_id in anchor_ids {
            if *anchor_id == doc_id {
                continue;
            }
            let Some(anchor_dirs) = self.doc_directories.get(anchor_id) else {
                continue;
            };
            for doc_dir in doc_dirs {
                for anchor_dir in anchor_dirs {
                    if doc_dir.path == anchor_dir.path {
                        evidence.insert(StructuralEvidence {
                            kind: "same_directory",
                            via: doc_dir.path.clone(),
                            edge_types: vec![MD_IN_DIRECTORY],
                            edge_weight: doc_dir.edge_weight.min(anchor_dir.edge_weight),
                        });
                    }
                    if self.has_parent(&doc_dir.path, &anchor_dir.path) {
                        evidence.insert(StructuralEvidence {
                            kind: "parent_child_directory",
                            via: anchor_dir.path.clone(),
                            edge_types: vec![MD_IN_DIRECTORY, MD_PARENT_DIRECTORY],
                            edge_weight: doc_dir.edge_weight.min(anchor_dir.edge_weight),
                        });
                    }
                    if self.has_parent(&anchor_dir.path, &doc_dir.path) {
                        evidence.insert(StructuralEvidence {
                            kind: "parent_child_directory",
                            via: doc_dir.path.clone(),
                            edge_types: vec![MD_IN_DIRECTORY, MD_PARENT_DIRECTORY],
                            edge_weight: doc_dir.edge_weight.min(anchor_dir.edge_weight),
                        });
                    }
                }
            }
        }
        let evidence = evidence.into_iter().collect::<Vec<_>>();
        StructuralSignal {
            score: evidence.iter().map(|item| item.edge_weight).sum(),
            evidence,
        }
    }

    fn has_parent(&self, child: &str, parent: &str) -> bool {
        self.directory_parents
            .get(child)
            .is_some_and(|parents| parents.iter().any(|edge| edge.path == parent))
    }
}

#[derive(Clone, Debug)]
struct DirectoryEdge {
    path: String,
    edge_weight: usize,
}

fn load_structural_index(session: &mut Session) -> Result<StructuralIndex, String> {
    let doc_rows = session
        .execute_script(
            "MATCH (d:MarkdownDocument)-[e:MD_IN_DIRECTORY]->(dir:MarkdownDirectory)
             RETURN id(d), dir.`src.path`, e.`md.edge_weight`
             ORDER BY id(d), dir.`src.path`",
            &BTreeMap::new(),
        )
        .map_err(|error| error.to_string())?
        .remove(0)
        .rows;
    let mut doc_directories: BTreeMap<i64, Vec<DirectoryEdge>> = BTreeMap::new();
    for row in doc_rows {
        doc_directories
            .entry(int_at(&row, 0))
            .or_default()
            .push(DirectoryEdge {
                path: string_at(&row, 1),
                edge_weight: edge_weight_at(&row, 2),
            });
    }

    let parent_rows = session
        .execute_script(
            "MATCH (child:MarkdownDirectory)-[e:MD_PARENT_DIRECTORY]->(parent:MarkdownDirectory)
             RETURN child.`src.path`, parent.`src.path`, e.`md.edge_weight`
             ORDER BY child.`src.path`, parent.`src.path`",
            &BTreeMap::new(),
        )
        .map_err(|error| error.to_string())?
        .remove(0)
        .rows;
    let mut directory_parents: BTreeMap<String, Vec<DirectoryEdge>> = BTreeMap::new();
    for row in parent_rows {
        directory_parents
            .entry(string_at(&row, 0))
            .or_default()
            .push(DirectoryEdge {
                path: string_at(&row, 1),
                edge_weight: edge_weight_at(&row, 2),
            });
    }

    Ok(StructuralIndex {
        doc_directories,
        directory_parents,
    })
}

#[derive(Clone, Debug)]
struct SearchQuery {
    normalized: String,
    terms: Vec<String>,
}

impl SearchQuery {
    fn new(query: &str) -> Self {
        let normalized = query.to_ascii_lowercase();
        let mut terms = normalized
            .split_ascii_whitespace()
            .map(ToOwned::to_owned)
            .collect::<Vec<_>>();
        terms.sort();
        terms.dedup();
        Self { normalized, terms }
    }
}

#[derive(Clone, Debug)]
struct LexicalScore {
    score: usize,
    category: &'static str,
    fields: Vec<&'static str>,
    terms: Vec<String>,
}

impl LexicalScore {
    fn new() -> Self {
        Self {
            score: usize::MAX,
            category: "",
            fields: Vec::new(),
            terms: Vec::new(),
        }
    }

    fn add_field(
        &mut self,
        field: &'static str,
        tier: usize,
        normalized_value: &str,
        query: &SearchQuery,
    ) {
        let matched_terms = matched_terms(normalized_value, query);
        if matched_terms.is_empty() {
            return;
        }
        let score = lexical_score(tier, matched_terms.len(), query.terms.len());
        if score < self.score {
            self.score = score;
            self.category = category_for_field(field);
            self.fields.clear();
            self.terms = matched_terms.clone();
        }
        if score == self.score {
            push_unique_field(&mut self.fields, field);
            merge_terms(&mut self.terms, matched_terms);
        }
    }

    fn add_values(
        &mut self,
        field: &'static str,
        tier: usize,
        values: &[String],
        query: &SearchQuery,
    ) {
        for value in values {
            self.add_field(field, tier, &value.to_ascii_lowercase(), query);
        }
    }

    fn into_match(self) -> Option<SearchMatch> {
        (self.score != usize::MAX)
            .then(|| SearchMatch::new(self.score, self.category, self.fields, self.terms))
    }
}

fn lexical_score(tier: usize, matched_terms: usize, query_terms: usize) -> usize {
    tier + query_terms.saturating_sub(matched_terms)
}

fn category_for_field(field: &str) -> &'static str {
    match field {
        "title" | "path" => "partial_title_or_path",
        "aliases" | "tags" | "headings" => "structured_metadata",
        "body" => "body",
        _ => "lexical",
    }
}

fn matched_terms(normalized_value: &str, query: &SearchQuery) -> Vec<String> {
    let mut terms = if normalized_value.contains(&query.normalized) {
        query.terms.clone()
    } else {
        query
            .terms
            .iter()
            .filter(|term| normalized_value.contains(term.as_str()))
            .cloned()
            .collect()
    };
    terms.sort();
    terms.dedup();
    terms
}

fn matched_term_count(normalized_value: &str, terms: &[String]) -> usize {
    terms
        .iter()
        .filter(|term| normalized_value.contains(term.as_str()))
        .count()
}

fn merge_terms(target: &mut Vec<String>, terms: Vec<String>) {
    for term in terms {
        if !target.contains(&term) {
            target.push(term);
        }
    }
    target.sort();
}

fn push_unique_field(fields: &mut Vec<&'static str>, field: &'static str) {
    if !fields.contains(&field) {
        fields.push(field);
    }
}

fn best_matching_text<'a>(values: &'a [String], terms: &[String]) -> Option<&'a str> {
    values
        .iter()
        .filter_map(|value| {
            let count = matched_term_count(&value.to_ascii_lowercase(), terms);
            (count > 0).then_some((count, value.as_str()))
        })
        .max_by(|(left_count, left), (right_count, right)| {
            left_count.cmp(right_count).then_with(|| right.cmp(left))
        })
        .map(|(_, value)| value)
}

fn best_matching_line<'a>(body: &'a str, terms: &[String]) -> Option<&'a str> {
    body.lines()
        .filter_map(|line| {
            let count = matched_term_count(&line.to_ascii_lowercase(), terms);
            (count > 0).then_some((count, line))
        })
        .max_by(|(left_count, left), (right_count, right)| {
            left_count.cmp(right_count).then_with(|| right.cmp(left))
        })
        .map(|(_, line)| line)
}

fn matched_fields<const N: usize>(fields: [(&'static str, bool); N]) -> Vec<&'static str> {
    fields
        .into_iter()
        .filter_map(|(field, matched)| matched.then_some(field))
        .collect()
}

fn filter_tags(docs: Vec<MemoryDoc>, tags: &[String]) -> Vec<MemoryDoc> {
    if tags.is_empty() {
        return docs;
    }
    docs.into_iter()
        .filter(|doc| {
            tags.iter()
                .all(|tag| doc.tags.iter().any(|doc_tag| doc_tag == tag))
        })
        .collect()
}

fn safe_relative_path(title: &str, path_hint: Option<&str>) -> Result<PathBuf, String> {
    let raw = path_hint
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| format!("{}.md", slug(title)));
    let path = PathBuf::from(raw);
    if path.is_absolute() {
        return Err("path_hint must stay under markdown root".to_owned());
    }
    if path.components().any(|component| {
        matches!(
            component,
            Component::ParentDir | Component::RootDir | Component::Prefix(_)
        )
    }) {
        return Err("path_hint must stay under markdown root".to_owned());
    }
    if path.extension().and_then(|extension| extension.to_str()) != Some("md") {
        return Err("path_hint must end with .md".to_owned());
    }
    Ok(path)
}

fn ensure_confined_write(root: &Path, note_path: &Path) -> Result<(), String> {
    let root = root
        .canonicalize()
        .map_err(|error| format!("cannot canonicalize markdown root: {error}"))?;
    let parent = note_path
        .parent()
        .ok_or_else(|| "note path has no parent".to_owned())?
        .canonicalize()
        .map_err(|error| format!("cannot canonicalize note parent: {error}"))?;
    if !parent.starts_with(&root) {
        return Err("path_hint must stay under markdown root".to_owned());
    }
    Ok(())
}

fn note_markdown(content: &str, title: &str, tags: &[String], source: Option<&str>) -> String {
    let mut output = String::new();
    output.push_str("---\n");
    output.push_str("title: ");
    output.push_str(&yaml_scalar(title));
    output.push('\n');
    if !tags.is_empty() {
        output.push_str("tags: [");
        for (index, tag) in tags.iter().enumerate() {
            if index > 0 {
                output.push_str(", ");
            }
            output.push_str(&yaml_scalar(tag));
        }
        output.push_str("]\n");
    }
    if let Some(source) = source {
        output.push_str("source: ");
        output.push_str(&yaml_scalar(source));
        output.push('\n');
    }
    output.push_str("---\n\n");
    output.push_str(content);
    output.push('\n');
    output
}

fn yaml_scalar(input: &str) -> String {
    let mut output = String::from("\"");
    for ch in input.chars() {
        match ch {
            '"' => output.push_str("\\\""),
            '\\' => output.push_str("\\\\"),
            '\n' | '\r' => output.push(' '),
            ch => output.push(ch),
        }
    }
    output.push('"');
    output
}

fn slug(input: &str) -> String {
    let mut output = String::new();
    let mut last_dash = false;
    for ch in input.chars() {
        if ch.is_ascii_alphanumeric() {
            output.push(ch.to_ascii_lowercase());
            last_dash = false;
        } else if !last_dash {
            output.push('-');
            last_dash = true;
        }
    }
    let output = output.trim_matches('-');
    if output.is_empty() {
        "memory-note".to_owned()
    } else {
        output.to_owned()
    }
}

fn truncate(input: &str, max_chars: usize) -> (String, bool) {
    let mut output = String::new();
    for (index, ch) in input.chars().enumerate() {
        if index >= max_chars {
            return (output, true);
        }
        output.push(ch);
    }
    (output, false)
}

fn string_at(row: &[RuntimeValue], index: usize) -> String {
    match row.get(index) {
        Some(RuntimeValue::String(value)) => value.clone(),
        _ => String::new(),
    }
}

fn string_list_at(row: &[RuntimeValue], index: usize) -> Vec<String> {
    match row.get(index) {
        Some(RuntimeValue::List(values)) => values
            .iter()
            .filter_map(|value| match value {
                RuntimeValue::String(value) => Some(value.clone()),
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    }
}

fn int_at(row: &[RuntimeValue], index: usize) -> i64 {
    match row.get(index) {
        Some(RuntimeValue::Int(value)) => *value,
        _ => 0,
    }
}

fn edge_weight_at(row: &[RuntimeValue], index: usize) -> usize {
    match row.get(index) {
        Some(RuntimeValue::Float(value)) if value.is_finite() && *value > 0.0 => {
            (*value * 100.0).round() as usize
        }
        Some(RuntimeValue::Int(value)) if *value > 0 => *value as usize,
        _ => 0,
    }
}

fn path_json(path: impl AsRef<Path>) -> JsonValue {
    JsonValue::from(path_to_string(path.as_ref()))
}

fn path_to_string(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

fn provenance() -> JsonValue {
    JsonValue::object([
        ("source", JsonValue::from("cupld_db")),
        ("markdown_source", JsonValue::from("configured_local_root")),
        ("network_used", JsonValue::from(false)),
    ])
}

fn report_json(report: &MarkdownSyncReport) -> JsonValue {
    JsonValue::object([
        ("root", path_json(&report.root)),
        ("scanned_documents", report.scanned_documents.into()),
        ("upserted_documents", report.upserted_documents.into()),
        ("tombstoned_documents", report.tombstoned_documents.into()),
        ("link_edges", report.link_edges.into()),
        ("upserted_directories", report.upserted_directories.into()),
        (
            "tombstoned_directories",
            report.tombstoned_directories.into(),
        ),
        ("structural_edges", report.structural_edges.into()),
    ])
}

#[allow(dead_code)]
fn _value_json(value: &Value) -> JsonValue {
    match value {
        Value::Null => JsonValue::Null,
        Value::Bool(value) => JsonValue::from(*value),
        Value::Int(value) => JsonValue::from(*value),
        Value::Float(value) => JsonValue::from(*value),
        Value::String(value) => JsonValue::from(value.clone()),
        Value::Bytes(_) | Value::Datetime(_) | Value::List(_) | Value::Map(_) => JsonValue::Null,
    }
}
