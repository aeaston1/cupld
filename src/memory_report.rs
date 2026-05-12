use std::path::PathBuf;

use crate::{
    QueryResult, RuntimeValue,
    json::{self, JsonValue},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MemoryMaintenanceStatus {
    Pass,
    Warn,
    Fail,
}

impl MemoryMaintenanceStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pass => "pass",
            Self::Warn => "warn",
            Self::Fail => "fail",
        }
    }

    fn ok(self) -> bool {
        !matches!(self, Self::Fail)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct MemoryMaintenanceCheck {
    pub name: String,
    pub status: MemoryMaintenanceStatus,
    pub value: RuntimeValue,
    pub message: Option<String>,
}

impl MemoryMaintenanceCheck {
    pub fn new(
        name: impl Into<String>,
        status: MemoryMaintenanceStatus,
        value: RuntimeValue,
    ) -> Self {
        Self {
            name: name.into(),
            status,
            value,
            message: None,
        }
    }

    pub fn with_message(mut self, message: impl Into<String>) -> Self {
        self.message = Some(message.into());
        self
    }
}

#[derive(Clone, Debug)]
pub struct MemoryMaintenanceReport {
    pub command: &'static str,
    pub db_path: PathBuf,
    pub root: Option<PathBuf>,
    pub strict: Option<bool>,
    pub status: MemoryMaintenanceStatus,
    pub checks: Vec<MemoryMaintenanceCheck>,
    pub items: QueryResult,
}

impl MemoryMaintenanceReport {
    pub fn as_table(&self) -> String {
        let mut output = String::new();
        push_section(
            &mut output,
            "report",
            &QueryResult {
                columns: vec!["field".to_owned(), "value".to_owned()],
                rows: self.table_metadata_rows(),
            },
        );
        push_section(&mut output, "checks", &self.checks_table());
        push_section(
            &mut output,
            "items",
            &if self.items.rows.is_empty() {
                QueryResult {
                    columns: vec!["count".to_owned()],
                    rows: vec![vec![RuntimeValue::Int(0)]],
                }
            } else {
                self.items.clone()
            },
        );
        output
    }

    pub fn as_json(&self) -> String {
        json::stringify(&JsonValue::object(self.json_fields(false)))
    }

    pub fn as_ndjson(&self) -> Vec<String> {
        let mut meta = self.json_fields(true);
        meta.push((
            "check_count".to_owned(),
            JsonValue::from(self.checks.len() as i64),
        ));
        meta.push((
            "item_count".to_owned(),
            JsonValue::from(self.items.rows.len() as i64),
        ));

        let mut lines = vec![json::stringify(&JsonValue::Object(meta))];
        for (index, check) in self.checks.iter().enumerate() {
            lines.push(json::stringify(&JsonValue::object([
                ("kind", JsonValue::from("memory_check")),
                ("command", JsonValue::from(self.command)),
                ("check_index", JsonValue::from(index as i64)),
                ("name", JsonValue::from(check.name.clone())),
                ("status", JsonValue::from(check.status.as_str())),
                ("value", json::runtime_value_to_json(&check.value)),
                (
                    "message",
                    check
                        .message
                        .as_ref()
                        .map(|message| JsonValue::from(message.clone()))
                        .unwrap_or(JsonValue::Null),
                ),
            ])));
        }
        for (index, row) in self.items.rows.iter().enumerate() {
            lines.push(json::stringify(&JsonValue::object([
                ("kind", JsonValue::from("memory_item")),
                ("command", JsonValue::from(self.command)),
                ("item_index", JsonValue::from(index as i64)),
                ("item", json::row_to_json_object(&self.items.columns, row)),
            ])));
        }
        lines
    }

    fn json_fields(&self, ndjson_meta: bool) -> Vec<(String, JsonValue)> {
        let mut fields = Vec::new();
        if ndjson_meta {
            fields.push(("kind".to_owned(), JsonValue::from("memory_meta")));
        }
        fields.push(("ok".to_owned(), JsonValue::Bool(self.status.ok())));
        fields.push(("command".to_owned(), JsonValue::from(self.command)));
        fields.push(("status".to_owned(), JsonValue::from(self.status.as_str())));
        fields.push((
            "db_path".to_owned(),
            JsonValue::from(self.db_path.display().to_string()),
        ));
        fields.push((
            "root".to_owned(),
            self.root
                .as_ref()
                .map(|root| JsonValue::from(root.display().to_string()))
                .unwrap_or(JsonValue::Null),
        ));
        if let Some(strict) = self.strict {
            fields.push(("strict".to_owned(), JsonValue::Bool(strict)));
        }
        fields.push(("summary".to_owned(), self.summary_json()));
        if !ndjson_meta {
            fields.push((
                "checks".to_owned(),
                JsonValue::array(self.checks.iter().map(check_json)),
            ));
            fields.push((
                "items".to_owned(),
                json::query_result_rows_to_json(&self.items),
            ));
        }
        fields
    }

    fn summary_json(&self) -> JsonValue {
        JsonValue::Object(
            self.checks
                .iter()
                .map(|check| {
                    (
                        check.name.clone(),
                        json::runtime_value_to_json(&check.value),
                    )
                })
                .collect(),
        )
    }

    fn table_metadata_rows(&self) -> Vec<Vec<RuntimeValue>> {
        let mut rows = vec![
            row("command", self.command),
            row("status", self.status.as_str()),
            row("db_path", self.db_path.display().to_string()),
            row(
                "root",
                self.root
                    .as_ref()
                    .map(|root| root.display().to_string())
                    .unwrap_or_else(|| "null".to_owned()),
            ),
        ];
        if let Some(strict) = self.strict {
            rows.push(vec![
                RuntimeValue::String("strict".to_owned()),
                RuntimeValue::Bool(strict),
            ]);
        }
        rows
    }

    fn checks_table(&self) -> QueryResult {
        QueryResult {
            columns: vec![
                "name".to_owned(),
                "status".to_owned(),
                "value".to_owned(),
                "message".to_owned(),
            ],
            rows: self
                .checks
                .iter()
                .map(|check| {
                    vec![
                        RuntimeValue::String(check.name.clone()),
                        RuntimeValue::String(check.status.as_str().to_owned()),
                        check.value.clone(),
                        check
                            .message
                            .as_ref()
                            .map(|message| RuntimeValue::String(message.clone()))
                            .unwrap_or(RuntimeValue::Null),
                    ]
                })
                .collect(),
        }
    }
}

fn row(field: impl Into<String>, value: impl Into<String>) -> Vec<RuntimeValue> {
    vec![
        RuntimeValue::String(field.into()),
        RuntimeValue::String(value.into()),
    ]
}

fn check_json(check: &MemoryMaintenanceCheck) -> JsonValue {
    JsonValue::object([
        ("name", JsonValue::from(check.name.clone())),
        ("status", JsonValue::from(check.status.as_str())),
        ("value", json::runtime_value_to_json(&check.value)),
        (
            "message",
            check
                .message
                .as_ref()
                .map(|message| JsonValue::from(message.clone()))
                .unwrap_or(JsonValue::Null),
        ),
    ])
}

fn push_section(output: &mut String, name: &str, result: &QueryResult) {
    if !output.is_empty() {
        output.push('\n');
    }
    output.push_str(name);
    output.push('\n');
    output.push_str(&format_table(result));
}

fn format_table(result: &QueryResult) -> String {
    if result.columns.is_empty() {
        return String::new();
    }
    let mut widths = result
        .columns
        .iter()
        .map(|column| column.len())
        .collect::<Vec<_>>();
    let rows = result
        .rows
        .iter()
        .map(|row| row.iter().map(table_value).collect::<Vec<_>>())
        .collect::<Vec<_>>();
    for row in &rows {
        for (index, value) in row.iter().enumerate() {
            widths[index] = widths[index].max(value.len());
        }
    }

    let mut output = String::new();
    output.push_str(&format_table_row(&result.columns, &widths));
    output.push('\n');
    output.push_str(
        &widths
            .iter()
            .map(|width| "-".repeat(*width))
            .collect::<Vec<_>>()
            .join("-+-"),
    );
    for row in rows {
        output.push('\n');
        output.push_str(&format_table_row(&row, &widths));
    }
    output.push('\n');
    output
}

fn format_table_row(values: &[String], widths: &[usize]) -> String {
    values
        .iter()
        .enumerate()
        .map(|(index, value)| {
            if index + 1 == values.len() {
                value.clone()
            } else {
                format!("{value:width$}", width = widths[index])
            }
        })
        .collect::<Vec<_>>()
        .join(" | ")
}

fn table_value(value: &RuntimeValue) -> String {
    let rendered = value_string(value);
    if rendered.len() > 60 {
        format!("{}...", &rendered[..57])
    } else {
        rendered
    }
}

fn value_string(value: &RuntimeValue) -> String {
    match value {
        RuntimeValue::Null => "null".to_owned(),
        RuntimeValue::Bool(value) => value.to_string(),
        RuntimeValue::Int(value) => value.to_string(),
        RuntimeValue::Float(value) => value.to_string(),
        RuntimeValue::String(value) => value.clone(),
        RuntimeValue::Bytes(value) => format!("{value:?}"),
        RuntimeValue::Datetime(value) => format!("{value:?}"),
        RuntimeValue::List(values) => format!(
            "[{}]",
            values
                .iter()
                .map(value_string)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        RuntimeValue::Map(entries) => format!(
            "{{{}}}",
            entries
                .iter()
                .map(|(key, value)| format!("{key}: {}", value_string(value)))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        RuntimeValue::Node(node_id) => format!("n{}", node_id.get()),
        RuntimeValue::Edge(edge_id) => format!("e{}", edge_id.get()),
    }
}

#[cfg(test)]
mod tests {
    use super::{MemoryMaintenanceCheck, MemoryMaintenanceReport, MemoryMaintenanceStatus};
    use crate::{QueryResult, RuntimeValue, json};
    use std::path::PathBuf;

    fn sample_report() -> MemoryMaintenanceReport {
        MemoryMaintenanceReport {
            command: "memory.find-stale",
            db_path: PathBuf::from("/tmp/default.cupld"),
            root: Some(PathBuf::from("/tmp/notes")),
            strict: None,
            status: MemoryMaintenanceStatus::Warn,
            checks: vec![MemoryMaintenanceCheck::new(
                "stale_items",
                MemoryMaintenanceStatus::Warn,
                RuntimeValue::Int(1),
            )],
            items: QueryResult {
                columns: vec!["path".to_owned(), "reason".to_owned()],
                rows: vec![vec![
                    RuntimeValue::String("note.md".to_owned()),
                    RuntimeValue::String("hash_mismatch".to_owned()),
                ]],
            },
        }
    }

    #[test]
    fn json_report_exposes_status_checks_and_items() {
        let parsed = json::parse(&sample_report().as_json()).unwrap();

        assert_eq!(
            parsed.get("ok").and_then(json::JsonValue::as_bool),
            Some(true)
        );
        assert_eq!(
            parsed.get("status").and_then(json::JsonValue::as_str),
            Some("warn")
        );
        assert_eq!(
            parsed
                .get("summary")
                .and_then(|summary| summary.get("stale_items"))
                .and_then(json::JsonValue::as_i64),
            Some(1)
        );
        assert_eq!(
            parsed
                .get("checks")
                .and_then(json::JsonValue::as_array)
                .and_then(|checks| checks.first())
                .and_then(|check| check.get("status"))
                .and_then(json::JsonValue::as_str),
            Some("warn")
        );
        assert_eq!(
            parsed
                .get("items")
                .and_then(json::JsonValue::as_array)
                .and_then(|items| items.first())
                .and_then(|item| item.get("path"))
                .and_then(json::JsonValue::as_str),
            Some("note.md")
        );
    }

    #[test]
    fn ndjson_report_emits_meta_checks_then_items() {
        let lines = sample_report().as_ndjson();

        assert_eq!(lines.len(), 3);
        let meta = json::parse(&lines[0]).unwrap();
        assert_eq!(
            meta.get("kind").and_then(json::JsonValue::as_str),
            Some("memory_meta")
        );
        assert_eq!(
            meta.get("check_count").and_then(json::JsonValue::as_i64),
            Some(1)
        );
        let check = json::parse(&lines[1]).unwrap();
        assert_eq!(
            check.get("kind").and_then(json::JsonValue::as_str),
            Some("memory_check")
        );
        let item = json::parse(&lines[2]).unwrap();
        assert_eq!(
            item.get("kind").and_then(json::JsonValue::as_str),
            Some("memory_item")
        );
    }

    #[test]
    fn table_report_is_deterministic() {
        assert_eq!(
            sample_report().as_table(),
            "report\n\
field   | value\n\
--------+-------------------\n\
command | memory.find-stale\n\
status  | warn\n\
db_path | /tmp/default.cupld\n\
root    | /tmp/notes\n\
\n\
checks\n\
name        | status | value | message\n\
------------+--------+-------+--------\n\
stale_items | warn   | 1     | null\n\
\n\
items\n\
path    | reason\n\
--------+--------------\n\
note.md | hash_mismatch\n"
        );
    }

    #[test]
    fn fail_status_marks_report_not_ok() {
        let mut report = sample_report();
        report.status = MemoryMaintenanceStatus::Fail;

        let parsed = json::parse(&report.as_json()).unwrap();

        assert_eq!(
            parsed.get("ok").and_then(json::JsonValue::as_bool),
            Some(false)
        );
    }
}
