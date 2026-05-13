use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    process,
    time::{SystemTime, UNIX_EPOCH},
};

use crate::{
    CupldEngine, RuntimeValue, Session, Value,
    json::{self, JsonValue},
    sync_markdown_root,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MemoryEvalConfig {
    pub fixtures: PathBuf,
    pub case: Option<String>,
    pub update_snapshots: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct MemoryEvalReport {
    pub ok: bool,
    pub command: String,
    pub fixtures_path: String,
    pub selected_case: Option<String>,
    pub summary: MemoryEvalSummary,
    pub cases: Vec<MemoryEvalCaseReport>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MemoryEvalSummary {
    pub fixtures: usize,
    pub cases: usize,
    pub passed: usize,
    pub failed: usize,
    pub warnings: usize,
}

#[derive(Clone, Debug, PartialEq)]
pub struct MemoryEvalCaseReport {
    pub fixture: String,
    pub case: String,
    pub status: EvalStatus,
    pub assertions: Vec<MemoryEvalAssertionReport>,
    pub warnings: Vec<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct MemoryEvalAssertionReport {
    pub fixture: String,
    pub case: String,
    pub assertion_type: String,
    pub expected: JsonValue,
    pub actual: JsonValue,
    pub status: EvalStatus,
    pub diff: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EvalStatus {
    Pass,
    Fail,
    Warn,
}

impl EvalStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Pass => "pass",
            Self::Fail => "fail",
            Self::Warn => "warn",
        }
    }
}

struct Fixture {
    name: String,
    path: PathBuf,
    cases: Vec<CaseSpec>,
}

struct CaseSpec {
    name: String,
    setup: Vec<String>,
    assertions: Vec<AssertionSpec>,
}

struct AssertionSpec {
    assertion_type: String,
    query: String,
    expected: JsonValue,
}

pub fn run(config: MemoryEvalConfig) -> Result<MemoryEvalReport, String> {
    let fixtures = discover_fixtures(&config.fixtures, config.case.as_deref())?;
    let mut cases = Vec::new();
    let mut summary = MemoryEvalSummary {
        fixtures: fixtures.len(),
        ..MemoryEvalSummary::default()
    };

    for fixture in fixtures {
        for case in &fixture.cases {
            let case_report = run_case(&fixture, case)?;
            summary.cases += 1;
            summary.warnings += case_report.warnings.len();
            if case_report.status == EvalStatus::Fail {
                summary.failed += 1;
            } else {
                summary.passed += 1;
            }
            cases.push(case_report);
        }
    }

    Ok(MemoryEvalReport {
        ok: summary.failed == 0,
        command: "eval memory".to_owned(),
        fixtures_path: config.fixtures.display().to_string(),
        selected_case: config.case,
        summary,
        cases,
    })
}

pub fn report_as_json(report: &MemoryEvalReport) -> String {
    json::stringify(&JsonValue::object(report_json_fields(report, false)))
}

pub fn report_as_ndjson(report: &MemoryEvalReport) -> Vec<String> {
    let mut lines = vec![json::stringify(&JsonValue::object(report_json_fields(
        report, true,
    )))];
    for case in &report.cases {
        lines.push(json::stringify(&JsonValue::object([
            ("kind", JsonValue::from("eval_memory_case")),
            ("fixture", JsonValue::from(case.fixture.clone())),
            ("case", JsonValue::from(case.case.clone())),
            ("status", JsonValue::from(case.status.as_str())),
            (
                "warnings",
                JsonValue::array(case.warnings.iter().cloned().map(JsonValue::from)),
            ),
        ])));
        for assertion in &case.assertions {
            lines.push(json::stringify(&JsonValue::object(assertion_json_fields(
                assertion,
            ))));
        }
    }
    lines
}

pub fn report_as_table(report: &MemoryEvalReport) -> String {
    let mut output = String::from("fixture | case | status | assertions | warnings\n");
    output.push_str("--------+------+--------+------------+---------\n");
    for case in &report.cases {
        output.push_str(&format!(
            "{} | {} | {} | {} | {}\n",
            case.fixture,
            case.case,
            case.status.as_str(),
            case.assertions.len(),
            case.warnings.len()
        ));
    }
    output.push_str(&format!(
        "summary | * | {} | passed={} failed={} | warnings={}\n",
        if report.ok { "pass" } else { "fail" },
        report.summary.passed,
        report.summary.failed,
        report.summary.warnings
    ));
    output
}

fn discover_fixtures(root: &Path, selected: Option<&str>) -> Result<Vec<Fixture>, String> {
    let mut entries = fs::read_dir(root)
        .map_err(|error| format!("failed to read fixtures `{}`: {error}", root.display()))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| format!("failed to read fixtures `{}`: {error}", root.display()))?;
    entries.sort_by_key(|entry| entry.file_name());

    let mut fixtures = Vec::new();
    for entry in entries {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().into_owned();
        if selected.is_some_and(|selected| selected != name) {
            continue;
        }
        let spec_path = path.join("case.json");
        if !spec_path.exists() {
            continue;
        }
        fixtures.push(Fixture {
            name,
            path,
            cases: parse_fixture_spec(&spec_path)?,
        });
    }

    if fixtures.is_empty() {
        let detail = selected
            .map(|case| format!(" matching --case `{case}`"))
            .unwrap_or_default();
        return Err(format!(
            "no memory eval fixtures{detail} found in `{}`",
            root.display()
        ));
    }

    Ok(fixtures)
}

fn parse_fixture_spec(path: &Path) -> Result<Vec<CaseSpec>, String> {
    let input = fs::read_to_string(path)
        .map_err(|error| format!("failed to read fixture `{}`: {error}", path.display()))?;
    let parsed = json::parse(&input)
        .map_err(|error| format!("failed to parse fixture `{}`: {error}", path.display()))?;
    let cases = parsed
        .get("cases")
        .and_then(JsonValue::as_array)
        .ok_or_else(|| format!("fixture `{}` must contain a cases array", path.display()))?;

    cases.iter().map(parse_case_spec).collect()
}

fn parse_case_spec(value: &JsonValue) -> Result<CaseSpec, String> {
    let name = required_string(value, "name")?;
    let setup = optional_string_array(value, "setup")?;
    let assertions = value
        .get("assertions")
        .and_then(JsonValue::as_array)
        .ok_or_else(|| format!("case `{name}` must contain an assertions array"))?
        .iter()
        .map(parse_assertion_spec)
        .collect::<Result<Vec<_>, _>>()?;

    Ok(CaseSpec {
        name,
        setup,
        assertions,
    })
}

fn parse_assertion_spec(value: &JsonValue) -> Result<AssertionSpec, String> {
    Ok(AssertionSpec {
        assertion_type: required_string(value, "type")?,
        query: required_string(value, "query")?,
        expected: value
            .get("expected")
            .cloned()
            .ok_or_else(|| "assertion must contain expected".to_owned())?,
    })
}

fn run_case(fixture: &Fixture, case: &CaseSpec) -> Result<MemoryEvalCaseReport, String> {
    let sandbox = EvalSandbox::new(&fixture.name, &case.name)?;
    let db_path = sandbox.path.join("case.cupld");
    let mut engine = CupldEngine::default();
    let markdown_root = fixture.path.join("markdown");
    let mut warnings = Vec::new();

    if markdown_root.exists() {
        sync_markdown_root(&mut engine, &markdown_root).map_err(|error| {
            format!(
                "failed to sync markdown for fixture `{}` case `{}`: {error}",
                fixture.name, case.name
            )
        })?;
    } else {
        warnings.push("fixture has no markdown directory".to_owned());
    }

    let mut session = Session::from_engine(engine);
    session
        .save_as(&db_path)
        .map_err(|error| error.to_string())?;

    for statement in &case.setup {
        session
            .execute_script(statement, &BTreeMap::<String, Value>::new())
            .map_err(|error| {
                format!(
                    "failed setup for fixture `{}` case `{}`: {error}",
                    fixture.name, case.name
                )
            })?;
    }

    let mut assertions = Vec::new();
    for assertion in &case.assertions {
        assertions.push(run_assertion(
            &mut session,
            &fixture.name,
            &case.name,
            assertion,
        )?);
    }

    let status = if assertions
        .iter()
        .any(|assertion| assertion.status == EvalStatus::Fail)
    {
        EvalStatus::Fail
    } else if !warnings.is_empty() {
        EvalStatus::Warn
    } else {
        EvalStatus::Pass
    };

    Ok(MemoryEvalCaseReport {
        fixture: fixture.name.clone(),
        case: case.name.clone(),
        status,
        assertions,
        warnings,
    })
}

fn run_assertion(
    session: &mut Session,
    fixture: &str,
    case: &str,
    assertion: &AssertionSpec,
) -> Result<MemoryEvalAssertionReport, String> {
    let results = session
        .execute_script(&assertion.query, &BTreeMap::<String, Value>::new())
        .map_err(|error| {
            format!("failed assertion query for fixture `{fixture}` case `{case}`: {error}")
        })?;
    let actual = JsonValue::array(results.iter().map(query_result_json));
    let status = if actual == assertion.expected {
        EvalStatus::Pass
    } else {
        EvalStatus::Fail
    };
    let diff = (status == EvalStatus::Fail).then(|| concise_diff(&assertion.expected, &actual));

    Ok(MemoryEvalAssertionReport {
        fixture: fixture.to_owned(),
        case: case.to_owned(),
        assertion_type: assertion.assertion_type.clone(),
        expected: assertion.expected.clone(),
        actual,
        status,
        diff,
    })
}

fn query_result_json(result: &crate::QueryResult) -> JsonValue {
    JsonValue::object([
        (
            "columns",
            JsonValue::array(result.columns.iter().cloned().map(JsonValue::from)),
        ),
        (
            "rows",
            JsonValue::array(
                result
                    .rows
                    .iter()
                    .map(|row| JsonValue::array(row.iter().map(normalized_runtime_value_json))),
            ),
        ),
    ])
}

fn normalized_runtime_value_json(value: &RuntimeValue) -> JsonValue {
    match value {
        RuntimeValue::Node(node_id) => JsonValue::from(format!("node:{}", node_id.get())),
        RuntimeValue::Edge(edge_id) => JsonValue::from(format!("edge:{}", edge_id.get())),
        _ => json::runtime_value_to_json(value),
    }
}

fn report_json_fields(report: &MemoryEvalReport, ndjson: bool) -> Vec<(String, JsonValue)> {
    let mut fields = vec![
        ("kind".to_owned(), JsonValue::from("eval_memory_suite")),
        ("ok".to_owned(), JsonValue::from(report.ok)),
        (
            "command".to_owned(),
            JsonValue::from(report.command.clone()),
        ),
        (
            "fixtures".to_owned(),
            JsonValue::from(report.fixtures_path.clone()),
        ),
        (
            "case".to_owned(),
            report
                .selected_case
                .clone()
                .map(JsonValue::from)
                .unwrap_or(JsonValue::Null),
        ),
        ("summary".to_owned(), summary_json(&report.summary)),
    ];
    if !ndjson {
        fields.push((
            "cases".to_owned(),
            JsonValue::array(report.cases.iter().map(case_json)),
        ));
    }
    fields
}

fn summary_json(summary: &MemoryEvalSummary) -> JsonValue {
    JsonValue::object([
        ("fixtures", JsonValue::from(summary.fixtures as u64)),
        ("cases", JsonValue::from(summary.cases as u64)),
        ("passed", JsonValue::from(summary.passed as u64)),
        ("failed", JsonValue::from(summary.failed as u64)),
        ("warnings", JsonValue::from(summary.warnings as u64)),
    ])
}

fn case_json(case: &MemoryEvalCaseReport) -> JsonValue {
    JsonValue::object([
        ("fixture", JsonValue::from(case.fixture.clone())),
        ("case", JsonValue::from(case.case.clone())),
        ("status", JsonValue::from(case.status.as_str())),
        (
            "warnings",
            JsonValue::array(case.warnings.iter().cloned().map(JsonValue::from)),
        ),
        (
            "assertions",
            JsonValue::array(
                case.assertions
                    .iter()
                    .map(|assertion| JsonValue::object(assertion_json_fields(assertion))),
            ),
        ),
    ])
}

fn assertion_json_fields(assertion: &MemoryEvalAssertionReport) -> Vec<(String, JsonValue)> {
    vec![
        ("kind".to_owned(), JsonValue::from("eval_memory_assertion")),
        (
            "fixture".to_owned(),
            JsonValue::from(assertion.fixture.clone()),
        ),
        ("case".to_owned(), JsonValue::from(assertion.case.clone())),
        (
            "assertion_type".to_owned(),
            JsonValue::from(assertion.assertion_type.clone()),
        ),
        ("expected".to_owned(), assertion.expected.clone()),
        ("actual".to_owned(), assertion.actual.clone()),
        (
            "status".to_owned(),
            JsonValue::from(assertion.status.as_str()),
        ),
        (
            "diff".to_owned(),
            assertion
                .diff
                .clone()
                .map(JsonValue::from)
                .unwrap_or(JsonValue::Null),
        ),
    ]
}

fn concise_diff(expected: &JsonValue, actual: &JsonValue) -> String {
    format!(
        "expected {}, actual {}",
        truncate(&json::stringify(expected), 160),
        truncate(&json::stringify(actual), 160)
    )
}

fn truncate(value: &str, max: usize) -> String {
    if value.len() <= max {
        value.to_owned()
    } else {
        format!("{}...", &value[..max])
    }
}

fn required_string(value: &JsonValue, key: &str) -> Result<String, String> {
    value
        .get(key)
        .and_then(JsonValue::as_str)
        .map(str::to_owned)
        .ok_or_else(|| format!("expected string field `{key}`"))
}

fn optional_string_array(value: &JsonValue, key: &str) -> Result<Vec<String>, String> {
    let Some(values) = value.get(key) else {
        return Ok(Vec::new());
    };
    values
        .as_array()
        .ok_or_else(|| format!("expected `{key}` to be an array"))?
        .iter()
        .map(|value| {
            value
                .as_str()
                .map(str::to_owned)
                .ok_or_else(|| format!("expected `{key}` entries to be strings"))
        })
        .collect()
}

struct EvalSandbox {
    path: PathBuf,
}

impl EvalSandbox {
    fn new(fixture: &str, case: &str) -> Result<Self, String> {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| error.to_string())?
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "cupld-memory-eval-{}-{}-{}-{nanos}",
            process::id(),
            sanitize_path_segment(fixture),
            sanitize_path_segment(case)
        ));
        fs::create_dir_all(&path).map_err(|error| {
            format!(
                "failed to create eval sandbox `{}`: {error}",
                path.display()
            )
        })?;
        Ok(Self { path })
    }
}

impl Drop for EvalSandbox {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

fn sanitize_path_segment(value: &str) -> String {
    value
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '-' })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_failed_assertion_with_expected_actual_and_diff() {
        let assertion = MemoryEvalAssertionReport {
            fixture: "basic".to_owned(),
            case: "counts".to_owned(),
            assertion_type: "query".to_owned(),
            expected: JsonValue::from(1_i64),
            actual: JsonValue::from(2_i64),
            status: EvalStatus::Fail,
            diff: Some("expected 1, actual 2".to_owned()),
        };

        let fields = assertion_json_fields(&assertion);
        let rendered = json::stringify(&JsonValue::object(fields));
        let parsed = json::parse(&rendered).unwrap();
        assert_eq!(parsed.get("expected"), Some(&JsonValue::from(1_i64)));
        assert_eq!(parsed.get("actual"), Some(&JsonValue::from(2_i64)));
        assert_eq!(
            parsed.get("diff").and_then(JsonValue::as_str),
            Some("expected 1, actual 2")
        );
    }
}
