use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
    process,
    time::{SystemTime, UNIX_EPOCH},
};

use crate::{
    CupldEngine, Edge, Node, PropertyMap, RuntimeValue, Session, Value,
    context::{ContextDirection, ContextRequest, context_as_json},
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
    options: BTreeMap<String, JsonValue>,
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
    let assertion_type = required_string(value, "type")?;
    let query = if assertion_type == "graph_snapshot" || assertion_type == "citation_metadata" {
        value
            .get("query")
            .and_then(JsonValue::as_str)
            .unwrap_or("")
            .to_owned()
    } else {
        required_string(value, "query")?
    };
    Ok(AssertionSpec {
        assertion_type,
        query,
        expected: value.get("expected").cloned().unwrap_or(JsonValue::Null),
        options: value
            .as_object()
            .map(|entries| entries.iter().cloned().collect())
            .unwrap_or_default(),
    })
}

fn run_case(fixture: &Fixture, case: &CaseSpec) -> Result<MemoryEvalCaseReport, String> {
    let sandbox = EvalSandbox::new(&fixture.name, &case.name)?;
    let db_path = sandbox.path.join("case.cupld");
    let mut engine = CupldEngine::default();
    let markdown_root = fixture.path.join("markdown");
    let vault_before = fixture.path.join("vault-before");
    let vault_after = fixture.path.join("vault-after");
    let mut warnings = Vec::new();

    if vault_before.exists() && vault_after.exists() {
        let transition_root = sandbox.path.join("vault");
        copy_dir_all(&vault_before, &transition_root).map_err(|error| {
            format!(
                "failed to stage vault-before for fixture `{}` case `{}`: {error}",
                fixture.name, case.name
            )
        })?;
        sync_fixture_markdown_root(&mut engine, &transition_root, &fixture.name, &case.name)?;
        fs::remove_dir_all(&transition_root).map_err(|error| {
            format!(
                "failed to clear staged vault for fixture `{}` case `{}`: {error}",
                fixture.name, case.name
            )
        })?;
        copy_dir_all(&vault_after, &transition_root).map_err(|error| {
            format!(
                "failed to stage vault-after for fixture `{}` case `{}`: {error}",
                fixture.name, case.name
            )
        })?;
        sync_fixture_markdown_root(&mut engine, &transition_root, &fixture.name, &case.name)?;
    } else if markdown_root.exists() {
        sync_fixture_markdown_root(&mut engine, &markdown_root, &fixture.name, &case.name)?;
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
            &db_path,
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

fn sync_fixture_markdown_root(
    engine: &mut CupldEngine,
    markdown_root: &Path,
    fixture: &str,
    case: &str,
) -> Result<(), String> {
    sync_markdown_root(engine, markdown_root).map_err(|error| {
        format!(
            "failed to sync markdown root `{}` for fixture `{fixture}` case `{case}`: {error}",
            markdown_root.display()
        )
    })?;
    Ok(())
}

fn copy_dir_all(source: &Path, destination: &Path) -> Result<(), std::io::Error> {
    fs::create_dir_all(destination)?;
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        let source_path = entry.path();
        let destination_path = destination.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_dir_all(&source_path, &destination_path)?;
        } else {
            fs::copy(&source_path, &destination_path)?;
        }
    }
    Ok(())
}

fn run_assertion(
    session: &mut Session,
    db_path: &Path,
    fixture: &str,
    case: &str,
    assertion: &AssertionSpec,
) -> Result<MemoryEvalAssertionReport, String> {
    if assertion.assertion_type == "graph_snapshot" {
        return run_graph_snapshot_assertion(session, fixture, case, assertion);
    }
    if assertion.assertion_type == "citation_metadata" {
        return run_citation_metadata_assertion(session, db_path, fixture, case, assertion);
    }

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

fn run_citation_metadata_assertion(
    session: &mut Session,
    db_path: &Path,
    fixture: &str,
    case: &str,
    assertion: &AssertionSpec,
) -> Result<MemoryEvalAssertionReport, String> {
    let source =
        assertion_option_string(assertion, "source")?.unwrap_or_else(|| "query".to_owned());
    let required_fields = assertion_option_string_array(assertion, "required_fields")?
        .unwrap_or_else(|| {
            vec![
                "src.path".to_owned(),
                "src.status".to_owned(),
                "src.hash".to_owned(),
            ]
        });
    let path_fields = assertion_option_string_array(assertion, "path_fields")?
        .unwrap_or_else(|| vec!["src.path".to_owned()]);
    let hash_fields = assertion_option_string_array(assertion, "hash_fields")?
        .unwrap_or_else(|| vec!["src.hash".to_owned()]);

    let entries = match source.as_str() {
        "graph" => citation_entries_from_graph(session.engine()),
        "query" => citation_entries_from_query(session, fixture, case, assertion)?,
        "context" => citation_entries_from_context(session, db_path, fixture, case, assertion)?,
        other => {
            return Err(format!(
                "invalid citation_metadata source `{other}` for fixture `{fixture}` case `{case}`"
            ));
        }
    };
    let failures = citation_metadata_failures(
        fixture,
        case,
        &entries,
        &required_fields,
        &path_fields,
        &hash_fields,
    );
    let status = if failures.is_empty() {
        EvalStatus::Pass
    } else {
        EvalStatus::Fail
    };
    let actual = JsonValue::object([
        ("source", JsonValue::from(source)),
        (
            "entries",
            JsonValue::array(entries.iter().map(CitationMetadataEntry::as_json)),
        ),
        (
            "failures",
            JsonValue::array(failures.iter().map(CitationMetadataFailure::as_json)),
        ),
    ]);

    Ok(MemoryEvalAssertionReport {
        fixture: fixture.to_owned(),
        case: case.to_owned(),
        assertion_type: assertion.assertion_type.clone(),
        expected: JsonValue::object([
            (
                "required_fields",
                JsonValue::array(required_fields.iter().cloned().map(JsonValue::from)),
            ),
            (
                "path_fields",
                JsonValue::array(path_fields.iter().cloned().map(JsonValue::from)),
            ),
            (
                "hash_fields",
                JsonValue::array(hash_fields.iter().cloned().map(JsonValue::from)),
            ),
        ]),
        actual,
        status,
        diff: (status == EvalStatus::Fail).then(|| {
            failures
                .iter()
                .map(CitationMetadataFailure::as_diff)
                .collect::<Vec<_>>()
                .join("; ")
        }),
    })
}

fn run_graph_snapshot_assertion(
    session: &mut Session,
    fixture: &str,
    case: &str,
    assertion: &AssertionSpec,
) -> Result<MemoryEvalAssertionReport, String> {
    let snapshot = normalized_markdown_graph_snapshot(session.engine());
    let actual = snapshot.as_json();
    let diff = compare_graph_snapshots(&snapshot, &assertion.expected).map_err(|error| {
        format!("invalid graph snapshot assertion for fixture `{fixture}` case `{case}`: {error}")
    })?;
    let status = if diff.is_empty() {
        EvalStatus::Pass
    } else {
        EvalStatus::Fail
    };

    Ok(MemoryEvalAssertionReport {
        fixture: fixture.to_owned(),
        case: case.to_owned(),
        assertion_type: assertion.assertion_type.clone(),
        expected: assertion.expected.clone(),
        actual,
        status,
        diff: (status == EvalStatus::Fail).then(|| diff.join("; ")),
    })
}

#[derive(Clone, Debug, PartialEq)]
struct CitationMetadataEntry {
    path: String,
    metadata: BTreeMap<String, JsonValue>,
}

impl CitationMetadataEntry {
    fn as_json(&self) -> JsonValue {
        JsonValue::object([
            ("path", JsonValue::from(self.path.clone())),
            (
                "metadata",
                JsonValue::object(
                    self.metadata
                        .iter()
                        .map(|(key, value)| (key.clone(), value.clone())),
                ),
            ),
        ])
    }
}

#[derive(Clone, Debug, PartialEq)]
struct CitationMetadataFailure {
    fixture: String,
    case: String,
    path: String,
    missing_field: String,
    actual: JsonValue,
}

impl CitationMetadataFailure {
    fn as_json(&self) -> JsonValue {
        JsonValue::object([
            ("fixture", JsonValue::from(self.fixture.clone())),
            ("case", JsonValue::from(self.case.clone())),
            ("path", JsonValue::from(self.path.clone())),
            ("missing_field", JsonValue::from(self.missing_field.clone())),
            ("actual", self.actual.clone()),
        ])
    }

    fn as_diff(&self) -> String {
        format!(
            "fixture `{}` case `{}` path `{}` missing `{}` actual {}",
            self.fixture,
            self.case,
            self.path,
            self.missing_field,
            json::stringify(&self.actual)
        )
    }
}

fn citation_entries_from_graph(engine: &CupldEngine) -> Vec<CitationMetadataEntry> {
    engine
        .nodes()
        .filter(|node| node.labels().contains("MarkdownDocument"))
        .filter_map(|node| {
            let metadata = normalized_properties(node.properties());
            citation_path(&metadata, &["src.path".to_owned()])
                .map(|path| CitationMetadataEntry { path, metadata })
        })
        .collect()
}

fn citation_entries_from_query(
    session: &mut Session,
    fixture: &str,
    case: &str,
    assertion: &AssertionSpec,
) -> Result<Vec<CitationMetadataEntry>, String> {
    let field_columns = assertion_field_columns(assertion)?;
    let path_fields = assertion_option_string_array(assertion, "path_fields")?
        .unwrap_or_else(|| vec!["src.path".to_owned()]);
    let results = session
        .execute_script(&assertion.query, &BTreeMap::<String, Value>::new())
        .map_err(|error| {
            format!("failed citation query for fixture `{fixture}` case `{case}`: {error}")
        })?;
    let mut entries = Vec::new();
    for result in results {
        for row in result.rows {
            let metadata = field_columns
                .iter()
                .filter_map(|(field, column)| {
                    row.get(column.saturating_sub(1))
                        .map(|value| (field.clone(), normalized_runtime_value_json(value)))
                })
                .collect::<BTreeMap<_, _>>();
            let path =
                citation_path(&metadata, &path_fields).unwrap_or_else(|| "<unknown>".to_owned());
            entries.push(CitationMetadataEntry { path, metadata });
        }
    }
    Ok(entries)
}

fn citation_entries_from_context(
    session: &mut Session,
    db_path: &Path,
    fixture: &str,
    case: &str,
    assertion: &AssertionSpec,
) -> Result<Vec<CitationMetadataEntry>, String> {
    session.save().map_err(|error| error.to_string())?;
    let seed_path = assertion_option_string(assertion, "seed_path")?
        .ok_or_else(|| {
            format!("citation_metadata context assertion for fixture `{fixture}` case `{case}` requires `seed_path`")
        })?;
    let request = ContextRequest {
        db_path: db_path.to_path_buf(),
        nodes: Vec::new(),
        paths: vec![seed_path.clone()],
        seeds: Vec::new(),
        depth: assertion_option_u64(assertion, "depth")?.unwrap_or(1) as u8,
        direction: ContextDirection::Both,
        edge_types: Vec::new(),
        labels: Vec::new(),
        max_nodes: assertion_option_u64(assertion, "max_nodes")?.unwrap_or(25) as usize,
        max_edges: assertion_option_u64(assertion, "max_edges")?.unwrap_or(100) as usize,
    };
    let response = request.run().map_err(|error| {
        format!("failed citation context for fixture `{fixture}` case `{case}`: {error}")
    })?;
    let _json_contract = context_as_json(&response);
    Ok(response
        .nodes
        .iter()
        .filter(|node| node.labels.iter().any(|label| label == "MarkdownDocument"))
        .map(|node| {
            let metadata = normalized_value_map(&node.properties);
            let path = citation_path(&metadata, &["src.path".to_owned()])
                .or_else(|| node.display.clone())
                .unwrap_or_else(|| format!("node:{}", node.node_id));
            CitationMetadataEntry { path, metadata }
        })
        .collect())
}

fn citation_metadata_failures(
    fixture: &str,
    case: &str,
    entries: &[CitationMetadataEntry],
    required_fields: &[String],
    path_fields: &[String],
    hash_fields: &[String],
) -> Vec<CitationMetadataFailure> {
    let mut failures = Vec::new();
    for entry in entries {
        for field in required_fields {
            if field == "src.hash" {
                if hash_fields
                    .iter()
                    .any(|field| present_hash(&entry.metadata, field))
                {
                    continue;
                }
            } else if present_metadata_field(&entry.metadata, field) {
                continue;
            }
            failures.push(CitationMetadataFailure {
                fixture: fixture.to_owned(),
                case: case.to_owned(),
                path: entry.path.clone(),
                missing_field: field.clone(),
                actual: JsonValue::object(
                    entry
                        .metadata
                        .iter()
                        .map(|(key, value)| (key.clone(), value.clone())),
                ),
            });
        }
        if citation_path(&entry.metadata, path_fields).is_none() {
            failures.push(CitationMetadataFailure {
                fixture: fixture.to_owned(),
                case: case.to_owned(),
                path: entry.path.clone(),
                missing_field: "stable_markdown_identity".to_owned(),
                actual: JsonValue::object(
                    entry
                        .metadata
                        .iter()
                        .map(|(key, value)| (key.clone(), value.clone())),
                ),
            });
        }
    }
    failures
}

fn present_metadata_field(metadata: &BTreeMap<String, JsonValue>, field: &str) -> bool {
    metadata
        .get(field)
        .is_some_and(|value| !matches!(value, JsonValue::Null))
}

fn present_hash(metadata: &BTreeMap<String, JsonValue>, field: &str) -> bool {
    metadata
        .get(field)
        .and_then(JsonValue::as_str)
        .is_some_and(|value| value.len() >= 8 && value.chars().all(|ch| ch.is_ascii_hexdigit()))
}

fn citation_path(metadata: &BTreeMap<String, JsonValue>, fields: &[String]) -> Option<String> {
    fields.iter().find_map(|field| {
        metadata
            .get(field)
            .and_then(JsonValue::as_str)
            .map(str::to_owned)
    })
}

fn assertion_field_columns(assertion: &AssertionSpec) -> Result<Vec<(String, usize)>, String> {
    let columns = assertion
        .options
        .get("field_columns")
        .and_then(JsonValue::as_object)
        .ok_or_else(|| "citation_metadata query assertion requires `field_columns`".to_owned())?;
    columns
        .iter()
        .map(|(field, value)| {
            let index = value.as_i64().filter(|index| *index > 0).ok_or_else(|| {
                "`field_columns` values must be positive column numbers".to_owned()
            })?;
            Ok((field.clone(), index as usize))
        })
        .collect()
}

fn assertion_option_string(assertion: &AssertionSpec, key: &str) -> Result<Option<String>, String> {
    assertion
        .options
        .get(key)
        .map(|value| {
            value
                .as_str()
                .map(str::to_owned)
                .ok_or_else(|| format!("expected `{key}` to be a string"))
        })
        .transpose()
}

fn assertion_option_u64(assertion: &AssertionSpec, key: &str) -> Result<Option<u64>, String> {
    assertion
        .options
        .get(key)
        .map(|value| {
            value
                .as_i64()
                .filter(|value| *value >= 0)
                .map(|value| value as u64)
                .ok_or_else(|| format!("expected `{key}` to be a non-negative integer"))
        })
        .transpose()
}

fn assertion_option_string_array(
    assertion: &AssertionSpec,
    key: &str,
) -> Result<Option<Vec<String>>, String> {
    assertion
        .options
        .get(key)
        .map(|value| {
            value
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
        })
        .transpose()
}

#[derive(Clone, Debug, Default, PartialEq)]
struct NormalizedGraphSnapshot {
    nodes: BTreeMap<String, NormalizedGraphNode>,
    edges: BTreeMap<String, NormalizedGraphEdge>,
}

impl NormalizedGraphSnapshot {
    fn as_json(&self) -> JsonValue {
        JsonValue::object([
            (
                "nodes",
                JsonValue::array(self.nodes.values().map(NormalizedGraphNode::as_json)),
            ),
            (
                "edges",
                JsonValue::array(self.edges.values().map(NormalizedGraphEdge::as_json)),
            ),
        ])
    }
}

#[derive(Clone, Debug, PartialEq)]
struct NormalizedGraphNode {
    key: String,
    labels: Vec<String>,
    properties: BTreeMap<String, JsonValue>,
}

impl NormalizedGraphNode {
    fn as_json(&self) -> JsonValue {
        JsonValue::object([
            ("key", JsonValue::from(self.key.clone())),
            (
                "labels",
                JsonValue::array(self.labels.iter().cloned().map(JsonValue::from)),
            ),
            (
                "properties",
                JsonValue::object(
                    self.properties
                        .iter()
                        .map(|(key, value)| (key.clone(), value.clone())),
                ),
            ),
        ])
    }
}

#[derive(Clone, Debug, PartialEq)]
struct NormalizedGraphEdge {
    key: String,
    from: String,
    edge_type: String,
    to: String,
    properties: BTreeMap<String, JsonValue>,
}

impl NormalizedGraphEdge {
    fn as_json(&self) -> JsonValue {
        JsonValue::object([
            ("key", JsonValue::from(self.key.clone())),
            ("from", JsonValue::from(self.from.clone())),
            ("type", JsonValue::from(self.edge_type.clone())),
            ("to", JsonValue::from(self.to.clone())),
            (
                "properties",
                JsonValue::object(
                    self.properties
                        .iter()
                        .map(|(key, value)| (key.clone(), value.clone())),
                ),
            ),
        ])
    }
}

fn normalized_markdown_graph_snapshot(engine: &CupldEngine) -> NormalizedGraphSnapshot {
    let node_keys = engine
        .nodes()
        .filter_map(|node| markdown_node_key(node).map(|key| (node.id(), key)))
        .collect::<BTreeMap<_, _>>();
    let nodes = engine
        .nodes()
        .filter_map(normalized_markdown_node)
        .map(|node| (node.key.clone(), node))
        .collect::<BTreeMap<_, _>>();
    let edges = engine
        .edges()
        .filter_map(|edge| normalized_markdown_edge(edge, &node_keys))
        .map(|edge| (edge.key.clone(), edge))
        .collect::<BTreeMap<_, _>>();

    NormalizedGraphSnapshot { nodes, edges }
}

fn normalized_markdown_node(node: &Node) -> Option<NormalizedGraphNode> {
    let key = markdown_node_key(node)?;
    Some(NormalizedGraphNode {
        key,
        labels: node.labels().iter().cloned().collect(),
        properties: normalized_properties(node.properties()),
    })
}

fn markdown_node_key(node: &Node) -> Option<String> {
    let path = string_property(node.property("src.path"))?;
    if node.labels().contains("MarkdownDocument") {
        Some(path.to_owned())
    } else if node.labels().contains("MarkdownDirectory") {
        Some(format!("dir:{path}"))
    } else {
        None
    }
}

fn normalized_markdown_edge(
    edge: &Edge,
    node_keys: &BTreeMap<crate::NodeId, String>,
) -> Option<NormalizedGraphEdge> {
    if !edge.edge_type().starts_with("MD_") {
        return None;
    }
    let from = node_keys.get(&edge.from())?.clone();
    let to = node_keys.get(&edge.to())?.clone();
    let key = format!("{from}|{}|{to}", edge.edge_type());
    Some(NormalizedGraphEdge {
        key,
        from,
        edge_type: edge.edge_type().to_owned(),
        to,
        properties: normalized_properties(edge.properties()),
    })
}

fn normalized_properties(properties: &PropertyMap) -> BTreeMap<String, JsonValue> {
    properties
        .iter()
        .filter(|(key, _)| !is_volatile_property(key))
        .map(|(key, value)| (key.to_owned(), value_json(value)))
        .collect()
}

fn normalized_value_map(properties: &BTreeMap<String, Value>) -> BTreeMap<String, JsonValue> {
    properties
        .iter()
        .filter(|(key, _)| !is_volatile_property(key))
        .map(|(key, value)| (key.to_owned(), value_json(value)))
        .collect()
}

fn is_volatile_property(key: &str) -> bool {
    key == "id" || key.ends_with(".id") || key.ends_with("_id")
}

fn value_json(value: &Value) -> JsonValue {
    match value {
        Value::Null => JsonValue::Null,
        Value::Bool(value) => JsonValue::from(*value),
        Value::Int(value) => JsonValue::from(*value),
        Value::Float(value) => JsonValue::from(*value),
        Value::String(value) => JsonValue::from(value.clone()),
        Value::Bytes(value) => {
            JsonValue::array(value.iter().map(|byte| JsonValue::from(u64::from(*byte))))
        }
        Value::Datetime(value) => JsonValue::from(format!("{value:?}")),
        Value::List(values) => JsonValue::array(values.iter().map(value_json)),
        Value::Map(fields) => JsonValue::object(
            fields
                .iter()
                .filter(|(key, _)| !is_volatile_property(key))
                .map(|(key, value)| (key.clone(), value_json(value))),
        ),
    }
}

fn string_property(value: Option<&Value>) -> Option<&str> {
    match value {
        Some(Value::String(value)) => Some(value),
        _ => None,
    }
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

fn compare_graph_snapshots(
    actual: &NormalizedGraphSnapshot,
    expected: &JsonValue,
) -> Result<Vec<String>, String> {
    let expected_nodes = expected_graph_nodes(expected)?;
    let expected_edges = expected_graph_edges(expected)?;
    let mut diff = Vec::new();

    diff.extend(key_set_diff(
        "node",
        actual.nodes.keys().cloned().collect(),
        expected_nodes.keys().cloned().collect(),
    ));
    diff.extend(key_set_diff(
        "edge",
        actual.edges.keys().cloned().collect(),
        expected_edges.keys().cloned().collect(),
    ));

    for (key, expected_node) in expected_nodes {
        let Some(actual_node) = actual.nodes.get(&key) else {
            continue;
        };
        diff.extend(compare_expected_properties(
            &format!("node `{key}`"),
            &actual_node.properties,
            expected_node.properties.as_ref(),
            &expected_node.required_properties,
        ));
    }

    for (key, expected_edge) in expected_edges {
        let Some(actual_edge) = actual.edges.get(&key) else {
            continue;
        };
        diff.extend(compare_expected_properties(
            &format!("edge `{key}`"),
            &actual_edge.properties,
            expected_edge.properties.as_ref(),
            &expected_edge.required_properties,
        ));
    }

    Ok(diff)
}

#[derive(Clone, Debug)]
struct ExpectedGraphEntry {
    key: String,
    properties: Option<BTreeMap<String, JsonValue>>,
    required_properties: Vec<String>,
}

fn expected_graph_nodes(
    expected: &JsonValue,
) -> Result<BTreeMap<String, ExpectedGraphEntry>, String> {
    let nodes = expected
        .get("nodes")
        .and_then(JsonValue::as_array)
        .ok_or_else(|| "graph_snapshot expected value must contain a nodes array".to_owned())?;
    nodes
        .iter()
        .map(expected_graph_node)
        .map(|entry| entry.map(|entry| (entry.key.clone(), entry)))
        .collect()
}

fn expected_graph_edges(
    expected: &JsonValue,
) -> Result<BTreeMap<String, ExpectedGraphEntry>, String> {
    let edges = expected
        .get("edges")
        .and_then(JsonValue::as_array)
        .ok_or_else(|| "graph_snapshot expected value must contain an edges array".to_owned())?;
    edges
        .iter()
        .map(expected_graph_edge)
        .map(|entry| entry.map(|entry| (entry.key.clone(), entry)))
        .collect()
}

fn expected_graph_node(value: &JsonValue) -> Result<ExpectedGraphEntry, String> {
    expected_graph_entry(value, "node", |value| {
        value
            .get("key")
            .and_then(JsonValue::as_str)
            .map(str::to_owned)
            .ok_or_else(|| "graph_snapshot node entries must contain string `key`".to_owned())
    })
}

fn expected_graph_edge(value: &JsonValue) -> Result<ExpectedGraphEntry, String> {
    expected_graph_entry(value, "edge", |value| {
        if let Some(key) = value.get("key").and_then(JsonValue::as_str) {
            return Ok(key.to_owned());
        }
        let from = value
            .get("from")
            .and_then(JsonValue::as_str)
            .ok_or_else(|| "graph_snapshot edge entries must contain string `from`".to_owned())?;
        let edge_type = value
            .get("type")
            .and_then(JsonValue::as_str)
            .ok_or_else(|| "graph_snapshot edge entries must contain string `type`".to_owned())?;
        let to = value
            .get("to")
            .and_then(JsonValue::as_str)
            .ok_or_else(|| "graph_snapshot edge entries must contain string `to`".to_owned())?;
        Ok(format!("{from}|{edge_type}|{to}"))
    })
}

fn expected_graph_entry(
    value: &JsonValue,
    kind: &str,
    key: impl FnOnce(&JsonValue) -> Result<String, String>,
) -> Result<ExpectedGraphEntry, String> {
    let properties = value
        .get("properties")
        .map(json_object_map)
        .transpose()
        .map_err(|error| format!("graph_snapshot {kind} {error}"))?;
    Ok(ExpectedGraphEntry {
        key: key(value)?,
        properties,
        required_properties: optional_json_string_array(value, "required_properties")?,
    })
}

fn json_object_map(value: &JsonValue) -> Result<BTreeMap<String, JsonValue>, String> {
    value
        .as_object()
        .ok_or_else(|| "`properties` must be an object".to_owned())
        .map(|entries| entries.iter().cloned().collect())
}

fn optional_json_string_array(value: &JsonValue, key: &str) -> Result<Vec<String>, String> {
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

fn key_set_diff(kind: &str, actual: BTreeSet<String>, expected: BTreeSet<String>) -> Vec<String> {
    let mut diff = Vec::new();
    for key in expected.difference(&actual) {
        diff.push(format!(
            "{kind} added in expected/missing in actual: `{key}`"
        ));
    }
    for key in actual.difference(&expected) {
        diff.push(format!(
            "{kind} removed from expected/present in actual: `{key}`"
        ));
    }
    diff
}

fn compare_expected_properties(
    context: &str,
    actual: &BTreeMap<String, JsonValue>,
    expected: Option<&BTreeMap<String, JsonValue>>,
    required: &[String],
) -> Vec<String> {
    let mut diff = Vec::new();
    for key in required {
        if !actual.contains_key(key) {
            diff.push(format!("{context} missing required property `{key}`"));
        }
    }
    if let Some(expected) = expected {
        for (key, expected_value) in expected {
            match actual.get(key) {
                Some(actual_value) if actual_value == expected_value => {}
                Some(actual_value) => diff.push(format!(
                    "{context} property `{key}` changed: expected {}, actual {}",
                    json::stringify(expected_value),
                    json::stringify(actual_value)
                )),
                None => diff.push(format!("{context} missing property `{key}`")),
            }
        }
    }
    diff
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

    #[test]
    fn graph_snapshot_diff_reports_shape_and_property_changes() {
        let actual = NormalizedGraphSnapshot {
            nodes: BTreeMap::from([
                (
                    "changed.md".to_owned(),
                    NormalizedGraphNode {
                        key: "changed.md".to_owned(),
                        labels: vec!["MarkdownDocument".to_owned()],
                        properties: BTreeMap::from([(
                            "src.status".to_owned(),
                            JsonValue::from("missing"),
                        )]),
                    },
                ),
                (
                    "extra.md".to_owned(),
                    NormalizedGraphNode {
                        key: "extra.md".to_owned(),
                        labels: vec!["MarkdownDocument".to_owned()],
                        properties: BTreeMap::new(),
                    },
                ),
            ]),
            edges: BTreeMap::new(),
        };
        let expected = JsonValue::object([
            (
                "nodes",
                JsonValue::array([
                    JsonValue::object([
                        ("key", JsonValue::from("changed.md")),
                        (
                            "required_properties",
                            JsonValue::array([JsonValue::from("src.hash")]),
                        ),
                        (
                            "properties",
                            JsonValue::object([("src.status", JsonValue::from("current"))]),
                        ),
                    ]),
                    JsonValue::object([("key", JsonValue::from("missing.md"))]),
                ]),
            ),
            ("edges", JsonValue::array([])),
        ]);

        let diff = compare_graph_snapshots(&actual, &expected).unwrap();

        assert!(
            diff.contains(&"node added in expected/missing in actual: `missing.md`".to_owned())
        );
        assert!(
            diff.contains(&"node removed from expected/present in actual: `extra.md`".to_owned())
        );
        assert!(
            diff.contains(&"node `changed.md` missing required property `src.hash`".to_owned())
        );
        assert!(
            diff.iter()
                .any(|entry| entry.contains("node `changed.md` property `src.status` changed"))
        );
    }

    #[test]
    fn citation_metadata_failure_reports_auditable_context() {
        let entries = vec![CitationMetadataEntry {
            path: "notes/source.md".to_owned(),
            metadata: BTreeMap::from([
                ("src.path".to_owned(), JsonValue::from("notes/source.md")),
                ("src.status".to_owned(), JsonValue::from("current")),
            ]),
        }];

        let failures = citation_metadata_failures(
            "citation_metadata",
            "missing-hash",
            &entries,
            &[
                "src.path".to_owned(),
                "src.status".to_owned(),
                "src.hash".to_owned(),
            ],
            &["src.path".to_owned()],
            &["src.hash".to_owned()],
        );

        assert_eq!(failures.len(), 1);
        let failure = failures[0].as_json();
        assert_eq!(
            failure.get("fixture").and_then(JsonValue::as_str),
            Some("citation_metadata")
        );
        assert_eq!(
            failure.get("case").and_then(JsonValue::as_str),
            Some("missing-hash")
        );
        assert_eq!(
            failure.get("path").and_then(JsonValue::as_str),
            Some("notes/source.md")
        );
        assert_eq!(
            failure.get("missing_field").and_then(JsonValue::as_str),
            Some("src.hash")
        );
        assert!(
            failure
                .get("actual")
                .and_then(JsonValue::as_object)
                .is_some_and(|actual| actual.iter().any(|(key, _)| key == "src.status"))
        );
    }
}
