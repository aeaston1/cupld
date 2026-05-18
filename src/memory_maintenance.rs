use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use cupld::{
    MemoryMaintenanceCheck, MemoryMaintenanceReport, MemoryMaintenanceStatus, QueryResult,
    RuntimeValue, Session, Value, automation::AutomationError, markdown_alias_diagnostics,
};

use crate::{
    OutputFormat, format_command_error, open_initial_session, print_memory_report,
    resolve_markdown_root, value_string,
};

const MARKDOWN_DOCUMENT_LABEL: &str = "MarkdownDocument";
const MD_LINKS_TO: &str = "MD_LINKS_TO";
const MD_IN_DIRECTORY: &str = "MD_IN_DIRECTORY";
const MD_PARENT_DIRECTORY: &str = "MD_PARENT_DIRECTORY";
const REQUIRED_MARKDOWN_SOURCE_METADATA: [&str; 6] = [
    "src.connector",
    "src.kind",
    "src.root",
    "src.path",
    "src.hash",
    "src.status",
];

fn resolved_report_db_path(db_path: &Path, output: OutputFormat) -> Result<PathBuf, String> {
    if db_path.is_absolute() {
        return Ok(db_path.to_path_buf());
    }
    std::env::current_dir()
        .map(|cwd| cwd.join(db_path))
        .map_err(|error| {
            format_command_error(
                output,
                &AutomationError::new(
                    "memory_db_path",
                    format!("failed to resolve database path: {error}"),
                ),
            )
        })
}

fn maintenance_status_for_problem(problem: bool, strict: bool) -> MemoryMaintenanceStatus {
    match (problem, strict) {
        (true, true) => MemoryMaintenanceStatus::Fail,
        (true, false) => MemoryMaintenanceStatus::Warn,
        (false, _) => MemoryMaintenanceStatus::Pass,
    }
}

fn maintenance_report_status(checks: &[MemoryMaintenanceCheck]) -> MemoryMaintenanceStatus {
    if checks
        .iter()
        .any(|check| check.status == MemoryMaintenanceStatus::Fail)
    {
        MemoryMaintenanceStatus::Fail
    } else if checks
        .iter()
        .any(|check| check.status == MemoryMaintenanceStatus::Warn)
    {
        MemoryMaintenanceStatus::Warn
    } else {
        MemoryMaintenanceStatus::Pass
    }
}

pub(crate) fn build_memory_check_report(
    db_path: &Path,
    root_override: Option<&Path>,
    output: OutputFormat,
    strict: bool,
) -> Result<MemoryMaintenanceReport, String> {
    let report_db_path = resolved_report_db_path(db_path, output)?;
    let integrity = Session::check(db_path)
        .map_err(AutomationError::from)
        .map_err(|error| format_command_error(output, &error))?;
    let mut session = Session::open(db_path)
        .map_err(AutomationError::from)
        .map_err(|error| format_command_error(output, &error))?;
    let root =
        resolve_markdown_root(root_override.as_deref(), Some(&session)).map_err(|message| {
            format_command_error(output, &AutomationError::new("memory_root", message))
        })?;
    let stale = memory_stale_items(&mut session, &root)
        .map_err(|error| format_command_error(output, &error))?;
    let orphans =
        memory_orphan_items(&mut session).map_err(|error| format_command_error(output, &error))?;
    let alias_diagnostics = markdown_alias_diagnostics(session.engine());
    let stale_summary = memory_stale_summary(&stale);
    let metadata_summary = markdown_metadata_summary(&session);
    let duplicate_path_count = duplicate_current_markdown_path_count(&session);
    let duplicate_link_edge_count = duplicate_connector_owned_markdown_link_edge_count(&session);
    let schema_index_summary = schema_index_summary(&session);
    let has_warning = integrity.recovered_tail
        || stale_summary.missing_or_tombstoned_documents > 0
        || stale_summary.stale_current_documents > 0
        || metadata_summary.missing_required_metadata > 0
        || duplicate_path_count > 0
        || duplicate_link_edge_count > 0
        || alias_diagnostics.ambiguous_alias_count() > 0
        || schema_index_summary.non_ready_indexes > 0
        || !orphans.rows.is_empty();
    let aggregate_status = if has_warning {
        MemoryMaintenanceStatus::Warn
    } else {
        MemoryMaintenanceStatus::Pass
    };
    let checks = vec![
        MemoryMaintenanceCheck::new(
            "status",
            aggregate_status,
            RuntimeValue::String(aggregate_status.as_str().to_owned()),
        ),
        MemoryMaintenanceCheck::new(
            "last_tx_id",
            MemoryMaintenanceStatus::Pass,
            RuntimeValue::Int(integrity.last_tx_id as i64),
        ),
        MemoryMaintenanceCheck::new(
            "wal_records",
            MemoryMaintenanceStatus::Pass,
            RuntimeValue::Int(integrity.wal_records as i64),
        ),
        MemoryMaintenanceCheck::new(
            "recovered_tail",
            maintenance_status_for_problem(integrity.recovered_tail, false),
            RuntimeValue::Bool(integrity.recovered_tail),
        ),
        MemoryMaintenanceCheck::new(
            "missing_tombstoned_markdown_documents",
            maintenance_status_for_problem(
                stale_summary.missing_or_tombstoned_documents > 0,
                false,
            ),
            RuntimeValue::Int(stale_summary.missing_or_tombstoned_documents as i64),
        ),
        MemoryMaintenanceCheck::new(
            "stale_current_markdown_documents",
            maintenance_status_for_problem(stale_summary.stale_current_documents > 0, false),
            RuntimeValue::Int(stale_summary.stale_current_documents as i64),
        ),
        MemoryMaintenanceCheck::new(
            "markdown_documents_missing_source_metadata",
            maintenance_status_for_problem(metadata_summary.missing_required_metadata > 0, false),
            RuntimeValue::Int(metadata_summary.missing_required_metadata as i64),
        )
        .with_message(format!(
            "required_metadata={}",
            REQUIRED_MARKDOWN_SOURCE_METADATA.join(",")
        )),
        MemoryMaintenanceCheck::new(
            "duplicate_current_markdown_document_paths",
            maintenance_status_for_problem(duplicate_path_count > 0, false),
            RuntimeValue::Int(duplicate_path_count as i64),
        ),
        MemoryMaintenanceCheck::new(
            "duplicate_connector_owned_md_links_to_edges",
            maintenance_status_for_problem(duplicate_link_edge_count > 0, false),
            RuntimeValue::Int(duplicate_link_edge_count as i64),
        ),
        MemoryMaintenanceCheck::new(
            "schema_indexes",
            maintenance_status_for_problem(schema_index_summary.non_ready_indexes > 0, false),
            RuntimeValue::Int(schema_index_summary.total_indexes as i64),
        )
        .with_message(format!(
            "ready={} non_ready={}",
            schema_index_summary.ready_indexes, schema_index_summary.non_ready_indexes
        )),
        MemoryMaintenanceCheck::new(
            "stale_items",
            maintenance_status_for_problem(!stale.rows.is_empty(), false),
            RuntimeValue::Int(stale.rows.len() as i64),
        ),
        MemoryMaintenanceCheck::new(
            "orphan_items",
            maintenance_status_for_problem(!orphans.rows.is_empty(), false),
            RuntimeValue::Int(orphans.rows.len() as i64),
        ),
        MemoryMaintenanceCheck::new(
            "ambiguous_markdown_aliases",
            maintenance_status_for_problem(alias_diagnostics.ambiguous_alias_count() > 0, false),
            RuntimeValue::Int(alias_diagnostics.ambiguous_alias_count() as i64),
        ),
    ];
    let status = maintenance_report_status(&checks);
    let report = MemoryMaintenanceReport {
        command: "memory.check",
        db_path: report_db_path,
        root: Some(root),
        strict: Some(strict),
        status,
        checks,
        markdown_alias_diagnostics: Some(alias_diagnostics),
        items: QueryResult {
            columns: Vec::new(),
            rows: Vec::new(),
        },
    };
    Ok(report)
}

pub(crate) fn run_memory_find_stale(
    db_path: PathBuf,
    root_override: Option<PathBuf>,
    output: OutputFormat,
) -> Result<(), String> {
    let report_db_path = resolved_report_db_path(&db_path, output)?;
    let mut session = Session::open(&db_path)
        .map_err(AutomationError::from)
        .map_err(|error| format_command_error(output, &error))?;
    let root =
        resolve_markdown_root(root_override.as_deref(), Some(&session)).map_err(|message| {
            format_command_error(output, &AutomationError::new("memory_root", message))
        })?;
    let items = memory_stale_items(&mut session, &root)
        .map_err(|error| format_command_error(output, &error))?;
    let checks = vec![MemoryMaintenanceCheck::new(
        "stale_items",
        maintenance_status_for_problem(!items.rows.is_empty(), false),
        RuntimeValue::Int(items.rows.len() as i64),
    )];
    let report = MemoryMaintenanceReport {
        command: "memory.find-stale",
        db_path: report_db_path,
        root: Some(root),
        strict: None,
        status: maintenance_report_status(&checks),
        checks,
        markdown_alias_diagnostics: None,
        items,
    };
    print_memory_report(&report, output);
    Ok(())
}

pub(crate) fn run_memory_find_orphans(
    db_path: PathBuf,
    output: OutputFormat,
) -> Result<(), String> {
    let report_db_path = resolved_report_db_path(&db_path, output)?;
    let mut session = Session::open(&db_path)
        .map_err(AutomationError::from)
        .map_err(|error| format_command_error(output, &error))?;
    let items =
        memory_orphan_items(&mut session).map_err(|error| format_command_error(output, &error))?;
    let checks = vec![MemoryMaintenanceCheck::new(
        "orphan_items",
        maintenance_status_for_problem(!items.rows.is_empty(), false),
        RuntimeValue::Int(items.rows.len() as i64),
    )];
    let report = MemoryMaintenanceReport {
        command: "memory.find-orphans",
        db_path: report_db_path,
        root: None,
        strict: None,
        status: maintenance_report_status(&checks),
        checks,
        markdown_alias_diagnostics: None,
        items,
    };
    print_memory_report(&report, output);
    Ok(())
}

pub(crate) fn run_memory_reindex(db_path: PathBuf, output: OutputFormat) -> Result<(), String> {
    let report_db_path = resolved_report_db_path(&db_path, output)?;
    let session = open_initial_session(Some(db_path.clone())).map_err(|message| {
        format_command_error(output, &AutomationError::new("memory_db", message))
    })?;
    let indexes = session.engine().show_indexes(None);
    let index_count = indexes.len();
    let checks = vec![
        MemoryMaintenanceCheck::new(
            "index_count",
            MemoryMaintenanceStatus::Pass,
            RuntimeValue::Int(index_count as i64),
        ),
        MemoryMaintenanceCheck::new(
            "schema_indexes",
            MemoryMaintenanceStatus::Pass,
            RuntimeValue::String(if index_count == 0 { "none" } else { "verified" }.to_owned()),
        )
        .with_message(
            "existing schema index definitions were inspected; no new indexes were created",
        ),
    ];
    let items = QueryResult {
        columns: vec![
            "name".to_owned(),
            "target_kind".to_owned(),
            "target_name".to_owned(),
            "property".to_owned(),
            "kind".to_owned(),
            "unique".to_owned(),
            "status".to_owned(),
            "outcome".to_owned(),
        ],
        rows: indexes
            .into_iter()
            .map(|index| {
                let outcome = if index.status == "ready" {
                    "verified"
                } else {
                    "status_preserved"
                };
                vec![
                    RuntimeValue::String(index.name),
                    RuntimeValue::String(index.target_kind),
                    RuntimeValue::String(index.target_name),
                    RuntimeValue::String(index.property),
                    RuntimeValue::String(index.kind),
                    RuntimeValue::Bool(index.unique),
                    RuntimeValue::String(index.status),
                    RuntimeValue::String(outcome.to_owned()),
                ]
            })
            .collect(),
    };
    let report = MemoryMaintenanceReport {
        command: "memory.reindex",
        db_path: report_db_path,
        root: None,
        strict: None,
        status: maintenance_report_status(&checks),
        checks,
        markdown_alias_diagnostics: None,
        items,
    };
    print_memory_report(&report, output);
    Ok(())
}

fn memory_stale_items(session: &mut Session, root: &Path) -> Result<QueryResult, AutomationError> {
    let result = session
        .execute_script(
            "MATCH (d:MarkdownDocument)
             RETURN d.`src.path` AS path,
                    d.`md.title` AS title,
                    d.`src.hash` AS source_hash,
                    d.`src.root` AS source_root,
                    d.`src.status` AS status
             ORDER BY d.`src.path`",
            &BTreeMap::new(),
        )
        .map_err(AutomationError::from)?
        .into_iter()
        .next()
        .unwrap_or_else(|| QueryResult {
            columns: Vec::new(),
            rows: Vec::new(),
        });
    let mut rows = Vec::new();
    for row in result.rows {
        let path = optional_string_column(&result.columns, &row, "path")?;
        let title = optional_string_column(&result.columns, &row, "title")?;
        let source_hash = optional_string_column(&result.columns, &row, "source_hash")?;
        let source_root = optional_string_column(&result.columns, &row, "source_root")?;
        let status = optional_string_column(&result.columns, &row, "status")?;
        let metadata_incomplete =
            path.is_none() || source_hash.is_none() || source_root.is_none() || status.is_none();
        let path_for_report = path.clone().unwrap_or_default();
        let status_for_report = status.clone().unwrap_or_else(|| "unknown".to_owned());
        let root_for_report = root.display().to_string();
        if metadata_incomplete {
            push_stale_item(
                &mut rows,
                "metadata_incomplete",
                &path_for_report,
                title.as_deref(),
                &status_for_report,
                source_hash.as_deref(),
                None,
                source_root.as_deref(),
                &root_for_report,
            );
            continue;
        }

        let Some(path) = path else {
            continue;
        };
        if source_root.as_deref() != Some(root_for_report.as_str()) {
            push_stale_item(
                &mut rows,
                "root_mismatch",
                &path,
                title.as_deref(),
                &status_for_report,
                source_hash.as_deref(),
                None,
                source_root.as_deref(),
                &root_for_report,
            );
        }

        let disk_path = root.join(&path);
        match fs::read(&disk_path) {
            Ok(bytes) => {
                let disk_hash = stable_hash_hex(&bytes);
                if status.as_deref() == Some("missing") {
                    push_stale_item(
                        &mut rows,
                        "tombstoned_document",
                        &path,
                        title.as_deref(),
                        &status_for_report,
                        source_hash.as_deref(),
                        Some(&disk_hash),
                        source_root.as_deref(),
                        &root_for_report,
                    );
                } else if source_hash.as_deref() != Some(disk_hash.as_str()) {
                    push_stale_item(
                        &mut rows,
                        "hash_mismatch",
                        &path,
                        title.as_deref(),
                        &status_for_report,
                        source_hash.as_deref(),
                        Some(&disk_hash),
                        source_root.as_deref(),
                        &root_for_report,
                    );
                }
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                if status.as_deref() == Some("missing") {
                    push_stale_item(
                        &mut rows,
                        "tombstoned_document",
                        &path,
                        title.as_deref(),
                        &status_for_report,
                        source_hash.as_deref(),
                        None,
                        source_root.as_deref(),
                        &root_for_report,
                    );
                } else if status.as_deref() == Some("current") {
                    push_stale_item(
                        &mut rows,
                        "missing_file",
                        &path,
                        title.as_deref(),
                        &status_for_report,
                        source_hash.as_deref(),
                        None,
                        source_root.as_deref(),
                        &root_for_report,
                    );
                }
            }
            Err(error) => {
                return Err(AutomationError::new(
                    "memory_file_read",
                    format!("failed to read {}: {error}", disk_path.display()),
                ));
            }
        }
    }
    Ok(QueryResult {
        columns: vec![
            "kind".to_owned(),
            "path".to_owned(),
            "title".to_owned(),
            "status".to_owned(),
            "stored_hash".to_owned(),
            "current_hash".to_owned(),
            "stored_root".to_owned(),
            "resolved_root".to_owned(),
            "suggestion".to_owned(),
        ],
        rows,
    })
}

fn optional_string_column(
    columns: &[String],
    row: &[RuntimeValue],
    column: &str,
) -> Result<Option<String>, AutomationError> {
    let Some(index) = columns.iter().position(|name| name == column) else {
        return Err(AutomationError::new(
            "memory_query_contract",
            format!("missing expected `{column}` column in memory query result"),
        ));
    };
    match row.get(index) {
        Some(RuntimeValue::String(value)) => Ok(Some(value.clone())),
        Some(RuntimeValue::Null) => Ok(None),
        Some(other) => Err(AutomationError::new(
            "memory_query_contract",
            format!("expected `{column}` to be a string, found {other:?}"),
        )),
        None => Err(AutomationError::new(
            "memory_query_contract",
            format!("missing value for `{column}` in memory query result row"),
        )),
    }
}

fn stable_hash_hex(bytes: &[u8]) -> String {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("{hash:016x}")
}

fn push_stale_item(
    rows: &mut Vec<Vec<RuntimeValue>>,
    kind: &str,
    path: &str,
    title: Option<&str>,
    status: &str,
    stored_hash: Option<&str>,
    current_hash: Option<&str>,
    stored_root: Option<&str>,
    resolved_root: &str,
) {
    rows.push(vec![
        RuntimeValue::String(kind.to_owned()),
        string_or_null(path),
        option_string(title),
        RuntimeValue::String(status.to_owned()),
        option_string(stored_hash),
        option_string(current_hash),
        option_string(stored_root),
        RuntimeValue::String(resolved_root.to_owned()),
        RuntimeValue::String(stale_item_suggestion(kind, resolved_root)),
    ]);
}

fn stale_item_suggestion(kind: &str, root: &str) -> String {
    match kind {
        "missing_file" => format!(
            "restore the file or run `cupld sync markdown --db ... --root {root}` to refresh persisted markdown state"
        ),
        "hash_mismatch" => format!(
            "run `cupld sync markdown --db ... --root {root}` to refresh persisted markdown state"
        ),
        "tombstoned_document" => format!(
            "restore the file and run `cupld sync markdown --db ... --root {root}` if the document should be current"
        ),
        "metadata_incomplete" => format!(
            "run `cupld sync markdown --db ... --root {root}` to restore required source metadata"
        ),
        "root_mismatch" => format!(
            "run `cupld sync markdown --db ... --root {root}` if this is the intended markdown root"
        ),
        _ => format!("run `cupld sync markdown --db ... --root {root}`"),
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct MemoryStaleSummary {
    missing_or_tombstoned_documents: usize,
    stale_current_documents: usize,
}

fn memory_stale_summary(items: &QueryResult) -> MemoryStaleSummary {
    let Some(kind_index) = items.columns.iter().position(|column| column == "kind") else {
        return MemoryStaleSummary::default();
    };
    let mut summary = MemoryStaleSummary::default();
    for row in &items.rows {
        let Some(RuntimeValue::String(kind)) = row.get(kind_index) else {
            continue;
        };
        match kind.as_str() {
            "missing_file" | "tombstoned_document" => {
                summary.missing_or_tombstoned_documents += 1;
            }
            "hash_mismatch" => {
                summary.stale_current_documents += 1;
            }
            _ => {}
        }
    }
    summary
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct MarkdownMetadataSummary {
    missing_required_metadata: usize,
}

fn markdown_metadata_summary(session: &Session) -> MarkdownMetadataSummary {
    let mut missing_required_metadata = 0;
    for node in session
        .engine()
        .nodes()
        .filter(|node| node.labels().contains(MARKDOWN_DOCUMENT_LABEL))
    {
        if REQUIRED_MARKDOWN_SOURCE_METADATA.iter().any(
            |key| !matches!(node.property(key), Some(Value::String(value)) if !value.is_empty()),
        ) {
            missing_required_metadata += 1;
        }
    }
    MarkdownMetadataSummary {
        missing_required_metadata,
    }
}

fn duplicate_current_markdown_path_count(session: &Session) -> usize {
    let mut seen = BTreeSet::new();
    let mut duplicates = BTreeSet::new();
    for node in session
        .engine()
        .nodes()
        .filter(|node| node.labels().contains(MARKDOWN_DOCUMENT_LABEL))
    {
        if string_property(node.property("src.status")) != Some("current") {
            continue;
        }
        let Some(path) = string_property(node.property("src.path")) else {
            continue;
        };
        if !seen.insert(path.to_owned()) {
            duplicates.insert(path.to_owned());
        }
    }
    duplicates.len()
}

fn duplicate_connector_owned_markdown_link_edge_count(session: &Session) -> usize {
    let mut seen = BTreeSet::new();
    let mut duplicates = 0;
    for edge in session
        .engine()
        .edges()
        .filter(|edge| edge.edge_type() == MD_LINKS_TO)
        .filter(|edge| string_property(edge.property("src.connector")) == Some("markdown"))
    {
        let from_path = session
            .engine()
            .node(edge.from())
            .and_then(|node| string_property(node.property("src.path")));
        let to_path = session
            .engine()
            .node(edge.to())
            .and_then(|node| string_property(node.property("src.path")));
        let key = (
            from_path.map(ToOwned::to_owned),
            to_path.map(ToOwned::to_owned),
            edge.edge_type().to_owned(),
        );
        if !seen.insert(key) {
            duplicates += 1;
        }
    }
    duplicates
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct SchemaIndexSummary {
    total_indexes: usize,
    ready_indexes: usize,
    non_ready_indexes: usize,
}

fn schema_index_summary(session: &Session) -> SchemaIndexSummary {
    let rows = session.engine().show_indexes(None);
    let ready_indexes = rows.iter().filter(|row| row.status == "ready").count();
    SchemaIndexSummary {
        total_indexes: rows.len(),
        ready_indexes,
        non_ready_indexes: rows.len().saturating_sub(ready_indexes),
    }
}

fn option_string(value: Option<&str>) -> RuntimeValue {
    value
        .map(|value| RuntimeValue::String(value.to_owned()))
        .unwrap_or(RuntimeValue::Null)
}

fn string_or_null(value: &str) -> RuntimeValue {
    if value.is_empty() {
        RuntimeValue::Null
    } else {
        RuntimeValue::String(value.to_owned())
    }
}

fn memory_orphan_items(session: &mut Session) -> Result<QueryResult, AutomationError> {
    let mut rows = Vec::new();
    for node in session
        .engine()
        .nodes()
        .filter(|node| node.labels().contains(MARKDOWN_DOCUMENT_LABEL))
    {
        let status = string_property(node.property("src.status")).unwrap_or("unknown");
        if status != "current" {
            continue;
        }
        let node_id = node.id();
        let mut markdown_inbound_count = 0;
        let mut markdown_outbound_count = 0;
        let mut native_inbound_count = 0;
        let mut native_outbound_count = 0;
        for edge in session.engine().edges() {
            let touches_node = edge.from() == node_id || edge.to() == node_id;
            if !touches_node {
                continue;
            }
            if edge.edge_type() == MD_LINKS_TO {
                if edge.to() == node_id {
                    markdown_inbound_count += 1;
                }
                if edge.from() == node_id {
                    markdown_outbound_count += 1;
                }
            } else if !is_markdown_structural_edge(edge.edge_type()) {
                if edge.to() == node_id {
                    native_inbound_count += 1;
                }
                if edge.from() == node_id {
                    native_outbound_count += 1;
                }
            }
        }
        if markdown_inbound_count == 0
            && markdown_outbound_count == 0
            && native_inbound_count == 0
            && native_outbound_count == 0
        {
            rows.push(vec![
                RuntimeValue::String(
                    string_property(node.property("src.path"))
                        .unwrap_or("unknown")
                        .to_owned(),
                ),
                RuntimeValue::String(
                    string_property(node.property("md.title"))
                        .unwrap_or("")
                        .to_owned(),
                ),
                RuntimeValue::String(status.to_owned()),
                RuntimeValue::Int(markdown_inbound_count),
                RuntimeValue::Int(markdown_outbound_count),
                RuntimeValue::Int(native_inbound_count),
                RuntimeValue::Int(native_outbound_count),
                RuntimeValue::String("no_markdown_or_native_connectivity".to_owned()),
            ]);
        }
    }
    rows.sort_by(|left, right| value_string(&left[0]).cmp(&value_string(&right[0])));
    Ok(QueryResult {
        columns: vec![
            "path".to_owned(),
            "title".to_owned(),
            "status".to_owned(),
            "markdown_inbound_count".to_owned(),
            "markdown_outbound_count".to_owned(),
            "native_inbound_count".to_owned(),
            "native_outbound_count".to_owned(),
            "reason".to_owned(),
        ],
        rows,
    })
}

fn is_markdown_structural_edge(edge_type: &str) -> bool {
    edge_type == MD_IN_DIRECTORY || edge_type == MD_PARENT_DIRECTORY
}

fn string_property(value: Option<&Value>) -> Option<&str> {
    match value {
        Some(Value::String(value)) => Some(value),
        _ => None,
    }
}
