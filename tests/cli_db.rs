mod support;

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use cupld::{PropertyMap, RuntimeValue, Session, Value, json};

use support::{TestDb, run, seed_person_graph};

static NEXT_NEW_DB_ID: AtomicUsize = AtomicUsize::new(1);

struct TempPath {
    path: PathBuf,
}

impl TempPath {
    fn new(prefix: &str) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let suffix = NEXT_NEW_DB_ID.fetch_add(1, Ordering::Relaxed);
        Self {
            path: std::env::temp_dir().join(format!(
                "cupld_{prefix}_{}_{}_{}.cupld",
                std::process::id(),
                timestamp,
                suffix
            )),
        }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempPath {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new(prefix: &str) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let suffix = NEXT_NEW_DB_ID.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "cupld_{prefix}_{}_{}_{}",
            std::process::id(),
            timestamp,
            suffix
        ));
        fs::create_dir_all(&path).unwrap();
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

fn run_cli(args: &[&str]) -> std::process::Output {
    run_cli_with_input_in_dir(args, "", None)
}

fn run_cli_with_input(args: &[&str], input: &str) -> std::process::Output {
    run_cli_with_input_in_dir(args, input, None)
}

fn run_cli_in_dir(args: &[&str], dir: &Path) -> std::process::Output {
    run_cli_with_input_in_dir(args, "", Some(dir))
}

fn run_cli_with_input_in_dir(
    args: &[&str],
    input: &str,
    dir: Option<&Path>,
) -> std::process::Output {
    run_cli_with_env_in_dir(args, input, dir, &[])
}

fn run_cli_with_env_in_dir(
    args: &[&str],
    input: &str,
    dir: Option<&Path>,
    envs: &[(&str, &str)],
) -> std::process::Output {
    let home = TempDir::new("cli_home");
    let config = TempDir::new("cli_config");
    let mut command = Command::new(env!("CARGO_BIN_EXE_cupld"));
    command
        .args(args)
        .current_dir(dir.unwrap_or_else(|| Path::new(".")))
        .env("CUPLD_NO_INSTALL_PROMPT", "1")
        .env("CUPLD_NO_UPGRADE_CHECK", "1")
        .env("HOME", home.path())
        .env("USERPROFILE", home.path())
        .env("XDG_CONFIG_HOME", config.path())
        .env("APPDATA", config.path())
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    for (key, value) in envs {
        command.env(key, value);
    }
    let mut child = command.spawn().unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(input.as_bytes())
        .unwrap();
    child.wait_with_output().unwrap()
}

fn workspace_default_db_path(workspace: &Path) -> PathBuf {
    workspace.join(".cupld").join("default.cupld")
}

fn seed_workspace_default_db(workspace: &Path) -> PathBuf {
    let db_path = workspace_default_db_path(workspace);
    fs::create_dir_all(db_path.parent().unwrap()).unwrap();
    let mut session = Session::new_in_memory();
    seed_person_graph(&mut session);
    session.save_as(&db_path).unwrap();
    db_path
}

#[cfg(unix)]
fn write_fake_curl(dir: &Path, body: &str, exit_code: i32) -> PathBuf {
    use std::os::unix::fs::PermissionsExt;

    let path = dir.join("curl");
    fs::write(
        &path,
        format!(
            "#!/bin/sh\nprintf '%s\\n' '{}'\nexit {exit_code}\n",
            body.replace('\'', "'\\''")
        ),
    )
    .unwrap();
    let mut permissions = fs::metadata(&path).unwrap().permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(&path, permissions).unwrap();
    path
}

#[test]
fn cli_version_flags_print_package_version() {
    for flag in ["--version", "-v"] {
        let output = run_cli(&[flag]);
        assert!(
            output.status.success(),
            "{flag} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(
            String::from_utf8(output.stdout).unwrap(),
            concat!("cupld ", env!("CARGO_PKG_VERSION"), "\n")
        );
    }
}

#[test]
fn cli_help_flags_print_one_help_block() {
    for flag in ["--help", "-h"] {
        let output = run_cli(&[flag]);
        assert!(
            output.status.success(),
            "{flag} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let stdout = String::from_utf8(output.stdout).unwrap();
        assert_eq!(stdout.matches("Usage:").count(), 1);
        assert_eq!(stdout.matches("Commands:").count(), 1);
        assert!(!stdout.contains("Examples:"));
    }
}

#[test]
fn cli_repl_creates_a_new_db_when_path_is_missing() {
    let db = TempPath::new("cli_new_db");
    assert!(!db.path().exists());

    let output = run_cli_with_input(&[db.path().to_str().unwrap()], ".quit\n");
    assert!(output.status.success());
    assert!(db.path().exists());

    let report = Session::check(db.path()).unwrap();
    assert_eq!(report.last_tx_id, 0);
    assert_eq!(report.wal_records, 0);
    assert!(!report.recovered_tail);
}

#[test]
fn cli_query_reads_from_generated_db() {
    let db = TestDb::new("cli_query");
    let mut session = db.open();
    seed_person_graph(&mut session);
    drop(session);

    let output = run_cli(&[
        "query",
        "--db",
        db.path().to_str().unwrap(),
        "MATCH (n:Person) RETURN n.name ORDER BY n.name",
    ]);

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("col_1"));
    assert!(stdout.contains("Ada"));
    assert!(stdout.contains("Alan"));
    assert!(stdout.contains("Bob"));
    assert!(stdout.contains("Grace"));
}

#[test]
fn cli_query_reads_from_default_db_alias() {
    let workspace = TempDir::new("cli_query_default_alias");
    seed_workspace_default_db(workspace.path());

    let output = run_cli_in_dir(
        &[
            "query",
            "--db",
            "default",
            "MATCH (n:Person) RETURN n.name ORDER BY n.name",
        ],
        workspace.path(),
    );

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("Ada"));
    assert!(stdout.contains("Grace"));
}

#[test]
fn cli_query_json_outputs_machine_envelope() {
    let db = TestDb::new("cli_query_json");
    let mut session = db.open();
    seed_person_graph(&mut session);
    drop(session);

    let output = run_cli(&[
        "query",
        "--db",
        db.path().to_str().unwrap(),
        "--output",
        "json",
        "--max-rows",
        "2",
        "MATCH (n:Person) RETURN n.name ORDER BY n.name",
    ]);

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    let parsed = json::parse(&stdout).unwrap();
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
            .get("policy")
            .and_then(|policy| policy.get("execution_mode"))
            .and_then(json::JsonValue::as_str),
        Some("automation_read_write")
    );
    assert_eq!(
        parsed
            .get("policy")
            .and_then(|policy| policy.get("max_rows"))
            .and_then(json::JsonValue::as_i64),
        Some(2)
    );
    assert_eq!(
        parsed
            .get("results")
            .and_then(json::JsonValue::as_array)
            .and_then(|results| results[0].get("row_count"))
            .and_then(json::JsonValue::as_i64),
        Some(2)
    );
    assert_eq!(
        parsed
            .get("results")
            .and_then(json::JsonValue::as_array)
            .and_then(|results| results[0].get("truncated"))
            .and_then(json::JsonValue::as_bool),
        Some(true)
    );
    let rows = parsed
        .get("results")
        .and_then(json::JsonValue::as_array)
        .and_then(|results| results[0].get("rows"))
        .and_then(json::JsonValue::as_array)
        .unwrap();
    assert_eq!(
        rows[0].get("col_1").and_then(json::JsonValue::as_str),
        Some("Ada")
    );
    assert_eq!(
        rows[1].get("col_1").and_then(json::JsonValue::as_str),
        Some("Alan")
    );
}

#[test]
fn cli_query_json_errors_use_machine_envelope() {
    let db = TestDb::new("cli_query_json_error");

    let output = run_cli(&[
        "query",
        "--db",
        db.path().to_str().unwrap(),
        "--output",
        "json",
        "--params-json",
        "{bad",
        "MATCH (n) RETURN n",
    ]);

    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    let parsed = json::parse(&stderr).unwrap();
    assert_eq!(
        parsed.get("ok").and_then(json::JsonValue::as_bool),
        Some(false)
    );
    assert_eq!(
        parsed
            .get("error")
            .and_then(|error| error.get("code"))
            .and_then(json::JsonValue::as_str),
        Some("params_json_parse")
    );
}

#[test]
fn cli_context_ndjson_outputs_budgeted_contract() {
    let db = TestDb::new("cli_context_ndjson");
    let mut session = db.open();
    seed_person_graph(&mut session);
    drop(session);

    let output = run_cli(&[
        "context",
        "--db",
        db.path().to_str().unwrap(),
        "--output",
        "ndjson",
        "--node",
        "1",
        "--max-nodes",
        "2",
    ]);

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    let lines = stdout.lines().collect::<Vec<_>>();
    assert_eq!(lines.len(), 5);
    assert_eq!(
        lines
            .iter()
            .map(|line| {
                json::parse(line)
                    .unwrap()
                    .get("kind")
                    .and_then(json::JsonValue::as_str)
                    .unwrap()
                    .to_owned()
            })
            .collect::<Vec<_>>(),
        vec![
            "context_meta",
            "context_seed",
            "context_node",
            "context_node",
            "context_edge"
        ]
    );

    let meta = json::parse(lines[0]).unwrap();
    assert_eq!(
        meta.get("kind").and_then(json::JsonValue::as_str),
        Some("context_meta")
    );
    assert_eq!(
        meta.get("policy")
            .and_then(|policy| policy.get("execution_mode"))
            .and_then(json::JsonValue::as_str),
        Some("automation_read_only")
    );
    assert_eq!(
        meta.get("policy")
            .and_then(|policy| policy.get("retrieval_budget"))
            .and_then(|budget| budget.get("nodes"))
            .and_then(json::JsonValue::as_i64),
        Some(2)
    );
    assert_eq!(
        meta.get("retrieval_usage")
            .and_then(|usage| usage.get("nodes"))
            .and_then(json::JsonValue::as_i64),
        Some(2)
    );
    assert!(meta.get("snippets").is_none());

    let seed = json::parse(lines[1]).unwrap();
    assert_eq!(
        seed.get("seed")
            .and_then(|seed| seed.get("node_ids"))
            .and_then(json::JsonValue::as_array)
            .map(|values| values.len()),
        Some(1)
    );

    let node = json::parse(lines[2]).unwrap();
    assert_eq!(
        node.get("node")
            .and_then(|entry| entry.get("properties"))
            .and_then(|properties| properties.get("email"))
            .and_then(json::JsonValue::as_str),
        Some("ada@example.com")
    );
}

#[test]
fn cli_context_json_outputs_seeded_golden_contract() {
    let db = TestDb::new("cli_context_json_seeded");
    let mut session = db.open();
    seed_person_graph(&mut session);
    drop(session);

    let output = run_cli(&[
        "context",
        "--db",
        db.path().to_str().unwrap(),
        "--output",
        "json",
        "--node",
        "1",
        "--depth",
        "2",
        "--max-nodes",
        "2",
    ]);

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    let parsed = json::parse(stdout.trim()).unwrap();

    assert!(parsed.get("items").is_none());
    assert!(parsed.get("snippets").is_none());
    assert_eq!(
        parsed.get("mode").and_then(json::JsonValue::as_str),
        Some("seeded")
    );
    assert_eq!(
        parsed
            .get("nodes")
            .and_then(json::JsonValue::as_array)
            .map(|values| values.len()),
        Some(2)
    );
    assert_eq!(
        parsed
            .get("edges")
            .and_then(json::JsonValue::as_array)
            .map(|values| values.len()),
        Some(1)
    );
    assert_eq!(
        parsed
            .get("retrieval_usage")
            .and_then(|usage| usage.get("nodes"))
            .and_then(json::JsonValue::as_i64),
        Some(2)
    );
    assert_eq!(
        parsed
            .get("retrieval_usage")
            .and_then(|usage| usage.get("edges"))
            .and_then(json::JsonValue::as_i64),
        Some(1)
    );
    assert_eq!(
        parsed
            .get("retrieval_usage")
            .and_then(|usage| usage.get("total_payload_bytes"))
            .and_then(json::JsonValue::as_i64),
        Some(stdout.trim().len() as i64)
    );
    assert_eq!(
        parsed
            .get("retrieval_usage")
            .and_then(|usage| usage.get("truncated"))
            .and_then(json::JsonValue::as_bool),
        Some(true)
    );
    assert_eq!(
        parsed
            .get("nodes")
            .and_then(json::JsonValue::as_array)
            .and_then(|nodes| nodes.first())
            .and_then(|node| node.get("properties"))
            .and_then(|properties| properties.get("email"))
            .and_then(json::JsonValue::as_str),
        Some("ada@example.com")
    );
}

#[test]
fn cli_context_resolves_node_and_path_seeds_in_request_order() {
    let db = TestDb::new("cli_context_seed_order");
    let mut session = db.open();
    run(&mut session, "CREATE (:Person {name: 'Ada'})");
    run(
        &mut session,
        "CREATE (:MarkdownDocument {`src.path`: 'projects/foo.md', `src.status`: 'current', name: 'Foo'})",
    );
    let node_rows = run(&mut session, "MATCH (n:Person {name: 'Ada'}) RETURN id(n)");
    let ada_id = match &node_rows.rows[0][0] {
        RuntimeValue::Int(value) => *value,
        other => panic!("expected Ada node id, found {other:?}"),
    };
    drop(session);

    let output = run_cli(&[
        "context",
        "--db",
        db.path().to_str().unwrap(),
        "--path",
        "projects/foo.md",
        "--node",
        &ada_id.to_string(),
        "--depth",
        "0",
    ]);

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    let parsed = json::parse(&stdout).unwrap();
    let nodes = parsed
        .get("nodes")
        .and_then(json::JsonValue::as_array)
        .unwrap();
    assert_eq!(
        nodes
            .iter()
            .map(|node| node
                .get("display")
                .and_then(json::JsonValue::as_str)
                .unwrap())
            .collect::<Vec<_>>(),
        vec!["Foo", "Ada"]
    );
}

#[test]
fn cli_context_fails_missing_node_and_path_seeds() {
    let db = TestDb::new("cli_context_missing_seeds");

    let missing_node = run_cli(&[
        "context",
        "--db",
        db.path().to_str().unwrap(),
        "--node",
        "999",
    ]);
    assert!(!missing_node.status.success());
    let stderr = String::from_utf8(missing_node.stderr).unwrap();
    let parsed = json::parse(&stderr).unwrap();
    assert_eq!(
        parsed
            .get("error")
            .and_then(|error| error.get("code"))
            .and_then(json::JsonValue::as_str),
        Some("context_seed_not_found")
    );

    let missing_path = run_cli(&[
        "context",
        "--db",
        db.path().to_str().unwrap(),
        "--path",
        "projects/missing.md",
    ]);
    assert!(!missing_path.status.success());
    let stderr = String::from_utf8(missing_path.stderr).unwrap();
    let parsed = json::parse(&stderr).unwrap();
    assert_eq!(
        parsed
            .get("error")
            .and_then(|error| error.get("code"))
            .and_then(json::JsonValue::as_str),
        Some("context_seed_path_not_found")
    );
}

#[test]
fn cli_context_handles_duplicate_ambiguous_and_stale_path_seeds() {
    let db = TestDb::new("cli_context_path_resolution");
    let mut session = db.open();
    for statement in [
        "CREATE (:MarkdownDocument {`src.path`: 'projects/foo.md', `src.status`: 'missing', name: 'Old Foo'})",
        "CREATE (:MarkdownDocument {`src.path`: 'projects/foo.md', `src.status`: 'current', name: 'Current Foo'})",
        "CREATE (:MarkdownDocument {`src.path`: 'projects/ambiguous.md', `src.status`: 'current', name: 'A'})",
        "CREATE (:MarkdownDocument {`src.path`: 'projects/ambiguous.md', `src.status`: 'current', name: 'B'})",
        "CREATE (:MarkdownDocument {`src.path`: 'projects/stale.md', `src.status`: 'missing', name: 'Stale'})",
        "CREATE (:MarkdownDocument {`src.path`: 'projects/no-status.md', name: 'No Status'})",
    ] {
        run(&mut session, statement);
    }
    drop(session);

    let resolved = run_cli(&[
        "context",
        "--db",
        db.path().to_str().unwrap(),
        "--path",
        "projects/foo.md",
        "--path",
        "projects/foo.md",
        "--path",
        "projects/stale.md",
        "--path",
        "projects/no-status.md",
        "--depth",
        "0",
    ]);
    assert!(resolved.status.success());
    let stdout = String::from_utf8(resolved.stdout).unwrap();
    let parsed = json::parse(&stdout).unwrap();
    let nodes = parsed
        .get("nodes")
        .and_then(json::JsonValue::as_array)
        .unwrap();
    assert_eq!(nodes.len(), 3);
    let warnings = parsed
        .get("warnings")
        .and_then(json::JsonValue::as_array)
        .unwrap();
    let warning_codes = warnings
        .iter()
        .filter_map(|warning| warning.get("code").and_then(json::JsonValue::as_str))
        .collect::<Vec<_>>();
    assert!(warning_codes.contains(&"context_seed_path_multiple_matches"));
    assert!(warning_codes.contains(&"context_seed_duplicate"));
    assert!(warning_codes.contains(&"context_seed_source_stale"));
    assert!(warning_codes.contains(&"context_seed_source_missing"));

    let ambiguous = run_cli(&[
        "context",
        "--db",
        db.path().to_str().unwrap(),
        "--path",
        "projects/ambiguous.md",
    ]);
    assert!(!ambiguous.status.success());
    let stderr = String::from_utf8(ambiguous.stderr).unwrap();
    let parsed = json::parse(&stderr).unwrap();
    assert_eq!(
        parsed
            .get("error")
            .and_then(|error| error.get("code"))
            .and_then(json::JsonValue::as_str),
        Some("context_seed_path_ambiguous")
    );
}

#[test]
fn cli_schema_prints_generated_schema() {
    let db = TestDb::new("cli_schema");
    let mut session = db.open();
    seed_person_graph(&mut session);
    drop(session);

    let output = run_cli(&["schema", "--db", db.path().to_str().unwrap()]);

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("CREATE LABEL Person"));
    assert!(stdout.contains("CREATE EDGE TYPE KNOWS"));
    assert!(stdout.contains("CREATE INDEX idx_label_Person_email_eq ON :Person(email)"));
    assert!(stdout.contains("constraint_label_Person_email_unique"));
}

#[test]
fn cli_schema_reads_from_default_db_alias() {
    let workspace = TempDir::new("cli_schema_default_alias");
    seed_workspace_default_db(workspace.path());

    let output = run_cli_in_dir(&["schema", "--db", "default"], workspace.path());

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("CREATE LABEL Person"));
    assert!(stdout.contains("CREATE EDGE TYPE KNOWS"));
}

#[test]
fn cli_check_reports_generated_db_integrity() {
    let db = TestDb::new("cli_check");
    let mut session = db.open();
    seed_person_graph(&mut session);
    drop(session);

    let output = run_cli(&["check", "--db", db.path().to_str().unwrap()]);

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("ok db="));
    assert!(stdout.contains("wal_records="));
    assert!(stdout.contains("recovered_tail=false"));
    assert!(stdout.contains("ambiguous_markdown_aliases=0"));
}

#[test]
fn cli_memory_check_json_reports_ambiguous_markdown_aliases_from_current_documents() {
    let db = TestDb::new("cli_memory_check_alias_diagnostics");
    let root = TempDir::new("cli_memory_check_alias_diagnostics_root");
    fs::write(
        root.path().join("one.md"),
        "---\n\
aliases: [Shared, Solo]\n\
---\n\
# One\n",
    )
    .unwrap();
    fs::write(
        root.path().join("two.md"),
        "---\n\
aliases: [Shared]\n\
---\n\
# Two\n",
    )
    .unwrap();
    fs::write(
        root.path().join("unique.md"),
        "---\n\
aliases: [Unique]\n\
---\n\
# Unique\n",
    )
    .unwrap();

    let sync = run_cli(&[
        "sync",
        "markdown",
        "--db",
        db.path().to_str().unwrap(),
        "--root",
        root.path().to_str().unwrap(),
    ]);
    assert!(sync.status.success());

    let output = run_cli(&[
        "memory",
        "check",
        "--db",
        db.path().to_str().unwrap(),
        "--root",
        root.path().to_str().unwrap(),
        "--output",
        "json",
    ]);
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    let parsed = json::parse(&stdout).unwrap();
    assert_eq!(
        parsed.get("ok").and_then(json::JsonValue::as_bool),
        Some(true)
    );
    assert_eq!(
        parsed.get("command").and_then(json::JsonValue::as_str),
        Some("memory.check")
    );
    assert_eq!(
        parsed
            .get("summary")
            .and_then(|summary| summary.get("ambiguous_markdown_aliases"))
            .and_then(json::JsonValue::as_i64),
        Some(1)
    );
    assert_eq!(
        parsed
            .get("markdown")
            .and_then(|markdown| markdown.get("ambiguous_alias_count"))
            .and_then(json::JsonValue::as_i64),
        Some(1)
    );
    let aliases = parsed
        .get("markdown")
        .and_then(|markdown| markdown.get("ambiguous_aliases"))
        .and_then(json::JsonValue::as_array)
        .unwrap();
    assert_eq!(aliases.len(), 1);
    assert_eq!(
        aliases[0].get("alias").and_then(json::JsonValue::as_str),
        Some("Shared")
    );
    let paths = aliases[0]
        .get("paths")
        .and_then(json::JsonValue::as_array)
        .unwrap();
    assert_eq!(
        paths
            .iter()
            .map(|path| path.as_str().unwrap())
            .collect::<Vec<_>>(),
        vec!["one.md", "two.md"]
    );

    fs::remove_file(root.path().join("two.md")).unwrap();
    let sync = run_cli(&[
        "sync",
        "markdown",
        "--db",
        db.path().to_str().unwrap(),
        "--root",
        root.path().to_str().unwrap(),
    ]);
    assert!(sync.status.success());

    let output = run_cli(&[
        "memory",
        "check",
        "--db",
        db.path().to_str().unwrap(),
        "--root",
        root.path().to_str().unwrap(),
        "--output",
        "json",
    ]);
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    let parsed = json::parse(&stdout).unwrap();
    assert_eq!(
        parsed
            .get("markdown")
            .and_then(|markdown| markdown.get("ambiguous_alias_count"))
            .and_then(json::JsonValue::as_i64),
        Some(0)
    );
}

#[test]
fn cli_memory_check_outputs_json_report() {
    let db = TestDb::new("cli_memory_check_json");

    let output = run_cli(&[
        "memory",
        "check",
        "--db",
        db.path().to_str().unwrap(),
        "--output",
        "json",
        "--strict",
    ]);

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    let parsed = json::parse(&stdout).unwrap();
    assert_eq!(
        parsed.get("ok").and_then(json::JsonValue::as_bool),
        Some(true)
    );
    assert_eq!(
        parsed.get("command").and_then(json::JsonValue::as_str),
        Some("memory.check")
    );
    assert_eq!(
        parsed.get("strict").and_then(json::JsonValue::as_bool),
        Some(true)
    );
    assert_eq!(
        parsed
            .get("summary")
            .and_then(|summary| summary.get("status"))
            .and_then(json::JsonValue::as_str),
        Some("pass")
    );
    assert_eq!(
        parsed.get("status").and_then(json::JsonValue::as_str),
        Some("pass")
    );
    assert_eq!(
        parsed
            .get("checks")
            .and_then(json::JsonValue::as_array)
            .and_then(|checks| checks.first())
            .and_then(|check| check.get("status"))
            .and_then(json::JsonValue::as_str),
        Some("pass")
    );
}

#[test]
fn cli_memory_check_empty_db_passes_in_table_output() {
    let db = TestDb::new("cli_memory_check_empty_table");

    let output = run_cli(&["memory", "check", "--db", db.path().to_str().unwrap()]);

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("command | memory.check"));
    assert!(stdout.contains("status  | pass"));
    assert!(stdout.contains("stale_items"));
    assert!(stdout.contains("orphan_items"));
}

#[test]
fn cli_memory_check_default_alias_uses_workspace_db() {
    let workspace = TempDir::new("cli_memory_check_default_alias");
    seed_workspace_default_db(workspace.path());

    let output = run_cli_in_dir(
        &["memory", "check", "--db", "default", "--output", "json"],
        workspace.path(),
    );

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    let parsed = json::parse(&stdout).unwrap();
    assert_eq!(
        parsed.get("db_path").and_then(json::JsonValue::as_str),
        workspace_default_db_path(workspace.path()).to_str()
    );
    assert_eq!(
        parsed.get("status").and_then(json::JsonValue::as_str),
        Some("pass")
    );
}

#[test]
fn cli_memory_check_reports_markdown_health_summary() {
    let db = TestDb::new("cli_memory_check_health_summary");
    let root = TempDir::new("cli_memory_check_health_summary_root");
    for path in [
        "changed.md",
        "missing.md",
        "duplicate.md",
        "source.md",
        "target.md",
    ] {
        fs::write(root.path().join(path), format!("# {path}\n")).unwrap();
    }

    let sync = run_cli(&[
        "sync",
        "markdown",
        "--db",
        db.path().to_str().unwrap(),
        "--root",
        root.path().to_str().unwrap(),
    ]);
    assert!(sync.status.success());
    fs::write(root.path().join("changed.md"), "# Changed\n\nupdated\n").unwrap();
    fs::remove_file(root.path().join("missing.md")).unwrap();

    let mut session = db.open();
    let mut engine = session.engine().clone();
    let source_node = engine
        .nodes()
        .find(|node| node.property("src.path") == Some(&Value::from("duplicate.md")))
        .unwrap();
    let source = source_node.id();
    let source_hash = source_node.property("src.hash").unwrap().clone();
    let duplicate = engine
        .create_node(
            ["MarkdownDocument"],
            PropertyMap::from_pairs([
                ("src.connector", Value::from("markdown")),
                ("src.kind", Value::from("document")),
                ("src.root", Value::from(root.path().display().to_string())),
                ("src.path", Value::from("duplicate.md")),
                ("src.hash", source_hash),
                ("src.status", Value::from("current")),
            ]),
        )
        .unwrap();
    let target = engine
        .nodes()
        .find(|node| node.property("src.path") == Some(&Value::from("target.md")))
        .unwrap()
        .id();
    engine
        .create_edge(
            source,
            target,
            "MD_LINKS_TO",
            PropertyMap::from_pairs([("src.connector", Value::from("markdown"))]),
        )
        .unwrap();
    engine
        .create_edge(
            duplicate,
            target,
            "MD_LINKS_TO",
            PropertyMap::from_pairs([("src.connector", Value::from("markdown"))]),
        )
        .unwrap();
    let metadata_id = engine
        .nodes()
        .find(|node| node.property("src.path") == Some(&Value::from("target.md")))
        .unwrap()
        .id();
    engine
        .remove_node_property(metadata_id, "src.hash")
        .unwrap();
    engine.commit().unwrap();
    session.replace_engine(engine).unwrap();
    session.save().unwrap();

    let output = run_cli(&[
        "memory",
        "check",
        "--db",
        db.path().to_str().unwrap(),
        "--root",
        root.path().to_str().unwrap(),
        "--output",
        "json",
    ]);

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    let parsed = json::parse(&stdout).unwrap();
    assert_eq!(
        parsed.get("status").and_then(json::JsonValue::as_str),
        Some("warn")
    );
    let summary = parsed.get("summary").unwrap();
    assert_eq!(
        summary
            .get("missing_tombstoned_markdown_documents")
            .and_then(json::JsonValue::as_i64),
        Some(1)
    );
    assert_eq!(
        summary
            .get("stale_current_markdown_documents")
            .and_then(json::JsonValue::as_i64),
        Some(1)
    );
    assert_eq!(
        summary
            .get("markdown_documents_missing_source_metadata")
            .and_then(json::JsonValue::as_i64),
        Some(1)
    );
    assert_eq!(
        summary
            .get("duplicate_current_markdown_document_paths")
            .and_then(json::JsonValue::as_i64),
        Some(1)
    );
    assert_eq!(
        summary
            .get("duplicate_connector_owned_md_links_to_edges")
            .and_then(json::JsonValue::as_i64),
        Some(1)
    );
    assert_eq!(
        summary
            .get("schema_indexes")
            .and_then(json::JsonValue::as_i64),
        Some(0)
    );
}

#[test]
fn cli_memory_check_counts_duplicate_markdown_links_without_repairing() {
    let db = TestDb::new("cli_memory_check_duplicate_links_preserved");
    let root = TempDir::new("cli_memory_check_duplicate_links_preserved_root");
    fs::write(root.path().join("source.md"), "[target](target.md)\n").unwrap();
    fs::write(root.path().join("target.md"), "# Target\n").unwrap();

    let sync = run_cli(&[
        "sync",
        "markdown",
        "--db",
        db.path().to_str().unwrap(),
        "--root",
        root.path().to_str().unwrap(),
    ]);
    assert!(sync.status.success());

    let mut session = db.open();
    let mut engine = session.engine().clone();
    let source_node = engine
        .nodes()
        .find(|node| node.property("src.path") == Some(&Value::from("source.md")))
        .unwrap();
    let source_hash = source_node.property("src.hash").unwrap().clone();
    let duplicate = engine
        .create_node(
            ["MarkdownDocument"],
            PropertyMap::from_pairs([
                ("src.connector", Value::from("markdown")),
                ("src.kind", Value::from("document")),
                ("src.root", Value::from(root.path().display().to_string())),
                ("src.path", Value::from("source.md")),
                ("src.hash", source_hash),
                ("src.status", Value::from("current")),
            ]),
        )
        .unwrap();
    let target = engine
        .nodes()
        .find(|node| node.property("src.path") == Some(&Value::from("target.md")))
        .unwrap()
        .id();
    engine
        .create_edge(
            duplicate,
            target,
            "MD_LINKS_TO",
            PropertyMap::from_pairs([("src.connector", Value::from("markdown"))]),
        )
        .unwrap();
    engine.commit().unwrap();
    session.replace_engine(engine).unwrap();
    session.save().unwrap();

    let output = run_cli(&[
        "memory",
        "check",
        "--db",
        db.path().to_str().unwrap(),
        "--root",
        root.path().to_str().unwrap(),
        "--output",
        "json",
    ]);

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    let parsed = json::parse(&stdout).unwrap();
    assert_eq!(
        parsed
            .get("summary")
            .and_then(|summary| summary.get("duplicate_connector_owned_md_links_to_edges"))
            .and_then(json::JsonValue::as_i64),
        Some(1)
    );
    let session = db.open();
    let duplicate_edges = session
        .engine()
        .edges()
        .filter(|edge| edge.edge_type() == "MD_LINKS_TO")
        .filter(|edge| edge.property("src.connector") == Some(&Value::from("markdown")))
        .count();
    assert_eq!(duplicate_edges, 2);
}

#[test]
fn cli_memory_check_strict_warn_exits_two() {
    let db = TestDb::new("cli_memory_check_strict_warn");
    let root = TempDir::new("cli_memory_check_strict_warn_root");
    fs::write(root.path().join("note.md"), "# Original\n").unwrap();
    let sync = run_cli(&[
        "sync",
        "markdown",
        "--db",
        db.path().to_str().unwrap(),
        "--root",
        root.path().to_str().unwrap(),
    ]);
    assert!(sync.status.success());
    fs::write(root.path().join("note.md"), "# Changed\n").unwrap();

    let output = run_cli(&[
        "memory",
        "check",
        "--db",
        db.path().to_str().unwrap(),
        "--root",
        root.path().to_str().unwrap(),
        "--output",
        "json",
        "--strict",
    ]);

    assert_eq!(output.status.code(), Some(2));
    let stdout = String::from_utf8(output.stdout).unwrap();
    let parsed = json::parse(&stdout).unwrap();
    assert_eq!(
        parsed.get("status").and_then(json::JsonValue::as_str),
        Some("warn")
    );
}

#[test]
fn cli_memory_find_stale_ndjson_reports_changed_markdown() {
    let db = TestDb::new("cli_memory_find_stale");
    let root = TempDir::new("cli_memory_find_stale_root");
    fs::write(root.path().join("note.md"), "# Original").unwrap();

    let sync = run_cli(&[
        "sync",
        "markdown",
        "--db",
        db.path().to_str().unwrap(),
        "--root",
        root.path().to_str().unwrap(),
    ]);
    assert!(sync.status.success());
    fs::write(root.path().join("note.md"), "# Changed").unwrap();

    let output = run_cli(&[
        "memory",
        "find-stale",
        "--db",
        db.path().to_str().unwrap(),
        "--root",
        root.path().to_str().unwrap(),
        "--output",
        "ndjson",
    ]);

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    let lines = stdout.lines().collect::<Vec<_>>();
    assert_eq!(lines.len(), 3);
    let meta = json::parse(lines[0]).unwrap();
    assert_eq!(
        meta.get("kind").and_then(json::JsonValue::as_str),
        Some("memory_meta")
    );
    assert_eq!(
        meta.get("status").and_then(json::JsonValue::as_str),
        Some("warn")
    );
    assert_eq!(
        meta.get("check_count").and_then(json::JsonValue::as_i64),
        Some(1)
    );
    assert_eq!(
        meta.get("item_count").and_then(json::JsonValue::as_i64),
        Some(1)
    );
    let check = json::parse(lines[1]).unwrap();
    assert_eq!(
        check.get("kind").and_then(json::JsonValue::as_str),
        Some("memory_check")
    );
    assert_eq!(
        check.get("name").and_then(json::JsonValue::as_str),
        Some("stale_items")
    );
    assert_eq!(
        check.get("status").and_then(json::JsonValue::as_str),
        Some("warn")
    );
    let item = json::parse(lines[2]).unwrap();
    assert_eq!(
        item.get("kind").and_then(json::JsonValue::as_str),
        Some("memory_item")
    );
    assert_eq!(
        item.get("item")
            .and_then(|item| item.get("path"))
            .and_then(json::JsonValue::as_str),
        Some("note.md")
    );
    assert_eq!(
        item.get("item")
            .and_then(|item| item.get("kind"))
            .and_then(json::JsonValue::as_str),
        Some("hash_mismatch")
    );
    assert!(
        item.get("item")
            .and_then(|item| item.get("stored_hash"))
            .and_then(json::JsonValue::as_str)
            .is_some()
    );
    assert!(
        item.get("item")
            .and_then(|item| item.get("current_hash"))
            .and_then(json::JsonValue::as_str)
            .is_some()
    );
}

#[test]
fn cli_memory_find_stale_json_reports_filesystem_freshness_kinds() {
    let db = TestDb::new("cli_memory_find_stale_kinds");
    let root = TempDir::new("cli_memory_find_stale_kinds_root");
    for (path, title) in [
        ("changed.md", "Changed"),
        ("missing.md", "Missing"),
        ("tombstone.md", "Tombstone"),
        ("metadata.md", "Metadata"),
        ("root.md", "Root"),
    ] {
        fs::write(root.path().join(path), format!("# {title}\n")).unwrap();
    }

    let sync = run_cli(&[
        "sync",
        "markdown",
        "--db",
        db.path().to_str().unwrap(),
        "--root",
        root.path().to_str().unwrap(),
    ]);
    assert!(sync.status.success());

    fs::write(root.path().join("changed.md"), "# Changed\n\nupdated\n").unwrap();
    fs::remove_file(root.path().join("missing.md")).unwrap();
    let mut session = db.open();
    let mut engine = session.engine().clone();
    let tombstone_id = engine
        .nodes()
        .find(|node| node.property("src.path") == Some(&Value::from("tombstone.md")))
        .unwrap()
        .id();
    engine
        .set_node_property(tombstone_id, "src.status", Value::from("missing"))
        .unwrap();
    let metadata_id = engine
        .nodes()
        .find(|node| node.property("src.path") == Some(&Value::from("metadata.md")))
        .unwrap()
        .id();
    engine
        .remove_node_property(metadata_id, "src.hash")
        .unwrap();
    let root_id = engine
        .nodes()
        .find(|node| node.property("src.path") == Some(&Value::from("root.md")))
        .unwrap()
        .id();
    engine
        .set_node_property(root_id, "src.root", Value::from("/tmp/other-root"))
        .unwrap();
    engine.commit().unwrap();
    session.replace_engine(engine).unwrap();
    session.save().unwrap();

    let output = run_cli(&[
        "memory",
        "find-stale",
        "--db",
        db.path().to_str().unwrap(),
        "--root",
        root.path().to_str().unwrap(),
        "--output",
        "json",
    ]);

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    let parsed = json::parse(&stdout).unwrap();
    assert_eq!(
        parsed.get("status").and_then(json::JsonValue::as_str),
        Some("warn")
    );
    let items = parsed
        .get("items")
        .and_then(json::JsonValue::as_array)
        .unwrap();
    for kind in [
        "hash_mismatch",
        "missing_file",
        "tombstoned_document",
        "metadata_incomplete",
        "root_mismatch",
    ] {
        let item = items
            .iter()
            .find(|item| item.get("kind").and_then(json::JsonValue::as_str) == Some(kind))
            .unwrap_or_else(|| panic!("missing stale item kind {kind}: {stdout}"));
        assert!(item.get("path").is_some());
        assert!(item.get("title").is_some());
        assert!(item.get("status").is_some());
        assert!(item.get("stored_hash").is_some());
        assert!(item.get("current_hash").is_some());
        assert!(
            item.get("suggestion")
                .and_then(json::JsonValue::as_str)
                .unwrap()
                .contains("cupld sync markdown --db ... --root")
        );
    }
}

#[test]
fn cli_memory_find_stale_table_reports_missing_current_file() {
    let db = TestDb::new("cli_memory_find_stale_missing_table");
    let root = TempDir::new("cli_memory_find_stale_missing_table_root");
    fs::write(root.path().join("gone.md"), "# Gone\n").unwrap();

    let sync = run_cli(&[
        "sync",
        "markdown",
        "--db",
        db.path().to_str().unwrap(),
        "--root",
        root.path().to_str().unwrap(),
    ]);
    assert!(sync.status.success());
    fs::remove_file(root.path().join("gone.md")).unwrap();

    let output = run_cli(&[
        "memory",
        "find-stale",
        "--db",
        db.path().to_str().unwrap(),
        "--root",
        root.path().to_str().unwrap(),
        "--output",
        "table",
    ]);

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("status  | warn"));
    assert!(stdout.contains("gone.md"));
    assert!(stdout.contains("missing_file"));
}

#[test]
fn cli_memory_find_orphans_json_reports_disconnected_current_markdown() {
    let db = TestDb::new("cli_memory_find_orphans");
    let root = TempDir::new("cli_memory_find_orphans_root");
    fs::write(root.path().join("b.md"), "# Bee").unwrap();
    fs::write(root.path().join("a.md"), "# Aye").unwrap();

    let sync = run_cli(&[
        "sync",
        "markdown",
        "--db",
        db.path().to_str().unwrap(),
        "--root",
        root.path().to_str().unwrap(),
    ]);
    assert!(sync.status.success());

    let output = run_cli(&[
        "memory",
        "find-orphans",
        "--db",
        db.path().to_str().unwrap(),
        "--output",
        "json",
    ]);

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    let parsed = json::parse(&stdout).unwrap();
    assert_eq!(
        parsed.get("status").and_then(json::JsonValue::as_str),
        Some("warn")
    );
    assert_eq!(
        parsed
            .get("summary")
            .and_then(|summary| summary.get("orphan_items"))
            .and_then(json::JsonValue::as_i64),
        Some(2)
    );
    let items = parsed
        .get("items")
        .and_then(json::JsonValue::as_array)
        .unwrap();
    assert_eq!(items.len(), 2);
    assert_eq!(
        items[0].get("path").and_then(json::JsonValue::as_str),
        Some("a.md")
    );
    assert_eq!(
        items[0].get("title").and_then(json::JsonValue::as_str),
        Some("Aye")
    );
    assert_eq!(
        items[0].get("status").and_then(json::JsonValue::as_str),
        Some("current")
    );
    assert_eq!(
        items[0]
            .get("markdown_inbound_count")
            .and_then(json::JsonValue::as_i64),
        Some(0)
    );
    assert_eq!(
        items[0]
            .get("markdown_outbound_count")
            .and_then(json::JsonValue::as_i64),
        Some(0)
    );
    assert_eq!(
        items[0]
            .get("native_inbound_count")
            .and_then(json::JsonValue::as_i64),
        Some(0)
    );
    assert_eq!(
        items[0]
            .get("native_outbound_count")
            .and_then(json::JsonValue::as_i64),
        Some(0)
    );
    assert_eq!(
        items[0].get("reason").and_then(json::JsonValue::as_str),
        Some("no_markdown_or_native_connectivity")
    );
    assert_eq!(
        items[1].get("path").and_then(json::JsonValue::as_str),
        Some("b.md")
    );
    let checks = parsed
        .get("checks")
        .and_then(json::JsonValue::as_array)
        .unwrap();
    assert_eq!(
        checks[0].get("name").and_then(json::JsonValue::as_str),
        Some("orphan_items")
    );
    assert_eq!(
        checks[0].get("status").and_then(json::JsonValue::as_str),
        Some("warn")
    );
}

#[test]
fn cli_memory_find_orphans_excludes_markdown_links_and_native_edges() {
    let db = TestDb::new("cli_memory_find_orphans_connected");
    let root = TempDir::new("cli_memory_find_orphans_connected_root");
    fs::write(
        root.path().join("linked-source.md"),
        "[target](linked-target.md)",
    )
    .unwrap();
    fs::write(root.path().join("linked-target.md"), "# Linked Target").unwrap();
    fs::write(root.path().join("native-in.md"), "# Native In").unwrap();
    fs::write(root.path().join("native-out.md"), "# Native Out").unwrap();
    fs::write(root.path().join("directory-only.md"), "# Directory Only").unwrap();

    let sync = run_cli(&[
        "sync",
        "markdown",
        "--db",
        db.path().to_str().unwrap(),
        "--root",
        root.path().to_str().unwrap(),
        "--filesystem-graph",
    ]);
    assert!(sync.status.success());

    let mut session = db.open();
    run(
        &mut session,
        "MATCH (d:MarkdownDocument {`src.path`: 'native-in.md'})
         CREATE (:Person {name: 'Ada'})-[:REFERS_TO]->(d)",
    );
    run(
        &mut session,
        "MATCH (d:MarkdownDocument {`src.path`: 'native-out.md'})
         CREATE (d)-[:DESCRIBES]->(:Topic {name: 'Graph'})",
    );
    session.save().unwrap();

    let output = run_cli(&[
        "memory",
        "find-orphans",
        "--db",
        db.path().to_str().unwrap(),
        "--output",
        "ndjson",
    ]);

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    let mut paths = Vec::new();
    for line in stdout.lines() {
        let parsed = json::parse(line).unwrap();
        if parsed.get("kind").and_then(json::JsonValue::as_str) == Some("memory_item") {
            paths.push(
                parsed
                    .get("item")
                    .and_then(|item| item.get("path"))
                    .and_then(json::JsonValue::as_str)
                    .unwrap()
                    .to_owned(),
            );
        }
    }
    assert_eq!(paths, vec!["directory-only.md"]);
}

#[test]
fn cli_memory_reindex_reads_default_workspace_root() {
    let workspace = TempDir::new("cli_memory_reindex_default_root");
    fs::create_dir_all(workspace.path().join(".cupld")).unwrap();
    fs::write(
        workspace.path().join(".cupld").join("config.toml"),
        "version = 1\n\n[package]\nmarkdown_root = \"notes\"\n",
    )
    .unwrap();

    let output = run_cli_in_dir(
        &["memory", "reindex", "--db", "default", "--output", "json"],
        workspace.path(),
    );

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    let parsed = json::parse(&stdout).unwrap();
    assert_eq!(
        parsed.get("db_path").and_then(json::JsonValue::as_str),
        workspace_default_db_path(workspace.path()).to_str()
    );
    assert_eq!(parsed.get("root"), Some(&json::JsonValue::Null));
}

#[test]
fn cli_memory_reindex_handles_empty_db_without_indexes() {
    let db = TestDb::new("cli_memory_reindex_empty");

    let output = run_cli(&[
        "memory",
        "reindex",
        "--db",
        db.path().to_str().unwrap(),
        "--output",
        "json",
    ]);

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    let parsed = json::parse(&stdout).unwrap();
    assert_eq!(
        parsed.get("command").and_then(json::JsonValue::as_str),
        Some("memory.reindex")
    );
    assert_eq!(
        parsed.get("status").and_then(json::JsonValue::as_str),
        Some("pass")
    );
    assert_eq!(
        parsed.get("db_path").and_then(json::JsonValue::as_str),
        db.path().to_str()
    );
    assert_eq!(parsed.get("root"), Some(&json::JsonValue::Null));
    assert_eq!(
        parsed
            .get("summary")
            .and_then(|summary| summary.get("index_count"))
            .and_then(json::JsonValue::as_i64),
        Some(0)
    );
    assert_eq!(
        parsed
            .get("summary")
            .and_then(|summary| summary.get("schema_indexes"))
            .and_then(json::JsonValue::as_str),
        Some("none")
    );
    assert_eq!(
        parsed
            .get("items")
            .and_then(json::JsonValue::as_array)
            .unwrap()
            .len(),
        0
    );
}

#[test]
fn cli_memory_reindex_reports_existing_schema_indexes() {
    let db = TestDb::new("cli_memory_reindex_schema_indexes");
    let mut session = Session::new_in_memory();
    run(&mut session, "CREATE LABEL Person");
    run(
        &mut session,
        "CREATE INDEX idx_person_name ON :Person(name)",
    );
    run(
        &mut session,
        "CREATE INDEX idx_person_age ON :Person(age) KIND RANGE",
    );
    run(
        &mut session,
        "ALTER INDEX idx_person_age SET STATUS INVALID",
    );
    session.save_as(db.path()).unwrap();

    let output = run_cli(&[
        "memory",
        "reindex",
        "--db",
        db.path().to_str().unwrap(),
        "--output",
        "json",
    ]);

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    let parsed = json::parse(&stdout).unwrap();
    assert_eq!(
        parsed
            .get("summary")
            .and_then(|summary| summary.get("index_count"))
            .and_then(json::JsonValue::as_i64),
        Some(2)
    );
    assert_eq!(
        parsed
            .get("summary")
            .and_then(|summary| summary.get("schema_indexes"))
            .and_then(json::JsonValue::as_str),
        Some("verified")
    );
    let items = parsed
        .get("items")
        .and_then(json::JsonValue::as_array)
        .unwrap();
    assert_eq!(items.len(), 2);
    assert_eq!(
        items[0].get("name").and_then(json::JsonValue::as_str),
        Some("idx_person_age")
    );
    assert_eq!(
        items[0].get("status").and_then(json::JsonValue::as_str),
        Some("invalid")
    );
    assert_eq!(
        items[0].get("outcome").and_then(json::JsonValue::as_str),
        Some("status_preserved")
    );
    assert_eq!(
        items[1].get("name").and_then(json::JsonValue::as_str),
        Some("idx_person_name")
    );
    assert_eq!(
        items[1].get("status").and_then(json::JsonValue::as_str),
        Some("ready")
    );
    assert_eq!(
        items[1].get("outcome").and_then(json::JsonValue::as_str),
        Some("verified")
    );
}

#[test]
fn cli_memory_reindex_supports_table_and_ndjson_output() {
    let db = TestDb::new("cli_memory_reindex_outputs");
    let mut session = Session::new_in_memory();
    run(&mut session, "CREATE LABEL Doc");
    run(
        &mut session,
        "CREATE INDEX idx_doc_tags ON :Doc(tags) KIND LIST",
    );
    session.save_as(db.path()).unwrap();

    let table = run_cli(&[
        "memory",
        "reindex",
        "--db",
        db.path().to_str().unwrap(),
        "--output",
        "table",
    ]);
    assert!(table.status.success());
    let stdout = String::from_utf8(table.stdout).unwrap();
    assert!(stdout.contains("idx_doc_tags"));
    assert!(stdout.contains("verified"));

    let ndjson = run_cli(&[
        "memory",
        "reindex",
        "--db",
        db.path().to_str().unwrap(),
        "--output",
        "ndjson",
    ]);
    assert!(ndjson.status.success());
    let stdout = String::from_utf8(ndjson.stdout).unwrap();
    let lines = stdout.lines().collect::<Vec<_>>();
    assert_eq!(lines.len(), 4);
    assert!(lines[0].contains("\"kind\":\"memory_meta\""));
    assert!(lines[3].contains("\"kind\":\"memory_item\""));
    assert!(lines[3].contains("\"idx_doc_tags\""));
}

#[test]
fn cli_memory_json_errors_use_machine_envelope() {
    let dir = TempDir::new("cli_memory_json_error");
    let missing_db = dir.path().join("missing.cupld");
    let missing_db = missing_db.to_str().unwrap().to_owned();

    let output = run_cli(&[
        "memory",
        "check",
        "--db",
        missing_db.as_str(),
        "--output",
        "json",
    ]);

    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    let parsed = json::parse(&stderr).unwrap();
    assert_eq!(
        parsed.get("ok").and_then(json::JsonValue::as_bool),
        Some(false)
    );
    assert!(
        parsed
            .get("error")
            .and_then(|error| error.get("code"))
            .and_then(json::JsonValue::as_str)
            .is_some()
    );
}

#[test]
fn cli_memory_deferred_and_unknown_subcommands_error_clearly() {
    let repair = run_cli(&["memory", "repair", "--db", "default"]);
    assert!(!repair.status.success());
    let stderr = String::from_utf8(repair.stderr).unwrap();
    assert!(stderr.contains("intentionally out of scope"));

    let citation_audit = run_cli(&["memory", "citation-audit", "--db", "default"]);
    assert!(!citation_audit.status.success());
    let stderr = String::from_utf8(citation_audit.stderr).unwrap();
    assert!(stderr.contains("intentionally out of scope"));
    assert!(stderr.contains("citation-audit"));

    let unknown = run_cli(&["memory", "wat"]);
    assert!(!unknown.status.success());
    let stderr = String::from_utf8(unknown.stderr).unwrap();
    assert!(stderr.contains("unknown memory subcommand `wat`"));
}

#[test]
fn cli_compact_resets_wal_for_generated_db() {
    let db = TestDb::new("cli_compact");
    let mut session = db.open();
    seed_person_graph(&mut session);
    drop(session);

    let compact = run_cli(&["compact", "--db", db.path().to_str().unwrap()]);
    assert!(compact.status.success());

    let check = run_cli(&["check", "--db", db.path().to_str().unwrap()]);
    assert!(check.status.success());
    let stdout = String::from_utf8(check.stdout).unwrap();
    assert!(stdout.contains("wal_records=0"));
}

#[test]
fn cli_upgrade_backs_up_default_db_and_runs_checks() {
    let workspace = TempDir::new("cli_upgrade_default");
    let db_path = seed_workspace_default_db(workspace.path());
    let notes_root = workspace.path().join(".cupld").join("data");
    fs::create_dir_all(&notes_root).unwrap();

    let output = run_cli_in_dir(&["upgrade"], workspace.path());

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("backup="));
    assert!(stdout.contains("check=pass"));
    assert!(stdout.contains("memory_check=pass"));
    assert!(db_path.exists());

    let backups = fs::read_dir(db_path.parent().unwrap())
        .unwrap()
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("default.cupld.backup."))
        })
        .collect::<Vec<_>>();
    assert_eq!(backups.len(), 1);
}

#[cfg(unix)]
#[test]
fn cli_db_command_warns_when_latest_release_is_newer() {
    let workspace = TempDir::new("cli_upgrade_hint_workspace");
    seed_workspace_default_db(workspace.path());
    let curl_dir = TempDir::new("cli_upgrade_hint_curl");
    write_fake_curl(
        curl_dir.path(),
        r#"{"tag_name":"v99.0.0","html_url":"https://github.com/aeaston1/cupld/releases/tag/v99.0.0"}"#,
        0,
    );
    let path = curl_dir.path().to_str().unwrap();

    let output = run_cli_with_env_in_dir(
        &["schema", "--db", "default"],
        "",
        Some(workspace.path()),
        &[("CUPLD_NO_UPGRADE_CHECK", "0"), ("PATH", path)],
    );

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("A newer cupld release is available: v99.0.0"));
    assert!(stderr.contains("cupld upgrade --db"));
}

#[cfg(unix)]
#[test]
fn cli_db_command_stays_silent_when_latest_release_check_fails() {
    let workspace = TempDir::new("cli_upgrade_hint_failure_workspace");
    seed_workspace_default_db(workspace.path());
    let curl_dir = TempDir::new("cli_upgrade_hint_failure_curl");
    write_fake_curl(curl_dir.path(), "unavailable", 22);
    let path = curl_dir.path().to_str().unwrap();

    let output = run_cli_with_env_in_dir(
        &["schema", "--db", "default"],
        "",
        Some(workspace.path()),
        &[("CUPLD_NO_UPGRADE_CHECK", "0"), ("PATH", path)],
    );

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(!stderr.contains("A newer cupld release is available"));
}

#[test]
fn cli_visualise_requires_interactive_terminal() {
    let db = TestDb::new("cli_visualise_tty");
    let mut session = db.open();
    seed_person_graph(&mut session);
    drop(session);

    let output = run_cli(&["--visualise", "--db", db.path().to_str().unwrap()]);

    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("requires an interactive terminal"));
}

#[test]
fn cli_visualise_default_alias_still_requires_interactive_terminal() {
    let workspace = TempDir::new("cli_visualise_default_alias");
    seed_workspace_default_db(workspace.path());

    let output = run_cli_in_dir(&["--visualise", "--db", "default"], workspace.path());

    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("requires an interactive terminal"));
}

#[test]
fn cli_visualise_with_query_flag_still_rejects_piped_stdio() {
    let db = TestDb::new("cli_visualise_query");
    let mut session = db.open();
    seed_person_graph(&mut session);
    drop(session);

    let output = run_cli(&[
        "--visualise",
        "--db",
        db.path().to_str().unwrap(),
        "--query",
        "MATCH (n:Person) RETURN n LIMIT 5",
    ]);

    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("requires an interactive terminal"));
}

#[test]
fn cli_query_with_markdown_uses_default_cwd_root() {
    let db = TestDb::new("cli_markdown_default_root");
    let cwd = TempDir::new("cli_markdown_default_root_cwd");
    let notes_root = cwd.path().join(".cupld").join("data");
    fs::create_dir_all(&notes_root).unwrap();
    fs::write(notes_root.join("default.md"), "# Default Root Note").unwrap();

    let output = run_cli_in_dir(
        &[
            "query",
            "--db",
            db.path().to_str().unwrap(),
            "--with-md",
            "MATCH (d:MarkdownDocument) RETURN d.`md.title`",
        ],
        cwd.path(),
    );

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("Default Root Note"));
}

#[test]
fn cli_source_set_root_persists_default_markdown_root() {
    let db = TestDb::new("cli_markdown_set_root");
    let configured_root = TempDir::new("cli_markdown_configured_root");
    fs::write(
        configured_root.path().join("configured.md"),
        "# Configured Root Note",
    )
    .unwrap();

    let set_root = run_cli(&[
        "source",
        "set-root",
        "--db",
        db.path().to_str().unwrap(),
        configured_root.path().to_str().unwrap(),
    ]);
    assert!(set_root.status.success());

    let unrelated_cwd = TempDir::new("cli_markdown_other_cwd");
    let output = run_cli_in_dir(
        &[
            "query",
            "--db",
            db.path().to_str().unwrap(),
            "--with-md",
            "MATCH (d:MarkdownDocument) RETURN d.`src.path`, d.`md.title`",
        ],
        unrelated_cwd.path(),
    );

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("configured.md"));
    assert!(stdout.contains("Configured Root Note"));
}

#[test]
fn cli_source_set_root_works_with_default_db_alias() {
    let workspace = TempDir::new("cli_markdown_set_root_default_alias");
    let configured_root = TempDir::new("cli_markdown_set_root_default_alias_root");
    seed_workspace_default_db(workspace.path());
    fs::write(
        configured_root.path().join("configured.md"),
        "# Configured Root Note",
    )
    .unwrap();

    let set_root = run_cli_in_dir(
        &[
            "source",
            "set-root",
            "--db",
            "default",
            configured_root.path().to_str().unwrap(),
        ],
        workspace.path(),
    );
    assert!(set_root.status.success());

    let db_path = workspace_default_db_path(workspace.path());
    assert!(db_path.exists());

    let output = run_cli_in_dir(
        &[
            "query",
            "--db",
            "default",
            "--with-md",
            "MATCH (d:MarkdownDocument) RETURN d.`src.path`, d.`md.title`",
        ],
        workspace.path(),
    );

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("configured.md"));
    assert!(stdout.contains("Configured Root Note"));
}

#[test]
fn cli_query_root_override_wins_over_db_root() {
    let db = TestDb::new("cli_markdown_root_override");
    let configured_root = TempDir::new("cli_markdown_override_configured");
    let override_root = TempDir::new("cli_markdown_override_explicit");
    fs::write(
        configured_root.path().join("configured.md"),
        "# Configured Root Note",
    )
    .unwrap();
    fs::write(
        override_root.path().join("override.md"),
        "# Override Root Note",
    )
    .unwrap();

    let set_root = run_cli(&[
        "source",
        "set-root",
        "--db",
        db.path().to_str().unwrap(),
        configured_root.path().to_str().unwrap(),
    ]);
    assert!(set_root.status.success());

    let output = run_cli(&[
        "query",
        "--db",
        db.path().to_str().unwrap(),
        "--with-md",
        "--root",
        override_root.path().to_str().unwrap(),
        "MATCH (d:MarkdownDocument) RETURN d.`md.title`",
    ]);

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("Override Root Note"));
    assert!(!stdout.contains("Configured Root Note"));
}

#[test]
fn cli_sync_markdown_persists_documents_into_db() {
    let db = TestDb::new("cli_markdown_sync");
    let root = TempDir::new("cli_markdown_sync_root");
    fs::write(root.path().join("synced.md"), "# Synced From CLI").unwrap();

    let output = run_cli(&[
        "sync",
        "markdown",
        "--db",
        db.path().to_str().unwrap(),
        "--root",
        root.path().to_str().unwrap(),
    ]);

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("scanned=1"));

    let mut session = db.open();
    let result = session
        .execute_script(
            "MATCH (d:MarkdownDocument) RETURN d.`md.title`, d.`src.status`",
            &std::collections::BTreeMap::new(),
        )
        .unwrap()
        .remove(0);
    assert_eq!(result.rows.len(), 1);
    assert!(format!("{:?}", result.rows[0]).contains("Synced From CLI"));
    assert!(format!("{:?}", result.rows[0]).contains("current"));
}

#[test]
fn cli_sync_markdown_include_fs_graph_persists_structural_graph() {
    let db = TestDb::new("cli_markdown_sync_fs_graph");
    let root = TempDir::new("cli_markdown_sync_fs_graph_root");
    fs::create_dir_all(root.path().join("notes")).unwrap();
    fs::write(root.path().join("notes").join("synced.md"), "# Synced").unwrap();

    let output = run_cli(&[
        "sync",
        "markdown",
        "--db",
        db.path().to_str().unwrap(),
        "--root",
        root.path().to_str().unwrap(),
        "--include-fs-graph",
    ]);

    assert!(output.status.success());

    let mut session = db.open();
    let result = run(
        &mut session,
        "MATCH (doc:MarkdownDocument {`src.path`: 'notes/synced.md'})-[e:MD_IN_DIRECTORY]->(dir:MarkdownDirectory {`src.path`: 'notes'})
         RETURN dir.name, doc.`src.path`, e.`md.edge_source`",
    );
    assert_eq!(
        result.rows,
        vec![vec![
            RuntimeValue::String("notes".to_owned()),
            RuntimeValue::String("notes/synced.md".to_owned()),
            RuntimeValue::String("filesystem".to_owned()),
        ]]
    );
}

#[test]
fn cli_sync_markdown_watch_can_include_filesystem_graph() {
    let db = TestDb::new("cli_markdown_sync_fs_graph_watch");
    let root = TempDir::new("cli_markdown_sync_fs_graph_watch_root");
    fs::create_dir_all(root.path().join("project")).unwrap();
    fs::write(root.path().join("project").join("synced.md"), "# Synced").unwrap();

    let output = run_cli(&[
        "sync",
        "markdown",
        "--db",
        db.path().to_str().unwrap(),
        "--root",
        root.path().to_str().unwrap(),
        "--include-fs-graph",
        "--watch",
        "--max-runs",
        "1",
    ]);

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("runs=1"));
    assert!(stdout.contains("scanned=1"));

    let mut session = db.open();
    let result = run(
        &mut session,
        "MATCH (d:MarkdownDocument)-[:MD_IN_DIRECTORY]->(dir:MarkdownDirectory)
         RETURN d.`src.path`, dir.`src.path`
         ORDER BY d.`src.path`",
    );
    assert_eq!(
        result.rows,
        vec![vec![
            RuntimeValue::String("project/synced.md".to_owned()),
            RuntimeValue::String("project".to_owned()),
        ]]
    );
}

#[test]
fn cli_sync_markdown_reads_include_fs_graph_from_workspace_config() {
    let workspace = TempDir::new("cli_markdown_sync_fs_graph_config");
    let db_path = seed_workspace_default_db(workspace.path());
    let notes_root = workspace.path().join("notes");
    fs::create_dir_all(&notes_root).unwrap();
    fs::write(notes_root.join("configured.md"), "# Configured").unwrap();
    fs::write(
        workspace.path().join(".cupld").join("config.toml"),
        "version = 1\n\n[package]\nmarkdown_root = \"notes\"\n\n[markdown]\ninclude_fs_graph = true\n",
    )
    .unwrap();

    let output = run_cli_in_dir(&["sync", "markdown", "--db", "default"], workspace.path());

    assert!(output.status.success());

    let mut session = Session::open(&db_path).unwrap();
    let result = run(
        &mut session,
        "MATCH (doc:MarkdownDocument {`src.path`: 'configured.md'})-[:MD_IN_DIRECTORY]->(dir:MarkdownDirectory {`src.path`: '.'})
         RETURN doc.`src.path`",
    );
    assert_eq!(
        result.rows,
        vec![vec![RuntimeValue::String("configured.md".to_owned())]]
    );
}
