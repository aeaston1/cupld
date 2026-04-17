mod support;

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use cupld::Session;
use serde_json::Value as JsonValue;

use support::{TestDb, seed_person_graph};

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
    let mut child = Command::new(env!("CARGO_BIN_EXE_cupld"))
        .args(args)
        .current_dir(dir.unwrap_or_else(|| Path::new(".")))
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(input.as_bytes())
        .unwrap();
    child.wait_with_output().unwrap()
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
    let parsed: JsonValue = serde_json::from_str(&stdout).unwrap();
    assert_eq!(parsed["ok"], JsonValue::Bool(true));
    assert_eq!(parsed["command"], JsonValue::String("query".to_owned()));
    assert_eq!(
        parsed["policy"]["execution_mode"],
        JsonValue::String("automation_read_write".to_owned())
    );
    assert_eq!(parsed["policy"]["max_rows"], JsonValue::Number(2.into()));
    assert_eq!(
        parsed["results"][0]["row_count"],
        JsonValue::Number(2.into())
    );
    assert_eq!(parsed["results"][0]["truncated"], JsonValue::Bool(true));
    assert_eq!(parsed["results"][0]["rows"][0]["col_1"], "Ada");
    assert_eq!(parsed["results"][0]["rows"][1]["col_1"], "Alan");
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
    let parsed: JsonValue = serde_json::from_str(&stderr).unwrap();
    assert_eq!(parsed["ok"], JsonValue::Bool(false));
    assert_eq!(
        parsed["error"]["code"],
        JsonValue::String("params_json_parse".to_owned())
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
        "--top-k",
        "2",
    ]);

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    let lines = stdout.lines().collect::<Vec<_>>();
    assert_eq!(lines.len(), 3);

    let meta: JsonValue = serde_json::from_str(lines[0]).unwrap();
    assert_eq!(meta["kind"], JsonValue::String("context_meta".to_owned()));
    assert_eq!(
        meta["policy"]["execution_mode"],
        JsonValue::String("automation_read_only".to_owned())
    );
    assert_eq!(
        meta["policy"]["retrieval_budget"]["nodes"],
        JsonValue::Number(2.into())
    );
    assert_eq!(
        meta["retrieval_usage"]["nodes"],
        JsonValue::Number(2.into())
    );

    let item: JsonValue = serde_json::from_str(lines[1]).unwrap();
    assert_eq!(item["kind"], JsonValue::String("context_item".to_owned()));
    assert!(item["item"]["node_id"].is_number());
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
            "--with-markdown",
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
            "--with-markdown",
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
        "--with-markdown",
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
