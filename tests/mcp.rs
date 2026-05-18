mod support;

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use cupld::json::{self, JsonValue};
use cupld::mcp::{self, McpConfig};
use cupld::{Session, sync_markdown_root};

use support::TestDb;

static NEXT_TEMP_DIR_ID: AtomicUsize = AtomicUsize::new(1);

#[test]
fn protocol_lists_memory_tools_and_resources() {
    let db = TestDb::new("mcp_protocol");
    let root = temp_dir("mcp_protocol");
    fs::create_dir_all(&root).unwrap();
    let config = config(db.path(), &root, false);

    let tools = rpc(&config, r#"{"jsonrpc":"2.0","id":1,"method":"tools/list"}"#);
    assert!(json_text(&tools).contains("memory_search"));

    let resources = rpc(
        &config,
        r#"{"jsonrpc":"2.0","id":2,"method":"resources/list"}"#,
    );
    assert!(json_text(&resources).contains("memory://index"));

    fs::remove_dir_all(root).unwrap();
}

#[test]
fn protocol_reports_malformed_json_and_unknown_methods() {
    let db = TestDb::new("mcp_protocol_errors");
    let root = temp_dir("mcp_protocol_errors");
    fs::create_dir_all(&root).unwrap();
    let config = config(db.path(), &root, false);

    let malformed = mcp::handle_json_line(&config, "{").unwrap();
    assert!(malformed.contains("parse_error"));
    let unknown = rpc(
        &config,
        r#"{"jsonrpc":"2.0","id":3,"method":"unknown/method"}"#,
    );
    assert!(json_text(&unknown).contains("method_not_found"));

    fs::remove_dir_all(root).unwrap();
}

#[test]
fn reads_use_db_state_not_unsynced_markdown() {
    let db = TestDb::new("mcp_db_only");
    let root = temp_dir("mcp_db_only");
    fs::create_dir_all(&root).unwrap();
    fs::write(root.join("synced.md"), "# Synced\nVisible").unwrap();
    sync_root(db.path(), &root);
    fs::write(root.join("unsynced.md"), "# Unsynced\nHidden").unwrap();

    let config = config(db.path(), &root, false);
    let response = call(
        &config,
        "memory_search",
        r#"{"query":"Unsynced","limit":10}"#,
    );
    let text = tool_text(&response);
    assert!(text.contains(r#""items":[]"#), "{text}");

    fs::remove_dir_all(root).unwrap();
}

#[test]
fn memory_sync_makes_markdown_visible_to_reads() {
    let db = TestDb::new("mcp_sync");
    let root = temp_dir("mcp_sync");
    fs::create_dir_all(&root).unwrap();
    fs::write(root.join("note.md"), "# Fresh\nNeedle").unwrap();

    let config = config(db.path(), &root, false);
    call(&config, "memory_sync", "{}");
    let response = call(&config, "memory_search", r#"{"query":"Needle"}"#);
    assert!(tool_text(&response).contains("note.md"));

    fs::remove_dir_all(root).unwrap();
}

#[test]
fn memory_search_exposes_retrieval_contract_metadata() {
    let db = TestDb::new("mcp_search_contract");
    let root = temp_dir("mcp_search_contract");
    fs::create_dir_all(root.join("notes")).unwrap();
    fs::write(
        root.join("notes/body.md"),
        format!("# Body Match\n\nNeedle {}", "x".repeat(600)),
    )
    .unwrap();
    fs::write(root.join("notes/title.md"), "# Needle Title\n\nOther").unwrap();
    sync_root(db.path(), &root);

    let config = config(db.path(), &root, false);
    let response = call(
        &config,
        "memory_search",
        r#"{"query":"  Needle  ","limit":10}"#,
    );
    let payload = tool_payload(&response);
    assert_eq!(
        payload.get("query").and_then(JsonValue::as_str),
        Some("Needle")
    );
    let text = json_text(&payload);
    assert!(text.contains(r#""mode":"lexical""#), "{text}");
    assert!(text.contains(r#""deterministic":true"#), "{text}");
    assert!(text.contains(r#""semantic":false"#), "{text}");
    assert!(text.contains(r#""index_used":false"#), "{text}");
    assert!(text.contains(r#""source":"cupld_db""#), "{text}");
    assert!(text.contains(r#""network_used":false"#), "{text}");

    let items = payload
        .get("items")
        .and_then(JsonValue::as_array)
        .expect("items array");
    assert_eq!(items.len(), 2);

    let first = &items[0];
    assert_eq!(
        first.get("path").and_then(JsonValue::as_str),
        Some("notes/title.md")
    );
    assert_eq!(first.get("rank").and_then(JsonValue::as_i64), Some(1));
    assert_eq!(first.get("score").and_then(JsonValue::as_i64), Some(1));
    assert_eq!(
        first.get("matched_category").and_then(JsonValue::as_str),
        Some("partial_title_or_path")
    );
    assert!(json_text(first).contains(r#""matched_fields":["title"]"#));
    assert!(first.get("uri").and_then(JsonValue::as_str).is_some());
    assert!(first.get("title").and_then(JsonValue::as_str).is_some());
    assert!(first.get("tags").and_then(JsonValue::as_array).is_some());
    assert!(first.get("snippet").and_then(JsonValue::as_str).is_some());

    let second = &items[1];
    assert_eq!(second.get("rank").and_then(JsonValue::as_i64), Some(2));
    assert_eq!(second.get("score").and_then(JsonValue::as_i64), Some(3));
    assert_eq!(
        second.get("matched_category").and_then(JsonValue::as_str),
        Some("body")
    );
    assert!(json_text(second).contains(r#""matched_fields":["body"]"#));
    assert!(json_text(second).contains(r#""truncated":true"#));
    assert!(json_text(second).contains(r#""max_chars":500"#));
    assert!(json_text(second).contains(r#""source":"body""#));

    fs::remove_dir_all(root).unwrap();
}

#[test]
fn memory_search_uses_markdown_indexes_without_changing_ranked_results() {
    let fallback_db = TestDb::new("mcp_search_fallback_candidates");
    let indexed_db = TestDb::new("mcp_search_indexed_candidates");
    let root = temp_dir("mcp_search_indexed_candidates");
    fs::create_dir_all(root.join("notes")).unwrap();
    fs::write(
        root.join("notes/body.md"),
        "# Body Match\n\nNeedle appears in the body.",
    )
    .unwrap();
    fs::write(root.join("notes/title.md"), "# Needle Title\n\nOther").unwrap();
    fs::write(root.join("notes/other.md"), "# Other\n\nNo match").unwrap();
    sync_root(fallback_db.path(), &root);
    sync_root(indexed_db.path(), &root);
    create_markdown_search_indexes(indexed_db.path());

    let fallback = tool_payload(&call(
        &config(fallback_db.path(), &root, false),
        "memory_search",
        r#"{"query":"Needle","limit":10}"#,
    ));
    let indexed = tool_payload(&call(
        &config(indexed_db.path(), &root, false),
        "memory_search",
        r#"{"query":"Needle","limit":10}"#,
    ));

    assert_eq!(
        fallback
            .get("retrieval")
            .and_then(|retrieval| retrieval.get("index_used"))
            .and_then(JsonValue::as_bool),
        Some(false)
    );
    assert_eq!(
        indexed
            .get("retrieval")
            .and_then(|retrieval| retrieval.get("index_used"))
            .and_then(JsonValue::as_bool),
        Some(true)
    );
    assert_eq!(result_paths(&fallback), result_paths(&indexed));
    assert_eq!(result_scores(&fallback), result_scores(&indexed));

    fs::remove_dir_all(root).unwrap();
}

#[test]
fn memory_search_rejects_missing_and_blank_queries() {
    let db = TestDb::new("mcp_search_validation");
    let root = temp_dir("mcp_search_validation");
    fs::create_dir_all(&root).unwrap();
    fs::write(root.join("note.md"), "# Note\n\nShould not match").unwrap();
    sync_root(db.path(), &root);

    let config = config(db.path(), &root, false);
    let missing = call(&config, "memory_search", "{}");
    assert!(tool_text(&missing).contains(r#""code":"validation_error""#));
    assert!(tool_text(&missing).contains("expected query"));

    let blank = call(&config, "memory_search", r#"{"query":"   "}"#);
    let text = tool_text(&blank);
    assert!(text.contains(r#""code":"validation_error""#), "{text}");
    assert!(text.contains("expected non-empty query"), "{text}");
    assert!(!text.contains(r#""items":["#), "{text}");
    assert!(!text.contains("note.md"), "{text}");

    fs::remove_dir_all(root).unwrap();
}

#[test]
fn memory_search_snippet_falls_back_to_raw_when_body_is_empty() {
    let db = TestDb::new("mcp_search_empty_body");
    let root = temp_dir("mcp_search_empty_body");
    fs::create_dir_all(&root).unwrap();
    fs::write(root.join("heading.md"), "---\ntitle: Heading Only\n---\n").unwrap();
    sync_root(db.path(), &root);

    let config = config(db.path(), &root, false);
    let response = call(&config, "memory_search", r#"{"query":"Heading Only"}"#);
    let payload = tool_payload(&response);
    let item = payload
        .get("items")
        .and_then(JsonValue::as_array)
        .and_then(|items| items.first())
        .expect("search item");
    assert_eq!(
        item.get("snippet").and_then(JsonValue::as_str),
        Some("---\ntitle: Heading Only\n---\n")
    );
    let text = json_text(item);
    assert!(text.contains(r#""source":"raw""#), "{text}");
    assert!(text.contains(r#""empty_body_fallback":true"#), "{text}");
    assert!(text.contains(r#""truncated":false"#), "{text}");

    fs::remove_dir_all(root).unwrap();
}

#[test]
fn memory_add_writes_markdown_and_syncs_before_success() {
    let db = TestDb::new("mcp_add");
    let root = temp_dir("mcp_add");
    fs::create_dir_all(&root).unwrap();

    let config = config(db.path(), &root, false);
    let added = call(
        &config,
        "memory_add",
        r#"{"title":"Added Note","tags":["project"],"path_hint":"notes/added.md","content":"Remember this detail."}"#,
    );
    assert!(
        tool_text(&added).contains(r#""db_updated":true"#),
        "{}",
        tool_text(&added)
    );
    assert!(root.join("notes/added.md").exists());
    let found = call(&config, "memory_search", r#"{"query":"Remember"}"#);
    assert!(tool_text(&found).contains("notes/added.md"));

    fs::remove_dir_all(root).unwrap();
}

#[test]
fn read_only_rejects_write_and_sync_tools() {
    let db = TestDb::new("mcp_read_only");
    let root = temp_dir("mcp_read_only");
    fs::create_dir_all(&root).unwrap();
    let config = config(db.path(), &root, true);

    let sync = call(&config, "memory_sync", "{}");
    assert!(tool_text(&sync).contains("read_only"));
    let add = call(&config, "memory_add", r#"{"content":"Nope"}"#);
    assert!(tool_text(&add).contains("read_only"));

    fs::remove_dir_all(root).unwrap();
}

#[test]
fn memory_add_rejects_path_traversal() {
    let db = TestDb::new("mcp_traversal");
    let root = temp_dir("mcp_traversal");
    fs::create_dir_all(&root).unwrap();
    let config = config(db.path(), &root, false);

    let response = call(
        &config,
        "memory_add",
        r#"{"path_hint":"../escape.md","content":"Nope"}"#,
    );
    assert!(tool_text(&response).contains("invalid_path"));
    assert!(!root.join("../escape.md").exists());

    fs::remove_dir_all(root).unwrap();
}

#[cfg(unix)]
#[test]
fn memory_add_rejects_symlink_escape() {
    use std::os::unix::fs::symlink;

    let db = TestDb::new("mcp_symlink");
    let root = temp_dir("mcp_symlink");
    let outside = temp_dir("mcp_symlink_outside");
    fs::create_dir_all(&root).unwrap();
    fs::create_dir_all(&outside).unwrap();
    symlink(&outside, root.join("link")).unwrap();
    let config = config(db.path(), &root, false);

    let response = call(
        &config,
        "memory_add",
        r#"{"path_hint":"link/escape.md","content":"Nope"}"#,
    );
    assert!(tool_text(&response).contains("invalid_path"));
    assert!(!outside.join("escape.md").exists());

    fs::remove_dir_all(root).unwrap();
    fs::remove_dir_all(outside).unwrap();
}

fn config(db_path: &Path, root: &Path, read_only: bool) -> McpConfig {
    McpConfig {
        db_path: db_path.to_path_buf(),
        root_override: Some(root.to_path_buf()),
        read_only,
    }
}

fn rpc(config: &McpConfig, input: &str) -> JsonValue {
    let output = mcp::handle_json_line(config, input).unwrap();
    json::parse(&output).unwrap()
}

fn call(config: &McpConfig, name: &str, args: &str) -> JsonValue {
    let input = format!(
        r#"{{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{{"name":"{name}","arguments":{args}}}}}"#
    );
    rpc(config, &input)
}

fn json_text(value: &JsonValue) -> String {
    json::stringify(value)
}

fn tool_text(value: &JsonValue) -> String {
    value
        .get("result")
        .and_then(|result| result.get("content"))
        .and_then(JsonValue::as_array)
        .and_then(|items| items.first())
        .and_then(|item| item.get("text"))
        .and_then(JsonValue::as_str)
        .unwrap()
        .to_owned()
}

fn tool_payload(value: &JsonValue) -> JsonValue {
    json::parse(&tool_text(value)).unwrap()
}

fn sync_root(db_path: &Path, root: &Path) {
    let mut session = Session::open(db_path).unwrap();
    let mut engine = session.engine().clone();
    sync_markdown_root(&mut engine, root).unwrap();
    engine.commit().unwrap();
    session.replace_engine(engine).unwrap();
    session.save().unwrap();
}

fn create_markdown_search_indexes(db_path: &Path) {
    let mut session = Session::open(db_path).unwrap();
    session
        .execute_script(
            "CREATE INDEX ON :MarkdownDocument(`md.body`) KIND FULLTEXT",
            &Default::default(),
        )
        .unwrap();
    session
        .execute_script(
            "CREATE INDEX ON :MarkdownDocument(`md.tags`) KIND LIST",
            &Default::default(),
        )
        .unwrap();
    session.save().unwrap();
}

fn result_paths(payload: &JsonValue) -> Vec<String> {
    payload
        .get("items")
        .and_then(JsonValue::as_array)
        .unwrap()
        .iter()
        .map(|item| {
            item.get("path")
                .and_then(JsonValue::as_str)
                .unwrap()
                .to_owned()
        })
        .collect()
}

fn result_scores(payload: &JsonValue) -> Vec<i64> {
    payload
        .get("items")
        .and_then(JsonValue::as_array)
        .unwrap()
        .iter()
        .map(|item| item.get("score").and_then(JsonValue::as_i64).unwrap())
        .collect()
}

fn temp_dir(prefix: &str) -> PathBuf {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let suffix = NEXT_TEMP_DIR_ID.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "cupld_{prefix}_{}_{}_{}",
        std::process::id(),
        timestamp,
        suffix
    ))
}
