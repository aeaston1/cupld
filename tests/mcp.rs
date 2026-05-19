mod support;

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use cupld::json::{self, JsonValue};
use cupld::mcp::{self, McpConfig};
use cupld::{MarkdownSyncOptions, Session, sync_markdown_root, sync_markdown_root_with_options};

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
    assert!(json_text(&tools).contains("memory_context"));

    let resources = rpc(
        &config,
        r#"{"jsonrpc":"2.0","id":2,"method":"resources/list"}"#,
    );
    assert!(json_text(&resources).contains("memory://index"));

    fs::remove_dir_all(root).unwrap();
}

#[test]
fn memory_health_reports_harness_readiness() {
    let db = TestDb::new("mcp_health");
    let root = temp_dir("mcp_health");
    fs::create_dir_all(&root).unwrap();
    let writable = config(db.path(), &root, false);

    let payload = tool_payload(&call(&writable, "memory_health", "{}"));
    assert_eq!(payload.get("ok").and_then(JsonValue::as_bool), Some(true));
    assert_eq!(
        payload
            .get("markdown_root_exists")
            .and_then(JsonValue::as_bool),
        Some(true)
    );
    assert_eq!(
        payload.get("safe_for_writes").and_then(JsonValue::as_bool),
        Some(true)
    );
    assert_eq!(
        payload.get("write_status").and_then(JsonValue::as_str),
        Some("ready")
    );
    assert!(json_text(&payload).contains("MCP reads are DB-backed"));

    let read_only = config(db.path(), &root, true);
    let payload = tool_payload(&call(&read_only, "memory_health", "{}"));
    assert_eq!(
        payload.get("safe_for_writes").and_then(JsonValue::as_bool),
        Some(false)
    );
    assert_eq!(
        payload.get("write_status").and_then(JsonValue::as_str),
        Some("read_only")
    );

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
    assert_eq!(first.get("score").and_then(JsonValue::as_i64), Some(100));
    assert_eq!(
        first.get("lexical_score").and_then(JsonValue::as_i64),
        Some(100)
    );
    assert!(matches!(first.get("semantic_score"), Some(JsonValue::Null)));
    assert!(matches!(first.get("blended_score"), Some(JsonValue::Null)));
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
    assert_eq!(second.get("score").and_then(JsonValue::as_i64), Some(300));
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
fn memory_context_expands_from_memory_uri_without_shelling_out() {
    let db = TestDb::new("mcp_context");
    let root = temp_dir("mcp_context");
    fs::create_dir_all(root.join("notes")).unwrap();
    fs::write(
        root.join("notes/seed.md"),
        "# Seed\n\n[[neighbor]] linked context",
    )
    .unwrap();
    fs::write(root.join("notes/neighbor.md"), "# Neighbor\n\nNearby").unwrap();
    sync_root(db.path(), &root);

    let config = config(db.path(), &root, true);
    let response = call(
        &config,
        "memory_context",
        r#"{"id_or_uri":"memory://note/notes/seed.md","depth":1,"max_nodes":10,"max_edges":10}"#,
    );
    let payload = tool_payload(&response);
    assert_eq!(payload.get("ok").and_then(JsonValue::as_bool), Some(true));
    assert_eq!(
        payload.get("command").and_then(JsonValue::as_str),
        Some("context")
    );
    assert!(json_text(&payload).contains("notes/seed.md"));
    assert!(json_text(&payload).contains("notes/neighbor.md"));

    fs::remove_dir_all(root).unwrap();
}

#[test]
fn memory_search_defaults_to_lexical_when_retrieval_mode_is_omitted() {
    let db = TestDb::new("mcp_search_default_lexical_mode");
    let root = temp_dir("mcp_search_default_lexical_mode");
    fs::create_dir_all(&root).unwrap();
    fs::write(root.join("note.md"), "# Needle\n\nDefault retrieval").unwrap();
    sync_root(db.path(), &root);

    let response = call(
        &config(db.path(), &root, false),
        "memory_search",
        r#"{"query":"Needle"}"#,
    );
    let payload = tool_payload(&response);

    assert_eq!(payload.get("ok").and_then(JsonValue::as_bool), Some(true));
    assert_eq!(
        payload
            .get("retrieval")
            .and_then(|retrieval| retrieval.get("mode"))
            .and_then(JsonValue::as_str),
        Some("lexical")
    );
    assert_eq!(result_paths(&payload), vec!["note.md"]);

    fs::remove_dir_all(root).unwrap();
}

#[test]
fn memory_search_semantic_opt_in_returns_stable_unconfigured_boundary() {
    let db = TestDb::new("mcp_search_semantic_unconfigured");
    let root = temp_dir("mcp_search_semantic_unconfigured");
    fs::create_dir_all(&root).unwrap();
    fs::write(root.join("note.md"), "# Needle\n\nNo semantic fallback").unwrap();
    sync_root(db.path(), &root);

    let response = call(
        &config(db.path(), &root, false),
        "memory_search",
        r#"{"query":"Needle","retrieval_mode":"semantic"}"#,
    );
    let payload = tool_payload(&response);
    let text = json_text(&payload);

    assert_eq!(payload.get("ok").and_then(JsonValue::as_bool), Some(false));
    assert!(text.contains(r#""code":"unconfigured""#), "{text}");
    assert!(text.contains(r#""mode":"semantic""#), "{text}");
    assert!(text.contains(r#""semantic":true"#), "{text}");
    assert!(text.contains(r#""backend":"unconfigured""#), "{text}");
    assert!(text.contains(r#""network_used":false"#), "{text}");
    assert_eq!(
        payload
            .get("items")
            .and_then(JsonValue::as_array)
            .unwrap()
            .len(),
        0
    );
    assert!(!text.contains("note.md"), "{text}");

    fs::remove_dir_all(root).unwrap();
}

#[test]
fn memory_search_rejects_unknown_retrieval_mode() {
    let db = TestDb::new("mcp_search_unknown_retrieval_mode");
    let root = temp_dir("mcp_search_unknown_retrieval_mode");
    fs::create_dir_all(&root).unwrap();
    fs::write(root.join("note.md"), "# Needle\n\nBody").unwrap();
    sync_root(db.path(), &root);

    let response = call(
        &config(db.path(), &root, false),
        "memory_search",
        r#"{"query":"Needle","retrieval_mode":"remote"}"#,
    );
    let text = tool_text(&response);

    assert!(text.contains(r#""code":"validation_error""#), "{text}");
    assert!(text.contains("expected retrieval_mode"), "{text}");
    assert!(!text.contains("note.md"), "{text}");

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
fn memory_search_multi_term_body_query_matches_with_or_without_indexes() {
    let fallback_db = TestDb::new("mcp_search_multi_term_fallback_candidates");
    let indexed_db = TestDb::new("mcp_search_multi_term_indexed_candidates");
    let root = temp_dir("mcp_search_multi_term_indexed_candidates");
    fs::create_dir_all(root.join("notes")).unwrap();
    fs::write(
        root.join("notes/body.md"),
        "# Body Match\n\nBerth assignments are reviewed before the annual report.",
    )
    .unwrap();
    fs::write(
        root.join("notes/weak.md"),
        "# Weak Match\n\nBerth assignments are reviewed weekly.",
    )
    .unwrap();
    fs::write(root.join("notes/other.md"), "# Other\n\nNo match").unwrap();
    sync_root(fallback_db.path(), &root);
    sync_root(indexed_db.path(), &root);
    create_markdown_search_indexes(indexed_db.path());

    let fallback = tool_payload(&call(
        &config(fallback_db.path(), &root, false),
        "memory_search",
        r#"{"query":"berth annual","limit":10}"#,
    ));
    let indexed = tool_payload(&call(
        &config(indexed_db.path(), &root, false),
        "memory_search",
        r#"{"query":"berth annual","limit":10}"#,
    ));

    assert_eq!(
        indexed
            .get("retrieval")
            .and_then(|retrieval| retrieval.get("index_used"))
            .and_then(JsonValue::as_bool),
        Some(false)
    );
    assert_eq!(result_paths(&fallback), result_paths(&indexed));
    assert_eq!(result_scores(&fallback), result_scores(&indexed));
    assert_eq!(
        result_paths(&indexed),
        vec!["notes/body.md".to_owned(), "notes/weak.md".to_owned(),]
    );
    assert_eq!(result_scores(&indexed), vec![300, 301]);

    fs::remove_dir_all(root).unwrap();
}

#[test]
fn memory_search_multi_term_cross_field_query_matches_with_or_without_indexes() {
    let fallback_db = TestDb::new("mcp_search_cross_field_fallback_candidates");
    let indexed_db = TestDb::new("mcp_search_cross_field_indexed_candidates");
    let root = temp_dir("mcp_search_cross_field_indexed_candidates");
    fs::create_dir_all(root.join("notes")).unwrap();
    fs::write(
        root.join("notes/berth.md"),
        "# Berth Schedule\n\nThe annual report is ready.",
    )
    .unwrap();
    fs::write(root.join("notes/other.md"), "# Other\n\nNo match").unwrap();
    sync_root(fallback_db.path(), &root);
    sync_root(indexed_db.path(), &root);
    create_markdown_search_indexes(indexed_db.path());

    let fallback = tool_payload(&call(
        &config(fallback_db.path(), &root, false),
        "memory_search",
        r#"{"query":"berth annual","limit":10}"#,
    ));
    let indexed = tool_payload(&call(
        &config(indexed_db.path(), &root, false),
        "memory_search",
        r#"{"query":"berth annual","limit":10}"#,
    ));

    assert_eq!(result_paths(&fallback), result_paths(&indexed));
    assert_eq!(result_scores(&fallback), result_scores(&indexed));
    assert_eq!(result_paths(&indexed), vec!["notes/berth.md".to_owned()]);

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
fn memory_search_ranks_exact_partial_structured_body_and_ties_deterministically() {
    let db = TestDb::new("mcp_search_lexical_ranking");
    let root = temp_dir("mcp_search_lexical_ranking");
    fs::create_dir_all(root.join("notes")).unwrap();
    fs::write(root.join("alpha.md"), "# Alpha Project\n\nUnrelated").unwrap();
    fs::write(root.join("alpha project.md"), "# Other\n\nUnrelated").unwrap();
    fs::write(
        root.join("notes/alpha-project.md"),
        "# Something Else\n\nUnrelated",
    )
    .unwrap();
    fs::write(
        root.join("notes/structured.md"),
        "---\naliases: [Alpha Project]\ntags: [research]\n---\n# Planning\n\nUnrelated",
    )
    .unwrap();
    fs::write(
        root.join("notes/body-a.md"),
        "# Body A\n\nThe alpha project details live here.",
    )
    .unwrap();
    fs::write(
        root.join("notes/body-b.md"),
        "# Body B\n\nThe alpha project details live here too.",
    )
    .unwrap();
    sync_root(db.path(), &root);

    let config = config(db.path(), &root, false);
    let response = call(
        &config,
        "memory_search",
        r#"{"query":"Alpha Project","limit":10}"#,
    );
    let payload = tool_payload(&response);
    let paths = item_paths(&payload);
    assert_eq!(
        paths,
        vec![
            "alpha.md",
            "alpha project.md",
            "notes/alpha-project.md",
            "notes/structured.md",
            "notes/body-a.md",
            "notes/body-b.md",
        ]
    );

    let items = payload
        .get("items")
        .and_then(JsonValue::as_array)
        .expect("items array");
    assert_eq!(items[0].get("score").and_then(JsonValue::as_i64), Some(0));
    assert_eq!(items[1].get("score").and_then(JsonValue::as_i64), Some(110));
    assert_eq!(items[2].get("score").and_then(JsonValue::as_i64), Some(110));
    assert_eq!(items[3].get("score").and_then(JsonValue::as_i64), Some(200));
    assert_eq!(items[4].get("score").and_then(JsonValue::as_i64), Some(300));
    assert_eq!(
        items[3].get("matched_category").and_then(JsonValue::as_str),
        Some("structured_metadata")
    );
    assert!(json_text(&items[3]).contains(r#""matched_fields":["aliases"]"#));

    let exact_path = call(
        &config,
        "memory_search",
        r#"{"query":"alpha project.md","limit":10}"#,
    );
    let exact_path_payload = tool_payload(&exact_path);
    let exact_path_items = exact_path_payload
        .get("items")
        .and_then(JsonValue::as_array)
        .expect("items array");
    assert_eq!(
        exact_path_items[0].get("path").and_then(JsonValue::as_str),
        Some("alpha project.md")
    );
    assert_eq!(
        exact_path_items[0]
            .get("matched_category")
            .and_then(JsonValue::as_str),
        Some("exact_title_or_path")
    );

    fs::remove_dir_all(root).unwrap();
}

#[test]
fn memory_search_ranks_more_matched_terms_within_a_tier() {
    let db = TestDb::new("mcp_search_multi_term");
    let root = temp_dir("mcp_search_multi_term");
    fs::create_dir_all(root.join("notes")).unwrap();
    fs::write(root.join("notes/one.md"), "# Alpha Note\n\nUnrelated").unwrap();
    fs::write(root.join("notes/two.md"), "# Alpha Beta Note\n\nUnrelated").unwrap();
    sync_root(db.path(), &root);

    let config = config(db.path(), &root, false);
    let response = call(
        &config,
        "memory_search",
        r#"{"query":"alpha beta","limit":10}"#,
    );
    let payload = tool_payload(&response);
    let items = payload
        .get("items")
        .and_then(JsonValue::as_array)
        .expect("items array");
    assert_eq!(
        items[0].get("path").and_then(JsonValue::as_str),
        Some("notes/two.md")
    );
    assert_eq!(items[0].get("score").and_then(JsonValue::as_i64), Some(100));
    assert_eq!(
        items[1].get("path").and_then(JsonValue::as_str),
        Some("notes/one.md")
    );
    assert_eq!(items[1].get("score").and_then(JsonValue::as_i64), Some(101));

    fs::remove_dir_all(root).unwrap();
}

#[test]
fn memory_search_uses_same_directory_signal_as_weak_tie_breaker() {
    let db = TestDb::new("mcp_search_same_directory_signal");
    let root = temp_dir("mcp_search_same_directory_signal");
    fs::create_dir_all(root.join("projects/alpha")).unwrap();
    fs::create_dir_all(&root).unwrap();
    fs::write(
        root.join("projects/alpha/source.md"),
        "# Needle Source\n\nTop lexical anchor.",
    )
    .unwrap();
    fs::write(root.join("a-outside.md"), "# Outside\n\nNeedle detail.").unwrap();
    fs::write(
        root.join("projects/alpha/z-near.md"),
        "# Near\n\nNeedle detail.",
    )
    .unwrap();
    sync_root_with_fs_graph(db.path(), &root);

    let config = config(db.path(), &root, false);
    let payload = tool_payload(&call(
        &config,
        "memory_search",
        r#"{"query":"Needle","limit":10}"#,
    ));
    assert_eq!(
        item_paths(&payload),
        vec![
            "projects/alpha/source.md",
            "projects/alpha/z-near.md",
            "a-outside.md",
        ]
    );
    assert_eq!(
        payload
            .get("retrieval")
            .and_then(|retrieval| retrieval.get("structural_signal_available"))
            .and_then(JsonValue::as_bool),
        Some(true)
    );
    let items = payload
        .get("items")
        .and_then(JsonValue::as_array)
        .expect("items array");
    let near_text = json_text(&items[1]);
    assert!(
        near_text.contains(r#""kind":"same_directory""#),
        "{near_text}"
    );
    assert!(
        near_text.contains(r#""edge_types":["MD_IN_DIRECTORY"]"#),
        "{near_text}"
    );
    assert!(near_text.contains(r#""edge_weight":25"#), "{near_text}");
    assert_eq!(items[1].get("score").and_then(JsonValue::as_i64), Some(300));

    fs::remove_dir_all(root).unwrap();
}

#[test]
fn memory_search_uses_parent_child_directory_signal_as_weak_tie_breaker() {
    let db = TestDb::new("mcp_search_parent_child_signal");
    let root = temp_dir("mcp_search_parent_child_signal");
    fs::create_dir_all(root.join("projects/alpha/child")).unwrap();
    fs::create_dir_all(root.join("projects/zeta")).unwrap();
    fs::write(
        root.join("projects/alpha/source.md"),
        "# Needle Source\n\nTop lexical anchor.",
    )
    .unwrap();
    fs::write(
        root.join("projects/zeta/a-outside.md"),
        "# Outside\n\nNeedle detail.",
    )
    .unwrap();
    fs::write(
        root.join("projects/alpha/child/z-child.md"),
        "# Child\n\nNeedle detail.",
    )
    .unwrap();
    sync_root_with_fs_graph(db.path(), &root);

    let config = config(db.path(), &root, false);
    let payload = tool_payload(&call(
        &config,
        "memory_search",
        r#"{"query":"Needle","limit":10}"#,
    ));
    assert_eq!(
        item_paths(&payload),
        vec![
            "projects/alpha/source.md",
            "projects/alpha/child/z-child.md",
            "projects/zeta/a-outside.md",
        ]
    );
    let items = payload
        .get("items")
        .and_then(JsonValue::as_array)
        .expect("items array");
    let child_text = json_text(&items[1]);
    assert!(
        child_text.contains(r#""kind":"parent_child_directory""#),
        "{child_text}"
    );
    assert!(
        child_text.contains(r#""edge_types":["MD_IN_DIRECTORY","MD_PARENT_DIRECTORY"]"#),
        "{child_text}"
    );
    assert_eq!(items[1].get("score").and_then(JsonValue::as_i64), Some(300));

    fs::remove_dir_all(root).unwrap();
}

#[test]
fn memory_search_without_filesystem_graph_keeps_lexical_order() {
    let db = TestDb::new("mcp_search_no_filesystem_signal");
    let root = temp_dir("mcp_search_no_filesystem_signal");
    fs::create_dir_all(root.join("projects/alpha")).unwrap();
    fs::create_dir_all(&root).unwrap();
    fs::write(
        root.join("projects/alpha/source.md"),
        "# Needle Source\n\nTop lexical anchor.",
    )
    .unwrap();
    fs::write(root.join("a-outside.md"), "# Outside\n\nNeedle detail.").unwrap();
    fs::write(
        root.join("projects/alpha/z-near.md"),
        "# Near\n\nNeedle detail.",
    )
    .unwrap();
    sync_root(db.path(), &root);

    let config = config(db.path(), &root, false);
    let payload = tool_payload(&call(
        &config,
        "memory_search",
        r#"{"query":"Needle","limit":10}"#,
    ));
    assert_eq!(
        item_paths(&payload),
        vec![
            "projects/alpha/source.md",
            "a-outside.md",
            "projects/alpha/z-near.md",
        ]
    );
    assert_eq!(
        payload
            .get("retrieval")
            .and_then(|retrieval| retrieval.get("structural_signal_available"))
            .and_then(JsonValue::as_bool),
        Some(false)
    );
    assert!(json_text(&payload).contains(r#""structural_signal":{"score":0,"evidence":[]}"#));

    fs::remove_dir_all(root).unwrap();
}

#[test]
fn memory_search_keeps_authored_links_separate_from_filesystem_signal() {
    let db = TestDb::new("mcp_search_authored_link_separation");
    let root = temp_dir("mcp_search_authored_link_separation");
    fs::create_dir_all(root.join("notes")).unwrap();
    fs::write(
        root.join("notes/source.md"),
        "# Needle Source\n\n[[linked]]",
    )
    .unwrap();
    fs::write(root.join("notes/linked.md"), "# Linked\n\nNeedle detail.").unwrap();
    sync_root_with_fs_graph(db.path(), &root);

    let config = config(db.path(), &root, false);
    let payload = tool_payload(&call(
        &config,
        "memory_search",
        r#"{"query":"Needle","limit":10}"#,
    ));
    let text = json_text(&payload);
    assert!(text.contains(r#""kind":"same_directory""#), "{text}");
    assert!(text.contains("MD_IN_DIRECTORY"), "{text}");
    assert!(!text.contains("MD_LINKS_TO"), "{text}");

    fs::remove_dir_all(root).unwrap();
}

#[test]
fn memory_search_filters_tags_before_ranking_and_uses_matching_snippets() {
    let db = TestDb::new("mcp_search_tags_snippets");
    let root = temp_dir("mcp_search_tags_snippets");
    fs::create_dir_all(root.join("notes")).unwrap();
    fs::write(
        root.join("notes/body.md"),
        "---\ntags: [keep]\n---\n# Body\n\nIntro line.\nThe useful needle context is here.\n",
    )
    .unwrap();
    fs::write(
        root.join("notes/heading.md"),
        "---\ntitle: Metadata Title\ntags: [keep]\n---\n# Needle Heading\n\nBody without query.",
    )
    .unwrap();
    fs::write(
        root.join("notes/filtered.md"),
        "---\ntags: [drop]\n---\n# Needle Exact\n\nShould not appear.",
    )
    .unwrap();
    sync_root(db.path(), &root);

    let config = config(db.path(), &root, false);
    let response = call(
        &config,
        "memory_search",
        r#"{"query":"needle","tags":["keep"],"limit":10}"#,
    );
    let payload = tool_payload(&response);
    let paths = item_paths(&payload);
    assert_eq!(paths, vec!["notes/heading.md", "notes/body.md"]);
    let items = payload
        .get("items")
        .and_then(JsonValue::as_array)
        .expect("items array");
    assert_eq!(
        items[0].get("snippet").and_then(JsonValue::as_str),
        Some("Needle Heading")
    );
    assert!(json_text(&items[0]).contains(r#""source":"headings""#));
    assert_eq!(
        items[1].get("snippet").and_then(JsonValue::as_str),
        Some("The useful needle context is here.")
    );
    assert!(json_text(&items[1]).contains(r#""source":"body""#));

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

fn item_paths(payload: &JsonValue) -> Vec<&str> {
    payload
        .get("items")
        .and_then(JsonValue::as_array)
        .expect("items array")
        .iter()
        .map(|item| item.get("path").and_then(JsonValue::as_str).unwrap())
        .collect()
}

fn sync_root(db_path: &Path, root: &Path) {
    let mut session = Session::open(db_path).unwrap();
    let mut engine = session.engine().clone();
    sync_markdown_root(&mut engine, root).unwrap();
    engine.commit().unwrap();
    session.replace_engine(engine).unwrap();
    session.save().unwrap();
}

fn sync_root_with_fs_graph(db_path: &Path, root: &Path) {
    let mut session = Session::open(db_path).unwrap();
    let mut engine = session.engine().clone();
    sync_markdown_root_with_options(
        &mut engine,
        root,
        &MarkdownSyncOptions {
            include_fs_graph: true,
        },
    )
    .unwrap();
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
