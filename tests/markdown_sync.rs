mod support;

use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

use cupld::{
    MarkdownWatchOptions, RuntimeValue, Session, configured_markdown_root, set_markdown_root,
    sync_markdown_root, watch_markdown_root,
};

use support::{TestDb, run};

static NEXT_TEMP_DIR_ID: AtomicUsize = AtomicUsize::new(1);

#[test]
fn synced_markdown_persists_in_cupld_file() {
    let db = TestDb::new("markdown_persist");
    let root = temp_dir("markdown_persist");
    fs::create_dir_all(&root).unwrap();
    fs::write(
        root.join("note.md"),
        "---\n\
title: Synced Title\n\
tags: [rust]\n\
---\n\
Body\n",
    )
    .unwrap();
    fs::write(root.join("plain.md"), "# Plain Title\nPlain body").unwrap();

    sync_root_into_db(db.path(), &root);

    let mut reopened = db.open();
    let result = run(
        &mut reopened,
        "MATCH (d:MarkdownDocument)
         RETURN d.`src.path`, d.`md.title`, d.`md.has_frontmatter`, d.`src.status`
         ORDER BY d.`src.path`",
    );
    assert_eq!(
        result.rows,
        vec![
            vec![
                RuntimeValue::String("note.md".to_owned()),
                RuntimeValue::String("Synced Title".to_owned()),
                RuntimeValue::Bool(true),
                RuntimeValue::String("current".to_owned()),
            ],
            vec![
                RuntimeValue::String("plain.md".to_owned()),
                RuntimeValue::String("Plain Title".to_owned()),
                RuntimeValue::Bool(false),
                RuntimeValue::String("current".to_owned()),
            ],
        ]
    );

    fs::remove_dir_all(root).unwrap();
}

#[test]
fn tombstones_missing_documents_without_breaking_native_edges() {
    let db = TestDb::new("markdown_tombstone");
    let root = temp_dir("markdown_tombstone");
    fs::create_dir_all(&root).unwrap();
    fs::write(root.join("note.md"), "# Note").unwrap();

    sync_root_into_db(db.path(), &root);

    let mut session = db.open();
    run(
        &mut session,
        "MATCH (d:MarkdownDocument {`src.path`: 'note.md'})
         CREATE (:Person {name: 'Ada'})-[:REFERS_TO]->(d)",
    );
    drop(session);

    fs::remove_file(root.join("note.md")).unwrap();
    sync_root_into_db(db.path(), &root);

    let mut reopened = db.open();
    let result = run(
        &mut reopened,
        "MATCH (p:Person)-[:REFERS_TO]->(d:MarkdownDocument {`src.path`: 'note.md'})
         RETURN p.name, d.`src.status`",
    );
    assert_eq!(
        result.rows,
        vec![vec![
            RuntimeValue::String("Ada".to_owned()),
            RuntimeValue::String("missing".to_owned()),
        ]]
    );

    fs::remove_dir_all(root).unwrap();
}

#[test]
fn configured_root_survives_save_open_and_compact() {
    let db = TestDb::new("markdown_config");
    let root = temp_dir("markdown_config");
    fs::create_dir_all(&root).unwrap();
    let expected_root = root.canonicalize().unwrap();

    let mut session = db.open();
    let mut engine = session.engine().clone();
    set_markdown_root(&mut engine, &root).unwrap();
    engine.commit().unwrap();
    session.replace_engine(engine).unwrap();
    session.save().unwrap();
    drop(session);

    let mut reopened = db.open();
    assert_eq!(
        configured_markdown_root(reopened.engine()),
        Some(expected_root.clone())
    );
    reopened.compact().unwrap();
    drop(reopened);

    let reopened = Session::open(db.path()).unwrap();
    assert_eq!(
        configured_markdown_root(reopened.engine()),
        Some(expected_root)
    );

    fs::remove_dir_all(root).unwrap();
}

#[test]
fn watch_mode_coalesces_duplicate_events_and_partial_writes() {
    let db = TestDb::new("markdown_watch_partial");
    let root = temp_dir("markdown_watch_partial");
    fs::create_dir_all(&root).unwrap();
    fs::write(root.join("note.md"), "# Start\nBody").unwrap();

    let writer_root = root.clone();
    let writer = thread::spawn(move || {
        thread::sleep(Duration::from_millis(40));
        fs::write(writer_root.join("note.md"), "# Start\nBody v2").unwrap();
        thread::sleep(Duration::from_millis(15));
        fs::write(writer_root.join("note.md"), "# Start\nBody final").unwrap();
    });

    let report = watch_root_into_db(
        db.path(),
        &root,
        MarkdownWatchOptions {
            poll_interval: Duration::from_millis(10),
            debounce: Duration::from_millis(40),
            max_batch_window: Duration::from_millis(150),
            idle_timeout: Some(Duration::from_millis(500)),
            max_runs: Some(2),
        },
    );
    writer.join().unwrap();

    assert_eq!(report.sync_runs, 2);
    assert!(report.events_seen >= 1);

    let mut reopened = db.open();
    let result = run(
        &mut reopened,
        "MATCH (d:MarkdownDocument {`src.path`: 'note.md'}) RETURN d.`md.body`",
    );
    assert_eq!(
        result.rows,
        vec![vec![RuntimeValue::String("# Start\nBody final".to_owned())]]
    );

    fs::remove_dir_all(root).unwrap();
}

#[test]
fn watch_mode_handles_rename_save_restart_and_malformed_frontmatter() {
    let db = TestDb::new("markdown_watch_restart");
    let root = temp_dir("markdown_watch_restart");
    fs::create_dir_all(&root).unwrap();
    fs::write(root.join("note.md"), "# One").unwrap();
    sync_root_into_db(db.path(), &root);

    let temp_path = root.join("note.tmp.md");
    let final_path = root.join("note.md");
    let writer = thread::spawn({
        let temp_path = temp_path.clone();
        let final_path = final_path.clone();
        move || {
            thread::sleep(Duration::from_millis(40));
            fs::write(
                &temp_path,
                "---\nfoo: [unterminated\n# Recovered\nBody after restart",
            )
            .unwrap();
            fs::rename(&temp_path, &final_path).unwrap();
        }
    });

    let report = watch_root_into_db(
        db.path(),
        &root,
        MarkdownWatchOptions {
            poll_interval: Duration::from_millis(10),
            debounce: Duration::from_millis(40),
            max_batch_window: Duration::from_millis(150),
            idle_timeout: Some(Duration::from_millis(500)),
            max_runs: Some(2),
        },
    );
    writer.join().unwrap();

    assert_eq!(report.sync_runs, 2);

    let mut reopened = db.open();
    let result = run(
        &mut reopened,
        "MATCH (d:MarkdownDocument {`src.path`: 'note.md'}) RETURN d.`md.title`, d.`md.has_frontmatter`",
    );
    assert_eq!(
        result.rows,
        vec![vec![
            RuntimeValue::String("Recovered".to_owned()),
            RuntimeValue::Bool(false),
        ]]
    );

    fs::remove_dir_all(root).unwrap();
}

fn sync_root_into_db(db_path: &std::path::Path, root: &std::path::Path) {
    let mut session = Session::open(db_path).unwrap();
    let mut engine = session.engine().clone();
    let report = sync_markdown_root(&mut engine, root).unwrap();
    assert!(report.upserted_documents > 0 || report.tombstoned_documents > 0);
    engine.commit().unwrap();
    session.replace_engine(engine).unwrap();
    session.save().unwrap();
}

fn watch_root_into_db(
    db_path: &std::path::Path,
    root: &std::path::Path,
    options: MarkdownWatchOptions,
) -> cupld::MarkdownWatchReport {
    let mut session = Session::open(db_path).unwrap();
    let mut engine = session.engine().clone();
    let report = watch_markdown_root(&mut engine, root, &options).unwrap();
    engine.commit().unwrap();
    session.replace_engine(engine).unwrap();
    session.save().unwrap();
    report
}

fn temp_dir(prefix: &str) -> PathBuf {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let suffix = NEXT_TEMP_DIR_ID.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "cupld_markdown_{prefix}_{}_{}_{}",
        std::process::id(),
        timestamp,
        suffix
    ))
}
