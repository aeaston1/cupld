mod support;

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use cupld::{RuntimeValue, Session};

use support::run;

static NEXT_DIRECTORY: AtomicUsize = AtomicUsize::new(0);

#[test]
fn stale_sessions_and_clones_cannot_overwrite_a_newer_commit() {
    let directory = TestDirectory::new("stale_sessions");
    let path = directory.path().join("memory.cupld");
    let mut first = create_database(&path);
    let stale = Session::open(&path).unwrap();
    let original_tx = stale.transaction_info().last_tx_id;
    run(&mut first, "CREATE (:Note {name: 'newer'})");
    let newer_bytes = fs::read(&path).unwrap();

    for operation in ["write", "save", "compact", "save_as"] {
        // A cloned Session carries the original revision, not a refreshed one.
        let mut second = stale.clone();
        let error = match operation {
            "write" => second
                .execute_script("CREATE (:Note {name: 'stale'})", &Default::default())
                .unwrap_err(),
            "save" => second.save().unwrap_err(),
            "compact" => second.compact().unwrap_err(),
            "save_as" => second.save_as(&path).unwrap_err(),
            _ => unreachable!(),
        };
        assert_eq!(error.code(), "database_changed", "{operation}: {error}");
        assert_eq!(note_names(&mut second), vec!["base"], "{operation}");
        assert_eq!(second.transaction_info().last_tx_id, original_tx);
        assert!(!second.is_dirty());
        assert!(!second.transaction_info().active);
        assert_eq!(fs::read(&path).unwrap(), newer_bytes, "{operation}");
    }
    assert_eq!(
        note_names(&mut Session::open(&path).unwrap()),
        vec!["base", "newer"]
    );
}

#[test]
fn a_stale_save_preserves_unsaved_engine_replacement() {
    let directory = TestDirectory::new("stale_dirty_save");
    let path = directory.path().join("memory.cupld");
    let mut first = create_database(&path);
    let mut stale = Session::open(&path).unwrap();
    let mut local = Session::from_engine(stale.engine().clone());
    run(&mut local, "CREATE (:Note {name: 'local'})");
    stale.replace_engine(local.engine().clone()).unwrap();
    run(&mut first, "CREATE (:Note {name: 'newer'})");
    let newer_bytes = fs::read(&path).unwrap();
    let local_tx = stale.transaction_info().last_tx_id;

    assert_eq!(stale.save().unwrap_err().code(), "database_changed");
    assert!(stale.is_dirty());
    assert_eq!(stale.transaction_info().last_tx_id, local_tx);
    assert_eq!(note_names(&mut stale), vec!["base", "local"]);
    assert_eq!(fs::read(&path).unwrap(), newer_bytes);
}

#[test]
fn a_stale_transaction_commit_can_roll_back_without_publishing() {
    let directory = TestDirectory::new("stale_transaction");
    let path = directory.path().join("memory.cupld");
    let mut first = create_database(&path);
    let mut stale = Session::open(&path).unwrap();
    run(&mut stale, "BEGIN");
    run(&mut stale, "CREATE (:Note {name: 'pending'})");
    run(&mut stale, "SAVEPOINT pending");
    let pending_tx = stale.transaction_info().last_tx_id;
    run(&mut first, "CREATE (:Note {name: 'newer'})");
    let newer_bytes = fs::read(&path).unwrap();

    let error = stale
        .execute_script("COMMIT", &Default::default())
        .unwrap_err();
    assert_eq!(error.code(), "database_changed");
    assert!(stale.transaction_info().active);
    assert_eq!(stale.transaction_info().last_tx_id, pending_tx);
    assert_eq!(stale.transaction_info().savepoints, 1);
    assert!(stale.is_dirty());
    assert_eq!(note_names(&mut stale), vec!["base", "pending"]);
    assert_eq!(fs::read(&path).unwrap(), newer_bytes);

    run(&mut stale, "ROLLBACK");
    assert!(!stale.transaction_info().active);
    assert!(!stale.is_dirty());
    assert_eq!(note_names(&mut stale), vec!["base"]);
    assert_eq!(fs::read(&path).unwrap(), newer_bytes);
}

#[test]
fn active_transactions_cannot_be_published_by_save_or_compaction() {
    let directory = TestDirectory::new("transaction_save");
    let path = directory.path().join("memory.cupld");
    let destination = directory.path().join("copy.cupld");
    let mut session = create_database(&path);
    let original = fs::read(&path).unwrap();
    run(&mut session, "BEGIN");
    run(&mut session, "CREATE (:Note {name: 'uncommitted'})");
    let pending_tx = session.transaction_info().last_tx_id;

    for operation in ["save", "compact", "save_as", "save_as_same"] {
        let error = match operation {
            "save" => session.save().unwrap_err(),
            "compact" => session.compact().unwrap_err(),
            "save_as" => session.save_as(&destination).unwrap_err(),
            "save_as_same" => session.save_as(&path).unwrap_err(),
            _ => unreachable!(),
        };
        assert_eq!(error.code(), "transaction_active", "{operation}: {error}");
        assert!(session.transaction_info().active);
        assert_eq!(session.transaction_info().last_tx_id, pending_tx);
        assert!(session.is_dirty());
        assert_eq!(note_names(&mut session), vec!["base", "uncommitted"]);
        assert_eq!(fs::read(&path).unwrap(), original, "{operation}");
        assert!(!destination.exists(), "{operation}");
    }
    run(&mut session, "ROLLBACK");
    assert_eq!(note_names(&mut session), vec!["base"]);
    assert_eq!(fs::read(&path).unwrap(), original);
}

#[test]
fn same_target_save_as_preserves_identity_and_refreshes_the_revision() {
    let directory = TestDirectory::new("same_target_save_as");
    let path = directory.path().join("memory.cupld");
    let alias = directory.path().join(".").join("memory.cupld");
    let mut session = create_database(&path);
    run(&mut session, "CREATE (:Note {name: 'before_compaction'})");
    let mut stale = Session::open(&path).unwrap();
    let before = Session::check(&path).unwrap();
    assert!(before.wal_records > 0);

    session.save_as(&alias).unwrap();
    let after = Session::check(&path).unwrap();
    assert_eq!(after.db_uuid, before.db_uuid);
    assert_eq!(after.last_tx_id, before.last_tx_id);
    assert_eq!(after.wal_records, 0);
    let compacted = fs::read(&path).unwrap();
    assert_eq!(
        stale.save_as(&alias).unwrap_err().code(),
        "database_changed"
    );
    assert_eq!(fs::read(&path).unwrap(), compacted);

    // The saving session must retain its new revision, so its next write works.
    run(&mut session, "CREATE (:Note {name: 'after_compaction'})");
    assert_eq!(Session::check(&path).unwrap().db_uuid, before.db_uuid);
    assert_eq!(
        note_names(&mut Session::open(&path).unwrap()),
        vec!["after_compaction", "base", "before_compaction"]
    );
}

#[test]
fn save_as_does_not_replace_another_database_or_rebind_the_session() {
    let directory = TestDirectory::new("existing_save_as");
    let source = directory.path().join("source.cupld");
    let destination = directory.path().join("destination.cupld");
    let mut session = create_database(&source);
    let mut other = create_database(&destination);
    run(&mut other, "CREATE (:Note {name: 'destination'})");
    let other_bytes = fs::read(&destination).unwrap();
    let original_path = session.path().unwrap().to_path_buf();

    assert_eq!(
        session.save_as(&destination).unwrap_err().code(),
        "database_exists"
    );
    assert_eq!(session.path(), Some(original_path.as_path()));
    assert_eq!(fs::read(&destination).unwrap(), other_bytes);
    run(&mut session, "CREATE (:Note {name: 'source'})");
    assert_eq!(fs::read(&destination).unwrap(), other_bytes);
    assert_eq!(
        note_names(&mut Session::open(&source).unwrap()),
        vec!["base", "source"]
    );
}

#[test]
fn current_format_readers_and_mcp_diagnostics_create_no_sidecars() {
    let directory = TestDirectory::new("readers_without_sidecars");
    let source = directory.path().join("source.cupld");
    create_database(&source);
    let read_directory = directory.path().join("read_only");
    fs::create_dir(&read_directory).unwrap();
    let copy = read_directory.join("memory.cupld");
    fs::copy(&source, &copy).unwrap();
    let bytes = fs::read(&copy).unwrap();
    let entries = directory_entries(&read_directory);

    let mut session = Session::open(&copy).unwrap();
    assert_eq!(note_names(&mut session), vec!["base"]);
    Session::check(&copy).unwrap();
    let config = cupld::mcp::McpConfig {
        db_path: copy.clone(),
        root_override: Some(read_directory.clone()),
        read_only: true,
    };
    for tool in ["memory_health", "memory_doctor"] {
        let response = cupld::mcp::handle_json_line(&config, &format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{{"name":"{tool}","arguments":{{"deep":true}}}}}}"#,
        )).unwrap();
        let response = cupld::json::parse(&response).unwrap();
        let result = response.get("result").expect("diagnostic tool response");
        assert_ne!(
            result
                .get("isError")
                .and_then(cupld::json::JsonValue::as_bool),
            Some(true)
        );
    }
    assert_eq!(fs::read(&copy).unwrap(), bytes);
    assert_eq!(directory_entries(&read_directory), entries);
}

#[cfg(unix)]
#[test]
fn sessions_opened_through_symlinks_keep_the_original_target_after_retargeting() {
    use std::os::unix::fs::symlink;

    let directory = TestDirectory::new("retargeted_symlink");
    let original = directory.path().join("original.cupld");
    let replacement = directory.path().join("replacement.cupld");
    let alias = directory.path().join("alias.cupld");
    create_database(&original);
    // Identical bytes/UUID ensure that revision comparison alone cannot detect
    // retargeting; the Session must bind the canonical path when opened.
    fs::copy(&original, &replacement).unwrap();
    let replacement_bytes = fs::read(&replacement).unwrap();
    symlink(&original, &alias).unwrap();
    let mut session = Session::open(&alias).unwrap();
    fs::remove_file(&alias).unwrap();
    symlink(&replacement, &alias).unwrap();

    run(&mut session, "CREATE (:Note {name: 'through_alias'})");
    assert_eq!(
        session.path(),
        Some(fs::canonicalize(&original).unwrap().as_path())
    );
    assert_eq!(fs::read(&replacement).unwrap(), replacement_bytes);
    assert_eq!(fs::read_link(&alias).unwrap(), replacement);
    assert_eq!(
        note_names(&mut Session::open(&original).unwrap()),
        vec!["base", "through_alias"]
    );
}

#[cfg(unix)]
#[test]
fn hard_linked_databases_reject_writes_without_replacing_either_link() {
    use std::os::unix::fs::MetadataExt;

    let directory = TestDirectory::new("hard_linked_database");
    let original = directory.path().join("original.cupld");
    let alias = directory.path().join("alias.cupld");
    create_database(&original);
    fs::hard_link(&original, &alias).unwrap();
    let original_bytes = fs::read(&original).unwrap();
    let metadata = fs::metadata(&original).unwrap();
    let identity = (metadata.dev(), metadata.ino());

    for path in [&original, &alias] {
        let mut session = Session::open(path).unwrap();
        let original_tx = session.transaction_info().last_tx_id;
        let error = session
            .execute_script("CREATE (:Note {name: 'rejected'})", &Default::default())
            .unwrap_err();
        assert_eq!(error.code(), "io_error");
        assert!(error.message().contains("multiple hard links"));
        assert_eq!(session.save().unwrap_err().code(), "io_error");
        assert_eq!(note_names(&mut session), vec!["base"]);
        assert_eq!(session.transaction_info().last_tx_id, original_tx);
        assert!(!session.is_dirty());

        for preserved in [&original, &alias] {
            assert_eq!(fs::read(preserved).unwrap(), original_bytes);
            let metadata = fs::metadata(preserved).unwrap();
            assert_eq!((metadata.dev(), metadata.ino()), identity);
            assert_eq!(metadata.nlink(), 2);
        }
    }
}

fn create_database(path: &Path) -> Session {
    let mut session = Session::new_in_memory();
    run(&mut session, "CREATE (:Note {name: 'base'})");
    session.save_as(path).unwrap();
    session
}

fn note_names(session: &mut Session) -> Vec<String> {
    run(session, "MATCH (n:Note) RETURN n.name ORDER BY n.name")
        .rows
        .into_iter()
        .map(|row| match row.into_iter().next().unwrap() {
            RuntimeValue::String(name) => name,
            other => panic!("expected note name, got {other:?}"),
        })
        .collect()
}

fn directory_entries(path: &Path) -> BTreeSet<std::ffi::OsString> {
    fs::read_dir(path)
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect()
}

struct TestDirectory(PathBuf);

impl TestDirectory {
    fn new(name: &str) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let suffix = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "cupld_writer_{name}_{}_{timestamp}_{suffix}",
            std::process::id()
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
