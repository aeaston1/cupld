mod support;

use std::collections::BTreeMap;
use std::fs;

use cupld::{RuntimeValue, Session};

use support::{TestDb, run};

#[test]
fn reads_preserve_graph_dirty_state_and_persistence() {
    let db = TestDb::new("read_state");
    let mut persisted = db.open();
    run(
        &mut persisted,
        "CREATE (a:Person {name: 'Ada'})-[:KNOWS]->(b:Person {name: 'Grace'})",
    );
    let bytes_before = fs::read(db.path()).unwrap();
    let mut unsaved = Session::new_in_memory();
    unsaved.replace_engine(persisted.engine().clone()).unwrap();

    for session in [&mut persisted, &mut unsaved] {
        let was_dirty = session.is_dirty();
        let stats = session.engine().stats();
        let nodes = session.engine().nodes().cloned().collect::<Vec<_>>();
        let edges = session.engine().edges().cloned().collect::<Vec<_>>();
        let snapshot = session.engine().snapshot();

        let result = run(
            session,
            "MATCH (a:Person)-[:KNOWS]->(b:Person)
             WHERE a.name = 'Ada'
             WITH b.name AS name ORDER BY name DESC LIMIT 1
             RETURN name",
        );
        assert_eq!(
            result.rows,
            vec![vec![RuntimeValue::String("Grace".to_owned())]]
        );
        assert_eq!(run(session, "MATCH (n) RETURN *").rows.len(), 2);
        assert_eq!(
            run(session, "MATCH (n) RETURN count(n)").rows,
            vec![vec![RuntimeValue::Int(2)]]
        );
        assert!(run(session, "MATCH (n:Absent) RETURN n").rows.is_empty());

        for (query, code) in [
            ("MATCH (n) WHERE 42 RETURN n", "where_type_error"),
            (
                "MATCH (n) WITH missing AS name RETURN name",
                "unknown_variable",
            ),
            ("MATCH (n) RETURN 1 / 0", "division_by_zero"),
        ] {
            let error = session.execute_script(query, &BTreeMap::new()).unwrap_err();
            assert_eq!(error.code(), code);
            assert!(!session.transaction_info().failed);
            assert_eq!(run(session, "MATCH (n) RETURN n").rows.len(), 2);
        }

        assert_eq!(session.is_dirty(), was_dirty);
        assert_eq!(session.engine().stats(), stats);
        assert_eq!(session.engine().nodes().cloned().collect::<Vec<_>>(), nodes);
        assert_eq!(session.engine().edges().cloned().collect::<Vec<_>>(), edges);
        assert_eq!(snapshot.nodes().cloned().collect::<Vec<_>>(), nodes);
        assert_eq!(snapshot.edges().cloned().collect::<Vec<_>>(), edges);
    }
    assert!(!persisted.is_dirty());
    assert!(unsaved.is_dirty());
    assert_eq!(fs::read(db.path()).unwrap(), bytes_before);
}

#[test]
fn failed_transactional_read_preserves_pending_writes_and_savepoint_recovery() {
    let db = TestDb::new("read_failure_savepoint");
    let mut session = db.open();
    run(&mut session, "CREATE (n:Person {name: 'Ada'})");
    let snapshot = session.engine().snapshot();
    let bytes_before = fs::read(db.path()).unwrap();

    run(&mut session, "BEGIN");
    run(&mut session, "CREATE (n:Person {name: 'Grace'})");
    run(&mut session, "SAVEPOINT after_create");
    run(
        &mut session,
        "MATCH (n:Person {name: 'Ada'}) SET n.name = 'Changed'",
    );
    let pending = session.engine().nodes().cloned().collect::<Vec<_>>();
    let stats = session.engine().stats();
    assert_eq!(run(&mut session, "MATCH (n) RETURN n").rows.len(), 2);

    let error = session
        .execute_script("MATCH (n) RETURN 1 / 0", &BTreeMap::new())
        .unwrap_err();
    assert_eq!(error.code(), "division_by_zero");
    assert_eq!(
        session.engine().nodes().cloned().collect::<Vec<_>>(),
        pending
    );
    assert_eq!(session.engine().stats(), stats);
    assert!(session.is_dirty());
    let info = session.transaction_info();
    assert!(info.active);
    assert!(info.failed);
    assert_eq!(info.savepoints, 1);
    assert_eq!(info.last_tx_id, snapshot.tx_id().get());
    assert_eq!(fs::read(db.path()).unwrap(), bytes_before);

    for query in ["MATCH (n) RETURN n", "CREATE (n)", "COMMIT"] {
        assert_eq!(
            session
                .execute_script(query, &BTreeMap::new())
                .unwrap_err()
                .code(),
            "transaction_failed"
        );
    }
    run(&mut session, "ROLLBACK TO SAVEPOINT after_create");
    assert!(!session.transaction_info().failed);
    assert!(session.is_dirty());
    assert_eq!(
        run(&mut session, "MATCH (n) RETURN n.name ORDER BY n.name").rows,
        vec![
            vec![RuntimeValue::String("Ada".to_owned())],
            vec![RuntimeValue::String("Grace".to_owned())],
        ]
    );
    run(&mut session, "COMMIT");
    assert!(!session.is_dirty());
    assert!(!session.transaction_info().active);
    assert_eq!(
        session.transaction_info().last_tx_id,
        snapshot.tx_id().get() + 1
    );
    assert_eq!(snapshot.nodes().count(), 1);
    assert_eq!(run(&mut db.open(), "MATCH (n) RETURN n").rows.len(), 2);
}

#[test]
fn failed_read_only_transaction_rolls_back_to_clean_state() {
    let mut session = Session::new_in_memory();
    run(&mut session, "BEGIN");
    run(&mut session, "RETURN 42");
    assert!(!session.is_dirty());
    assert!(!session.transaction_info().failed);
    let error = session
        .execute_script("RETURN missing", &BTreeMap::new())
        .unwrap_err();
    assert_eq!(error.code(), "unknown_variable");
    assert!(session.transaction_info().failed);
    assert!(!session.is_dirty());
    run(&mut session, "ROLLBACK");
    assert!(!session.transaction_info().active);
    assert!(!session.transaction_info().failed);
    assert!(!session.is_dirty());
    assert_eq!(session.transaction_info().last_tx_id, 0);
    assert_eq!(
        run(&mut session, "RETURN 42").rows,
        vec![vec![RuntimeValue::Int(42)]]
    );
}

#[test]
fn mutation_clauses_still_apply_and_restore_after_late_evaluation_errors() {
    for transactional in [false, true] {
        for (query, expected_names) in [
            (
                "CREATE (n:Person {name: 'Grace'})",
                vec![Some("Ada"), Some("Grace")],
            ),
            (
                "MERGE (n:Person {name: 'Grace'})",
                vec![Some("Ada"), Some("Grace")],
            ),
            ("MATCH (n:Person) SET n.name = 'Grace'", vec![Some("Grace")]),
            ("MATCH (n:Person) REMOVE n.name", vec![None]),
            ("MATCH (n:Person) DELETE n", vec![]),
        ] {
            let mut session = Session::new_in_memory();
            run(&mut session, "CREATE (n:Person {name: 'Ada'})");
            let before = session.engine().nodes().cloned().collect::<Vec<_>>();
            let stats = session.engine().stats();
            if transactional {
                run(&mut session, "BEGIN");
            }
            let error = session
                .execute_script(&format!("{query} RETURN 1 / 0"), &BTreeMap::new())
                .unwrap_err();
            assert_eq!(error.code(), "division_by_zero", "{query}");
            assert_eq!(
                session.engine().nodes().cloned().collect::<Vec<_>>(),
                before
            );
            assert_eq!(session.engine().stats(), stats);
            assert_eq!(session.transaction_info().failed, transactional);
            assert!(session.is_dirty());
            if transactional {
                run(&mut session, "ROLLBACK");
                run(&mut session, "BEGIN");
            }

            run(&mut session, query);
            if transactional {
                run(&mut session, "COMMIT");
            }
            let expected = expected_names
                .into_iter()
                .map(|name| {
                    vec![
                        name.map(|name| RuntimeValue::String(name.to_owned()))
                            .unwrap_or(RuntimeValue::Null),
                    ]
                })
                .collect::<Vec<_>>();
            assert_eq!(
                run(&mut session, "MATCH (n) RETURN n.name ORDER BY n.name").rows,
                expected,
                "{query}"
            );
            assert_eq!(session.transaction_info().last_tx_id, stats.last_tx_id + 1);
        }
    }
}
