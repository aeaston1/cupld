mod support;

use std::collections::BTreeMap;

use cupld::{RuntimeValue, Session, Value};

use support::{TestDb, run, run_with_params, seed_person_graph, sorted_debug_rows, string_cell};

#[test]
fn file_backed_db_lifecycle_create_query_compact_and_spin_down() {
    let db = TestDb::new("lifecycle");
    let mut session = db.open();

    seed_person_graph(&mut session);

    let people = run(
        &mut session,
        "MATCH (n:Person)
         RETURN n.name, n.email, n.age
         ORDER BY n.name",
    );
    assert_eq!(
        people.rows,
        vec![
            vec![
                RuntimeValue::String("Ada".to_owned()),
                RuntimeValue::String("ada@example.com".to_owned()),
                RuntimeValue::Int(36),
            ],
            vec![
                RuntimeValue::String("Alan".to_owned()),
                RuntimeValue::String("alan@example.com".to_owned()),
                RuntimeValue::Int(41),
            ],
            vec![
                RuntimeValue::String("Bob".to_owned()),
                RuntimeValue::Null,
                RuntimeValue::Int(29),
            ],
            vec![
                RuntimeValue::String("Grace".to_owned()),
                RuntimeValue::String("grace@example.com".to_owned()),
                RuntimeValue::Int(37),
            ],
        ]
    );

    let direct_knows = run(
        &mut session,
        "MATCH (a:Person)-[:KNOWS]->(b:Person)
         RETURN a.name AS source, b.name AS target",
    );
    assert_eq!(
        sorted_debug_rows(&direct_knows),
        vec![
            r#"[String("Ada"), String("Grace")]"#.to_owned(),
            r#"[String("Grace"), String("Alan")]"#.to_owned(),
        ]
    );

    let filtered_people = run(
        &mut session,
        "MATCH (n:Person)
         WHERE n.name STARTS WITH 'A' OR n.age >= 37
         RETURN n.name, n.age",
    );
    assert_eq!(
        sorted_debug_rows(&filtered_people),
        vec![
            r#"[String("Ada"), Int(36)]"#.to_owned(),
            r#"[String("Alan"), Int(41)]"#.to_owned(),
            r#"[String("Grace"), Int(37)]"#.to_owned(),
        ]
    );

    let multi_hop = run(
        &mut session,
        "MATCH (a:Person)-[:KNOWS*1..3]->(b:Person)
         RETURN a.name, b.name",
    );
    assert_eq!(
        sorted_debug_rows(&multi_hop),
        vec![
            r#"[String("Ada"), String("Alan")]"#.to_owned(),
            r#"[String("Ada"), String("Grace")]"#.to_owned(),
            r#"[String("Grace"), String("Alan")]"#.to_owned(),
        ]
    );

    let set_role = run(
        &mut session,
        "MATCH (n:Person {name: 'Ada'})
         SET n.role = 'engineer'
         RETURN n.name, n.role",
    );
    assert_eq!(
        set_role.rows,
        vec![vec![
            RuntimeValue::String("Ada".to_owned()),
            RuntimeValue::String("engineer".to_owned()),
        ]]
    );

    let remove_role = run(
        &mut session,
        "MATCH (n:Person {name: 'Ada'})
         REMOVE n.role
         RETURN n.name",
    );
    assert_eq!(
        remove_role.rows,
        vec![vec![RuntimeValue::String("Ada".to_owned())]]
    );

    let show_schema = run(&mut session, "SHOW SCHEMA");
    assert_eq!(
        show_schema.columns,
        vec!["kind", "name", "description", "ddl"]
    );
    assert!(
        show_schema
            .rows
            .iter()
            .any(|row| { row[3] == RuntimeValue::String("CREATE LABEL Person".to_owned()) })
    );
    assert!(show_schema.rows.iter().any(|row| {
        row[3]
            == RuntimeValue::String(
                "CREATE INDEX idx_label_Person_email_eq ON :Person(email)".to_owned(),
            )
    }));
    assert!(show_schema.rows.iter().any(|row| {
        row[3]
            == RuntimeValue::String(
                "CREATE CONSTRAINT constraint_label_Person_email_unique ON :Person REQUIRE email UNIQUE"
                    .to_owned(),
            )
    }));

    let show_stats = run(&mut session, "SHOW STATS");
    assert_eq!(
        show_stats.columns,
        vec![
            "node_count",
            "edge_count",
            "label_count",
            "edge_type_count",
            "index_count",
            "constraint_count",
            "last_tx_id",
            "wal_bytes",
        ]
    );
    assert_eq!(show_stats.rows[0][0], RuntimeValue::Int(4));
    assert_eq!(show_stats.rows[0][1], RuntimeValue::Int(2));
    assert_eq!(show_stats.rows[0][2], RuntimeValue::Int(1));
    assert_eq!(show_stats.rows[0][3], RuntimeValue::Int(1));
    assert_eq!(show_stats.rows[0][4], RuntimeValue::Int(2));
    assert_eq!(show_stats.rows[0][5], RuntimeValue::Int(2));
    assert!(matches!(show_stats.rows[0][6], RuntimeValue::Int(value) if value > 0));
    assert_eq!(show_stats.rows[0][7], RuntimeValue::Int(0));

    let explain = run(
        &mut session,
        "EXPLAIN MATCH (n:Person) RETURN n.name LIMIT 5",
    );
    assert_eq!(
        explain.columns,
        vec!["id", "parent_id", "operator", "detail"]
    );
    assert!(explain.rows.iter().any(|row| {
        row[2] == RuntimeValue::String("Limit".to_owned())
            && row[3] == RuntimeValue::String("LIMIT".to_owned())
    }));

    drop(session);

    let mut reopened = db.open();
    let reopened_people = run(
        &mut reopened,
        "MATCH (n:Person)
         RETURN n.name, n.email, n.age
         ORDER BY n.name",
    );
    assert_eq!(reopened_people.rows, people.rows);
    drop(reopened);

    let report_before_compact = Session::check(db.path()).unwrap();
    assert!(report_before_compact.wal_records > 0);
    assert!(!report_before_compact.recovered_tail);

    let mut compacted = db.open();
    compacted.compact().unwrap();
    drop(compacted);

    let report_after_compact = Session::check(db.path()).unwrap();
    assert_eq!(report_after_compact.wal_records, 0);
    assert_eq!(
        report_after_compact.last_tx_id,
        report_before_compact.last_tx_id
    );
    assert!(!report_after_compact.recovered_tail);

    let mut reopened_after_compact = db.open();
    let compacted_people = run(
        &mut reopened_after_compact,
        "MATCH (n:Person)
         RETURN n.name, n.email, n.age
         ORDER BY n.name",
    );
    assert_eq!(compacted_people.rows, people.rows);
}

#[test]
fn file_backed_transaction_commit_persists_after_reopen() {
    let db = TestDb::new("commit");
    let mut session = db.open();
    seed_person_graph(&mut session);

    run(&mut session, "BEGIN");
    run(
        &mut session,
        "CREATE (n:Person {name: 'Edsger', email: 'edsger@example.com', age: 42})",
    );
    run(&mut session, "COMMIT");
    drop(session);

    let mut reopened = db.open();
    let result = run(
        &mut reopened,
        "MATCH (n:Person {name: 'Edsger'}) RETURN n.name, n.email, n.age",
    );
    assert_eq!(
        result.rows,
        vec![vec![
            RuntimeValue::String("Edsger".to_owned()),
            RuntimeValue::String("edsger@example.com".to_owned()),
            RuntimeValue::Int(42),
        ]]
    );
}

#[test]
fn query_ergonomics_support_literals_indexing_and_extended_membership() {
    let db = TestDb::new("ergonomics_literals");
    let mut session = db.open();

    let result = run(
        &mut session,
        "RETURN ['Ada', 'Grace'][1],
                {name: 'Ada'}['name'],
                'ace' IN 'grace',
                'name' IN {name: 'Ada'},
                [1, 2, 3] CONTAINS 2,
                {name: 'Ada'} CONTAINS 'name',
                bytes'abc',
                datetime'2024-01-02T03:04:05Z'",
    );

    assert_eq!(
        result.rows,
        vec![vec![
            RuntimeValue::String("Grace".to_owned()),
            RuntimeValue::String("Ada".to_owned()),
            RuntimeValue::Bool(true),
            RuntimeValue::Bool(true),
            RuntimeValue::Bool(true),
            RuntimeValue::Bool(true),
            RuntimeValue::Bytes(b"abc".to_vec()),
            result.rows[0][7].clone(),
        ]]
    );
    assert!(matches!(result.rows[0][7], RuntimeValue::Datetime(_)));
}

#[test]
fn query_ergonomics_support_edge_helpers_regex_and_alternation() {
    let db = TestDb::new("ergonomics_match");
    let mut session = db.open();
    seed_person_graph(&mut session);

    run(
        &mut session,
        "MATCH (ada:Person {name: 'Ada'})
         CREATE (ada)-[:MENTORS {since: datetime'2024-01-02T03:04:05Z'}]->
           (lin:Person {name: 'Lin', email: 'lin@example.com', age: 33, badge: bytes'gold'})",
    );

    let mentors = run(
        &mut session,
        "MATCH (a:Person)-[e:KNOWS|MENTORS]->(b:Person)
         WHERE has_label(a, 'Person')
           AND edge_type(e) =~ '^(KNOWS|MENTORS)$'
           AND b.name ENDS WITH 'n'
         RETURN a.name, edge_type(e), b.name
         ORDER BY b.name",
    );

    assert_eq!(
        mentors.rows,
        vec![
            vec![
                RuntimeValue::String("Grace".to_owned()),
                RuntimeValue::String("KNOWS".to_owned()),
                RuntimeValue::String("Alan".to_owned()),
            ],
            vec![
                RuntimeValue::String("Ada".to_owned()),
                RuntimeValue::String("MENTORS".to_owned()),
                RuntimeValue::String("Lin".to_owned()),
            ],
        ]
    );

    let typed_properties = run(
        &mut session,
        "MATCH (:Person)-[e:MENTORS]->(b:Person {name: 'Lin'})
         RETURN b.badge, e.since",
    );
    assert_eq!(
        typed_properties.rows[0][0],
        RuntimeValue::Bytes(b"gold".to_vec())
    );
    assert!(matches!(
        typed_properties.rows[0][1],
        RuntimeValue::Datetime(_)
    ));
}

#[test]
fn query_ergonomics_report_stable_errors() {
    let db = TestDb::new("ergonomics_errors");
    let mut session = db.open();

    let regex_error = session
        .execute_script("RETURN 'Ada' =~ '('", &BTreeMap::new())
        .unwrap_err();
    assert_eq!(regex_error.code(), "regex_compile_error");

    let index_error = session
        .execute_script("RETURN [1]['bad']", &BTreeMap::new())
        .unwrap_err();
    assert_eq!(index_error.code(), "index_type_error");
}

#[test]
fn schema_metadata_filters_and_planner_survive_reopen() {
    let db = TestDb::new("schema_wave3");
    let mut session = db.open();

    run(
        &mut session,
        "CREATE LABEL Service DESCRIPTION 'Long-running services'",
    );
    run(
        &mut session,
        "CREATE EDGE TYPE CALLS DESCRIPTION 'Service-to-service calls'",
    );
    run(&mut session, "CREATE INDEX ON :Service(name)");
    run(
        &mut session,
        "CREATE CONSTRAINT ON [:CALLS] REQUIRE latency TYPE int",
    );

    let schema = run(&mut session, "SHOW SCHEMA");
    let service_row = schema
        .rows
        .iter()
        .find(|row| row[1] == RuntimeValue::String("Service".to_owned()))
        .unwrap();
    assert_eq!(
        service_row[2],
        RuntimeValue::String("Long-running services".to_owned())
    );
    assert_eq!(
        service_row[3],
        RuntimeValue::String(
            "CREATE LABEL Service DESCRIPTION \"Long-running services\"".to_owned(),
        )
    );

    let filtered_indexes = run(&mut session, "SHOW INDEXES ON :Service");
    assert_eq!(filtered_indexes.rows.len(), 1);
    assert_eq!(string_cell(&filtered_indexes, 0, 2), "Service");
    assert_eq!(string_cell(&filtered_indexes, 0, 3), "name");

    let filtered_constraints = run(&mut session, "SHOW CONSTRAINTS ON [:CALLS]");
    assert_eq!(filtered_constraints.rows.len(), 1);
    assert_eq!(string_cell(&filtered_constraints, 0, 2), "CALLS");
    assert_eq!(string_cell(&filtered_constraints, 0, 3), "latency");

    drop(session);

    let mut reopened = db.open();
    let reopened_schema = run(&mut reopened, "SHOW SCHEMA");
    assert!(reopened_schema.rows.iter().any(|row| {
        row[1] == RuntimeValue::String("CALLS".to_owned())
            && row[2] == RuntimeValue::String("Service-to-service calls".to_owned())
    }));
}

#[test]
fn explain_uses_index_seek_and_range_scan() {
    let db = TestDb::new("planner_wave3");
    let mut session = db.open();
    seed_person_graph(&mut session);
    run(&mut session, "CREATE INDEX ON :Person(age)");

    let explain_seek = run(
        &mut session,
        "EXPLAIN MATCH (n:Person) WHERE n.email = 'ada@example.com' RETURN n.name",
    );
    assert!(explain_seek.rows.iter().any(|row| {
        row[2] == RuntimeValue::String("NodeIndexSeek".to_owned())
            && format!("{:?}", row[3]).contains(":Person(email)")
    }));

    let explain_range = run(
        &mut session,
        "EXPLAIN MATCH (n:Person) WHERE n.age >= 37 RETURN n.name",
    );
    assert!(explain_range.rows.iter().any(|row| {
        row[2] == RuntimeValue::String("NodeIndexRangeScan".to_owned())
            && format!("{:?}", row[3]).contains(":Person(age)")
    }));

    let seek_result = run(
        &mut session,
        "MATCH (n:Person) WHERE n.email = 'ada@example.com' RETURN n.name",
    );
    assert_eq!(
        seek_result.rows,
        vec![vec![RuntimeValue::String("Ada".to_owned())]]
    );

    let range_result = run(
        &mut session,
        "MATCH (n:Person) WHERE n.age >= 37 RETURN n.name ORDER BY n.name",
    );
    assert_eq!(
        range_result.rows,
        vec![
            vec![RuntimeValue::String("Alan".to_owned())],
            vec![RuntimeValue::String("Grace".to_owned())],
        ]
    );
}

#[test]
fn file_backed_transaction_rollback_discards_changes() {
    let db = TestDb::new("rollback");
    let mut session = db.open();
    seed_person_graph(&mut session);

    run(&mut session, "BEGIN");
    run(
        &mut session,
        "CREATE (n:Person {name: 'Barbara', email: 'barbara@example.com', age: 35})",
    );
    run(&mut session, "ROLLBACK");
    drop(session);

    let mut reopened = db.open();
    let result = run(
        &mut reopened,
        "MATCH (n:Person {name: 'Barbara'}) RETURN n.name",
    );
    assert!(result.rows.is_empty());
}

#[test]
fn file_backed_savepoints_restore_intermediate_state() {
    let db = TestDb::new("savepoint");
    let mut session = db.open();
    seed_person_graph(&mut session);

    run(&mut session, "BEGIN");
    run(
        &mut session,
        "MATCH (n:Person {name: 'Ada'})
         SET n.role = 'engineer'
         RETURN n.role",
    );
    run(&mut session, "SAVEPOINT before_remove");
    run(
        &mut session,
        "MATCH (n:Person {name: 'Ada'})
         REMOVE n.role
         RETURN n.name",
    );
    run(&mut session, "ROLLBACK TO SAVEPOINT before_remove");
    run(&mut session, "COMMIT");
    drop(session);

    let mut reopened = db.open();
    let result = run(
        &mut reopened,
        "MATCH (n:Person {name: 'Ada'}) RETURN n.role",
    );
    assert_eq!(
        result.rows,
        vec![vec![RuntimeValue::String("engineer".to_owned())]]
    );
}

#[test]
fn file_backed_constraint_violation_marks_transaction_failed() {
    let db = TestDb::new("failed_tx");
    let mut session = db.open();
    seed_person_graph(&mut session);

    run(&mut session, "BEGIN");
    let error = session
        .execute_script(
            "CREATE CONSTRAINT ON :Person REQUIRE name TYPE int",
            &BTreeMap::new(),
        )
        .unwrap_err();
    assert_eq!(error.code(), "constraint_type_violation");

    let transactions = run(&mut session, "SHOW TRANSACTIONS");
    assert_eq!(
        transactions.rows,
        vec![vec![
            RuntimeValue::Bool(true),
            RuntimeValue::Bool(true),
            RuntimeValue::Int(0),
            transactions.rows[0][3].clone(),
        ]]
    );
    assert!(matches!(transactions.rows[0][3], RuntimeValue::Int(_)));

    let blocked = session
        .execute_script("MATCH (n:Person) RETURN n.name", &BTreeMap::new())
        .unwrap_err();
    assert_eq!(blocked.code(), "transaction_failed");

    run(&mut session, "ROLLBACK");
    let recovered = run(
        &mut session,
        "MATCH (n:Person) RETURN n.name ORDER BY n.name",
    );
    assert_eq!(recovered.rows.len(), 4);
}

#[test]
fn file_backed_queries_accept_named_parameters() {
    let db = TestDb::new("params");
    let mut session = db.open();
    seed_person_graph(&mut session);

    let mut params = BTreeMap::new();
    params.insert("name".to_owned(), Value::from("Ada"));

    let result = run_with_params(
        &mut session,
        "MATCH (n:Person {name: $name}) RETURN n.name, n.age",
        &params,
    );
    assert_eq!(
        result.rows,
        vec![vec![
            RuntimeValue::String("Ada".to_owned()),
            RuntimeValue::Int(36),
        ]]
    );

    let mut reopened = db.open();
    let reopened_result = run_with_params(
        &mut reopened,
        "MATCH (n:Person {name: $name}) RETURN n.email",
        &params,
    );
    assert_eq!(string_cell(&reopened_result, 0, 0), "ada@example.com");
}
