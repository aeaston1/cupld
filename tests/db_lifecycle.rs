mod support;

use std::collections::BTreeMap;
use std::time::Instant;

use cupld::{PropertyMap, RuntimeValue, Session, Value};

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
    run(&mut session, "CREATE INDEX ON :Person(age) KIND RANGE");

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
fn with_and_aggregates_share_the_projection_pipeline() {
    let db = TestDb::new("pipeline_wave4");
    let mut session = db.open();
    seed_person_graph(&mut session);

    let grouped = run(
        &mut session,
        "MATCH (n:Person)
         WITH n.age >= 37 AS senior, count(*) AS total, collect(n.name) AS names
         RETURN senior, total, names
         ORDER BY senior",
    );

    assert_eq!(grouped.columns, vec!["col_1", "col_2", "col_3"]);
    assert_eq!(
        grouped.rows,
        vec![
            vec![
                RuntimeValue::Bool(false),
                RuntimeValue::Int(2),
                RuntimeValue::List(vec![
                    RuntimeValue::String("Ada".to_owned()),
                    RuntimeValue::String("Bob".to_owned()),
                ]),
            ],
            vec![
                RuntimeValue::Bool(true),
                RuntimeValue::Int(2),
                RuntimeValue::List(vec![
                    RuntimeValue::String("Grace".to_owned()),
                    RuntimeValue::String("Alan".to_owned()),
                ]),
            ],
        ]
    );
}

#[test]
fn merge_path_results_and_return_star_use_the_staged_executor() {
    let db = TestDb::new("merge_wave4");
    let mut session = db.open();
    seed_person_graph(&mut session);

    let merged = run(
        &mut session,
        "MATCH (a:Person {name: 'Ada'})
         MERGE p = (a)-[:MENTORS]->(m:Person {name: 'Lin', email: 'lin@example.com', age: 33})
         RETURN *",
    );

    assert_eq!(merged.columns, vec!["a", "m", "p"]);
    assert!(matches!(merged.rows[0][0], RuntimeValue::Node(_)));
    assert!(matches!(merged.rows[0][1], RuntimeValue::Node(_)));
    match &merged.rows[0][2] {
        RuntimeValue::Map(entries) => {
            assert_eq!(entries.len(), 2);
            assert!(matches!(&entries[0].1, RuntimeValue::List(nodes) if nodes.len() == 2));
            assert!(matches!(&entries[1].1, RuntimeValue::List(edges) if edges.len() == 1));
        }
        other => panic!("expected path map, got {other:?}"),
    }

    run(
        &mut session,
        "MATCH (a:Person {name: 'Ada'})
         MERGE p = (a)-[:MENTORS]->(m:Person {name: 'Lin', email: 'lin@example.com', age: 33})
         RETURN p",
    );

    let mentors = run(
        &mut session,
        "MATCH (a:Person)-[:MENTORS]->(m:Person {name: 'Lin'})
         RETURN count(*)",
    );
    assert_eq!(mentors.rows, vec![vec![RuntimeValue::Int(1)]]);
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

#[test]
fn wave5_update_semantics_cover_list_patches_merges_and_label_removal() {
    let db = TestDb::new("wave5_updates");
    let mut session = db.open();

    run(
        &mut session,
        "CREATE (:Person {name: 'Ada', tags: ['rust', 'cli'], old_field: 'legacy'})",
    );

    let result = run(
        &mut session,
        "MATCH (n:Person {name: 'Ada'})
         SET n.tags = insert(n.tags, 1, 'graph'),
             n.tags[0] = 'systems',
             n += {role: 'engineer'}
         REMOVE n:Person, n.old_field
         RETURN n.tags, n.role, has_label(n, 'Person'), has_prop(n, 'old_field')",
    );

    assert_eq!(
        result.rows,
        vec![vec![
            RuntimeValue::List(vec![
                RuntimeValue::String("systems".to_owned()),
                RuntimeValue::String("graph".to_owned()),
                RuntimeValue::String("cli".to_owned()),
            ]),
            RuntimeValue::String("engineer".to_owned()),
            RuntimeValue::Bool(false),
            RuntimeValue::Bool(false),
        ]]
    );
}

#[test]
fn wave5_schema_evolution_and_parameterized_ddl_work_end_to_end() {
    let db = TestDb::new("wave5_schema");
    let mut session = db.open();
    let mut params = BTreeMap::new();
    params.insert("label".to_owned(), Value::from("Person"));
    params.insert("edge".to_owned(), Value::from("KNOWS"));
    params.insert("property".to_owned(), Value::from("email"));
    params.insert("description".to_owned(), Value::from("People"));
    params.insert("constraint".to_owned(), Value::from("person_email_required"));
    params.insert("renamed".to_owned(), Value::from("person_email_presence"));

    run_with_params(
        &mut session,
        "CREATE OR REPLACE LABEL $label DESCRIPTION $description",
        &params,
    );
    run_with_params(
        &mut session,
        "CREATE OR REPLACE EDGE TYPE $edge",
        &params,
    );
    run_with_params(
        &mut session,
        "CREATE INDEX idx_person_lookup ON :$label($property)",
        &params,
    );

    params.insert("property".to_owned(), Value::from("age"));
    run_with_params(
        &mut session,
        "CREATE OR REPLACE INDEX idx_person_lookup ON :$label($property)",
        &params,
    );
    run(
        &mut session,
        "ALTER INDEX idx_person_lookup SET STATUS INVALID",
    );

    params.insert("property".to_owned(), Value::from("email"));
    run_with_params(
        &mut session,
        "CREATE CONSTRAINT $constraint ON :$label REQUIRE $property REQUIRED",
        &params,
    );
    run_with_params(
        &mut session,
        "ALTER CONSTRAINT $constraint RENAME TO $renamed",
        &params,
    );

    let schema = run(&mut session, "SHOW SCHEMA");
    assert!(schema.rows.iter().any(|row| {
        row[0] == RuntimeValue::String("label".to_owned())
            && row[1] == RuntimeValue::String("Person".to_owned())
            && row[2] == RuntimeValue::String("People".to_owned())
    }));

    let indexes = run(&mut session, "SHOW INDEXES ON :Person");
    assert!(indexes.rows.iter().any(|row| {
        row[0] == RuntimeValue::String("idx_person_lookup".to_owned())
            && row[3] == RuntimeValue::String("age".to_owned())
            && row[5] == RuntimeValue::String("invalid".to_owned())
    }));

    let constraints = run(&mut session, "SHOW CONSTRAINTS ON :Person");
    assert!(constraints.rows.iter().any(|row| {
        row[0] == RuntimeValue::String("person_email_presence".to_owned())
            && row[3] == RuntimeValue::String("email".to_owned())
    }));
}

#[test]
fn wave5_edge_endpoint_and_cardinality_constraints_validate_existing_data() {
    let db = TestDb::new("wave5_edge_constraints");
    let mut session = db.open();

    run(
        &mut session,
        "CREATE (:Service {name: 'api'})-[:KNOWS]->(:Person {name: 'Ada'})",
    );
    let endpoint_error = session
        .execute_script(
            "CREATE CONSTRAINT ON [:KNOWS] REQUIRE ENDPOINTS :Person -> :Person",
            &BTreeMap::new(),
        )
        .unwrap_err();
    assert_eq!(endpoint_error.code(), "constraint_endpoint_violation");

    let mut engine = session.engine().clone();
    let grace = engine
        .create_node(["Person"], PropertyMap::from_pairs([("name", Value::from("Grace"))]))
        .unwrap();
    let lin = engine
        .create_node(["Person"], PropertyMap::from_pairs([("name", Value::from("Lin"))]))
        .unwrap();
    let barbara = engine
        .create_node(
            ["Person"],
            PropertyMap::from_pairs([("name", Value::from("Barbara"))]),
        )
        .unwrap();
    engine
        .create_edge(grace, lin, "MENTORS", PropertyMap::new())
        .unwrap();
    engine
        .create_edge(grace, barbara, "MENTORS", PropertyMap::new())
        .unwrap();
    session.replace_engine(engine).unwrap();
    let cardinality_error = session
        .execute_script(
            "CREATE CONSTRAINT ON [:MENTORS] REQUIRE MAX OUTGOING 1",
            &BTreeMap::new(),
        )
        .unwrap_err();
    assert_eq!(cardinality_error.code(), "constraint_cardinality_violation");
}

#[test]
fn wave6_index_kinds_and_temporal_fields_survive_reopen() {
    let db = TestDb::new("wave6_indexes");
    let mut session = db.open();

    run(
        &mut session,
        "CREATE (:Article {
            title: 'Rust Systems',
            published: 2024,
            tags: ['rust', 'systems'],
            body: 'Rust systems programming and compiler work'
        })",
    );
    run(
        &mut session,
        "CREATE (:Article {
            title: 'Graph Search',
            published: 2022,
            tags: ['graphs', 'query'],
            body: 'Query planners and storage indexes'
        })",
    );
    run(&mut session, "CREATE INDEX ON :Article(published) KIND RANGE");
    run(&mut session, "CREATE INDEX ON :Article(tags) KIND LIST");
    run(&mut session, "CREATE INDEX ON :Article(body) KIND FULLTEXT");

    let explain_range = run(
        &mut session,
        "EXPLAIN MATCH (a:Article) WHERE a.published >= 2024 RETURN a.title",
    );
    assert!(explain_range.rows.iter().any(|row| {
        row[2] == RuntimeValue::String("NodeIndexRangeScan".to_owned())
            && format!("{:?}", row[3]).contains(":Article(published)")
    }));

    let explain_list = run(
        &mut session,
        "EXPLAIN MATCH (a:Article) WHERE 'rust' IN a.tags RETURN a.title",
    );
    assert!(explain_list.rows.iter().any(|row| {
        row[2] == RuntimeValue::String("NodeListIndexScan".to_owned())
            && format!("{:?}", row[3]).contains(":Article(tags)")
    }));

    let explain_text = run(
        &mut session,
        "EXPLAIN MATCH (a:Article) WHERE a.body CONTAINS 'compiler' RETURN a.title",
    );
    assert!(explain_text.rows.iter().any(|row| {
        row[2] == RuntimeValue::String("NodeFullTextIndexScan".to_owned())
            && format!("{:?}", row[3]).contains(":Article(body)")
    }));

    let temporal = run(
        &mut session,
        "MATCH (a:Article {title: 'Rust Systems'}) RETURN a.valid_from, a.valid_to",
    );
    assert!(matches!(temporal.rows[0][0], RuntimeValue::Datetime(_)));
    assert_eq!(temporal.rows[0][1], RuntimeValue::Null);

    drop(session);

    let mut reopened = db.open();
    let indexes = run(&mut reopened, "SHOW INDEXES ON :Article");
    assert!(indexes.rows.iter().any(|row| {
        row[3] == RuntimeValue::String("published".to_owned())
            && row[6] == RuntimeValue::String("range".to_owned())
    }));
    assert!(indexes.rows.iter().any(|row| {
        row[3] == RuntimeValue::String("tags".to_owned())
            && row[6] == RuntimeValue::String("list".to_owned())
    }));
    assert!(indexes.rows.iter().any(|row| {
        row[3] == RuntimeValue::String("body".to_owned())
            && row[6] == RuntimeValue::String("fulltext".to_owned())
    }));

    let reopened_temporal = run(
        &mut reopened,
        "MATCH (a:Article {title: 'Rust Systems'}) RETURN a.valid_from, a.valid_to",
    );
    assert!(matches!(reopened_temporal.rows[0][0], RuntimeValue::Datetime(_)));
    assert_eq!(reopened_temporal.rows[0][1], RuntimeValue::Null);
}

#[test]
fn wave6_index_kind_benchmark_smoke() {
    let db = TestDb::new("wave6_bench");
    let mut session = db.open();

    for index in 0..100 {
        run(
            &mut session,
            &format!(
                "CREATE (:Doc {{
                    rank: {},
                    tags: ['tag{}', 'shared'],
                    body: 'document {} contains benchmark text'
                }})",
                index,
                index % 5,
                index
            ),
        );
    }
    run(&mut session, "CREATE INDEX ON :Doc(rank) KIND RANGE");
    run(&mut session, "CREATE INDEX ON :Doc(tags) KIND LIST");
    run(&mut session, "CREATE INDEX ON :Doc(body) KIND FULLTEXT");

    let started = Instant::now();
    for _ in 0..25 {
        let range = run(
            &mut session,
            "MATCH (d:Doc) WHERE d.rank >= 90 RETURN d.rank ORDER BY d.rank",
        );
        assert_eq!(range.rows.len(), 10);
        let list = run(
            &mut session,
            "MATCH (d:Doc) WHERE 'shared' IN d.tags RETURN d.rank ORDER BY d.rank",
        );
        assert_eq!(list.rows.len(), 100);
        let text = run(
            &mut session,
            "MATCH (d:Doc) WHERE d.body CONTAINS 'benchmark' RETURN d.rank ORDER BY d.rank",
        );
        assert_eq!(text.rows.len(), 100);
    }
    assert!(started.elapsed().as_nanos() > 0);
}
