#![allow(dead_code)]

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use cupld::{QueryResult, RuntimeValue, Session, Value};

static NEXT_TEST_DB_ID: AtomicUsize = AtomicUsize::new(1);

const PERSON_DDL: &[&str] = &[
    "CREATE LABEL Person",
    "CREATE EDGE TYPE KNOWS",
    "CREATE INDEX ON :Person(email)",
    "CREATE CONSTRAINT ON :Person REQUIRE email UNIQUE",
    "CREATE CONSTRAINT ON :Person REQUIRE age TYPE int",
];

const PERSON_DATA: &[&str] = &[
    "CREATE
      (ada:Person {name: 'Ada', email: 'ada@example.com', age: 36})
      -[:KNOWS {since: 2020}]->
      (grace:Person {name: 'Grace', email: 'grace@example.com', age: 37})",
    "MATCH (grace:Person {name: 'Grace'})
     CREATE (grace)-[:KNOWS {since: 2021}]->
       (alan:Person {name: 'Alan', email: 'alan@example.com', age: 41})",
    "CREATE (bob:Person {name: 'Bob', age: 29})",
];

pub struct TestDb {
    path: PathBuf,
}

impl TestDb {
    pub fn new(prefix: &str) -> Self {
        let path = unique_temp_path(prefix);
        let mut session = Session::new_in_memory();
        session.save_as(&path).unwrap();
        drop(session);
        Self { path }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn open(&self) -> Session {
        Session::open(&self.path).unwrap()
    }
}

impl Drop for TestDb {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

pub fn seed_person_graph(session: &mut Session) {
    for statement in PERSON_DDL {
        run(session, statement);
    }
    for statement in PERSON_DATA {
        run(session, statement);
    }
}

pub fn run(session: &mut Session, query: &str) -> QueryResult {
    run_with_params(session, query, &BTreeMap::new())
}

pub fn run_with_params(
    session: &mut Session,
    query: &str,
    params: &BTreeMap<String, Value>,
) -> QueryResult {
    let mut results = session.execute_script(query, params).unwrap();
    assert_eq!(
        results.len(),
        1,
        "expected exactly one result for query: {query}"
    );
    results.remove(0)
}

#[allow(dead_code)]
pub fn sorted_debug_rows(result: &QueryResult) -> Vec<String> {
    let mut rows = result
        .rows
        .iter()
        .map(|row| format!("{row:?}"))
        .collect::<Vec<_>>();
    rows.sort();
    rows
}

#[allow(dead_code)]
pub fn string_cell(result: &QueryResult, row: usize, column: usize) -> &str {
    match &result.rows[row][column] {
        RuntimeValue::String(value) => value,
        other => panic!("expected string cell, got {other:?}"),
    }
}

fn unique_temp_path(prefix: &str) -> PathBuf {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let suffix = NEXT_TEST_DB_ID.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "cupld_{prefix}_{}_{}_{}.cupld",
        std::process::id(),
        timestamp,
        suffix
    ))
}
