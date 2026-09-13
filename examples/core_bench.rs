//! Internal worker for scripts/benchmark_core.py. No benchmark framework required.
use std::collections::BTreeMap;
use std::error::Error;
use std::path::Path;
use std::time::Instant;

use cupld::json::{self, JsonValue};
use cupld::{CupldEngine, PropertyMap, QueryResult, RuntimeValue, Session, Value};

type BenchResult<T> = Result<T, Box<dyn Error>>;

#[derive(Clone, Copy)]
struct Shape {
    nodes: usize,
    degree: usize,
    payload_bytes: usize,
}

impl Shape {
    fn validate(self) -> BenchResult<Self> {
        if self.nodes < 2 || self.degree >= self.nodes {
            return Err("nodes must be at least 2 and degree must be smaller than nodes".into());
        }
        i64::try_from(self.nodes)?;
        self.nodes
            .checked_mul(self.degree)
            .ok_or("edge count overflow")?;
        Ok(self)
    }
}

fn build_graph(shape: Shape) -> BenchResult<CupldEngine> {
    let mut engine = CupldEngine::default();
    let mut ids = Vec::with_capacity(shape.nodes);
    for ordinal in 0..shape.nodes {
        // Stable logical data; engine timestamps and database UUIDs are not fixed.
        let payload = payload(ordinal, shape.payload_bytes);
        ids.push(engine.create_node(
            ["Entity"],
            PropertyMap::from_pairs([
                ("ordinal", Value::Int(ordinal as i64)),
                ("group", Value::Int((ordinal % 16) as i64)),
                ("name", Value::String(format!("entity-{ordinal:08}"))),
                ("payload", Value::String(payload)),
                ("revision", Value::Int(0)),
            ]),
        )?);
    }
    for ordinal in 0..shape.nodes {
        for offset in 1..=shape.degree {
            engine.create_edge(
                ids[ordinal],
                ids[(ordinal + offset) % shape.nodes],
                "LINKS",
                PropertyMap::from_pairs([("offset", Value::Int(offset as i64))]),
            )?;
        }
    }
    Ok(engine)
}

fn payload(ordinal: usize, bytes: usize) -> String {
    const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_";
    let mut state = (ordinal as u64).wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut text = String::with_capacity(bytes);
    for _ in 0..bytes {
        state ^= state >> 12;
        state ^= state << 25;
        state ^= state >> 27;
        let mixed = state.wrapping_mul(2_685_821_657_736_338_717);
        text.push(char::from(ALPHABET[(mixed >> 58) as usize]));
    }
    text
}

fn verify_graph(session: &Session, shape: Shape) {
    let stats = session.engine().stats();
    assert_eq!(stats.node_count, shape.nodes, "node count changed");
    assert_eq!(
        stats.edge_count,
        shape.nodes * shape.degree,
        "edge count changed"
    );
    let seed = session.engine().nodes().next().expect("seed exists");
    assert_eq!(seed.property("ordinal"), Some(&Value::Int(0)));
    assert_eq!(
        seed.property("payload"),
        Some(&Value::String(payload(0, shape.payload_bytes))),
        "seed payload changed"
    );
}

fn execute(session: &mut Session, script: &str) -> BenchResult<QueryResult> {
    let mut results = session.execute_script(script, &BTreeMap::new())?;
    assert_eq!(results.len(), 1, "worker expects one statement result");
    Ok(results.remove(0))
}

fn query_case(name: &str, shape: Shape) -> BenchResult<(String, Vec<Vec<RuntimeValue>>)> {
    let integer_rows = |values: Vec<usize>| {
        values
            .into_iter()
            .map(|value| vec![RuntimeValue::Int(value as i64)])
            .collect()
    };
    match name {
        "count" => Ok((
            "MATCH (n:Entity) RETURN count(n) AS total".into(),
            integer_rows(vec![shape.nodes]),
        )),
        "lookup" => Ok((
            format!("MATCH (n:Entity) WHERE n.ordinal = {} RETURN n.ordinal AS ordinal", shape.nodes / 2),
            integer_rows(vec![shape.nodes / 2]),
        )),
        "limited_scan" => Ok((
            "MATCH (n:Entity) RETURN n.ordinal AS ordinal ORDER BY n.ordinal LIMIT 25".into(),
            integer_rows((0..shape.nodes.min(25)).collect()),
        )),
        "traversal" => Ok((
            "MATCH (n:Entity {ordinal: 0})-[:LINKS]->(m:Entity) RETURN m.ordinal AS ordinal ORDER BY m.ordinal".into(),
            integer_rows((1..=shape.degree).collect()),
        )),
        _ => Err(format!("unknown query case: {name}").into()),
    }
}

fn elapsed(start: Instant) -> JsonValue {
    JsonValue::from(start.elapsed().as_secs_f64())
}

fn run() -> BenchResult<JsonValue> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.len() != 7 {
        return Err(
            "usage: core_bench PHASE DB NODES DEGREE PAYLOAD_BYTES ITERATIONS QUERY_CASE".into(),
        );
    }
    let phase = args[0].as_str();
    let path = Path::new(&args[1]);
    let shape = Shape {
        nodes: args[2].parse()?,
        degree: args[3].parse()?,
        payload_bytes: args[4].parse()?,
    }
    .validate()?;
    let iterations: usize = args[5].parse()?;
    if iterations == 0 {
        return Err("iterations must be positive".into());
    }
    let mut fields = vec![("phase", JsonValue::from(phase))];
    if phase == "seed" {
        if path.try_exists()? {
            return Err("seed refuses to overwrite an existing database".into());
        }
        let started = Instant::now();
        let mut engine = build_graph(shape)?;
        fields.push(("build_seconds", elapsed(started)));
        let started = Instant::now();
        engine.commit()?;
        fields.push(("engine_commit_seconds", elapsed(started)));
        let mut session = Session::from_engine(engine);
        let started = Instant::now();
        session.save_as(path)?;
        fields.push(("initial_save_seconds", elapsed(started)));
        verify_graph(&session, shape);
        fields.push((
            "seed_node_id",
            JsonValue::from(session.engine().nodes().next().unwrap().id().get()),
        ));
        fields.push(("database_bytes", JsonValue::from(path.metadata()?.len())));
    } else {
        let started = Instant::now();
        let mut session = Session::open(path)?;
        fields.push(("open_seconds", elapsed(started)));
        verify_graph(&session, shape);
        match phase {
            "open" => {}
            "query" => {
                let (query, expected) = query_case(&args[6], shape)?;
                let mut samples = Vec::with_capacity(iterations);
                for _ in 0..iterations {
                    let started = Instant::now();
                    let result = execute(&mut session, &query)?;
                    samples.push(elapsed(started));
                    assert_eq!(result.rows, expected, "wrong result for {query}");
                }
                fields.push(("query", JsonValue::from(query)));
                fields.push(("query_seconds", JsonValue::array(samples)));
                fields.push(("verified_rows_per_query", JsonValue::from(expected.len())));
            }
            "mutate" => {
                let mut samples = Vec::with_capacity(iterations);
                fields.push((
                    "initial_database_bytes",
                    JsonValue::from(path.metadata()?.len()),
                ));
                for revision in 1..=iterations {
                    let started = Instant::now();
                    execute(&mut session, "BEGIN")?;
                    let begin_seconds = elapsed(started);
                    let query =
                        format!("MATCH (n:Entity {{ordinal: 0}}) SET n.revision = {revision}");
                    let started = Instant::now();
                    execute(&mut session, &query)?;
                    let mutation_seconds = elapsed(started);
                    let started = Instant::now();
                    execute(&mut session, "COMMIT")?;
                    let commit_seconds = elapsed(started);
                    let seed = session.engine().nodes().next().unwrap();
                    assert_eq!(
                        seed.property("revision"),
                        Some(&Value::Int(revision as i64))
                    );
                    samples.push(JsonValue::object([
                        ("revision", JsonValue::from(revision)),
                        ("begin_seconds", begin_seconds),
                        ("mutation_seconds", mutation_seconds),
                        ("commit_seconds", commit_seconds),
                        ("database_bytes", JsonValue::from(path.metadata()?.len())),
                    ]));
                }
                fields.push(("edits", JsonValue::array(samples)));
            }
            "compact" => {
                fields.push((
                    "before_database_bytes",
                    JsonValue::from(path.metadata()?.len()),
                ));
                let started = Instant::now();
                session.compact()?;
                fields.push(("compact_seconds", elapsed(started)));
                fields.push((
                    "after_database_bytes",
                    JsonValue::from(path.metadata()?.len()),
                ));
            }
            "verify" => {
                let seed = session.engine().nodes().next().unwrap();
                assert_eq!(
                    seed.property("revision"),
                    Some(&Value::Int(iterations as i64))
                );
                let (query, expected) = query_case("count", shape)?;
                assert_eq!(execute(&mut session, &query)?.rows, expected);
                fields.push(("verified_revision", JsonValue::from(iterations)));
            }
            _ => return Err(format!("unknown phase: {phase}").into()),
        }
    }
    fields.push(("verified", JsonValue::from(true)));
    Ok(JsonValue::object(fields))
}

fn main() {
    match run() {
        Ok(report) => println!("{}", json::stringify(&report)),
        Err(error) => {
            eprintln!("core benchmark failed: {error}");
            std::process::exit(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixture_has_exact_payload_and_ring_edges_including_wraparound() {
        let shape = Shape {
            nodes: 7,
            degree: 3,
            payload_bytes: 17,
        }
        .validate()
        .unwrap();
        let engine = build_graph(shape).unwrap();
        let nodes = engine.nodes().collect::<Vec<_>>();
        assert_eq!(nodes.len(), 7);
        for (ordinal, node) in nodes.iter().enumerate() {
            assert_eq!(node.property("ordinal"), Some(&Value::Int(ordinal as i64)));
            let Some(Value::String(payload)) = node.property("payload") else {
                panic!("missing payload");
            };
            assert_eq!(payload.len(), 17);
            assert!(payload.is_ascii());
            if ordinal > 0 {
                assert_ne!(node.property("payload"), nodes[0].property("payload"));
            }
            let mut destinations = engine
                .outgoing_edge_ids(node.id())
                .into_iter()
                .map(|id| {
                    let edge = engine.edge(id).unwrap();
                    let Some(Value::Int(offset)) = edge.property("offset") else {
                        panic!("missing offset");
                    };
                    assert_eq!(edge.to(), nodes[(ordinal + *offset as usize) % 7].id());
                    edge.to()
                })
                .collect::<Vec<_>>();
            destinations.sort();
            destinations.dedup();
            assert_eq!(destinations.len(), 3);
        }
    }

    #[test]
    fn all_query_cases_match_the_fixture_with_and_without_edges() {
        for degree in [0, 3] {
            let shape = Shape {
                nodes: 31,
                degree,
                payload_bytes: 9,
            }
            .validate()
            .unwrap();
            let mut session = Session::from_engine(build_graph(shape).unwrap());
            for name in ["count", "lookup", "limited_scan", "traversal"] {
                let (query, expected) = query_case(name, shape).unwrap();
                assert_eq!(
                    execute(&mut session, &query).unwrap().rows,
                    expected,
                    "{name}"
                );
            }
        }
    }
}
