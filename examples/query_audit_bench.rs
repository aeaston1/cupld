//! Dependency-free measurement driver; see scripts/query_audit.py.
use std::collections::BTreeMap;
use std::hint::black_box;
use std::path::Path;
use std::time::Instant;

use cupld::{CupldEngine, PropertyMap, Session, Value};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    let mode = args.get(1).ok_or("expected seed|read|edit|compact")?;
    let path = Path::new(args.get(2).ok_or("expected database path")?);
    let count: usize = args.get(3).ok_or("expected count")?.parse()?;
    if count == 0 {
        return Err("count must be positive".into());
    }
    let params = BTreeMap::new();
    if mode == "seed" {
        let mut engine = CupldEngine::default();
        let mut previous = None;
        for i in 0..count {
            let node = engine.create_node(
                ["Item"],
                PropertyMap::from_pairs([
                    ("key", Value::Int(i as i64)),
                    ("body", Value::String("x".repeat(256))),
                ]),
            )?;
            if let Some(previous) = previous {
                engine.create_edge(previous, node, "NEXT", PropertyMap::new())?;
            }
            previous = Some(node);
        }
        engine.commit()?;
        let mut session = Session::from_engine(engine);
        session.execute_script("CREATE INDEX ON :Item(key)", &params)?;
        session.save_as(path)?;
        println!("{{\"nodes\":{count},\"edges\":{}}}", count - 1);
        return Ok(());
    }
    let start = Instant::now();
    let mut session = Session::open(path)?;
    println!(
        "{{\"metric\":\"open_ms\",\"value\":{}}}",
        start.elapsed().as_secs_f64() * 1000.0
    );
    match mode.as_str() {
        "read" => {
            let query = args.get(4).ok_or("expected query")?;
            // Warm the already-open session once; parsing remains in the timing.
            black_box(session.execute_script(query, &params)?);
            for _ in 0..count {
                let start = Instant::now();
                black_box(session.execute_script(query, &params)?);
                println!(
                    "{{\"metric\":\"query_ms\",\"value\":{}}}",
                    start.elapsed().as_secs_f64() * 1000.0
                );
            }
        }
        "edit" => {
            for i in 0..count {
                let query = format!("MATCH (n:Item {{key: 0}}) SET n.edit = {i}");
                let start = Instant::now();
                session.execute_script(&query, &params)?;
                println!(
                    "{{\"metric\":\"commit_ms\",\"value\":{},\"db_bytes\":{}}}",
                    start.elapsed().as_secs_f64() * 1000.0,
                    std::fs::metadata(path)?.len()
                );
            }
            drop(session);
            let mut reopened = Session::open(path)?;
            let result =
                reopened.execute_script("MATCH (n:Item {key: 0}) RETURN n.edit", &params)?;
            assert_eq!(
                result[0].rows[0][0],
                cupld::RuntimeValue::Int((count - 1) as i64)
            );
        }
        "compact" => {
            let start = Instant::now();
            session.compact()?;
            println!(
                "{{\"metric\":\"compact_ms\",\"value\":{},\"db_bytes\":{}}}",
                start.elapsed().as_secs_f64() * 1000.0,
                std::fs::metadata(path)?.len()
            );
        }
        _ => return Err("expected seed|read|edit|compact".into()),
    }
    Ok(())
}
