# cupld

`cupld` is a lightweight, in-process graph database for people and their agents. Query and update local graphs through the CLI, explore them in the REPL, or embed the Rust library in your application.

The core is built in Rust with zero third-party crate dependencies and runs without a database server. Your existing agent can inspect the schema, execute parameterized graph queries, and consume structured results. Markdown-backed agent memory remains a supported specialized workflow.

## Highlights

- Local graph storage in `.cupld` files, plus in-memory sessions
- Nodes, typed edges, properties, schema, constraints, and transactions
- CLI queries with named parameters and JSON/NDJSON output
- Seeded graph context with traversal and response budgets
- Interactive REPL, graph viewer, integrity checks, and compaction
- Embedded Rust `Session` and `CupldEngine` APIs
- Bundled markdown sync and MCP memory tools

## Install

From package channels:

```bash
brew install aeaston1/tap/cupld
cargo install cupld
```

Manual from GitHub Releases:

- Open the [latest release](https://github.com/aeaston1/cupld/releases/latest)
- Select the asset for your OS and architecture
- Extract the archive or run the installer
- Move `cupld` onto your `PATH`

Optional release installer scripts:

```bash
curl --proto '=https' --tlsv1.2 -LsSf https://github.com/aeaston1/cupld/releases/latest/download/cupld-installer.sh | sh
```

```powershell
powershell -ExecutionPolicy Bypass -c "irm https://github.com/aeaston1/cupld/releases/latest/download/cupld-installer.ps1 | iex"
```

From a local checkout:

```bash
cargo install --path .
```

## Quickstart

Create a small graph using the existing REPL with stdin. Start with a new `graph.cupld` path; rerunning this example adds more nodes.

```bash
export CUPLD_NO_INSTALL_PROMPT=1
export CUPLD_NO_UPGRADE_CHECK=1
cupld graph.cupld <<'EOF'
CREATE (:Person {name: 'Ada'})-[:KNOWS]->(:Person {name: 'Grace'})
.quit
EOF
```

The environment settings suppress optional setup prompts and release checks. `cupld graph.cupld` opens or creates the file; `cupld` alone opens an in-memory REPL. One-shot `query` requires an existing database.

Inspect the schema, then query the graph with your agent:

```bash
cupld query --db graph.cupld --output json 'SHOW SCHEMA'
cupld query --db graph.cupld --output json --params-json '{"name":"Ada"}' \
  'MATCH (a:Person {name: $name})-[:KNOWS]->(b:Person) RETURN id(a) AS node_id, a.name AS person, b.name AS knows ORDER BY b.name LIMIT 10'
```

The result identifies Ada's node and the connection to Grace. Pass a returned `node_id` to `context`; on this newly created graph, Ada's ID is `1`:

```bash
cupld context --db graph.cupld --node 1 --depth 1 --max-nodes 10 --max-edges 10 --output json
cupld check --db graph.cupld
cupld compact --db graph.cupld
```

Queries allow reads and writes by default. Use `BEGIN`/`COMMIT` for multi-statement batches; there is no generic CLI `--read-only` flag. `query` and `context` provide machine output, while `schema` and `check` currently print table/text output. See the [agent guide](docs/agents/README.md) for update examples, output contracts, and error handling.

Open the graph viewer from an interactive terminal:

```bash
cupld --db graph.cupld --visualise
```

## Embed in Rust

With `cupld` as a dependency in your Rust application:

```rust
use std::collections::BTreeMap;
use cupld::{Session, Value};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut db = Session::new_in_memory();
    let params = BTreeMap::from([("name".to_owned(), Value::String("Ada".to_owned()))]);
    db.execute_script("CREATE (:Person {name: $name})", &params)?;
    let results = db.execute_script(
        "MATCH (p:Person) RETURN p.name AS name ORDER BY p.name",
        &BTreeMap::new(),
    )?;
    println!("{:?}", results[0].rows);
    Ok(())
}
```

Use `Session::open(path)` for an existing file or `Session::save_as(path)` to persist a new in-memory database.

## Current Boundaries

Graphs and query results currently reside in memory. Output limits bound returned data; they do not provide a working-memory cap or larger-than-RAM execution. Generic bulk import/export and machine-readable capability discovery are not shipped commands. The [core audit](docs/core-audit.md) separates current behavior from the roadmap, and the [core benchmarks](docs/core-benchmarks.md) define reproducible measurements.

Opening or checking an older `.cupld` file may upgrade its format in place. Database writes use atomic replacement and a persistent `<database>.lock` sidecar; stale sessions must reopen before saving. Keep that sidecar in place. See the [persistence contract](docs/agents/README.md#database-persistence) for recovery errors and platform durability limits.

## Markdown Memory

The bundled markdown connector, MCP memory tools, maintenance commands, and `cupld-md-memory` skill remain supported. Use the [memory guide](docs/memory.md) for setup, sync, search, and harness configuration. Memory is a specialized workflow on the database; a separate first-party extension is a future packaging step.

## Development

Run the local hygiene gate from a checkout:

```bash
cargo fmt --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
cargo run --locked -- eval memory --ci
```

The CI memory eval command uses `tests/fixtures/memory` by default, does not update snapshots, does not enter watch mode, and reports concise drift failures with fixture, case, assertion, expected, actual, and diff fields. To refresh snapshots intentionally during local fixture work, run:

```bash
cargo run --locked -- eval memory --update-snapshots
```

Search relevance evals are included in the default CI suite. To run only the local deterministic `memory_search` subset, use:

```bash
cargo run --locked -- eval memory --case search_relevance --output table
cargo run --locked -- eval memory --case search_large_vault --output table
```

## Documentation

- [Docs index](docs/README.md)
- [Agent CLI guide](docs/agents/README.md)
- [Markdown memory](docs/memory.md)
- [Viewer notes](docs/agents/visualise.md)
- [Security policy](SECURITY.md) and [code of conduct](CODE_OF_CONDUCT.md)
