# Core capability audit

This audit defines the first milestone of cupld's database-first refocus. The
starting revision is `1d982b1` (persistence hardening, PR #53). Source inspection
establishes behavior; the [benchmark harness](core-benchmarks.md) establishes
resource observations. Neither establishes a larger-than-RAM guarantee.

The [first recorded comparison](benchmarks/2026-09-09-core-baseline.md) includes
the baseline and candidate artifacts, measured read-copy improvement, and
remaining storage growth and process-memory costs.

## Product contract

cupld is an in-process property graph database with a CLI and an embedded Rust
API. People use their existing agents to query and modify their graphs. The
primary agent interface is the CLI; queries remain read/write by default.

- Keep zero third-party Rust dependencies, including development and build
  dependencies, and a standalone binary. The current dependency checks remain
  release gates.
- Make the generic database useful without markdown, an MCP connection, an LLM,
  provider configuration, or a database server.
- A complete toolbox means create/open, ingest, inspect, query/update, traverse,
  export, validate, and compact. This is the target; the matrix below identifies
  incomplete parts.
- Preserve existing memory features during a gradual separation. Markdown
  ingestion, note lifecycle, memory ranking, and harness memory installation are
  candidates for an early first-party extension. No extension loader, registry,
  or new packaging contract is introduced in this milestone.
- Bounded RAM as graphs grow is the eventual storage goal. In-process does not
  mean that the whole database must fit in memory, but the current engine does
  require that. Do not describe it as out-of-core or claim a configurable memory
  budget.

## Capability matrix

| Capability | Current behavior and evidence | Remaining gap |
| --- | --- | --- |
| Embedded API | `Session` exposes in-memory sessions, open/save, parameterized scripts, transactions, and savepoints; `CupldEngine` exposes nodes, edges, and typed properties. See [runtime](../src/runtime/mod.rs) and [public exports](../src/lib.rs). | All database and memory modules are exported by one crate. There is no separately versioned extension API. |
| Database creation | A file-backed REPL opens or creates a database, including with piped stdin. `query` opens an existing database. See `open_initial_session` and `run_repl` in [CLI](../src/main.rs). | No dedicated noninteractive create command. Piped REPL is a bootstrap path, not a structured batch API. |
| Queries and writes | Cypher-style queries, parameters, transactions, constraints, schema changes, aggregates, and graph traversal are supported. See [agent reference](agents/README.md). | It is a documented subset, not a claim of complete Cypher/GQL compatibility. Generic CLI queries do not offer a read-only option. |
| Discovery | Help, `SHOW SCHEMA`, `SHOW INDEXES`, `SHOW CONSTRAINTS`, `SHOW STATS`, and `EXPLAIN` are present. `SHOW` can be run through JSON/NDJSON `query`. | No machine-readable capabilities/grammar inventory. Dedicated `schema` and `check` commands emit human output only. |
| Machine output | `query` and seeded `context` offer JSON/NDJSON contracts and output limits; `query` also accepts named parameters. See [automation](../src/automation.rs). | Some CLI argument/missing-query errors are plain text even when machine output is requested. Row caps apply after query execution; NDJSON is assembled in memory. |
| Graph interchange | Query scripts can create graphs; query results can be emitted as JSON/NDJSON. Markdown is an existing specialized importer. | No generic bulk importer or lossless whole-graph export/import contract. Query result output is not a graph backup format. |
| Context and inspection | Node-seeded traversal supports depth, direction, label/type filters, and response budgets. A viewer supports human exploration. See [context](../src/context.rs). | Context loads a complete graph representation before applying response budgets. Those budgets do not cap process memory. |
| Persistence | Atomic file replacement, cooperating writer locks, stale-session rejection, WAL-tail recovery, and migration/failure tests are present in [storage](../src/storage/mod.rs) and [writer tests](../tests/storage_writers.rs). | This is not a concurrent multi-writer service. A stale session must reopen/replay deliberately. Opening/checking older stores may migrate them. |
| Indexes | Schema index definitions and planner access paths exist; equality/range candidate entries are constructed from current nodes during execution. | An index plan does not establish a persistent disk index or sublinear lookup cost. Full-text/list paths also inspect graph data. |
| Local startup | Scripted commands skip release fetching and release-cache writes after this milestone's startup fix. The optional hint remains on interactive file-backed REPL startup. | Interactive REPL hints may invoke `curl`; `CUPLD_NO_UPGRADE_CHECK=1` disables them. |
| Memory application | Markdown synchronization, maintenance, MCP memory tools, retrieval, installation, and evals remain supported. | Separate application-specific behavior gradually; keep compatibility until a replacement exists. Generic graph benchmarks do not measure memory retrieval quality. |

## Query correctness follow-up

The final RETURN pipeline now projects or aggregates before sorting and limiting.
A two-node `RETURN count(n) AS total LIMIT 1` returns `total: 2`, and sorting by
an explicit output alias uses the projected value. Ordinary source-expression
sorts still work. Invalid sort expressions now return execution errors, including
single-row results; writes retain rollback and transaction recovery behavior.
This composes with the immutable read path above, without restoring read snapshots.

The [supplemental audit](benchmarks/2026-09-09-query-audit.md) records the original
failures, additional CLI gaps, and a separate 1k/10k/50k chain-graph baseline.
Its [raw artifact](benchmarks/2026-09-09-query-audit.json) predates the read-copy
optimization and is historical evidence, not a measurement of this combined branch.
It retains its own reproducible driver because its fixture and phases differ from
the main benchmark. Use a new output path when rerunning; do not overwrite history.

## Resource findings

The [engine](../src/engine/graph.rs), [runtime](../src/runtime/mod.rs), and
[storage](../src/storage/mod.rs) reveal several independent costs:

1. Opening reads the database file and reconstructs the graph; the engine owns
   working data and a committed snapshot. Removing one extra query copy does
   not remove these resident representations.
2. Before this milestone, the data-statement path cloned the engine even for
   reads. The read-efficiency change removes that rollback copy for immutable
   query execution while preserving failure and transaction semantics.
3. Query matches, intermediate rows, ordering, and result rows are materialized.
   A small `LIMIT` or `--max-rows` is not an execution-memory bound.
4. Each persistent commit encodes a full graph state into the logical WAL, then
   atomically replaces the file. Repeated small edits can grow the file and
   amplify copying, serialization, and disk writes. Compaction resets the WAL
   but is itself a whole-store operation.
5. Traversal responses and NDJSON output can be small while their preparation
   still consumes memory proportional to the graph or intermediate results.

The first optimization is deliberately limited to the redundant read snapshot.
Use the harness to measure remaining costs before changing representation,
transaction isolation, the file format, or query execution.

## Validation and release evidence

Run the repository's existing gates through the pinned toolchain:

```sh
mise exec -- cargo fmt --check
mise exec -- cargo clippy --locked --all-targets -- -D warnings
mise exec -- cargo test --locked
mise exec -- cargo run --locked -- eval memory --ci
```

- Keep durability, stale-writer, interrupted-WAL, migration, savepoint,
  constraint, and memory-regression coverage. Existing persistence work is
  already merged; it is not a new backlog task.
- Exercise successful and failed reads inside and outside transactions. A
  failed read must retain the existing failed-transaction recovery behavior.
  Late errors in writes must still restore graph state.
- Test scripted startup with a fake release client and isolated configuration:
  no release subprocess, warning, or cache file should occur.
- Run the generic benchmark smoke with semantic assertions. Record larger
  release-mode runs as observations, not timing-sensitive pass/fail tests.
- Preserve the Linux CI gate and existing macOS/Windows persistence checks.
  A local Linux run does not establish those remote platform results.

## Next milestones and acceptance boundaries

See the [prioritized backlog](backlog.md) for follow-on work. The next toolbox
milestone should supply structured creation/discovery/errors and generic
interchange without changing the default write policy. Define the interchange
identity/type contract before implementation, then require lossless round trips.

The storage milestone requires a separate design covering disk access,
transaction durability, eviction, oversized values, intermediate query data,
and migration compatibility. Its acceptance gate is a graph larger than the
enforced process-memory budget, with correct results and explicit handling of
operations that cannot fit. Existing result truncation does not pass that gate.

Memory extraction follows a stable core boundary and preserves existing user
data and workflows. Deciding an extension ABI or replacing working memory
commands is outside this first milestone.
