# Supplemental query correctness and resource audit

This is the supplemental audit from the query-correctness work. Its observations
precede integration with PR #54 and complement the [main core audit](../core-audit.md).
The 445-test follow-up count below records the standalone fix; the combined PR
receives a fresh validation run. The archived raw data is unchanged.

Audit date: 2026-09-09. Engine baseline: `1d982b1304324cd339d3881886109e1193bb1ffa`
(main, including persistence #53, memory creation #52, and tombstone retrieval #51).
The [raw observations](2026-09-09-query-audit.json), [Rust measurement driver](../../examples/query_audit_bench.rs),
and [runner](../../scripts/query_audit.py) accompany this report.

cupld already offers a useful dependency-free embedded graph engine and read/write
CLI. Its immediate hardening priorities are wrong-result query behavior, reliable
automation, unnecessary graph copies, and commit amplification. It is not yet a
complete generic database toolbox or a bounded-memory database. No production
engine behavior is changed by this audit.

## H1 follow-up — 2026-09-13

Both query defects below are fixed in the local runtime. Final projection and
aggregation now precede sorting and LIMIT. Ordinary ordering preserves source
bindings and explicit aliases; grouped ordering uses computed projection values.
Invalid sort expressions now propagate errors, including for single-row results,
and statement/transaction rollback behavior is covered.

Validation: 445 tests passed (19 new lifecycle/CLI regressions), one existing
subprocess helper ignored, and all eight memory eval cases passed. Formatting and
clippy passed. See the [query contract](../agents/README.md#query-surface) for exact
scope and ordering rules. H1 is resolved locally; H2–H7 remain open.

The capability inventory, reproduced failures, measurements, and verification
counts below describe the original 2026-09-09 baseline. They and the raw benchmark
artifact are retained as historical evidence rather than rewritten as a new run.

## Capability inventory

| Area | Present and verified | Gap or qualification | Evidence |
| --- | --- | --- | --- |
| Distribution/API | Zero third-party Cargo dependencies; standalone binary; public `CupldEngine` and `Session` | Optional interactive upgrade hint can invoke `curl`; benchmarks disable it | Cargo metadata, `tests/dependency_free.rs`, `src/lib.rs`, `src/main.rs` |
| Persistence | Synced temporary replacement, writer lock, stale revision rejection, guarded saves, validated WAL recovery | Advisory cooperating-writer coordination; no stale edit merging; hardware/platform durability limits remain | `src/storage/mod.rs`, `tests/storage_writers.rs`, [persistence contract](../agents/README.md#database-persistence) |
| Transactions | Autocommit, explicit transactions, savepoints, failed-transaction recovery | Graph copies for rollback, savepoints, and commit; no concurrent-session snapshot refresh | `Session::execute_statement`, `begin`, `commit`, `savepoint`; `tests/db_lifecycle.rs` |
| Migrations | Legacy fixture migrates in place; failed migration preserves original bytes | Even open/check can write on legacy files; no public generic read-only open policy | Storage tests and lifecycle fixture tests |
| Schema/constraints | Labels, edge types, properties, indexes, unique/type/endpoint/cardinality constraints | Schema catalogs describe definitions; index planning is not evidence of a maintained physical index | `tests/db_lifecycle.rs`, `src/engine/schema.rs`, runtime planner |
| Queries | MATCH, filters, writes, WITH, aggregates, bounded-hop traversal, parameters, EXPLAIN | Two reproduced projection/ordering defects; eager matching and projection | Semantic probes below; `src/runtime/mod.rs` |
| Generic agent workflow | Create/open workaround, structured SHOW, parameterized update, ID discovery, seeded context, NDJSON row export | Creation receipt, lossless graph interchange, structured administration and discovery missing | Runner's `generic_workflow` checks |
| Machine contracts | JSON/NDJSON query/context; structured query parser/runtime failures; output truncation metadata | Usage failures can be plain text; REPL statement errors can exit zero; schema/check/compact are text only | `cli_probes` in raw observations; `src/main.rs` |
| Resource controls | Output caps, context budgets, max traversal depth 10, intermediate row guard 100,000 | Whole graph loads; row guard runs after match materialization; no execution byte budget, spilling, or cancellation budget | Runtime matcher, `src/query/mod.rs`, `src/context.rs` |
| Database tools | Integrity check, manual compaction, schema DDL/catalogs, visual viewer | No generic import/export format, explicit create command, generic read-only sessions, structured command discovery, or standalone backup/restore workflow | CLI parser/help; public library API |
| Memory compatibility | Markdown sync/watch, MCP memory, tombstone filtering, atomic note creation | Still coupled to the core and some administration; `check_database` also computes markdown alias diagnostics | 8 memory eval cases and existing MCP/markdown tests |

## Reproduced correctness defects

The runner creates two native `Person` nodes, Ada and Grace, linked by `KNOWS`.
It updates Ada's score to 3; Grace's score is 2. No markdown/MCP is involved.

| Query | Expected | Observed |
| --- | --- | --- |
| `MATCH (n:Person) RETURN count(n) AS total LIMIT 1` | One aggregate row, `total: 2` | `total: 1` |
| `MATCH (n:Person) RETURN n.score AS score ORDER BY score` | Scores 2, 3 | Scores 3, 2 |

`Session::execute_query` calls `apply_order_and_limit` before
`project_result_rows`. Thus the aggregate sees limited input, and the sort runs
before the projected alias exists. These are reproduced wrong-result defects,
not merely missing syntax. The audit records expected and actual values rather
than adding tests that bless the defective behavior. Fixing the projection
pipeline and adding regression coverage is backlog item H1.

Additional CLI observations:

- `query --db <db> --output json 'RETURN ('` exits 1 with a JSON error on stderr.
- `query --output json 'RETURN 1'` exits 1 with plain-text missing-DB error.
- `schema` and `check` reject `--output json`. The previous guide incorrectly
  claimed a machine contract for `check`; the guide is corrected in this milestone.
- Piping `RETURN (` into the file-backed REPL prints a parse error but exits 0.
  Use one-shot `query` for scripted operations until H2 is complete.

These probes are not an exhaustive query language conformance audit. Nulls,
mixed numeric ordering, aggregate grouping, mutation rollback, and malformed
storage inputs deserve broader boundary coverage before stronger correctness claims.

## Measurement method

Run from the repository root:

```bash
mise exec -- cargo build --locked --offline --release --bin cupld --example query_audit_bench --target-dir target/core-audit
python3 scripts/query_audit.py --output /tmp/cupld-query-audit.json
```

The Rust driver uses only the existing library and standard library. The
development runner uses Python 3's standard library and GNU `/usr/bin/time` on
Linux; those are not cupld runtime dependencies. It creates temporary databases,
checks subprocess failures, verifies the final edit after reopen and compaction,
and removes its fixtures. `--sizes`, `--samples`, and `--edits` allow repeat runs.
Semantic defects are recorded as `matches_expected: false`; successful completion
of the runner does not mean those defects passed.

Environment: Linux 6.8.0-111-generic, aarch64, glibc 2.39, Rust 1.95.0,
about 15.2 GiB host RAM, shared host, no swap. Build is
`release`, not the thin-LTO `dist` profile; binary is unstripped. Fixtures have
1,000 / 10,000 / 50,000 nodes, a 256-byte ASCII body and integer key per node,
N−1 directed chain edges, and an equality index on `Item.key`. Timestamps and
database identities vary; topology and property payloads are deterministic.
Seeding uses the embedded API and is excluded from query/commit timings.

Five repeated CLI launches measure wall time including spawn, open, parse,
execution, serialization, and captured output. Each already-open session uses
one unmeasured warmup and five timed queries including parsing and result
materialization, excluding open and serialization. There is no OS cache eviction:
these are warm-cache process launches, not cold-disk startup measurements.
CLI wall times include the GNU time wrapper; use comparative measurements, not
sub-millisecond precision. Background host activity can affect results; no p95
or production SLA is inferred from five samples.

RSS is GNU time's process high-water resident set, including loading, allocator
retention, and all work in that child. It is not an allocation profile or
per-query incremental memory measurement. The edit process includes ten commits
and final reopen verification. No enforced memory budget was used and none of
these fixtures exceeds available RAM.

## Results

Release binary: **3,345,624 bytes** (3.19 MiB). `--version` launch median:
**3.35 ms**, maximum RSS **2,432 KiB**. This is a minimal startup reference;
database open costs are substantially larger.

Median latency in milliseconds; initial compacted database:

| Operation | 1k nodes | 10k nodes | 50k nodes |
| --- | ---: | ---: | ---: |
| CLI `RETURN 1 AS value` | 13.70 | 97.15 | 495.18 |
| Open-session `RETURN 1 AS value` | 0.88 | 13.18 | 74.98 |
| CLI indexed key lookup | 12.99 | 103.15 | 520.88 |
| Open-session indexed key lookup | 1.19 | 20.34 | 97.85 |
| CLI scan with query `LIMIT 10` | 14.62 | 112.87 | 544.81 |
| Open-session scan with query `LIMIT 10` | 1.37 | 21.23 | 107.93 |
| CLI full scan, output cap 10 | 15.09 | 112.73 | 562.55 |
| Open-session full scan, no serialization/cap | 1.57 | 23.63 | 123.86 |
| CLI indexed start + one hop | 13.25 | 104.86 | 517.54 |
| Open-session indexed start + one hop | 1.22 | 19.64 | 99.65 |

The full-scan session returns all rows internally; the corresponding CLI keeps
only ten for output. These are deliberately distinct phases, not equivalent
end-to-end serialization benchmarks. First open in the lookup driver took
7.04 / 56.94 / 300.13 ms respectively (one observation per size).

| Storage/resource measure | 1k nodes | 10k nodes | 50k nodes |
| --- | ---: | ---: | ---: |
| Initial file, bytes | 373,211 | 3,730,211 | 18,650,211 |
| File after 10 single-node edits, bytes | 4,104,691 | 41,031,691 | 205,151,691 |
| File after compaction, bytes | 373,228 | 3,730,228 | 18,650,228 |
| First edit including autocommit, ms | 16.81 | 130.23 | 620.22 |
| Tenth edit including autocommit, ms | 61.66 | 514.97 | 2,692.17 |
| CLI constant query after edits, median ms | 47.44 | 430.55 | 2,158.07 |
| Compaction excluding open, ms (single sample) | 35.62 | 321.13 | 1,575.05 |
| Indexed CLI lookup peak RSS, MiB | 8.63 | 55.21 | 262.05 |
| CLI scan `LIMIT 10` peak RSS, MiB | 9.13 | 59.84 | 285.49 |
| Edit/reopen process peak RSS, MiB | 27.87 | 260.32 | 1,096.52 |

Commits here are autocommit statements including matching, mutation, validation,
snapshotting, encoding, lock/revision checks, and durable replacement. They do
not isolate filesystem `fsync` latency. Manual compaction reduces file size back
to one state but itself loads/rewrites data. There is no automatic compaction policy.

## Source-backed cost explanation

1. **Whole-file and whole-graph loading.** `storage::load_with_migration_policy`
   uses `fs::read`; `parse_file` retains snapshot and WAL data; `decode_engine`
   decodes the snapshot then every retained full-state WAL payload.
   `CupldEngine` retains working graph data and a committed snapshot.
2. **Reads copy the working graph.** `Session::execute_statement` clones the
   engine before ordinary data statements, including `RETURN 1` and read-only
   MATCH. `SHOW` and `EXPLAIN` take separate branches. Constant-query scaling is
   consistent with this copying; an allocation profiler was not used to isolate
   its exact share. Removing unnecessary read rollback copies is H3.
3. **Index lookup is rebuilt.** `index_seek_candidates` calls
   `build_node_index_entries`, scanning nodes into a new BTreeMap on each query.
   `NodeIndexSeek` in EXPLAIN therefore does not imply logarithmic end-to-end
   lookup. List/fulltext candidate builders also inspect graph nodes. See H4.
4. **Limits follow matching.** `match_pattern_rows` builds its vector before
   checking 100,000 rows. `execute_query` applies LIMIT later; automation applies
   `--max-rows` after execution. `query_as_ndjson` returns a vector of serialized
   lines. Neither output caps nor NDJSON provide bounded execution. See H5.
5. **Full-state WAL amplification.** `append_commit` reads and parses the
   existing file, encodes `engine.to_state()` as a new WAL record, assembles a
   complete replacement, and calls `write_durable`. Ten tiny edits retain about
   eleven complete states. Atomic replacement is a correctness improvement to
   preserve; weakening durability is not an acceptable optimization. See H6.

Decoder count fields also feed `Vec::with_capacity` before payload exhaustion
checks. This is a source-inspection concern for corrupt/adversarial input; no
OOM repro was run. H7 covers allocation bounds and malformed-input tests.

## Verification and scope

Reproduce the regression gate with the same isolated target:

```bash
mise exec -- cargo fmt --check
mise exec -- cargo clippy --locked --offline --all-targets --target-dir target/core-audit -- -D warnings
mise exec -- cargo test --locked --offline --target-dir target/core-audit
mise exec -- cargo run --locked --offline --target-dir target/core-audit -- eval memory --ci
mise exec -- cargo metadata --locked --offline --no-deps --format-version 1
```

- Fresh target directory: **426 tests passed, 1 ignored**, including transaction,
  savepoint, recovery, migration, constraint, stale writer, and memory regressions.
  The ignored test is the subprocess lock helper, exercised by its parent test.
- **8/8 memory eval cases passed**, no warnings or snapshot updates.
- `cargo fmt --check`, clippy with `--all-targets -- -D warnings`, release build,
  and offline Cargo metadata passed; dependency list is empty.
- Generic CLI smoke: JSON schema discovery, parameterized persisted update,
  reopened reads, ID-based context with two nodes and one edge, and NDJSON row
  export all checked without markdown or MCP.
- Existing debug cache initially linked stale library artifacts; a fresh
  `target/core-audit` build resolved that mismatch. Results above use that target.
- This run verifies Linux aarch64 only; it does not establish new Windows/macOS
  or power-loss durability evidence. Passing regressions did not detect the two
  new semantic defects; they remain open and prioritized.

The [hardening backlog](../backlog.md) separates this milestone from generic toolbox
completion, disk-backed storage/bounded execution, and the eventual first-party
memory extension. Require lossless round-trip tests when interchange lands, and
larger-than-RAM workloads under an enforced memory budget before claiming bounded
RAM. Preserve the Rust API, standalone distribution, zero third-party dependencies,
read/write defaults, and existing memory behavior throughout.
