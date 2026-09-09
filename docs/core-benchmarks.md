# Core graph resource benchmarks

`scripts/benchmark_core.py` measures generic graph operations through the embedded Rust API and the actual CLI. It uses a release example, Python 3's standard library, and optional GNU `time`; it adds no Cargo dependencies. Every measured query verifies its expected answer. A failure exits nonzero and leaves a report with `status: "failed"` instead of publishing a successful timing.

Run from the checkout with the repository's Rust toolchain and Python 3 available:

```sh
python3 scripts/benchmark_core.py --smoke --output /tmp/cupld-core-smoke.json
python3 scripts/benchmark_core.py --output /tmp/cupld-core-baseline.json
```

The script builds `cupld` and `examples/core_bench.rs` with `cargo build --locked --offline --release`, using `mise exec` when available. A new checkout may need its reviewed `mise.toml` trusted first. The default run measures 100, 1,000 and 10,000 nodes, three outgoing edges per node, 256 payload bytes per node, five samples per query/open/CLI case and five committed edits. Smoke mode uses 32 and 128 nodes with two samples and edits. Both modes execute the same checks. Output paths must be new; their parent directories must already exist.

To change graph size, connectivity, storage location or measurement repetitions:

```sh
python3 scripts/benchmark_core.py \
  --sizes 1000,5000,20000 --degree 4 --payload-bytes 1024 \
  --repeats 7 --edits 10 --timeout 300 --temp-dir /tmp \
  --output /tmp/cupld-core-large.json
```

The worker builds entities directly with `CupldEngine`, commits once, and saves a new database. This avoids paying for a separate graph-wide query snapshot for every seeded node. Each entity has an ordinal, one of 16 groups, a name, a deterministic varied ASCII payload of exactly the requested byte length, and a revision. Each node links to its next `degree` neighbors in a ring, including wraparound; edges carry an offset. Zero edges are supported. Degree must be smaller than every requested node count. Logical data and topology are repeatable; internal timestamps and database UUIDs are not byte-identical.

## What the artifact measures

| Case | Timed work and verified outcome |
| --- | --- |
| Seed | Graph construction, initial engine commit and initial save separately; node/edge counts and seed payload verified. |
| Open | Fresh worker process for each sample; `Session::open` duration separated from whole-process wall time. |
| Session queries | One freshly opened worker per query case, then repeated calls on that same session. Count, unindexed midpoint lookup, ordered `LIMIT 25`, and one-hop outgoing traversal each verify all returned values. |
| CLI count | Repeated executable launches, DB opening, query execution, JSON formatting and process exit; the JSON count must equal the seeded node count. |
| CLI context | Repeated native node-seeded, depth-one outgoing context commands; exact neighbor ordinals, seed edges and targets verified. |
| Edits | Repeated transactions update only the seed's revision. `BEGIN`, mutation and durable `COMMIT` durations are separated; every post-commit file size is retained. |
| Compaction | Compact the edited database and record before/after file size. Independent reopen checks verify the last edit and graph counts both before and after compaction. |

The artifact includes raw samples, medians, CLI and worker binary sizes and SHA-256 hashes, harness hashes, revision and working-tree status, toolchain, platform, CPU/memory/cgroup details where available, and the temporary parent/filesystem. Query durations include parsing and execution; validation happens after the query timer stops. The first sample is retained and identified by its position; no warmup is discarded. A single seed, edit sequence and compaction occur per size; rerun the whole suite for distributions of those operations.

On Linux with GNU `/usr/bin/time`, each child reports its lifetime peak RSS in bytes. A query worker's peak includes its open, all repeated queries, result validation and shutdown. A mutation worker's peak includes its open and all edits. These are **process peaks**, not incremental allocation measurements; the script does not subtract one high-water mark from another. The Python coordinator and Cargo build are outside the measured child. Without supported GNU `time`, RSS is `null` and latency/file-size measurements remain available. No RAM conclusion should be drawn from a `null` value.

## Compare a baseline and a candidate

1. Use isolated checkouts of both revisions on the same host. Both must contain exactly the same harness files; if the baseline predates this harness, copy only the worker and driver into it and record that untracked addition. Do not backport the implementation being compared.
2. Build both with the same release profile, toolchain, environment, graph settings and storage filesystem. Run sequentially, with other heavy workloads stopped where practical. Repeat both runs, alternating order, before interpreting small timing differences.
3. Use distinct report paths. Require `status: "complete"`, compare `config`, `harness`, `machine`, `build`, filesystem and source identity, then compare `cases[*].summary` and inspect the underlying samples. Uncommitted implementation edits must be recorded alongside the artifact because a revision alone cannot identify them.
4. Compare CLI startup plus open with CLI samples, embedded query latency with `query_seconds`, and absolute phase peaks with the matching phase. Do not compare one command's whole-process latency to another command's query-only timer.

Existing release binaries can be measured with `--skip-build`; optional `--binary` and `--worker` select explicit paths. `CARGO_TARGET_DIR` is respected. In this mode hashes identify the executables, but the script cannot prove they were built from its recorded checkout revision. For a defensible source comparison, build and run the driver inside each corresponding checkout, and retain the build logs if flags or artifacts were supplied externally.

## Interpretation and safety

These are controlled synthetic workloads, not a claim to reproduce a particular application's graph. Uniform ring degree does not model a graph with a few very highly connected nodes. No schema index is created, so the midpoint lookup measures the current unindexed query path. Additional indexed and skewed-topology scenarios should be added when evaluating those capabilities.

OS page caches are not cleared. A fresh process is a new CLI launch, **not proof of a cold disk read**. Defaults should finish on ordinary development machines, but graph cloning, serialization, query materialization and WAL growth can make larger settings expensive. Existing query intermediate-row and context payload budgets remain in force: a larger case may fail instead of producing a valid measurement. `LIMIT 25` restricts returned rows and does not establish bounded execution memory. Raising sizes does not establish larger-than-RAM support, and the runner does not enforce a memory budget.

All databases and sidecars live in an automatically removed temporary directory. The driver never opens a caller-supplied database. The worker's seed operation refuses an existing destination. CLI commands run from the isolated directory with installation prompts and release upgrade checks disabled and a disposable `XDG_CONFIG_HOME`; `HOME` remains untouched. The only retained output is the requested new JSON artifact. Each measured child has a timeout; on POSIX, timeout cleanup kills its process group. A killed coordinator or machine may leave a `running` artifact, which is incomplete and must not be used as a successful result.

Validate changes to the harness with:

```sh
mise exec -- cargo test --offline --example core_bench
mise exec -- cargo clippy --offline --example core_bench -- -D warnings
python3 scripts/benchmark_core.py --smoke --output /tmp/cupld-core-smoke-new.json
```
