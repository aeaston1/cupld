# Core baseline and read-efficiency comparison — 2026-09-09

Removing the read-query rollback copy reduced query time and peak process RAM
on these synthetic graphs. Database opening, context construction, and mutation
memory remain substantial costs. This is a local measurement, not a
larger-than-RAM or cross-platform performance guarantee.

## Reproduction and source identity

- Baseline: `dfb59aee62324f9633269dcaa9f27f04d90e7432`, which adds only the
  harness to the unchanged `1d982b1` implementation.
- Candidate: `ed122e29cc1c0f88e1bbd902fb7ccf12dd6d3039`, including immutable
  read evaluation and interactive-only release hints. Both runs disable release
  hints, so network checking is not part of this performance comparison.
- Both source trees were clean, both rebuilt with the same Rust 1.95 toolchain
  and release profile, and driver/worker source hashes match. Binary hashes,
  compiler details, filesystem, and machine metadata are in the raw reports.
- Host: Linux aarch64, eight logical CPUs, approximately 15.2 GiB RAM. The
  disposable databases used the same `/tmp` filesystem. OS caches were not
  cleared, and this was a shared development host.
- Workload: 1,000 and 10,000 entities, three ring edges per entity, five
  properties per entity including a 256-byte payload, and no schema indexes.
- Run order: baseline A, candidate A, candidate B, baseline B. Each run uses
  seven query/open/CLI samples and five single-node committed edits per size.

Run in each corresponding checkout:

```sh
python3 scripts/benchmark_core.py --sizes 1000,10000 --repeats 7 --edits 5 \
  --output /tmp/cupld-core-new-run.json
```

All four artifacts have `status: "complete"` and pass the harness's result
checks. Latencies below are pooled medians: 14 query/open/CLI samples, or ten
commits, per implementation and graph size. Query samples come from two open
sessions with seven queries each. RSS values are the maximum child-process
peak across the two runs, including opening and verification; they are not
incremental allocations. See the [measurement contract](../core-benchmarks.md).

## Read results

| Graph | Count query median, baseline → candidate | Count-worker peak RSS, baseline → candidate |
| --- | --- | --- |
| 1,000 nodes / 3,000 edges | 2.72 → 0.42 ms | 12.42 → 9.84 MiB |
| 10,000 nodes / 30,000 edges | 39.37 → 6.64 ms | 102.64 → 78.11 MiB |

At 10,000 nodes the count query is about 5.9 times faster, with about 24% lower
peak process RAM. Other immutable query paths also improve:

| 10,000-node operation | Baseline median | Candidate median |
| --- | ---: | ---: |
| Unindexed midpoint lookup, open session | 34.71 ms | 9.84 ms |
| Ordered scan with `LIMIT 25`, open session | 36.21 ms | 10.00 ms |
| One-hop outgoing traversal, open session | 35.20 ms | 2.62 ms |
| Actual CLI count, including launch/open/JSON/exit | 198.67 ms | 149.02 ms |
| Embedded database open | 102.47 ms | 103.31 ms |
| Actual CLI context, including launch/open/JSON/exit | 215.60 ms | 211.37 ms |
| Durable transaction commit | 349.76 ms | 352.22 ms |

Opening, context, and commit timing do not show a comparable improvement; small
differences on this host should not be interpreted as regressions or gains.
The optimization removes one copy from read execution, not full-store loading
or result materialization.

## Remaining memory and growth costs

For the 10,000-node graph, both implementations have approximately:

| Measurement | Observation |
| --- | ---: |
| Open-worker peak RSS | 78.11 MiB |
| CLI-context peak RSS | 115.66 MiB |
| Mutation-worker peak RSS | 323.32 MiB |
| Compaction-worker peak RSS | 190.39 MiB |
| Initial database size | 6,090,222 bytes |
| Database size after five single-node commits | 36,540,932 bytes |
| Database size after compaction | 6,090,222 bytes |

Whole-state WAL records make five tiny edits grow the store to approximately
six times its compacted size. The next resource work should investigate
resident graph representations, mutation/commit copies and serialization, and
full-state WAL growth. Context construction and result materialization need
their own bounded-execution work. The observed 78 MiB peak while opening a
roughly 6 MB file also shows why eliminating a read copy is only a first step.

The release CLI grew from 3,345,624 to 3,345,840 bytes on this target (216 bytes).
Zero third-party Rust dependencies are retained. These are ordinary `release`
build sizes; release-distribution profile or stripping choices can differ.

## Raw evidence and checks

The complete JSON artifacts are stored compressed:

- [Baseline A](cupld-core-baseline-20260909-a.json.gz)
- [Candidate A](cupld-core-candidate-20260909-a.json.gz)
- [Candidate B](cupld-core-candidate-20260909-b.json.gz)
- [Baseline B](cupld-core-baseline-20260909-b.json.gz)

Read one using Python's standard library:

```sh
python3 - <<'PY'
import gzip
import json
with gzip.open('docs/benchmarks/cupld-core-candidate-20260909-a.json.gz', 'rt') as source:
    report = json.load(source)
print(json.dumps([case['summary'] for case in report['cases']], indent=2))
PY
```

The integrated source passed formatting, all-target Clippy, 430 ordinary tests
(one pre-existing ignored test), two benchmark fixture/query tests, and all
eight memory eval cases. The same release benchmark smoke used by the new CI
step passed on the candidate. Existing macOS/Windows persistence jobs remain
in the workflow; they were not run remotely for this local implementation.
