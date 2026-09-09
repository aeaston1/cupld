# Deferred development backlog

Deferred after repository triage on 2026-09-07. These retain their original
priority numbers; priority 4 covers reconciling PR #50 and preparing a release.
They are recorded for later and are outside the implementation scope of PR #50.

## 1. Prevent accidental memory overwrites

- Reproduced: two `memory_add` calls containing only `content` both succeed at
  `memory-note.md`; the second replaces the first note.
- Introduce collision-safe creation and explicit update semantics. Validate the
  final file target as well as its parent so a symlink cannot escape the root.
- Starting points: `src/mcp.rs` (`memory_add`, `safe_relative_path`,
  `ensure_confined_write`) and `tests/mcp.rs`.

## 2. Exclude deleted notes from normal retrieval

- Resolved. Ordinary MCP search, get, list, and resources exclude notes marked
  `src.status = missing`; deleted structural nodes cannot affect ranking.
- MCP context excludes deleted markdown notes/directories and their incident
  edges before traversal. Legacy notes without status and native graph nodes
  remain readable; explicit graph queries and CLI context retain tombstones.
- `memory_sync` and `memory_add` honor the workspace
  `[markdown] include_fs_graph` setting, so MCP syncs tombstone deleted
  directories the same way `cupld sync markdown` does.
- Regression coverage exercises delete/sync/read, indexed search, restoration,
  identity lookups including title collisions and renames, resources, context
  budgets, MCP-driven directory tombstones, and historical compatibility.

## 3. Make database persistence crash-safe

- Source finding: `write_durable` truncates the live database with `File::create`
  before writing and syncing its replacement. An interrupted or failed write
  can damage the existing store; no crash reproduction was attempted.
- Add atomic replacement, protection against competing writers, and meaningful
  interruption/recovery tests. Include WAL recovery and migration failure paths
  in the investigation.
- Starting points: `src/storage/mod.rs` (`write_durable`, `append_commit`,
  `parse_wal`, migration handling) and `src/runtime/mod.rs` (`Session`).

## 5. Establish realistic retrieval and growth baselines

- Current large-vault coverage uses 1,000 synthetic filler notes plus targeted
  examples. It is not a realistic latency or repeated-edit growth benchmark.
- Measure search quality, latency, database size, and reopening cost using
  realistic note sizes and repeated edits. Optimize the measured bottlenecks
  before expanding semantic retrieval.
- Starting points: `tests/fixtures/memory/search_large_vault`, `src/mcp.rs`
  (`load_search_docs`), `src/context.rs`, and `src/storage/mod.rs`.

At triage, the inspected branch was `codex/agent-harness-reliability-v2` at
`115c667`. All 391 tests, eight memory eval cases, formatting, and clippy passed.
That baseline does not cover the failures and risks above.
