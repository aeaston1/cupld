# Deferred development backlog

Deferred after repository triage on 2026-09-07. These retain their original
priority numbers; priority 4 covers reconciling PR #50 and preparing a release.
They are recorded for later and are outside the implementation scope of PR #50.

## 1. Prevent accidental memory overwrites

- Shipped: `memory_add` creates notes atomically and never replaces an existing
  file.
- Reproduced before the fix: two `memory_add` calls containing only `content`
  both succeeded at `memory-note.md`; the second replaced the first note.
- Implicit filenames receive unique numeric suffixes, and an occupied explicit
  `path_hint` returns `already_exists`. Final file symlinks are never followed,
  parent confinement is checked before creating nested directories, and
  `note_path` and `uri` report the canonical path. Updates use direct markdown
  editing followed by `memory_sync`; after `markdown_written_sync_failed`, the
  recovery is `memory_sync`, not a second `memory_add`.
- Starting points: `src/mcp.rs` (`memory_add`, `safe_relative_path`,
  `prepare_confined_parent`) and `tests/mcp.rs`.

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

- Implemented on `codex/crash-safe-storage`; awaiting review and merge.
- Saves, commits, compaction, and migrations now use a synced temporary file and
  atomic replacement. Persistent writer locks plus revision checks reject
  overlapping or stale writes; active-transaction saves are rejected.
- WAL recovery keeps only complete validated records. Migration failures retain
  original bytes, and post-rename flush failures require reopening the session.
- Coverage includes injected partial-write/sync/rename failures, interrupted WAL
  headers and payloads, migration failure, process locks, stale/cloned sessions,
  permissions, aliases, and side-effect-free diagnostic reads.
- Details and platform limits: `docs/agents/README.md`, Database Persistence.

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
