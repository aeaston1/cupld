# Development roadmap

The 2026-09-09 refocus makes the general in-process graph database and its CLI
the primary product. Keep zero third-party dependencies and read/write queries
by default. Retain memory while gradually separating it into a future extension.
The [core audit](core-audit.md) distinguishes shipped behavior from the target
toolbox; [resource benchmarks](core-benchmarks.md) provide reproducible evidence.

## Current milestone: core hardening and evidence

- Establish the generic capability audit and resource harness, including CLI
  startup versus already-open session work, peak RAM, and repeated-edit growth.
- Remove the redundant engine snapshot on read-only query execution, preserving
  transaction errors, savepoints, rollback, and all write behavior.
- Keep scripted CLI startup local by limiting release hints to interactive REPLs.
- Lead onboarding with generic graphs. Existing markdown and MCP memory remain
  documented and supported; no extension packaging or storage-format change.

## Next priorities

1. **Reduce measured core costs.** Use the baseline to prioritize resident graph
   copies, materialized query/context data, index candidate construction, and
   full-state WAL growth. Require before/after measurements and behavior tests;
   avoid claiming bounded memory from a response cap.
2. **Complete the agent CLI contract.** Add dedicated noninteractive creation,
   structured capability discovery, consistent schema/check output and argument
   errors, and optional read-only access. Preserve read/write defaults. Read-only
   access must address migration-on-open as well as query mutation. Acceptance:
   an agent can create/open, discover, query/update, and diagnose a native graph
   through documented machine interfaces without markdown or MCP.
3. **Add generic graph interchange.** Specify typed values, IDs/endpoints,
   schema, duplicate handling, and failure atomicity before implementing bulk
   import/export. Acceptance: lossless round trips of nodes, edges, properties,
   and supported schema, including malformed and interrupted input cases.
   Query row exports alone do not meet this requirement.
4. **Deliver bounded-RAM storage and execution.** Design disk access, eviction,
   transaction durability, intermediate results, and format migration together.
   Acceptance: correct queries and updates on graphs larger than an enforced
   memory budget, with explicit handling of oversized operations. This is a
   separate engine milestone, not a claim about current behavior.
5. **Extract the memory application.** Move markdown ingestion, note lifecycle,
   memory ranking, and harness memory setup behind a stable core boundary.
   Preserve existing data and workflows until an optional first-party package
   supplies their replacement. Defer an extension loader/registry until this
   integration has demonstrated the necessary API.

## Resolved: prevent accidental memory overwrites

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

## Resolved: exclude deleted notes from normal retrieval

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

## Resolved: make database persistence crash-safe

- Merged in PR #53 (`1d982b1`), alongside the memory fixes in PRs #51 and #52.
- Saves, commits, compaction, and migrations now use a synced temporary file and
  atomic replacement. Persistent writer locks plus revision checks reject
  overlapping or stale writes; active-transaction saves are rejected.
- WAL recovery keeps only complete validated records. Migration failures retain
  original bytes, and post-rename flush failures require reopening the session.
- Coverage includes injected partial-write/sync/rename failures, interrupted WAL
  headers and payloads, migration failure, process locks, stale/cloned sessions,
  permissions, aliases, and side-effect-free diagnostic reads.
- Details and platform limits: `docs/agents/README.md`, Database Persistence.

## Deferred memory work: realistic retrieval baselines

- Current large-vault coverage uses 1,000 synthetic filler notes plus targeted
  examples. It is not a realistic latency or repeated-edit growth benchmark.
- Generic graph resource/growth measurement now belongs to the core harness.
  Memory-specific relevance, realistic note sizes, and retrieval latency remain
  future application work. Core benchmarks do not establish retrieval quality.
- Starting points: `tests/fixtures/memory/search_large_vault`, `src/mcp.rs`
  (`load_search_docs`), `src/context.rs`, and `src/storage/mod.rs`.

Historical context: at the 2026-09-07 triage, the inspected branch was
`codex/agent-harness-reliability-v2` at
`115c667`. All 391 tests, eight memory eval cases, formatting, and clippy passed.
That baseline does not cover the failures and risks above.
