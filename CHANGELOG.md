# Changelog

All notable changes to `cupld` will be documented in this file.

## [0.4.0] - 2026-05-13

- Seeded `cupld context`: path/node seeds, BFS traversal, budgets, and table/JSON/NDJSON output. (`c9a48f0`, 2026-05-13; `3282c5e`, 2026-05-13; `455f657`, 2026-05-13; `5d4260f`, 2026-05-13; `c2d4cd3`, 2026-05-13; `f864016`, 2026-05-13)
- Markdown filesystem graph sync: directories, parent edges, and opt-in structural graph data. (`e9d4608`, 2026-05-12; `f542479`, 2026-05-12; `062864c`, 2026-05-12; `533009b`, 2026-05-12; `e036361`, 2026-05-12)
- New `cupld memory` maintenance suite: check, reindex, stale docs, orphans, and alias diagnostics. (`85743b9`, 2026-05-12; `9d09f90`, 2026-05-12; `b3b9667`, 2026-05-12; `6e9a527`, 2026-05-13; `22c06ff`, 2026-05-13; `3edf59f`, 2026-05-13; `4fa11d4`, 2026-05-13; `49e3e1c`, 2026-05-13)
- Deterministic memory evals with fixtures, snapshots, citation checks, stale-doc checks, and CI coverage. (`699a320`, 2026-05-13; `122ea15`, 2026-05-13; `c0c4b7b`, 2026-05-13; `de0c7ae`, 2026-05-13; `82bceab`, 2026-05-13; `902596f`, 2026-05-13; `bae1416`, 2026-05-13; `2c87b3a`, 2026-05-13; `193ed2a`, 2026-05-13)
- Added release publishing automation and upgrade/release hints. (`ac79181`, 2026-05-12; `5c54185`, 2026-05-13)
- Fixed memory report path normalization on macOS so default DB/root paths compare consistently. (`37385d0`, 2026-05-13)

## [0.3.0] - 2026-05-12

- Opening or checking a `.cupld` file from an older release now upgrades it in place to the current on-disk format. During beta, treat `.cupld` files as forward-only if rollback matters.
- Added `cupld mcp serve` stdio MCP server. (`70f00e6`, 2026-05-11)
- Added MCP memory tools: `memory_health`, `memory_get`, `memory_list`, `memory_search`, `memory_sync`, `memory_add`. (`70f00e6`, 2026-05-11)
- Added `cupld install --mcp` for Codex/Claude MCP config setup, with backups, dry-run, print-only, state tracking. (`a388225`, 2026-05-09)
- Improved markdown link resolution: `index.md`, slugs, aliases, case-insensitive paths, site-style URL paths. (`995e629`, 2026-05-06; `62d829a`, 2026-05-06)
- Renamed query overlay flag from `--with-markdown` to `--with-md`. (`c3e2ff3`, 2026-05-06)
- Removed implicit 1000-row runtime query cap; only explicit `LIMIT` truncates now. (`ad701a4`, 2026-05-06)
- Added `-v` / `--version`. (`49132e2`, 2026-04-30)
- Added Symphony workflow. (`4e6297e`, 2026-05-11)
- Simplified CLI help and updated docs/agent skill docs. (`49132e2`, 2026-04-30; `c3e2ff3`, 2026-05-06)
- Added substantial tests for MCP, install flow, markdown sync, CLI behavior. (`995e629`, 2026-05-06; `62d829a`, 2026-05-06; `a388225`, 2026-05-09; `70f00e6`, 2026-05-11)

## [0.1.0] - 2026-04-01

- Initial local graph database CLI and REPL with file-backed database support.
- Added `query`, `schema`, `compact`, `check`, and `--visualise` command paths.
- Added markdown source syncing, in-DB graph operations, transactions, and REPL mode.
- Bootstrapped release distribution with `cargo-dist` including GitHub workflow, Linux/macOS/Windows target matrix, shell and PowerShell installers, MSI, and artifact hashes.
- Added release/install documentation covering GitHub Releases, Homebrew, WinGet, and `cargo install cupld`.
- Added package metadata and docs needed for publishing, plus MIT license.
