# Changelog

All notable changes to `cupld` will be documented in this file.

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
