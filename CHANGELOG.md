# Changelog

All notable changes to `cupld` will be documented in this file.

## Unreleased

- Opening or checking a `.cupld` file from an older release now upgrades it in place to the current on-disk format. During beta, treat `.cupld` files as forward-only if rollback matters.

## [0.1.0] - 2026-04-01

- Initial local graph database CLI and REPL with file-backed database support.
- Added `query`, `schema`, `compact`, `check`, and `--visualise` command paths.
- Added markdown source syncing, in-DB graph operations, transactions, and REPL mode.
- Bootstrapped release distribution with `cargo-dist` including GitHub workflow, Linux/macOS/Windows target matrix, shell and PowerShell installers, MSI, and artifact hashes.
- Added release/install documentation covering GitHub Releases, Homebrew, WinGet, and `cargo install cupld`.
- Added package metadata and docs needed for publishing, plus MIT license.
