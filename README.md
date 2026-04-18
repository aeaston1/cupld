# cupld

`cupld` is a local graph database CLI and REPL with first-class support for markdown-backed memory workflows.

It provides interactive exploration, one-shot queries, compact context output for agents, markdown sync, and a visual graph viewer over file-backed `.cupld` stores. It is built in Rust as a single binary with no external runtime dependencies.

## Highlights

- Local-first graph database with file-backed `.cupld` stores
- Pure Rust binary with no external runtime dependencies
- Interactive REPL plus scriptable `query`, `context`, `schema`, and `check` commands
- Stable JSON and NDJSON envelopes for `query` and `context` automation
- Markdown sync, optional watch mode, and bundled `cupld-md-memory` skill bootstrap
- Visual graph viewer for inspecting a database

## Install

From package channels:

```bash
brew install aeaston1/tap/cupld
cargo install cupld
```

Manual from GitHub Releases:

- Open the [latest release](https://github.com/aeaston1/cupld/releases/latest)
- Select the asset for your OS and architecture
- Extract the archive or run the installer
- Move `cupld` onto your `PATH`

Optional release installer scripts:

```bash
curl --proto '=https' --tlsv1.2 -LsSf https://github.com/aeaston1/cupld/releases/latest/download/cupld-installer.sh | sh
```

```powershell
powershell -ExecutionPolicy Bypass -c "irm https://github.com/aeaston1/cupld/releases/latest/download/cupld-installer.ps1 | iex"
```

From a local checkout:

```bash
cargo install --path .
```

## Quickstart

Start an in-memory REPL:

```bash
cupld
```

Open or create a file-backed database:

```bash
cupld mydb.cupld
```

Run a one-shot query:

```bash
cupld query --db default 'MATCH (n) RETURN n LIMIT 10'
```

Run the same query with the machine envelope:

```bash
cupld query --db default --output json 'MATCH (n) RETURN n LIMIT 10'
```

Build compact context rows for agent prompts:

```bash
cupld context --db default --top-k 25
```

Inspect, validate, and compact a database:

```bash
cupld schema --db default
cupld check --db default
cupld compact --db default
```

Open the viewer:

```bash
cupld --db default --visualise
```

## Markdown Memory

Bootstrap the bundled `cupld-md-memory` skill and a local `.cupld` memory DB:

```bash
cupld install
```

By default, `install` uses `.cupld/default.cupld` for the database file and `.cupld/data` for the markdown root. `--db default` is a shortcut for that database path.

Sync markdown into a database and override the root:

```bash
cupld sync markdown --db default --root notes
cupld source set-root --db default notes
```

Watch markdown after the initial persisted sync:

```bash
cupld sync markdown --db default --root notes --watch --idle-ms 500 --max-runs 2
```

Use `cupld query --with-markdown` for transient overlay reads and `cupld sync markdown` when you want later plain queries to see persisted markdown state.

Install into a provider-specific skills directory or a custom path:

```bash
cupld install --target codex --scope home --db default
cupld install --target claude --scope cwd --db default --root notes
cupld install --target opencode --scope home --db default
cupld install --path /custom/skills --db default --yes
```

The interactive installer asks for a skill location, DB path, and markdown root. Interactive REPL launches can also offer the same bootstrap flow once.

Repo-local package settings live in `.cupld/config.toml`. `install` and markdown-aware commands use it as the workspace default for DB path and markdown root.

## Documentation

- Docs index: [`docs/README.md`](./docs/README.md)
- Agent guide: [`docs/agents/README.md`](./docs/agents/README.md) for the full current CLI and automation contract
- Viewer notes: [`docs/agents/visualise.md`](./docs/agents/visualise.md)
- Security policy: [`SECURITY.md`](./SECURITY.md)
- Code of conduct: [`CODE_OF_CONDUCT.md`](./CODE_OF_CONDUCT.md)
