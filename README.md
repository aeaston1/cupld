# cupld

`cupld` is a local graph database CLI and REPL with first-class support for markdown-backed memory workflows.

It gives you an interactive shell, one-shot query commands, integrity checks, a scene viewer, and a bundled skill for wiring markdown notes into a local memory database.

## Highlights

- Local-first graph database with file-backed `.cupld` stores
- REPL for interactive exploration and updates
- Scriptable `query`, `schema`, and `check` commands
- Markdown sync and bundled `cupld-md-memory` skill bootstrap
- Visual graph viewer for inspecting a database

## Install

From source:

```bash
cargo install --path .
```

After GitHub releases are published:

```bash
curl --proto '=https' --tlsv1.2 -LsSf https://github.com/aeaston1/cupld/releases/latest/download/cupld-installer.sh | sh
```

```powershell
powershell -ExecutionPolicy Bypass -c "irm https://github.com/aeaston1/cupld/releases/latest/download/cupld-installer.ps1 | iex"
```

After the corresponding channel publish:

```bash
brew install aeaston1/tap/cupld
cargo install cupld
```

```powershell
winget install aeaston1.cupld
```

## Quickstart

Start an in-memory REPL:

```bash
cupld
```

Open or create a file-backed database:

```bash
cupld state/dev.cupld
```

Run a one-shot query:

```bash
cupld query --db state/dev.cupld 'MATCH (n) RETURN n LIMIT 10'
```

Inspect and validate a database:

```bash
cupld schema --db state/dev.cupld
cupld check --db state/dev.cupld
```

Open the viewer:

```bash
cupld --visualise state/dev.cupld
```

## Markdown Memory

Bootstrap the bundled `cupld-md-memory` skill and a local `.cupld` memory DB:

```bash
cupld install
```

Install into a provider-specific skills directory or a custom path:

```bash
cupld install --target codex --scope home --db .cupld/default.cupld
cupld install --target claude --scope cwd --db .cupld/default.cupld --root notes
cupld install --target opencode --scope home --db .cupld/default.cupld
cupld install --path /custom/skills --db .cupld/default.cupld --yes
```

The interactive installer asks for a skill location, DB path, and markdown root. Interactive REPL launches can also offer the same bootstrap flow once.

## Documentation

- Docs index: [`docs/README.md`](./docs/README.md)
- Agent guide: [`docs/agents/README.md`](./docs/agents/README.md)
- Viewer notes: [`docs/agents/visualise.md`](./docs/agents/visualise.md)
- Security policy: [`SECURITY.md`](./SECURITY.md)
- Code of conduct: [`CODE_OF_CONDUCT.md`](./CODE_OF_CONDUCT.md)
