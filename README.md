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
cupld context --db default --path notes/example.md --depth 2 --max-nodes 25 --output table
cupld context --db default --node 42 --depth 1 --output table
cupld context --db default --path notes/example.md --depth 2 --output json
```

`context` is seeded: pass one or more `--node <id>` or `--path <src.path>` seeds to get a bounded neighborhood around explicit graph nodes or synced markdown source paths. Use `--depth <n>`, `--direction <in|out|both>`, repeated `--edge-type <type>`, repeated `--label <label>`, `--max-nodes <n>`, and `--max-edges <n>` to control traversal. Table output is human-facing and contains `row`, `depth`, `id`, `labels/type`, `display`, `source`, and `target` columns. JSON and NDJSON remain the stable machine contracts for automation; JSON is the default output mode.

Use `cupld query` for global node listings and ad hoc graph reads:

```bash
cupld query --db default 'MATCH (n) RETURN id(n), labels(n), n.name, n.`src.path` ORDER BY id(n) LIMIT 25'
```

Inspect, validate, and compact a database:

```bash
cupld schema --db default
cupld check --db default
cupld compact --db default
```

Beta note: opening or checking a `.cupld` created by an older `cupld` release upgrades it in place to the current on-disk format. Treat `.cupld` files as forward-only during beta if you may need rollback.

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

Opt in to persisted filesystem structure when directory traversal matters:

```bash
cupld sync markdown --db default --include-fs-graph
```

Watch markdown after the initial persisted sync:

```bash
cupld sync markdown --db default --root notes --watch --idle-ms 500 --max-runs 2
```

Use `cupld query --with-md` for transient overlay reads and `cupld sync markdown` when you want later plain queries to see persisted markdown state. By default sync persists markdown documents and authored `MD_LINKS_TO` compatibility edges. Body links and generic frontmatter `link` / `links` relationships create only `MD_LINKS_TO`; typed frontmatter relationships also add authored signal edges: `up` / `parent` as `MD_UP`, `related` as `MD_RELATED`, `next` as `MD_NEXT`, and `previous` as `MD_PREVIOUS`. `--include-fs-graph` also persists `MarkdownDirectory` nodes plus `MD_IN_DIRECTORY` and `MD_PARENT_DIRECTORY` edges; it does not create `MD_SIBLING_OF` pairwise sibling edges.

Maintain markdown-derived memory state:

```bash
cupld memory check --db default
cupld memory check --db default --strict --output json
cupld memory find-stale --db default --root notes --output table
cupld memory find-orphans --db default --output ndjson
cupld memory reindex --db default --output json
```

`memory check` validates storage integrity, markdown freshness, metadata, duplicate markdown paths and edges, schema index readiness, stale items, orphans, and ambiguous markdown aliases. `memory find-stale` lists markdown documents whose persisted state no longer matches the filesystem, `memory find-orphans` lists current markdown documents without markdown or native graph connectivity, and `memory reindex` inspects existing schema index definitions and reports their status. These commands are diagnostic: use `cupld sync markdown --db default` or `cupld sync markdown --db default --root notes` to refresh markdown-derived DB state after editing notes.

Markdown root resolution for commands that accept `--root` is: explicit `--root`, `.cupld/config.toml`, the DB root saved by `cupld source set-root`, then `./.cupld/data`. Relative roots are resolved against the workspace package root. `memory find-orphans` and `memory reindex` do not need a markdown root and report `root: null` in machine output.

Maintenance reports use stable statuses: `pass` means no problem was found, `warn` means the command found stale or suspicious state but completed successfully, and `fail` is reserved for hard failures. `memory check --strict` keeps warning details in the report but exits with code 2 when the aggregate status is `warn`; without `--strict`, warnings exit successfully. Table output is the default. `--output json` emits one report envelope with `ok`, `command`, `status`, `db_path`, `root`, `summary`, `checks`, and `items`; `--output ndjson` emits one `memory_meta` line followed by `memory_check` and `memory_item` lines.

`cupld memory repair` and `cupld memory citation-audit` are intentionally deferred in this implementation round.

Install into a provider-specific skills directory or a custom path:

```bash
cupld install --target codex --scope home --db default
cupld install --target claude --scope cwd --db default --root notes
cupld install --target opencode --scope home --db default
cupld install --path /custom/skills --db default --yes
```

The interactive installer asks for a skill location, DB path, and markdown root. Interactive REPL launches can offer the same bootstrap flow when no install is tracked, and can prompt to refresh when the bundled skill becomes stale.

`install` records each skill path with its DB path, markdown root, bundle revision, and skill signature in the user config `install-state.toml`. That state lets REPL startup reuse saved paths for refresh prompts. If the state file is corrupt or points at the wrong install, rerun `cupld install ...` with the desired target/path, DB, and root to rewrite it.

Repo-local package settings live in `.cupld/config.toml`. `install` and markdown-aware commands use it as the workspace default for DB path and markdown root.
Use `[markdown] include_fs_graph = true` to enable filesystem graph sync for `cupld sync markdown` by default.

## Development

Run the Rust test suite from a checkout:

```bash
cargo test --locked
```

Run the default deterministic memory eval suite against committed fixtures and snapshots:

```bash
cargo run --locked -- eval memory --ci
```

The CI memory eval command uses `tests/fixtures/memory` by default, does not update snapshots, does not enter watch mode, and reports concise drift failures with fixture, case, assertion, expected, actual, and diff fields. To refresh snapshots intentionally during local fixture work, run:

```bash
cargo run --locked -- eval memory --update-snapshots
```

## Documentation

- Docs index: [`docs/README.md`](./docs/README.md)
- Agent guide: [`docs/agents/README.md`](./docs/agents/README.md) for the full current CLI and automation contract
- Viewer notes: [`docs/agents/visualise.md`](./docs/agents/visualise.md)
- Security policy: [`SECURITY.md`](./SECURITY.md)
- Code of conduct: [`CODE_OF_CONDUCT.md`](./CODE_OF_CONDUCT.md)
