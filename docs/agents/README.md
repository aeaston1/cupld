# cupld

`cupld` is a local graph database CLI and REPL.

This is the canonical agent guide for operating `cupld`. Use the [`docs` index](../README.md) for the full map and the [repo root README](../../README.md) for the short entrypoint.

## Start Here

- Use `cupld` to start an in-memory REPL.
- Use `cupld <path.cupld>` or `cupld --db <path.cupld|default>` to open or create a file-backed REPL.
- Use `cupld install` to install the bundled `cupld-md-memory` skill and bootstrap local memory. The default DB path is `.cupld/default.cupld` and the default markdown root is `.cupld/data`.
- Use `--db default` to target `./.cupld/default.cupld`.
- Use `cupld query --db <path.cupld|default> ...` for one-shot automation.
- Use `cupld schema --db <path.cupld|default>` and `cupld check --db <path.cupld|default>` before making assumptions about a DB.
- Use `cupld --visualise <path.cupld>` to open the scene viewer.

## CLI Shape

```text
cupld
cupld <path.cupld>
cupld --db <path.cupld|default>
cupld --visualise <path.cupld>
cupld --db <path.cupld|default> --visualise
cupld --visualise --db <path.cupld|default> --query 'MATCH (n) RETURN n LIMIT 10'
cupld query --db <path.cupld|default> [--output <table|json|ndjson>] [--params-json <json> | --params-file <path>] [--max-rows <n>] [query]
cupld context --db <path.cupld|default> [--top-k <n>] [--output <table|json|ndjson>]
cupld schema --db <path.cupld|default>
cupld compact --db <path.cupld|default>
cupld check --db <path.cupld|default>
cupld sync markdown --db <path.cupld|default> [--root <path>]
cupld source set-root --db <path.cupld|default> <path>
cupld install [--target <codex|claude|opencode> [--scope <cwd|home>] | --path <skills-root>] [--db <path.cupld|default>] [--root <path>] [--force] [--yes]
```

Important constraints:

- `query`, `schema`, `compact`, and `check` require `--db <path.cupld|default>`.
- `--query` is a top-level option and only works with `--visualise`.
- Dot-commands are REPL-only. They do not work with `cupld query`.
- Passing a missing `path.cupld` to the REPL creates a new database file.
- Interactive REPL launches may prompt once to run the same install/bootstrap flow if it has not been installed yet.
- Repo-local package defaults live in `.cupld/config.toml`.
- `--db default` is an alias for `./.cupld/default.cupld`.

## Agent Workflow

For safe automation, prefer this order:

1. `cupld check --db default`
2. `cupld schema --db default`
3. `cupld query --db default --output json 'MATCH (n) RETURN n ORDER BY id(n) LIMIT 10'`
4. Use `cupld context --db ...` when prompt assembly needs a top-k summary of nodes.
5. Switch to the REPL when you need repeated interactive exploration.

Use explicit transactions for multi-statement batches. Outside a transaction, mutating statements commit immediately.

## Common Commands

Inspect schema:

```bash
cupld schema --db default
```

Create described schema objects and inspect filtered catalogs:

```bash
cupld query --db default "CREATE LABEL Service DESCRIPTION 'Long-running services'"
cupld query --db default "CREATE INDEX ON :Service(name)"
cupld query --db default "SHOW INDEXES ON :Service"
```

Run one query inline:

```bash
cupld query --db default 'MATCH (n:Person) RETURN n.name ORDER BY n.name'
```

Run a multiline query from stdin:

```bash
cat <<'EOF' | cupld query --db default
BEGIN;
MATCH (n:Person {name: 'Ada'})
SET n.role = 'engineer'
RETURN n.name, n.role;
COMMIT;
EOF
```

Use typed literals, dynamic indexing, and richer edge filters:

```bash
cupld query --db default "
MATCH (a:Person)-[e:KNOWS|MENTORS]->(b:Person)
WHERE has_label(a, 'Person')
  AND edge_type(e) =~ '^(KNOWS|MENTORS)$'
  AND b.name ENDS WITH 'n'
RETURN b.name, {name: b.name}['name'], bytes'gold', datetime'2024-01-02T03:04:05Z'
"
```

Use staged projections, aggregates, and `MERGE` in one query:

```bash
cupld query --db default "
MATCH (n:Person)
WITH n.age >= 37 AS senior, count(*) AS total
RETURN senior, total
"
```

Validate and compact a database:

```bash
cupld check --db default
cupld compact --db default
```

Install the bundled markdown-memory skill and bootstrap local memory:

```bash
cupld install
cupld install --target codex --scope home --db default
cupld install --target claude --scope cwd --db default --root notes
cupld install --target opencode --scope home --db default
cupld install --path ~/.claude/skills --db default --yes
```

Defaults:

- DB path: `.cupld/default.cupld`
- Markdown root: `.cupld/data`

Open the scene viewer:

```bash
cupld --db default --visualise
```

Seed the scene viewer with a read-only query:

```bash
cupld --visualise --db default --query 'MATCH (n:Person) RETURN n LIMIT 10'
```

## REPL

Start the REPL with either:

```bash
cupld
cupld mydb.cupld
```

Available dot-commands:

```text
.help
.quit
.output table|json|ndjson
.open <path.cupld>
.save
.saveas <path.cupld>
.schema
.indexes
.constraints
.stats
.transactions
```

REPL notes:

- `.output json` and `.output ndjson` are the current machine-friendly output modes.
- `cupld query` and `cupld context` support table, JSON, and NDJSON output (`--output`).
- Table output may truncate long values. JSON and NDJSON are safer for machine parsing.
- `.save` only works after the session has a file path.
- `.saveas <path.cupld>` persists an in-memory database.
- Exiting a dirty session prompts to save.

## Query Surface

Verified statement families in the current CLI/runtime:

- Schema: `CREATE LABEL`, `CREATE EDGE TYPE`, `CREATE INDEX`, `CREATE CONSTRAINT`
- Reads: `MATCH`, `WHERE`, `RETURN`, `ORDER BY`, `LIMIT`, `SHOW`, `EXPLAIN`
- Writes: `CREATE`, `SET`, `REMOVE`, `DELETE`
- Transactions: `BEGIN`, `COMMIT`, `ROLLBACK`, `SAVEPOINT`, `ROLLBACK TO SAVEPOINT`, `RELEASE SAVEPOINT`

Verified builtin functions in expressions:

- `append`
- `remove`
- `merge`
- `size`
- `type`
- `id`
- `labels`
- `has_prop`
- `keys`
- `values`
- `contains`

Automation guidance:

- Prefer `--output json` or `--output ndjson` for machine consumption.
- Always add explicit `ORDER BY` plus explicit `LIMIT` for deterministic context windows.
- Use named parameters with `--params-json` or `--params-file`.

## Automation Contracts

`query` and `context` now expose a stable machine contract when `--output json` or `--output ndjson` is selected.

- `cupld query --output json` writes one JSON envelope to stdout:
  - `ok`
  - `command`
  - `policy`
  - `results`
- `cupld query --output ndjson` writes one `query_meta` line, one `query_result` line per result set, and one `query_row` line per returned row.
- `cupld context --output json` writes one JSON envelope to stdout:
  - `ok`
  - `command`
  - `policy`
  - `retrieval_usage`
  - `provenance`
  - `items`
- `cupld context --output ndjson` writes one `context_meta` line plus one `context_item` line per item.
- `query` and `context` failures in JSON or NDJSON mode write a machine error envelope to stderr:
  - `ok: false`
  - `error.code`
  - `error.message`

Current automation controls:

- `CUPLD_QUERY_MAX_ROWS` sets the default `query --max-rows` cap.
- `CUPLD_NO_INSTALL_PROMPT=1` disables the interactive bootstrap prompt on REPL startup.
- Markdown root resolution order is: explicit `--root`, then `.cupld/config.toml`, then DB metadata from `source set-root`, then `./.cupld/data`.
- `cupld query --with-markdown` overlays markdown into a temporary session and does not persist the sync.
- `cupld sync markdown` is the explicit persisted sync boundary.

## Further Docs

Use this README for day-to-day CLI operation. For the rest of the public docs:

- [`../README.md`](../README.md): docs index and reading order
- [`./visualise.md`](./visualise.md): notes specific to the `--visualise` scene viewer

If you are changing CLI behavior, update this guide first.
