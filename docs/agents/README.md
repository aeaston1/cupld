# cupld

`cupld` is a local graph database CLI and REPL.

This is the canonical agent guide for operating `cupld`. Use the [`docs` index](../README.md) for the full map and the [repo root README](../../README.md) for the short entrypoint.

## Start Here

- Use `cupld` to start an in-memory REPL.
- Use `cupld <path.cupld>` or `cupld --db <path.cupld>` to open or create a file-backed REPL.
- Use `cupld install` to install the bundled `cupld-md-memory` skill and bootstrap a local `.cupld` memory DB/root.
- Use `cupld query --db <path.cupld> ...` for one-shot automation.
- Use `cupld schema --db <path.cupld>` and `cupld check --db <path.cupld>` before making assumptions about a DB.
- Use `cupld --visualise <path.cupld>` to open the scene viewer.

## CLI Shape

```text
cupld
cupld <path.cupld>
cupld --db <path.cupld>
cupld --visualise <path.cupld>
cupld --db <path.cupld> --visualise
cupld --visualise --db <path.cupld> --query 'MATCH (n) RETURN n LIMIT 10'
cupld query --db <path.cupld> [--output <table|json|ndjson>] [--params-json <json> | --params-file <path>] [--max-rows <n>] [query]
cupld context --db <path.cupld> [--top-k <n>] [--output <table|json|ndjson>]
cupld mcp --db <path.cupld> [schema|check|query ...]
cupld schema --db <path.cupld>
cupld compact --db <path.cupld>
cupld check --db <path.cupld>
cupld install [--target <codex|claude|opencode> [--scope <cwd|home>] | --path <skills-root>] [--db <path.cupld>] [--root <path>] [--force] [--yes]
```

Important constraints:

- `query`, `schema`, `compact`, and `check` require `--db <path.cupld>`.
- `--query` is a top-level option and only works with `--visualise`.
- Dot-commands are REPL-only. They do not work with `cupld query`.
- Passing a missing `path.cupld` to the REPL creates a new database file.
- Interactive REPL launches may prompt once to run the same install/bootstrap flow if it has not been installed yet.

## Agent Workflow

For safe automation, prefer this order:

1. `cupld check --db state/dev.cupld`
2. `cupld schema --db state/dev.cupld`
3. `cupld query --db state/dev.cupld --output json 'MATCH (n) RETURN n ORDER BY id(n) LIMIT 10'`
4. Use `cupld context --db ...` when prompt assembly needs a top-k summary of nodes.
5. Switch to the REPL when you need repeated interactive exploration.

Use explicit transactions for multi-statement batches. Outside a transaction, mutating statements commit immediately.

## Common Commands

Inspect schema:

```bash
cupld schema --db state/dev.cupld
```

Run one query inline:

```bash
cupld query --db state/dev.cupld 'MATCH (n:Person) RETURN n.name ORDER BY n.name'
```

Run a multiline query from stdin:

```bash
cat <<'EOF' | cupld query --db state/dev.cupld
BEGIN;
MATCH (n:Person {name: 'Ada'})
SET n.role = 'engineer'
RETURN n.name, n.role;
COMMIT;
EOF
```

Validate and compact a database:

```bash
cupld check --db state/dev.cupld
cupld compact --db state/dev.cupld
```

Install the bundled markdown-memory skill and bootstrap local memory:

```bash
cupld install
cupld install --target codex --scope home --db .cupld/default.cupld
cupld install --target claude --scope cwd --db .cupld/default.cupld --root notes
cupld install --target opencode --scope home --db .cupld/default.cupld
cupld install --path ~/.claude/skills --db .cupld/default.cupld --yes
```

Open the scene viewer:

```bash
cupld --visualise state/dev.cupld
```

Seed the scene viewer with a read-only query:

```bash
cupld --visualise --db state/dev.cupld --query 'MATCH (n:Person) RETURN n LIMIT 10'
```

## REPL

Start the REPL with either:

```bash
cupld
cupld state/dev.cupld
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
- `cupld query` supports table, JSON, and NDJSON output (`--output`).
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

## Further Docs

Use this README for day-to-day CLI operation. For the rest of the public docs:

- [`../README.md`](../README.md): docs index and reading order
- [`./visualise.md`](./visualise.md): notes specific to the `--visualise` scene viewer

If you are changing CLI behavior, update this guide first.
