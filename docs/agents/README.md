# cupld

`cupld` is a local graph database CLI and REPL.

This is the canonical agent guide for current shipped behavior. Use the [`docs` index](../README.md) for the public docs map and treat `internal/` roadmap and design notes as historical planning material.

## Start Here

- Use `cupld` to start an in-memory REPL.
- Use `cupld <path.cupld>` or `cupld --db <path.cupld|default>` to open or create a file-backed REPL.
- Use `cupld install` to install the bundled `cupld-md-memory` skill and bootstrap local memory. The default DB path is `./.cupld/default.cupld` and the default markdown root is `./.cupld/data/`.
- Use `--db default` to target `./.cupld/default.cupld`.
- Use `cupld query --db <path.cupld|default> ...` for one-shot automation and `cupld context --db <path.cupld|default> ...` for top-k context rows.
- Use `cupld query --db ... --with-markdown` for transient markdown overlay reads.
- Use `cupld sync markdown --db ...` to persist markdown and `cupld sync markdown --db ... --watch` to keep syncing after the initial run.
- Use `cupld schema --db ...` and `cupld check --db ...` before making automation assumptions about a database.
- Opening or checking a database from an older `cupld` release may upgrade the `.cupld` file in place. Treat on-disk databases as forward-only during beta if rollback matters.
- Use `--output json` or `--output ndjson` for machine consumption.

## CLI Shape

```text
cupld
cupld <path.cupld>
cupld --db <path.cupld|default>
cupld --visualise <path.cupld>
cupld <path.cupld> --visualise
cupld --visualise --db <path.cupld|default>
cupld --db <path.cupld|default> --visualise
cupld --visualise --db <path.cupld|default> --query 'MATCH (n) RETURN n LIMIT 10'
cupld query --db <path.cupld|default> [--with-markdown] [--root <path>] [--output <table|json|ndjson>] [--params-json <json> | --params-file <path>] [--max-rows <n>] [query]
cupld context --db <path.cupld|default> [--top-k <n>] [--output <table|json|ndjson>]
cupld schema --db <path.cupld|default>
cupld compact --db <path.cupld|default>
cupld check --db <path.cupld|default>
cupld sync markdown --db <path.cupld|default> [--root <path>] [--watch] [--poll-ms <n>] [--debounce-ms <n>] [--batch-ms <n>] [--idle-ms <n>] [--max-runs <n>]
cupld source set-root --db <path.cupld|default> <path>
cupld install [--target <codex|claude|opencode> [--scope <cwd|home>] | --path <skills-root>] [--db <path.cupld|default>] [--root <path>] [--force] [--yes]
```

Important constraints:

- `query`, `context`, `schema`, `compact`, `check`, `sync markdown`, and `source set-root` require `--db <path.cupld|default>`.
- `--query` is a top-level option and only works with `--visualise`.
- Dot-commands are REPL-only. They do not work with `cupld query`.
- Passing a missing `path.cupld` to the REPL creates a new database file.
- Opening or checking an older `.cupld` file may upgrade it in place to the current on-disk format.
- Repo-local package defaults live in `.cupld/config.toml`.
- `--db default` is an alias for `./.cupld/default.cupld`.

## Agent Workflow

For safe automation, prefer this order:

1. `cupld check --db default`
2. `cupld schema --db default`
3. `cupld query --db default --output json 'MATCH (n) RETURN n ORDER BY id(n) LIMIT 10'`
4. `cupld context --db default --top-k 25 --output json` when prompt assembly needs a bounded context window
5. `cupld query --db default --with-markdown ...` when you want markdown overlaid without persisting it
6. `cupld sync markdown --db default` or `cupld sync markdown --db default --watch ...` when later plain queries should see persisted markdown state

Use explicit transactions for multi-statement batches. Outside a transaction, mutating statements commit immediately.

## Common Commands

Inspect schema and filtered catalogs:

```bash
cupld schema --db default
cupld query --db default "CREATE LABEL Service DESCRIPTION 'Long-running services'"
cupld query --db default "CREATE EDGE TYPE CALLS DESCRIPTION 'Service-to-service calls'"
cupld query --db default "CREATE INDEX ON :Service(name)"
cupld query --db default "SHOW INDEXES ON :Service"
cupld query --db default "SHOW CONSTRAINTS ON [:CALLS]"
```

Use typed literals, dynamic indexing, alternation, and regex/string predicates:

```bash
cupld query --db default "
RETURN ['Ada', 'Grace'][1],
       {name: 'Ada'}['name'],
       'ace' IN 'grace',
       'name' IN {name: 'Ada'},
       bytes'abc',
       datetime'2024-01-02T03:04:05Z'
"
```

```bash
cupld query --db default "
MATCH (a:Person)-[e:KNOWS|MENTORS]->(b:Person)
WHERE has_label(a, 'Person')
  AND edge_type(e) =~ '^(KNOWS|MENTORS)$'
  AND b.name ENDS WITH 'n'
RETURN a.name, edge_type(e), b.name
ORDER BY b.name
"
```

Use `WITH` and aggregates in one staged query:

```bash
cupld query --db default "
MATCH (n:Person)
WITH n.age >= 37 AS senior, count(*) AS total, collect(n.name) AS names
RETURN senior, total, names
ORDER BY senior
"
```

Use `MERGE`, path-valued results, and `RETURN *`:

```bash
cupld query --db default "
MATCH (a:Person {name: 'Ada'})
MERGE p = (a)-[:MENTORS]->(m:Person {name: 'Lin', email: 'lin@example.com', age: 33})
RETURN *
"
```

Use update helpers, positional list edits, and label removal:

```bash
cupld query --db default "
MATCH (n:Person {name: 'Ada'})
SET n.tags = insert(n.tags, 1, 'graph'),
    n.tags[0] = 'systems',
    n += {role: 'engineer'}
REMOVE n:Person, n.old_field
RETURN n.tags, n.role, has_label(n, 'Person'), has_prop(n, 'old_field')
"
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

- `.output json` and `.output ndjson` are the machine-friendly output modes.
- `cupld query` and `cupld context` support table, JSON, and NDJSON output through `--output`.
- Table output may truncate long values. JSON and NDJSON are safer for machine parsing.
- `.save` only works after the session has a file path.
- `.saveas <path.cupld>` persists an in-memory database.
- Exiting a dirty session prompts to save.

## Query Surface

Verified current statement families:

- Reads: `MATCH`, `WHERE`, `WITH`, `RETURN`, `ORDER BY`, `LIMIT`, `SHOW`, `EXPLAIN`
- Writes: `CREATE`, `MERGE`, `SET`, `REMOVE`, `DELETE`
- Transactions: `BEGIN`, `COMMIT`, `ROLLBACK`, `SAVEPOINT`, `ROLLBACK TO SAVEPOINT`, `RELEASE SAVEPOINT`

Verified current expression and syntax surface:

- bytes literals: `bytes'abc'`
- datetime literals: `datetime'2024-01-02T03:04:05Z'`
- list indexing: `n.tags[0]`
- dynamic map-key access: `{name: 'Ada'}['name']`
- edge-type alternation: `[e:KNOWS|MENTORS]`
- line comments `-- ...` and block comments `/* ... */`
- string and membership operators: `IN`, `CONTAINS`, `STARTS WITH`, `ENDS WITH`, `=~`
- path-valued results through `MERGE p = ... RETURN p` and `RETURN *`

Verified builtin functions:

- `append`
- `remove`
- `merge`
- `insert`
- `size`
- `type`
- `id`
- `labels`
- `edge_type`
- `has_label`
- `has_prop`
- `keys`
- `values`
- `contains`
- aggregate functions: `count`, `sum`, `avg`, `min`, `max`, `collect`

Current behavior notes:

- `type(x)` returns runtime types; use `edge_type(e)` for edge type names.
- `IN` works with lists, strings, and maps.
- `CONTAINS` supports strings, list membership, and map-key checks.
- Variable-length traversal must be bounded.
- `LIMIT` accepts a positive integer literal or parameter.
- Parser and runtime failures return stable machine-readable error codes. Invalid regex patterns return `regex_compile_error`.

## Schema And Index Surface

Verified current schema surface:

- Object DDL: `CREATE LABEL`, `CREATE EDGE TYPE`, `CREATE INDEX`, `CREATE CONSTRAINT`
- Replacement and evolution: `CREATE OR REPLACE`, `ALTER INDEX`, `ALTER CONSTRAINT`
- Conditional DDL: `IF NOT EXISTS`, `IF EXISTS`
- Inspection: `SHOW SCHEMA`, `SHOW INDEXES`, `SHOW CONSTRAINTS`
- Target filters: `SHOW INDEXES ON :Label`, `SHOW CONSTRAINTS ON [:TYPE]`
- Named parameters in DDL through `--params-json` or `--params-file`

Current schema and index behavior:

- Labels and edge types can carry descriptions and `SHOW SCHEMA` returns `kind`, `name`, `description`, and canonical `ddl`.
- Index kinds are equality by default, plus `KIND RANGE`, `KIND LIST`, and `KIND FULLTEXT`.
- `EXPLAIN` shows planner-visible index use for equality, range, list-membership, and full-text scans.
- Constraint types include `UNIQUE`, `REQUIRED`, `TYPE`, `ENDPOINTS`, and `MAX OUTGOING`.
- Edge endpoint constraints use syntax like `CREATE CONSTRAINT ON [:KNOWS] REQUIRE ENDPOINTS :Person -> :Person`.
- Edge cardinality constraints currently use `MAX OUTGOING`, for example `CREATE CONSTRAINT ON [:MENTORS] REQUIRE MAX OUTGOING 1`.
- Temporal validity fields are exposed on nodes and edges as `valid_from` and `valid_to`.

Examples:

```bash
cupld query --db default --params-json '{"label":"Person","property":"age"}' \
  "CREATE OR REPLACE INDEX idx_person_lookup ON :$label($property)"
cupld query --db default "ALTER INDEX idx_person_lookup SET STATUS INVALID"
```

```bash
cupld query --db default "
CREATE INDEX ON :Article(published) KIND RANGE;
CREATE INDEX ON :Article(tags) KIND LIST;
CREATE INDEX ON :Article(body) KIND FULLTEXT;
SHOW INDEXES ON :Article
"
```

## Markdown Memory And Watch Mode

Markdown root resolution order is:

1. explicit `--root`
2. `.cupld/config.toml`
3. the DB root set by `cupld source set-root`
4. `./.cupld/data`

Markdown behavior:

- `cupld query --with-markdown` overlays markdown into a temporary query session and does not persist imported notes.
- `cupld sync markdown` persists markdown documents and `:MD_LINKS_TO` edges from body links plus supported frontmatter relationship keys into the database.
- `cupld sync markdown --watch` performs the initial persisted sync, then keeps polling for changes.
- `--poll-ms` controls the poll interval.
- `--debounce-ms` controls the stable-change debounce window.
- `--batch-ms` bounds the coalescing window before a forced watched sync.
- `--idle-ms` exits watched sync after that long with no pending changes.
- `--max-runs` stops watched sync after that many sync runs, including the initial run.
- Watch mode is intended for bounded continuous sync and is exercised against duplicate events, partial writes, rename-save patterns, restart recovery, and malformed frontmatter.

Useful commands:

```bash
cupld query --db default --with-markdown \
  "MATCH (d:MarkdownDocument) RETURN d.`src.path`, d.`md.title` ORDER BY d.`src.path`"
```

```bash
cupld sync markdown --db default --watch --idle-ms 500 --max-runs 2
```

```bash
cupld source set-root --db default /absolute/path/to/notes
```

Markdown notes:

- Dotted keys must be backtick-quoted in queries, for example `d.\`src.path\`` and `d.\`md.title\``.
- `src.status` is `current` for present files and `missing` for tombstoned files.
- Title resolution is frontmatter `title`, then first heading, then filename stem.
- Supported top-level frontmatter outbound-link keys are `up`, `parent`, `related`, `next`, `previous`, `link`, and `links`.
- `md.links` contains the deduped union of body links plus supported frontmatter relationship targets in encounter order.
- Aliases are stored in `md.aliases` and participate in link resolution only as a fallback after exact path and stem matching.
- Ambiguous alias collisions create no markdown edge and do not fail sync.
- Fragments remain document-level: `other.md#section` resolves to `other.md`, while `#section` alone creates no edge.
- Malformed frontmatter falls back to body-only parsing.

## Automation Contracts

`query` and `context` expose stable machine contracts when `--output json` or `--output ndjson` is selected.

- `cupld query --output json` writes one JSON envelope to stdout with `ok`, `command`, `policy`, and `results`.
- `cupld query --output ndjson` writes one `query_meta` line, one `query_result` line per result set, and one `query_row` line per returned row.
- `cupld context --output json` writes one JSON envelope to stdout with `ok`, `command`, `policy`, `retrieval_usage`, `provenance`, and `items`.
- `cupld context --output ndjson` writes one `context_meta` line plus one `context_item` line per item.
- `query` and `context` failures in JSON or NDJSON mode write a machine error envelope to stderr with `ok: false`, `error.code`, and `error.message`.

Current automation controls:

- `CUPLD_QUERY_MAX_ROWS` sets the default `query --max-rows` cap.
- `CUPLD_NO_INSTALL_PROMPT=1` disables the interactive bootstrap prompt on REPL startup.
- Prefer explicit `ORDER BY` plus explicit `LIMIT` for deterministic context windows.
- Use named parameters with `--params-json` or `--params-file`.

## Further Docs

Use this guide for current day-to-day CLI and automation behavior.

- [`../README.md`](../README.md): public docs map and reading order
- [`./visualise.md`](./visualise.md): notes specific to the `--visualise` scene viewer
- [`../../README.md`](../../README.md): short project overview

If you are changing shipped CLI behavior, update this guide first.
