---
name: cupld-md-memory
description: "Use when an agent has the `cupld` binary and needs local graph memory through the cupld MCP server, with CLI query/context as advanced fallback paths."
---

# cupld-md-memory

Use this skill when `cupld` is available and the task is to read, inspect, persist, or connect markdown notes as local memory.

Prefer the cupld MCP tools when the harness exposes them. Use CLI commands as the fallback when MCP is unavailable.

## Defaults

- Edit markdown with normal filesystem tools. `cupld` reads and syncs markdown; it does not write notes back for you.
- Root resolution order is: explicit `--root`, then `.cupld/config.toml`, then the DB root set by `cupld source set-root`, then `./.cupld/data/` under the current working directory.
- `cupld install` bootstraps `./.cupld/default.cupld` by default for local markdown memory work.
- `--db default` is an alias for `./.cupld/default.cupld`.
- `cupld mcp serve --db default` starts the local stdio MCP memory server.
- MCP reads are DB-backed only and do not run hidden markdown syncs.
- MCP `memory_sync` persists markdown into the DB. MCP `memory_add` writes markdown under the configured root and syncs it before success.
- MCP `--read-only` disables `memory_sync` and `memory_add`.
- `cupld install` and `source set-root` keep repo-local defaults in `.cupld/config.toml`.
- The skill install location (`.agents/skills`, `.claude/skills`, or a custom path) is separate from the DB path and markdown root. Installing the skill elsewhere does not move `./.cupld/default.cupld` or `./.cupld/data/`.
- MCP `memory_context` expands a search result URI/path or explicit node/path seed into bounded graph context without shelling out.
- `cupld query --with-md` overlays markdown into a temporary query session and does not persist the imported notes. Treat it as an advanced fallback, not the normal memory path.
- `cupld sync markdown` persists markdown documents and authored markdown link edges into the `.cupld` database.
- `cupld sync markdown --db default --include-fs-graph` opts in to persisted filesystem structure with `MarkdownDirectory`, `MD_IN_DIRECTORY`, and `MD_PARENT_DIRECTORY`.
- Use `cupld memory check`, `find-stale`, `find-orphans`, and `reindex` to inspect markdown-derived DB state. Use `cupld sync markdown` to refresh markdown-derived state after note edits.
- Memory maintenance statuses are `pass`, `warn`, and `fail`. `warn` still means the command completed; `cupld memory check --strict` exits 2 when the aggregate report is `warn`.
- Maintenance commands support `--output table|json|ndjson`. JSON emits one report envelope; NDJSON emits `memory_meta`, `memory_check`, and `memory_item` lines.
- `cupld memory repair` and `cupld memory citation-audit` are intentionally deferred in this round.
- `MD_LINKS_TO` remains authored-only; filesystem structure uses filesystem edge types and never pairwise `MD_SIBLING_OF` edges.
- Filesystem edges persist `md.edge_weight` for downstream context and retrieval work. Core ranking does not consume it in this workflow.
- `cupld sync markdown --watch` is operator-oriented. It performs the initial persisted sync, then keeps polling for changes with timing knobs.
- `cupld query --db ...` requires an existing database file. If the DB is missing, create it first with `cupld <path.cupld>`.
- `cupld query` and `cupld context` support `--output table|json|ndjson` without using the REPL.
- In JSON or NDJSON mode, `query` and `context` emit stable machine envelopes instead of raw table text.
- `CUPLD_QUERY_MAX_ROWS` sets the default non-interactive row cap. `CUPLD_NO_INSTALL_PROMPT=1` disables the one-time REPL bootstrap prompt.
- Dot-commands are REPL-only. They do not work with `cupld query`.

## Recommended Workflow

1. Bootstrap the local memory DB and MCP config. Use `--dry-run` first when writing harness config.
   ```bash
   cupld install --mcp --target codex --scope cwd --dry-run --db default
   ```
2. For manual MCP-capable harness config, point the server at the local DB.
   ```toml
   [mcp_servers.cupld-memory]
   command = "cupld"
   args = ["mcp", "serve", "--db", "default"]
   ```
3. Add persistent harness instructions.
   ```md
   ## Memory
   Use the cupld MCP server for durable local memory.
   Before non-trivial tasks, search memory when prior preferences, project decisions,
   architecture choices, recurring workflows, or local notes may matter.
   When the user explicitly asks you to remember something, call `memory_add`.
   Do not store secrets, credentials, tokens, private keys, or transient command output.
   ```
4. Start with `memory_health`. Check `db_path`, `markdown_root`, `markdown_root_exists`, `read_only`, `safe_for_writes`, `write_status`, and `db_last_tx_id`.
5. Use `memory_search` for retrieval, `memory_get` for exact note reads, and `memory_context` with a result `uri` when prompt assembly needs bounded graph context.
6. Call `memory_add` when the user asks you to remember something. Call `memory_sync` after direct markdown edits.
7. If the markdown root should stay stable across working directories, persist or update it once.
   ```bash
   cupld source set-root --db default /absolute/path/to/notes
   ```
8. Use CLI query/context only as advanced fallbacks when MCP is unavailable or the graph operation cannot be expressed by the MCP tools.
   ```bash
   cupld context --db default --path notes/example.md --depth 1 --max-nodes 25 --output json
   ```
9. For a one-off transient CLI root override, pass `--root` with `--with-md`.
   ```bash
   cupld query --db default --with-md --root /absolute/path/to/notes \
     "MATCH (d:MarkdownDocument) RETURN d.\`src.path\`, d.\`md.title\` ORDER BY d.\`src.path\`"
   ```
10. Persist markdown when you want later plain queries or MCP reads to see it.
   ```bash
   cupld sync markdown --db default
   ```
11. Persist filesystem structure when directory traversal matters.
   ```bash
   cupld sync markdown --db default --include-fs-graph
   ```
12. For bounded continuous persisted sync, use watch mode as an operator workflow after the initial sync.
   ```bash
   cupld sync markdown --db default --watch --idle-ms 500 --max-runs 2
   ```
13. Use maintenance commands before making assumptions about a DB.
   ```bash
   cupld memory check --db default
   cupld memory find-stale --db default --output table
   cupld memory find-orphans --db default --output ndjson
   cupld memory reindex --db default --output json
   ```
14. If maintenance reports stale markdown, refresh persisted state.
   ```bash
   cupld sync markdown --db default
   ```

## Markdown Authoring Convention

When creating or editing notes under the markdown root, follow this shape.

What markdown creates automatically:

- Each `.md` file becomes one `:MarkdownDocument` node.
- Body links plus supported frontmatter relationship fields create `:MD_LINKS_TO` edges.
- Frontmatter and body content populate document properties like `md.title`, `md.tags`, `md.aliases`, `md.headings`, `md.body`, `md.links`, and `md.frontmatter`.

What markdown does not create automatically:

- Arbitrary native nodes, native edge types, or native properties outside the markdown document model.
- To add native graph structure, use `cupld query` with `CREATE`, `MERGE`, `SET`, `REMOVE`, or `DELETE`.

Recommended note shape:

```md
---
title: Project Rollout
tags: [project, rollout]
aliases: [Rollout Plan]
up: Program Overview
related:
  - [[notes/schema-notes]]
  - launch-checklist.md
next: rollout-phase-2.md
status: active
owner: yourname
---

# Project Rollout

Short summary in 1-3 lines.

## Context
Key facts and background.

## Related
- [[notes/schema-notes]]
- [Launch checklist](../projects/launch-checklist.md)

## Next
- Follow-up item
- Another item
```

Authoring rules:

- Prefer one note per concept, project, person, meeting, or artifact.
- Use a stable file path. `src.path` is the durable document identity.
- Prefer a `title` field in frontmatter. If omitted, the first `# Heading` becomes the title.
- Put structured metadata in frontmatter and prose in the body.
- Use `tags` and `aliases` as lists of strings.
- Use `up`, `parent`, `related`, `next`, `previous`, `link`, or `links` only when you intend to create markdown relationships.
- Supported frontmatter relationship values are a single string or a list whose entries resolve to strings. Non-string structured values are ignored.
- Frontmatter relationship values may be plain targets like `Other Note` or `notes/other.md#section`, or Obsidian wikilinks like `[[Other Note]]`.
- Use wikilinks or normal markdown links in the body for note-to-note relationships.
- Inline hashtags are allowed and are added to `md.tags`.
- Keep frontmatter simple: spaces only, no tabs, simple scalars/lists/maps. Malformed frontmatter is ignored.

Mental model:

- file => node
- body link or supported frontmatter relationship => edge
- frontmatter/body => document properties
- graph facts beyond that => native `cupld query`

## Markdown Graph Model

- Markdown documents are nodes with label `:MarkdownDocument`.
- Body links and supported frontmatter relationship fields become `:MD_LINKS_TO` edges between markdown documents.
- `MD_LINKS_TO` is authored-only. Directory structure is not encoded as link edges.
- `cupld sync markdown --db default --include-fs-graph` also persists `:MarkdownDirectory` nodes.
- Documents connect to their containing directory with `:MD_IN_DIRECTORY`.
- Child directories connect to parent directories with `:MD_PARENT_DIRECTORY`.
- Filesystem sync does not create `:MD_SIBLING_OF` or pairwise sibling edges.
- Filesystem edges include `md.edge_weight` for downstream context and retrieval use. Core ranking does not consume it here.
- Dotted keys must be backtick-quoted in queries, for example `d.\`src.path\`` and `d.\`md.title\``.
- Useful document properties:
  - `src.root`
  - `src.path`
  - `src.hash`
  - `src.status`
  - `md.raw`
  - `md.body`
  - `md.title`
  - `md.has_frontmatter`
  - `md.tags`
  - `md.aliases`
  - `md.links`
  - `md.headings`
  - `md.frontmatter`
- `src.status` is `current` for present files and `missing` for tombstoned files.
- Title resolution is: frontmatter `title`, then first heading, then filename stem.
- Tags come from frontmatter plus inline hashtags.
- Supported top-level frontmatter outbound-link keys are `up`, `parent`, `related`, `next`, `previous`, `link`, and `links`.
- Canonical frontmatter relation names are `up`, `related`, `next`, `previous`, and `link`. `parent` normalizes to `up`, and `links` normalizes to `link`.
- `md.links` contains the deduped union of body links plus supported frontmatter relationship targets in encounter order.
- Aliases are stored in `md.aliases` and participate in link resolution only as a fallback after exact path and stem matching.
- Ambiguous alias collisions create no markdown edge and do not fail sync.
- Wikilinks and standard markdown links are both extracted.
- Link resolution handles relative paths, root-relative paths, bare stems, omitted `.md`, plain frontmatter targets, and Obsidian wikilink strings. It strips `#anchor` and `|alias` parts before resolution.
- Fragments remain document-level: `other.md#section` resolves to `other.md`, while `#section` alone creates no edge.
- Markdown edge metadata keeps `md.link_target` for compatibility and exposes aggregated lists on the edge: `md.link_targets`, `md.link_sources`, and `md.link_rels`.
- Malformed frontmatter falls back to body-only parsing.

## Query Surface

Use the normal `cupld` query language. The current useful surface here is:

- Reads: `MATCH`, `WHERE`, `WITH`, `RETURN`, `ORDER BY`, `LIMIT`, `SHOW`, `EXPLAIN`
- Writes: `CREATE`, `MERGE`, `SET`, `REMOVE`, `DELETE`
- Transactions: `BEGIN`, `COMMIT`, `ROLLBACK`, `SAVEPOINT`, `ROLLBACK TO SAVEPOINT`, `RELEASE SAVEPOINT`
- Useful expressions and helpers: `IN`, `CONTAINS`, `STARTS WITH`, `ENDS WITH`, `=~`, `edge_type`, `has_label`, `insert`, list indexing, dynamic map-key access, bytes literals, and datetime literals
- Machine automation: prefer `--output json` or `--output ndjson` for downstream parsing

Do not assume:

- automatic markdown write-back
- MCP reads can see unsynced raw markdown
- external concurrent DB writers are supported

For persisted markdown-heavy workloads, the same schema surface supports optional list and full-text indexes. Follow current shipped DDL syntax from the agent guide instead of inventing custom markdown-only conventions.

## Snippets

List markdown docs:

```bash
cupld query --db default --with-md \
  "MATCH (d:MarkdownDocument)
   RETURN d.\`src.path\`, d.\`md.title\`, d.\`src.status\`
   ORDER BY d.\`src.path\`"
```

Look up one note by path:

```bash
cupld query --db default --with-md \
  "MATCH (d:MarkdownDocument { \`src.path\`: 'projects/cupld-rollout.md' })
   RETURN d.\`md.title\`, d.\`md.tags\`, d.\`md.headings\`, d.\`md.body\`"
```

Traverse outlinks:

```bash
cupld query --db default --with-md \
  "MATCH (a:MarkdownDocument)-[e:MD_LINKS_TO]->(b:MarkdownDocument)
   RETURN a.\`src.path\`, e.\`md.link_target\`, e.\`md.link_sources\`, e.\`md.link_rels\`, b.\`src.path\`
   ORDER BY a.\`src.path\`, b.\`src.path\`"
```

Inspect frontmatter-driven relationships and aliases:

```bash
cupld query --db default --with-md \
  "MATCH (d:MarkdownDocument { \`src.path\`: 'projects/cupld-rollout.md' })
   RETURN d.\`md.title\`, d.\`md.aliases\`, d.\`md.links\`, d.\`md.frontmatter\`"
```

Traverse backlinks:

```bash
cupld query --db default --with-md \
  "MATCH (a:MarkdownDocument)-[:MD_LINKS_TO]->(b:MarkdownDocument { \`src.path\`: 'notes/schema-notes.md' })
   RETURN a.\`src.path\`
   ORDER BY a.\`src.path\`"
```

Document to directory traversal after opting into filesystem sync:

```bash
cupld sync markdown --db default --include-fs-graph
cupld query --db default --with-md \
  "MATCH (d:MarkdownDocument)-[:MD_IN_DIRECTORY]->(dir:MarkdownDirectory)
   RETURN d.\`src.path\`, dir.\`src.path\`
   ORDER BY d.\`src.path\`"
```

Child directory to parent directory traversal:

```bash
cupld query --db default --with-md \
  "MATCH (child:MarkdownDirectory)-[:MD_PARENT_DIRECTORY]->(parent:MarkdownDirectory)
   RETURN child.\`src.path\`, parent.\`src.path\`
   ORDER BY child.\`src.path\`"
```

Same-folder document discovery via two-hop traversal:

```bash
cupld query --db default --with-md \
  "MATCH (source:MarkdownDocument { \`src.path\`: 'projects/cupld/plan.md' })-[:MD_IN_DIRECTORY]->(dir:MarkdownDirectory)<-[:MD_IN_DIRECTORY]-(peer:MarkdownDocument)
   WHERE peer.\`src.path\` != source.\`src.path\`
   RETURN peer.\`src.path\`, peer.\`md.title\`
   ORDER BY peer.\`src.path\`"
```

Join native graph data to markdown notes:

```bash
cupld query --db default --with-md \
  "MATCH (topic)-[:REFERENCES]->(d:MarkdownDocument)
   RETURN topic.name, d.\`src.path\`, d.\`md.title\`, d.\`src.status\`"
```

Find tombstoned markdown docs:

```bash
cupld query --db default \
  "MATCH (d:MarkdownDocument)
   WHERE d.\`src.status\` = 'missing'
   RETURN d.\`src.path\`, d.\`md.title\`"
```

Create native graph edges pointing at markdown docs:

```bash
cat <<'EOF' | cupld query --db default --with-md
BEGIN;
MATCH (d:MarkdownDocument {`src.path`: 'projects/cupld-rollout.md'})
CREATE (:Topic {name: 'Rollout'})-[:REFERENCES]->(d);
COMMIT;
EOF
```

After `cupld sync markdown`, use the REPL for NDJSON output:

```bash
printf '.output ndjson\nMATCH (d:MarkdownDocument) RETURN d.`src.path`, d.`md.title` LIMIT 5\n.quit\n' \
  | cupld --db default
```

## REPL Commands

Useful dot-commands:

- `.output table|json|ndjson`
- `.open <path.cupld>`
- `.save`
- `.saveas <path.cupld>`
- `.schema`
- `.indexes`
- `.constraints`
- `.stats`
- `.transactions`
