---
name: cupld-md-memory
description: "Use when an agent has the `cupld` binary and needs to treat a markdown vault as local graph memory: edit `.md` files under the markdown root, inspect them with `cupld query --with-markdown`, persist them with `cupld sync markdown`, or join markdown notes with native graph data."
---

# cupld-md-memory

Use this skill when `cupld` is available and the task is to read, inspect, persist, or connect markdown notes as local memory.

## Defaults

- Edit markdown with normal filesystem tools. `cupld` reads and syncs markdown; it does not write notes back for you.
- Root resolution order is: explicit `--root`, then `.cupld/config.toml`, then the DB root set by `cupld source set-root`, then `./.cupld/data/` under the current working directory.
- `cupld install` bootstraps `./.cupld/default.cupld` by default for local markdown memory work.
- `--db default` is an alias for `./.cupld/default.cupld`.
- `cupld install` and `source set-root` keep repo-local defaults in `.cupld/config.toml`.
- The skill install location (`.agents/skills`, `.claude/skills`, or a custom path) is separate from the DB path and markdown root. Installing the skill elsewhere does not move `./.cupld/default.cupld` or `./.cupld/data/`.
- `cupld query --with-markdown` overlays markdown into a temporary query session and does not persist the imported notes.
- `cupld sync markdown` persists markdown documents and markdown link edges into the `.cupld` database.
- `cupld query --db ...` requires an existing database file. If the DB is missing, create it first with `cupld <path.cupld>`.
- `cupld query` and `cupld context` support `--output table|json|ndjson` without using the REPL.
- In JSON or NDJSON mode, `query` and `context` emit stable machine envelopes instead of raw table text.
- `CUPLD_QUERY_MAX_ROWS` sets the default non-interactive row cap. `CUPLD_NO_INSTALL_PROMPT=1` disables the one-time REPL bootstrap prompt.
- Dot-commands are REPL-only. They do not work with `cupld query`.

## Recommended Workflow

1. Bootstrap the local memory DB and skill.
   ```bash
   cupld install
   ```
2. If the markdown root should stay stable across working directories, persist or update it once.
   ```bash
   cupld source set-root --db default /absolute/path/to/notes
   ```
3. For a one-off root override, pass `--root` directly.
   ```bash
   cupld query --db default --with-markdown --root /absolute/path/to/notes \
     "MATCH (d:MarkdownDocument) RETURN d.\`src.path\`, d.\`md.title\` ORDER BY d.\`src.path\`"
   ```
4. After editing notes, use overlay queries for transient reads.
   ```bash
   cupld query --db default --with-markdown \
     "MATCH (d:MarkdownDocument) RETURN d.\`src.path\`, d.\`md.title\` ORDER BY d.\`src.path\`"
   ```
5. Persist markdown when you want later plain queries to see it.
   ```bash
   cupld sync markdown --db default
   ```
6. Use maintenance commands before making assumptions about a DB.
   ```bash
   cupld check --db default
   cupld schema --db default
   cupld compact --db default
   ```

## Markdown Graph Model

- Markdown documents are nodes with label `:MarkdownDocument`.
- Markdown links become `:MD_LINKS_TO` edges between markdown documents.
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
- Aliases are stored in `md.aliases` but are not currently used for link resolution.
- Wikilinks and standard markdown links are both extracted.
- Link resolution handles relative paths, root-relative paths, bare stems, and omitted `.md`. It strips `#anchor` and `|alias` parts before resolution.
- Malformed frontmatter falls back to body-only parsing.

## Query Surface

Use the normal `cupld` query language. The current useful surface here is:

- Reads: `MATCH`, `WHERE`, `RETURN`, `ORDER BY`, `LIMIT`, `SHOW`, `EXPLAIN`
- Writes: `CREATE`, `SET`, `REMOVE`, `DELETE`
- Transactions: `BEGIN`, `COMMIT`, `ROLLBACK`, `SAVEPOINT`, `ROLLBACK TO SAVEPOINT`, `RELEASE SAVEPOINT`

Do not assume:

- full-text search
- automatic markdown write-back

## Snippets

List markdown docs:

```bash
cupld query --db default --with-markdown \
  "MATCH (d:MarkdownDocument)
   RETURN d.\`src.path\`, d.\`md.title\`, d.\`src.status\`
   ORDER BY d.\`src.path\`"
```

Look up one note by path:

```bash
cupld query --db default --with-markdown \
  "MATCH (d:MarkdownDocument { \`src.path\`: 'projects/cupld-rollout.md' })
   RETURN d.\`md.title\`, d.\`md.tags\`, d.\`md.headings\`, d.\`md.body\`"
```

Traverse outlinks:

```bash
cupld query --db default --with-markdown \
  "MATCH (a:MarkdownDocument)-[e:MD_LINKS_TO]->(b:MarkdownDocument)
   RETURN a.\`src.path\`, e.\`md.link_target\`, b.\`src.path\`
   ORDER BY a.\`src.path\`, b.\`src.path\`"
```

Traverse backlinks:

```bash
cupld query --db default --with-markdown \
  "MATCH (a:MarkdownDocument)-[:MD_LINKS_TO]->(b:MarkdownDocument { \`src.path\`: 'notes/schema-notes.md' })
   RETURN a.\`src.path\`
   ORDER BY a.\`src.path\`"
```

Join native graph data to markdown notes:

```bash
cupld query --db default --with-markdown \
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
cat <<'EOF' | cupld query --db default --with-markdown
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
