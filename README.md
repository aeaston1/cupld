# cupld

Your markdown notes are a graph. `cupld` lets you query them like one.

A local-first graph database with a Cypher-like query language, a REPL, and a markdown sync layer that turns your vault into queryable nodes and edges — no server, no cloud, one portable file.

## Install

From source (requires Rust 1.85+):

```bash
cargo install --path .
```

## Quickstart

```bash
# In-memory REPL — kick the tyres
cupld

# Open or create a file-backed database
cupld state/dev.cupld

# One-shot query
cupld query --db state/dev.cupld 'MATCH (n) RETURN n LIMIT 10'

# Inspect schema, validate integrity
cupld schema --db state/dev.cupld
cupld check --db state/dev.cupld

# Visual graph viewer
cupld --visualise state/dev.cupld
```

## Markdown Memory

Point `cupld` at a folder of markdown files and they become graph nodes — frontmatter, tags, headings, wikilinks and all. Links between notes become edges. Query everything without touching the files.

```bash
# Bootstrap skill + local DB (interactive)
cupld install

# Wire into your agent (Claude, Codex, OpenCode)
cupld install --target claude --scope cwd --db .cupld/default.cupld --root notes
cupld install --target codex --scope home --db .cupld/default.cupld
cupld install --target opencode --scope home --db .cupld/default.cupld
```

Three commands cover the core workflow:

```bash
# Overlay markdown into a query session (transient — nothing persisted)
cupld query --db .cupld/default.cupld --with-markdown \
  "MATCH (d:MarkdownDocument) RETURN d.\`src.path\`, d.\`md.title\` ORDER BY d.\`src.path\`"

# Persist markdown into the graph
cupld sync markdown --db .cupld/default.cupld

# Pull a top-k summary for agent context windows
cupld context --db .cupld/default.cupld --top-k 20 --output json
```

Every markdown document is a `:MarkdownDocument` node. Every link is a `:MD_LINKS_TO` edge. Backlinks, tag queries, cross-note traversals — standard graph queries, nothing magic.

## Docs

- [Full docs index](./docs/README.md)
- [Agent guide](./docs/agents/README.md)
- [Visualiser](./docs/agents/visualise.md)
- [Security policy](./SECURITY.md)
