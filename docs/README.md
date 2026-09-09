# cupld Docs

cupld is an in-process graph database with a CLI for people and their existing agents, plus an embedded Rust API. It has zero third-party crate dependencies. Markdown-backed agent memory is a supported specialized workflow.

## Start Here

1. Follow the [project quickstart](../README.md#quickstart) to create and query a graph. The [Rust example](../README.md#embed-in-rust) uses the same database library in process.
2. Use the [agent CLI guide](agents/README.md) for schema discovery, queries, writes, transactions, bounded context, and machine output.
3. Read the [persistence contract](agents/README.md#database-persistence) before relying on concurrent writes or format migration.

## Reference And Workflows

- [Agent CLI guide](agents/README.md): canonical contract for shipped CLI and automation behavior
- [Markdown memory](memory.md): supported memory setup, sync, MCP, and maintenance
- [Viewer](agents/visualise.md): interactive graph exploration
- [Core audit](core-audit.md): current capabilities, gaps, and database roadmap
- [Core benchmarks](core-benchmarks.md): reproducible performance and resource measurements
- [Reliability backlog](backlog.md): previously recorded reliability and retrieval issues

The resource goal is bounded RAM and eventual larger-than-RAM graph operation. The current engine loads its graph into memory; response budgets are not an execution-memory guarantee. Treat `internal/` notes as historical planning material and use the public references for current behavior.

## Authoring Rule

Keep public user-facing documentation under this tree. The root README should stay brief and point here. Label planned capabilities explicitly, and keep the general database contract separate from specialized memory semantics.
