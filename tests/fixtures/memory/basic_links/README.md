# Basic Links Memory Fixture

This fixture protects the MVP markdown link extraction path. It keeps body wikilinks, standard markdown links, repeated targets, and unresolved links visible in a tiny vault that can be inspected without running any tooling.

The `body-link-surface` case asserts two things:

- `md.links` preserves the deduplicated body link targets found in the source note.
- `MD_LINKS_TO` edges are created only for targets that resolve to committed markdown documents.

`[[Missing Note]]` is intentionally unresolved. It should remain in `md.links` but should not produce an edge.
