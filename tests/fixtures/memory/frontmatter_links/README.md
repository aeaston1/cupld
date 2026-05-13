# Frontmatter Links Memory Fixture

This fixture protects relationship links declared in markdown frontmatter. It exists so changes to frontmatter parsing, provenance, and edge relationship metadata have a small committed example to compare against.

The source note uses supported relationship fields: `parent`, `related`, `next`, and `links`. The expected snapshot checks that:

- relationship fields contribute to `md.links`;
- frontmatter-created edges carry `md.link_sources = ["frontmatter"]`;
- canonical relationship names are stored in `md.link_rels`;
- repeated body links merge body and frontmatter provenance onto one edge.
