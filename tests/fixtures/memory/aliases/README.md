# Aliases Memory Fixture

This fixture protects alias resolution for memory evals. It includes one unique alias that should resolve and one shared alias that must remain ambiguous.

The `alias-resolution` case asserts that:

- `[[Project Codename]]` resolves through the unique alias on `unique-target.md`;
- `[[Shared Alias]]` does not resolve because two current documents claim it;
- ambiguous alias documents still keep their `md.aliases` values for diagnostics.
