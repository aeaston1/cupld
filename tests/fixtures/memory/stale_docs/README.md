# Stale Documents Memory Fixture

This fixture protects the stale-document story used by the MVP memory eval suite. It includes explicit `vault-before` and `vault-after` directories so reviewers can inspect the file transition that creates changed, missing, and newly added notes.

The executable `markdown` vault mirrors `vault-after`. The `stale-transition-shape` case does not mutate the filesystem; instead it asserts that the committed after-state contains the paths expected after a refresh.

The intended transition is:

- `changed.md` exists before and after, with edited body text.
- `removed.md` exists only in `vault-before`.
- `added.md` exists only in `vault-after`.
- `stable.md` exists unchanged in both vaults.
