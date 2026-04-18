# cupld `--visualise`

`--visualise` is the top-level scene-viewer entrypoint.

## Current behavior

- `cupld --visualise <path.cupld>` opens the interactive scene viewer for that database.
- `cupld --visualise --db <path.cupld|default>` and `cupld --db <path.cupld|default> --visualise` are equivalent forms.
- `--db default` is an alias for `./.cupld/default.cupld`.
- `--query` is only valid with `--visualise`.
- `--query` is intended to seed the viewer with one read-only `RETURN` query.

## Examples

```bash
cupld --visualise mydb.cupld
cupld --db default --visualise
cupld --visualise --db default --query 'MATCH (n:Person) RETURN n LIMIT 10'
```

## Notes

- Treat `--visualise` and `--query` as top-level options, not subcommand flags.
- Use the [`agent guide`](./README.md) for the general CLI workflow and command map.
- Use the [`docs index`](../README.md) for the broader documentation map.
- Keep this file focused on viewer-specific behavior only.
