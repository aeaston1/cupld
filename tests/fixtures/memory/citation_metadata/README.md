# Citation Metadata Memory Fixture

This fixture protects the source metadata that makes markdown-derived memory auditable. It checks that synced documents expose enough stable metadata to cite where a fact came from.

The `auditability-metadata` case asserts:

- connector and kind identify the markdown source system;
- path, title, hash, status, and root are present;
- raw and body text are retained so later citation/audit commands can inspect source content.

The root value is intentionally asserted only as non-null through query filtering because it is an absolute sandbox path at runtime.
