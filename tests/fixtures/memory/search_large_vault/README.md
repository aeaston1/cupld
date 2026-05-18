This fixture generates 1,000 synthetic filler Markdown documents plus targeted search documents inside the eval sandbox.

Run only the search eval subset locally with:

```bash
mise exec -- cargo run --locked -- eval memory --case search_relevance --output table
mise exec -- cargo run --locked -- eval memory --case search_large_vault --output table
```

On this workspace, the two search fixtures completed in under 2 seconds total, well below the 30 second CI budget for the added search subset.
