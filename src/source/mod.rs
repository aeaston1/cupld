mod markdown;

pub use markdown::{
    MarkdownDocument, MarkdownSyncOptions, MarkdownSyncReport, MarkdownWatchOptions,
    MarkdownWatchReport, SourceError, configured_markdown_root, set_markdown_root,
    sync_markdown_root, sync_markdown_root_with_options, watch_markdown_root,
    watch_markdown_root_with_sync_options,
};
