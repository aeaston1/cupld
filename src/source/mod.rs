mod markdown;

pub use markdown::{
    MarkdownDocument, MarkdownSyncReport, MarkdownWatchOptions, MarkdownWatchReport, SourceError,
    configured_markdown_root, set_markdown_root, sync_markdown_root, watch_markdown_root,
};
