mod markdown;

pub use markdown::{
    MarkdownDocument, MarkdownSyncReport, SourceError, configured_markdown_root, set_markdown_root,
    sync_markdown_root,
};
