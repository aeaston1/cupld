pub mod automation;
pub mod engine;
pub mod json;
pub mod mcp;
pub mod memory_report;
pub mod package;
pub mod query;
mod regex_lite;
pub mod runtime;
pub mod source;
pub mod storage;

pub use engine::{
    ConstraintRow, ConstraintType, CupldEngine, Edge, EdgeId, GraphError, GraphSnapshot,
    GraphStats, IndexKind, IndexRow, Node, NodeId, PropertyMap, PropertyType, SchemaCatalog,
    SchemaObjectOptions, SchemaRow, SchemaTarget, TargetKind, TxId, Value,
};
pub use memory_report::{MemoryMaintenanceCheck, MemoryMaintenanceReport, MemoryMaintenanceStatus};
pub use query::{
    BinaryOp, ConstraintSpec, Direction, EdgePattern, Expr, HopRange, NodePattern, OrderItem,
    ParamValue, Pattern, PatternSegment, PropertyTarget, Query, QueryError, RemoveTarget,
    ReturnItem, SchemaTargetExpr, SetAssignment, SetOperator, SetTarget, ShowKind, Statement,
    UnaryOp, WithClause, parse_script,
};
pub use runtime::{ExecutionError, QueryResult, RuntimeValue, Session, TransactionInfo};
pub use source::{
    MARKDOWN_DIRECTORY_LABEL, MD_IN_DIRECTORY, MD_PARENT_DIRECTORY, MarkdownDocument,
    MarkdownSyncOptions, MarkdownSyncReport, MarkdownWatchOptions, MarkdownWatchReport,
    SourceError, configured_markdown_root, set_markdown_root, sync_markdown_root,
    sync_markdown_root_with_options, watch_markdown_root, watch_markdown_root_with_sync_options,
};
pub use storage::{IntegrityReport, StorageError};
