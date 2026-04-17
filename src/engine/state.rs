use super::PropertyMap;
use super::schema::{ConstraintType, IndexStatus, SchemaTarget};

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct EngineState {
    pub next_tx_id: u64,
    pub next_node_id: u64,
    pub next_edge_id: u64,
    pub nodes: Vec<NodeState>,
    pub edges: Vec<EdgeState>,
    pub schema: SchemaState,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct NodeState {
    pub id: u64,
    pub labels: Vec<String>,
    pub properties: PropertyMap,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct EdgeState {
    pub id: u64,
    pub from: u64,
    pub to: u64,
    pub edge_type: String,
    pub properties: PropertyMap,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SchemaState {
    pub labels: Vec<String>,
    pub edge_types: Vec<String>,
    pub object_options: Vec<SchemaObjectState>,
    pub indexes: Vec<IndexState>,
    pub constraints: Vec<ConstraintState>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SchemaObjectState {
    pub target: SchemaTarget,
    pub description: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct IndexState {
    pub name: String,
    pub target: SchemaTarget,
    pub property: String,
    pub unique: bool,
    pub status: IndexStatus,
    pub owned_by_constraint: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ConstraintState {
    pub name: String,
    pub target: SchemaTarget,
    pub property: String,
    pub constraint_type: ConstraintType,
}
