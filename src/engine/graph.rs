use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Arc;
use std::time::SystemTime;

use super::schema::generated_name;
use super::state::{EdgeState, EngineState, NodeState};
use super::{
    ConstraintRow, ConstraintType, EdgeId, IndexRow, NodeId, PropertyMap, PropertyType,
    SchemaCatalog, SchemaRow, SchemaTarget, TargetKind, TxId, Value,
};

#[derive(Clone, Debug, PartialEq)]
pub struct Node {
    id: NodeId,
    labels: BTreeSet<String>,
    properties: PropertyMap,
    valid_from: Option<SystemTime>,
    valid_to: Option<SystemTime>,
}

impl Node {
    fn new(
        id: NodeId,
        labels: BTreeSet<String>,
        properties: PropertyMap,
        valid_from: Option<SystemTime>,
        valid_to: Option<SystemTime>,
    ) -> Self {
        Self {
            id,
            labels,
            properties,
            valid_from,
            valid_to,
        }
    }

    pub fn id(&self) -> NodeId {
        self.id
    }

    pub fn labels(&self) -> &BTreeSet<String> {
        &self.labels
    }

    pub fn properties(&self) -> &PropertyMap {
        &self.properties
    }

    pub fn property(&self, key: &str) -> Option<&Value> {
        self.properties.get(key)
    }

    pub fn valid_from(&self) -> Option<SystemTime> {
        self.valid_from
    }

    pub fn valid_to(&self) -> Option<SystemTime> {
        self.valid_to
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Edge {
    id: EdgeId,
    from: NodeId,
    to: NodeId,
    edge_type: String,
    properties: PropertyMap,
    valid_from: Option<SystemTime>,
    valid_to: Option<SystemTime>,
}

impl Edge {
    fn new(
        id: EdgeId,
        from: NodeId,
        to: NodeId,
        edge_type: String,
        properties: PropertyMap,
        valid_from: Option<SystemTime>,
        valid_to: Option<SystemTime>,
    ) -> Self {
        Self {
            id,
            from,
            to,
            edge_type,
            properties,
            valid_from,
            valid_to,
        }
    }

    pub fn id(&self) -> EdgeId {
        self.id
    }

    pub fn from(&self) -> NodeId {
        self.from
    }

    pub fn to(&self) -> NodeId {
        self.to
    }

    pub fn edge_type(&self) -> &str {
        &self.edge_type
    }

    pub fn properties(&self) -> &PropertyMap {
        &self.properties
    }

    pub fn property(&self, key: &str) -> Option<&Value> {
        self.properties.get(key)
    }

    pub fn valid_from(&self) -> Option<SystemTime> {
        self.valid_from
    }

    pub fn valid_to(&self) -> Option<SystemTime> {
        self.valid_to
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GraphStats {
    pub node_count: usize,
    pub edge_count: usize,
    pub label_count: usize,
    pub edge_type_count: usize,
    pub index_count: usize,
    pub constraint_count: usize,
    pub last_tx_id: u64,
    pub wal_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GraphError {
    EmptyLabel,
    EmptyEdgeType,
    EmptyPropertyName,
    NodeNotFound(NodeId),
    EdgeNotFound(EdgeId),
    MissingEndpoint(NodeId),
    MissingSchemaTarget {
        kind: TargetKind,
        name: String,
    },
    DuplicateEdge {
        from: NodeId,
        edge_type: String,
        to: NodeId,
    },
    LabelExists(String),
    EdgeTypeExists(String),
    LabelNotFound(String),
    EdgeTypeNotFound(String),
    SchemaObjectExists(String),
    SchemaObjectNotFound(String),
    LabelInUse(String),
    EdgeTypeInUse(String),
    InvalidSchemaOperation(String),
    BackingIndexOwned {
        index: String,
        constraint: String,
    },
    ConstraintViolation {
        code: &'static str,
        name: String,
        detail: String,
    },
}

impl GraphError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::EmptyLabel => "empty_label",
            Self::EmptyEdgeType => "empty_edge_type",
            Self::EmptyPropertyName => "empty_property_name",
            Self::NodeNotFound(_) => "node_not_found",
            Self::EdgeNotFound(_) => "edge_not_found",
            Self::MissingEndpoint(_) => "missing_endpoint",
            Self::MissingSchemaTarget { .. } => "missing_schema_target",
            Self::DuplicateEdge { .. } => "duplicate_edge",
            Self::LabelExists(_) => "label_exists",
            Self::EdgeTypeExists(_) => "edge_type_exists",
            Self::LabelNotFound(_) => "label_not_found",
            Self::EdgeTypeNotFound(_) => "edge_type_not_found",
            Self::SchemaObjectExists(_) => "schema_object_exists",
            Self::SchemaObjectNotFound(_) => "schema_object_not_found",
            Self::LabelInUse(_) => "label_in_use",
            Self::EdgeTypeInUse(_) => "edge_type_in_use",
            Self::InvalidSchemaOperation(_) => "invalid_schema_operation",
            Self::BackingIndexOwned { .. } => "backing_index_owned",
            Self::ConstraintViolation { code, .. } => code,
        }
    }
}

impl fmt::Display for GraphError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyLabel => write!(f, "node labels must not be empty"),
            Self::EmptyEdgeType => write!(f, "edge types must not be empty"),
            Self::EmptyPropertyName => write!(f, "property names must not be empty"),
            Self::NodeNotFound(id) => write!(f, "node {id} does not exist"),
            Self::EdgeNotFound(id) => write!(f, "edge {id} does not exist"),
            Self::MissingEndpoint(id) => write!(f, "edge endpoint {id} does not exist"),
            Self::MissingSchemaTarget { kind, name } => {
                write!(f, "{} {} does not exist", kind.as_str(), name)
            }
            Self::DuplicateEdge {
                from,
                edge_type,
                to,
            } => write!(f, "edge ({from})-[:{edge_type}]->({to}) already exists"),
            Self::LabelExists(name) => write!(f, "label {} already exists", name),
            Self::EdgeTypeExists(name) => write!(f, "edge type {} already exists", name),
            Self::LabelNotFound(name) => write!(f, "label {} does not exist", name),
            Self::EdgeTypeNotFound(name) => write!(f, "edge type {} does not exist", name),
            Self::SchemaObjectExists(name) => write!(f, "schema object {} already exists", name),
            Self::SchemaObjectNotFound(name) => {
                write!(f, "schema object {} does not exist", name)
            }
            Self::LabelInUse(name) => write!(f, "label {} is still in use", name),
            Self::EdgeTypeInUse(name) => write!(f, "edge type {} is still in use", name),
            Self::InvalidSchemaOperation(detail) => write!(f, "{detail}"),
            Self::BackingIndexOwned { index, constraint } => {
                write!(f, "index {} is owned by constraint {}", index, constraint)
            }
            Self::ConstraintViolation { detail, .. } => write!(f, "{}", detail),
        }
    }
}

impl std::error::Error for GraphError {}

#[derive(Clone, Copy)]
enum SchemaObjectKind {
    Label,
    EdgeType,
}

impl SchemaObjectKind {
    fn empty_error(self) -> GraphError {
        match self {
            Self::Label => GraphError::EmptyLabel,
            Self::EdgeType => GraphError::EmptyEdgeType,
        }
    }

    fn exists_error(self, name: String) -> GraphError {
        match self {
            Self::Label => GraphError::LabelExists(name),
            Self::EdgeType => GraphError::EdgeTypeExists(name),
        }
    }

    fn not_found_error(self, name: String) -> GraphError {
        match self {
            Self::Label => GraphError::LabelNotFound(name),
            Self::EdgeType => GraphError::EdgeTypeNotFound(name),
        }
    }

    fn in_use_error(self, name: String) -> GraphError {
        match self {
            Self::Label => GraphError::LabelInUse(name),
            Self::EdgeType => GraphError::EdgeTypeInUse(name),
        }
    }
}

#[derive(Clone, Debug)]
pub struct GraphSnapshot {
    tx_id: TxId,
    data: Arc<GraphData>,
}

impl GraphSnapshot {
    fn new(tx_id: TxId, data: GraphData) -> Self {
        Self {
            tx_id,
            data: Arc::new(data),
        }
    }

    pub fn tx_id(&self) -> TxId {
        self.tx_id
    }

    pub fn node(&self, id: NodeId) -> Option<&Node> {
        self.data.nodes.get(&id)
    }

    pub fn nodes(&self) -> impl Iterator<Item = &Node> {
        self.data.nodes.values()
    }

    pub fn edge(&self, id: EdgeId) -> Option<&Edge> {
        self.data.edges.get(&id)
    }

    pub fn edges(&self) -> impl Iterator<Item = &Edge> {
        self.data.edges.values()
    }

    pub fn outgoing_edge_ids(&self, node_id: NodeId) -> Vec<EdgeId> {
        self.data
            .outgoing
            .get(&node_id)
            .map(|edge_ids| edge_ids.iter().copied().collect())
            .unwrap_or_default()
    }

    pub fn incoming_edge_ids(&self, node_id: NodeId) -> Vec<EdgeId> {
        self.data
            .incoming
            .get(&node_id)
            .map(|edge_ids| edge_ids.iter().copied().collect())
            .unwrap_or_default()
    }

    pub fn show_schema(&self) -> Vec<SchemaRow> {
        self.data.schema.show_schema_rows()
    }

    pub fn show_indexes(&self, target_filter: Option<&SchemaTarget>) -> Vec<IndexRow> {
        self.data.schema.show_index_rows(target_filter)
    }

    pub fn show_constraints(&self, target_filter: Option<&SchemaTarget>) -> Vec<ConstraintRow> {
        self.data.schema.show_constraint_rows(target_filter)
    }

    pub fn stats(&self) -> GraphStats {
        self.data.stats(self.tx_id.get())
    }
}

#[derive(Clone, Debug)]
pub struct CupldEngine {
    next_tx_id: u64,
    working: GraphData,
    committed: GraphSnapshot,
}

impl Default for CupldEngine {
    fn default() -> Self {
        let working = GraphData::default();
        let committed = GraphSnapshot::new(TxId::new(0), working.clone());

        Self {
            next_tx_id: 0,
            working,
            committed,
        }
    }
}

impl CupldEngine {
    pub(crate) fn to_state(&self) -> EngineState {
        EngineState {
            next_tx_id: self.next_tx_id,
            next_node_id: self.working.next_node_id,
            next_edge_id: self.working.next_edge_id,
            nodes: self
                .working
                .nodes
                .values()
                .map(|node| NodeState {
                    id: node.id().get(),
                    labels: node.labels().iter().cloned().collect(),
                    properties: node.properties().clone(),
                    valid_from: node.valid_from(),
                    valid_to: node.valid_to(),
                })
                .collect(),
            edges: self
                .working
                .edges
                .values()
                .map(|edge| EdgeState {
                    id: edge.id().get(),
                    from: edge.from().get(),
                    to: edge.to().get(),
                    edge_type: edge.edge_type().to_owned(),
                    properties: edge.properties().clone(),
                    valid_from: edge.valid_from(),
                    valid_to: edge.valid_to(),
                })
                .collect(),
            schema: self.working.schema.to_state(),
        }
    }

    pub(crate) fn from_state(state: EngineState) -> Result<Self, GraphError> {
        let mut data = GraphData {
            next_node_id: state.next_node_id,
            next_edge_id: state.next_edge_id,
            nodes: BTreeMap::new(),
            edges: BTreeMap::new(),
            outgoing: BTreeMap::new(),
            incoming: BTreeMap::new(),
            edge_lookup: BTreeMap::new(),
            schema: SchemaCatalog::from_state(state.schema),
        };

        for node in state.nodes {
            let node_id = NodeId::new(node.id);
            let labels = node.labels.into_iter().collect::<BTreeSet<_>>();
            data.outgoing.entry(node_id).or_default();
            data.incoming.entry(node_id).or_default();
            data.nodes.insert(
                node_id,
                Node::new(
                    node_id,
                    labels,
                    node.properties,
                    node.valid_from,
                    node.valid_to,
                ),
            );
        }
        for edge in state.edges {
            let edge_id = EdgeId::new(edge.id);
            let from = NodeId::new(edge.from);
            let to = NodeId::new(edge.to);
            if !data.nodes.contains_key(&from) || !data.nodes.contains_key(&to) {
                return Err(GraphError::MissingEndpoint(
                    if !data.nodes.contains_key(&from) {
                        from
                    } else {
                        to
                    },
                ));
            }
            data.edges.insert(
                edge_id,
                Edge::new(
                    edge_id,
                    from,
                    to,
                    edge.edge_type.clone(),
                    edge.properties,
                    edge.valid_from,
                    edge.valid_to,
                ),
            );
            data.outgoing.entry(from).or_default().insert(edge_id);
            data.incoming.entry(to).or_default().insert(edge_id);
            data.edge_lookup
                .insert(EdgeKey::new(from, edge.edge_type, to), edge_id);
        }
        validate_schema_rules(&data.schema, &data)?;

        let committed = GraphSnapshot::new(TxId::new(state.next_tx_id), data.clone());
        Ok(Self {
            next_tx_id: state.next_tx_id,
            working: data,
            committed,
        })
    }

    pub fn create_node<I, S>(
        &mut self,
        labels: I,
        properties: PropertyMap,
    ) -> Result<NodeId, GraphError>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let mut label_set = BTreeSet::new();
        for label in labels {
            let label = label.into();
            if label.is_empty() {
                return Err(GraphError::EmptyLabel);
            }

            self.working.schema.ensure_label(label.clone());
            label_set.insert(label);
        }

        let node_id = self.working.allocate_node_id();
        let node = Node::new(
            node_id,
            label_set,
            properties,
            Some(SystemTime::now()),
            None,
        );
        self.working.nodes.insert(node_id, node);
        self.working.outgoing.entry(node_id).or_default();
        self.working.incoming.entry(node_id).or_default();
        Ok(node_id)
    }

    pub fn create_edge<S>(
        &mut self,
        from: NodeId,
        to: NodeId,
        edge_type: S,
        properties: PropertyMap,
    ) -> Result<EdgeId, GraphError>
    where
        S: Into<String>,
    {
        if !self.working.nodes.contains_key(&from) {
            return Err(GraphError::MissingEndpoint(from));
        }
        if !self.working.nodes.contains_key(&to) {
            return Err(GraphError::MissingEndpoint(to));
        }

        let edge_type = edge_type.into();
        if edge_type.is_empty() {
            return Err(GraphError::EmptyEdgeType);
        }

        let edge_key = EdgeKey::new(from, edge_type.clone(), to);
        if self.working.edge_lookup.contains_key(&edge_key) {
            return Err(GraphError::DuplicateEdge {
                from,
                edge_type,
                to,
            });
        }

        self.working.schema.ensure_edge_type(edge_type.clone());
        let edge_id = self.working.allocate_edge_id();
        let edge = Edge::new(
            edge_id,
            from,
            to,
            edge_key.edge_type.clone(),
            properties,
            Some(SystemTime::now()),
            None,
        );
        self.working.edges.insert(edge_id, edge);
        self.working
            .outgoing
            .entry(from)
            .or_default()
            .insert(edge_id);
        self.working.incoming.entry(to).or_default().insert(edge_id);
        self.working.edge_lookup.insert(edge_key, edge_id);
        Ok(edge_id)
    }

    pub fn create_label(
        &mut self,
        name: &str,
        description: Option<String>,
        if_not_exists: bool,
        or_replace: bool,
    ) -> Result<(), GraphError> {
        self.create_schema_object(
            SchemaObjectKind::Label,
            name,
            description,
            if_not_exists,
            or_replace,
        )
    }

    pub fn create_edge_type(
        &mut self,
        name: &str,
        description: Option<String>,
        if_not_exists: bool,
        or_replace: bool,
    ) -> Result<(), GraphError> {
        self.create_schema_object(
            SchemaObjectKind::EdgeType,
            name,
            description,
            if_not_exists,
            or_replace,
        )
    }

    pub fn drop_label(&mut self, name: &str, if_exists: bool) -> Result<(), GraphError> {
        self.drop_schema_object(SchemaObjectKind::Label, name, if_exists)
    }

    pub fn drop_edge_type(&mut self, name: &str, if_exists: bool) -> Result<(), GraphError> {
        self.drop_schema_object(SchemaObjectKind::EdgeType, name, if_exists)
    }

    pub fn create_index(
        &mut self,
        name: Option<&str>,
        target: SchemaTarget,
        property: &str,
        kind: super::IndexKind,
        if_not_exists: bool,
        or_replace: bool,
    ) -> Result<String, GraphError> {
        ensure_property_name(property)?;
        ensure_target_exists(&self.working.schema, &target)?;

        let requested_name = name.map(ToOwned::to_owned).unwrap_or_else(|| {
            generated_name(
                "idx",
                target.kind(),
                target.name(),
                property,
                Some(kind.as_str()),
            )
        });

        if self.working.schema.object_exists(&requested_name) {
            if if_not_exists {
                return Ok(requested_name);
            }
            if !or_replace {
                return Err(GraphError::SchemaObjectExists(requested_name));
            }
            if let Some(existing) = self.working.schema.index(&requested_name)
                && existing.owned_by_constraint().is_some()
            {
                return Err(GraphError::BackingIndexOwned {
                    index: requested_name,
                    constraint: existing.owned_by_constraint().unwrap().to_owned(),
                });
            }
        }

        let mut schema = self.working.schema.clone();
        if or_replace {
            schema.drop_index(&requested_name);
        }
        let created_name = schema.create_index(
            Some(requested_name.clone()),
            target,
            property.to_owned(),
            kind,
            false,
            None,
        );
        validate_schema_rules(&schema, &self.working)?;
        self.working.schema = schema;
        Ok(created_name)
    }

    pub fn alter_index_status(
        &mut self,
        name: &str,
        status: super::IndexStatus,
    ) -> Result<(), GraphError> {
        if !self.working.schema.set_index_status(name, status) {
            return Err(GraphError::SchemaObjectNotFound(name.to_owned()));
        }
        Ok(())
    }

    pub fn drop_index(&mut self, name: &str, if_exists: bool) -> Result<(), GraphError> {
        let Some(index) = self.working.schema.index(name).cloned() else {
            if if_exists {
                return Ok(());
            }
            return Err(GraphError::SchemaObjectNotFound(name.to_owned()));
        };
        if let Some(constraint) = index.owned_by_constraint() {
            return Err(GraphError::BackingIndexOwned {
                index: name.to_owned(),
                constraint: constraint.to_owned(),
            });
        }

        self.working.schema.drop_index(name);
        Ok(())
    }

    pub fn create_constraint(
        &mut self,
        name: Option<&str>,
        target: SchemaTarget,
        property: &str,
        constraint_type: ConstraintType,
        if_not_exists: bool,
        or_replace: bool,
    ) -> Result<String, GraphError> {
        ensure_target_exists(&self.working.schema, &target)?;
        if matches!(
            constraint_type,
            ConstraintType::Unique | ConstraintType::Required | ConstraintType::Type(_)
        ) {
            ensure_property_name(property)?;
        } else if target.kind() != TargetKind::EdgeType {
            return Err(GraphError::InvalidSchemaOperation(
                "edge endpoint and cardinality constraints require an edge type target".to_owned(),
            ));
        }

        let requested_name = name.map(ToOwned::to_owned).unwrap_or_else(|| {
            generated_name(
                "constraint",
                target.kind(),
                target.name(),
                constraint_subject_key(property, &constraint_type),
                Some(&constraint_type.generated_suffix()),
            )
        });
        if self.working.schema.object_exists(&requested_name) {
            if if_not_exists {
                return Ok(requested_name);
            }
            if !or_replace {
                return Err(GraphError::SchemaObjectExists(requested_name));
            }
        }

        let mut schema = self.working.schema.clone();
        if or_replace {
            schema.drop_constraint(&requested_name);
            let backing_indexes = schema
                .indexes()
                .filter(|index| index.owned_by_constraint() == Some(&requested_name))
                .map(|index| index.name().to_owned())
                .collect::<Vec<_>>();
            for index_name in backing_indexes {
                schema.drop_index(&index_name);
            }
        }
        let created_name = schema.create_constraint(
            Some(requested_name.clone()),
            target.clone(),
            property.to_owned(),
            constraint_type.clone(),
        );

        if matches!(constraint_type, ConstraintType::Unique) {
            let backing_name = generated_name(
                "idx",
                target.kind(),
                target.name(),
                property,
                Some("unique"),
            );
            if schema.object_exists(&backing_name) && !or_replace {
                return Err(GraphError::SchemaObjectExists(backing_name));
            }
            if or_replace {
                schema.drop_index(&backing_name);
            }
            schema.create_index(
                Some(backing_name),
                target,
                property.to_owned(),
                super::IndexKind::Equality,
                true,
                Some(created_name.clone()),
            );
        }

        validate_schema_rules(&schema, &self.working)?;
        self.working.schema = schema;
        Ok(created_name)
    }

    pub fn rename_constraint(&mut self, name: &str, rename_to: &str) -> Result<(), GraphError> {
        if self.working.schema.object_exists(rename_to) {
            return Err(GraphError::SchemaObjectExists(rename_to.to_owned()));
        }
        if !self.working.schema.rename_constraint(name, rename_to) {
            return Err(GraphError::SchemaObjectNotFound(name.to_owned()));
        }
        Ok(())
    }

    pub fn drop_constraint(&mut self, name: &str, if_exists: bool) -> Result<(), GraphError> {
        let Some(_) = self.working.schema.constraint(name) else {
            if if_exists {
                return Ok(());
            }
            return Err(GraphError::SchemaObjectNotFound(name.to_owned()));
        };
        self.working.schema.drop_constraint(name);

        let backing_indexes = self
            .working
            .schema
            .indexes()
            .filter(|index| index.owned_by_constraint() == Some(name))
            .map(|index| index.name().to_owned())
            .collect::<Vec<_>>();
        for index_name in backing_indexes {
            self.working.schema.drop_index(&index_name);
        }

        validate_schema_rules(&self.working.schema, &self.working)?;
        Ok(())
    }

    pub fn node(&self, id: NodeId) -> Option<&Node> {
        self.working.nodes.get(&id)
    }

    pub fn edge(&self, id: EdgeId) -> Option<&Edge> {
        self.working.edges.get(&id)
    }

    pub fn nodes(&self) -> impl Iterator<Item = &Node> {
        self.working.nodes.values()
    }

    pub fn edges(&self) -> impl Iterator<Item = &Edge> {
        self.working.edges.values()
    }

    pub fn outgoing_edge_ids(&self, node_id: NodeId) -> Vec<EdgeId> {
        self.working
            .outgoing
            .get(&node_id)
            .map(|edge_ids| edge_ids.iter().copied().collect())
            .unwrap_or_default()
    }

    pub fn incoming_edge_ids(&self, node_id: NodeId) -> Vec<EdgeId> {
        self.working
            .incoming
            .get(&node_id)
            .map(|edge_ids| edge_ids.iter().copied().collect())
            .unwrap_or_default()
    }

    pub fn show_schema(&self) -> Vec<SchemaRow> {
        self.working.schema.show_schema_rows()
    }

    pub fn show_indexes(&self, target_filter: Option<&SchemaTarget>) -> Vec<IndexRow> {
        self.working.schema.show_index_rows(target_filter)
    }

    pub fn show_constraints(&self, target_filter: Option<&SchemaTarget>) -> Vec<ConstraintRow> {
        self.working.schema.show_constraint_rows(target_filter)
    }

    pub fn schema_catalog(&self) -> &SchemaCatalog {
        &self.working.schema
    }

    pub fn set_node_property<K>(
        &mut self,
        node_id: NodeId,
        key: K,
        value: Value,
    ) -> Result<Option<Value>, GraphError>
    where
        K: Into<String>,
    {
        let key = key.into();
        ensure_property_name(&key)?;
        let node = self
            .working
            .nodes
            .get_mut(&node_id)
            .ok_or(GraphError::NodeNotFound(node_id))?;
        let previous = node.properties.insert(key, value);
        Ok(previous)
    }

    pub fn remove_node_property(
        &mut self,
        node_id: NodeId,
        key: &str,
    ) -> Result<Option<Value>, GraphError> {
        ensure_property_name(key)?;
        let node = self
            .working
            .nodes
            .get_mut(&node_id)
            .ok_or(GraphError::NodeNotFound(node_id))?;
        let previous = node.properties.remove(key);
        Ok(previous)
    }

    pub fn replace_node_properties(
        &mut self,
        node_id: NodeId,
        properties: PropertyMap,
    ) -> Result<PropertyMap, GraphError> {
        let node = self
            .working
            .nodes
            .get_mut(&node_id)
            .ok_or(GraphError::NodeNotFound(node_id))?;
        let previous = std::mem::replace(&mut node.properties, properties);
        Ok(previous)
    }

    pub fn remove_node_label(&mut self, node_id: NodeId, label: &str) -> Result<bool, GraphError> {
        let removed = {
            let node = self
                .working
                .nodes
                .get_mut(&node_id)
                .ok_or(GraphError::NodeNotFound(node_id))?;
            node.labels.remove(label)
        };
        validate_schema_rules(&self.working.schema, &self.working)?;
        Ok(removed)
    }

    pub fn set_edge_property<K>(
        &mut self,
        edge_id: EdgeId,
        key: K,
        value: Value,
    ) -> Result<Option<Value>, GraphError>
    where
        K: Into<String>,
    {
        let key = key.into();
        ensure_property_name(&key)?;
        let edge = self
            .working
            .edges
            .get_mut(&edge_id)
            .ok_or(GraphError::EdgeNotFound(edge_id))?;
        let previous = edge.properties.insert(key, value);
        Ok(previous)
    }

    pub fn remove_edge_property(
        &mut self,
        edge_id: EdgeId,
        key: &str,
    ) -> Result<Option<Value>, GraphError> {
        ensure_property_name(key)?;
        let edge = self
            .working
            .edges
            .get_mut(&edge_id)
            .ok_or(GraphError::EdgeNotFound(edge_id))?;
        let previous = edge.properties.remove(key);
        Ok(previous)
    }

    pub fn replace_edge_properties(
        &mut self,
        edge_id: EdgeId,
        properties: PropertyMap,
    ) -> Result<PropertyMap, GraphError> {
        let edge = self
            .working
            .edges
            .get_mut(&edge_id)
            .ok_or(GraphError::EdgeNotFound(edge_id))?;
        let previous = std::mem::replace(&mut edge.properties, properties);
        Ok(previous)
    }

    pub fn delete_edge(&mut self, edge_id: EdgeId) -> Result<Edge, GraphError> {
        let edge = self
            .working
            .edges
            .remove(&edge_id)
            .ok_or(GraphError::EdgeNotFound(edge_id))?;

        if let Some(outgoing) = self.working.outgoing.get_mut(&edge.from) {
            outgoing.remove(&edge_id);
        }
        if let Some(incoming) = self.working.incoming.get_mut(&edge.to) {
            incoming.remove(&edge_id);
        }

        self.working
            .edge_lookup
            .remove(&EdgeKey::new(edge.from, edge.edge_type.clone(), edge.to));
        Ok(edge)
    }

    pub fn delete_node(&mut self, node_id: NodeId) -> Result<Node, GraphError> {
        if !self.working.nodes.contains_key(&node_id) {
            return Err(GraphError::NodeNotFound(node_id));
        }

        let mut edge_ids = self
            .working
            .outgoing
            .get(&node_id)
            .cloned()
            .unwrap_or_default();
        edge_ids.extend(
            self.working
                .incoming
                .get(&node_id)
                .cloned()
                .unwrap_or_default(),
        );
        for edge_id in edge_ids {
            self.delete_edge(edge_id)?;
        }

        self.working.outgoing.remove(&node_id);
        self.working.incoming.remove(&node_id);
        let node = self
            .working
            .nodes
            .remove(&node_id)
            .ok_or(GraphError::NodeNotFound(node_id))?;
        Ok(node)
    }

    pub fn commit(&mut self) -> Result<TxId, GraphError> {
        validate_schema_rules(&self.working.schema, &self.working)?;
        self.next_tx_id += 1;
        let tx_id = TxId::new(self.next_tx_id);
        self.committed = GraphSnapshot::new(tx_id, self.working.clone());
        Ok(tx_id)
    }

    pub fn snapshot(&self) -> GraphSnapshot {
        self.committed.clone()
    }

    pub fn stats(&self) -> GraphStats {
        self.working.stats(self.committed.tx_id().get())
    }

    fn create_schema_object(
        &mut self,
        kind: SchemaObjectKind,
        name: &str,
        description: Option<String>,
        if_not_exists: bool,
        or_replace: bool,
    ) -> Result<(), GraphError> {
        if name.is_empty() {
            return Err(kind.empty_error());
        }
        let exists = match kind {
            SchemaObjectKind::Label => self.working.schema.has_label(name),
            SchemaObjectKind::EdgeType => self.working.schema.has_edge_type(name),
        };
        if exists {
            if if_not_exists {
                return Ok(());
            }
            if !or_replace {
                return Err(kind.exists_error(name.to_owned()));
            }
            match kind {
                SchemaObjectKind::Label => {
                    self.working.schema.create_label(name, description);
                }
                SchemaObjectKind::EdgeType => {
                    self.working.schema.create_edge_type(name, description);
                }
            }
            return Ok(());
        }

        match kind {
            SchemaObjectKind::Label => {
                self.working.schema.create_label(name, description);
            }
            SchemaObjectKind::EdgeType => {
                self.working.schema.create_edge_type(name, description);
            }
        }
        Ok(())
    }

    fn drop_schema_object(
        &mut self,
        kind: SchemaObjectKind,
        name: &str,
        if_exists: bool,
    ) -> Result<(), GraphError> {
        let exists = match kind {
            SchemaObjectKind::Label => self.working.schema.has_label(name),
            SchemaObjectKind::EdgeType => self.working.schema.has_edge_type(name),
        };
        if !exists {
            if if_exists {
                return Ok(());
            }
            return Err(kind.not_found_error(name.to_owned()));
        }

        let target = match kind {
            SchemaObjectKind::Label => SchemaTarget::label(name),
            SchemaObjectKind::EdgeType => SchemaTarget::edge_type(name),
        };
        let in_use = match kind {
            SchemaObjectKind::Label => {
                self.working.schema.depends_on_target(&target) || self.working.uses_label(name)
            }
            SchemaObjectKind::EdgeType => {
                self.working.schema.depends_on_target(&target) || self.working.uses_edge_type(name)
            }
        };
        if in_use {
            return Err(kind.in_use_error(name.to_owned()));
        }

        match kind {
            SchemaObjectKind::Label => {
                self.working.schema.remove_label(name);
            }
            SchemaObjectKind::EdgeType => {
                self.working.schema.remove_edge_type(name);
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default)]
struct GraphData {
    next_node_id: u64,
    next_edge_id: u64,
    nodes: BTreeMap<NodeId, Node>,
    edges: BTreeMap<EdgeId, Edge>,
    outgoing: BTreeMap<NodeId, BTreeSet<EdgeId>>,
    incoming: BTreeMap<NodeId, BTreeSet<EdgeId>>,
    edge_lookup: BTreeMap<EdgeKey, EdgeId>,
    schema: SchemaCatalog,
}

impl GraphData {
    fn allocate_node_id(&mut self) -> NodeId {
        self.next_node_id += 1;
        NodeId::new(self.next_node_id)
    }

    fn allocate_edge_id(&mut self) -> EdgeId {
        self.next_edge_id += 1;
        EdgeId::new(self.next_edge_id)
    }

    fn uses_label(&self, label: &str) -> bool {
        self.nodes.values().any(|node| node.labels.contains(label))
    }

    fn uses_edge_type(&self, edge_type: &str) -> bool {
        self.edges.values().any(|edge| edge.edge_type == edge_type)
    }

    fn stats(&self, last_tx_id: u64) -> GraphStats {
        GraphStats {
            node_count: self.nodes.len(),
            edge_count: self.edges.len(),
            label_count: self.schema.labels().count(),
            edge_type_count: self.schema.edge_types().count(),
            index_count: self.schema.indexes().count(),
            constraint_count: self.schema.constraints().count(),
            last_tx_id,
            wal_bytes: 0,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct EdgeKey {
    from: NodeId,
    edge_type: String,
    to: NodeId,
}

impl EdgeKey {
    fn new(from: NodeId, edge_type: String, to: NodeId) -> Self {
        Self {
            from,
            edge_type,
            to,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum EntityRef {
    Node(NodeId),
    Edge(EdgeId),
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum ValueKey {
    Bool(bool),
    Int(i64),
    Float(u64),
    String(String),
    Bytes(Vec<u8>),
    Datetime(SystemTime),
    List(Vec<ValueKey>),
    Map(Vec<(String, ValueKey)>),
}

fn ensure_property_name(property: &str) -> Result<(), GraphError> {
    if property.is_empty() {
        return Err(GraphError::EmptyPropertyName);
    }
    Ok(())
}

fn ensure_target_exists(schema: &SchemaCatalog, target: &SchemaTarget) -> Result<(), GraphError> {
    let exists = match target.kind() {
        TargetKind::Label => schema.has_label(target.name()),
        TargetKind::EdgeType => schema.has_edge_type(target.name()),
    };
    if exists {
        Ok(())
    } else {
        Err(GraphError::MissingSchemaTarget {
            kind: target.kind(),
            name: target.name().to_owned(),
        })
    }
}

fn property_type_of(value: &Value) -> PropertyType {
    match value {
        Value::Null => PropertyType::Null,
        Value::Bool(_) => PropertyType::Bool,
        Value::Int(_) => PropertyType::Int,
        Value::Float(_) => PropertyType::Float,
        Value::String(_) => PropertyType::String,
        Value::Bytes(_) => PropertyType::Bytes,
        Value::Datetime(_) => PropertyType::Datetime,
        Value::List(_) => PropertyType::List,
        Value::Map(_) => PropertyType::Map,
    }
}

fn key_for_value(value: &Value) -> Option<ValueKey> {
    match value {
        Value::Null => None,
        Value::Bool(value) => Some(ValueKey::Bool(*value)),
        Value::Int(value) => Some(ValueKey::Int(*value)),
        Value::Float(value) => Some(ValueKey::Float(value.to_bits())),
        Value::String(value) => Some(ValueKey::String(value.clone())),
        Value::Bytes(value) => Some(ValueKey::Bytes(value.clone())),
        Value::Datetime(value) => Some(ValueKey::Datetime(*value)),
        Value::List(values) => Some(ValueKey::List(
            values
                .iter()
                .map(key_for_value_strict)
                .collect::<Result<_, _>>()
                .ok()?,
        )),
        Value::Map(map) => Some(ValueKey::Map(
            map.iter()
                .map(|(key, value)| Ok((key.to_owned(), key_for_value_strict(value)?)))
                .collect::<Result<_, GraphError>>()
                .ok()?,
        )),
    }
}

fn key_for_value_strict(value: &Value) -> Result<ValueKey, GraphError> {
    key_for_value(value).ok_or_else(|| GraphError::ConstraintViolation {
        code: "non_indexable_value",
        name: "value".to_owned(),
        detail: "null values are not indexed".to_owned(),
    })
}

fn node_refs_for_target(data: &GraphData, target: &SchemaTarget) -> Vec<EntityRef> {
    match target.kind() {
        TargetKind::Label => data
            .nodes
            .values()
            .filter(|node| node.labels.contains(target.name()))
            .map(|node| EntityRef::Node(node.id()))
            .collect(),
        TargetKind::EdgeType => data
            .edges
            .values()
            .filter(|edge| edge.edge_type == target.name())
            .map(|edge| EntityRef::Edge(edge.id()))
            .collect(),
    }
}

fn property_value_for<'a>(
    data: &'a GraphData,
    entity: &EntityRef,
    property: &str,
) -> Option<&'a Value> {
    match entity {
        EntityRef::Node(node_id) => data.nodes.get(node_id)?.property(property),
        EntityRef::Edge(edge_id) => data.edges.get(edge_id)?.property(property),
    }
}

fn constraint_subject_key<'a>(property: &'a str, constraint_type: &ConstraintType) -> &'a str {
    if !property.is_empty() {
        return property;
    }
    match constraint_type {
        ConstraintType::Endpoints { .. } => "endpoints",
        ConstraintType::MaxOutgoing(_) => "max_outgoing",
        ConstraintType::Unique | ConstraintType::Required | ConstraintType::Type(_) => property,
    }
}

fn validate_schema_rules(schema: &SchemaCatalog, data: &GraphData) -> Result<(), GraphError> {
    for constraint in schema.constraints() {
        let refs = node_refs_for_target(data, constraint.target());
        match constraint.constraint_type() {
            ConstraintType::Required => {
                for entity in refs {
                    let value =
                        property_value_for(data, &entity, constraint.property().unwrap_or(""));
                    match value {
                        Some(Value::Null) | None => {
                            return Err(GraphError::ConstraintViolation {
                                code: "constraint_required_violation",
                                name: constraint.name().to_owned(),
                                detail: format!(
                                    "constraint {} requires property {} to exist and be non-null",
                                    constraint.name(),
                                    constraint.property().unwrap_or("")
                                ),
                            });
                        }
                        Some(_) => {}
                    }
                }
            }
            ConstraintType::Type(expected) => {
                for entity in refs {
                    if let Some(value) =
                        property_value_for(data, &entity, constraint.property().unwrap_or(""))
                        && *expected != property_type_of(value)
                    {
                        return Err(GraphError::ConstraintViolation {
                            code: "constraint_type_violation",
                            name: constraint.name().to_owned(),
                            detail: format!(
                                "constraint {} requires property {} to be {}",
                                constraint.name(),
                                constraint.property().unwrap_or(""),
                                expected.as_str()
                            ),
                        });
                    }
                }
            }
            ConstraintType::Endpoints {
                from_label,
                to_label,
            } => {
                for entity in refs {
                    let EntityRef::Edge(edge_id) = entity else {
                        continue;
                    };
                    let Some(edge) = data.edges.get(&edge_id) else {
                        continue;
                    };
                    let source_ok = data
                        .nodes
                        .get(&edge.from())
                        .map(|node| node.labels().contains(from_label))
                        .unwrap_or(false);
                    let target_ok = data
                        .nodes
                        .get(&edge.to())
                        .map(|node| node.labels().contains(to_label))
                        .unwrap_or(false);
                    if !source_ok || !target_ok {
                        return Err(GraphError::ConstraintViolation {
                            code: "constraint_endpoint_violation",
                            name: constraint.name().to_owned(),
                            detail: format!(
                                "constraint {} requires :{} -> :{} endpoints",
                                constraint.name(),
                                from_label,
                                to_label
                            ),
                        });
                    }
                }
            }
            ConstraintType::MaxOutgoing(limit) => {
                let mut counts = BTreeMap::<NodeId, usize>::new();
                for entity in refs {
                    let EntityRef::Edge(edge_id) = entity else {
                        continue;
                    };
                    let Some(edge) = data.edges.get(&edge_id) else {
                        continue;
                    };
                    let count = counts.entry(edge.from()).or_default();
                    *count += 1;
                    if *count > *limit {
                        return Err(GraphError::ConstraintViolation {
                            code: "constraint_cardinality_violation",
                            name: constraint.name().to_owned(),
                            detail: format!(
                                "constraint {} requires at most {} outgoing edges per source",
                                constraint.name(),
                                limit
                            ),
                        });
                    }
                }
            }
            ConstraintType::Unique => {}
        }
    }
    for index in schema.indexes() {
        let mut entries = BTreeMap::<ValueKey, BTreeSet<EntityRef>>::new();
        for entity in node_refs_for_target(data, index.target()) {
            let Some(value) = property_value_for(data, &entity, index.property()) else {
                continue;
            };
            let Some(key) = key_for_value(value) else {
                continue;
            };

            let slot = entries.entry(key).or_default();
            slot.insert(entity.clone());
            if index.unique() && slot.len() > 1 {
                return Err(GraphError::ConstraintViolation {
                    code: "constraint_unique_violation",
                    name: index
                        .owned_by_constraint()
                        .unwrap_or(index.name())
                        .to_owned(),
                    detail: format!(
                        "unique index {} found duplicate values for property {}",
                        index.name(),
                        index.property()
                    ),
                });
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        ConstraintType, CupldEngine, GraphError, PropertyMap, PropertyType, SchemaTarget, Value,
    };
    use crate::engine::IndexKind;

    #[test]
    fn ids_are_monotonic_across_creates_and_deletes() {
        let mut engine = CupldEngine::default();
        let first = engine.create_node(["Person"], PropertyMap::new()).unwrap();
        let second = engine.create_node(["Person"], PropertyMap::new()).unwrap();
        let deleted = engine.delete_node(first).unwrap();
        let third = engine.create_node(["Person"], PropertyMap::new()).unwrap();

        assert_eq!(deleted.id().get(), 1);
        assert_eq!(second.get(), 2);
        assert_eq!(third.get(), 3);
    }

    #[test]
    fn rejects_duplicate_edges_for_same_triplet() {
        let mut engine = CupldEngine::default();
        let left = engine.create_node(["Person"], PropertyMap::new()).unwrap();
        let right = engine.create_node(["Person"], PropertyMap::new()).unwrap();

        engine
            .create_edge(left, right, "KNOWS", PropertyMap::new())
            .unwrap();

        let err = engine
            .create_edge(left, right, "KNOWS", PropertyMap::new())
            .unwrap_err();

        assert_eq!(
            err,
            GraphError::DuplicateEdge {
                from: left,
                edge_type: "KNOWS".to_owned(),
                to: right,
            }
        );
    }

    #[test]
    fn deleting_a_node_cascades_incident_edges_and_updates_indexes() {
        let mut engine = CupldEngine::default();
        let left = engine.create_node(["Person"], PropertyMap::new()).unwrap();
        let middle = engine.create_node(["Person"], PropertyMap::new()).unwrap();
        let right = engine.create_node(["Person"], PropertyMap::new()).unwrap();

        let incoming = engine
            .create_edge(left, middle, "KNOWS", PropertyMap::new())
            .unwrap();
        let outgoing = engine
            .create_edge(middle, right, "KNOWS", PropertyMap::new())
            .unwrap();

        engine.delete_node(middle).unwrap();

        assert_eq!(engine.stats().edge_count, 0);
        assert!(engine.edge(incoming).is_none());
        assert!(engine.edge(outgoing).is_none());
        assert!(engine.outgoing_edge_ids(left).is_empty());
        assert!(engine.incoming_edge_ids(right).is_empty());
    }

    #[test]
    fn snapshots_stay_pinned_until_commit() {
        let mut engine = CupldEngine::default();
        let node = engine
            .create_node(
                ["Person"],
                PropertyMap::from_pairs([("name", Value::from("Ada"))]),
            )
            .unwrap();

        let first_tx = engine.commit().unwrap();
        let committed = engine.snapshot();
        assert_eq!(first_tx.get(), 1);
        assert_eq!(
            committed
                .node(node)
                .and_then(|current| current.property("name")),
            Some(&Value::from("Ada"))
        );

        engine
            .set_node_property(node, "name", Value::from("Grace"))
            .unwrap();

        assert_eq!(
            engine
                .node(node)
                .and_then(|current| current.property("name")),
            Some(&Value::from("Grace"))
        );
        assert_eq!(
            committed
                .node(node)
                .and_then(|current| current.property("name")),
            Some(&Value::from("Ada"))
        );

        let second_tx = engine.commit().unwrap();
        let refreshed = engine.snapshot();

        assert_eq!(second_tx.get(), 2);
        assert_eq!(
            refreshed
                .node(node)
                .and_then(|current| current.property("name")),
            Some(&Value::from("Grace"))
        );
    }

    #[test]
    fn self_loops_are_allowed_and_indexed_once() {
        let mut engine = CupldEngine::default();
        let node = engine.create_node(["Service"], PropertyMap::new()).unwrap();
        let edge = engine
            .create_edge(node, node, "DEPENDS_ON", PropertyMap::new())
            .unwrap();

        assert!(engine.edge(edge).is_some());
        assert_eq!(engine.outgoing_edge_ids(node), vec![edge]);
        assert_eq!(engine.incoming_edge_ids(node), vec![edge]);
    }

    #[test]
    fn explicit_and_implicit_schema_targets_share_the_catalog() {
        let mut engine = CupldEngine::default();
        engine.create_label("Person", None, false, false).unwrap();
        engine
            .create_edge_type("KNOWS", None, false, false)
            .unwrap();
        let left = engine.create_node(["Person"], PropertyMap::new()).unwrap();
        let right = engine.create_node(["Person"], PropertyMap::new()).unwrap();
        engine
            .create_edge(left, right, "KNOWS", PropertyMap::new())
            .unwrap();

        let schema = engine.show_schema();
        assert!(schema.iter().any(|row| row.ddl == "CREATE LABEL Person"));
        assert!(schema.iter().any(|row| row.ddl == "CREATE EDGE TYPE KNOWS"));
    }

    #[test]
    fn generated_schema_names_are_deterministic_and_visible_in_show_schema() {
        let mut engine = CupldEngine::default();
        engine.create_label("Person", None, false, false).unwrap();
        let index_name = engine
            .create_index(
                None,
                SchemaTarget::label("Person"),
                "email",
                IndexKind::Equality,
                false,
                false,
            )
            .unwrap();
        let constraint_name = engine
            .create_constraint(
                None,
                SchemaTarget::label("Person"),
                "email",
                ConstraintType::Required,
                false,
                false,
            )
            .unwrap();

        assert_eq!(index_name, "idx_label_Person_email_eq");
        assert_eq!(constraint_name, "constraint_label_Person_email_required");
        let schema = engine.show_schema();
        assert!(
            schema
                .iter()
                .any(|row| row.ddl == "CREATE INDEX idx_label_Person_email_eq ON :Person(email)")
        );
        assert!(schema.iter().any(|row| row.ddl
            == "CREATE CONSTRAINT constraint_label_Person_email_required ON :Person REQUIRE email REQUIRED"));
    }

    #[test]
    fn unique_constraints_allow_missing_values_but_reject_duplicate_non_null_values() {
        let mut engine = CupldEngine::default();
        engine.create_label("Person", None, false, false).unwrap();
        engine
            .create_constraint(
                None,
                SchemaTarget::label("Person"),
                "email",
                ConstraintType::Unique,
                false,
                false,
            )
            .unwrap();

        let first = engine.create_node(["Person"], PropertyMap::new()).unwrap();
        let second = engine.create_node(["Person"], PropertyMap::new()).unwrap();
        engine
            .set_node_property(first, "email", Value::from("ada@example.com"))
            .unwrap();
        engine
            .set_node_property(second, "email", Value::from("ada@example.com"))
            .unwrap();

        let err = engine.commit().unwrap_err();
        assert_eq!(err.code(), "constraint_unique_violation");
    }

    #[test]
    fn required_and_type_constraints_validate_existing_data_immediately() {
        let mut engine = CupldEngine::default();
        engine.create_label("Person", None, false, false).unwrap();
        let node = engine.create_node(["Person"], PropertyMap::new()).unwrap();
        engine
            .set_node_property(node, "age", Value::from("wrong"))
            .unwrap();

        let required = engine.create_constraint(
            None,
            SchemaTarget::label("Person"),
            "name",
            ConstraintType::Required,
            false,
            false,
        );
        let typed = engine.create_constraint(
            None,
            SchemaTarget::label("Person"),
            "age",
            ConstraintType::Type(PropertyType::Int),
            false,
            false,
        );

        assert_eq!(
            required.unwrap_err().code(),
            "constraint_required_violation"
        );
        assert_eq!(typed.unwrap_err().code(), "constraint_type_violation");
    }

    #[test]
    fn dropping_labels_or_edge_types_is_blocked_when_data_or_schema_depends_on_them() {
        let mut engine = CupldEngine::default();
        engine.create_label("Person", None, false, false).unwrap();
        engine
            .create_edge_type("KNOWS", None, false, false)
            .unwrap();
        let left = engine.create_node(["Person"], PropertyMap::new()).unwrap();
        let right = engine.create_node(["Person"], PropertyMap::new()).unwrap();
        engine
            .create_edge(left, right, "KNOWS", PropertyMap::new())
            .unwrap();
        engine
            .create_index(
                None,
                SchemaTarget::label("Person"),
                "email",
                IndexKind::Equality,
                false,
                false,
            )
            .unwrap();

        assert_eq!(
            engine.drop_label("Person", false).unwrap_err().code(),
            "label_in_use"
        );
        assert_eq!(
            engine.drop_edge_type("KNOWS", false).unwrap_err().code(),
            "edge_type_in_use"
        );
    }

    #[test]
    fn unique_constraints_create_backing_indexes_that_cannot_be_dropped_directly() {
        let mut engine = CupldEngine::default();
        engine.create_label("Person", None, false, false).unwrap();
        let constraint_name = engine
            .create_constraint(
                None,
                SchemaTarget::label("Person"),
                "email",
                ConstraintType::Unique,
                false,
                false,
            )
            .unwrap();
        let backing_index = "idx_label_Person_email_unique";
        let err = engine.drop_index(backing_index, false).unwrap_err();

        assert_eq!(constraint_name, "constraint_label_Person_email_unique");
        assert_eq!(err.code(), "backing_index_owned");

        engine.drop_constraint(&constraint_name, false).unwrap();
        assert!(
            !engine
                .show_indexes(None)
                .iter()
                .any(|row| row.name == backing_index)
        );
    }
}
