use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use super::state::{ConstraintState, IndexState, SchemaState};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TargetKind {
    Label,
    EdgeType,
}

impl TargetKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Label => "label",
            Self::EdgeType => "edge_type",
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct SchemaObjectOptions {
    description: Option<String>,
}

impl SchemaObjectOptions {
    pub fn new(description: Option<String>) -> Self {
        Self { description }
    }

    pub fn description(&self) -> Option<&str> {
        self.description.as_deref()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SchemaTarget {
    kind: TargetKind,
    name: String,
}

impl SchemaTarget {
    pub fn label<S>(name: S) -> Self
    where
        S: Into<String>,
    {
        Self {
            kind: TargetKind::Label,
            name: name.into(),
        }
    }

    pub fn edge_type<S>(name: S) -> Self
    where
        S: Into<String>,
    {
        Self {
            kind: TargetKind::EdgeType,
            name: name.into(),
        }
    }

    pub fn kind(&self) -> TargetKind {
        self.kind
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn display_target(&self) -> String {
        match self.kind {
            TargetKind::Label => format!(":{}", self.name),
            TargetKind::EdgeType => format!("[:{}]", self.name),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum PropertyType {
    String,
    Int,
    Float,
    Bool,
    Bytes,
    Datetime,
    List,
    Map,
    Null,
}

impl PropertyType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::String => "string",
            Self::Int => "int",
            Self::Float => "float",
            Self::Bool => "bool",
            Self::Bytes => "bytes",
            Self::Datetime => "datetime",
            Self::List => "list",
            Self::Map => "map",
            Self::Null => "null",
        }
    }
}

impl fmt::Display for PropertyType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConstraintType {
    Unique,
    Required,
    Type(PropertyType),
    Endpoints {
        from_label: String,
        to_label: String,
    },
    MaxOutgoing(usize),
}

impl ConstraintType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Unique => "UNIQUE",
            Self::Required => "REQUIRED",
            Self::Type(_) => "TYPE",
            Self::Endpoints { .. } => "ENDPOINTS",
            Self::MaxOutgoing(_) => "MAX_OUTGOING",
        }
    }

    pub(crate) fn generated_suffix(&self) -> String {
        match self {
            Self::Unique => "unique".to_owned(),
            Self::Required => "required".to_owned(),
            Self::Type(property_type) => format!("type_{}", property_type.as_str()),
            Self::Endpoints {
                from_label,
                to_label,
            } => format!("endpoints_{}_{}", sanitize(from_label), sanitize(to_label)),
            Self::MaxOutgoing(limit) => format!("max_outgoing_{limit}"),
        }
    }

    pub(crate) fn detail_string(&self) -> String {
        match self {
            Self::Unique => "unique".to_owned(),
            Self::Required => "required".to_owned(),
            Self::Type(property_type) => format!("type={}", property_type.as_str()),
            Self::Endpoints {
                from_label,
                to_label,
            } => format!("endpoints=:{}->:{}", from_label, to_label),
            Self::MaxOutgoing(limit) => format!("max_outgoing={limit}"),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IndexStatus {
    Ready,
    Building,
    Invalid,
}

impl IndexStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::Building => "building",
            Self::Invalid => "invalid",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IndexKind {
    Equality,
    Range,
    ListMembership,
    FullText,
}

impl IndexKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Equality => "eq",
            Self::Range => "range",
            Self::ListMembership => "list",
            Self::FullText => "fulltext",
        }
    }

    pub fn ddl_fragment(self) -> Option<&'static str> {
        match self {
            Self::Equality => None,
            Self::Range => Some("RANGE"),
            Self::ListMembership => Some("LIST"),
            Self::FullText => Some("FULLTEXT"),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexDefinition {
    name: String,
    target: SchemaTarget,
    property: String,
    kind: IndexKind,
    unique: bool,
    status: IndexStatus,
    owned_by_constraint: Option<String>,
}

impl IndexDefinition {
    pub fn new(
        name: String,
        target: SchemaTarget,
        property: String,
        kind: IndexKind,
        unique: bool,
        status: IndexStatus,
        owned_by_constraint: Option<String>,
    ) -> Self {
        Self {
            name,
            target,
            property,
            kind,
            unique,
            status,
            owned_by_constraint,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn target(&self) -> &SchemaTarget {
        &self.target
    }

    pub fn property(&self) -> &str {
        &self.property
    }

    pub fn unique(&self) -> bool {
        self.unique
    }

    pub fn kind(&self) -> IndexKind {
        self.kind
    }

    pub fn status(&self) -> IndexStatus {
        self.status
    }

    pub fn owned_by_constraint(&self) -> Option<&str> {
        self.owned_by_constraint.as_deref()
    }

    pub fn canonical_ddl(&self) -> String {
        let mut ddl = format!(
            "CREATE INDEX {} ON {}({})",
            self.name,
            self.target.display_target(),
            self.property
        );
        if let Some(kind) = self.kind.ddl_fragment() {
            ddl.push_str(" KIND ");
            ddl.push_str(kind);
        }
        ddl
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConstraintDefinition {
    name: String,
    target: SchemaTarget,
    property: String,
    constraint_type: ConstraintType,
}

impl ConstraintDefinition {
    pub fn new(
        name: String,
        target: SchemaTarget,
        property: String,
        constraint_type: ConstraintType,
    ) -> Self {
        Self {
            name,
            target,
            property,
            constraint_type,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn target(&self) -> &SchemaTarget {
        &self.target
    }

    pub fn property(&self) -> Option<&str> {
        (!self.property.is_empty()).then_some(self.property.as_str())
    }

    pub fn constraint_type(&self) -> &ConstraintType {
        &self.constraint_type
    }

    pub fn canonical_ddl(&self) -> String {
        match &self.constraint_type {
            ConstraintType::Unique => format!(
                "CREATE CONSTRAINT {} ON {} REQUIRE {} UNIQUE",
                self.name,
                self.target.display_target(),
                self.property
            ),
            ConstraintType::Required => format!(
                "CREATE CONSTRAINT {} ON {} REQUIRE {} REQUIRED",
                self.name,
                self.target.display_target(),
                self.property
            ),
            ConstraintType::Type(property_type) => format!(
                "CREATE CONSTRAINT {} ON {} REQUIRE {} TYPE {}",
                self.name,
                self.target.display_target(),
                self.property,
                property_type
            ),
            ConstraintType::Endpoints {
                from_label,
                to_label,
            } => format!(
                "CREATE CONSTRAINT {} ON {} REQUIRE ENDPOINTS :{} -> :{}",
                self.name,
                self.target.display_target(),
                from_label,
                to_label
            ),
            ConstraintType::MaxOutgoing(limit) => format!(
                "CREATE CONSTRAINT {} ON {} REQUIRE MAX OUTGOING {}",
                self.name,
                self.target.display_target(),
                limit
            ),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct SchemaCatalog {
    labels: BTreeSet<String>,
    edge_types: BTreeSet<String>,
    object_options: BTreeMap<SchemaTarget, SchemaObjectOptions>,
    indexes: BTreeMap<String, IndexDefinition>,
    constraints: BTreeMap<String, ConstraintDefinition>,
}

impl SchemaCatalog {
    pub fn labels(&self) -> impl Iterator<Item = &str> {
        self.labels.iter().map(String::as_str)
    }

    pub fn edge_types(&self) -> impl Iterator<Item = &str> {
        self.edge_types.iter().map(String::as_str)
    }

    pub fn indexes(&self) -> impl Iterator<Item = &IndexDefinition> {
        self.indexes.values()
    }

    pub fn constraints(&self) -> impl Iterator<Item = &ConstraintDefinition> {
        self.constraints.values()
    }

    pub fn object_options(&self, target: &SchemaTarget) -> Option<&SchemaObjectOptions> {
        self.object_options.get(target)
    }

    pub fn ensure_label<S>(&mut self, name: S)
    where
        S: Into<String>,
    {
        let name = name.into();
        self.labels.insert(name.clone());
        self.object_options
            .entry(SchemaTarget::label(name))
            .or_default();
    }

    pub fn ensure_edge_type<S>(&mut self, name: S)
    where
        S: Into<String>,
    {
        let name = name.into();
        self.edge_types.insert(name.clone());
        self.object_options
            .entry(SchemaTarget::edge_type(name))
            .or_default();
    }

    pub fn create_label<S>(&mut self, name: S, description: Option<String>) -> String
    where
        S: Into<String>,
    {
        let name = insert_name(&mut self.labels, name.into());
        self.object_options.insert(
            SchemaTarget::label(name.clone()),
            SchemaObjectOptions::new(description),
        );
        name
    }

    pub fn create_edge_type<S>(&mut self, name: S, description: Option<String>) -> String
    where
        S: Into<String>,
    {
        let name = insert_name(&mut self.edge_types, name.into());
        self.object_options.insert(
            SchemaTarget::edge_type(name.clone()),
            SchemaObjectOptions::new(description),
        );
        name
    }

    pub fn remove_label(&mut self, name: &str) -> bool {
        self.object_options.remove(&SchemaTarget::label(name));
        self.labels.remove(name)
    }

    pub fn remove_edge_type(&mut self, name: &str) -> bool {
        self.object_options.remove(&SchemaTarget::edge_type(name));
        self.edge_types.remove(name)
    }

    pub fn has_label(&self, name: &str) -> bool {
        self.labels.contains(name)
    }

    pub fn has_edge_type(&self, name: &str) -> bool {
        self.edge_types.contains(name)
    }

    pub fn create_index(
        &mut self,
        name: Option<String>,
        target: SchemaTarget,
        property: String,
        kind: IndexKind,
        unique: bool,
        owned_by_constraint: Option<String>,
    ) -> String {
        let name = name.unwrap_or_else(|| {
            generated_name("idx", target.kind(), target.name(), &property, Some(kind.as_str()))
        });
        let definition = IndexDefinition::new(
            name.clone(),
            target,
            property,
            kind,
            unique,
            IndexStatus::Ready,
            owned_by_constraint,
        );
        self.indexes.insert(name.clone(), definition);
        name
    }

    pub fn create_constraint(
        &mut self,
        name: Option<String>,
        target: SchemaTarget,
        property: String,
        constraint_type: ConstraintType,
    ) -> String {
        let generated = name.unwrap_or_else(|| {
            generated_name(
                "constraint",
                target.kind(),
                target.name(),
                &property,
                Some(&constraint_type.generated_suffix()),
            )
        });
        let definition =
            ConstraintDefinition::new(generated.clone(), target, property, constraint_type);
        self.constraints.insert(generated.clone(), definition);
        generated
    }

    pub fn drop_index(&mut self, name: &str) -> Option<IndexDefinition> {
        self.indexes.remove(name)
    }

    pub fn index_mut(&mut self, name: &str) -> Option<&mut IndexDefinition> {
        self.indexes.get_mut(name)
    }

    pub fn set_index_status(&mut self, name: &str, status: IndexStatus) -> bool {
        let Some(index) = self.indexes.get_mut(name) else {
            return false;
        };
        index.status = status;
        true
    }

    pub fn drop_constraint(&mut self, name: &str) -> Option<ConstraintDefinition> {
        self.constraints.remove(name)
    }

    pub fn constraint_mut(&mut self, name: &str) -> Option<&mut ConstraintDefinition> {
        self.constraints.get_mut(name)
    }

    pub fn rename_constraint(&mut self, name: &str, rename_to: &str) -> bool {
        let Some(mut constraint) = self.constraints.remove(name) else {
            return false;
        };
        constraint.name = rename_to.to_owned();
        self.constraints.insert(rename_to.to_owned(), constraint);
        for index in self.indexes.values_mut() {
            if index.owned_by_constraint.as_deref() == Some(name) {
                index.owned_by_constraint = Some(rename_to.to_owned());
            }
        }
        true
    }

    pub fn index(&self, name: &str) -> Option<&IndexDefinition> {
        self.indexes.get(name)
    }

    pub fn find_index(
        &self,
        target: &SchemaTarget,
        property: &str,
        kind: IndexKind,
    ) -> Option<&IndexDefinition> {
        self.indexes.values().find(|index| {
            index.target() == target
                && index.property() == property
                && index.kind() == kind
                && index.status() == IndexStatus::Ready
        })
    }

    pub fn constraint(&self, name: &str) -> Option<&ConstraintDefinition> {
        self.constraints.get(name)
    }

    pub fn object_exists(&self, name: &str) -> bool {
        self.indexes.contains_key(name) || self.constraints.contains_key(name)
    }

    pub fn depends_on_target(&self, target: &SchemaTarget) -> bool {
        self.indexes.values().any(|index| index.target == *target)
            || self
                .constraints
                .values()
                .any(|constraint| constraint.target == *target)
    }

    pub fn show_schema_rows(&self) -> Vec<SchemaRow> {
        let mut rows = Vec::new();
        for label in &self.labels {
            let target = SchemaTarget::label(label.clone());
            let options = self
                .object_options
                .get(&target)
                .cloned()
                .unwrap_or_default();
            rows.push(SchemaRow {
                kind: "label".to_owned(),
                name: label.clone(),
                description: options.description().map(ToOwned::to_owned),
                ddl: schema_object_ddl(&target, &options),
            });
        }
        for edge_type in &self.edge_types {
            let target = SchemaTarget::edge_type(edge_type.clone());
            let options = self
                .object_options
                .get(&target)
                .cloned()
                .unwrap_or_default();
            rows.push(SchemaRow {
                kind: "edge_type".to_owned(),
                name: edge_type.clone(),
                description: options.description().map(ToOwned::to_owned),
                ddl: schema_object_ddl(&target, &options),
            });
        }
        for index in self.indexes.values() {
            rows.push(SchemaRow {
                kind: "index".to_owned(),
                name: index.name.clone(),
                description: None,
                ddl: index.canonical_ddl(),
            });
        }
        for constraint in self.constraints.values() {
            rows.push(SchemaRow {
                kind: "constraint".to_owned(),
                name: constraint.name.clone(),
                description: None,
                ddl: constraint.canonical_ddl(),
            });
        }
        rows
    }

    pub fn show_index_rows(&self, target_filter: Option<&SchemaTarget>) -> Vec<IndexRow> {
        self.indexes
            .values()
            .filter(|index| target_filter.is_none_or(|target| index.target() == target))
            .map(|index| IndexRow {
                name: index.name.clone(),
                target_kind: index.target.kind.as_str().to_owned(),
                target_name: index.target.name.clone(),
                property: index.property.clone(),
                unique: index.unique,
                status: index.status.as_str().to_owned(),
                kind: index.kind.as_str().to_owned(),
            })
            .collect()
    }

    pub fn show_constraint_rows(&self, target_filter: Option<&SchemaTarget>) -> Vec<ConstraintRow> {
        self.constraints
            .values()
            .filter(|constraint| target_filter.is_none_or(|target| constraint.target() == target))
            .map(|constraint| ConstraintRow {
                name: constraint.name.clone(),
                target_kind: constraint.target.kind.as_str().to_owned(),
                target_name: constraint.target.name.clone(),
                property: constraint.property().unwrap_or_default().to_owned(),
                constraint_type: constraint.constraint_type.as_str().to_owned(),
                details: constraint.constraint_type.detail_string(),
            })
            .collect()
    }

    pub(crate) fn to_state(&self) -> SchemaState {
        SchemaState {
            labels: self.labels.iter().cloned().collect(),
            edge_types: self.edge_types.iter().cloned().collect(),
            object_options: self
                .object_options
                .iter()
                .map(|(target, options)| super::state::SchemaObjectState {
                    target: target.clone(),
                    description: options.description().map(ToOwned::to_owned),
                })
                .collect(),
            indexes: self
                .indexes
                .values()
                .map(|index| IndexState {
                    name: index.name.clone(),
                    target: index.target.clone(),
                    property: index.property.clone(),
                    kind: index.kind,
                    unique: index.unique,
                    status: index.status,
                    owned_by_constraint: index.owned_by_constraint.clone(),
                })
                .collect(),
            constraints: self
                .constraints
                .values()
                .map(|constraint| ConstraintState {
                    name: constraint.name.clone(),
                    target: constraint.target.clone(),
                    property: constraint.property.clone(),
                    constraint_type: constraint.constraint_type.clone(),
                })
                .collect(),
        }
    }

    pub(crate) fn from_state(state: SchemaState) -> Self {
        Self {
            labels: state.labels.into_iter().collect(),
            edge_types: state.edge_types.into_iter().collect(),
            object_options: state
                .object_options
                .into_iter()
                .map(|object| (object.target, SchemaObjectOptions::new(object.description)))
                .collect(),
            indexes: state
                .indexes
                .into_iter()
                .map(|index| {
                    (
                        index.name.clone(),
                        IndexDefinition::new(
                            index.name,
                            index.target,
                            index.property,
                            index.kind,
                            index.unique,
                            index.status,
                            index.owned_by_constraint,
                        ),
                    )
                })
                .collect(),
            constraints: state
                .constraints
                .into_iter()
                .map(|constraint| {
                    (
                        constraint.name.clone(),
                        ConstraintDefinition::new(
                            constraint.name,
                            constraint.target,
                            constraint.property,
                            constraint.constraint_type,
                        ),
                    )
                })
                .collect(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SchemaRow {
    pub kind: String,
    pub name: String,
    pub description: Option<String>,
    pub ddl: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexRow {
    pub name: String,
    pub target_kind: String,
    pub target_name: String,
    pub property: String,
    pub unique: bool,
    pub status: String,
    pub kind: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConstraintRow {
    pub name: String,
    pub target_kind: String,
    pub target_name: String,
    pub property: String,
    pub constraint_type: String,
    pub details: String,
}

fn schema_object_ddl(target: &SchemaTarget, options: &SchemaObjectOptions) -> String {
    let mut ddl = match target.kind() {
        TargetKind::Label => format!("CREATE LABEL {}", target.name()),
        TargetKind::EdgeType => format!("CREATE EDGE TYPE {}", target.name()),
    };
    if let Some(description) = options.description() {
        ddl.push_str(" DESCRIPTION ");
        ddl.push_str(&quote_ddl_string(description));
    }
    ddl
}

fn quote_ddl_string(input: &str) -> String {
    let escaped = input
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
        .replace('\t', "\\t");
    format!("\"{escaped}\"")
}

pub(crate) fn generated_name(
    prefix: &str,
    target_kind: TargetKind,
    target_name: &str,
    property: &str,
    suffix: Option<&str>,
) -> String {
    let mut name = format!(
        "{}_{}_{}_{}",
        prefix,
        target_kind.as_str(),
        sanitize(target_name),
        sanitize(property)
    );
    if let Some(suffix) = suffix {
        name.push('_');
        name.push_str(&sanitize(suffix));
    }
    name
}

fn insert_name(set: &mut BTreeSet<String>, name: String) -> String {
    set.insert(name.clone());
    name
}

fn sanitize(input: &str) -> String {
    input
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
        .collect()
}
