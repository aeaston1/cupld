use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::path::{Path, PathBuf};

use crate::automation::{
    AutomationError, AutomationPolicy, RetrievalUsage, automation_policy_json_value,
    retrieval_usage_json_value,
};
use crate::json::{self, JsonValue};
use crate::{QueryResult, RuntimeValue, Session, Value};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ContextDirection {
    In,
    Out,
    Both,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ContextRequest {
    pub db_path: PathBuf,
    pub nodes: Vec<usize>,
    pub paths: Vec<String>,
    pub depth: u8,
    pub direction: ContextDirection,
    pub edge_types: Vec<String>,
    pub labels: Vec<String>,
    pub max_nodes: usize,
    pub max_edges: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ContextEvidence {
    pub field: String,
    pub value: String,
    pub source: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ContextNode {
    pub node_id: i64,
    pub labels: Vec<String>,
    pub properties: BTreeMap<String, Value>,
    pub name: Option<String>,
    pub title: Option<String>,
    pub display: Option<String>,
    pub evidence: Vec<ContextEvidence>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ContextEdge {
    pub from_node_id: i64,
    pub to_node_id: i64,
    pub edge_type: String,
    pub properties: BTreeMap<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ContextSeed {
    pub kind: String,
    pub value: String,
    pub node_ids: Vec<i64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ContextWarning {
    pub code: String,
    pub message: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ContextProvenance {
    pub db_path: String,
    pub source: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ContextEnvelope {
    pub ok: bool,
    pub command: String,
    pub mode: String,
    pub policy: AutomationPolicy,
    pub retrieval_usage: RetrievalUsage,
    pub provenance: ContextProvenance,
    pub request: ContextRequestSummary,
    pub seeds: Vec<ContextSeed>,
    pub nodes: Vec<ContextNode>,
    pub edges: Vec<ContextEdge>,
    pub warnings: Vec<ContextWarning>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ContextRequestSummary {
    pub depth: u8,
    pub direction: ContextDirection,
    pub edge_types: Vec<String>,
    pub labels: Vec<String>,
    pub max_nodes: usize,
    pub max_edges: usize,
}

#[derive(Clone, Debug)]
struct ContextGraph {
    nodes: BTreeMap<i64, ContextNode>,
    edges: Vec<ContextEdge>,
}

impl ContextRequest {
    pub fn run(&self) -> Result<ContextEnvelope, AutomationError> {
        let mut session = Session::open(&self.db_path).map_err(AutomationError::from)?;
        let graph = load_context_graph(&mut session)?;
        build_context_response(&self.db_path, self, graph)
    }
}

pub fn context_as_json(response: &ContextEnvelope) -> String {
    json::stringify(&context_json_value(response))
}

pub fn context_as_ndjson(response: &ContextEnvelope) -> Vec<String> {
    let mut lines = vec![json::stringify(&JsonValue::object([
        ("kind", JsonValue::from("context_meta")),
        ("ok", JsonValue::Bool(response.ok)),
        ("command", JsonValue::from(response.command.clone())),
        ("policy", automation_policy_json_value(&response.policy)),
        (
            "retrieval_usage",
            retrieval_usage_json_value(&response.retrieval_usage),
        ),
        (
            "provenance",
            context_provenance_json_value(&response.provenance),
        ),
        (
            "warnings",
            JsonValue::array(response.warnings.iter().map(context_warning_json_value)),
        ),
    ]))];

    for seed in &response.seeds {
        lines.push(json::stringify(&JsonValue::object([
            ("kind", JsonValue::from("context_seed")),
            ("seed", context_seed_json_value(seed)),
        ])));
    }

    for (node_index, node) in response.nodes.iter().enumerate() {
        lines.push(json::stringify(&JsonValue::object([
            ("kind", JsonValue::from("context_node")),
            ("node_index", JsonValue::from(node_index)),
            ("node", context_node_json_value(node)),
        ])));
    }

    for (edge_index, edge) in response.edges.iter().enumerate() {
        lines.push(json::stringify(&JsonValue::object([
            ("kind", JsonValue::from("context_edge")),
            ("edge_index", JsonValue::from(edge_index)),
            ("edge", context_edge_json_value(edge)),
        ])));
    }
    lines
}

pub fn context_as_query_result(response: &ContextEnvelope) -> QueryResult {
    QueryResult {
        columns: vec![
            "node_id".to_owned(),
            "labels".to_owned(),
            "name".to_owned(),
            "title".to_owned(),
            "display".to_owned(),
        ],
        rows: response
            .nodes
            .iter()
            .map(|item| {
                vec![
                    RuntimeValue::Int(item.node_id),
                    RuntimeValue::List(
                        item.labels
                            .iter()
                            .cloned()
                            .map(RuntimeValue::String)
                            .collect(),
                    ),
                    optional_runtime_string(&item.name),
                    optional_runtime_string(&item.title),
                    optional_runtime_string(&item.display),
                ]
            })
            .collect(),
    }
}

fn optional_runtime_string(value: &Option<String>) -> RuntimeValue {
    value
        .as_ref()
        .map(|value| RuntimeValue::String(value.clone()))
        .unwrap_or(RuntimeValue::Null)
}

fn load_context_graph(session: &mut Session) -> Result<ContextGraph, AutomationError> {
    let snapshot = session.engine().snapshot();
    let nodes = snapshot
        .nodes()
        .map(|node| {
            let context_node = ContextNode::from_node(node);
            (context_node.node_id, context_node)
        })
        .collect::<BTreeMap<_, _>>();
    let edges = snapshot
        .edges()
        .map(ContextEdge::from_edge)
        .collect::<Vec<_>>();

    Ok(ContextGraph { nodes, edges })
}

fn build_context_response(
    db_path: &Path,
    request: &ContextRequest,
    graph: ContextGraph,
) -> Result<ContextEnvelope, AutomationError> {
    let policy = AutomationPolicy::context(request.max_nodes, request.max_edges);
    let Some(retrieval_budget) = policy.retrieval_budget else {
        unreachable!("context policy should include retrieval budget");
    };
    let seed_nodes = resolve_seed_nodes(request, &graph);
    let seeds = context_seed_summaries(request, &graph);
    let mut warnings = Vec::new();
    for node_id in &request.nodes {
        if !graph.nodes.contains_key(&(*node_id as i64)) {
            warnings.push(ContextWarning {
                code: "context_seed_not_found".to_owned(),
                message: format!("node seed `{node_id}` was not found"),
            });
        }
    }
    for path in &request.paths {
        if !graph.nodes.values().any(|node| {
            node.evidence
                .iter()
                .any(|evidence| evidence.field == "src.path" && evidence.value == *path)
        }) {
            warnings.push(ContextWarning {
                code: "context_seed_not_found".to_owned(),
                message: format!("path seed `{path}` was not found"),
            });
        }
    }

    let (node_ids, mut edges, truncated_by_budget) = traverse_context(request, &graph, &seed_nodes);
    let mut nodes = node_ids
        .iter()
        .filter_map(|node_id| graph.nodes.get(node_id).cloned())
        .collect::<Vec<_>>();
    let mut truncated = truncated_by_budget || nodes.len() > retrieval_budget.nodes;
    nodes.truncate(retrieval_budget.nodes);
    let retained_ids = nodes
        .iter()
        .map(|node| node.node_id)
        .collect::<BTreeSet<_>>();
    edges.retain(|edge| {
        retained_ids.contains(&edge.from_node_id) && retained_ids.contains(&edge.to_node_id)
    });
    if edges.len() > retrieval_budget.edges {
        edges.truncate(retrieval_budget.edges);
        truncated = true;
    }

    let mut buffer = String::new();
    loop {
        let mut response = ContextEnvelope {
            ok: true,
            command: "context".to_owned(),
            mode: "seeded".to_owned(),
            policy,
            retrieval_usage: RetrievalUsage {
                nodes: nodes.len(),
                edges: edges.len(),
                snippet_bytes: 0,
                total_payload_bytes: 0,
                truncated,
            },
            provenance: ContextProvenance {
                db_path: db_path.display().to_string(),
                source: "cupld.context".to_owned(),
            },
            request: ContextRequestSummary::from_request(request),
            seeds: seeds.clone(),
            nodes: nodes.clone(),
            edges: edges.clone(),
            warnings: warnings.clone(),
        };
        let payload_bytes = context_payload_bytes(&mut response, &mut buffer);
        if payload_bytes <= retrieval_budget.total_payload_bytes || response.nodes.is_empty() {
            return Ok(response);
        }
        response.nodes.pop();
        let retained_ids = response
            .nodes
            .iter()
            .map(|node| node.node_id)
            .collect::<BTreeSet<_>>();
        nodes = response.nodes;
        edges.retain(|edge| {
            retained_ids.contains(&edge.from_node_id) && retained_ids.contains(&edge.to_node_id)
        });
        truncated = true;
    }
}

fn resolve_seed_nodes(request: &ContextRequest, graph: &ContextGraph) -> BTreeSet<i64> {
    let mut seeds = request
        .nodes
        .iter()
        .filter_map(|node_id| i64::try_from(*node_id).ok())
        .filter(|node_id| graph.nodes.contains_key(node_id))
        .collect::<BTreeSet<_>>();
    for path in &request.paths {
        seeds.extend(graph.nodes.values().filter_map(|node| {
            node.evidence
                .iter()
                .any(|evidence| evidence.field == "src.path" && evidence.value == *path)
                .then_some(node.node_id)
        }));
    }
    seeds
}

fn context_seed_summaries(request: &ContextRequest, graph: &ContextGraph) -> Vec<ContextSeed> {
    let mut seeds = Vec::new();
    for node_id in &request.nodes {
        let resolved_node_id = i64::try_from(*node_id).ok();
        seeds.push(ContextSeed {
            kind: "node".to_owned(),
            value: node_id.to_string(),
            node_ids: resolved_node_id
                .filter(|resolved_node_id| graph.nodes.contains_key(resolved_node_id))
                .into_iter()
                .collect(),
        });
    }
    for path in &request.paths {
        seeds.push(ContextSeed {
            kind: "path".to_owned(),
            value: path.clone(),
            node_ids: graph
                .nodes
                .values()
                .filter(|node| {
                    node.evidence
                        .iter()
                        .any(|evidence| evidence.field == "src.path" && evidence.value == *path)
                })
                .map(|node| node.node_id)
                .collect(),
        });
    }
    seeds
}

fn traverse_context(
    request: &ContextRequest,
    graph: &ContextGraph,
    seeds: &BTreeSet<i64>,
) -> (Vec<i64>, Vec<ContextEdge>, bool) {
    let mut visited = BTreeSet::new();
    let mut ordered_nodes = Vec::new();
    let mut selected_edges = Vec::new();
    let mut selected_edge_keys = BTreeSet::new();
    let mut queued = VecDeque::new();
    let mut truncated = false;

    for seed in seeds {
        if include_node(*seed, request, graph) {
            visited.insert(*seed);
            ordered_nodes.push(*seed);
            queued.push_back((*seed, 0u8));
        }
    }

    while let Some((node_id, depth)) = queued.pop_front() {
        if depth >= request.depth {
            continue;
        }
        for edge in graph
            .edges
            .iter()
            .filter(|edge| edge_matches(node_id, edge, request))
        {
            if selected_edges.len() >= request.max_edges {
                truncated = true;
                break;
            }
            let next_id = if edge.from_node_id == node_id {
                edge.to_node_id
            } else {
                edge.from_node_id
            };
            if !include_node(next_id, request, graph) {
                continue;
            }
            if selected_edge_keys.insert((
                edge.from_node_id,
                edge.to_node_id,
                edge.edge_type.clone(),
            )) {
                selected_edges.push(edge.clone());
            }
            if visited.insert(next_id) {
                if ordered_nodes.len() >= request.max_nodes {
                    truncated = true;
                    continue;
                }
                ordered_nodes.push(next_id);
                queued.push_back((next_id, depth + 1));
            }
        }
    }

    (ordered_nodes, selected_edges, truncated)
}

fn edge_matches(node_id: i64, edge: &ContextEdge, request: &ContextRequest) -> bool {
    if !request.edge_types.is_empty()
        && !request
            .edge_types
            .iter()
            .any(|edge_type| edge_type == &edge.edge_type)
    {
        return false;
    }
    match request.direction {
        ContextDirection::In => edge.to_node_id == node_id,
        ContextDirection::Out => edge.from_node_id == node_id,
        ContextDirection::Both => edge.from_node_id == node_id || edge.to_node_id == node_id,
    }
}

fn include_node(node_id: i64, request: &ContextRequest, graph: &ContextGraph) -> bool {
    request.labels.is_empty()
        || graph.nodes.get(&node_id).is_some_and(|node| {
            node.labels
                .iter()
                .any(|label| request.labels.iter().any(|expected| expected == label))
        })
}

fn context_payload_bytes(response: &mut ContextEnvelope, buffer: &mut String) -> usize {
    loop {
        buffer.clear();
        json::write_to(buffer, &context_json_value(response));
        let payload_bytes = buffer.len();
        if response.retrieval_usage.total_payload_bytes == payload_bytes {
            return payload_bytes;
        }
        response.retrieval_usage.total_payload_bytes = payload_bytes;
    }
}

fn context_evidence_json_value(evidence: &ContextEvidence) -> JsonValue {
    JsonValue::object([
        ("field", JsonValue::from(evidence.field.clone())),
        ("value", JsonValue::from(evidence.value.clone())),
        ("source", JsonValue::from(evidence.source.clone())),
    ])
}

fn context_node_json_value(item: &ContextNode) -> JsonValue {
    let mut fields = vec![
        ("node_id".to_owned(), JsonValue::from(item.node_id)),
        (
            "labels".to_owned(),
            JsonValue::array(item.labels.iter().cloned().map(JsonValue::from)),
        ),
        (
            "properties".to_owned(),
            JsonValue::object(
                item.properties
                    .iter()
                    .map(|(key, value)| (key.clone(), value_json_value(value))),
            ),
        ),
    ];
    if let Some(name) = &item.name {
        fields.push(("name".to_owned(), JsonValue::from(name.clone())));
    }
    if let Some(title) = &item.title {
        fields.push(("title".to_owned(), JsonValue::from(title.clone())));
    }
    if let Some(display) = &item.display {
        fields.push(("display".to_owned(), JsonValue::from(display.clone())));
    }
    fields.push((
        "evidence".to_owned(),
        JsonValue::array(item.evidence.iter().map(context_evidence_json_value)),
    ));
    JsonValue::Object(fields)
}

fn context_edge_json_value(edge: &ContextEdge) -> JsonValue {
    JsonValue::object([
        ("from_node_id", JsonValue::from(edge.from_node_id)),
        ("to_node_id", JsonValue::from(edge.to_node_id)),
        ("edge_type", JsonValue::from(edge.edge_type.clone())),
        (
            "properties",
            JsonValue::object(
                edge.properties
                    .iter()
                    .map(|(key, value)| (key.clone(), value_json_value(value))),
            ),
        ),
    ])
}

fn context_seed_json_value(seed: &ContextSeed) -> JsonValue {
    JsonValue::object([
        ("kind", JsonValue::from(seed.kind.clone())),
        ("value", JsonValue::from(seed.value.clone())),
        (
            "node_ids",
            JsonValue::array(seed.node_ids.iter().copied().map(JsonValue::from)),
        ),
    ])
}

fn context_request_json_value(request: &ContextRequestSummary) -> JsonValue {
    JsonValue::object([
        ("depth", JsonValue::from(usize::from(request.depth))),
        (
            "direction",
            JsonValue::from(context_direction_name(request.direction)),
        ),
        (
            "edge_types",
            JsonValue::array(request.edge_types.iter().cloned().map(JsonValue::from)),
        ),
        (
            "labels",
            JsonValue::array(request.labels.iter().cloned().map(JsonValue::from)),
        ),
        ("max_nodes", JsonValue::from(request.max_nodes)),
        ("max_edges", JsonValue::from(request.max_edges)),
    ])
}

fn context_warning_json_value(warning: &ContextWarning) -> JsonValue {
    JsonValue::object([
        ("code", JsonValue::from(warning.code.clone())),
        ("message", JsonValue::from(warning.message.clone())),
    ])
}

fn context_provenance_json_value(provenance: &ContextProvenance) -> JsonValue {
    JsonValue::object([
        ("db_path", JsonValue::from(provenance.db_path.clone())),
        ("source", JsonValue::from(provenance.source.clone())),
    ])
}

fn context_json_value(response: &ContextEnvelope) -> JsonValue {
    JsonValue::object([
        ("ok", JsonValue::Bool(response.ok)),
        ("command", JsonValue::from(response.command.clone())),
        ("mode", JsonValue::from(response.mode.clone())),
        ("policy", automation_policy_json_value(&response.policy)),
        (
            "retrieval_usage",
            retrieval_usage_json_value(&response.retrieval_usage),
        ),
        (
            "provenance",
            context_provenance_json_value(&response.provenance),
        ),
        ("request", context_request_json_value(&response.request)),
        (
            "seeds",
            JsonValue::array(response.seeds.iter().map(context_seed_json_value)),
        ),
        (
            "nodes",
            JsonValue::array(response.nodes.iter().map(context_node_json_value)),
        ),
        (
            "edges",
            JsonValue::array(response.edges.iter().map(context_edge_json_value)),
        ),
        (
            "warnings",
            JsonValue::array(response.warnings.iter().map(context_warning_json_value)),
        ),
    ])
}

fn context_direction_name(direction: ContextDirection) -> &'static str {
    match direction {
        ContextDirection::In => "in",
        ContextDirection::Out => "out",
        ContextDirection::Both => "both",
    }
}

fn value_json_value(value: &Value) -> JsonValue {
    match value {
        Value::Null => JsonValue::Null,
        Value::Bool(value) => JsonValue::Bool(*value),
        Value::Int(value) => JsonValue::from(*value),
        Value::Float(value) => {
            if value.is_finite() {
                JsonValue::from(*value)
            } else {
                JsonValue::from(value.to_string())
            }
        }
        Value::String(value) => JsonValue::from(value.clone()),
        Value::Bytes(value) => JsonValue::from(format!("{value:?}")),
        Value::Datetime(value) => JsonValue::from(format!("{value:?}")),
        Value::List(values) => JsonValue::array(values.iter().map(value_json_value)),
        Value::Map(entries) => JsonValue::object(
            entries
                .iter()
                .map(|(key, value)| (key.clone(), value_json_value(value))),
        ),
    }
}

impl ContextRequestSummary {
    fn from_request(request: &ContextRequest) -> Self {
        Self {
            depth: request.depth,
            direction: request.direction,
            edge_types: request.edge_types.clone(),
            labels: request.labels.clone(),
            max_nodes: request.max_nodes,
            max_edges: request.max_edges,
        }
    }
}

impl ContextNode {
    fn from_node(node: &crate::Node) -> Self {
        let node_id = node.id().get() as i64;
        let labels = node.labels().iter().cloned().collect::<Vec<_>>();
        let properties = node
            .properties()
            .iter()
            .map(|(key, value)| (key.to_owned(), value.clone()))
            .collect::<BTreeMap<_, _>>();
        let name = string_property(&properties, "name");
        let title = string_property(&properties, "title");
        let src_path = string_property(&properties, "src.path");
        let display = name
            .clone()
            .or_else(|| title.clone())
            .or_else(|| src_path.clone());
        let mut evidence = Vec::new();
        for field in ["name", "title", "src.path"] {
            if let Some(value) = string_property(&properties, field) {
                evidence.push(ContextEvidence {
                    field: field.to_owned(),
                    value,
                    source: format!("property:{field}"),
                });
            }
        }
        if !labels.is_empty() {
            evidence.push(ContextEvidence {
                field: "labels".to_owned(),
                value: labels.join(","),
                source: "labels(n)".to_owned(),
            });
        }

        Self {
            node_id,
            labels,
            properties,
            name,
            title,
            display,
            evidence,
        }
    }
}

impl ContextEdge {
    fn from_edge(edge: &crate::Edge) -> Self {
        Self {
            from_node_id: edge.from().get() as i64,
            to_node_id: edge.to().get() as i64,
            edge_type: edge.edge_type().to_owned(),
            properties: edge
                .properties()
                .iter()
                .map(|(key, value)| (key.to_owned(), value.clone()))
                .collect(),
        }
    }
}

fn string_property(properties: &BTreeMap<String, Value>, key: &str) -> Option<String> {
    match properties.get(key) {
        Some(Value::String(value)) => Some(value.clone()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn traverses_from_seed_with_direction_and_budget() {
        let graph = ContextGraph {
            nodes: BTreeMap::from([
                (1, node(1, &["Person"], Some("Ada"))),
                (2, node(2, &["Person"], Some("Grace"))),
                (3, node(3, &["Doc"], Some("Notes"))),
            ]),
            edges: vec![
                edge(1, 2, "KNOWS"),
                edge(2, 3, "MENTIONS"),
                edge(3, 1, "REFERS_TO"),
            ],
        };
        let request = ContextRequest {
            db_path: PathBuf::from("test.cupld"),
            nodes: vec![1],
            paths: Vec::new(),
            depth: 2,
            direction: ContextDirection::Out,
            edge_types: Vec::new(),
            labels: Vec::new(),
            max_nodes: 2,
            max_edges: 10,
        };

        let response = build_context_response(Path::new("test.cupld"), &request, graph).unwrap();

        assert_eq!(
            response
                .nodes
                .iter()
                .map(|node| node.node_id)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert_eq!(response.edges.len(), 1);
        assert!(response.retrieval_usage.truncated);
    }

    #[test]
    fn builds_context_envelope_with_budgets_and_evidence() {
        let node = node(7, &["Person"], Some("Ada"));
        let graph = ContextGraph {
            nodes: BTreeMap::from([(node.node_id, node)]),
            edges: Vec::new(),
        };
        let request = ContextRequest {
            db_path: PathBuf::from("/tmp/test.cupld"),
            nodes: vec![7],
            paths: Vec::new(),
            depth: 1,
            direction: ContextDirection::Both,
            edge_types: Vec::new(),
            labels: Vec::new(),
            max_nodes: 5,
            max_edges: 5,
        };

        let envelope =
            build_context_response(Path::new("/tmp/test.cupld"), &request, graph).unwrap();

        assert_eq!(envelope.policy.retrieval_budget.unwrap().nodes, 5);
        assert_eq!(envelope.mode, "seeded");
        assert_eq!(envelope.nodes[0].display.as_deref(), Some("Ada"));
        assert!(
            envelope.nodes[0]
                .evidence
                .iter()
                .any(|evidence| evidence.field == "name")
        );

        let parsed = json::parse(&context_as_json(&envelope)).unwrap();
        assert!(parsed.get("items").is_none());
        assert!(parsed.get("snippets").is_none());
        assert_eq!(
            parsed.get("nodes").unwrap().as_array().unwrap()[0]
                .get("node_id")
                .unwrap()
                .as_i64(),
            Some(7)
        );
    }

    fn node(id: i64, labels: &[&str], name: Option<&str>) -> ContextNode {
        let properties = name
            .map(|name| BTreeMap::from([("name".to_owned(), Value::from(name))]))
            .unwrap_or_default();
        let evidence = name
            .map(|name| {
                vec![ContextEvidence {
                    field: "name".to_owned(),
                    value: name.to_owned(),
                    source: "property:name".to_owned(),
                }]
            })
            .unwrap_or_default();
        ContextNode {
            node_id: id,
            labels: labels.iter().map(|label| (*label).to_owned()).collect(),
            properties,
            name: name.map(str::to_owned),
            title: None,
            display: name.map(str::to_owned),
            evidence,
        }
    }

    fn edge(from_node_id: i64, to_node_id: i64, edge_type: &str) -> ContextEdge {
        ContextEdge {
            from_node_id,
            to_node_id,
            edge_type: edge_type.to_owned(),
            properties: BTreeMap::new(),
        }
    }
}
