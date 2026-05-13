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
    pub seeds: Vec<ContextSeedRequest>,
    pub depth: u8,
    pub direction: ContextDirection,
    pub edge_types: Vec<String>,
    pub labels: Vec<String>,
    pub max_nodes: usize,
    pub max_edges: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ContextSeedRequest {
    Node(usize),
    Path(String),
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
    pub depth: u8,
    pub labels: Vec<String>,
    pub properties: BTreeMap<String, Value>,
    pub name: Option<String>,
    pub title: Option<String>,
    pub display: Option<String>,
    pub evidence: Vec<ContextEvidence>,
    pub src_status: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ContextEdge {
    pub edge_id: i64,
    pub source_node_id: i64,
    pub target_node_id: i64,
    pub direction_from_seed: String,
    pub depth: u8,
    pub edge_type: String,
    pub properties: BTreeMap<String, Value>,
    pub evidence: Vec<ContextEvidence>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ContextSeedSummary {
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
    pub seeds: Vec<ContextSeedSummary>,
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
    edges: BTreeMap<i64, ContextEdge>,
    outgoing: BTreeMap<i64, Vec<i64>>,
    incoming: BTreeMap<i64, Vec<i64>>,
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
    let mut edges = BTreeMap::new();
    let mut outgoing: BTreeMap<i64, Vec<i64>> = BTreeMap::new();
    let mut incoming: BTreeMap<i64, Vec<i64>> = BTreeMap::new();
    for edge in snapshot.edges().map(ContextEdge::from_edge) {
        outgoing
            .entry(edge.source_node_id)
            .or_default()
            .push(edge.edge_id);
        incoming
            .entry(edge.target_node_id)
            .or_default()
            .push(edge.edge_id);
        edges.insert(edge.edge_id, edge);
    }

    Ok(ContextGraph {
        nodes,
        edges,
        outgoing,
        incoming,
    })
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
    let (seed_nodes, mut warnings) = resolve_seed_nodes(request, &graph)?;
    let seeds = context_seed_summaries(request, &graph);

    let (node_depths, mut edges, truncated_by_budget) =
        traverse_context(request, &graph, &seed_nodes);
    let mut nodes = node_depths
        .iter()
        .filter_map(|(node_id, depth)| {
            let mut node = graph.nodes.get(node_id).cloned()?;
            node.depth = *depth;
            Some(node)
        })
        .collect::<Vec<_>>();
    let seed_count = seed_nodes.len().min(nodes.len());
    let retained_node_count = retrieval_budget.nodes.max(seed_count).min(nodes.len());
    let mut truncated = truncated_by_budget
        || nodes.len() > retained_node_count
        || retrieval_budget.nodes < seed_count;
    nodes.truncate(retained_node_count);
    let retained_ids = nodes
        .iter()
        .map(|node| node.node_id)
        .collect::<BTreeSet<_>>();
    edges.retain(|edge| {
        retained_ids.contains(&edge.source_node_id) && retained_ids.contains(&edge.target_node_id)
    });
    if edges.len() > retrieval_budget.edges {
        edges.truncate(retrieval_budget.edges);
        truncated = true;
    }
    if truncated
        && !warnings
            .iter()
            .any(|warning| warning.code == "context_budget_truncated")
    {
        warnings.push(ContextWarning {
            code: "context_budget_truncated".to_owned(),
            message: "context traversal was truncated by retrieval budgets".to_owned(),
        });
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
        if payload_bytes <= retrieval_budget.total_payload_bytes
            || response.nodes.len() <= seed_count
        {
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
            retained_ids.contains(&edge.source_node_id)
                && retained_ids.contains(&edge.target_node_id)
        });
        if !warnings
            .iter()
            .any(|warning| warning.code == "context_budget_truncated")
        {
            warnings.push(ContextWarning {
                code: "context_budget_truncated".to_owned(),
                message: "context traversal was truncated by retrieval budgets".to_owned(),
            });
        }
        truncated = true;
    }
}

fn resolve_seed_nodes(
    request: &ContextRequest,
    graph: &ContextGraph,
) -> Result<(Vec<i64>, Vec<ContextWarning>), AutomationError> {
    let requested_seeds = if request.seeds.is_empty() {
        request
            .nodes
            .iter()
            .copied()
            .map(ContextSeedRequest::Node)
            .chain(request.paths.iter().cloned().map(ContextSeedRequest::Path))
            .collect::<Vec<_>>()
    } else {
        request.seeds.clone()
    };
    let mut seed_nodes = Vec::new();
    let mut seen_nodes = BTreeSet::new();
    let mut warnings = Vec::new();

    for seed in requested_seeds {
        let node_id = match seed {
            ContextSeedRequest::Node(node_id) => {
                let node_id = i64::try_from(node_id).map_err(|_| {
                    AutomationError::new(
                        "context_seed_not_found",
                        format!("node seed `{node_id}` was not found"),
                    )
                })?;
                if !graph.nodes.contains_key(&node_id) {
                    return Err(AutomationError::new(
                        "context_seed_not_found",
                        format!("node seed `{node_id}` was not found"),
                    ));
                }
                node_id
            }
            ContextSeedRequest::Path(path) => resolve_path_seed(&path, graph, &mut warnings)?,
        };

        if !seen_nodes.insert(node_id) {
            warnings.push(ContextWarning {
                code: "context_seed_duplicate".to_owned(),
                message: format!("duplicate seed resolved to node `{node_id}`"),
            });
            continue;
        }
        warn_for_seed_source(node_id, graph, &mut warnings);
        seed_nodes.push(node_id);
    }

    Ok((seed_nodes, warnings))
}

fn resolve_path_seed(
    path: &str,
    graph: &ContextGraph,
    warnings: &mut Vec<ContextWarning>,
) -> Result<i64, AutomationError> {
    let matches = graph
        .nodes
        .values()
        .filter(|node| node.src_path() == Some(path))
        .collect::<Vec<_>>();
    match matches.as_slice() {
        [] => Err(AutomationError::new(
            "context_seed_path_not_found",
            format!("path seed `{path}` was not found"),
        )),
        [node] => Ok(node.node_id),
        many => {
            let current = many
                .iter()
                .copied()
                .filter(|node| node.src_status.as_deref() == Some("current"))
                .collect::<Vec<_>>();
            match current.as_slice() {
                [node] => {
                    warnings.push(ContextWarning {
                        code: "context_seed_path_multiple_matches".to_owned(),
                        message: format!(
                            "path seed `{path}` matched multiple nodes; using current node `{}`",
                            node.node_id
                        ),
                    });
                    Ok(node.node_id)
                }
                _ => Err(AutomationError::new(
                    "context_seed_path_ambiguous",
                    format!("path seed `{path}` matched multiple nodes"),
                )),
            }
        }
    }
}

fn warn_for_seed_source(node_id: i64, graph: &ContextGraph, warnings: &mut Vec<ContextWarning>) {
    let Some(node) = graph.nodes.get(&node_id) else {
        return;
    };
    if node.src_path().is_none() && !node.labels.iter().any(|label| label == "MarkdownDocument") {
        return;
    }
    match node.src_status.as_deref() {
        Some("current") => {}
        Some(status) => warnings.push(ContextWarning {
            code: "context_seed_source_stale".to_owned(),
            message: format!("seed node `{node_id}` has src.status `{status}`"),
        }),
        None => warnings.push(ContextWarning {
            code: "context_seed_source_missing".to_owned(),
            message: format!("seed node `{node_id}` has no src.status"),
        }),
    }
}

fn context_seed_summaries(
    request: &ContextRequest,
    graph: &ContextGraph,
) -> Vec<ContextSeedSummary> {
    let mut seeds = Vec::new();
    for node_id in &request.nodes {
        let resolved_node_id = i64::try_from(*node_id).ok();
        seeds.push(ContextSeedSummary {
            kind: "node".to_owned(),
            value: node_id.to_string(),
            node_ids: resolved_node_id
                .filter(|resolved_node_id| graph.nodes.contains_key(resolved_node_id))
                .into_iter()
                .collect(),
        });
    }
    for path in &request.paths {
        seeds.push(ContextSeedSummary {
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
    seeds: &[i64],
) -> (Vec<(i64, u8)>, Vec<ContextEdge>, bool) {
    let max_depth = request.depth;
    let seed_set = seeds.iter().copied().collect::<BTreeSet<_>>();
    let mut node_depths = BTreeMap::new();
    let mut non_seed_node_count = 0usize;
    let mut selected_edges = Vec::new();
    let mut selected_edge_ids = BTreeSet::new();
    let mut queued = VecDeque::new();
    let mut truncated = false;

    for seed in seeds {
        if graph.nodes.contains_key(seed) && !node_depths.contains_key(seed) {
            node_depths.insert(*seed, 0u8);
            queued.push_back((*seed, 0u8, *seed));
        }
    }

    while let Some((node_id, depth, seed_id)) = queued.pop_front() {
        if depth >= max_depth {
            continue;
        }
        for (edge_id, direction_from_seed) in incident_edge_ids(node_id, request, graph) {
            if selected_edges.len() >= request.max_edges {
                truncated = true;
                break;
            }
            let Some(edge) = graph.edges.get(&edge_id) else {
                continue;
            };
            if !edge_type_matches(edge, request) {
                continue;
            }
            let next_id = if edge.source_node_id == node_id {
                edge.target_node_id
            } else {
                edge.source_node_id
            };
            let next_depth = depth + 1;
            if !seed_set.contains(&next_id) && !include_node(next_id, request, graph) {
                continue;
            }
            if selected_edge_ids.insert(edge.edge_id) {
                selected_edges.push(edge.with_traversal(
                    direction_from_seed,
                    next_depth,
                    seed_id,
                    next_id,
                ));
            }
            match node_depths.get(&next_id).copied() {
                Some(existing_depth) if existing_depth <= next_depth => {}
                _ => {
                    if !seed_set.contains(&next_id) && non_seed_node_count >= request.max_nodes {
                        truncated = true;
                        continue;
                    }
                    node_depths.insert(next_id, next_depth);
                    if !seed_set.contains(&next_id) {
                        non_seed_node_count += 1;
                    }
                    queued.push_back((next_id, next_depth, seed_id));
                }
            }
        }
    }

    let mut ordered_nodes = node_depths
        .into_iter()
        .map(|(node_id, depth)| (node_id, depth))
        .collect::<Vec<_>>();
    ordered_nodes.sort_by_key(|(node_id, depth)| (*depth, *node_id));
    selected_edges.sort_by_key(|edge| (edge.depth, edge.edge_id));
    (ordered_nodes, selected_edges, truncated)
}

fn incident_edge_ids(
    node_id: i64,
    request: &ContextRequest,
    graph: &ContextGraph,
) -> Vec<(i64, &'static str)> {
    let mut edge_ids = Vec::new();
    if matches!(
        request.direction,
        ContextDirection::Out | ContextDirection::Both
    ) {
        if let Some(outgoing) = graph.outgoing.get(&node_id) {
            edge_ids.extend(outgoing.iter().copied().map(|edge_id| (edge_id, "out")));
        }
    }
    if matches!(
        request.direction,
        ContextDirection::In | ContextDirection::Both
    ) {
        if let Some(incoming) = graph.incoming.get(&node_id) {
            edge_ids.extend(incoming.iter().copied().map(|edge_id| (edge_id, "in")));
        }
    }
    edge_ids
}

fn edge_type_matches(edge: &ContextEdge, request: &ContextRequest) -> bool {
    request.edge_types.is_empty()
        || request
            .edge_types
            .iter()
            .any(|edge_type| edge_type == &edge.edge_type)
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
        ("depth".to_owned(), JsonValue::from(usize::from(item.depth))),
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
        ("edge_id", JsonValue::from(edge.edge_id)),
        ("type", JsonValue::from(edge.edge_type.clone())),
        ("source_node_id", JsonValue::from(edge.source_node_id)),
        ("target_node_id", JsonValue::from(edge.target_node_id)),
        (
            "direction_from_seed",
            JsonValue::from(edge.direction_from_seed.clone()),
        ),
        ("depth", JsonValue::from(usize::from(edge.depth))),
        ("from_node_id", JsonValue::from(edge.source_node_id)),
        ("to_node_id", JsonValue::from(edge.target_node_id)),
        ("edge_type", JsonValue::from(edge.edge_type.clone())),
        (
            "properties",
            JsonValue::object(
                edge.properties
                    .iter()
                    .map(|(key, value)| (key.clone(), value_json_value(value))),
            ),
        ),
        (
            "evidence",
            JsonValue::array(edge.evidence.iter().map(context_evidence_json_value)),
        ),
    ])
}

fn context_seed_json_value(seed: &ContextSeedSummary) -> JsonValue {
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
        let src_status = string_property(&properties, "src.status");
        let mut evidence = Vec::new();
        for field in ["name", "title", "src.path", "src.status"] {
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
            depth: 0,
            labels,
            properties,
            name,
            title,
            display,
            evidence,
            src_status,
        }
    }

    fn src_path(&self) -> Option<&str> {
        self.evidence
            .iter()
            .find(|evidence| evidence.field == "src.path")
            .map(|evidence| evidence.value.as_str())
    }
}

impl ContextEdge {
    fn from_edge(edge: &crate::Edge) -> Self {
        let edge_id = edge.id().get() as i64;
        let source_node_id = edge.from().get() as i64;
        let target_node_id = edge.to().get() as i64;
        let edge_type = edge.edge_type().to_owned();
        Self {
            edge_id,
            source_node_id,
            target_node_id,
            direction_from_seed: String::new(),
            depth: 0,
            edge_type,
            properties: edge
                .properties()
                .iter()
                .map(|(key, value)| (key.to_owned(), value.clone()))
                .collect(),
            evidence: Vec::new(),
        }
    }

    fn with_traversal(
        &self,
        direction_from_seed: &str,
        depth: u8,
        seed_id: i64,
        neighbor_id: i64,
    ) -> Self {
        let mut edge = self.clone();
        edge.direction_from_seed = direction_from_seed.to_owned();
        edge.depth = depth;
        edge.evidence = vec![
            ContextEvidence {
                field: "edge_id".to_owned(),
                value: edge.edge_id.to_string(),
                source: format!("edge:e{}", edge.edge_id),
            },
            ContextEvidence {
                field: "traversal".to_owned(),
                value: format!(
                    "seed {seed_id} reached node {neighbor_id} via {direction_from_seed} edge at depth {depth}"
                ),
                source: "cupld.context.traversal".to_owned(),
            },
        ];
        edge
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
            ..graph_edges(vec![
                edge(1, 1, 2, "KNOWS"),
                edge(2, 2, 3, "MENTIONS"),
                edge(3, 3, 1, "REFERS_TO"),
            ])
        };
        let request = ContextRequest {
            db_path: PathBuf::from("test.cupld"),
            nodes: vec![1],
            paths: Vec::new(),
            seeds: vec![ContextSeedRequest::Node(1)],
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
        assert_eq!(response.nodes[0].depth, 0);
        assert_eq!(response.nodes[1].depth, 1);
        assert_eq!(response.edges[0].edge_id, 1);
        assert_eq!(response.edges[0].direction_from_seed, "out");
        assert_eq!(response.edges[0].depth, 1);
        assert!(!response.edges[0].evidence.is_empty());
    }

    #[test]
    fn traverses_bfs_depth_filters_cycles_and_budgets_deterministically() {
        let graph = ContextGraph {
            nodes: BTreeMap::from([
                (1, node(1, &["Seed"], Some("Seed"))),
                (2, node(2, &["MarkdownDocument"], Some("Two"))),
                (3, node(3, &["Other"], Some("Three"))),
                (4, node(4, &["MarkdownDocument"], Some("Four"))),
                (5, node(5, &["MarkdownDocument"], Some("Five"))),
            ]),
            ..graph_edges(vec![
                edge(10, 1, 2, "MD_LINKS_TO"),
                edge(11, 2, 4, "MD_LINKS_TO"),
                edge(12, 2, 1, "MD_LINKS_TO"),
                edge(13, 1, 3, "MENTIONS"),
                edge(14, 2, 5, "MD_LINKS_TO"),
            ])
        };
        let request = ContextRequest {
            db_path: PathBuf::from("test.cupld"),
            nodes: vec![1],
            paths: Vec::new(),
            seeds: vec![ContextSeedRequest::Node(1)],
            depth: 2,
            direction: ContextDirection::Out,
            edge_types: vec!["MD_LINKS_TO".to_owned()],
            labels: vec!["MarkdownDocument".to_owned()],
            max_nodes: 4,
            max_edges: 10,
        };

        let response = build_context_response(Path::new("test.cupld"), &request, graph).unwrap();

        assert_eq!(
            response
                .nodes
                .iter()
                .map(|node| (node.node_id, node.depth))
                .collect::<Vec<_>>(),
            vec![(1, 0), (2, 1), (4, 2), (5, 2)]
        );
        assert_eq!(
            response
                .edges
                .iter()
                .map(|edge| (edge.edge_id, edge.depth))
                .collect::<Vec<_>>(),
            vec![(10, 1), (11, 2), (12, 2), (14, 2)]
        );
        assert!(response.warnings.is_empty());
    }

    #[test]
    fn depth_zero_and_small_budgets_preserve_seeds_with_warning() {
        let graph = ContextGraph {
            nodes: BTreeMap::from([
                (1, node(1, &["Seed"], Some("One"))),
                (2, node(2, &["Seed"], Some("Two"))),
                (3, node(3, &["MarkdownDocument"], Some("Three"))),
            ]),
            ..graph_edges(vec![
                edge(1, 1, 3, "MD_LINKS_TO"),
                edge(2, 2, 3, "MD_LINKS_TO"),
            ])
        };
        let request = ContextRequest {
            db_path: PathBuf::from("test.cupld"),
            nodes: vec![1, 2],
            paths: Vec::new(),
            seeds: vec![ContextSeedRequest::Node(1), ContextSeedRequest::Node(2)],
            depth: 0,
            direction: ContextDirection::Out,
            edge_types: Vec::new(),
            labels: vec!["MarkdownDocument".to_owned()],
            max_nodes: 1,
            max_edges: 10,
        };

        let response = build_context_response(Path::new("test.cupld"), &request, graph).unwrap();

        assert_eq!(
            response
                .nodes
                .iter()
                .map(|node| (node.node_id, node.depth))
                .collect::<Vec<_>>(),
            vec![(1, 0), (2, 0)]
        );
        assert!(response.edges.is_empty());
        assert!(
            response
                .warnings
                .iter()
                .any(|warning| warning.code == "context_budget_truncated")
        );
    }

    #[test]
    fn builds_context_envelope_with_budgets_and_evidence() {
        let node = node(7, &["Person"], Some("Ada"));
        let graph = ContextGraph {
            nodes: BTreeMap::from([(node.node_id, node)]),
            ..graph_edges(Vec::new())
        };
        let request = ContextRequest {
            db_path: PathBuf::from("/tmp/test.cupld"),
            nodes: vec![7],
            paths: Vec::new(),
            seeds: vec![ContextSeedRequest::Node(7)],
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
            depth: 0,
            labels: labels.iter().map(|label| (*label).to_owned()).collect(),
            properties,
            name: name.map(str::to_owned),
            title: None,
            display: name.map(str::to_owned),
            evidence,
            src_status: None,
        }
    }

    fn edge(
        edge_id: i64,
        source_node_id: i64,
        target_node_id: i64,
        edge_type: &str,
    ) -> ContextEdge {
        ContextEdge {
            edge_id,
            source_node_id,
            target_node_id,
            direction_from_seed: String::new(),
            depth: 0,
            edge_type: edge_type.to_owned(),
            properties: BTreeMap::new(),
            evidence: Vec::new(),
        }
    }

    fn graph_edges(edges: Vec<ContextEdge>) -> ContextGraph {
        let mut graph = ContextGraph {
            nodes: BTreeMap::new(),
            edges: BTreeMap::new(),
            outgoing: BTreeMap::new(),
            incoming: BTreeMap::new(),
        };
        for edge in edges {
            graph
                .outgoing
                .entry(edge.source_node_id)
                .or_default()
                .push(edge.edge_id);
            graph
                .incoming
                .entry(edge.target_node_id)
                .or_default()
                .push(edge.edge_id);
            graph.edges.insert(edge.edge_id, edge);
        }
        graph
    }
}
