use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::path::{Path, PathBuf};

use crate::automation::{
    AutomationError, AutomationPolicy, RetrievalUsage, automation_policy_json_value,
    retrieval_usage_json_value,
};
use crate::json::{self, JsonValue};
use crate::{QueryResult, RuntimeValue, Session};

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
    pub seeds: Vec<ContextSeed>,
    pub depth: u8,
    pub direction: ContextDirection,
    pub edge_types: Vec<String>,
    pub labels: Vec<String>,
    pub max_nodes: usize,
    pub max_edges: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ContextSeed {
    Node(usize),
    Path(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ContextEvidence {
    pub field: String,
    pub value: String,
    pub source: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ContextNode {
    pub node_id: i64,
    pub labels: Vec<String>,
    pub name: Option<String>,
    pub title: Option<String>,
    pub display: Option<String>,
    pub evidence: Vec<ContextEvidence>,
    pub src_status: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ContextEdge {
    pub from_node_id: i64,
    pub to_node_id: i64,
    pub edge_type: String,
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ContextEnvelope {
    pub ok: bool,
    pub command: String,
    pub policy: AutomationPolicy,
    pub retrieval_usage: RetrievalUsage,
    pub provenance: ContextProvenance,
    pub items: Vec<ContextNode>,
    pub edges: Vec<ContextEdge>,
    pub warnings: Vec<ContextWarning>,
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

    for (item_index, item) in response.items.iter().enumerate() {
        lines.push(json::stringify(&JsonValue::object([
            ("kind", JsonValue::from("context_item")),
            ("item_index", JsonValue::from(item_index)),
            ("item", context_node_json_value(item)),
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
            .items
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
    let node_result = single_result(
        session,
        "MATCH (n) RETURN id(n) AS node_id, labels(n) AS labels, n.name AS name, n.title AS title, n.`src.path` AS src_path, n.`src.status` AS src_status ORDER BY id(n)",
    )?;
    let edge_result = single_result(
        session,
        "MATCH (a)-[e]->(b) RETURN id(a) AS from_node_id, id(b) AS to_node_id, edge_type(e) AS edge_type ORDER BY id(a), id(b), edge_type(e)",
    )?;
    let nodes = node_result
        .rows
        .iter()
        .map(|row| parse_context_node(&node_result.columns, row).map(|node| (node.node_id, node)))
        .collect::<Result<BTreeMap<_, _>, _>>()?;
    let edges = edge_result
        .rows
        .iter()
        .map(|row| parse_context_edge(&edge_result.columns, row))
        .collect::<Result<Vec<_>, _>>()?;

    Ok(ContextGraph { nodes, edges })
}

fn single_result(session: &mut Session, query: &str) -> Result<QueryResult, AutomationError> {
    let mut results = session
        .execute_script(query, &BTreeMap::new())
        .map_err(AutomationError::from)?;
    results.pop().ok_or_else(|| {
        AutomationError::new("context_contract", "context query returned no result set")
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
    let (seed_nodes, warnings) = resolve_seed_nodes(request, &graph)?;

    let (node_ids, mut edges, truncated_by_budget) = traverse_context(request, &graph, &seed_nodes);
    let mut items = node_ids
        .iter()
        .filter_map(|node_id| graph.nodes.get(node_id).cloned())
        .collect::<Vec<_>>();
    let mut truncated = truncated_by_budget || items.len() > retrieval_budget.nodes;
    items.truncate(retrieval_budget.nodes);
    let retained_ids = items
        .iter()
        .map(|item| item.node_id)
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
            policy,
            retrieval_usage: RetrievalUsage {
                nodes: items.len(),
                edges: edges.len(),
                snippet_bytes: 0,
                total_payload_bytes: 0,
                truncated,
            },
            provenance: ContextProvenance {
                db_path: db_path.display().to_string(),
                source: "cupld.context".to_owned(),
            },
            items: items.clone(),
            edges: edges.clone(),
            warnings: warnings.clone(),
        };
        let payload_bytes = context_payload_bytes(&mut response, &mut buffer);
        if payload_bytes <= retrieval_budget.total_payload_bytes || response.items.is_empty() {
            return Ok(response);
        }
        response.items.pop();
        let retained_ids = response
            .items
            .iter()
            .map(|item| item.node_id)
            .collect::<BTreeSet<_>>();
        items = response.items;
        edges.retain(|edge| {
            retained_ids.contains(&edge.from_node_id) && retained_ids.contains(&edge.to_node_id)
        });
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
            .map(ContextSeed::Node)
            .chain(request.paths.iter().cloned().map(ContextSeed::Path))
            .collect::<Vec<_>>()
    } else {
        request.seeds.clone()
    };
    let mut seed_nodes = Vec::new();
    let mut seen_nodes = BTreeSet::new();
    let mut warnings = Vec::new();

    for seed in requested_seeds {
        let node_id = match seed {
            ContextSeed::Node(node_id) => {
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
            ContextSeed::Path(path) => resolve_path_seed(&path, graph, &mut warnings)?,
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

fn traverse_context(
    request: &ContextRequest,
    graph: &ContextGraph,
    seeds: &[i64],
) -> (Vec<i64>, Vec<ContextEdge>, bool) {
    let mut visited = BTreeSet::new();
    let mut ordered_nodes = Vec::new();
    let mut selected_edges = Vec::new();
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
            selected_edges.push(edge.clone());
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
            "items",
            JsonValue::array(response.items.iter().map(context_node_json_value)),
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

fn parse_context_node(
    columns: &[String],
    row: &[RuntimeValue],
) -> Result<ContextNode, AutomationError> {
    let node_id = expect_int(columns, row, "node_id")?;
    let labels = expect_string_list(columns, row, "labels")?;
    let name = optional_string(columns, row, "name")?;
    let title = optional_string(columns, row, "title")?;
    let src_path = optional_string(columns, row, "src_path")?;
    let src_status = optional_string(columns, row, "src_status")?;
    let display = name
        .clone()
        .or_else(|| title.clone())
        .or_else(|| src_path.clone());
    let mut evidence = Vec::new();
    if let Some(name) = &name {
        evidence.push(ContextEvidence {
            field: "name".to_owned(),
            value: name.clone(),
            source: "property:name".to_owned(),
        });
    }
    if let Some(title) = &title {
        evidence.push(ContextEvidence {
            field: "title".to_owned(),
            value: title.clone(),
            source: "property:title".to_owned(),
        });
    }
    if let Some(src_path) = &src_path {
        evidence.push(ContextEvidence {
            field: "src.path".to_owned(),
            value: src_path.clone(),
            source: "property:src.path".to_owned(),
        });
    }
    if let Some(src_status) = &src_status {
        evidence.push(ContextEvidence {
            field: "src.status".to_owned(),
            value: src_status.clone(),
            source: "property:src.status".to_owned(),
        });
    }
    if !labels.is_empty() {
        evidence.push(ContextEvidence {
            field: "labels".to_owned(),
            value: labels.join(","),
            source: "labels(n)".to_owned(),
        });
    }

    Ok(ContextNode {
        node_id,
        labels,
        name,
        title,
        display,
        evidence,
        src_status,
    })
}

impl ContextNode {
    fn src_path(&self) -> Option<&str> {
        self.evidence
            .iter()
            .find(|evidence| evidence.field == "src.path")
            .map(|evidence| evidence.value.as_str())
    }
}

fn parse_context_edge(
    columns: &[String],
    row: &[RuntimeValue],
) -> Result<ContextEdge, AutomationError> {
    Ok(ContextEdge {
        from_node_id: expect_int(columns, row, "from_node_id")?,
        to_node_id: expect_int(columns, row, "to_node_id")?,
        edge_type: expect_string(columns, row, "edge_type")?,
    })
}

fn column_index(columns: &[String], expected: &str) -> Result<usize, AutomationError> {
    columns
        .iter()
        .position(|column| column == expected)
        .ok_or_else(|| {
            AutomationError::new(
                "context_contract",
                format!("missing expected `{expected}` column in context result"),
            )
        })
}

fn expect_int(
    columns: &[String],
    row: &[RuntimeValue],
    column: &str,
) -> Result<i64, AutomationError> {
    let index = column_index(columns, column)?;
    match row.get(index) {
        Some(RuntimeValue::Int(value)) => Ok(*value),
        Some(other) => Err(AutomationError::new(
            "context_contract",
            format!("expected `{column}` to be an integer, found {other:?}"),
        )),
        None => Err(AutomationError::new(
            "context_contract",
            format!("missing value for `{column}` in context result row"),
        )),
    }
}

fn expect_string(
    columns: &[String],
    row: &[RuntimeValue],
    column: &str,
) -> Result<String, AutomationError> {
    let index = column_index(columns, column)?;
    match row.get(index) {
        Some(RuntimeValue::String(value)) => Ok(value.clone()),
        Some(other) => Err(AutomationError::new(
            "context_contract",
            format!("expected `{column}` to be a string, found {other:?}"),
        )),
        None => Err(AutomationError::new(
            "context_contract",
            format!("missing value for `{column}` in context result row"),
        )),
    }
}

fn expect_string_list(
    columns: &[String],
    row: &[RuntimeValue],
    column: &str,
) -> Result<Vec<String>, AutomationError> {
    let index = column_index(columns, column)?;
    match row.get(index) {
        Some(RuntimeValue::List(values)) => values
            .iter()
            .map(|value| match value {
                RuntimeValue::String(value) => Ok(value.clone()),
                other => Err(AutomationError::new(
                    "context_contract",
                    format!("expected `{column}` items to be strings, found {other:?}"),
                )),
            })
            .collect(),
        Some(other) => Err(AutomationError::new(
            "context_contract",
            format!("expected `{column}` to be a list, found {other:?}"),
        )),
        None => Err(AutomationError::new(
            "context_contract",
            format!("missing value for `{column}` in context result row"),
        )),
    }
}

fn optional_string(
    columns: &[String],
    row: &[RuntimeValue],
    column: &str,
) -> Result<Option<String>, AutomationError> {
    let index = column_index(columns, column)?;
    match row.get(index) {
        Some(RuntimeValue::Null) => Ok(None),
        Some(RuntimeValue::String(value)) => Ok(Some(value.clone())),
        Some(other) => Err(AutomationError::new(
            "context_contract",
            format!("expected `{column}` to be a string or null, found {other:?}"),
        )),
        None => Err(AutomationError::new(
            "context_contract",
            format!("missing value for `{column}` in context result row"),
        )),
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
            seeds: vec![ContextSeed::Node(1)],
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
                .items
                .iter()
                .map(|item| item.node_id)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert_eq!(response.edges.len(), 1);
        assert!(response.retrieval_usage.truncated);
    }

    #[test]
    fn builds_context_envelope_with_budgets_and_evidence() {
        let result = QueryResult {
            columns: vec![
                "node_id".to_owned(),
                "labels".to_owned(),
                "name".to_owned(),
                "title".to_owned(),
                "src_path".to_owned(),
                "src_status".to_owned(),
            ],
            rows: vec![vec![
                RuntimeValue::Int(7),
                RuntimeValue::List(vec![RuntimeValue::String("Person".to_owned())]),
                RuntimeValue::String("Ada".to_owned()),
                RuntimeValue::Null,
                RuntimeValue::Null,
                RuntimeValue::Null,
            ]],
        };

        let node = parse_context_node(&result.columns, &result.rows[0]).unwrap();
        let graph = ContextGraph {
            nodes: BTreeMap::from([(node.node_id, node)]),
            edges: Vec::new(),
        };
        let request = ContextRequest {
            db_path: PathBuf::from("/tmp/test.cupld"),
            nodes: vec![7],
            paths: Vec::new(),
            seeds: vec![ContextSeed::Node(7)],
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
        assert_eq!(envelope.items[0].display.as_deref(), Some("Ada"));
        assert!(
            envelope.items[0]
                .evidence
                .iter()
                .any(|evidence| evidence.field == "name")
        );

        let parsed = json::parse(&context_as_json(&envelope)).unwrap();
        assert_eq!(
            parsed.get("items").unwrap().as_array().unwrap()[0]
                .get("node_id")
                .unwrap()
                .as_i64(),
            Some(7)
        );
    }

    fn node(id: i64, labels: &[&str], name: Option<&str>) -> ContextNode {
        ContextNode {
            node_id: id,
            labels: labels.iter().map(|label| (*label).to_owned()).collect(),
            name: name.map(str::to_owned),
            title: None,
            display: name.map(str::to_owned),
            evidence: Vec::new(),
            src_status: None,
        }
    }

    fn edge(from_node_id: i64, to_node_id: i64, edge_type: &str) -> ContextEdge {
        ContextEdge {
            from_node_id,
            to_node_id,
            edge_type: edge_type.to_owned(),
        }
    }
}
