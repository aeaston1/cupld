use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io;
use std::path::{Component, Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant, UNIX_EPOCH};

use crate::engine::{CupldEngine, EdgeId, GraphError, NodeId, PropertyMap, Value};

const MARKDOWN_DOCUMENT_LABEL: &str = "MarkdownDocument";
pub const MARKDOWN_DIRECTORY_LABEL: &str = "MarkdownDirectory";
const CONFIG_LABEL: &str = "SystemConfig";
const CONFIG_KIND: &str = "config";
const CONFIG_NAME: &str = "markdown_source";
const CONNECTOR_NAME: &str = "markdown";
const LINK_EDGE_TYPE: &str = "MD_LINKS_TO";
pub const MD_IN_DIRECTORY: &str = "MD_IN_DIRECTORY";
pub const MD_PARENT_DIRECTORY: &str = "MD_PARENT_DIRECTORY";
const STRUCTURAL_EDGE_TYPES: [&str; 2] = [MD_IN_DIRECTORY, MD_PARENT_DIRECTORY];

#[derive(Clone, Debug, PartialEq)]
pub struct MarkdownDocument {
    pub path: PathBuf,
    pub raw: String,
    pub body: String,
    pub frontmatter: Option<PropertyMap>,
    pub title: String,
    pub tags: Vec<String>,
    pub aliases: Vec<String>,
    pub links: Vec<String>,
    pub headings: Vec<String>,
    pub source_hash: String,
    pub has_frontmatter: bool,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MarkdownSyncOptions {
    pub filesystem_graph: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct MarkdownLinkRef {
    raw_target: String,
    source: MarkdownLinkSource,
    relation: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MarkdownLinkSource {
    Body,
    Frontmatter,
}

impl MarkdownLinkSource {
    fn as_str(self) -> &'static str {
        match self {
            Self::Body => "body",
            Self::Frontmatter => "frontmatter",
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct MarkdownResolutionIndex {
    direct: BTreeMap<String, String>,
    derived_paths: BTreeMap<String, Option<String>>,
    aliases: BTreeMap<String, Option<String>>,
    slugs: BTreeMap<String, Option<String>>,
    case_folded_paths: BTreeMap<String, Option<String>>,
    path_style_paths: BTreeMap<String, Option<String>>,
}

impl MarkdownResolutionIndex {
    fn insert_direct(&mut self, key: String, target: String) {
        self.direct.entry(key).or_insert(target);
    }

    fn insert_derived_path(&mut self, key: &str, target: &str) {
        insert_collision_aware(&mut self.derived_paths, key, target);
    }

    fn insert_alias(&mut self, key: &str, target: &str) {
        insert_collision_aware(&mut self.aliases, key, target);
    }

    fn insert_slug(&mut self, key: &str, target: &str) {
        insert_collision_aware(&mut self.slugs, key, target);
    }

    fn insert_case_folded_path(&mut self, key: &str, target: &str) {
        let folded = key.trim().to_lowercase();
        insert_collision_aware(&mut self.case_folded_paths, &folded, target);
    }

    fn insert_path_style_path(&mut self, key: &str, target: &str) {
        let Some(key) = path_style_key(key) else {
            return;
        };
        insert_collision_aware(&mut self.path_style_paths, &key, target);
    }

    fn resolve_direct(&self, key: &str) -> Option<String> {
        self.direct.get(key).cloned()
    }

    fn resolve_derived_path(&self, key: &str) -> Option<String> {
        resolve_collision_aware(&self.derived_paths, key)
    }

    fn resolve_alias(&self, key: &str) -> Option<String> {
        resolve_collision_aware(&self.aliases, key)
    }

    fn resolve_slug(&self, key: &str) -> Option<String> {
        resolve_collision_aware(&self.slugs, key)
    }

    fn resolve_case_folded_path(&self, key: &str) -> Option<String> {
        resolve_collision_aware(&self.case_folded_paths, &key.trim().to_lowercase())
    }

    fn resolve_path_style_path(&self, key: &str) -> Option<String> {
        let key = path_style_key(key)?;
        resolve_collision_aware(&self.path_style_paths, &key)
    }
}

fn insert_collision_aware(map: &mut BTreeMap<String, Option<String>>, key: &str, target: &str) {
    let trimmed = key.trim();
    if trimmed.is_empty() {
        return;
    }
    match map.get(trimmed) {
        Some(Some(existing)) if existing != target => {
            map.insert(trimmed.to_owned(), None);
        }
        Some(None) | Some(Some(_)) => {}
        None => {
            map.insert(trimmed.to_owned(), Some(target.to_owned()));
        }
    }
}

fn resolve_collision_aware(map: &BTreeMap<String, Option<String>>, key: &str) -> Option<String> {
    map.get(key.trim()).and_then(|target| target.clone())
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct ResolvedMarkdownTarget {
    raw_targets: Vec<String>,
    sources: Vec<String>,
    relations: Vec<String>,
}

impl ResolvedMarkdownTarget {
    fn record(&mut self, link_ref: &MarkdownLinkRef) {
        push_unique(&mut self.raw_targets, link_ref.raw_target.clone());
        push_unique(&mut self.sources, link_ref.source.as_str().to_owned());
        if let Some(relation) = &link_ref.relation {
            push_unique(&mut self.relations, relation.clone());
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MarkdownSyncReport {
    pub root: PathBuf,
    pub scanned_documents: usize,
    pub upserted_documents: usize,
    pub tombstoned_documents: usize,
    pub link_edges: usize,
    pub upserted_directories: usize,
    pub tombstoned_directories: usize,
    pub structural_edges: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MarkdownWatchOptions {
    pub poll_interval: Duration,
    pub debounce: Duration,
    pub max_batch_window: Duration,
    pub idle_timeout: Option<Duration>,
    pub max_runs: Option<usize>,
}

impl Default for MarkdownWatchOptions {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_millis(100),
            debounce: Duration::from_millis(200),
            max_batch_window: Duration::from_secs(2),
            idle_timeout: None,
            max_runs: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MarkdownWatchReport {
    pub root: PathBuf,
    pub sync_runs: usize,
    pub events_seen: usize,
    pub last_report: Option<MarkdownSyncReport>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum SourceErrorKind {
    Io,
    Graph(GraphError),
    NodeNotFound,
}

impl SourceErrorKind {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Io => "io_error",
            Self::Graph(error) => error.code(),
            Self::NodeNotFound => "node_not_found",
        }
    }
}

impl From<&'static str> for SourceErrorKind {
    fn from(value: &'static str) -> Self {
        match value {
            "io_error" => Self::Io,
            "node_not_found" => Self::NodeNotFound,
            _ => panic!("unknown source error code: {value}"),
        }
    }
}

#[derive(Clone, Debug)]
pub struct SourceError {
    kind: SourceErrorKind,
    message: String,
}

impl SourceError {
    fn new(kind: impl Into<SourceErrorKind>, message: impl Into<String>) -> Self {
        Self {
            kind: kind.into(),
            message: message.into(),
        }
    }

    pub fn code(&self) -> &'static str {
        self.kind.as_str()
    }
}

impl std::fmt::Display for SourceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code(), self.message)
    }
}

impl std::error::Error for SourceError {}

impl From<io::Error> for SourceError {
    fn from(value: io::Error) -> Self {
        Self::new(SourceErrorKind::Io, value.to_string())
    }
}

impl From<GraphError> for SourceError {
    fn from(value: GraphError) -> Self {
        let message = value.to_string();
        Self::new(SourceErrorKind::Graph(value), message)
    }
}

pub fn configured_markdown_root(engine: &CupldEngine) -> Option<PathBuf> {
    let node_id = find_config_node(engine)?;
    let node = engine.node(node_id)?;
    match node.property("src.root") {
        Some(Value::String(value)) => Some(PathBuf::from(value)),
        _ => None,
    }
}

pub fn set_markdown_root(engine: &mut CupldEngine, root: &Path) -> Result<(), SourceError> {
    let root = normalize_root_path(root)?;
    let root_string = path_to_string(&root);
    let config_node = match find_config_node(engine) {
        Some(node_id) => node_id,
        None => engine.create_node(
            [CONFIG_LABEL],
            PropertyMap::from_pairs([
                ("sys.kind", Value::from(CONFIG_KIND)),
                ("sys.name", Value::from(CONFIG_NAME)),
            ]),
        )?,
    };
    engine.set_node_property(config_node, "sys.kind", Value::from(CONFIG_KIND))?;
    engine.set_node_property(config_node, "sys.name", Value::from(CONFIG_NAME))?;
    engine.set_node_property(config_node, "src.connector", Value::from(CONNECTOR_NAME))?;
    engine.set_node_property(config_node, "src.kind", Value::from(CONFIG_KIND))?;
    engine.set_node_property(config_node, "src.root", Value::from(root_string))?;
    Ok(())
}

pub fn sync_markdown_root(
    engine: &mut CupldEngine,
    root: &Path,
) -> Result<MarkdownSyncReport, SourceError> {
    sync_markdown_root_with_options(engine, root, &MarkdownSyncOptions::default())
}

pub fn sync_markdown_root_with_options(
    engine: &mut CupldEngine,
    root: &Path,
    options: &MarkdownSyncOptions,
) -> Result<MarkdownSyncReport, SourceError> {
    let root = normalize_root_path(root)?;
    let documents = scan_markdown_root(&root)?;
    let root_string = path_to_string(&root);

    let mut existing_docs = collect_existing_documents(engine, &root_string);
    let mut upserted_directories = 0;
    let mut tombstoned_directories = 0;
    let mut structural_edges = 0;
    let doc_node_ids = upsert_documents(engine, &root_string, &documents, &mut existing_docs)?;
    if options.filesystem_graph {
        let mut existing_dirs = collect_existing_directories(engine, &root_string);
        let dir_node_ids =
            upsert_directories(engine, &root_string, &documents, &mut existing_dirs)?;
        upserted_directories = dir_node_ids.len();
        structural_edges = sync_structural_edges(
            engine,
            &root_string,
            &documents,
            &doc_node_ids,
            &dir_node_ids,
        )?;
        tombstoned_directories = tombstone_missing_directories(engine, &existing_dirs)?;
    }
    let link_edges = sync_link_edges(engine, &documents, &doc_node_ids)?;
    let tombstoned_documents = tombstone_missing_documents(engine, &existing_docs)?;

    Ok(MarkdownSyncReport {
        root,
        scanned_documents: documents.len(),
        upserted_documents: doc_node_ids.len(),
        tombstoned_documents,
        link_edges,
        upserted_directories,
        tombstoned_directories,
        structural_edges,
    })
}

pub fn watch_markdown_root(
    engine: &mut CupldEngine,
    root: &Path,
    options: &MarkdownWatchOptions,
) -> Result<MarkdownWatchReport, SourceError> {
    watch_markdown_root_with_sync_options(engine, root, &MarkdownSyncOptions::default(), options)
}

pub fn watch_markdown_root_with_sync_options(
    engine: &mut CupldEngine,
    root: &Path,
    sync_options: &MarkdownSyncOptions,
    options: &MarkdownWatchOptions,
) -> Result<MarkdownWatchReport, SourceError> {
    let root = normalize_root_path(root)?;
    let mut last_report = Some(sync_markdown_root_with_options(
        engine,
        &root,
        sync_options,
    )?);
    let mut report = MarkdownWatchReport {
        root: root.clone(),
        sync_runs: 1,
        events_seen: 0,
        last_report: last_report.clone(),
    };
    if options.max_runs == Some(1) {
        return Ok(report);
    }

    let mut snapshot = snapshot_markdown_root(&root)?;
    let mut batcher = WatchBatcher::default();
    let mut last_idle = Instant::now();

    loop {
        if let Some(idle_timeout) = options.idle_timeout
            && batcher.is_idle()
            && last_idle.elapsed() >= idle_timeout
        {
            report.last_report = last_report;
            return Ok(report);
        }

        thread::sleep(options.poll_interval);
        let current = snapshot_markdown_root(&root)?;
        let now = Instant::now();
        if current != snapshot {
            batcher.record_change(now);
            report.events_seen += 1;
            snapshot = current;
            last_idle = now;
        }

        if batcher.should_flush(now, options) {
            let sync_report = sync_markdown_root_with_options(engine, &root, sync_options)?;
            report.sync_runs += 1;
            last_report = Some(sync_report.clone());
            batcher.flush();
            last_idle = now;
            snapshot = snapshot_markdown_root(&root)?;
            if options
                .max_runs
                .is_some_and(|max_runs| report.sync_runs >= max_runs)
            {
                report.last_report = last_report;
                return Ok(report);
            }
        }
    }
}

fn collect_existing_documents(engine: &CupldEngine, root: &str) -> BTreeMap<String, NodeId> {
    let mut documents = BTreeMap::new();
    for node in engine.nodes() {
        let Some(Value::String(connector)) = node.property("src.connector") else {
            continue;
        };
        if connector != CONNECTOR_NAME {
            continue;
        }
        let Some(Value::String(kind)) = node.property("src.kind") else {
            continue;
        };
        if kind != "document" {
            continue;
        }
        let Some(Value::String(node_root)) = node.property("src.root") else {
            continue;
        };
        if node_root != root {
            continue;
        }
        let Some(Value::String(path)) = node.property("src.path") else {
            continue;
        };
        documents.insert(path.clone(), node.id());
    }
    documents
}

fn collect_existing_directories(engine: &CupldEngine, root: &str) -> BTreeMap<String, NodeId> {
    let mut directories = BTreeMap::new();
    for node in engine.nodes() {
        let Some(Value::String(connector)) = node.property("src.connector") else {
            continue;
        };
        if connector != CONNECTOR_NAME {
            continue;
        }
        let Some(Value::String(kind)) = node.property("src.kind") else {
            continue;
        };
        if kind != "directory" {
            continue;
        }
        let Some(Value::String(node_root)) = node.property("src.root") else {
            continue;
        };
        if node_root != root {
            continue;
        }
        let Some(Value::String(path)) = node.property("src.path") else {
            continue;
        };
        directories.insert(source_key(node_root, path), node.id());
    }
    directories
}

fn upsert_documents(
    engine: &mut CupldEngine,
    root: &str,
    documents: &[MarkdownDocument],
    existing_docs: &mut BTreeMap<String, NodeId>,
) -> Result<BTreeMap<String, NodeId>, SourceError> {
    let mut node_ids = BTreeMap::new();

    for document in documents {
        let relative = path_to_string(&document.path);
        let node_id = match existing_docs.remove(&relative) {
            Some(node_id) => {
                merge_document_properties(engine, node_id, root, document)?;
                node_id
            }
            None => {
                let properties = document_properties(root, document);
                engine.create_node([MARKDOWN_DOCUMENT_LABEL], properties)?
            }
        };
        node_ids.insert(relative, node_id);
    }

    Ok(node_ids)
}

fn upsert_directories(
    engine: &mut CupldEngine,
    root: &str,
    documents: &[MarkdownDocument],
    existing_dirs: &mut BTreeMap<String, NodeId>,
) -> Result<BTreeMap<String, NodeId>, SourceError> {
    let mut node_ids = BTreeMap::new();

    for directory in markdown_directories(documents) {
        let relative = directory_path_string(&directory);
        let key = source_key(root, &relative);
        let node_id = match existing_dirs.remove(&key) {
            Some(node_id) => {
                merge_directory_properties(engine, node_id, root, &directory)?;
                node_id
            }
            None => {
                let properties = directory_properties(root, &directory);
                engine.create_node([MARKDOWN_DIRECTORY_LABEL], properties)?
            }
        };
        node_ids.insert(key, node_id);
    }

    Ok(node_ids)
}

fn merge_document_properties(
    engine: &mut CupldEngine,
    node_id: NodeId,
    root: &str,
    document: &MarkdownDocument,
) -> Result<(), SourceError> {
    let Some(node) = engine.node(node_id) else {
        return Err(SourceError::new(
            "node_not_found",
            "document node disappeared",
        ));
    };
    let mut properties = node.properties().clone();
    let removable = properties
        .keys()
        .filter(|key| key.starts_with("src.") || key.starts_with("md."))
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    for key in removable {
        properties.remove(&key);
    }
    for (key, value) in document_properties(root, document).into_iter() {
        properties.insert(key, value);
    }
    engine.replace_node_properties(node_id, properties)?;
    Ok(())
}

fn merge_directory_properties(
    engine: &mut CupldEngine,
    node_id: NodeId,
    root: &str,
    directory: &Path,
) -> Result<(), SourceError> {
    let Some(node) = engine.node(node_id) else {
        return Err(SourceError::new(
            "node_not_found",
            "directory node disappeared",
        ));
    };
    let mut properties = node.properties().clone();
    let removable = properties
        .keys()
        .filter(|key| key.starts_with("src.") || *key == "name" || *key == "title")
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    for key in removable {
        properties.remove(&key);
    }
    for (key, value) in directory_properties(root, directory).into_iter() {
        properties.insert(key, value);
    }
    engine.replace_node_properties(node_id, properties)?;
    Ok(())
}

fn document_properties(root: &str, document: &MarkdownDocument) -> PropertyMap {
    let mut properties = PropertyMap::from_pairs([
        ("src.connector", Value::from(CONNECTOR_NAME)),
        ("src.kind", Value::from("document")),
        ("src.root", Value::from(root.to_owned())),
        ("src.path", Value::from(path_to_string(&document.path))),
        ("src.hash", Value::from(document.source_hash.clone())),
        ("src.status", Value::from("current")),
        ("md.raw", Value::from(document.raw.clone())),
        ("md.body", Value::from(document.body.clone())),
        ("md.title", Value::from(document.title.clone())),
        ("md.has_frontmatter", Value::from(document.has_frontmatter)),
        ("md.tags", list_value(&document.tags)),
        ("md.aliases", list_value(&document.aliases)),
        ("md.links", list_value(&document.links)),
        ("md.headings", list_value(&document.headings)),
    ]);
    if let Some(frontmatter) = &document.frontmatter {
        properties.insert("md.frontmatter", Value::from(frontmatter.clone()));
    }
    properties
}

fn directory_properties(root: &str, directory: &Path) -> PropertyMap {
    let path = directory_path_string(directory);
    let name = directory_name(directory);
    let title = directory_title(&name);
    PropertyMap::from_pairs([
        ("src.connector", Value::from(CONNECTOR_NAME)),
        ("src.kind", Value::from("directory")),
        ("src.root", Value::from(root.to_owned())),
        ("src.path", Value::from(path)),
        ("src.status", Value::from("current")),
        ("name", Value::from(name)),
        ("title", Value::from(title)),
    ])
}

fn sync_structural_edges(
    engine: &mut CupldEngine,
    root: &str,
    documents: &[MarkdownDocument],
    doc_node_ids: &BTreeMap<String, NodeId>,
    dir_node_ids: &BTreeMap<String, NodeId>,
) -> Result<usize, SourceError> {
    let mut created_edges = 0;

    for directory in markdown_directories(documents) {
        let directory_key = directory_path_string(&directory);
        let source_directory_key = source_key(root, &directory_key);
        let Some(directory_id) = dir_node_ids.get(&source_directory_key).copied() else {
            continue;
        };
        delete_connector_edges_of_types(engine, directory_id, &STRUCTURAL_EDGE_TYPES)?;

        let Some(parent_key) = parent_directory_key(&directory) else {
            continue;
        };
        let source_parent_key = source_key(root, &parent_key);
        let Some(parent_id) = dir_node_ids.get(&source_parent_key).copied() else {
            continue;
        };
        engine.create_edge(
            directory_id,
            parent_id,
            MD_PARENT_DIRECTORY,
            structural_edge_properties(root),
        )?;
        created_edges += 1;
    }

    for document in documents {
        let document_key = path_to_string(&document.path);
        let Some(document_id) = doc_node_ids.get(&document_key).copied() else {
            continue;
        };
        delete_connector_edges_of_types(engine, document_id, &STRUCTURAL_EDGE_TYPES)?;

        let directory_key = document_directory_key(&document.path);
        let source_directory_key = source_key(root, &directory_key);
        let Some(directory_id) = dir_node_ids.get(&source_directory_key).copied() else {
            continue;
        };
        engine.create_edge(
            document_id,
            directory_id,
            MD_IN_DIRECTORY,
            structural_edge_properties(root),
        )?;
        created_edges += 1;
    }

    Ok(created_edges)
}

fn structural_edge_properties(root: &str) -> PropertyMap {
    PropertyMap::from_pairs([
        ("src.connector", Value::from(CONNECTOR_NAME)),
        ("src.kind", Value::from("structural_edge")),
        ("src.root", Value::from(root.to_owned())),
        ("src.status", Value::from("current")),
        ("md.edge_source", Value::from("filesystem")),
        ("md.edge_weight", Value::from(0.25_f64)),
    ])
}

fn document_directory_key(path: &Path) -> String {
    path.parent()
        .and_then(normalize_relative_path)
        .map(|path| directory_path_string(&path))
        .unwrap_or_else(|| ".".to_owned())
}

fn parent_directory_key(path: &Path) -> Option<String> {
    if path.as_os_str().is_empty() {
        return None;
    }
    path.parent()
        .and_then(normalize_relative_path)
        .map(|path| directory_path_string(&path))
}

fn sync_link_edges(
    engine: &mut CupldEngine,
    documents: &[MarkdownDocument],
    node_ids: &BTreeMap<String, NodeId>,
) -> Result<usize, SourceError> {
    let resolution_index = build_resolution_index(documents);
    let mut created_edges = 0;

    for document in documents {
        let source_key = path_to_string(&document.path);
        let Some(source_id) = node_ids.get(&source_key).copied() else {
            continue;
        };
        delete_connector_edges_of_types(engine, source_id, &[LINK_EDGE_TYPE])?;

        let mut resolved_targets = BTreeMap::new();
        for link_ref in extract_document_link_refs(document.frontmatter.as_ref(), &document.body) {
            let Some(target) =
                resolve_link_path(&document.path, &link_ref.raw_target, &resolution_index)
            else {
                continue;
            };
            resolved_targets
                .entry(target)
                .or_insert_with(ResolvedMarkdownTarget::default)
                .record(&link_ref);
        }
        for (target, resolved) in resolved_targets {
            let Some(target_id) = node_ids.get(&target).copied() else {
                continue;
            };
            let mut properties = PropertyMap::from_pairs([
                ("src.connector", Value::from(CONNECTOR_NAME)),
                ("src.kind", Value::from("link")),
                (
                    "md.link_target",
                    Value::from(resolved.raw_targets.first().cloned().unwrap_or_default()),
                ),
                ("md.link_targets", list_value(&resolved.raw_targets)),
                ("md.link_sources", list_value(&resolved.sources)),
                ("md.link_rels", list_value(&resolved.relations)),
            ]);
            properties.insert("src.status", Value::from("current"));
            engine.create_edge(source_id, target_id, LINK_EDGE_TYPE, properties)?;
            created_edges += 1;
        }
    }

    Ok(created_edges)
}

fn tombstone_missing_documents(
    engine: &mut CupldEngine,
    existing_docs: &BTreeMap<String, NodeId>,
) -> Result<usize, SourceError> {
    for node_id in existing_docs.values().copied() {
        engine.set_node_property(node_id, "src.status", Value::from("missing"))?;
        delete_connector_edges_of_types(engine, node_id, &[LINK_EDGE_TYPE])?;
        delete_connector_edges_touching_of_types(engine, node_id, &STRUCTURAL_EDGE_TYPES)?;
    }
    Ok(existing_docs.len())
}

fn tombstone_missing_directories(
    engine: &mut CupldEngine,
    existing_dirs: &BTreeMap<String, NodeId>,
) -> Result<usize, SourceError> {
    for node_id in existing_dirs.values().copied() {
        engine.set_node_property(node_id, "src.status", Value::from("missing"))?;
        delete_connector_edges_touching_of_types(engine, node_id, &STRUCTURAL_EDGE_TYPES)?;
    }
    Ok(existing_dirs.len())
}

fn delete_connector_edges_of_types(
    engine: &mut CupldEngine,
    node_id: NodeId,
    edge_types: &[&str],
) -> Result<(), SourceError> {
    let edge_ids = engine.outgoing_edge_ids(node_id);
    delete_connector_edge_ids_of_types(engine, edge_ids, edge_types)
}

fn delete_connector_edges_touching_of_types(
    engine: &mut CupldEngine,
    node_id: NodeId,
    edge_types: &[&str],
) -> Result<(), SourceError> {
    let mut edge_ids = engine.outgoing_edge_ids(node_id);
    edge_ids.extend(engine.incoming_edge_ids(node_id));
    edge_ids.sort();
    edge_ids.dedup();
    delete_connector_edge_ids_of_types(engine, edge_ids, edge_types)
}

fn delete_connector_edge_ids_of_types(
    engine: &mut CupldEngine,
    edge_ids: Vec<EdgeId>,
    edge_types: &[&str],
) -> Result<(), SourceError> {
    for edge_id in edge_ids {
        let Some(edge) = engine.edge(edge_id) else {
            continue;
        };
        let is_connector_edge = edge_types
            .iter()
            .any(|edge_type| *edge_type == edge.edge_type())
            && matches!(
                edge.property("src.connector"),
                Some(Value::String(connector)) if connector == CONNECTOR_NAME
            );
        if !is_connector_edge {
            continue;
        }
        engine.delete_edge(edge_id)?;
    }
    Ok(())
}

fn scan_markdown_root(root: &Path) -> Result<Vec<MarkdownDocument>, SourceError> {
    if !root.exists() {
        return Ok(Vec::new());
    }
    let root = root.canonicalize()?;
    let mut paths = Vec::new();
    collect_markdown_files(&root, &root, &mut paths)?;
    paths.sort();

    let mut documents = Vec::new();
    for path in paths {
        documents.push(read_markdown_document(&root, &path)?);
    }
    Ok(documents)
}

fn collect_markdown_files(
    root: &Path,
    current: &Path,
    files: &mut Vec<PathBuf>,
) -> Result<(), SourceError> {
    let mut entries = fs::read_dir(current)?
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .map(|entry| entry.path())
        .collect::<Vec<_>>();
    entries.sort();

    for path in entries {
        if path.is_dir() {
            collect_markdown_files(root, &path, files)?;
            continue;
        }
        if !path.is_file() {
            continue;
        }
        let Some(extension) = path.extension().and_then(|ext| ext.to_str()) else {
            continue;
        };
        if extension.eq_ignore_ascii_case("md") {
            files.push(path.strip_prefix(root).unwrap_or(&path).to_path_buf());
        }
    }
    Ok(())
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct MarkdownRootSnapshot {
    entries: BTreeMap<String, (u64, u128)>,
}

fn snapshot_markdown_root(root: &Path) -> Result<MarkdownRootSnapshot, SourceError> {
    if !root.exists() {
        return Ok(MarkdownRootSnapshot::default());
    }
    let root = root.canonicalize()?;
    let mut files = Vec::new();
    collect_markdown_files(&root, &root, &mut files)?;
    files.sort();

    let mut entries = BTreeMap::new();
    for relative in files {
        let absolute = root.join(&relative);
        let metadata = fs::metadata(&absolute)?;
        let modified = metadata
            .modified()
            .ok()
            .and_then(|value| value.duration_since(UNIX_EPOCH).ok())
            .map(|value| value.as_nanos())
            .unwrap_or_default();
        entries.insert(path_to_string(&relative), (metadata.len(), modified));
    }
    Ok(MarkdownRootSnapshot { entries })
}

#[derive(Clone, Debug, Default)]
struct WatchBatcher {
    pending_since: Option<Instant>,
    last_change: Option<Instant>,
}

impl WatchBatcher {
    fn record_change(&mut self, now: Instant) {
        self.pending_since.get_or_insert(now);
        self.last_change = Some(now);
    }

    fn should_flush(&self, now: Instant, options: &MarkdownWatchOptions) -> bool {
        let Some(last_change) = self.last_change else {
            return false;
        };
        let Some(pending_since) = self.pending_since else {
            return false;
        };
        now.duration_since(last_change) >= options.debounce
            || now.duration_since(pending_since) >= options.max_batch_window
    }

    fn flush(&mut self) {
        self.pending_since = None;
        self.last_change = None;
    }

    fn is_idle(&self) -> bool {
        self.pending_since.is_none()
    }
}

fn read_markdown_document(root: &Path, relative: &Path) -> Result<MarkdownDocument, SourceError> {
    let absolute = root.join(relative);
    let raw = fs::read_to_string(&absolute)?;
    let (frontmatter, body, has_frontmatter) = parse_frontmatter(&raw);
    let headings = extract_headings(&body);
    let mut tags = extract_frontmatter_strings(frontmatter.as_ref(), &["tags", "tag"]);
    for tag in extract_inline_tags(&body) {
        push_unique(&mut tags, tag);
    }
    let aliases = extract_frontmatter_strings(frontmatter.as_ref(), &["aliases", "alias"]);
    let mut links = Vec::new();
    for link_ref in extract_document_link_refs(frontmatter.as_ref(), &body) {
        push_unique(&mut links, link_ref.raw_target);
    }
    let title = extract_frontmatter_title(frontmatter.as_ref())
        .or_else(|| headings.first().cloned())
        .unwrap_or_else(|| filename_title(relative));

    Ok(MarkdownDocument {
        path: normalize_relative_path(relative).unwrap_or_else(|| relative.to_path_buf()),
        raw: raw.clone(),
        body,
        frontmatter,
        title,
        tags,
        aliases,
        links,
        headings,
        source_hash: stable_hash_hex(raw.as_bytes()),
        has_frontmatter,
    })
}

fn markdown_directories(documents: &[MarkdownDocument]) -> BTreeSet<PathBuf> {
    let mut directories = BTreeSet::new();
    if documents.is_empty() {
        return directories;
    }

    directories.insert(PathBuf::new());
    for document in documents {
        let mut current = PathBuf::new();
        let Some(parent) = document.path.parent() else {
            continue;
        };
        for component in parent.components() {
            if let Component::Normal(value) = component {
                current.push(value);
                directories.insert(current.clone());
            }
        }
    }
    directories
}

fn parse_frontmatter(raw: &str) -> (Option<PropertyMap>, String, bool) {
    if !raw.starts_with("---\n") && !raw.starts_with("---\r\n") {
        return (None, raw.to_owned(), false);
    }

    let after_open = if raw.starts_with("---\r\n") { 5 } else { 4 };
    let remainder = &raw[after_open..];
    let mut consumed = 0usize;

    for segment in remainder.split_inclusive('\n') {
        let line = segment.trim_end_matches('\n').trim_end_matches('\r');
        if line == "---" {
            let frontmatter_text = &remainder[..consumed];
            let body = remainder[consumed + segment.len()..].to_owned();
            if let Some(frontmatter) = parse_frontmatter_map(frontmatter_text) {
                return (Some(frontmatter), body, true);
            }
            return (None, raw.to_owned(), false);
        }
        consumed += segment.len();
    }

    if let Some(stripped) = remainder.strip_suffix("---")
        && let Some(frontmatter) = parse_frontmatter_map(stripped)
    {
        return (Some(frontmatter), String::new(), true);
    }

    (None, raw.to_owned(), false)
}

fn parse_frontmatter_map(input: &str) -> Option<PropertyMap> {
    let lines = collect_frontmatter_lines(input)?;
    if lines.is_empty() {
        return Some(PropertyMap::new());
    }

    let mut index = 0usize;
    let properties = parse_frontmatter_mapping(&lines, &mut index, 0, true)?;
    if index == lines.len() {
        Some(properties)
    } else {
        None
    }
}

#[derive(Clone, Copy)]
struct FrontmatterLine<'a> {
    indent: usize,
    content: &'a str,
}

fn collect_frontmatter_lines(input: &str) -> Option<Vec<FrontmatterLine<'_>>> {
    let mut lines = Vec::new();
    for raw_line in input.lines() {
        let line = raw_line.trim_end_matches('\r');
        if line.trim().is_empty() {
            continue;
        }
        if line.contains('\t') {
            return None;
        }

        let indent = line.chars().take_while(|ch| *ch == ' ').count();
        let content = &line[indent..];
        if content.starts_with('#') {
            continue;
        }

        lines.push(FrontmatterLine { indent, content });
    }
    Some(lines)
}

fn parse_frontmatter_mapping(
    lines: &[FrontmatterLine<'_>],
    index: &mut usize,
    indent: usize,
    allow_empty: bool,
) -> Option<PropertyMap> {
    let mut properties = PropertyMap::new();
    let mut saw_entry = false;

    while let Some(line) = lines.get(*index) {
        if line.indent < indent {
            break;
        }
        if line.indent != indent || line.content.starts_with("- ") {
            return None;
        }

        let (key, inline_value) = split_mapping_entry(line.content)?;
        *index += 1;
        saw_entry = true;

        let value = if inline_value.is_empty() {
            parse_nested_block_or_null(lines, index, indent)?
        } else {
            parse_inline_value(inline_value)?
        };
        properties.insert(key, value);
    }

    if saw_entry || allow_empty {
        Some(properties)
    } else {
        None
    }
}

fn split_mapping_entry(line: &str) -> Option<(String, &str)> {
    let (key, value) = line.split_once(':')?;
    let key = key.trim();
    if key.is_empty() {
        return None;
    }
    Some((key.to_owned(), value.trim_start()))
}

fn parse_nested_block_or_null(
    lines: &[FrontmatterLine<'_>],
    index: &mut usize,
    parent_indent: usize,
) -> Option<Value> {
    match lines.get(*index) {
        Some(next) if next.indent > parent_indent => parse_block_value(lines, index, next.indent),
        _ => Some(Value::Null),
    }
}

fn parse_block_value(
    lines: &[FrontmatterLine<'_>],
    index: &mut usize,
    indent: usize,
) -> Option<Value> {
    let line = lines.get(*index)?;
    if line.indent != indent {
        return None;
    }
    if line.content.starts_with("- ") {
        parse_frontmatter_list(lines, index, indent).map(Value::List)
    } else {
        parse_frontmatter_mapping(lines, index, indent, false).map(Value::from)
    }
}

fn parse_frontmatter_list(
    lines: &[FrontmatterLine<'_>],
    index: &mut usize,
    indent: usize,
) -> Option<Vec<Value>> {
    let mut values = Vec::new();

    while let Some(line) = lines.get(*index) {
        if line.indent < indent {
            break;
        }
        if line.indent != indent || !line.content.starts_with("- ") {
            return None;
        }

        let remainder = line.content[2..].trim_start();
        *index += 1;
        let value = if remainder.is_empty() {
            parse_nested_block_or_null(lines, index, indent)?
        } else {
            parse_inline_value(remainder)?
        };
        values.push(value);
    }

    if values.is_empty() {
        None
    } else {
        Some(values)
    }
}

fn parse_inline_value(input: &str) -> Option<Value> {
    let input = strip_inline_comment(input).trim();
    if input.is_empty() {
        return Some(Value::Null);
    }
    if is_obsidian_wikilink_literal(input) {
        return Some(Value::String(input.to_owned()));
    }
    if input.starts_with('[') {
        return parse_inline_list(input).map(Value::List);
    }
    if input.starts_with('{') {
        return parse_inline_map(input).map(Value::from);
    }
    if input.starts_with('"') || input.starts_with('\'') {
        return parse_quoted_string(input).map(Value::String);
    }
    parse_scalar_value(input)
}

fn is_obsidian_wikilink_literal(input: &str) -> bool {
    input.starts_with("[[") && input.ends_with("]]") && input.len() >= 4
}

fn parse_inline_list(input: &str) -> Option<Vec<Value>> {
    let inner = input.strip_prefix('[')?.strip_suffix(']')?;
    let inner = inner.trim();
    if inner.is_empty() {
        return Some(Vec::new());
    }

    split_top_level(inner, ',')?
        .into_iter()
        .map(|part| parse_inline_value(part.trim()))
        .collect()
}

fn parse_inline_map(input: &str) -> Option<PropertyMap> {
    let inner = input.strip_prefix('{')?.strip_suffix('}')?;
    let inner = inner.trim();
    if inner.is_empty() {
        return Some(PropertyMap::new());
    }

    let mut properties = PropertyMap::new();
    for part in split_top_level(inner, ',')? {
        let (key, value) = split_mapping_entry(part.trim())?;
        properties.insert(key, parse_inline_value(value)?);
    }
    Some(properties)
}

fn split_top_level(input: &str, separator: char) -> Option<Vec<&str>> {
    let mut parts = Vec::new();
    let mut start = 0usize;
    let mut bracket_depth = 0usize;
    let mut brace_depth = 0usize;
    let mut quote = None;
    let mut escaped = false;

    for (index, ch) in input.char_indices() {
        if let Some(active) = quote {
            if escaped {
                escaped = false;
                continue;
            }
            if active == '"' && ch == '\\' {
                escaped = true;
                continue;
            }
            if ch == active {
                quote = None;
            }
            continue;
        }

        match ch {
            '\'' | '"' => quote = Some(ch),
            '[' => bracket_depth += 1,
            ']' => bracket_depth = bracket_depth.checked_sub(1)?,
            '{' => brace_depth += 1,
            '}' => brace_depth = brace_depth.checked_sub(1)?,
            _ if ch == separator && bracket_depth == 0 && brace_depth == 0 => {
                parts.push(&input[start..index]);
                start = index + ch.len_utf8();
            }
            _ => {}
        }
    }

    if quote.is_some() || bracket_depth != 0 || brace_depth != 0 {
        return None;
    }

    parts.push(&input[start..]);
    Some(parts)
}

fn parse_quoted_string(input: &str) -> Option<String> {
    let quote = input.chars().next()?;
    let inner = input.strip_prefix(quote)?.strip_suffix(quote)?;
    if inner.contains(quote) {
        return None;
    }
    if quote == '"' {
        let mut output = String::new();
        let mut chars = inner.chars();
        while let Some(ch) = chars.next() {
            if ch != '\\' {
                output.push(ch);
                continue;
            }
            let escaped = chars.next()?;
            output.push(match escaped {
                'n' => '\n',
                'r' => '\r',
                't' => '\t',
                '\\' => '\\',
                '"' => '"',
                '\'' => '\'',
                other => other,
            });
        }
        Some(output)
    } else {
        Some(inner.to_owned())
    }
}

fn strip_inline_comment(input: &str) -> &str {
    let mut quote = None;
    let mut bracket_depth = 0usize;
    let mut brace_depth = 0usize;
    let mut escaped = false;

    for (index, ch) in input.char_indices() {
        if let Some(active) = quote {
            if escaped {
                escaped = false;
                continue;
            }
            if active == '"' && ch == '\\' {
                escaped = true;
                continue;
            }
            if ch == active {
                quote = None;
            }
            continue;
        }

        match ch {
            '\'' | '"' => quote = Some(ch),
            '[' => bracket_depth += 1,
            ']' => bracket_depth = bracket_depth.saturating_sub(1),
            '{' => brace_depth += 1,
            '}' => brace_depth = brace_depth.saturating_sub(1),
            '#' if bracket_depth == 0 && brace_depth == 0 => {
                let comment_is_delimited = index == 0
                    || input[..index]
                        .chars()
                        .last()
                        .map(|prev| prev.is_whitespace())
                        .unwrap_or(true);
                if comment_is_delimited {
                    return input[..index].trim_end();
                }
            }
            _ => {}
        }
    }

    input
}

fn parse_scalar_value(input: &str) -> Option<Value> {
    if input.eq_ignore_ascii_case("true") {
        return Some(Value::Bool(true));
    }
    if input.eq_ignore_ascii_case("false") {
        return Some(Value::Bool(false));
    }
    if input.eq_ignore_ascii_case("null") || input == "~" {
        return Some(Value::Null);
    }
    if let Ok(value) = input.parse::<i64>() {
        return Some(Value::Int(value));
    }
    if looks_like_float(input) {
        if let Ok(value) = input.parse::<f64>() {
            return Some(Value::Float(value));
        }
        return None;
    }
    Some(Value::String(input.to_owned()))
}

fn looks_like_float(input: &str) -> bool {
    let mut has_digit = false;
    let mut has_float_marker = false;

    for ch in input.chars() {
        match ch {
            '0'..='9' => has_digit = true,
            '+' | '-' | '.' | 'e' | 'E' => {
                if matches!(ch, '.' | 'e' | 'E') {
                    has_float_marker = true;
                }
            }
            _ => return false,
        }
    }

    has_digit && has_float_marker
}

fn extract_frontmatter_title(frontmatter: Option<&PropertyMap>) -> Option<String> {
    extract_frontmatter_string(frontmatter, &["title"])
}

fn extract_frontmatter_string(frontmatter: Option<&PropertyMap>, keys: &[&str]) -> Option<String> {
    let frontmatter = frontmatter?;
    for key in keys {
        let Some(value) = frontmatter.get(key) else {
            continue;
        };
        if let Value::String(value) = value {
            let trimmed = value.trim();
            if !trimmed.is_empty() {
                return Some(trimmed.to_owned());
            }
        }
    }
    None
}

fn extract_frontmatter_strings(frontmatter: Option<&PropertyMap>, keys: &[&str]) -> Vec<String> {
    let mut values = Vec::new();
    let Some(frontmatter) = frontmatter else {
        return values;
    };
    for key in keys {
        let Some(value) = frontmatter.get(key) else {
            continue;
        };
        match value {
            Value::String(value) => push_unique(&mut values, value.trim().to_owned()),
            Value::List(entries) => {
                for entry in entries {
                    if let Value::String(value) = entry {
                        push_unique(&mut values, value.trim().to_owned());
                    }
                }
            }
            _ => {}
        }
    }
    values.retain(|value| !value.is_empty());
    values
}

fn extract_headings(body: &str) -> Vec<String> {
    let mut headings = Vec::new();
    for line in body.lines() {
        let trimmed = line.trim_start();
        let level = trimmed.chars().take_while(|ch| *ch == '#').count();
        if level == 0 {
            continue;
        }
        let remainder = trimmed[level..].trim();
        if remainder.is_empty() {
            continue;
        }
        let heading = remainder.trim_end_matches('#').trim();
        if !heading.is_empty() {
            push_unique(&mut headings, heading.to_owned());
        }
    }
    headings
}

fn extract_inline_tags(body: &str) -> Vec<String> {
    let mut tags = Vec::new();
    for line in body.lines() {
        let trimmed = line.trim_start();
        if trimmed.starts_with('#') && trimmed.chars().nth(1) == Some(' ') {
            continue;
        }
        let mut chars = line.char_indices().peekable();
        while let Some((index, current)) = chars.next() {
            if current != '#' {
                continue;
            }
            let previous_is_boundary = index == 0
                || line[..index]
                    .chars()
                    .last()
                    .map(|ch| ch.is_whitespace() || matches!(ch, '(' | '[' | '{'))
                    .unwrap_or(true);
            if !previous_is_boundary {
                continue;
            }
            let mut end = index + current.len_utf8();
            let mut found = false;
            while let Some(&(next_index, ch)) = chars.peek() {
                if ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '/') {
                    found = true;
                    end = next_index + ch.len_utf8();
                    chars.next();
                } else {
                    break;
                }
            }
            if found {
                push_unique(&mut tags, line[index + 1..end].to_owned());
            }
        }
    }
    tags
}

fn extract_links(body: &str) -> Vec<String> {
    let mut links = Vec::new();
    extract_wikilinks(body, &mut links);
    extract_markdown_links(body, &mut links);
    links
}

fn extract_document_link_refs(
    frontmatter: Option<&PropertyMap>,
    body: &str,
) -> Vec<MarkdownLinkRef> {
    let mut links = extract_frontmatter_link_refs(frontmatter);
    for link in extract_links(body) {
        push_unique_link_ref(&mut links, link, MarkdownLinkSource::Body, None);
    }
    links
}

fn extract_frontmatter_link_refs(frontmatter: Option<&PropertyMap>) -> Vec<MarkdownLinkRef> {
    let mut links = Vec::new();
    let Some(frontmatter) = frontmatter else {
        return links;
    };
    for (key, value) in frontmatter.iter() {
        let Some(relation) = canonical_frontmatter_relation(key) else {
            continue;
        };
        for target in extract_frontmatter_link_targets(value) {
            push_unique_link_ref(
                &mut links,
                target,
                MarkdownLinkSource::Frontmatter,
                Some(relation),
            );
        }
    }
    links
}

fn canonical_frontmatter_relation(key: &str) -> Option<&'static str> {
    match key {
        "up" | "parent" => Some("up"),
        "related" => Some("related"),
        "next" => Some("next"),
        "previous" => Some("previous"),
        "link" | "links" => Some("link"),
        _ => None,
    }
}

fn normalize_frontmatter_link_target(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }
    let normalized = if trimmed.starts_with("[[") && trimmed.ends_with("]]") && trimmed.len() >= 4 {
        trimmed[2..trimmed.len() - 2].trim()
    } else {
        trimmed
    };
    if normalized.is_empty() {
        None
    } else {
        Some(normalized.to_owned())
    }
}

fn extract_frontmatter_link_targets(value: &Value) -> Vec<String> {
    let mut targets = Vec::new();
    match value {
        Value::String(value) => {
            if let Some(target) = normalize_frontmatter_link_target(value) {
                push_unique(&mut targets, target);
            }
        }
        Value::List(entries) => {
            for entry in entries {
                match entry {
                    Value::String(value) => {
                        if let Some(target) = normalize_frontmatter_link_target(value) {
                            push_unique(&mut targets, target);
                        }
                    }
                    Value::List(nested) => {
                        if let Some(target) = normalize_frontmatter_wikilink_list(nested) {
                            push_unique(&mut targets, target);
                        }
                    }
                    _ => {}
                }
            }
        }
        _ => {}
    }
    targets
}

fn normalize_frontmatter_wikilink_list(entries: &[Value]) -> Option<String> {
    if entries.len() != 1 {
        return None;
    }
    let Value::String(value) = &entries[0] else {
        return None;
    };
    normalize_frontmatter_link_target(value)
}

fn extract_wikilinks(body: &str, links: &mut Vec<String>) {
    let mut cursor = 0usize;
    while let Some(start) = body[cursor..].find("[[") {
        let start = cursor + start + 2;
        let Some(end_rel) = body[start..].find("]]") else {
            break;
        };
        let target = body[start..start + end_rel].trim();
        if !target.is_empty() {
            push_unique(links, target.to_owned());
        }
        cursor = start + end_rel + 2;
    }
}

fn extract_markdown_links(body: &str, links: &mut Vec<String>) {
    let bytes = body.as_bytes();
    let mut index = 0usize;
    while index < bytes.len() {
        if bytes[index] != b'[' {
            index += 1;
            continue;
        }
        let Some(close_bracket) = body[index..].find("](") else {
            break;
        };
        let open_target = index + close_bracket + 2;
        let Some(close_target) = body[open_target..].find(')') else {
            break;
        };
        let target = body[open_target..open_target + close_target].trim();
        if !target.is_empty() && !target.starts_with('#') {
            push_unique(links, target.to_owned());
        }
        index = open_target + close_target + 1;
    }
}

fn filename_title(path: &Path) -> String {
    path.file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or("untitled")
        .to_owned()
}

fn directory_path_string(path: &Path) -> String {
    if path.as_os_str().is_empty() {
        ".".to_owned()
    } else {
        path_to_string(path)
    }
}

fn directory_name(path: &Path) -> String {
    if path.as_os_str().is_empty() {
        return "root".to_owned();
    }
    path.file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("untitled")
        .to_owned()
}

fn directory_title(name: &str) -> String {
    let mut title = String::new();
    let mut capitalize_next = true;
    for character in name.chars() {
        match character {
            '-' | '_' => {
                if !title.is_empty() && !title.ends_with(' ') {
                    title.push(' ');
                }
                capitalize_next = true;
            }
            value if capitalize_next => {
                title.push(value.to_ascii_uppercase());
                capitalize_next = false;
            }
            value => title.push(value),
        }
    }
    if title.is_empty() {
        "Untitled".to_owned()
    } else {
        title
    }
}

fn list_value(values: &[String]) -> Value {
    Value::List(values.iter().cloned().map(Value::String).collect())
}

fn push_unique(values: &mut Vec<String>, value: String) {
    if value.is_empty() || values.iter().any(|existing| existing == &value) {
        return;
    }
    values.push(value);
}

fn push_unique_link_ref(
    links: &mut Vec<MarkdownLinkRef>,
    raw_target: String,
    source: MarkdownLinkSource,
    relation: Option<&str>,
) {
    if raw_target.is_empty() {
        return;
    }
    let link_ref = MarkdownLinkRef {
        raw_target,
        source,
        relation: relation.map(ToOwned::to_owned),
    };
    if links.iter().any(|existing| existing == &link_ref) {
        return;
    }
    links.push(link_ref);
}

fn build_resolution_index(documents: &[MarkdownDocument]) -> MarkdownResolutionIndex {
    let mut index = MarkdownResolutionIndex::default();
    for document in documents {
        let relative = path_to_string(&document.path);
        index.insert_direct(relative.clone(), relative.clone());
        index.insert_case_folded_path(&relative, &relative);
        index.insert_path_style_path(&relative, &relative);

        let without_extension = strip_markdown_extension(&relative);
        index.insert_direct(without_extension.clone(), relative.clone());
        index.insert_case_folded_path(&without_extension, &relative);
        index.insert_path_style_path(&without_extension, &relative);

        if let Some(index_key) = index_document_parent_key(&document.path) {
            index.insert_derived_path(&index_key, &relative);
            index.insert_case_folded_path(&index_key, &relative);
            index.insert_path_style_path(&index_key, &relative);
        }

        if let Some(stem) = document.path.file_stem().and_then(|value| value.to_str()) {
            index.insert_direct(stem.to_owned(), relative.clone());
            index.insert_case_folded_path(stem, &relative);
        }
        for alias in &document.aliases {
            index.insert_alias(alias, &relative);
        }
        if let Some(slug) = extract_frontmatter_string(document.frontmatter.as_ref(), &["slug"]) {
            insert_slug_candidates(&mut index, &slug, &relative);
        }
    }
    index
}

fn resolve_link_path(
    current_path: &Path,
    raw_link: &str,
    index: &MarkdownResolutionIndex,
) -> Option<String> {
    let target = normalize_resolution_target(raw_link)?;
    if target.is_empty() {
        return None;
    }
    let target = target.as_str();
    let stripped_target = strip_markdown_extension(target);

    let path_target = Path::new(target);
    let mut candidate_paths = Vec::new();
    if let Some(normalized) = resolve_relative_link(current_path, path_target) {
        let string_path = path_to_string(&normalized);
        push_unique(&mut candidate_paths, string_path.clone());
        if let Some(found) = index.resolve_direct(&string_path) {
            return Some(found);
        }
        let without_extension = strip_markdown_extension(&string_path);
        push_unique(&mut candidate_paths, without_extension.clone());
        if let Some(found) = index.resolve_direct(&without_extension) {
            return Some(found);
        }
        if let Some(found) = index
            .resolve_derived_path(&string_path)
            .or_else(|| index.resolve_derived_path(&without_extension))
        {
            return Some(found);
        }
    }

    push_unique(&mut candidate_paths, target.to_owned());
    push_unique(&mut candidate_paths, stripped_target.clone());

    if let Some(found) = index
        .resolve_direct(target)
        .or_else(|| index.resolve_direct(&stripped_target))
        .or_else(|| index.resolve_derived_path(target))
        .or_else(|| index.resolve_derived_path(&stripped_target))
    {
        return Some(found);
    }

    for candidate in slug_resolution_candidates(target) {
        if let Some(found) = index.resolve_slug(&candidate) {
            return Some(found);
        }
    }

    if let Some(found) = index
        .resolve_alias(target)
        .or_else(|| index.resolve_alias(&stripped_target))
    {
        return Some(found);
    }

    for candidate in &candidate_paths {
        if let Some(found) = index.resolve_case_folded_path(candidate) {
            return Some(found);
        }
    }

    for candidate in &candidate_paths {
        if let Some(found) = index.resolve_path_style_path(candidate) {
            return Some(found);
        }
    }

    None
}

fn normalize_resolution_target(raw_link: &str) -> Option<String> {
    let target = raw_link.split('|').next()?.trim();
    if target.is_empty() {
        return None;
    }
    if target.contains("://") {
        return extract_url_path(target);
    }
    let target = strip_query_and_fragment(target).trim();
    if target.is_empty() {
        None
    } else {
        Some(target.to_owned())
    }
}

fn extract_url_path(target: &str) -> Option<String> {
    let scheme_end = target.find("://")?;
    let after_scheme = &target[scheme_end + 3..];
    let path_start = after_scheme.find('/')?;
    let path = strip_query_and_fragment(&after_scheme[path_start..]).trim();
    if path.is_empty() || path == "/" {
        None
    } else {
        Some(path.to_owned())
    }
}

fn strip_query_and_fragment(target: &str) -> &str {
    let query = target.find('?');
    let fragment = target.find('#');
    let end = match (query, fragment) {
        (Some(query), Some(fragment)) => query.min(fragment),
        (Some(index), None) | (None, Some(index)) => index,
        (None, None) => target.len(),
    };
    &target[..end]
}

fn index_document_parent_key(path: &Path) -> Option<String> {
    let file_name = path.file_name()?.to_str()?;
    if !file_name.eq_ignore_ascii_case("index.md") {
        return None;
    }
    let parent = path.parent()?;
    if parent.as_os_str().is_empty() {
        return None;
    }
    Some(path_to_string(parent))
}

fn insert_slug_candidates(index: &mut MarkdownResolutionIndex, slug: &str, target: &str) {
    for candidate in slug_resolution_candidates(slug) {
        index.insert_slug(&candidate, target);
    }
}

fn slug_resolution_candidates(target: &str) -> Vec<String> {
    let mut candidates = Vec::new();
    let trimmed = target.trim();
    if trimmed.is_empty() {
        return candidates;
    }
    push_unique(&mut candidates, trimmed.to_owned());

    let without_leading_slash = trimmed.trim_start_matches('/');
    push_unique(&mut candidates, without_leading_slash.to_owned());
    push_unique(&mut candidates, format!("/{without_leading_slash}"));
    push_unique(&mut candidates, format!("docs/{without_leading_slash}"));
    push_unique(&mut candidates, format!("/docs/{without_leading_slash}"));
    push_unique(
        &mut candidates,
        format!("en-US/docs/{without_leading_slash}"),
    );
    push_unique(
        &mut candidates,
        format!("/en-US/docs/{without_leading_slash}"),
    );

    candidates
}

fn path_style_key(path: &str) -> Option<String> {
    let mut normalized = path.trim().replace('\\', "/");
    if normalized.is_empty() {
        return None;
    }
    normalized = strip_markdown_extension(&normalized);
    let trimmed = normalized.trim_matches('/');
    if trimmed.is_empty() {
        return None;
    }
    let without_index = trimmed.strip_suffix("/index").unwrap_or(trimmed);
    let without_site_prefix = strip_site_path_prefix(without_index);
    let key = without_site_prefix
        .trim_matches('/')
        .to_lowercase()
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>()
        .join("/");
    if key.is_empty() { None } else { Some(key) }
}

fn strip_site_path_prefix(path: &str) -> &str {
    if let Some(rest) = path.strip_prefix("docs/") {
        return rest;
    }
    if path
        .get(..5)
        .map(|prefix| prefix.eq_ignore_ascii_case("docs/"))
        .unwrap_or(false)
    {
        return &path[5..];
    }

    let mut segments = path.splitn(3, '/');
    let Some(first) = segments.next() else {
        return path;
    };
    let Some(second) = segments.next() else {
        return path;
    };
    let Some(rest) = segments.next() else {
        return path;
    };
    if looks_locale_segment(first) && second.eq_ignore_ascii_case("docs") {
        rest
    } else {
        path
    }
}

fn looks_locale_segment(segment: &str) -> bool {
    let len = segment.len();
    (len == 2 || (segment.contains('-') && len <= 12))
        && segment
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
}

fn resolve_relative_link(current_path: &Path, target: &Path) -> Option<PathBuf> {
    let mut base = current_path
        .parent()
        .unwrap_or_else(|| Path::new(""))
        .to_path_buf();
    if target.is_absolute() {
        return normalize_relative_path(target.strip_prefix("/").ok()?);
    }
    base.push(target);
    if target.extension().is_none() {
        base.set_extension("md");
    }
    normalize_relative_path(&base)
}

fn strip_markdown_extension(path: &str) -> String {
    let candidate = Path::new(path);
    if candidate
        .extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| ext.eq_ignore_ascii_case("md"))
        .unwrap_or(false)
    {
        candidate
            .with_extension("")
            .to_string_lossy()
            .replace('\\', "/")
    } else {
        path.to_owned()
    }
}

fn find_config_node(engine: &CupldEngine) -> Option<NodeId> {
    engine.nodes().find_map(|node| {
        let kind = match node.property("sys.kind") {
            Some(Value::String(value)) => value,
            _ => return None,
        };
        let name = match node.property("sys.name") {
            Some(Value::String(value)) => value,
            _ => return None,
        };
        if kind == CONFIG_KIND && name == CONFIG_NAME {
            Some(node.id())
        } else {
            None
        }
    })
}

fn normalize_root_path(path: &Path) -> Result<PathBuf, SourceError> {
    let path = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    };
    if path.exists() {
        return path.canonicalize().map_err(SourceError::from);
    }
    Ok(path)
}

fn normalize_relative_path(path: &Path) -> Option<PathBuf> {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::Normal(value) => normalized.push(value),
            Component::ParentDir => {
                if !normalized.pop() {
                    return None;
                }
            }
            Component::Prefix(_) | Component::RootDir => return None,
        }
    }
    Some(normalized)
}

fn path_to_string(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

fn source_key(root: &str, path: &str) -> String {
    format!("{root}\0{path}")
}

fn stable_hash_hex(bytes: &[u8]) -> String {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("{hash:016x}")
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::{
        CONFIG_KIND, CONFIG_NAME, CONNECTOR_NAME, LINK_EDGE_TYPE, MARKDOWN_DIRECTORY_LABEL,
        MARKDOWN_DOCUMENT_LABEL, MD_IN_DIRECTORY, MD_PARENT_DIRECTORY, MarkdownDocument,
        MarkdownLinkRef, MarkdownLinkSource, MarkdownSyncOptions, build_resolution_index,
        configured_markdown_root, extract_document_link_refs, read_markdown_document,
        resolve_link_path, set_markdown_root, sync_markdown_root, sync_markdown_root_with_options,
    };
    use crate::engine::{CupldEngine, PropertyMap, Value};

    static NEXT_TEMP_ID: AtomicUsize = AtomicUsize::new(1);

    #[test]
    fn parses_frontmatter_and_body_fields() {
        let root = temp_dir("frontmatter");
        fs::create_dir_all(&root).unwrap();
        let path = root.join("note.md");
        fs::write(
            &path,
            r#"---
title: Frontmatter Title
tags:
  - rust
aliases: [One, Two]
related: [[other]]
parent: notes/parent.md#overview
links:
  - misc/topic
  - "[[Series Two]]"
unsupported:
  - ignored
nested:
  enabled: true
---
# Heading
Body with [[other]] and [deep](docs/page.md#intro) and #tagged
"#,
        )
        .unwrap();

        let document = read_markdown_document(&root, Path::new("note.md")).unwrap();
        assert_eq!(document.title, "Frontmatter Title");
        assert_eq!(document.tags, vec!["rust".to_owned(), "tagged".to_owned()]);
        assert_eq!(document.aliases, vec!["One".to_owned(), "Two".to_owned()]);
        assert_eq!(document.headings, vec!["Heading".to_owned()]);
        assert_eq!(
            document.links,
            vec![
                "other".to_owned(),
                "notes/parent.md#overview".to_owned(),
                "misc/topic".to_owned(),
                "Series Two".to_owned(),
                "docs/page.md#intro".to_owned(),
            ]
        );
        assert!(document.has_frontmatter);
        let frontmatter = document.frontmatter.unwrap();
        assert_eq!(
            frontmatter.get("nested"),
            Some(&Value::from(crate::engine::PropertyMap::from_pairs([(
                "enabled",
                Value::Bool(true),
            )])))
        );

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn falls_back_when_frontmatter_is_missing_or_malformed() {
        let root = temp_dir("no_frontmatter");
        fs::create_dir_all(&root).unwrap();
        fs::write(root.join("plain.md"), "# Plain\nBody").unwrap();
        fs::write(root.join("bad.md"), "---\nfoo: [unterminated\n# Heading").unwrap();

        let plain = read_markdown_document(&root, Path::new("plain.md")).unwrap();
        assert_eq!(plain.title, "Plain");
        assert_eq!(plain.body, "# Plain\nBody");
        assert!(!plain.has_frontmatter);
        assert!(plain.frontmatter.is_none());

        let bad = read_markdown_document(&root, Path::new("bad.md")).unwrap();
        assert_eq!(bad.body, "---\nfoo: [unterminated\n# Heading");
        assert!(!bad.has_frontmatter);
        assert!(bad.frontmatter.is_none());

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn syncs_documents_and_tombstones_missing_files() {
        let root = temp_dir("sync");
        fs::create_dir_all(&root).unwrap();
        fs::write(root.join("note.md"), "# Note\n[[other]]").unwrap();
        fs::write(root.join("other.md"), "# Other").unwrap();

        let mut engine = CupldEngine::default();
        let report = sync_markdown_root(&mut engine, &root).unwrap();
        assert_eq!(report.scanned_documents, 2);
        assert_eq!(report.upserted_documents, 2);
        assert_eq!(report.link_edges, 1);
        assert_eq!(report.upserted_directories, 0);
        assert_eq!(report.tombstoned_directories, 0);
        assert_eq!(report.structural_edges, 0);
        assert_eq!(
            engine
                .nodes()
                .filter(|node| node.labels().contains(MARKDOWN_DOCUMENT_LABEL))
                .count(),
            2
        );

        fs::remove_file(root.join("other.md")).unwrap();
        let report = sync_markdown_root(&mut engine, &root).unwrap();
        assert_eq!(report.tombstoned_documents, 1);
        let missing = engine.nodes().any(|node| {
            node.property("src.path") == Some(&Value::from("other.md"))
                && node.property("src.status") == Some(&Value::from("missing"))
        });
        assert!(missing);

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn filesystem_graph_options_are_default_off() {
        assert!(!MarkdownSyncOptions::default().filesystem_graph);
        assert_eq!(MARKDOWN_DIRECTORY_LABEL, "MarkdownDirectory");
        assert_eq!(MD_IN_DIRECTORY, "MD_IN_DIRECTORY");
        assert_eq!(MD_PARENT_DIRECTORY, "MD_PARENT_DIRECTORY");

        let root = temp_dir("sync_options");
        fs::create_dir_all(root.join("nested")).unwrap();
        fs::write(root.join("nested").join("note.md"), "# Note").unwrap();

        let mut engine = CupldEngine::default();
        let report =
            sync_markdown_root_with_options(&mut engine, &root, &MarkdownSyncOptions::default())
                .unwrap();
        assert_eq!(report.upserted_directories, 0);
        assert_eq!(report.tombstoned_directories, 0);
        assert_eq!(report.structural_edges, 0);
        assert!(
            engine
                .nodes()
                .all(|node| !node.labels().contains(MARKDOWN_DIRECTORY_LABEL))
        );
        assert!(engine.edges().all(|edge| {
            edge.edge_type() != MD_IN_DIRECTORY && edge.edge_type() != MD_PARENT_DIRECTORY
        }));

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn syncs_frontmatter_relationship_metadata_into_edges() {
        let root = temp_dir("frontmatter_edges");
        fs::create_dir_all(&root).unwrap();
        fs::write(
            root.join("note.md"),
            r#"---
related: [[other]]
parent: [[map]]
links:
  - misc
---
Body with [[other]] and [deep](other.md#intro) and [misc](misc.md)
"#,
        )
        .unwrap();
        fs::write(root.join("other.md"), "# Other").unwrap();
        fs::write(root.join("map.md"), "# Map").unwrap();
        fs::write(root.join("misc.md"), "# Misc").unwrap();

        let document = read_markdown_document(&root, Path::new("note.md")).unwrap();
        assert!(document.has_frontmatter);
        let link_refs = extract_document_link_refs(document.frontmatter.as_ref(), &document.body);
        assert!(link_refs.contains(&MarkdownLinkRef {
            raw_target: "map".to_owned(),
            source: MarkdownLinkSource::Frontmatter,
            relation: Some("up".to_owned()),
        }));

        let mut engine = CupldEngine::default();
        let report = sync_markdown_root(&mut engine, &root).unwrap();
        assert_eq!(report.link_edges, 3);

        let map_edge = engine
            .edges()
            .find(|edge| {
                edge.edge_type() == LINK_EDGE_TYPE
                    && edge.property("md.link_target") == Some(&Value::from("map"))
            })
            .unwrap();
        assert_eq!(
            map_edge.property("md.link_sources"),
            Some(&Value::List(vec![Value::from("frontmatter")]))
        );
        assert_eq!(
            map_edge.property("md.link_rels"),
            Some(&Value::List(vec![Value::from("up")]))
        );

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn resolver_prefers_explicit_markdown_file_over_index_fallback() {
        let documents = vec![
            test_document("foo/bar.md", &[], None),
            test_document("foo/bar/index.md", &[], None),
            test_document("source.md", &[], None),
        ];
        let index = build_resolution_index(&documents);

        assert_eq!(
            resolve_link_path(Path::new("source.md"), "foo/bar", &index),
            Some("foo/bar.md".to_owned())
        );
    }

    #[test]
    fn resolver_prefers_alias_over_case_and_path_style_fallbacks() {
        let documents = vec![
            test_document("alias-target.md", &["Web/JavaScript/Guide"], None),
            test_document("web/javascript/guide/index.md", &[], None),
            test_document("source.md", &[], None),
        ];
        let index = build_resolution_index(&documents);

        assert_eq!(
            resolve_link_path(Path::new("source.md"), "Web/JavaScript/Guide", &index),
            Some("alias-target.md".to_owned())
        );
    }

    #[test]
    fn resolver_skips_ambiguous_case_slug_and_path_style_matches() {
        let documents = vec![
            test_document("Docs/Topic.md", &[], Some("Shared")),
            test_document("docs/topic.md", &[], Some("Shared")),
            test_document("source.md", &[], None),
        ];
        let index = build_resolution_index(&documents);

        assert_eq!(
            resolve_link_path(Path::new("source.md"), "DOCS/TOPIC", &index),
            None
        );
        assert_eq!(
            resolve_link_path(Path::new("source.md"), "Shared", &index),
            None
        );
        assert_eq!(
            resolve_link_path(Path::new("source.md"), "/en-US/docs/docs/topic", &index),
            None
        );
    }

    #[test]
    fn resolver_resolves_url_paths_through_slug_candidates() {
        let documents = vec![
            test_document("games/tutorials/index.md", &[], Some("Games/Tutorials")),
            test_document("source.md", &[], None),
        ];
        let index = build_resolution_index(&documents);

        assert_eq!(
            resolve_link_path(
                Path::new("source.md"),
                "https://developer.mozilla.org/en-US/docs/Games/Tutorials?x=1#intro",
                &index,
            ),
            Some("games/tutorials/index.md".to_owned())
        );
    }

    #[test]
    fn persists_configured_root_in_engine() {
        let root = temp_dir("config");
        fs::create_dir_all(&root).unwrap();
        let expected_root = root.canonicalize().unwrap();
        let mut engine = CupldEngine::default();
        set_markdown_root(&mut engine, &root).unwrap();

        assert_eq!(configured_markdown_root(&engine), Some(expected_root));
        let config_node = engine
            .nodes()
            .find(|node| {
                node.property("sys.kind") == Some(&Value::from(CONFIG_KIND))
                    && node.property("sys.name") == Some(&Value::from(CONFIG_NAME))
            })
            .unwrap();
        assert_eq!(
            config_node.property("src.connector"),
            Some(&Value::from(CONNECTOR_NAME))
        );

        fs::remove_dir_all(root).unwrap();
    }

    fn test_document(path: &str, aliases: &[&str], slug: Option<&str>) -> MarkdownDocument {
        let frontmatter = slug.map(|slug| PropertyMap::from_pairs([("slug", Value::from(slug))]));
        MarkdownDocument {
            path: PathBuf::from(path),
            raw: String::new(),
            body: String::new(),
            frontmatter,
            title: path.to_owned(),
            tags: Vec::new(),
            aliases: aliases.iter().map(|alias| (*alias).to_owned()).collect(),
            links: Vec::new(),
            headings: Vec::new(),
            source_hash: String::new(),
            has_frontmatter: slug.is_some(),
        }
    }

    fn temp_dir(prefix: &str) -> PathBuf {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let suffix = NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "cupld_source_{prefix}_{}_{}_{}",
            std::process::id(),
            timestamp,
            suffix
        ))
    }
}
