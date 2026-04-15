use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io;
use std::path::{Component, Path, PathBuf};

use crate::engine::{CupldEngine, GraphError, NodeId, PropertyMap, Value};

const MARKDOWN_DOCUMENT_LABEL: &str = "MarkdownDocument";
const CONFIG_LABEL: &str = "SystemConfig";
const CONFIG_KIND: &str = "config";
const CONFIG_NAME: &str = "markdown_source";
const CONNECTOR_NAME: &str = "markdown";
const LINK_EDGE_TYPE: &str = "MD_LINKS_TO";

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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MarkdownSyncReport {
    pub root: PathBuf,
    pub scanned_documents: usize,
    pub upserted_documents: usize,
    pub tombstoned_documents: usize,
    pub link_edges: usize,
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
    let root = normalize_root_path(root)?;
    let documents = scan_markdown_root(&root)?;
    let root_string = path_to_string(&root);

    let mut existing_docs = collect_existing_documents(engine, &root_string);
    let doc_node_ids = upsert_documents(engine, &root_string, &documents, &mut existing_docs)?;
    let link_edges = sync_link_edges(engine, &documents, &doc_node_ids)?;
    let tombstoned_documents = tombstone_missing_documents(engine, &existing_docs)?;

    Ok(MarkdownSyncReport {
        root,
        scanned_documents: documents.len(),
        upserted_documents: doc_node_ids.len(),
        tombstoned_documents,
        link_edges,
    })
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
        delete_connector_link_edges(engine, source_id)?;

        let mut resolved_targets = BTreeSet::new();
        for link in &document.links {
            let Some(target) = resolve_link_path(&document.path, link, &resolution_index) else {
                continue;
            };
            if !resolved_targets.insert(target.clone()) {
                continue;
            }
            let Some(target_id) = node_ids.get(&target).copied() else {
                continue;
            };
            let mut properties = PropertyMap::from_pairs([
                ("src.connector", Value::from(CONNECTOR_NAME)),
                ("src.kind", Value::from("link")),
                ("md.link_target", Value::from(link.clone())),
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
        delete_connector_link_edges(engine, node_id)?;
    }
    Ok(existing_docs.len())
}

fn delete_connector_link_edges(
    engine: &mut CupldEngine,
    node_id: NodeId,
) -> Result<(), SourceError> {
    let edge_ids = engine.outgoing_edge_ids(node_id);
    for edge_id in edge_ids {
        let Some(edge) = engine.edge(edge_id) else {
            continue;
        };
        let is_connector_edge = edge.edge_type() == LINK_EDGE_TYPE
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
    let links = extract_links(&body);
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
        if !target.is_empty() && !target.contains("://") && !target.starts_with('#') {
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

fn list_value(values: &[String]) -> Value {
    Value::List(values.iter().cloned().map(Value::String).collect())
}

fn push_unique(values: &mut Vec<String>, value: String) {
    if value.is_empty() || values.iter().any(|existing| existing == &value) {
        return;
    }
    values.push(value);
}

fn build_resolution_index(documents: &[MarkdownDocument]) -> BTreeMap<String, String> {
    let mut index = BTreeMap::new();
    for document in documents {
        let relative = path_to_string(&document.path);
        index.insert(relative.clone(), relative.clone());
        let without_extension = strip_markdown_extension(&relative);
        index
            .entry(without_extension.clone())
            .or_insert(relative.clone());
        if let Some(stem) = document.path.file_stem().and_then(|value| value.to_str()) {
            index.entry(stem.to_owned()).or_insert(relative.clone());
        }
    }
    index
}

fn resolve_link_path(
    current_path: &Path,
    raw_link: &str,
    index: &BTreeMap<String, String>,
) -> Option<String> {
    let mut target = raw_link.split('|').next()?.trim();
    target = target.split('#').next()?.trim();
    if target.is_empty() {
        return None;
    }

    let path_target = Path::new(target);
    if let Some(normalized) = resolve_relative_link(current_path, path_target) {
        let string_path = path_to_string(&normalized);
        if let Some(found) = index.get(&string_path) {
            return Some(found.clone());
        }
        let without_extension = strip_markdown_extension(&string_path);
        if let Some(found) = index.get(&without_extension) {
            return Some(found.clone());
        }
    }

    index.get(target).cloned().or_else(|| {
        let normalized = strip_markdown_extension(target);
        index.get(&normalized).cloned()
    })
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
        CONFIG_KIND, CONFIG_NAME, CONNECTOR_NAME, MARKDOWN_DOCUMENT_LABEL,
        configured_markdown_root, read_markdown_document, set_markdown_root, sync_markdown_root,
    };
    use crate::engine::{CupldEngine, Value};

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
nested:
  enabled: true
---
# Heading
Body with [[other]] and #tagged
"#,
        )
        .unwrap();

        let document = read_markdown_document(&root, Path::new("note.md")).unwrap();
        assert_eq!(document.title, "Frontmatter Title");
        assert_eq!(document.tags, vec!["rust".to_owned(), "tagged".to_owned()]);
        assert_eq!(document.aliases, vec!["One".to_owned(), "Two".to_owned()]);
        assert_eq!(document.headings, vec!["Heading".to_owned()]);
        assert_eq!(document.links, vec!["other".to_owned()]);
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
