use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::f64::consts::PI;
use std::io::{self, IsTerminal, Read, Write};
use std::path::PathBuf;

use cupld::{
    EdgeId, NodeId, Query, QueryResult, RuntimeValue, Session, Statement, Value, parse_script,
};

const DEFAULT_VISUALISE_QUERY: &str = "MATCH (n) RETURN n LIMIT 25";
const MAX_VISIBLE_NODES: usize = 100;
const MAX_SETTLE_STEPS: usize = 96;
const LABEL_MAX_CHARS: usize = 12;
const MIN_SCALE: f64 = 0.5;
const MAX_SCALE: f64 = 24.0;

pub(crate) fn run(db_path: PathBuf, query: Option<String>) -> Result<(), String> {
    validate_interactive_terminals(io::stdin().is_terminal(), io::stdout().is_terminal())?;

    let seed_query = resolve_seed_query(query.as_deref());
    parse_seed_query(seed_query)?;

    let mut session = Session::open(&db_path).map_err(|error| error.to_string())?;
    let mut results = session
        .execute_script(seed_query, &BTreeMap::new())
        .map_err(|error| error.to_string())?;
    let result = results
        .pop()
        .ok_or_else(|| "seed query did not return a result".to_owned())?;

    let graph = SceneGraph::from_session(&session);
    let seed = SeedSubgraph::from_query_result(&graph, &result)?;
    if seed.node_ids.is_empty() && seed.edge_ids.is_empty() {
        return Err("seed query did not return any nodes or edges".to_owned());
    }

    let mut scene = SceneState::new(graph, seed);
    let mut terminal = TerminalGuard::enter()?;
    terminal.run(&mut scene)
}

fn resolve_seed_query(query: Option<&str>) -> &str {
    query.unwrap_or(DEFAULT_VISUALISE_QUERY)
}

fn validate_interactive_terminals(
    stdin_is_terminal: bool,
    stdout_is_terminal: bool,
) -> Result<(), String> {
    if stdin_is_terminal && stdout_is_terminal {
        Ok(())
    } else {
        Err("`--visualise` requires an interactive terminal on stdin and stdout".to_owned())
    }
}

fn parse_seed_query(input: &str) -> Result<Query, String> {
    let statements = parse_script(input).map_err(|error| error.to_string())?;
    if statements.len() != 1 {
        return Err("scene mode requires exactly one read-only query statement".to_owned());
    }

    match statements.into_iter().next().unwrap() {
        Statement::Query(query) => {
            if query.create_clause.is_some()
                || !query.set_clause.is_empty()
                || !query.remove_clause.is_empty()
                || !query.delete_clause.is_empty()
            {
                return Err("scene seed query must be read-only".to_owned());
            }
            if query.return_clause.is_empty() {
                return Err("scene seed query must include RETURN".to_owned());
            }
            Ok(query)
        }
        _ => Err("scene mode requires exactly one read-only query statement".to_owned()),
    }
}

#[derive(Clone, Debug)]
struct SceneNode {
    id: NodeId,
    labels: Vec<String>,
    caption: String,
}

impl SceneNode {
    fn primary_label(&self) -> &str {
        self.labels.first().map(String::as_str).unwrap_or("node")
    }
}

#[derive(Clone, Debug)]
struct SceneEdge {
    id: EdgeId,
    from: NodeId,
    to: NodeId,
    edge_type: String,
}

#[derive(Clone, Debug)]
struct SceneGraph {
    nodes: BTreeMap<NodeId, SceneNode>,
    edges: BTreeMap<EdgeId, SceneEdge>,
    adjacency: BTreeMap<NodeId, Vec<EdgeId>>,
    edge_types: Vec<String>,
}

impl SceneGraph {
    fn from_session(session: &Session) -> Self {
        let mut nodes = BTreeMap::new();
        let mut adjacency = BTreeMap::new();
        for node in session.engine().nodes() {
            let labels = node.labels().iter().cloned().collect::<Vec<_>>();
            adjacency.entry(node.id()).or_insert_with(Vec::new);
            nodes.insert(
                node.id(),
                SceneNode {
                    id: node.id(),
                    labels,
                    caption: preferred_visual_label(
                        node.id().get(),
                        node.property("name"),
                        node.property("title"),
                    ),
                },
            );
        }

        let mut edges = BTreeMap::new();
        let mut edge_types = BTreeSet::new();
        for edge in session.engine().edges() {
            adjacency
                .entry(edge.from())
                .or_insert_with(Vec::new)
                .push(edge.id());
            adjacency
                .entry(edge.to())
                .or_insert_with(Vec::new)
                .push(edge.id());
            edge_types.insert(edge.edge_type().to_owned());
            edges.insert(
                edge.id(),
                SceneEdge {
                    id: edge.id(),
                    from: edge.from(),
                    to: edge.to(),
                    edge_type: edge.edge_type().to_owned(),
                },
            );
        }

        for edge_ids in adjacency.values_mut() {
            edge_ids.sort();
        }

        Self {
            nodes,
            edges,
            adjacency,
            edge_types: edge_types.into_iter().collect(),
        }
    }
}

#[derive(Clone, Debug, Default)]
struct SeedSubgraph {
    node_ids: BTreeSet<NodeId>,
    edge_ids: BTreeSet<EdgeId>,
}

impl SeedSubgraph {
    fn from_query_result(graph: &SceneGraph, result: &QueryResult) -> Result<Self, String> {
        let mut seed = Self::default();
        let mut returned_any_edge = false;

        for row in &result.rows {
            for value in row {
                match value {
                    RuntimeValue::Node(node_id) => {
                        if graph.nodes.contains_key(node_id) {
                            seed.node_ids.insert(*node_id);
                        }
                    }
                    RuntimeValue::Edge(edge_id) => {
                        let Some(edge) = graph.edges.get(edge_id) else {
                            continue;
                        };
                        returned_any_edge = true;
                        seed.edge_ids.insert(*edge_id);
                        seed.node_ids.insert(edge.from);
                        seed.node_ids.insert(edge.to);
                    }
                    _ => {}
                }
            }
        }

        if !returned_any_edge && !seed.node_ids.is_empty() {
            for edge in graph.edges.values() {
                if seed.node_ids.contains(&edge.from) && seed.node_ids.contains(&edge.to) {
                    seed.edge_ids.insert(edge.id);
                }
            }
        }

        if seed.node_ids.is_empty() && seed.edge_ids.is_empty() {
            return Err("seed query did not return any nodes or edges".to_owned());
        }

        Ok(seed)
    }
}

type ExpansionRadii = BTreeMap<NodeId, u8>;

#[derive(Clone, Copy, Debug)]
struct Camera {
    pan_x: f64,
    pan_y: f64,
    scale: f64,
}

impl Default for Camera {
    fn default() -> Self {
        Self {
            pan_x: 0.0,
            pan_y: 0.0,
            scale: 6.0,
        }
    }
}

impl Camera {
    fn recenter(&mut self, position: Point) {
        self.pan_x = position.x;
        self.pan_y = position.y;
    }

    fn zoom_by(&mut self, factor: f64) {
        self.scale = (self.scale * factor).clamp(MIN_SCALE, MAX_SCALE);
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq)]
struct Point {
    x: f64,
    y: f64,
}

impl Point {
    fn add(self, other: Point) -> Self {
        Self {
            x: self.x + other.x,
            y: self.y + other.y,
        }
    }

    fn subtract(self, other: Point) -> Self {
        Self {
            x: self.x - other.x,
            y: self.y - other.y,
        }
    }

    fn scale(self, factor: f64) -> Self {
        Self {
            x: self.x * factor,
            y: self.y * factor,
        }
    }

    fn length(self) -> f64 {
        (self.x * self.x + self.y * self.y).sqrt()
    }

    fn normalized(self) -> Self {
        let length = self.length();
        if length <= f64::EPSILON {
            Self { x: 0.0, y: 0.0 }
        } else {
            self.scale(1.0 / length)
        }
    }
}

#[derive(Clone, Debug, Default)]
struct LayoutState {
    positions: BTreeMap<NodeId, Point>,
    velocities: BTreeMap<NodeId, Point>,
}

impl LayoutState {
    fn sync_positions(
        &mut self,
        graph: &SceneGraph,
        visible_nodes: &BTreeSet<NodeId>,
        anchor: Option<NodeId>,
    ) {
        for node_id in visible_nodes {
            if self.positions.contains_key(node_id) {
                continue;
            }
            let position = self.seed_position(*node_id, graph, visible_nodes, anchor);
            self.positions.insert(*node_id, position);
            self.velocities.entry(*node_id).or_default();
        }
    }

    fn seed_position(
        &self,
        node_id: NodeId,
        graph: &SceneGraph,
        visible_nodes: &BTreeSet<NodeId>,
        anchor: Option<NodeId>,
    ) -> Point {
        if let Some(anchor_id) = anchor
            && let Some(anchor_position) = self.positions.get(&anchor_id).copied()
        {
            return anchor_position.add(radial_offset(node_id, 0.9));
        }

        let mut neighbor_positions = Vec::new();
        if let Some(edge_ids) = graph.adjacency.get(&node_id) {
            for edge_id in edge_ids {
                let edge = graph.edges.get(edge_id).expect("adjacency edge exists");
                let neighbor = if edge.from == node_id {
                    edge.to
                } else {
                    edge.from
                };
                if !visible_nodes.contains(&neighbor) {
                    continue;
                }
                if let Some(position) = self.positions.get(&neighbor) {
                    neighbor_positions.push(*position);
                }
            }
        }

        if !neighbor_positions.is_empty() {
            let count = neighbor_positions.len() as f64;
            let sum = neighbor_positions
                .into_iter()
                .fold(Point::default(), |acc, point| acc.add(point));
            return sum.scale(1.0 / count).add(radial_offset(node_id, 0.5));
        }

        radial_offset(node_id, 2.0)
    }

    fn step(
        &mut self,
        graph: &SceneGraph,
        visible_nodes: &BTreeSet<NodeId>,
        visible_edges: &BTreeSet<EdgeId>,
    ) -> bool {
        let ordered_nodes = visible_nodes.iter().copied().collect::<Vec<_>>();
        if ordered_nodes.is_empty() {
            return false;
        }

        let mut forces = BTreeMap::new();
        for node_id in &ordered_nodes {
            forces.insert(*node_id, Point::default());
            self.velocities.entry(*node_id).or_default();
            self.positions
                .entry(*node_id)
                .or_insert_with(|| radial_offset(*node_id, 2.0));
        }

        for (index, left_id) in ordered_nodes.iter().enumerate() {
            for right_id in ordered_nodes.iter().skip(index + 1) {
                let left = self.positions[left_id];
                let right = self.positions[right_id];
                let delta = left.subtract(right);
                let distance_sq = (delta.x * delta.x + delta.y * delta.y).max(0.05);
                let force = delta.normalized().scale(0.08 / distance_sq);
                *forces.get_mut(left_id).unwrap() = forces[left_id].add(force);
                *forces.get_mut(right_id).unwrap() = forces[right_id].subtract(force);
            }
        }

        for edge_id in visible_edges {
            let edge = graph.edges.get(edge_id).expect("visible edge exists");
            let from = self.positions[&edge.from];
            let to = self.positions[&edge.to];
            let delta = to.subtract(from);
            let distance = delta.length().max(0.01);
            let force = delta.normalized().scale((distance - 2.0) * 0.035);
            *forces.get_mut(&edge.from).unwrap() = forces[&edge.from].add(force);
            *forces.get_mut(&edge.to).unwrap() = forces[&edge.to].subtract(force);
        }

        let mut changed = false;
        for node_id in ordered_nodes {
            let force = forces[&node_id];
            let position = self.positions.get_mut(&node_id).unwrap();
            let velocity = self.velocities.get_mut(&node_id).unwrap();
            velocity.x = (velocity.x + force.x) * 0.82;
            velocity.y = (velocity.y + force.y) * 0.82;
            let delta_x = velocity.x * 0.18;
            let delta_y = velocity.y * 0.18;
            position.x += delta_x;
            position.y += delta_y;
            if delta_x.abs() > 0.000_1 || delta_y.abs() > 0.000_1 {
                changed = true;
            }
        }
        changed
    }

    fn settle(
        &mut self,
        graph: &SceneGraph,
        visible_nodes: &BTreeSet<NodeId>,
        visible_edges: &BTreeSet<EdgeId>,
    ) {
        for _ in 0..MAX_SETTLE_STEPS {
            if !self.step(graph, visible_nodes, visible_edges) {
                break;
            }
        }
    }
}

#[derive(Clone, Debug)]
struct EdgeTypeFilter {
    all_types: Vec<String>,
    hidden_types: BTreeSet<String>,
}

impl EdgeTypeFilter {
    fn new(all_types: Vec<String>) -> Self {
        Self {
            all_types,
            hidden_types: BTreeSet::new(),
        }
    }

    fn is_visible(&self, edge_type: &str) -> bool {
        !self.hidden_types.contains(edge_type)
    }

    fn toggle_digit(&mut self, digit: usize) -> Option<String> {
        let edge_type = self.all_types.get(digit.checked_sub(1)?).cloned()?;
        if self.hidden_types.contains(&edge_type) {
            self.hidden_types.remove(&edge_type);
        } else {
            self.hidden_types.insert(edge_type.clone());
        }
        Some(edge_type)
    }

    fn legend(&self) -> String {
        self.all_types
            .iter()
            .take(9)
            .enumerate()
            .map(|(index, edge_type)| {
                let state = if self.is_visible(edge_type) {
                    "on"
                } else {
                    "off"
                };
                format!("{}:{} {}", index + 1, edge_type, state)
            })
            .collect::<Vec<_>>()
            .join("  ")
    }
}

#[derive(Clone, Debug)]
struct SceneState {
    graph: SceneGraph,
    seed: SeedSubgraph,
    expansion_radii: ExpansionRadii,
    edge_filter: EdgeTypeFilter,
    visible_nodes: BTreeSet<NodeId>,
    visible_edges: BTreeSet<EdgeId>,
    selected: Option<NodeId>,
    camera: Camera,
    layout: LayoutState,
    layout_frozen: bool,
    filter_mode: bool,
    help_overlay_visible: bool,
    cap_hit: bool,
    status_message: String,
}

impl SceneState {
    fn new(graph: SceneGraph, seed: SeedSubgraph) -> Self {
        let mut expansion_radii = BTreeMap::new();
        for node_id in &seed.node_ids {
            expansion_radii.insert(*node_id, 0);
        }

        let mut scene = Self {
            edge_filter: EdgeTypeFilter::new(graph.edge_types.clone()),
            graph,
            seed,
            expansion_radii,
            visible_nodes: BTreeSet::new(),
            visible_edges: BTreeSet::new(),
            selected: None,
            camera: Camera::default(),
            layout: LayoutState::default(),
            layout_frozen: false,
            filter_mode: false,
            help_overlay_visible: true,
            cap_hit: false,
            status_message: String::new(),
        };
        scene.rebuild_visible(None);
        scene
            .layout
            .settle(&scene.graph, &scene.visible_nodes, &scene.visible_edges);
        scene.recenter_on_selection();
        scene
    }

    fn rebuild_visible(&mut self, anchor: Option<NodeId>) {
        let mut visible_nodes = BTreeSet::new();
        let mut visible_edges = BTreeSet::new();
        let mut cap_hit = false;

        for node_id in &self.seed.node_ids {
            if visible_nodes.len() >= MAX_VISIBLE_NODES {
                cap_hit = true;
                break;
            }
            visible_nodes.insert(*node_id);
        }

        for edge_id in &self.seed.edge_ids {
            let Some(edge) = self.graph.edges.get(edge_id) else {
                continue;
            };
            if !self.edge_filter.is_visible(&edge.edge_type) {
                continue;
            }
            if visible_nodes.contains(&edge.from) && visible_nodes.contains(&edge.to) {
                visible_edges.insert(*edge_id);
            }
        }

        let mut expansion_sources = self
            .expansion_radii
            .iter()
            .filter_map(|(node_id, radius)| (*radius > 0).then_some((*node_id, *radius)))
            .collect::<Vec<_>>();
        expansion_sources.sort_by_key(|(node_id, _)| *node_id);

        for (source, radius) in expansion_sources {
            if !visible_nodes.contains(&source) {
                continue;
            }
            let mut queue = VecDeque::from([(source, 0u8)]);
            let mut seen = BTreeSet::from([source]);
            while let Some((node_id, depth)) = queue.pop_front() {
                if depth >= radius {
                    continue;
                }
                let Some(edge_ids) = self.graph.adjacency.get(&node_id) else {
                    continue;
                };
                for edge_id in edge_ids {
                    let edge = self
                        .graph
                        .edges
                        .get(edge_id)
                        .expect("adjacency edge exists");
                    if !self.edge_filter.is_visible(&edge.edge_type) {
                        continue;
                    }
                    let neighbor = if edge.from == node_id {
                        edge.to
                    } else {
                        edge.from
                    };
                    if !visible_nodes.contains(&neighbor) {
                        if visible_nodes.len() >= MAX_VISIBLE_NODES {
                            cap_hit = true;
                            continue;
                        }
                        visible_nodes.insert(neighbor);
                    }
                    visible_edges.insert(*edge_id);
                    if seen.insert(neighbor) {
                        queue.push_back((neighbor, depth + 1));
                    }
                }
            }
        }

        self.visible_nodes = visible_nodes;
        self.visible_edges = visible_edges
            .into_iter()
            .filter(|edge_id| {
                let edge = self.graph.edges.get(edge_id).expect("visible edge exists");
                self.visible_nodes.contains(&edge.from) && self.visible_nodes.contains(&edge.to)
            })
            .collect();
        self.cap_hit = cap_hit;
        self.layout
            .sync_positions(&self.graph, &self.visible_nodes, anchor);
        self.selected = self
            .selected
            .filter(|node_id| self.visible_nodes.contains(node_id))
            .or_else(|| self.visible_nodes.iter().next().copied());
    }

    fn recenter_on_selection(&mut self) {
        if let Some(node_id) = self.selected
            && let Some(position) = self.layout.positions.get(&node_id).copied()
        {
            self.camera.recenter(position);
        }
    }

    fn cycle_selection(&mut self, step: isize) {
        let ordered = self.visible_nodes.iter().copied().collect::<Vec<_>>();
        if ordered.is_empty() {
            self.selected = None;
            return;
        }
        let current_index = self
            .selected
            .and_then(|node_id| ordered.iter().position(|candidate| *candidate == node_id))
            .unwrap_or(0);
        let len = ordered.len() as isize;
        let next_index = (current_index as isize + step).rem_euclid(len) as usize;
        self.selected = Some(ordered[next_index]);
    }

    fn expand_selected(&mut self) {
        let Some(selected) = self.selected else {
            return;
        };
        let radius = self.expansion_radii.entry(selected).or_insert(0);
        *radius = radius.saturating_add(1);
        let expanded_radius = *radius;
        self.rebuild_visible(Some(selected));
        self.status_message = format!("expanded n{} to {} hop(s)", selected.get(), expanded_radius);
    }

    fn collapse_selected(&mut self) {
        let Some(selected) = self.selected else {
            return;
        };
        let radius = self.expansion_radii.entry(selected).or_insert(0);
        if *radius == 0 {
            self.status_message = format!("n{} is already collapsed", selected.get());
            return;
        }
        *radius -= 1;
        let collapsed_radius = *radius;
        self.rebuild_visible(None);
        self.status_message = format!(
            "collapsed n{} to {} hop(s)",
            selected.get(),
            collapsed_radius
        );
    }

    fn toggle_filter_digit(&mut self, digit: usize) {
        if let Some(edge_type) = self.edge_filter.toggle_digit(digit) {
            self.rebuild_visible(None);
            let state = if self.edge_filter.is_visible(&edge_type) {
                "shown"
            } else {
                "hidden"
            };
            self.status_message = format!("{edge_type} {state}");
        }
    }

    fn handle_key(&mut self, key: KeyEvent) -> bool {
        if self.help_overlay_visible {
            match key {
                KeyEvent::Char('q') => return true,
                KeyEvent::Char('?') | KeyEvent::Char('h') => return false,
                _ => {
                    self.help_overlay_visible = false;
                    return false;
                }
            }
        }

        match key {
            KeyEvent::Char('q') => return true,
            KeyEvent::Char('?') | KeyEvent::Char('h') => {
                self.help_overlay_visible = true;
            }
            KeyEvent::Char('n') => self.cycle_selection(1),
            KeyEvent::Char('p') => self.cycle_selection(-1),
            KeyEvent::ArrowLeft => self.camera.pan_x -= 0.5 / self.camera.scale.max(1.0),
            KeyEvent::ArrowRight => self.camera.pan_x += 0.5 / self.camera.scale.max(1.0),
            KeyEvent::ArrowUp => self.camera.pan_y -= 0.5 / self.camera.scale.max(1.0),
            KeyEvent::ArrowDown => self.camera.pan_y += 0.5 / self.camera.scale.max(1.0),
            KeyEvent::Char('+') => self.camera.zoom_by(1.2),
            KeyEvent::Char('-') => self.camera.zoom_by(1.0 / 1.2),
            KeyEvent::Char('0') => self.recenter_on_selection(),
            KeyEvent::Char('e') => self.expand_selected(),
            KeyEvent::Char('c') => self.collapse_selected(),
            KeyEvent::Char('f') => {
                self.filter_mode = !self.filter_mode;
                self.status_message = if self.filter_mode {
                    "filter mode enabled".to_owned()
                } else {
                    "filter mode disabled".to_owned()
                };
            }
            KeyEvent::Char(' ') => {
                self.layout_frozen = !self.layout_frozen;
                self.status_message = if self.layout_frozen {
                    "layout frozen".to_owned()
                } else {
                    "layout live".to_owned()
                };
            }
            KeyEvent::Char('r') => {
                for velocity in self.layout.velocities.values_mut() {
                    *velocity = Point::default();
                }
                self.layout_frozen = false;
                self.status_message = "layout reset".to_owned();
            }
            KeyEvent::Char(ch) if self.filter_mode && ch.is_ascii_digit() => {
                if let Some(index) = ch.to_digit(10) {
                    self.toggle_filter_digit(index as usize);
                }
            }
            _ => {}
        }
        false
    }
}

const HELP_OVERLAY_LINES: &[&str] = &[
    "Bottom bar shows selection, counts, layout, and last action.",
    "",
    "n/p: select next or previous node",
    "e/c: expand or collapse the selected node",
    "arrows: pan the camera",
    "+/-: zoom in or out",
    "0: recenter on the selected node",
    "f: filter relationship types; use digits 1-9 there",
    "space: freeze or unfreeze layout",
    "r: rerun layout from current positions",
    "q: quit",
    "? or h: reopen this guide",
    "",
    "Press any key to close.",
];

fn radial_offset(node_id: NodeId, radius: f64) -> Point {
    let angle = (node_id.get() as f64 * 0.618_033_988_75 * PI * 2.0) % (PI * 2.0);
    Point {
        x: angle.cos() * radius,
        y: angle.sin() * radius,
    }
}

fn preferred_visual_label(node_id: u64, name: Option<&Value>, title: Option<&Value>) -> String {
    value_label_text(name)
        .or_else(|| value_label_text(title))
        .unwrap_or_else(|| format!("n{}", node_id))
}

fn value_label_text(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::Null => Some("null".to_owned()),
        Value::Bool(value) => Some(value.to_string()),
        Value::Int(value) => Some(value.to_string()),
        Value::Float(value) => Some(value.to_string()),
        Value::String(value) => Some(value.clone()),
        Value::Bytes(value) => Some(format!("{value:?}")),
        Value::Datetime(value) => Some(format!("{value:?}")),
        Value::List(_) | Value::Map(_) => None,
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Cell {
    ch: char,
    fg: Option<u8>,
}

impl Default for Cell {
    fn default() -> Self {
        Self { ch: ' ', fg: None }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Frame {
    width: usize,
    height: usize,
    cells: Vec<Cell>,
}

impl Frame {
    fn new(width: usize, height: usize) -> Self {
        Self {
            width,
            height,
            cells: vec![Cell::default(); width.saturating_mul(height)],
        }
    }

    fn set(&mut self, x: i32, y: i32, ch: char, fg: Option<u8>) {
        if x < 0 || y < 0 {
            return;
        }
        let (x, y) = (x as usize, y as usize);
        if x >= self.width || y >= self.height {
            return;
        }
        let index = y * self.width + x;
        self.cells[index] = Cell { ch, fg };
    }

    fn write_text(&mut self, x: i32, y: i32, text: &str, fg: Option<u8>) {
        for (offset, ch) in text.chars().enumerate() {
            self.set(x + offset as i32, y, ch, fg);
        }
    }

    #[cfg(test)]
    fn plain_lines(&self) -> Vec<String> {
        (0..self.height)
            .map(|row| {
                self.cells[row * self.width..(row + 1) * self.width]
                    .iter()
                    .map(|cell| cell.ch)
                    .collect()
            })
            .collect()
    }

    fn to_ansi_string(&self, clear_screen: bool) -> String {
        let mut output = if clear_screen {
            String::from("\x1b[2J\x1b[H")
        } else {
            String::from("\x1b[H")
        };
        for row in 0..self.height {
            let mut current_color = None;
            for cell in &self.cells[row * self.width..(row + 1) * self.width] {
                if cell.fg != current_color {
                    match cell.fg {
                        Some(color) => {
                            output.push_str("\x1b[");
                            output.push_str(&color.to_string());
                            output.push('m');
                        }
                        None => output.push_str("\x1b[39m"),
                    }
                    current_color = cell.fg;
                }
                output.push(cell.ch);
            }
            if current_color.is_some() {
                output.push_str("\x1b[39m");
            }
            if row + 1 < self.height {
                output.push_str("\r\n");
            }
        }
        output
    }
}

#[derive(Clone, Debug)]
struct BrailleBuffer {
    width: usize,
    height: usize,
    masks: Vec<u8>,
    colors: Vec<Option<u8>>,
}

impl BrailleBuffer {
    fn new(width: usize, height: usize) -> Self {
        Self {
            width,
            height,
            masks: vec![0; width.saturating_mul(height)],
            colors: vec![None; width.saturating_mul(height)],
        }
    }

    fn plot(&mut self, sub_x: i32, sub_y: i32, color: Option<u8>) {
        if sub_x < 0 || sub_y < 0 {
            return;
        }
        let cell_x = sub_x as usize / 2;
        let cell_y = sub_y as usize / 4;
        if cell_x >= self.width || cell_y >= self.height {
            return;
        }
        let index = cell_y * self.width + cell_x;
        self.masks[index] |= braille_bit((sub_x as usize) % 2, (sub_y as usize) % 4);
        if self.colors[index].is_none() {
            self.colors[index] = color;
        }
    }

    fn draw_line(&mut self, start: (i32, i32), end: (i32, i32), color: Option<u8>) {
        let (mut x0, mut y0) = start;
        let (x1, y1) = end;
        let dx = (x1 - x0).abs();
        let sx = if x0 < x1 { 1 } else { -1 };
        let dy = -(y1 - y0).abs();
        let sy = if y0 < y1 { 1 } else { -1 };
        let mut error = dx + dy;

        loop {
            self.plot(x0, y0, color);
            if x0 == x1 && y0 == y1 {
                break;
            }
            let doubled = error * 2;
            if doubled >= dy {
                error += dy;
                x0 += sx;
            }
            if doubled <= dx {
                error += dx;
                y0 += sy;
            }
        }
    }

    fn flush_into(self, frame: &mut Frame) {
        for y in 0..self.height {
            for x in 0..self.width {
                let index = y * self.width + x;
                if self.masks[index] == 0 {
                    continue;
                }
                frame.set(
                    x as i32,
                    y as i32,
                    braille_char(self.masks[index]),
                    self.colors[index],
                );
            }
        }
    }
}

fn braille_bit(x: usize, y: usize) -> u8 {
    match (x, y) {
        (0, 0) => 0b0000_0001,
        (0, 1) => 0b0000_0010,
        (0, 2) => 0b0000_0100,
        (1, 0) => 0b0000_1000,
        (1, 1) => 0b0001_0000,
        (1, 2) => 0b0010_0000,
        (0, 3) => 0b0100_0000,
        (1, 3) => 0b1000_0000,
        _ => 0,
    }
}

fn braille_char(mask: u8) -> char {
    char::from_u32(0x2800 + mask as u32).unwrap_or(' ')
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Viewport {
    width: usize,
    height: usize,
}

fn render_scene(scene: &SceneState, viewport: Viewport) -> Frame {
    let mut frame = Frame::new(viewport.width, viewport.height);
    if viewport.width == 0 || viewport.height == 0 {
        return frame;
    }

    let plot_height = viewport.height.saturating_sub(1);
    if !scene.help_overlay_visible {
        let mut braille = BrailleBuffer::new(viewport.width, plot_height);
        let edge_lanes = edge_lane_offsets(scene);

        for edge_id in &scene.visible_edges {
            let edge = scene.graph.edges.get(edge_id).expect("visible edge exists");
            let Some(from) = scene.layout.positions.get(&edge.from).copied() else {
                continue;
            };
            let Some(to) = scene.layout.positions.get(&edge.to).copied() else {
                continue;
            };
            let lane = edge_lanes.get(edge_id).copied().unwrap_or(0.0);
            let (from, to) = offset_edge_points(from, to, lane);
            let Some(start) = project_subcell(scene.camera, from, viewport.width, plot_height)
            else {
                continue;
            };
            let Some(end) = project_subcell(scene.camera, to, viewport.width, plot_height) else {
                continue;
            };
            braille.draw_line(start, end, Some(edge_color(&edge.edge_type)));
        }
        braille.flush_into(&mut frame);

        for node_id in &scene.visible_nodes {
            let Some(position) = scene.layout.positions.get(node_id).copied() else {
                continue;
            };
            let Some((x, y)) = project_cell(scene.camera, position, viewport.width, plot_height)
            else {
                continue;
            };
            let node = scene.graph.nodes.get(node_id).expect("visible node exists");
            let glyph = if scene.selected == Some(*node_id) {
                '◎'
            } else {
                '●'
            };
            frame.set(x, y, glyph, Some(node_color(node.primary_label())));
        }

        let mut occupied = Vec::new();
        let mut label_candidates = scene
            .visible_nodes
            .iter()
            .copied()
            .map(|node_id| {
                let priority = if scene.selected == Some(node_id) {
                    0
                } else if scene.seed.node_ids.contains(&node_id) {
                    1
                } else {
                    2
                };
                (priority, node_id)
            })
            .collect::<Vec<_>>();
        label_candidates.sort();

        for (_, node_id) in label_candidates {
            let Some(position) = scene.layout.positions.get(&node_id).copied() else {
                continue;
            };
            let Some((x, y)) = project_cell(scene.camera, position, viewport.width, plot_height)
            else {
                continue;
            };
            let node = scene
                .graph
                .nodes
                .get(&node_id)
                .expect("visible node exists");
            let label = truncate_label(&node.caption, LABEL_MAX_CHARS);
            let label_width = label.chars().count() as i32;
            let mut label_x = x + 1;
            if label_x + label_width >= viewport.width as i32 {
                label_x = (x - label_width - 1).max(0);
            }
            let rect = LabelRect {
                x: label_x,
                y,
                width: label_width.max(0),
            };
            let is_selected = scene.selected == Some(node_id);
            if !is_selected && occupied.iter().any(|other| rect.overlaps(other)) {
                continue;
            }
            frame.write_text(
                rect.x,
                rect.y,
                &label,
                Some(node_color(node.primary_label())),
            );
            occupied.push(rect);
        }
    }

    let status = truncate_label(&status_line(scene), viewport.width);
    frame.write_text(0, viewport.height as i32 - 1, &status, Some(37));
    if scene.help_overlay_visible {
        render_help_overlay(&mut frame, viewport);
    }
    frame
}

fn render_help_overlay(frame: &mut Frame, viewport: Viewport) {
    let available_height = viewport.height.saturating_sub(1);
    if viewport.width < 8 || available_height < 5 {
        return;
    }

    let inner_width = HELP_OVERLAY_LINES
        .iter()
        .map(|line| line.chars().count())
        .max()
        .unwrap_or(0)
        .min(viewport.width.saturating_sub(4));
    let box_width = (inner_width + 4).min(viewport.width);
    let visible_lines = HELP_OVERLAY_LINES
        .len()
        .min(available_height.saturating_sub(2));
    let box_height = visible_lines + 2;
    if box_width < 4 || box_height < 3 {
        return;
    }

    let origin_x = ((viewport.width - box_width) / 2) as i32;
    let origin_y = ((available_height - box_height) / 2) as i32;
    let right = origin_x + box_width as i32 - 1;
    let bottom = origin_y + box_height as i32 - 1;

    for y in origin_y..=bottom {
        for x in origin_x..=right {
            frame.set(x, y, ' ', None);
        }
    }

    for x in origin_x + 1..right {
        frame.set(x, origin_y, '─', Some(37));
        frame.set(x, bottom, '─', Some(37));
    }
    for y in origin_y + 1..bottom {
        frame.set(origin_x, y, '│', Some(37));
        frame.set(right, y, '│', Some(37));
    }
    frame.set(origin_x, origin_y, '┌', Some(37));
    frame.set(right, origin_y, '┐', Some(37));
    frame.set(origin_x, bottom, '└', Some(37));
    frame.set(right, bottom, '┘', Some(37));

    let title = " Scene Guide ";
    let title_x = origin_x + ((box_width as i32 - title.chars().count() as i32) / 2).max(1);
    frame.write_text(title_x, origin_y, title, Some(36));

    for (index, line) in HELP_OVERLAY_LINES.iter().take(visible_lines).enumerate() {
        let y = origin_y + 1 + index as i32;
        let text = truncate_label(line, box_width.saturating_sub(4));
        frame.write_text(origin_x + 2, y, &text, Some(37));
    }
}

fn edge_lane_offsets(scene: &SceneState) -> BTreeMap<EdgeId, f64> {
    let mut grouped = BTreeMap::<(NodeId, NodeId), Vec<&SceneEdge>>::new();
    for edge_id in &scene.visible_edges {
        let edge = scene.graph.edges.get(edge_id).expect("visible edge exists");
        let key = if edge.from <= edge.to {
            (edge.from, edge.to)
        } else {
            (edge.to, edge.from)
        };
        grouped.entry(key).or_default().push(edge);
    }

    let mut lanes = BTreeMap::new();
    for (_, edges) in grouped.iter_mut() {
        edges.sort_by(|left, right| {
            left.edge_type
                .cmp(&right.edge_type)
                .then(left.from.cmp(&right.from))
                .then(left.to.cmp(&right.to))
                .then(left.id.cmp(&right.id))
        });
        let midpoint = (edges.len().saturating_sub(1) as f64) / 2.0;
        for (index, edge) in edges.iter().enumerate() {
            lanes.insert(edge.id, (index as f64 - midpoint) * 0.35);
        }
    }
    lanes
}

fn status_line(scene: &SceneState) -> String {
    let selection = scene
        .selected
        .and_then(|node_id| scene.graph.nodes.get(&node_id))
        .map(|node| format!("sel n{} {}", node.id.get(), node.primary_label()))
        .unwrap_or_else(|| "sel none".to_owned());
    let layout = if scene.layout_frozen {
        "frozen"
    } else {
        "live"
    };
    let mut status = format!(
        "{} | nodes {}/{} edges {} | layout {}",
        selection,
        scene.visible_nodes.len(),
        MAX_VISIBLE_NODES,
        scene.visible_edges.len(),
        layout
    );
    if scene.cap_hit {
        status.push_str(" | cap hit");
    }
    if !scene.status_message.is_empty() {
        status.push_str(" | ");
        status.push_str(&scene.status_message);
    }
    status.push_str(" | ");
    if scene.filter_mode {
        status.push_str("filter ");
        status.push_str(&scene.edge_filter.legend());
    } else {
        status.push_str(
            "n/p select  e/c expand  arrows pan  +/- zoom  0 center  f filter  space freeze  r rerun  ? help  q quit",
        );
    }
    status
}

fn truncate_label(input: &str, max_chars: usize) -> String {
    let char_count = input.chars().count();
    if char_count <= max_chars {
        return input.to_owned();
    }
    if max_chars <= 1 {
        return "…".to_owned();
    }
    input
        .chars()
        .take(max_chars - 1)
        .chain(std::iter::once('…'))
        .collect()
}

#[derive(Clone, Copy, Debug)]
struct LabelRect {
    x: i32,
    y: i32,
    width: i32,
}

impl LabelRect {
    fn overlaps(&self, other: &Self) -> bool {
        self.y == other.y && self.x < other.x + other.width && other.x < self.x + self.width
    }
}

fn project_cell(camera: Camera, point: Point, width: usize, height: usize) -> Option<(i32, i32)> {
    if width == 0 || height == 0 {
        return None;
    }
    Some((
        ((point.x - camera.pan_x) * camera.scale + width as f64 / 2.0).round() as i32,
        ((point.y - camera.pan_y) * camera.scale + height as f64 / 2.0).round() as i32,
    ))
}

fn project_subcell(
    camera: Camera,
    point: Point,
    width: usize,
    height: usize,
) -> Option<(i32, i32)> {
    if width == 0 || height == 0 {
        return None;
    }
    Some((
        ((point.x - camera.pan_x) * camera.scale * 2.0 + width as f64).round() as i32,
        ((point.y - camera.pan_y) * camera.scale * 4.0 + (height as f64 * 2.0)).round() as i32,
    ))
}

fn offset_edge_points(from: Point, to: Point, lane: f64) -> (Point, Point) {
    if lane.abs() <= f64::EPSILON {
        return (from, to);
    }
    let delta = to.subtract(from);
    let length = delta.length();
    if length <= f64::EPSILON {
        return (from, to);
    }
    let normal = Point {
        x: -delta.y / length,
        y: delta.x / length,
    };
    let offset = normal.scale(lane);
    (from.add(offset), to.add(offset))
}

fn stable_color(hash_seed: &str, palette: &[u8]) -> u8 {
    let hash = hash_seed.bytes().fold(0u64, |acc, byte| {
        acc.wrapping_mul(131).wrapping_add(byte as u64)
    });
    palette[(hash as usize) % palette.len()]
}

fn node_color(label: &str) -> u8 {
    stable_color(label, &[31, 32, 33, 34, 36, 91, 92, 93, 94, 96])
}

fn edge_color(edge_type: &str) -> u8 {
    stable_color(edge_type, &[90, 91, 92, 93, 94, 96, 37])
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum KeyEvent {
    ArrowUp,
    ArrowDown,
    ArrowLeft,
    ArrowRight,
    Char(char),
}

fn parse_key(bytes: &[u8]) -> Option<KeyEvent> {
    match bytes {
        [0x1b, b'[', b'A', ..] => Some(KeyEvent::ArrowUp),
        [0x1b, b'[', b'B', ..] => Some(KeyEvent::ArrowDown),
        [0x1b, b'[', b'C', ..] => Some(KeyEvent::ArrowRight),
        [0x1b, b'[', b'D', ..] => Some(KeyEvent::ArrowLeft),
        [byte, ..] if byte.is_ascii() => Some(KeyEvent::Char(*byte as char)),
        _ => None,
    }
}

struct TerminalGuard {
    stdin: io::Stdin,
    stdout: io::Stdout,
    raw_mode: RawModeGuard,
}

impl TerminalGuard {
    fn enter() -> Result<Self, String> {
        let stdin = io::stdin();
        let mut stdout = io::stdout();
        let raw_mode = RawModeGuard::new(&stdin)?;
        stdout
            .write_all(b"\x1b[?1049h\x1b[?25l\x1b[2J\x1b[H")
            .and_then(|_| stdout.flush())
            .map_err(|error| error.to_string())?;
        Ok(Self {
            stdin,
            stdout,
            raw_mode,
        })
    }

    fn run(&mut self, scene: &mut SceneState) -> Result<(), String> {
        let mut last_viewport = None;
        let mut clear_screen = true;
        let mut last_frame = None;
        let mut needs_redraw = true;

        loop {
            let viewport = viewport(&self.stdout).unwrap_or_else(|_| {
                last_viewport.unwrap_or(Viewport {
                    width: 80,
                    height: 24,
                })
            });
            if last_viewport != Some(viewport) {
                last_viewport = Some(viewport);
                clear_screen = true;
                needs_redraw = true;
            }

            if needs_redraw {
                let frame = render_scene(scene, viewport);
                if clear_screen || last_frame.as_ref() != Some(&frame) {
                    self.stdout
                        .write_all(frame.to_ansi_string(clear_screen).as_bytes())
                        .and_then(|_| self.stdout.flush())
                        .map_err(|error| error.to_string())?;
                }
                last_frame = Some(frame);
                clear_screen = false;
                needs_redraw = false;
            }

            if !scene.layout_frozen {
                let changed =
                    scene
                        .layout
                        .step(&scene.graph, &scene.visible_nodes, &scene.visible_edges);
                needs_redraw |= changed;
            }

            if let Some(key) = self.read_key()? {
                if scene.handle_key(key) {
                    return Ok(());
                }
                needs_redraw = true;
            }
        }
    }

    fn read_key(&mut self) -> Result<Option<KeyEvent>, String> {
        let _keep_raw_mode_alive = &self.raw_mode;
        let mut buffer = [0u8; 8];
        let bytes_read = self
            .stdin
            .read(&mut buffer)
            .map_err(|error| error.to_string())?;
        if bytes_read == 0 {
            return Ok(None);
        }
        Ok(parse_key(&buffer[..bytes_read]))
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let _ = self.stdout.write_all(b"\x1b[39m\x1b[?25h\x1b[?1049l");
        let _ = self.stdout.flush();
    }
}

#[cfg(unix)]
struct RawModeGuard {
    fd: std::os::fd::RawFd,
    original: termios_platform::Termios,
}

#[cfg(unix)]
impl RawModeGuard {
    fn new(stdin: &io::Stdin) -> Result<Self, String> {
        use std::os::fd::AsRawFd;

        let fd = stdin.as_raw_fd();
        let original = termios_platform::get(fd)?;
        let mut raw = original;
        termios_platform::make_raw(&mut raw);
        termios_platform::set(fd, &raw)?;
        Ok(Self { fd, original })
    }
}

#[cfg(unix)]
impl Drop for RawModeGuard {
    fn drop(&mut self) {
        let _ = termios_platform::set(self.fd, &self.original);
    }
}

#[cfg(not(unix))]
struct RawModeGuard;

#[cfg(not(unix))]
impl RawModeGuard {
    fn new(_: &io::Stdin) -> Result<Self, String> {
        Err("scene mode is only supported on unix in v1".to_owned())
    }
}

#[cfg(unix)]
fn viewport(stdout: &io::Stdout) -> Result<Viewport, String> {
    use std::os::fd::AsRawFd;

    termios_platform::viewport(stdout.as_raw_fd())
}

#[cfg(not(unix))]
fn viewport(_: &io::Stdout) -> Result<Viewport, String> {
    Err("scene mode is only supported on unix in v1".to_owned())
}

#[cfg(unix)]
mod termios_platform {
    use std::mem::MaybeUninit;
    use std::os::raw::{c_int, c_ulong};

    use super::Viewport;

    #[cfg(target_os = "linux")]
    const NCCS: usize = 32;
    #[cfg(target_os = "macos")]
    const NCCS: usize = 20;

    #[cfg(target_os = "linux")]
    pub(crate) type TcFlag = u32;
    #[cfg(target_os = "linux")]
    type Cc = u8;
    #[cfg(target_os = "linux")]
    type Speed = u32;

    #[cfg(target_os = "macos")]
    pub(crate) type TcFlag = u64;
    #[cfg(target_os = "macos")]
    type Cc = u8;
    #[cfg(target_os = "macos")]
    type Speed = u64;

    #[cfg(target_os = "linux")]
    #[repr(C)]
    #[derive(Clone, Copy)]
    pub(crate) struct Termios {
        c_iflag: TcFlag,
        c_oflag: TcFlag,
        c_cflag: TcFlag,
        c_lflag: TcFlag,
        c_line: Cc,
        c_cc: [Cc; NCCS],
        c_ispeed: Speed,
        c_ospeed: Speed,
    }

    #[cfg(target_os = "macos")]
    #[repr(C)]
    #[derive(Clone, Copy)]
    pub(crate) struct Termios {
        c_iflag: TcFlag,
        c_oflag: TcFlag,
        c_cflag: TcFlag,
        c_lflag: TcFlag,
        c_cc: [Cc; NCCS],
        c_ispeed: Speed,
        c_ospeed: Speed,
    }

    #[repr(C)]
    struct Winsize {
        ws_row: u16,
        ws_col: u16,
        ws_xpixel: u16,
        ws_ypixel: u16,
    }

    #[cfg(target_os = "linux")]
    const ECHO: TcFlag = 0o0000010;
    #[cfg(target_os = "linux")]
    const ICANON: TcFlag = 0o0000002;
    #[cfg(target_os = "linux")]
    const IEXTEN: TcFlag = 0o0100000;
    #[cfg(target_os = "linux")]
    const ISIG: TcFlag = 0o0000001;
    #[cfg(target_os = "linux")]
    const ICRNL: TcFlag = 0o0000400;
    #[cfg(target_os = "linux")]
    const IXON: TcFlag = 0o0002000;
    #[cfg(target_os = "linux")]
    const OPOST: TcFlag = 0o0000001;
    #[cfg(target_os = "linux")]
    const VTIME: usize = 5;
    #[cfg(target_os = "linux")]
    const VMIN: usize = 6;
    #[cfg(target_os = "linux")]
    const TIOCGWINSZ: c_ulong = 0x5413;

    #[cfg(target_os = "macos")]
    const ECHO: TcFlag = 0x0000_0008;
    #[cfg(target_os = "macos")]
    const ICANON: TcFlag = 0x0000_0100;
    #[cfg(target_os = "macos")]
    const IEXTEN: TcFlag = 0x0000_0400;
    #[cfg(target_os = "macos")]
    const ISIG: TcFlag = 0x0000_0080;
    #[cfg(target_os = "macos")]
    const ICRNL: TcFlag = 0x0000_0100;
    #[cfg(target_os = "macos")]
    const IXON: TcFlag = 0x0000_0200;
    #[cfg(target_os = "macos")]
    const OPOST: TcFlag = 0x0000_0001;
    #[cfg(target_os = "macos")]
    const VTIME: usize = 17;
    #[cfg(target_os = "macos")]
    const VMIN: usize = 16;
    #[cfg(target_os = "macos")]
    const TIOCGWINSZ: c_ulong = 0x4008_7468;

    const TCSAFLUSH: c_int = 2;

    unsafe extern "C" {
        fn tcgetattr(fd: c_int, termios_p: *mut Termios) -> c_int;
        fn tcsetattr(fd: c_int, optional_actions: c_int, termios_p: *const Termios) -> c_int;
        fn ioctl(fd: c_int, request: c_ulong, ...) -> c_int;
    }

    pub(crate) fn get(fd: c_int) -> Result<Termios, String> {
        let mut termios = MaybeUninit::<Termios>::uninit();
        let status = unsafe { tcgetattr(fd, termios.as_mut_ptr()) };
        if status == -1 {
            Err(std::io::Error::last_os_error().to_string())
        } else {
            Ok(unsafe { termios.assume_init() })
        }
    }

    pub(crate) fn set(fd: c_int, termios: &Termios) -> Result<(), String> {
        let status = unsafe { tcsetattr(fd, TCSAFLUSH, termios as *const Termios) };
        if status == -1 {
            Err(std::io::Error::last_os_error().to_string())
        } else {
            Ok(())
        }
    }

    pub(crate) fn make_raw(termios: &mut Termios) {
        termios.c_lflag &= !(ECHO | ICANON | IEXTEN | ISIG);
        termios.c_iflag &= !(ICRNL | IXON);
        termios.c_oflag &= !OPOST;
        termios.c_cc[VMIN] = 0;
        termios.c_cc[VTIME] = 1;
    }

    pub(crate) fn viewport(fd: c_int) -> Result<Viewport, String> {
        let mut winsize = MaybeUninit::<Winsize>::zeroed();
        let status = unsafe { ioctl(fd, TIOCGWINSZ, winsize.as_mut_ptr()) };
        if status == -1 {
            return Err(std::io::Error::last_os_error().to_string());
        }
        let winsize = unsafe { winsize.assume_init() };
        Ok(Viewport {
            width: winsize.ws_col.max(1) as usize,
            height: winsize.ws_row.max(1) as usize,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{
        Camera, KeyEvent, MAX_VISIBLE_NODES, Point, SceneGraph, SceneState, SeedSubgraph,
        braille_char, edge_lane_offsets, parse_key, parse_seed_query, project_cell, render_scene,
        resolve_seed_query, truncate_label, validate_interactive_terminals,
    };
    use cupld::Session;
    use std::collections::{BTreeMap, BTreeSet};

    fn seeded_session(statements: &[&str]) -> Session {
        let mut session = Session::new_in_memory();
        for statement in statements {
            session.execute_script(statement, &BTreeMap::new()).unwrap();
        }
        session
    }

    fn node_id_by_name(scene: &SceneGraph, name: &str) -> cupld::NodeId {
        scene
            .nodes
            .values()
            .find(|node| node.caption == name)
            .map(|node| node.id)
            .unwrap()
    }

    #[test]
    fn defaults_visualise_query() {
        assert_eq!(resolve_seed_query(None), "MATCH (n) RETURN n LIMIT 25");
        assert_eq!(
            resolve_seed_query(Some("MATCH (n) RETURN n")),
            "MATCH (n) RETURN n"
        );
    }

    #[test]
    fn validates_seed_query_shape() {
        assert!(parse_seed_query("MATCH (n) RETURN n").is_ok());
        assert!(parse_seed_query("SHOW SCHEMA").is_err());
        assert!(parse_seed_query("MATCH (n) SET n.name = 'Ada' RETURN n").is_err());
        assert!(parse_seed_query("MATCH (n)").is_err());
    }

    #[test]
    fn rejects_non_interactive_terminals() {
        assert!(validate_interactive_terminals(false, true).is_err());
        assert!(validate_interactive_terminals(true, false).is_err());
        assert!(validate_interactive_terminals(true, true).is_ok());
    }

    #[test]
    fn seed_extraction_auto_includes_connecting_edges() {
        let mut session = seeded_session(&[
            "CREATE (a:Person {name: 'Ada'})-[:KNOWS]->(b:Person {name: 'Grace'})",
        ]);
        let mut results = session
            .execute_script(
                "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a, b",
                &BTreeMap::new(),
            )
            .unwrap();
        let graph = SceneGraph::from_session(&session);
        let seed = SeedSubgraph::from_query_result(&graph, &results.remove(0)).unwrap();

        assert_eq!(
            seed.node_ids
                .into_iter()
                .map(|id| id.get())
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert_eq!(
            seed.edge_ids
                .into_iter()
                .map(|id| id.get())
                .collect::<Vec<_>>(),
            vec![1]
        );
    }

    #[test]
    fn expansion_radii_recompute_visible_graph() {
        let session = seeded_session(&[
            "CREATE (a:Person {name: 'Ada'})-[:KNOWS]->(b:Person {name: 'Grace'})",
            "MATCH (b:Person {name: 'Grace'}) CREATE (b)-[:KNOWS]->(c:Person {name: 'Linus'})",
        ]);
        let graph = SceneGraph::from_session(&session);
        let ada = node_id_by_name(&graph, "Ada");
        let seed = SeedSubgraph {
            node_ids: BTreeSet::from([ada]),
            edge_ids: BTreeSet::new(),
        };
        let mut scene = SceneState::new(graph, seed);

        assert_eq!(
            scene
                .visible_nodes
                .iter()
                .map(|id| id.get())
                .collect::<Vec<_>>(),
            vec![ada.get()]
        );
        scene.expand_selected();
        assert_eq!(
            scene
                .visible_nodes
                .iter()
                .map(|id| id.get())
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        scene.expand_selected();
        assert_eq!(
            scene
                .visible_nodes
                .iter()
                .map(|id| id.get())
                .collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        scene.collapse_selected();
        assert_eq!(
            scene
                .visible_nodes
                .iter()
                .map(|id| id.get())
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
    }

    #[test]
    fn edge_type_filter_blocks_expansion() {
        let session = seeded_session(&[
            "CREATE (a:Person {name: 'Ada'})-[:KNOWS]->(b:Person {name: 'Grace'})",
            "MATCH (a:Person {name: 'Ada'}) CREATE (a)-[:LIKES]->(c:Person {name: 'Linus'})",
        ]);
        let graph = SceneGraph::from_session(&session);
        let ada = node_id_by_name(&graph, "Ada");
        let seed = SeedSubgraph {
            node_ids: BTreeSet::from([ada]),
            edge_ids: BTreeSet::new(),
        };
        let mut scene = SceneState::new(graph, seed);
        scene.filter_mode = true;
        scene.toggle_filter_digit(1);
        scene.expand_selected();

        assert_eq!(
            scene
                .visible_nodes
                .iter()
                .map(|id| id.get())
                .collect::<Vec<_>>(),
            vec![1, 3]
        );
    }

    #[test]
    fn visible_node_cap_is_deterministic() {
        let mut session = Session::new_in_memory();
        session
            .execute_script("CREATE (root:Person {name: 'Root'})", &BTreeMap::new())
            .unwrap();
        for index in 0..150 {
            session
                .execute_script(
                    &format!(
                        "MATCH (root:Person {{name: 'Root'}}) CREATE (root)-[:KNOWS]->(:Person {{name: 'P{index}'}})"
                    ),
                    &BTreeMap::new(),
                )
                .unwrap();
        }
        let graph = SceneGraph::from_session(&session);
        let root = node_id_by_name(&graph, "Root");
        let seed = SeedSubgraph {
            node_ids: BTreeSet::from([root]),
            edge_ids: BTreeSet::new(),
        };
        let mut scene = SceneState::new(graph, seed);
        scene.expand_selected();

        let visible = scene
            .visible_nodes
            .iter()
            .map(|id| id.get())
            .collect::<Vec<_>>();
        assert_eq!(visible.len(), MAX_VISIBLE_NODES);
        assert_eq!(visible.first().copied(), Some(1));
        assert_eq!(visible.last().copied(), Some(100));
        assert!(scene.cap_hit);
    }

    #[test]
    fn selection_cycles_in_sorted_order() {
        let session = seeded_session(&[
            "CREATE (a:Person {name: 'Ada'})",
            "CREATE (b:Person {name: 'Grace'})",
            "CREATE (c:Person {name: 'Linus'})",
        ]);
        let graph = SceneGraph::from_session(&session);
        let ada = node_id_by_name(&graph, "Ada");
        let grace = node_id_by_name(&graph, "Grace");
        let linus = node_id_by_name(&graph, "Linus");
        let seed = SeedSubgraph {
            node_ids: BTreeSet::from([linus, ada, grace]),
            edge_ids: BTreeSet::new(),
        };
        let mut scene = SceneState::new(graph, seed);

        assert_eq!(scene.selected.map(|id| id.get()), Some(ada.get()));
        scene.cycle_selection(1);
        assert_eq!(scene.selected.map(|id| id.get()), Some(grace.get()));
        scene.cycle_selection(-1);
        assert_eq!(scene.selected.map(|id| id.get()), Some(ada.get()));
    }

    #[test]
    fn help_overlay_starts_visible() {
        let session = seeded_session(&["CREATE (a:Person {name: 'Ada'})"]);
        let graph = SceneGraph::from_session(&session);
        let ada = node_id_by_name(&graph, "Ada");
        let seed = SeedSubgraph {
            node_ids: BTreeSet::from([ada]),
            edge_ids: BTreeSet::new(),
        };
        let scene = SceneState::new(graph, seed);

        assert!(scene.help_overlay_visible);
    }

    #[test]
    fn help_overlay_closes_and_reopens() {
        let session = seeded_session(&["CREATE (a:Person {name: 'Ada'})"]);
        let graph = SceneGraph::from_session(&session);
        let ada = node_id_by_name(&graph, "Ada");
        let seed = SeedSubgraph {
            node_ids: BTreeSet::from([ada]),
            edge_ids: BTreeSet::new(),
        };
        let mut scene = SceneState::new(graph, seed);

        assert!(!scene.handle_key(KeyEvent::Char('n')));
        assert!(!scene.help_overlay_visible);
        assert!(!scene.handle_key(KeyEvent::Char('?')));
        assert!(scene.help_overlay_visible);
    }

    #[test]
    fn layout_preserves_existing_positions_across_expand_collapse() {
        let session = seeded_session(&[
            "CREATE (a:Person {name: 'Ada'})-[:KNOWS]->(b:Person {name: 'Grace'})",
        ]);
        let graph = SceneGraph::from_session(&session);
        let ada = node_id_by_name(&graph, "Ada");
        let seed = SeedSubgraph {
            node_ids: BTreeSet::from([ada]),
            edge_ids: BTreeSet::new(),
        };
        let mut scene = SceneState::new(graph, seed);
        scene
            .layout
            .positions
            .insert(ada, Point { x: 4.0, y: -2.0 });

        scene.expand_selected();
        assert_eq!(scene.layout.positions[&ada], Point { x: 4.0, y: -2.0 });
        scene.collapse_selected();
        assert_eq!(scene.layout.positions[&ada], Point { x: 4.0, y: -2.0 });
    }

    #[test]
    fn project_cell_centers_origin() {
        let projected = project_cell(Camera::default(), Point { x: 0.0, y: 0.0 }, 20, 10).unwrap();
        assert_eq!(projected, (10, 5));
    }

    #[test]
    fn braille_encoding_maps_mask_to_character() {
        assert_eq!(braille_char(0b0000_0001), '\u{2801}');
        assert_eq!(braille_char(0b0100_0111), '\u{2847}');
    }

    #[test]
    fn truncates_labels_with_ellipsis() {
        assert_eq!(truncate_label("abcdefghijklmnop", 12), "abcdefghijk…");
        assert_eq!(truncate_label("short", 12), "short");
    }

    #[test]
    fn render_suppresses_colliding_non_selected_labels() {
        let session = seeded_session(&[
            "CREATE (a:Person {name: 'Ada'})",
            "CREATE (b:Person {name: 'Alan'})",
        ]);
        let graph = SceneGraph::from_session(&session);
        let ada = node_id_by_name(&graph, "Ada");
        let alan = node_id_by_name(&graph, "Alan");
        let seed = SeedSubgraph {
            node_ids: BTreeSet::from([ada, alan]),
            edge_ids: BTreeSet::new(),
        };
        let mut scene = SceneState::new(graph, seed);
        scene.layout.positions.insert(ada, Point { x: 0.0, y: 0.0 });
        scene
            .layout
            .positions
            .insert(alan, Point { x: 0.05, y: 0.0 });
        scene.selected = Some(ada);
        scene.camera = Camera::default();
        scene.help_overlay_visible = false;

        let frame = render_scene(
            &scene,
            super::Viewport {
                width: 20,
                height: 6,
            },
        );
        let plain = frame.plain_lines().join("\n");

        assert!(plain.contains("Ada"));
        assert!(!plain.contains("Alan"));
    }

    #[test]
    fn render_includes_help_overlay_text() {
        let session = seeded_session(&["CREATE (a:Person {name: 'Ada'})"]);
        let graph = SceneGraph::from_session(&session);
        let ada = node_id_by_name(&graph, "Ada");
        let seed = SeedSubgraph {
            node_ids: BTreeSet::from([ada]),
            edge_ids: BTreeSet::new(),
        };
        let scene = SceneState::new(graph, seed);

        let frame = render_scene(
            &scene,
            super::Viewport {
                width: 72,
                height: 18,
            },
        );
        let plain = frame.plain_lines().join("\n");

        assert!(plain.contains("Scene Guide"));
        assert!(plain.contains("Bottom bar shows"));
        assert!(plain.contains("Press any key"));
    }

    #[test]
    fn ansi_writer_uses_crlf_between_rows() {
        let mut frame = super::Frame::new(2, 2);
        frame.write_text(0, 0, "ab", None);
        frame.write_text(0, 1, "cd", None);

        assert_eq!(frame.to_ansi_string(false), "\x1b[Hab\r\ncd");
    }

    #[test]
    fn parallel_edges_get_stable_symmetric_lanes() {
        let session = seeded_session(&[
            "CREATE (a:Person {name: 'Ada'})-[:A]->(b:Person {name: 'Grace'})",
            "MATCH (a:Person {name: 'Ada'})-[:A]->(b:Person {name: 'Grace'}) CREATE (a)-[:B]->(b)",
            "MATCH (a:Person {name: 'Ada'})-[:A]->(b:Person {name: 'Grace'}) CREATE (b)-[:C]->(a)",
        ]);
        let graph = SceneGraph::from_session(&session);
        let ada = node_id_by_name(&graph, "Ada");
        let grace = node_id_by_name(&graph, "Grace");
        let seed = SeedSubgraph {
            node_ids: BTreeSet::from([ada, grace]),
            edge_ids: graph.edges.keys().copied().collect(),
        };
        let scene = SceneState::new(graph, seed);

        let lanes = edge_lane_offsets(&scene);
        let edge_id_by_type = scene
            .graph
            .edges
            .values()
            .map(|edge| (edge.edge_type.as_str(), edge.id))
            .collect::<BTreeMap<_, _>>();
        assert_eq!(lanes.len(), 3);
        assert!((lanes[edge_id_by_type.get("A").unwrap()] + 0.35).abs() < 1e-9);
        assert!(lanes[edge_id_by_type.get("B").unwrap()].abs() < 1e-9);
        assert!((lanes[edge_id_by_type.get("C").unwrap()] - 0.35).abs() < 1e-9);
    }

    #[test]
    fn parses_arrow_and_char_keys() {
        assert_eq!(parse_key(&[0x1b, b'[', b'A']), Some(KeyEvent::ArrowUp));
        assert_eq!(parse_key(b"q"), Some(KeyEvent::Char('q')));
    }
}
