use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use regex::Regex;

use crate::engine::{
    ConstraintRow, ConstraintType, CupldEngine, Edge, EdgeId, GraphError, GraphStats, IndexRow,
    Node, NodeId, PropertyMap, SchemaRow, SchemaTarget, Value,
};
use crate::query::{
    BinaryOp, ConstraintSpec, Direction, EdgePattern, Expr, OrderItem, ParamValue, Pattern,
    PatternSegment, PropertyTarget, Query, QueryError, RemoveTarget, ReturnItem, SetOperator,
    SetTarget, ShowKind, Statement, UnaryOp, parse_script,
};
use crate::storage;

const DEFAULT_ROW_LIMIT: usize = 1_000;
const INTERMEDIATE_ROW_LIMIT: usize = 100_000;

#[derive(Clone, Debug)]
struct MatchPlan {
    access: MatchAccessPath,
    start_candidates: Vec<NodeId>,
}

#[derive(Clone, Debug)]
struct PathTrace {
    nodes: Vec<NodeId>,
    edges: Vec<EdgeId>,
}

#[derive(Clone, Debug)]
enum MatchAccessPath {
    NodeScan {
        detail: String,
    },
    NodeIndexSeek {
        target: SchemaTarget,
        property: String,
        value: RuntimeValue,
    },
    NodeIndexRangeScan {
        target: SchemaTarget,
        property: String,
        lower: Option<RangeBound>,
        upper: Option<RangeBound>,
    },
}

#[derive(Clone, Debug)]
struct RangeBound {
    value: RuntimeValue,
    inclusive: bool,
}

#[derive(Clone, Debug)]
struct PropertyConstraint {
    property: String,
    kind: ConstraintKind,
    value: RuntimeValue,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ConstraintKind {
    Eq,
    Lt,
    Lte,
    Gt,
    Gte,
}

#[derive(Clone, Debug)]
struct PlannerKey(RuntimeValue);

impl PartialEq for PlannerKey {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for PlannerKey {}

impl PartialOrd for PlannerKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PlannerKey {
    fn cmp(&self, other: &Self) -> Ordering {
        let ordering = compare_runtime_values(&self.0, &other.0);
        if ordering == Ordering::Equal && !same_runtime_type(&self.0, &other.0) {
            return runtime_type_name(&self.0).cmp(runtime_type_name(&other.0));
        }
        ordering
    }
}

#[derive(Clone, Debug)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<RuntimeValue>>,
}

#[derive(Clone, Debug)]
pub struct TransactionInfo {
    pub active: bool,
    pub failed: bool,
    pub savepoints: usize,
    pub last_tx_id: u64,
}

#[derive(Clone, Debug)]
pub struct Session {
    engine: CupldEngine,
    transaction_base: Option<(CupldEngine, bool)>,
    savepoints: Vec<(String, CupldEngine, bool)>,
    failed_transaction: bool,
    path: Option<PathBuf>,
    db_uuid: Option<[u8; 16]>,
    dirty: bool,
}

impl Default for Session {
    fn default() -> Self {
        Self::new_in_memory()
    }
}

impl Session {
    pub fn new_in_memory() -> Self {
        Self {
            engine: CupldEngine::default(),
            transaction_base: None,
            savepoints: Vec::new(),
            failed_transaction: false,
            path: None,
            db_uuid: None,
            dirty: false,
        }
    }

    pub fn from_engine(engine: CupldEngine) -> Self {
        Self {
            engine,
            transaction_base: None,
            savepoints: Vec::new(),
            failed_transaction: false,
            path: None,
            db_uuid: None,
            dirty: false,
        }
    }

    pub fn open(path: impl AsRef<Path>) -> Result<Self, ExecutionError> {
        let path = path.as_ref().to_path_buf();
        let (engine, report) = storage::load(&path).map_err(ExecutionError::from)?;
        Ok(Self {
            engine,
            transaction_base: None,
            savepoints: Vec::new(),
            failed_transaction: false,
            path: Some(path),
            db_uuid: Some(report.db_uuid),
            dirty: false,
        })
    }

    pub fn engine(&self) -> &CupldEngine {
        &self.engine
    }

    pub fn replace_engine(&mut self, engine: CupldEngine) -> Result<(), ExecutionError> {
        if self.transaction_base.is_some() {
            return Err(ExecutionError::new(
                "transaction_active",
                "cannot replace the engine while a transaction is active",
            ));
        }
        self.engine = engine;
        self.savepoints.clear();
        self.failed_transaction = false;
        self.dirty = true;
        Ok(())
    }

    pub fn path(&self) -> Option<&Path> {
        self.path.as_deref()
    }

    pub fn is_dirty(&self) -> bool {
        self.dirty
    }

    pub fn save_as(&mut self, path: impl AsRef<Path>) -> Result<(), ExecutionError> {
        let path = path.as_ref().to_path_buf();
        let db_uuid = storage::save_compacted(&path, &self.engine).map_err(ExecutionError::from)?;
        self.path = Some(path);
        self.db_uuid = Some(db_uuid);
        self.dirty = false;
        Ok(())
    }

    pub fn save(&mut self) -> Result<(), ExecutionError> {
        let Some(path) = self.path.clone() else {
            return Err(ExecutionError::new(
                "save_requires_path",
                "unnamed in-memory databases require SAVE AS",
            ));
        };
        let db_uuid = self
            .db_uuid
            .ok_or_else(|| ExecutionError::new("db_uuid_missing", "database UUID is missing"))?;
        storage::compact(&path, &self.engine, db_uuid).map_err(ExecutionError::from)?;
        self.dirty = false;
        Ok(())
    }

    pub fn compact(&mut self) -> Result<(), ExecutionError> {
        self.save()
    }

    pub fn check(path: impl AsRef<Path>) -> Result<storage::IntegrityReport, ExecutionError> {
        storage::check(path.as_ref()).map_err(ExecutionError::from)
    }

    pub fn transaction_info(&self) -> TransactionInfo {
        TransactionInfo {
            active: self.transaction_base.is_some(),
            failed: self.failed_transaction,
            savepoints: self.savepoints.len(),
            last_tx_id: self.engine.snapshot().tx_id().get(),
        }
    }

    pub fn execute_script(
        &mut self,
        input: &str,
        params: &BTreeMap<String, Value>,
    ) -> Result<Vec<QueryResult>, ExecutionError> {
        let statements = parse_script(input).map_err(ExecutionError::from)?;
        if statements.len() > 1 && !statements.iter().any(Statement::is_transaction_control) {
            return Err(ExecutionError::new(
                "multi_statement_requires_transaction",
                "multi-statement batches require an explicit BEGIN/COMMIT",
            ));
        }

        let mut results = Vec::new();
        for statement in statements {
            let result = self.execute_statement(&statement, params)?;
            results.push(result);
        }
        Ok(results)
    }

    pub fn execute_statement(
        &mut self,
        statement: &Statement,
        params: &BTreeMap<String, Value>,
    ) -> Result<QueryResult, ExecutionError> {
        if self.failed_transaction && !statement.allowed_in_failed_transaction() {
            return Err(ExecutionError::new(
                "transaction_failed",
                "transaction is in failed state; rollback or release to recover",
            ));
        }

        match statement {
            Statement::Begin => self.begin(),
            Statement::Commit => self.commit(),
            Statement::Rollback => self.rollback(),
            Statement::Savepoint(name) => self.savepoint(name),
            Statement::RollbackToSavepoint(name) => self.rollback_to_savepoint(name),
            Statement::ReleaseSavepoint(name) => self.release_savepoint(name),
            Statement::Explain(inner) => self.explain(inner),
            Statement::Show(kind) => self.show(kind),
            _ => {
                let snapshot = self.engine.clone();
                let result = self.execute_data_statement(statement, params);
                match (self.transaction_base.is_some(), result) {
                    (_, Ok(result)) if statement.is_mutating() => {
                        if self.transaction_base.is_some() {
                            self.dirty = true;
                            Ok(result)
                        } else {
                            self.engine.commit().map_err(ExecutionError::from)?;
                            self.persist_after_commit()?;
                            Ok(result)
                        }
                    }
                    (_, Ok(result)) => Ok(result),
                    (true, Err(error)) => {
                        self.engine = snapshot;
                        self.failed_transaction = true;
                        Err(error)
                    }
                    (false, Err(error)) => {
                        self.engine = snapshot;
                        Err(error)
                    }
                }
            }
        }
    }

    fn begin(&mut self) -> Result<QueryResult, ExecutionError> {
        if self.transaction_base.is_some() {
            return Err(ExecutionError::new(
                "transaction_active",
                "a transaction is already active",
            ));
        }
        self.transaction_base = Some((self.engine.clone(), self.dirty));
        self.savepoints.clear();
        self.failed_transaction = false;
        Ok(empty_result())
    }

    fn commit(&mut self) -> Result<QueryResult, ExecutionError> {
        if self.failed_transaction {
            return Err(ExecutionError::new(
                "transaction_failed_commit",
                "cannot commit a failed transaction",
            ));
        }
        if self.transaction_base.is_none() {
            return Err(ExecutionError::new(
                "transaction_not_active",
                "no active transaction",
            ));
        }
        self.engine.commit().map_err(ExecutionError::from)?;
        self.persist_after_commit()?;
        self.transaction_base = None;
        self.savepoints.clear();
        self.failed_transaction = false;
        Ok(empty_result())
    }

    fn rollback(&mut self) -> Result<QueryResult, ExecutionError> {
        let Some((base, dirty)) = self.transaction_base.take() else {
            return Err(ExecutionError::new(
                "transaction_not_active",
                "no active transaction",
            ));
        };
        self.engine = base;
        self.dirty = dirty;
        self.savepoints.clear();
        self.failed_transaction = false;
        Ok(empty_result())
    }

    fn savepoint(&mut self, name: &str) -> Result<QueryResult, ExecutionError> {
        self.require_active_transaction("savepoints require an active transaction")?;
        if self
            .savepoints
            .iter()
            .any(|(existing, _, _)| existing == name)
        {
            return Err(ExecutionError::new(
                "savepoint_exists",
                "savepoint already exists",
            ));
        }
        self.savepoints
            .push((name.to_owned(), self.engine.clone(), self.dirty));
        Ok(empty_result())
    }

    fn rollback_to_savepoint(&mut self, name: &str) -> Result<QueryResult, ExecutionError> {
        self.require_active_transaction("savepoints require an active transaction")?;
        let index = self.find_savepoint_index(name)?;
        self.engine = self.savepoints[index].1.clone();
        self.dirty = self.savepoints[index].2;
        self.savepoints.truncate(index + 1);
        self.failed_transaction = false;
        Ok(empty_result())
    }

    fn release_savepoint(&mut self, name: &str) -> Result<QueryResult, ExecutionError> {
        self.require_active_transaction("savepoints require an active transaction")?;
        let index = self.find_savepoint_index(name)?;
        self.savepoints.truncate(index);
        Ok(empty_result())
    }

    fn show(&self, kind: &ShowKind) -> Result<QueryResult, ExecutionError> {
        match kind {
            ShowKind::Schema => Ok(show_schema_result(self.engine.show_schema())),
            ShowKind::Indexes(target) => Ok(show_indexes_result(
                self.engine.show_indexes(target.as_ref()),
            )),
            ShowKind::Constraints(target) => Ok(show_constraints_result(
                self.engine.show_constraints(target.as_ref()),
            )),
            ShowKind::Stats => Ok(show_stats_result(self.engine.stats())),
            ShowKind::Transactions => Ok(show_transactions_result(self.transaction_info())),
        }
    }

    fn explain(&self, statement: &Statement) -> Result<QueryResult, ExecutionError> {
        let mut rows = Vec::new();
        let mut next_id = 1i64;
        let match_plan = match statement {
            Statement::Query(query) => self.plan_match(query, &BTreeMap::new())?,
            _ => None,
        };
        build_explain_rows(
            statement,
            None,
            &mut next_id,
            &mut rows,
            match_plan.as_ref(),
        );
        Ok(QueryResult {
            columns: vec![
                "id".to_owned(),
                "parent_id".to_owned(),
                "operator".to_owned(),
                "detail".to_owned(),
            ],
            rows,
        })
    }

    fn execute_data_statement(
        &mut self,
        statement: &Statement,
        params: &BTreeMap<String, Value>,
    ) -> Result<QueryResult, ExecutionError> {
        match statement {
            Statement::CreateLabel {
                name,
                description,
                if_not_exists,
                or_replace,
            } => {
                self.engine
                    .create_label(
                        &resolve_param_value(name, params, "schema name")?,
                        resolve_optional_param_value(description.as_ref(), params, "description")?,
                        *if_not_exists,
                        *or_replace,
                    )
                    .map_err(ExecutionError::from)?;
                Ok(empty_result())
            }
            Statement::DropLabel { name, if_exists } => {
                self.engine
                    .drop_label(&resolve_param_value(name, params, "schema name")?, *if_exists)
                    .map_err(ExecutionError::from)?;
                Ok(empty_result())
            }
            Statement::CreateEdgeType {
                name,
                description,
                if_not_exists,
                or_replace,
            } => {
                self.engine
                    .create_edge_type(
                        &resolve_param_value(name, params, "schema name")?,
                        resolve_optional_param_value(description.as_ref(), params, "description")?,
                        *if_not_exists,
                        *or_replace,
                    )
                    .map_err(ExecutionError::from)?;
                Ok(empty_result())
            }
            Statement::DropEdgeType { name, if_exists } => {
                self.engine
                    .drop_edge_type(&resolve_param_value(name, params, "schema name")?, *if_exists)
                    .map_err(ExecutionError::from)?;
                Ok(empty_result())
            }
            Statement::CreateIndex {
                name,
                target,
                property,
                if_not_exists,
                or_replace,
            } => {
                let resolved_name = resolve_optional_param_value(name.as_ref(), params, "index name")?;
                let resolved_target = resolve_schema_target(target, params)?;
                let resolved_property = resolve_param_value(property, params, "property name")?;
                self.engine
                    .create_index(
                        resolved_name.as_deref(),
                        resolved_target,
                        &resolved_property,
                        *if_not_exists,
                        *or_replace,
                    )
                    .map_err(ExecutionError::from)?;
                Ok(empty_result())
            }
            Statement::DropIndex { name, if_exists } => {
                self.engine
                    .drop_index(&resolve_param_value(name, params, "index name")?, *if_exists)
                    .map_err(ExecutionError::from)?;
                Ok(empty_result())
            }
            Statement::AlterIndex { name, status } => {
                self.engine
                    .alter_index_status(&resolve_param_value(name, params, "index name")?, *status)
                    .map_err(ExecutionError::from)?;
                Ok(empty_result())
            }
            Statement::CreateConstraint {
                name,
                target,
                constraint,
                if_not_exists,
                or_replace,
            } => {
                let resolved_name =
                    resolve_optional_param_value(name.as_ref(), params, "constraint name")?;
                let resolved_target = resolve_schema_target(target, params)?;
                let (property, resolved_constraint) = resolve_constraint_spec(constraint, params)?;
                self.engine
                    .create_constraint(
                        resolved_name.as_deref(),
                        resolved_target,
                        &property,
                        resolved_constraint,
                        *if_not_exists,
                        *or_replace,
                    )
                    .map_err(ExecutionError::from)?;
                Ok(empty_result())
            }
            Statement::DropConstraint { name, if_exists } => {
                self.engine
                    .drop_constraint(
                        &resolve_param_value(name, params, "constraint name")?,
                        *if_exists,
                    )
                    .map_err(ExecutionError::from)?;
                Ok(empty_result())
            }
            Statement::AlterConstraint { name, rename_to } => {
                self.engine
                    .rename_constraint(
                        &resolve_param_value(name, params, "constraint name")?,
                        &resolve_param_value(rename_to, params, "constraint name")?,
                    )
                    .map_err(ExecutionError::from)?;
                Ok(empty_result())
            }
            Statement::Query(query) => self.execute_query(query, params),
            Statement::Show(_) | Statement::Explain(_) => unreachable!(),
            Statement::Begin
            | Statement::Commit
            | Statement::Rollback
            | Statement::Savepoint(_)
            | Statement::RollbackToSavepoint(_)
            | Statement::ReleaseSavepoint(_) => unreachable!(),
        }
    }

    fn execute_query(
        &mut self,
        query: &Query,
        params: &BTreeMap<String, Value>,
    ) -> Result<QueryResult, ExecutionError> {
        let match_plan = self.plan_match(query, params)?;
        let mut rows = if let Some(pattern) = &query.match_clause {
            self.match_pattern_rows(vec![Row::default()], pattern, params, match_plan.as_ref())?
        } else {
            vec![Row::default()]
        };

        if let Some(predicate) = &query.where_clause {
            rows = self.filter_rows(rows, predicate, params)?;
        }

        for with_clause in &query.with_clauses {
            rows = self.apply_with_clause_rows(rows, with_clause, params)?;
        }

        if let Some(pattern) = &query.merge_clause {
            rows = self.merge_pattern_rows(rows, pattern, params)?;
        }

        if let Some(pattern) = &query.create_clause {
            rows = self.create_pattern_rows(rows, pattern, params)?;
        }
        if !query.set_clause.is_empty() {
            self.apply_set_clause(&rows, &query.set_clause, params)?;
        }
        if !query.remove_clause.is_empty() {
            self.apply_remove_clause(&rows, &query.remove_clause)?;
        }
        if !query.delete_clause.is_empty() {
            self.apply_delete_clause(&rows, &query.delete_clause)?;
        }

        rows = self.apply_order_and_limit(rows, &query.order_by, query.limit, params);

        if query.return_all {
            return self.return_all_rows(rows);
        }

        if query.return_clause.is_empty() {
            return Ok(empty_result());
        }

        self.project_result_rows(rows, &query.return_clause, params)
    }

    fn plan_match(
        &self,
        query: &Query,
        params: &BTreeMap<String, Value>,
    ) -> Result<Option<MatchPlan>, ExecutionError> {
        let Some(pattern) = &query.match_clause else {
            return Ok(None);
        };

        let schema = self.engine.schema_catalog();
        let mut equality_constraints = pattern
            .start
            .properties
            .iter()
            .filter_map(|(property, expr)| {
                self.eval_expr(expr, &Row::default(), params)
                    .ok()
                    .map(|value| PropertyConstraint {
                        property: property.clone(),
                        kind: ConstraintKind::Eq,
                        value,
                    })
            })
            .collect::<Vec<_>>();

        if let Some(variable) = pattern.start.variable.as_deref()
            && let Some(predicate) = &query.where_clause
        {
            let mut derived = Vec::new();
            self.collect_property_constraints(predicate, variable, params, &mut derived);
            equality_constraints.extend(derived);
        }

        for label in &pattern.start.labels {
            let target = SchemaTarget::label(label.clone());
            for constraint in &equality_constraints {
                if constraint.kind == ConstraintKind::Eq
                    && schema.find_index(&target, &constraint.property).is_some()
                {
                    let start_candidates = self.index_seek_candidates(
                        &target,
                        &constraint.property,
                        &constraint.value,
                    );
                    return Ok(Some(MatchPlan {
                        access: MatchAccessPath::NodeIndexSeek {
                            target,
                            property: constraint.property.clone(),
                            value: constraint.value.clone(),
                        },
                        start_candidates,
                    }));
                }
            }

            let range_constraints = fold_range_constraints(&equality_constraints);
            for (property, bounds) in range_constraints {
                if schema.find_index(&target, &property).is_some()
                    && (bounds.lower.is_some() || bounds.upper.is_some())
                {
                    let start_candidates = self.index_range_candidates(
                        &target,
                        &property,
                        bounds.lower.as_ref(),
                        bounds.upper.as_ref(),
                    );
                    return Ok(Some(MatchPlan {
                        access: MatchAccessPath::NodeIndexRangeScan {
                            target,
                            property,
                            lower: bounds.lower,
                            upper: bounds.upper,
                        },
                        start_candidates,
                    }));
                }
            }
        }

        let start_candidates = if let Some(label) = pattern.start.labels.first() {
            self.engine
                .nodes()
                .filter(|node| node.labels().contains(label))
                .map(Node::id)
                .collect::<Vec<_>>()
        } else {
            self.engine.nodes().map(Node::id).collect::<Vec<_>>()
        };
        let detail = if let Some(label) = pattern.start.labels.first() {
            format!(
                "label {}",
                SchemaTarget::label(label.clone()).display_target()
            )
        } else {
            "all nodes".to_owned()
        };
        Ok(Some(MatchPlan {
            access: MatchAccessPath::NodeScan { detail },
            start_candidates,
        }))
    }

    fn collect_property_constraints(
        &self,
        expr: &Expr,
        variable: &str,
        params: &BTreeMap<String, Value>,
        output: &mut Vec<PropertyConstraint>,
    ) {
        match expr {
            Expr::Binary {
                left,
                op: BinaryOp::And,
                right,
            } => {
                self.collect_property_constraints(left, variable, params, output);
                self.collect_property_constraints(right, variable, params, output);
            }
            Expr::Binary { left, op, right } => {
                if let Some(constraint) =
                    self.property_constraint_from_binary(left, *op, right, variable, params)
                {
                    output.push(constraint);
                } else if let Some(constraint) = self.property_constraint_from_binary(
                    right,
                    reverse_constraint_op(*op),
                    left,
                    variable,
                    params,
                ) {
                    output.push(constraint);
                }
            }
            _ => {}
        }
    }

    fn property_constraint_from_binary(
        &self,
        property_side: &Expr,
        op: BinaryOp,
        value_side: &Expr,
        variable: &str,
        params: &BTreeMap<String, Value>,
    ) -> Option<PropertyConstraint> {
        let property = match property_side {
            Expr::Property(base, property) => match &**base {
                Expr::Variable(name) if name == variable => property.clone(),
                _ => return None,
            },
            _ => return None,
        };
        let kind = constraint_kind_from_binary(op)?;
        let value = self.eval_expr(value_side, &Row::default(), params).ok()?;
        Some(PropertyConstraint {
            property,
            kind,
            value,
        })
    }

    fn index_seek_candidates(
        &self,
        target: &SchemaTarget,
        property: &str,
        value: &RuntimeValue,
    ) -> Vec<NodeId> {
        let entries = self.build_node_index_entries(target, property);
        entries
            .get(&PlannerKey(value.clone()))
            .map(|ids| ids.iter().copied().collect())
            .unwrap_or_default()
    }

    fn index_range_candidates(
        &self,
        target: &SchemaTarget,
        property: &str,
        lower: Option<&RangeBound>,
        upper: Option<&RangeBound>,
    ) -> Vec<NodeId> {
        self.build_node_index_entries(target, property)
            .into_iter()
            .filter(|(value, _)| range_matches(&value.0, lower, upper))
            .flat_map(|(_, ids)| ids.into_iter())
            .collect()
    }

    fn build_node_index_entries(
        &self,
        target: &SchemaTarget,
        property: &str,
    ) -> BTreeMap<PlannerKey, BTreeSet<NodeId>> {
        if target.kind() != crate::engine::TargetKind::Label {
            return BTreeMap::new();
        }
        let mut entries = BTreeMap::<PlannerKey, BTreeSet<NodeId>>::new();
        for node in self.engine.nodes() {
            if !node.labels().contains(target.name()) {
                continue;
            }
            let Some(value) = node.property(property) else {
                continue;
            };
            if matches!(value, Value::Null) {
                continue;
            }
            entries
                .entry(PlannerKey(runtime_from_value(value)))
                .or_default()
                .insert(node.id());
        }
        entries
    }

    fn filter_rows(
        &self,
        rows: Vec<Row>,
        predicate: &Expr,
        params: &BTreeMap<String, Value>,
    ) -> Result<Vec<Row>, ExecutionError> {
        rows.into_iter()
            .filter_map(|row| match self.eval_expr(predicate, &row, params) {
                Ok(RuntimeValue::Bool(true)) => Some(Ok(row)),
                Ok(RuntimeValue::Bool(false) | RuntimeValue::Null) => None,
                Ok(_) => Some(Err(ExecutionError::new(
                    "where_type_error",
                    "WHERE expressions must evaluate to bool or null",
                ))),
                Err(error) => Some(Err(error)),
            })
            .collect::<Result<Vec<_>, _>>()
    }

    fn apply_with_clause_rows(
        &self,
        mut rows: Vec<Row>,
        with_clause: &crate::query::WithClause,
        params: &BTreeMap<String, Value>,
    ) -> Result<Vec<Row>, ExecutionError> {
        if with_clause.all {
            if let Some(predicate) = &with_clause.where_clause {
                rows = self.filter_rows(rows, predicate, params)?;
            }
            return Ok(self.apply_order_and_limit(
                rows,
                &with_clause.order_by,
                with_clause.limit,
                params,
            ));
        }

        let projection =
            self.project_rows(rows, &with_clause.items, params, projection_name_for_with)?;
        let mut projected_rows = projection.1;
        if let Some(predicate) = &with_clause.where_clause {
            projected_rows = self.filter_rows(projected_rows, predicate, params)?;
        }
        Ok(self.apply_order_and_limit(
            projected_rows,
            &with_clause.order_by,
            with_clause.limit,
            params,
        ))
    }

    fn apply_order_and_limit(
        &self,
        mut rows: Vec<Row>,
        order_by: &[OrderItem],
        limit: Option<usize>,
        params: &BTreeMap<String, Value>,
    ) -> Vec<Row> {
        if !order_by.is_empty() {
            rows.sort_by(|left, right| self.compare_rows(left, right, order_by, params));
        }
        let limit = limit.unwrap_or(DEFAULT_ROW_LIMIT).min(DEFAULT_ROW_LIMIT);
        if rows.len() > limit {
            rows.truncate(limit);
        }
        rows
    }

    fn return_all_rows(&self, rows: Vec<Row>) -> Result<QueryResult, ExecutionError> {
        let mut columns = rows
            .iter()
            .flat_map(|row| row.keys().cloned())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        if columns.is_empty() {
            columns = Vec::new();
        }
        let output_rows = rows
            .into_iter()
            .map(|row| {
                columns
                    .iter()
                    .map(|column| row.get(column).cloned().unwrap_or(RuntimeValue::Null))
                    .collect::<Vec<_>>()
            })
            .collect();
        Ok(QueryResult {
            columns,
            rows: output_rows,
        })
    }

    fn project_result_rows(
        &self,
        rows: Vec<Row>,
        items: &[ReturnItem],
        params: &BTreeMap<String, Value>,
    ) -> Result<QueryResult, ExecutionError> {
        let (columns, projected_rows) =
            self.project_rows(rows, items, params, projection_name_for_return)?;
        let output_columns = columns.clone();
        Ok(QueryResult {
            columns,
            rows: projected_rows
                .into_iter()
                .map(|row| {
                    output_columns
                        .iter()
                        .map(|column| row.get(column).cloned().unwrap_or(RuntimeValue::Null))
                        .collect()
                })
                .collect(),
        })
    }

    fn project_rows(
        &self,
        rows: Vec<Row>,
        items: &[ReturnItem],
        params: &BTreeMap<String, Value>,
        name_fn: fn(usize, &ReturnItem) -> String,
    ) -> Result<(Vec<String>, Vec<Row>), ExecutionError> {
        let columns = items
            .iter()
            .enumerate()
            .map(|(index, item)| name_fn(index, item))
            .collect::<Vec<_>>();
        let aggregate_flags = items
            .iter()
            .map(|item| expr_contains_aggregate(&item.expr))
            .collect::<Vec<_>>();
        if aggregate_flags.iter().any(|flag| *flag) {
            if items
                .iter()
                .any(|item| aggregate_expression_is_invalid(&item.expr))
            {
                return Err(ExecutionError::new(
                    "function_error",
                    "aggregate expressions must be top-level aggregate function calls",
                ));
            }
            return self.project_aggregate_rows(rows, items, &columns, &aggregate_flags, params);
        }
        let output_rows = rows
            .into_iter()
            .map(|row| {
                let mut projected = Row::default();
                for (index, item) in items.iter().enumerate() {
                    projected.insert(
                        columns[index].clone(),
                        self.eval_expr(&item.expr, &row, params)?,
                    );
                }
                Ok(projected)
            })
            .collect::<Result<Vec<_>, ExecutionError>>()?;
        Ok((columns, output_rows))
    }

    fn project_aggregate_rows(
        &self,
        rows: Vec<Row>,
        items: &[ReturnItem],
        columns: &[String],
        aggregate_flags: &[bool],
        params: &BTreeMap<String, Value>,
    ) -> Result<(Vec<String>, Vec<Row>), ExecutionError> {
        let grouping_items = items
            .iter()
            .zip(aggregate_flags.iter())
            .enumerate()
            .filter_map(|(index, (item, is_aggregate))| (!is_aggregate).then_some((index, item)))
            .collect::<Vec<_>>();

        let mut groups = BTreeMap::<Vec<PlannerKey>, Vec<Row>>::new();
        for row in rows {
            let mut key = Vec::new();
            for (_, item) in &grouping_items {
                key.push(PlannerKey(self.eval_expr(&item.expr, &row, params)?));
            }
            groups.entry(key).or_default().push(row);
        }
        if groups.is_empty() && grouping_items.is_empty() {
            groups.insert(Vec::new(), Vec::new());
        }

        let mut output_rows = Vec::new();
        for (key, group_rows) in groups {
            let mut projected = Row::default();
            let mut key_iter = key.into_iter();
            for (index, item) in items.iter().enumerate() {
                let value = if aggregate_flags[index] {
                    self.eval_aggregate_expr(&item.expr, &group_rows, params)?
                } else {
                    key_iter
                        .next()
                        .map(|value| value.0)
                        .unwrap_or(RuntimeValue::Null)
                };
                projected.insert(columns[index].clone(), value);
            }
            output_rows.push(projected);
        }
        Ok((columns.to_vec(), output_rows))
    }

    fn eval_aggregate_expr(
        &self,
        expr: &Expr,
        rows: &[Row],
        params: &BTreeMap<String, Value>,
    ) -> Result<RuntimeValue, ExecutionError> {
        let Expr::FunctionCall { name, args } = expr else {
            return Err(ExecutionError::new(
                "function_error",
                "aggregate expressions must be aggregate function calls",
            ));
        };
        if args.is_empty() && name != "count" {
            return Err(ExecutionError::new(
                "function_error",
                format!("{name}() requires an argument"),
            ));
        }
        match name.as_str() {
            "count" => {
                if args.is_empty() {
                    return Err(ExecutionError::new(
                        "function_error",
                        "count() requires an argument",
                    ));
                }
                if matches!(args.as_slice(), [Expr::Wildcard]) {
                    return Ok(RuntimeValue::Int(rows.len() as i64));
                }
                let mut count = 0i64;
                for row in rows {
                    let value = self.eval_expr(&args[0], row, params)?;
                    if value != RuntimeValue::Null {
                        count += 1;
                    }
                }
                Ok(RuntimeValue::Int(count))
            }
            "collect" => rows
                .iter()
                .map(|row| self.eval_expr(&args[0], row, params))
                .collect::<Result<Vec<_>, _>>()
                .map(RuntimeValue::List),
            "sum" => sum_aggregate(rows, |row| self.eval_expr(&args[0], row, params)),
            "avg" => avg_aggregate(rows, |row| self.eval_expr(&args[0], row, params)),
            "min" => min_max_aggregate(rows, |row| self.eval_expr(&args[0], row, params), true),
            "max" => min_max_aggregate(rows, |row| self.eval_expr(&args[0], row, params), false),
            _ => Err(ExecutionError::new(
                "function_error",
                format!("unsupported aggregate function {name}"),
            )),
        }
    }

    fn match_pattern_rows(
        &self,
        seed_rows: Vec<Row>,
        pattern: &Pattern,
        params: &BTreeMap<String, Value>,
        plan: Option<&MatchPlan>,
    ) -> Result<Vec<Row>, ExecutionError> {
        let mut rows = Vec::new();
        let planned_nodes = plan
            .map(|plan| plan.start_candidates.clone())
            .unwrap_or_else(|| self.engine.nodes().map(Node::id).collect::<Vec<_>>());
        for seed_row in seed_rows {
            if let Some(variable) = &pattern.start.variable
                && let Some(bound_value) = seed_row.get(variable)
            {
                if let RuntimeValue::Node(node_id) = bound_value
                    && let Some(node) = self.engine.node(*node_id)
                    && let Some(row) =
                        self.bind_node_pattern(&seed_row, node, &pattern.start, params)?
                {
                    let trace = PathTrace {
                        nodes: vec![node.id()],
                        edges: Vec::new(),
                    };
                    self.match_segments(
                        &row,
                        node.id(),
                        &pattern.segments,
                        params,
                        &mut rows,
                        pattern.path_variable.as_deref(),
                        trace,
                    )?;
                }
                continue;
            }

            for node_id in &planned_nodes {
                let Some(node) = self.engine.node(*node_id) else {
                    continue;
                };
                if let Some(row) =
                    self.bind_node_pattern(&seed_row, node, &pattern.start, params)?
                {
                    let trace = PathTrace {
                        nodes: vec![node.id()],
                        edges: Vec::new(),
                    };
                    self.match_segments(
                        &row,
                        node.id(),
                        &pattern.segments,
                        params,
                        &mut rows,
                        pattern.path_variable.as_deref(),
                        trace,
                    )?;
                }
            }
        }
        if rows.len() > INTERMEDIATE_ROW_LIMIT {
            return Err(ExecutionError::new(
                "row_limit_exceeded",
                "intermediate result row cap exceeded",
            ));
        }
        Ok(rows)
    }

    fn match_segments(
        &self,
        row: &Row,
        current_node: NodeId,
        segments: &[PatternSegment],
        params: &BTreeMap<String, Value>,
        output: &mut Vec<Row>,
        path_variable: Option<&str>,
        trace: PathTrace,
    ) -> Result<(), ExecutionError> {
        let Some((segment, remaining_segments)) = segments.split_first() else {
            let mut final_row = row.clone();
            if let Some(path_variable) = path_variable {
                final_row.insert(path_variable.to_owned(), path_runtime_value(&trace));
            }
            output.push(final_row);
            return Ok(());
        };
        if segment.edge.hops.is_some() {
            self.match_variable_hops(
                row,
                current_node,
                segment,
                remaining_segments,
                params,
                output,
                path_variable,
                trace,
            )?;
            return Ok(());
        }

        for edge_id in self.edge_ids_for_direction(current_node, segment.direction) {
            let edge = self.engine.edge(edge_id).expect("edge exists");
            let next_node = match segment.direction {
                Direction::Outgoing => edge.to(),
                Direction::Incoming => edge.from(),
                Direction::Undirected => {
                    if edge.from() == current_node {
                        edge.to()
                    } else {
                        edge.from()
                    }
                }
            };
            let Some(row) = self.bind_edge_pattern(row, edge, &segment.edge, params)? else {
                continue;
            };
            let node = self.engine.node(next_node).expect("node exists");
            let Some(row) = self.bind_node_pattern(&row, node, &segment.node, params)? else {
                continue;
            };
            let mut next_trace = trace.clone();
            next_trace.edges.push(edge.id());
            next_trace.nodes.push(next_node);
            self.match_segments(
                &row,
                next_node,
                remaining_segments,
                params,
                output,
                path_variable,
                next_trace,
            )?;
        }
        Ok(())
    }

    fn match_variable_hops(
        &self,
        row: &Row,
        current_node: NodeId,
        segment: &PatternSegment,
        remaining_segments: &[PatternSegment],
        params: &BTreeMap<String, Value>,
        output: &mut Vec<Row>,
        path_variable: Option<&str>,
        trace: PathTrace,
    ) -> Result<(), ExecutionError> {
        if segment.edge.variable.is_some() {
            return Err(ExecutionError::new(
                "variable_hop_edge_binding",
                "variable-length traversals cannot bind edge variables in v1",
            ));
        }
        let hops = segment.edge.hops.expect("checked");
        let mut queue = VecDeque::from([(current_node, 0u8, trace)]);
        let mut seen = BTreeSet::from([(current_node, 0u8)]);

        while let Some((node_id, depth, trace)) = queue.pop_front() {
            if depth >= hops.max {
                continue;
            }
            for edge_id in self.edge_ids_for_direction(node_id, segment.direction) {
                let edge = self.engine.edge(edge_id).expect("edge exists");
                let next_node = match segment.direction {
                    Direction::Outgoing => edge.to(),
                    Direction::Incoming => edge.from(),
                    Direction::Undirected => {
                        if edge.from() == node_id {
                            edge.to()
                        } else {
                            edge.from()
                        }
                    }
                };
                if !self.edge_matches_filters(edge, &segment.edge, row, params)? {
                    continue;
                }
                let next_depth = depth + 1;
                if seen.insert((next_node, next_depth)) {
                    let mut next_trace = trace.clone();
                    next_trace.edges.push(edge.id());
                    next_trace.nodes.push(next_node);
                    queue.push_back((next_node, next_depth, next_trace));
                }
                if next_depth < hops.min {
                    continue;
                }
                let node = self.engine.node(next_node).expect("node exists");
                let Some(bound_row) = self.bind_node_pattern(row, node, &segment.node, params)?
                else {
                    continue;
                };
                let mut next_trace = trace.clone();
                next_trace.edges.push(edge.id());
                next_trace.nodes.push(next_node);
                self.match_segments(
                    &bound_row,
                    next_node,
                    remaining_segments,
                    params,
                    output,
                    path_variable,
                    next_trace,
                )?;
            }
        }
        Ok(())
    }

    fn merge_pattern_rows(
        &mut self,
        rows: Vec<Row>,
        pattern: &Pattern,
        params: &BTreeMap<String, Value>,
    ) -> Result<Vec<Row>, ExecutionError> {
        let mut merged_rows = Vec::new();
        for row in rows {
            let matches = self.match_pattern_rows(vec![row.clone()], pattern, params, None)?;
            if matches.is_empty() {
                merged_rows.extend(self.create_pattern_rows(vec![row], pattern, params)?);
            } else {
                merged_rows.extend(matches);
            }
        }
        Ok(merged_rows)
    }

    fn create_pattern_rows(
        &mut self,
        rows: Vec<Row>,
        pattern: &Pattern,
        params: &BTreeMap<String, Value>,
    ) -> Result<Vec<Row>, ExecutionError> {
        let mut created_rows = Vec::with_capacity(rows.len());
        for row in rows {
            let (row, current_node) = self.realize_node_pattern(row, &pattern.start, params)?;
            let mut row = row;
            let mut current_node = current_node;
            let mut trace = PathTrace {
                nodes: vec![current_node],
                edges: Vec::new(),
            };
            for segment in &pattern.segments {
                if segment.edge.hops.is_some() {
                    return Err(ExecutionError::new(
                        "create_variable_hops",
                        "CREATE does not support variable-length edges",
                    ));
                }
                let (next_row, next_node) =
                    self.realize_node_pattern(row.clone(), &segment.node, params)?;
                let edge_type = match segment.edge.edge_types.as_slice() {
                    [edge_type] => edge_type.clone(),
                    [] => {
                        return Err(ExecutionError::new(
                            "create_edge_type",
                            "CREATE edges require a type",
                        ));
                    }
                    _ => {
                        return Err(ExecutionError::new(
                            "create_edge_type",
                            "CREATE edges require exactly one type",
                        ));
                    }
                };
                let properties =
                    eval_property_map(self, &next_row, &segment.edge.properties, params)?;
                let edge_id = match segment.direction {
                    Direction::Incoming => self
                        .engine
                        .create_edge(next_node, current_node, edge_type, properties)
                        .map_err(ExecutionError::from)?,
                    _ => self
                        .engine
                        .create_edge(current_node, next_node, edge_type, properties)
                        .map_err(ExecutionError::from)?,
                };
                row = next_row;
                if let Some(variable) = &segment.edge.variable {
                    row.insert(variable.clone(), RuntimeValue::Edge(edge_id));
                }
                trace.edges.push(edge_id);
                trace.nodes.push(next_node);
                current_node = next_node;
            }
            if let Some(path_variable) = &pattern.path_variable {
                row.insert(path_variable.clone(), path_runtime_value(&trace));
            }
            created_rows.push(row);
        }
        Ok(created_rows)
    }

    fn realize_node_pattern(
        &mut self,
        mut row: Row,
        pattern: &crate::query::NodePattern,
        params: &BTreeMap<String, Value>,
    ) -> Result<(Row, NodeId), ExecutionError> {
        if let Some(variable) = &pattern.variable
            && let Some(node_id) = match row.get(variable) {
                Some(RuntimeValue::Node(node_id)) => Some(*node_id),
                _ => None,
            }
        {
            return Ok((row, node_id));
        }
        let properties = eval_property_map(self, &row, &pattern.properties, params)?;
        let node_id = self
            .engine
            .create_node(pattern.labels.iter().cloned(), properties)
            .map_err(ExecutionError::from)?;
        if let Some(variable) = &pattern.variable {
            row.insert(variable.clone(), RuntimeValue::Node(node_id));
        }
        Ok((row, node_id))
    }

    fn apply_set_clause(
        &mut self,
        rows: &[Row],
        assignments: &[crate::query::SetAssignment],
        params: &BTreeMap<String, Value>,
    ) -> Result<(), ExecutionError> {
        for row in rows {
            for assignment in assignments {
                let value = self.eval_expr(&assignment.value, row, params)?;
                match (&assignment.target, assignment.op) {
                    (SetTarget::Property(target), SetOperator::Assign) => {
                        let graph_value = value.to_graph_value()?;
                        self.apply_property_set(row, target, graph_value)?;
                    }
                    (SetTarget::PropertyIndex { target, index }, SetOperator::Assign) => {
                        let patch_index = self.eval_expr(index, row, params)?;
                        self.apply_indexed_property_set(row, target, &patch_index, value)?;
                    }
                    (SetTarget::Entity(variable), SetOperator::Merge) => {
                        self.apply_entity_merge(row, variable, value)?;
                    }
                    _ => {
                        return Err(ExecutionError::new(
                            "set_target",
                            "unsupported SET target or operator",
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    fn apply_remove_clause(
        &mut self,
        rows: &[Row],
        targets: &[RemoveTarget],
    ) -> Result<(), ExecutionError> {
        for row in rows {
            for target in targets {
                match target {
                    RemoveTarget::Property(target) => match row.get(&target.variable) {
                        Some(RuntimeValue::Node(node_id)) => {
                            self.engine
                                .remove_node_property(*node_id, &target.property)
                                .map_err(ExecutionError::from)?;
                        }
                        Some(RuntimeValue::Edge(edge_id)) => {
                            self.engine
                                .remove_edge_property(*edge_id, &target.property)
                                .map_err(ExecutionError::from)?;
                        }
                        _ => {
                            return Err(ExecutionError::new(
                                "remove_target",
                                "REMOVE targets must resolve to a node or edge variable",
                            ));
                        }
                    },
                    RemoveTarget::Label { variable, label } => match row.get(variable) {
                        Some(RuntimeValue::Node(node_id)) => {
                            self.engine
                                .remove_node_label(*node_id, label)
                                .map_err(ExecutionError::from)?;
                        }
                        _ => {
                            return Err(ExecutionError::new(
                                "remove_target",
                                "REMOVE label targets must resolve to a node variable",
                            ));
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn apply_property_set(
        &mut self,
        row: &Row,
        target: &PropertyTarget,
        graph_value: Value,
    ) -> Result<(), ExecutionError> {
        match row.get(&target.variable) {
            Some(RuntimeValue::Node(node_id)) => {
                self.engine
                    .set_node_property(*node_id, target.property.clone(), graph_value)
                    .map_err(ExecutionError::from)?;
            }
            Some(RuntimeValue::Edge(edge_id)) => {
                self.engine
                    .set_edge_property(*edge_id, target.property.clone(), graph_value)
                    .map_err(ExecutionError::from)?;
            }
            _ => {
                return Err(ExecutionError::new(
                    "set_target",
                    "SET targets must resolve to a node or edge variable",
                ));
            }
        }
        Ok(())
    }

    fn apply_indexed_property_set(
        &mut self,
        row: &Row,
        target: &PropertyTarget,
        patch_index: &RuntimeValue,
        value: RuntimeValue,
    ) -> Result<(), ExecutionError> {
        let index = match patch_index {
            RuntimeValue::Int(value) if *value >= 0 => *value as usize,
            _ => {
                return Err(ExecutionError::new(
                    "index_type_error",
                    "list patch indexes must be non-negative integers",
                ));
            }
        };
        match row.get(&target.variable) {
            Some(RuntimeValue::Node(node_id)) => {
                let mut list = match self
                    .engine
                    .node(*node_id)
                    .and_then(|node| node.property(&target.property))
                {
                    Some(Value::List(values)) => values.clone(),
                    Some(Value::Null) | None => Vec::new(),
                    Some(_) => {
                        return Err(ExecutionError::new(
                            "set_target",
                            "indexed SET targets require a list property",
                        ));
                    }
                };
                if index >= list.len() {
                    return Err(ExecutionError::new(
                        "index_type_error",
                        "list patch index is out of bounds",
                    ));
                }
                list[index] = value.to_graph_value()?;
                self.engine
                    .set_node_property(*node_id, target.property.clone(), Value::List(list))
                    .map_err(ExecutionError::from)?;
            }
            Some(RuntimeValue::Edge(edge_id)) => {
                let mut list = match self
                    .engine
                    .edge(*edge_id)
                    .and_then(|edge| edge.property(&target.property))
                {
                    Some(Value::List(values)) => values.clone(),
                    Some(Value::Null) | None => Vec::new(),
                    Some(_) => {
                        return Err(ExecutionError::new(
                            "set_target",
                            "indexed SET targets require a list property",
                        ));
                    }
                };
                if index >= list.len() {
                    return Err(ExecutionError::new(
                        "index_type_error",
                        "list patch index is out of bounds",
                    ));
                }
                list[index] = value.to_graph_value()?;
                self.engine
                    .set_edge_property(*edge_id, target.property.clone(), Value::List(list))
                    .map_err(ExecutionError::from)?;
            }
            _ => {
                return Err(ExecutionError::new(
                    "set_target",
                    "SET targets must resolve to a node or edge variable",
                ));
            }
        }
        Ok(())
    }

    fn apply_entity_merge(
        &mut self,
        row: &Row,
        variable: &str,
        value: RuntimeValue,
    ) -> Result<(), ExecutionError> {
        let RuntimeValue::Map(entries) = value else {
            return Err(ExecutionError::new(
                "graph_value_conversion",
                "SET += requires a map payload",
            ));
        };
        let patch = runtime_entries_to_property_map(&entries)?;
        match row.get(variable) {
            Some(RuntimeValue::Node(node_id)) => {
                let mut properties = self
                    .engine
                    .node(*node_id)
                    .map(|node| node.properties().clone())
                    .ok_or_else(|| {
                        ExecutionError::new("set_target", "SET targets must resolve to a node")
                    })?;
                for (key, value) in patch {
                    properties.insert(key, value);
                }
                self.engine
                    .replace_node_properties(*node_id, properties)
                    .map_err(ExecutionError::from)?;
            }
            Some(RuntimeValue::Edge(edge_id)) => {
                let mut properties = self
                    .engine
                    .edge(*edge_id)
                    .map(|edge| edge.properties().clone())
                    .ok_or_else(|| {
                        ExecutionError::new("set_target", "SET targets must resolve to an edge")
                    })?;
                for (key, value) in patch {
                    properties.insert(key, value);
                }
                self.engine
                    .replace_edge_properties(*edge_id, properties)
                    .map_err(ExecutionError::from)?;
            }
            _ => {
                return Err(ExecutionError::new(
                    "set_target",
                    "SET += targets must resolve to a node or edge variable",
                ));
            }
        }
        Ok(())
    }

    fn apply_delete_clause(
        &mut self,
        rows: &[Row],
        variables: &[String],
    ) -> Result<(), ExecutionError> {
        let mut edges = BTreeSet::new();
        let mut nodes = BTreeSet::new();
        for row in rows {
            for variable in variables {
                match row.get(variable) {
                    Some(RuntimeValue::Node(node_id)) => {
                        nodes.insert(*node_id);
                    }
                    Some(RuntimeValue::Edge(edge_id)) => {
                        edges.insert(*edge_id);
                    }
                    _ => {
                        return Err(ExecutionError::new(
                            "delete_target",
                            "DELETE targets must resolve to a node or edge variable",
                        ));
                    }
                }
            }
        }
        for edge_id in edges {
            if self.engine.edge(edge_id).is_some() {
                self.engine
                    .delete_edge(edge_id)
                    .map_err(ExecutionError::from)?;
            }
        }
        for node_id in nodes {
            if self.engine.node(node_id).is_some() {
                self.engine
                    .delete_node(node_id)
                    .map_err(ExecutionError::from)?;
            }
        }
        Ok(())
    }

    fn compare_rows(
        &self,
        left: &Row,
        right: &Row,
        order_by: &[OrderItem],
        params: &BTreeMap<String, Value>,
    ) -> Ordering {
        for item in order_by {
            let left_value = self.eval_expr(&item.expr, left, params);
            let right_value = self.eval_expr(&item.expr, right, params);
            let ordering = match (left_value, right_value) {
                (Ok(left), Ok(right)) => compare_runtime_values(&left, &right),
                _ => Ordering::Equal,
            };
            if ordering != Ordering::Equal {
                return if item.descending {
                    ordering.reverse()
                } else {
                    ordering
                };
            }
        }
        Ordering::Equal
    }

    fn persist_after_commit(&mut self) -> Result<(), ExecutionError> {
        if let Some(path) = self.path.clone() {
            let db_uuid = storage::append_commit(&path, &self.engine, self.db_uuid)
                .map_err(ExecutionError::from)?;
            self.db_uuid = Some(db_uuid);
            self.dirty = false;
        } else {
            self.dirty = true;
        }
        Ok(())
    }

    fn bind_node_pattern(
        &self,
        row: &Row,
        node: &Node,
        pattern: &crate::query::NodePattern,
        params: &BTreeMap<String, Value>,
    ) -> Result<Option<Row>, ExecutionError> {
        if !pattern
            .labels
            .iter()
            .all(|label| node.labels().contains(label))
        {
            return Ok(None);
        }
        for (key, expr) in &pattern.properties {
            let expected = self.eval_expr(expr, row, params)?;
            let Some(actual) = node.property(key) else {
                return Ok(None);
            };
            if runtime_from_value(actual) != expected {
                return Ok(None);
            }
        }
        let mut next = row.clone();
        if let Some(variable) = &pattern.variable {
            match next.get(variable) {
                Some(RuntimeValue::Node(existing)) if *existing == node.id() => {}
                Some(_) => return Ok(None),
                None => {
                    next.insert(variable.clone(), RuntimeValue::Node(node.id()));
                }
            }
        }
        Ok(Some(next))
    }

    fn bind_edge_pattern(
        &self,
        row: &Row,
        edge: &Edge,
        pattern: &EdgePattern,
        params: &BTreeMap<String, Value>,
    ) -> Result<Option<Row>, ExecutionError> {
        if !self.edge_matches_filters(edge, pattern, row, params)? {
            return Ok(None);
        }
        let mut next = row.clone();
        if let Some(variable) = &pattern.variable {
            match next.get(variable) {
                Some(RuntimeValue::Edge(existing)) if *existing == edge.id() => {}
                Some(_) => return Ok(None),
                None => {
                    next.insert(variable.clone(), RuntimeValue::Edge(edge.id()));
                }
            }
        }
        Ok(Some(next))
    }

    fn edge_matches_filters(
        &self,
        edge: &Edge,
        pattern: &EdgePattern,
        row: &Row,
        params: &BTreeMap<String, Value>,
    ) -> Result<bool, ExecutionError> {
        if !pattern.edge_types.is_empty()
            && !pattern
                .edge_types
                .iter()
                .any(|edge_type| edge.edge_type() == edge_type)
        {
            return Ok(false);
        }
        for (key, expr) in &pattern.properties {
            let expected = self.eval_expr(expr, row, params)?;
            let Some(actual) = edge.property(key) else {
                return Ok(false);
            };
            if runtime_from_value(actual) != expected {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn edge_ids_for_direction(&self, node_id: NodeId, direction: Direction) -> Vec<EdgeId> {
        match direction {
            Direction::Outgoing => self.engine.outgoing_edge_ids(node_id),
            Direction::Incoming => self.engine.incoming_edge_ids(node_id),
            Direction::Undirected => {
                let mut ids = self.engine.outgoing_edge_ids(node_id);
                ids.extend(self.engine.incoming_edge_ids(node_id));
                ids.sort();
                ids.dedup();
                ids
            }
        }
    }

    fn eval_expr(
        &self,
        expr: &Expr,
        row: &Row,
        params: &BTreeMap<String, Value>,
    ) -> Result<RuntimeValue, ExecutionError> {
        match expr {
            Expr::Null => Ok(RuntimeValue::Null),
            Expr::Bool(value) => Ok(RuntimeValue::Bool(*value)),
            Expr::Int(value) => Ok(RuntimeValue::Int(*value)),
            Expr::Float(value) => Ok(RuntimeValue::Float(*value)),
            Expr::String(value) => Ok(RuntimeValue::String(value.clone())),
            Expr::Bytes(value) => Ok(RuntimeValue::Bytes(value.clone())),
            Expr::Datetime(value) => Ok(RuntimeValue::Datetime(*value)),
            Expr::Wildcard => Err(ExecutionError::new(
                "function_error",
                "wildcard expressions are only valid in projection or aggregate contexts",
            )),
            Expr::Parameter(name) => Ok(params
                .get(name)
                .map(runtime_from_value)
                .unwrap_or(RuntimeValue::Null)),
            Expr::Variable(name) => row.get(name).cloned().ok_or_else(|| {
                ExecutionError::new("unknown_variable", format!("unknown variable {name}"))
            }),
            Expr::Property(base, property) => {
                let value = self.eval_expr(base, row, params)?;
                self.lookup_property(&value, property)
            }
            Expr::Index { target, index } => {
                let target = self.eval_expr(target, row, params)?;
                let index = self.eval_expr(index, row, params)?;
                self.lookup_index(target, index)
            }
            Expr::List(values) => values
                .iter()
                .map(|value| self.eval_expr(value, row, params))
                .collect::<Result<Vec<_>, _>>()
                .map(RuntimeValue::List),
            Expr::Map(entries) => entries
                .iter()
                .map(|(key, value)| Ok((key.clone(), self.eval_expr(value, row, params)?)))
                .collect::<Result<Vec<_>, ExecutionError>>()
                .map(RuntimeValue::Map),
            Expr::Unary { op, expr } => {
                let value = self.eval_expr(expr, row, params)?;
                match (op, value) {
                    (UnaryOp::Not, RuntimeValue::Bool(value)) => Ok(RuntimeValue::Bool(!value)),
                    (UnaryOp::Not, RuntimeValue::Null) => Ok(RuntimeValue::Null),
                    (UnaryOp::Negate, RuntimeValue::Int(value)) => Ok(RuntimeValue::Int(-value)),
                    (UnaryOp::Negate, RuntimeValue::Float(value)) => {
                        Ok(RuntimeValue::Float(-value))
                    }
                    _ => Err(ExecutionError::new(
                        "unary_type_error",
                        "invalid operand for unary operator",
                    )),
                }
            }
            Expr::Binary { left, op, right } => {
                let left = self.eval_expr(left, row, params)?;
                if *op == BinaryOp::Or {
                    return short_circuit_or(|| self.eval_expr(right, row, params), left);
                }
                if *op == BinaryOp::And {
                    return short_circuit_and(|| self.eval_expr(right, row, params), left);
                }
                let right = self.eval_expr(right, row, params)?;
                self.eval_binary(op, left, right)
            }
            Expr::IsNull { expr, negated } => {
                let value = self.eval_expr(expr, row, params)?;
                Ok(RuntimeValue::Bool(if *negated {
                    value != RuntimeValue::Null
                } else {
                    value == RuntimeValue::Null
                }))
            }
            Expr::FunctionCall { name, args } => {
                let args = args
                    .iter()
                    .map(|arg| self.eval_expr(arg, row, params))
                    .collect::<Result<Vec<_>, _>>()?;
                self.eval_function(name, args)
            }
        }
    }

    fn lookup_property(
        &self,
        value: &RuntimeValue,
        property: &str,
    ) -> Result<RuntimeValue, ExecutionError> {
        match value {
            RuntimeValue::Node(node_id) => Ok(self
                .engine
                .node(*node_id)
                .and_then(|node| node.property(property))
                .map(runtime_from_value)
                .unwrap_or(RuntimeValue::Null)),
            RuntimeValue::Edge(edge_id) => Ok(self
                .engine
                .edge(*edge_id)
                .and_then(|edge| edge.property(property))
                .map(runtime_from_value)
                .unwrap_or(RuntimeValue::Null)),
            RuntimeValue::Map(entries) => Ok(entries
                .iter()
                .find(|(key, _)| key == property)
                .map(|(_, value)| value.clone())
                .unwrap_or(RuntimeValue::Null)),
            RuntimeValue::Null => Ok(RuntimeValue::Null),
            _ => Err(ExecutionError::new(
                "property_access_type_error",
                "property access requires a node, edge, or map value",
            )),
        }
    }

    fn lookup_index(
        &self,
        target: RuntimeValue,
        index: RuntimeValue,
    ) -> Result<RuntimeValue, ExecutionError> {
        match (target, index) {
            (RuntimeValue::Null, _) | (_, RuntimeValue::Null) => Ok(RuntimeValue::Null),
            (RuntimeValue::List(values), RuntimeValue::Int(index)) => {
                let Ok(index) = usize::try_from(index) else {
                    return Ok(RuntimeValue::Null);
                };
                Ok(values.get(index).cloned().unwrap_or(RuntimeValue::Null))
            }
            (RuntimeValue::Map(entries), RuntimeValue::String(key)) => Ok(entries
                .into_iter()
                .find(|(existing, _)| *existing == key)
                .map(|(_, value)| value)
                .unwrap_or(RuntimeValue::Null)),
            _ => Err(ExecutionError::new(
                "index_type_error",
                "index access requires list[int] or map[string]",
            )),
        }
    }

    fn eval_binary(
        &self,
        op: &BinaryOp,
        left: RuntimeValue,
        right: RuntimeValue,
    ) -> Result<RuntimeValue, ExecutionError> {
        use BinaryOp as Op;
        if matches!(left, RuntimeValue::Null) || matches!(right, RuntimeValue::Null) {
            return Ok(RuntimeValue::Null);
        }

        match op {
            Op::Eq => Ok(RuntimeValue::Bool(left == right)),
            Op::NotEq => Ok(RuntimeValue::Bool(left != right)),
            Op::Lt | Op::Lte | Op::Gt | Op::Gte => {
                let ordering = compare_runtime_values(&left, &right);
                if ordering == Ordering::Equal && !same_runtime_type(&left, &right) {
                    return Err(ExecutionError::new(
                        "comparison_type_error",
                        "cross-type comparisons are not supported",
                    ));
                }
                let result = match op {
                    Op::Lt => ordering == Ordering::Less,
                    Op::Lte => matches!(ordering, Ordering::Less | Ordering::Equal),
                    Op::Gt => ordering == Ordering::Greater,
                    Op::Gte => matches!(ordering, Ordering::Greater | Ordering::Equal),
                    _ => unreachable!(),
                };
                Ok(RuntimeValue::Bool(result))
            }
            Op::Add => match (left, right) {
                (RuntimeValue::Int(left), RuntimeValue::Int(right)) => {
                    Ok(RuntimeValue::Int(left + right))
                }
                (RuntimeValue::Float(left), RuntimeValue::Float(right)) => {
                    Ok(RuntimeValue::Float(left + right))
                }
                _ => Err(ExecutionError::new(
                    "arithmetic_type_error",
                    "addition requires matching numeric types",
                )),
            },
            Op::Subtract => match (left, right) {
                (RuntimeValue::Int(left), RuntimeValue::Int(right)) => {
                    Ok(RuntimeValue::Int(left - right))
                }
                (RuntimeValue::Float(left), RuntimeValue::Float(right)) => {
                    Ok(RuntimeValue::Float(left - right))
                }
                _ => Err(ExecutionError::new(
                    "arithmetic_type_error",
                    "subtraction requires matching numeric types",
                )),
            },
            Op::Multiply => match (left, right) {
                (RuntimeValue::Int(left), RuntimeValue::Int(right)) => {
                    Ok(RuntimeValue::Int(left * right))
                }
                (RuntimeValue::Float(left), RuntimeValue::Float(right)) => {
                    Ok(RuntimeValue::Float(left * right))
                }
                _ => Err(ExecutionError::new(
                    "arithmetic_type_error",
                    "multiplication requires matching numeric types",
                )),
            },
            Op::Divide => match (left, right) {
                (RuntimeValue::Int(_), RuntimeValue::Int(0))
                | (RuntimeValue::Float(_), RuntimeValue::Float(0.0)) => {
                    Err(ExecutionError::new("division_by_zero", "division by zero"))
                }
                (RuntimeValue::Int(left), RuntimeValue::Int(right)) => {
                    Ok(RuntimeValue::Int(left / right))
                }
                (RuntimeValue::Float(left), RuntimeValue::Float(right)) => {
                    Ok(RuntimeValue::Float(left / right))
                }
                _ => Err(ExecutionError::new(
                    "arithmetic_type_error",
                    "division requires matching numeric types",
                )),
            },
            Op::In => match right {
                RuntimeValue::List(values) => Ok(RuntimeValue::Bool(values.contains(&left))),
                RuntimeValue::String(right) => match left {
                    RuntimeValue::String(left) => Ok(RuntimeValue::Bool(right.contains(&left))),
                    _ => Err(ExecutionError::new(
                        "in_type_error",
                        "IN requires string operands when matching against a string",
                    )),
                },
                RuntimeValue::Map(entries) => match left {
                    RuntimeValue::String(left) => Ok(RuntimeValue::Bool(
                        entries.iter().any(|(key, _)| key == &left),
                    )),
                    _ => Err(ExecutionError::new(
                        "in_type_error",
                        "IN requires a string key when matching against a map",
                    )),
                },
                _ => Err(ExecutionError::new(
                    "in_type_error",
                    "IN requires a list, string, or map on the right-hand side",
                )),
            },
            Op::Contains => match (left, right) {
                (RuntimeValue::String(left), RuntimeValue::String(right)) => {
                    Ok(RuntimeValue::Bool(left.contains(&right)))
                }
                (RuntimeValue::List(values), value) => {
                    Ok(RuntimeValue::Bool(values.contains(&value)))
                }
                (RuntimeValue::Map(entries), RuntimeValue::String(key)) => Ok(RuntimeValue::Bool(
                    entries.iter().any(|(existing, _)| existing == &key),
                )),
                _ => Err(ExecutionError::new(
                    "contains_type_error",
                    "CONTAINS requires string operands, list membership, or a map key",
                )),
            },
            Op::StartsWith => match (left, right) {
                (RuntimeValue::String(left), RuntimeValue::String(right)) => {
                    Ok(RuntimeValue::Bool(left.starts_with(&right)))
                }
                _ => Err(ExecutionError::new(
                    "starts_with_type_error",
                    "STARTS WITH requires string operands",
                )),
            },
            Op::EndsWith => match (left, right) {
                (RuntimeValue::String(left), RuntimeValue::String(right)) => {
                    Ok(RuntimeValue::Bool(left.ends_with(&right)))
                }
                _ => Err(ExecutionError::new(
                    "ends_with_type_error",
                    "ENDS WITH requires string operands",
                )),
            },
            Op::RegexMatch => match (left, right) {
                (RuntimeValue::String(left), RuntimeValue::String(right)) => {
                    let regex = Regex::new(&right).map_err(|error| {
                        ExecutionError::new(
                            "regex_compile_error",
                            format!("invalid regex pattern: {error}"),
                        )
                    })?;
                    Ok(RuntimeValue::Bool(regex.is_match(&left)))
                }
                _ => Err(ExecutionError::new(
                    "regex_type_error",
                    "regex matching requires string operands",
                )),
            },
            Op::Or | Op::And => unreachable!(),
        }
    }

    fn eval_function(
        &self,
        name: &str,
        args: Vec<RuntimeValue>,
    ) -> Result<RuntimeValue, ExecutionError> {
        match (name, args.as_slice()) {
            ("append", [RuntimeValue::Null, value]) => Ok(RuntimeValue::List(vec![value.clone()])),
            ("append", [RuntimeValue::List(values), value]) => {
                let mut values = values.clone();
                values.push(value.clone());
                Ok(RuntimeValue::List(values))
            }
            ("insert", [RuntimeValue::Null, RuntimeValue::Int(index), value]) if *index == 0 => {
                Ok(RuntimeValue::List(vec![value.clone()]))
            }
            ("insert", [RuntimeValue::List(values), RuntimeValue::Int(index), value])
                if *index >= 0 && (*index as usize) <= values.len() =>
            {
                let mut values = values.clone();
                values.insert(*index as usize, value.clone());
                Ok(RuntimeValue::List(values))
            }
            ("insert", [RuntimeValue::List(_), RuntimeValue::Int(_), _]) => {
                Err(ExecutionError::new(
                    "index_type_error",
                    "insert indexes must fall within the list bounds",
                ))
            }
            ("remove", [RuntimeValue::Null, _]) => Ok(RuntimeValue::Null),
            ("remove", [RuntimeValue::List(values), value]) => Ok(RuntimeValue::List(
                values
                    .iter()
                    .filter(|entry| *entry != value)
                    .cloned()
                    .collect(),
            )),
            ("merge", [RuntimeValue::Null, RuntimeValue::Map(entries)]) => {
                Ok(RuntimeValue::Map(entries.clone()))
            }
            ("merge", [RuntimeValue::Map(left), RuntimeValue::Map(right)]) => {
                let mut merged = left.clone();
                for (key, value) in right {
                    if let Some(slot) = merged.iter_mut().find(|(existing, _)| existing == key) {
                        slot.1 = value.clone();
                    } else {
                        merged.push((key.clone(), value.clone()));
                    }
                }
                Ok(RuntimeValue::Map(merged))
            }
            ("size", [RuntimeValue::Null]) => Ok(RuntimeValue::Null),
            ("size", [RuntimeValue::String(value)]) => Ok(RuntimeValue::Int(value.len() as i64)),
            ("size", [RuntimeValue::List(values)]) => Ok(RuntimeValue::Int(values.len() as i64)),
            ("size", [RuntimeValue::Map(entries)]) => Ok(RuntimeValue::Int(entries.len() as i64)),
            ("type", [value]) => Ok(RuntimeValue::String(runtime_type_name(value).to_owned())),
            ("id", [RuntimeValue::Node(node_id)]) => Ok(RuntimeValue::Int(node_id.get() as i64)),
            ("id", [RuntimeValue::Edge(edge_id)]) => Ok(RuntimeValue::Int(edge_id.get() as i64)),
            ("edge_type", [RuntimeValue::Null]) => Ok(RuntimeValue::Null),
            ("edge_type", [RuntimeValue::Edge(edge_id)]) => Ok(self
                .engine
                .edge(*edge_id)
                .map(|edge| RuntimeValue::String(edge.edge_type().to_owned()))
                .unwrap_or(RuntimeValue::Null)),
            ("labels", [RuntimeValue::Node(node_id)]) => Ok(RuntimeValue::List(
                self.engine
                    .node(*node_id)
                    .map(|node| {
                        node.labels()
                            .iter()
                            .cloned()
                            .map(RuntimeValue::String)
                            .collect()
                    })
                    .unwrap_or_default(),
            )),
            ("has_prop", [value, RuntimeValue::String(key)]) => {
                Ok(RuntimeValue::Bool(match value {
                    RuntimeValue::Node(node_id) => self
                        .engine
                        .node(*node_id)
                        .map(|node| node.properties().contains_key(key))
                        .unwrap_or(false),
                    RuntimeValue::Edge(edge_id) => self
                        .engine
                        .edge(*edge_id)
                        .map(|edge| edge.properties().contains_key(key))
                        .unwrap_or(false),
                    RuntimeValue::Map(entries) => {
                        entries.iter().any(|(existing, _)| existing == key)
                    }
                    RuntimeValue::Null => false,
                    _ => false,
                }))
            }
            ("has_label", [RuntimeValue::Null, RuntimeValue::String(_)]) => {
                Ok(RuntimeValue::Bool(false))
            }
            ("has_label", [RuntimeValue::Node(node_id), RuntimeValue::String(label)]) => {
                Ok(RuntimeValue::Bool(
                    self.engine
                        .node(*node_id)
                        .map(|node| node.labels().iter().any(|existing| existing == label))
                        .unwrap_or(false),
                ))
            }
            ("keys", [RuntimeValue::Null]) => Ok(RuntimeValue::Null),
            ("keys", [RuntimeValue::Map(entries)]) => Ok(RuntimeValue::List(
                entries
                    .iter()
                    .map(|(key, _)| RuntimeValue::String(key.clone()))
                    .collect(),
            )),
            ("values", [RuntimeValue::Null]) => Ok(RuntimeValue::Null),
            ("values", [RuntimeValue::Map(entries)]) => Ok(RuntimeValue::List(
                entries.iter().map(|(_, value)| value.clone()).collect(),
            )),
            ("contains", [RuntimeValue::Null, _]) => Ok(RuntimeValue::Null),
            ("contains", [RuntimeValue::List(values), value]) => {
                Ok(RuntimeValue::Bool(values.contains(value)))
            }
            _ => Err(ExecutionError::new(
                "function_error",
                format!("unsupported function call {name}"),
            )),
        }
    }
}

impl Statement {
    fn is_transaction_control(&self) -> bool {
        matches!(
            self,
            Self::Begin
                | Self::Commit
                | Self::Rollback
                | Self::Savepoint(_)
                | Self::RollbackToSavepoint(_)
                | Self::ReleaseSavepoint(_)
        )
    }

    fn allowed_in_failed_transaction(&self) -> bool {
        matches!(
            self,
            Self::Rollback
                | Self::RollbackToSavepoint(_)
                | Self::ReleaseSavepoint(_)
                | Self::Show(_)
        )
    }

    fn is_mutating(&self) -> bool {
        match self {
            Self::CreateLabel { .. }
            | Self::DropLabel { .. }
            | Self::CreateEdgeType { .. }
            | Self::DropEdgeType { .. }
            | Self::CreateIndex { .. }
            | Self::AlterIndex { .. }
            | Self::DropIndex { .. }
            | Self::CreateConstraint { .. }
            | Self::AlterConstraint { .. }
            | Self::DropConstraint { .. } => true,
            Self::Query(query) => {
                query.merge_clause.is_some()
                    || query.create_clause.is_some()
                    || !query.set_clause.is_empty()
                    || !query.remove_clause.is_empty()
                    || !query.delete_clause.is_empty()
            }
            _ => false,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum ExecutionErrorCode {
    ArithmeticTypeError,
    BooleanTypeError,
    ComparisonTypeError,
    ContainsTypeError,
    CreateEdgeType,
    CreateVariableHops,
    DbUuidMissing,
    DeleteTarget,
    DivisionByZero,
    EndsWithTypeError,
    FunctionError,
    GraphValueConversion,
    InTypeError,
    IndexTypeError,
    MultiStatementRequiresTransaction,
    PropertyAccessTypeError,
    RemoveTarget,
    RegexCompileError,
    RegexTypeError,
    RowLimitExceeded,
    SaveRequiresPath,
    SavepointExists,
    SavepointNotFound,
    SetTarget,
    StartsWithTypeError,
    TransactionActive,
    TransactionFailed,
    TransactionFailedCommit,
    TransactionNotActive,
    UnaryTypeError,
    UnknownVariable,
    VariableHopEdgeBinding,
    WhereTypeError,
}

impl ExecutionErrorCode {
    fn as_str(&self) -> &'static str {
        match self {
            Self::ArithmeticTypeError => "arithmetic_type_error",
            Self::BooleanTypeError => "boolean_type_error",
            Self::ComparisonTypeError => "comparison_type_error",
            Self::ContainsTypeError => "contains_type_error",
            Self::CreateEdgeType => "create_edge_type",
            Self::CreateVariableHops => "create_variable_hops",
            Self::DbUuidMissing => "db_uuid_missing",
            Self::DeleteTarget => "delete_target",
            Self::DivisionByZero => "division_by_zero",
            Self::EndsWithTypeError => "ends_with_type_error",
            Self::FunctionError => "function_error",
            Self::GraphValueConversion => "graph_value_conversion",
            Self::InTypeError => "in_type_error",
            Self::IndexTypeError => "index_type_error",
            Self::MultiStatementRequiresTransaction => "multi_statement_requires_transaction",
            Self::PropertyAccessTypeError => "property_access_type_error",
            Self::RemoveTarget => "remove_target",
            Self::RegexCompileError => "regex_compile_error",
            Self::RegexTypeError => "regex_type_error",
            Self::RowLimitExceeded => "row_limit_exceeded",
            Self::SaveRequiresPath => "save_requires_path",
            Self::SavepointExists => "savepoint_exists",
            Self::SavepointNotFound => "savepoint_not_found",
            Self::SetTarget => "set_target",
            Self::StartsWithTypeError => "starts_with_type_error",
            Self::TransactionActive => "transaction_active",
            Self::TransactionFailed => "transaction_failed",
            Self::TransactionFailedCommit => "transaction_failed_commit",
            Self::TransactionNotActive => "transaction_not_active",
            Self::UnaryTypeError => "unary_type_error",
            Self::UnknownVariable => "unknown_variable",
            Self::VariableHopEdgeBinding => "variable_hop_edge_binding",
            Self::WhereTypeError => "where_type_error",
        }
    }
}

impl From<&'static str> for ExecutionErrorCode {
    fn from(value: &'static str) -> Self {
        match value {
            "arithmetic_type_error" => Self::ArithmeticTypeError,
            "boolean_type_error" => Self::BooleanTypeError,
            "comparison_type_error" => Self::ComparisonTypeError,
            "contains_type_error" => Self::ContainsTypeError,
            "create_edge_type" => Self::CreateEdgeType,
            "create_variable_hops" => Self::CreateVariableHops,
            "db_uuid_missing" => Self::DbUuidMissing,
            "delete_target" => Self::DeleteTarget,
            "division_by_zero" => Self::DivisionByZero,
            "ends_with_type_error" => Self::EndsWithTypeError,
            "function_error" => Self::FunctionError,
            "graph_value_conversion" => Self::GraphValueConversion,
            "in_type_error" => Self::InTypeError,
            "index_type_error" => Self::IndexTypeError,
            "multi_statement_requires_transaction" => Self::MultiStatementRequiresTransaction,
            "property_access_type_error" => Self::PropertyAccessTypeError,
            "remove_target" => Self::RemoveTarget,
            "regex_compile_error" => Self::RegexCompileError,
            "regex_type_error" => Self::RegexTypeError,
            "row_limit_exceeded" => Self::RowLimitExceeded,
            "save_requires_path" => Self::SaveRequiresPath,
            "savepoint_exists" => Self::SavepointExists,
            "savepoint_not_found" => Self::SavepointNotFound,
            "set_target" => Self::SetTarget,
            "starts_with_type_error" => Self::StartsWithTypeError,
            "transaction_active" => Self::TransactionActive,
            "transaction_failed" => Self::TransactionFailed,
            "transaction_failed_commit" => Self::TransactionFailedCommit,
            "transaction_not_active" => Self::TransactionNotActive,
            "unary_type_error" => Self::UnaryTypeError,
            "unknown_variable" => Self::UnknownVariable,
            "variable_hop_edge_binding" => Self::VariableHopEdgeBinding,
            "where_type_error" => Self::WhereTypeError,
            _ => panic!("unknown execution error code: {value}"),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum ExecutionErrorKind {
    Runtime(ExecutionErrorCode),
    Query(QueryError),
    Graph(GraphError),
    Storage(storage::StorageError),
}

impl ExecutionErrorKind {
    fn code(&self) -> &'static str {
        match self {
            Self::Runtime(code) => code.as_str(),
            Self::Query(error) => error.code(),
            Self::Graph(error) => error.code(),
            Self::Storage(error) => error.code(),
        }
    }
}

impl From<&'static str> for ExecutionErrorKind {
    fn from(value: &'static str) -> Self {
        Self::Runtime(value.into())
    }
}

impl From<QueryError> for ExecutionErrorKind {
    fn from(value: QueryError) -> Self {
        Self::Query(value)
    }
}

impl From<GraphError> for ExecutionErrorKind {
    fn from(value: GraphError) -> Self {
        Self::Graph(value)
    }
}

impl From<storage::StorageError> for ExecutionErrorKind {
    fn from(value: storage::StorageError) -> Self {
        Self::Storage(value)
    }
}

#[derive(Clone, Debug)]
pub struct ExecutionError {
    kind: ExecutionErrorKind,
    message: String,
}

impl ExecutionError {
    fn new(kind: impl Into<ExecutionErrorKind>, message: impl Into<String>) -> Self {
        Self {
            kind: kind.into(),
            message: message.into(),
        }
    }

    pub fn code(&self) -> &'static str {
        self.kind.code()
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for ExecutionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.code(), self.message)
    }
}

impl std::error::Error for ExecutionError {}

impl From<QueryError> for ExecutionError {
    fn from(value: QueryError) -> Self {
        let message = value.to_string();
        Self::new(value, message)
    }
}

impl From<GraphError> for ExecutionError {
    fn from(value: GraphError) -> Self {
        let message = value.to_string();
        Self::new(value, message)
    }
}

impl From<storage::StorageError> for ExecutionError {
    fn from(value: storage::StorageError) -> Self {
        let message = value.to_string();
        Self::new(value, message)
    }
}

#[derive(Clone, Debug)]
pub enum RuntimeValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    Bytes(Vec<u8>),
    Datetime(SystemTime),
    List(Vec<RuntimeValue>),
    Map(Vec<(String, RuntimeValue)>),
    Node(NodeId),
    Edge(EdgeId),
}

impl PartialEq for RuntimeValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Null, Self::Null) => true,
            (Self::Bool(left), Self::Bool(right)) => left == right,
            (Self::Int(left), Self::Int(right)) => left == right,
            (Self::Float(left), Self::Float(right)) => left.to_bits() == right.to_bits(),
            (Self::String(left), Self::String(right)) => left == right,
            (Self::Bytes(left), Self::Bytes(right)) => left == right,
            (Self::Datetime(left), Self::Datetime(right)) => left == right,
            (Self::List(left), Self::List(right)) => left == right,
            (Self::Map(left), Self::Map(right)) => left == right,
            (Self::Node(left), Self::Node(right)) => left == right,
            (Self::Edge(left), Self::Edge(right)) => left == right,
            _ => false,
        }
    }
}

impl RuntimeValue {
    fn to_graph_value(&self) -> Result<Value, ExecutionError> {
        match self {
            Self::Null => Ok(Value::Null),
            Self::Bool(value) => Ok(Value::Bool(*value)),
            Self::Int(value) => Ok(Value::Int(*value)),
            Self::Float(value) => Ok(Value::Float(*value)),
            Self::String(value) => Ok(Value::String(value.clone())),
            Self::Bytes(value) => Ok(Value::Bytes(value.clone())),
            Self::Datetime(value) => Ok(Value::Datetime(*value)),
            Self::List(values) => values
                .iter()
                .map(RuntimeValue::to_graph_value)
                .collect::<Result<Vec<_>, _>>()
                .map(Value::List),
            Self::Map(entries) => {
                let mut properties = PropertyMap::new();
                for (key, value) in entries {
                    properties.insert(key.clone(), value.to_graph_value()?);
                }
                Ok(Value::from(properties))
            }
            Self::Node(_) | Self::Edge(_) => Err(ExecutionError::new(
                "graph_value_conversion",
                "node and edge values cannot be stored as properties",
            )),
        }
    }
}

type Row = BTreeMap<String, RuntimeValue>;

fn empty_result() -> QueryResult {
    query_result(&[], Vec::<Vec<RuntimeValue>>::new())
}

fn query_result(columns: &[&str], rows: Vec<Vec<RuntimeValue>>) -> QueryResult {
    QueryResult {
        columns: columns.iter().map(|column| (*column).to_owned()).collect(),
        rows,
    }
}

fn single_row_result(columns: &[&str], row: Vec<RuntimeValue>) -> QueryResult {
    query_result(columns, vec![row])
}

impl Session {
    fn require_active_transaction(&self, message: &'static str) -> Result<(), ExecutionError> {
        if self.transaction_base.is_some() {
            Ok(())
        } else {
            Err(ExecutionError::new("transaction_not_active", message))
        }
    }

    fn find_savepoint_index(&self, name: &str) -> Result<usize, ExecutionError> {
        self.savepoints
            .iter()
            .position(|(existing, _, _)| existing == name)
            .ok_or_else(|| ExecutionError::new("savepoint_not_found", "savepoint does not exist"))
    }
}

fn eval_property_map(
    session: &Session,
    row: &Row,
    properties: &[(String, Expr)],
    params: &BTreeMap<String, Value>,
) -> Result<PropertyMap, ExecutionError> {
    let mut map = PropertyMap::new();
    for (key, expr) in properties {
        let value = session.eval_expr(expr, row, params)?;
        map.insert(key.clone(), value.to_graph_value()?);
    }
    Ok(map)
}

fn runtime_entries_to_property_map(entries: &[(String, RuntimeValue)]) -> Result<PropertyMap, ExecutionError> {
    let mut map = PropertyMap::new();
    for (key, value) in entries {
        map.insert(key.clone(), value.to_graph_value()?);
    }
    Ok(map)
}

fn resolve_param_value(
    value: &ParamValue,
    params: &BTreeMap<String, Value>,
    role: &str,
) -> Result<String, ExecutionError> {
    match value {
        ParamValue::Literal(value) => Ok(value.clone()),
        ParamValue::Parameter(name) => match params.get(name) {
            Some(Value::String(value)) => Ok(value.clone()),
            Some(_) => Err(ExecutionError::new(
                "graph_value_conversion",
                format!("{role} parameters must resolve to strings"),
            )),
            None => Err(ExecutionError::new(
                "unknown_variable",
                format!("missing parameter ${name}"),
            )),
        },
    }
}

fn resolve_optional_param_value(
    value: Option<&ParamValue>,
    params: &BTreeMap<String, Value>,
    role: &str,
) -> Result<Option<String>, ExecutionError> {
    value.map(|value| resolve_param_value(value, params, role)).transpose()
}

fn resolve_schema_target(
    target: &crate::query::SchemaTargetExpr,
    params: &BTreeMap<String, Value>,
) -> Result<SchemaTarget, ExecutionError> {
    let name = resolve_param_value(target.name(), params, "schema target")?;
    Ok(match target.kind() {
        crate::engine::TargetKind::Label => SchemaTarget::label(name),
        crate::engine::TargetKind::EdgeType => SchemaTarget::edge_type(name),
    })
}

fn resolve_constraint_spec(
    spec: &ConstraintSpec,
    params: &BTreeMap<String, Value>,
) -> Result<(String, ConstraintType), ExecutionError> {
    Ok(match spec {
        ConstraintSpec::Unique { property } => (
            resolve_param_value(property, params, "property name")?,
            ConstraintType::Unique,
        ),
        ConstraintSpec::Required { property } => (
            resolve_param_value(property, params, "property name")?,
            ConstraintType::Required,
        ),
        ConstraintSpec::Type {
            property,
            value_type,
        } => (
            resolve_param_value(property, params, "property name")?,
            ConstraintType::Type(*value_type),
        ),
        ConstraintSpec::Endpoints {
            from_label,
            to_label,
        } => (
            String::new(),
            ConstraintType::Endpoints {
                from_label: resolve_param_value(from_label, params, "label name")?,
                to_label: resolve_param_value(to_label, params, "label name")?,
            },
        ),
        ConstraintSpec::MaxOutgoing(limit) => (String::new(), ConstraintType::MaxOutgoing(*limit)),
    })
}

fn runtime_from_value(value: &Value) -> RuntimeValue {
    match value {
        Value::Null => RuntimeValue::Null,
        Value::Bool(value) => RuntimeValue::Bool(*value),
        Value::Int(value) => RuntimeValue::Int(*value),
        Value::Float(value) => RuntimeValue::Float(*value),
        Value::String(value) => RuntimeValue::String(value.clone()),
        Value::Bytes(value) => RuntimeValue::Bytes(value.clone()),
        Value::Datetime(value) => RuntimeValue::Datetime(*value),
        Value::List(values) => RuntimeValue::List(values.iter().map(runtime_from_value).collect()),
        Value::Map(map) => RuntimeValue::Map(
            map.iter()
                .map(|(key, value)| (key.to_owned(), runtime_from_value(value)))
                .collect(),
        ),
    }
}

fn short_circuit_or<F>(rhs: F, left: RuntimeValue) -> Result<RuntimeValue, ExecutionError>
where
    F: FnOnce() -> Result<RuntimeValue, ExecutionError>,
{
    match left {
        RuntimeValue::Bool(true) => Ok(RuntimeValue::Bool(true)),
        RuntimeValue::Bool(false) => rhs().and_then(|right| match right {
            RuntimeValue::Bool(value) => Ok(RuntimeValue::Bool(value)),
            RuntimeValue::Null => Ok(RuntimeValue::Null),
            _ => Err(ExecutionError::new(
                "boolean_type_error",
                "OR requires bool operands",
            )),
        }),
        RuntimeValue::Null => rhs().and_then(|right| match right {
            RuntimeValue::Bool(true) => Ok(RuntimeValue::Bool(true)),
            RuntimeValue::Bool(false) | RuntimeValue::Null => Ok(RuntimeValue::Null),
            _ => Err(ExecutionError::new(
                "boolean_type_error",
                "OR requires bool operands",
            )),
        }),
        _ => Err(ExecutionError::new(
            "boolean_type_error",
            "OR requires bool operands",
        )),
    }
}

fn short_circuit_and<F>(rhs: F, left: RuntimeValue) -> Result<RuntimeValue, ExecutionError>
where
    F: FnOnce() -> Result<RuntimeValue, ExecutionError>,
{
    match left {
        RuntimeValue::Bool(false) => Ok(RuntimeValue::Bool(false)),
        RuntimeValue::Bool(true) => rhs().and_then(|right| match right {
            RuntimeValue::Bool(value) => Ok(RuntimeValue::Bool(value)),
            RuntimeValue::Null => Ok(RuntimeValue::Null),
            _ => Err(ExecutionError::new(
                "boolean_type_error",
                "AND requires bool operands",
            )),
        }),
        RuntimeValue::Null => rhs().and_then(|right| match right {
            RuntimeValue::Bool(false) => Ok(RuntimeValue::Bool(false)),
            RuntimeValue::Bool(true) | RuntimeValue::Null => Ok(RuntimeValue::Null),
            _ => Err(ExecutionError::new(
                "boolean_type_error",
                "AND requires bool operands",
            )),
        }),
        _ => Err(ExecutionError::new(
            "boolean_type_error",
            "AND requires bool operands",
        )),
    }
}

fn compare_runtime_values(left: &RuntimeValue, right: &RuntimeValue) -> Ordering {
    match (left, right) {
        (RuntimeValue::Null, RuntimeValue::Null) => Ordering::Equal,
        (RuntimeValue::Null, _) => Ordering::Greater,
        (_, RuntimeValue::Null) => Ordering::Less,
        (RuntimeValue::Bool(left), RuntimeValue::Bool(right)) => left.cmp(right),
        (RuntimeValue::Int(left), RuntimeValue::Int(right)) => left.cmp(right),
        (RuntimeValue::Float(left), RuntimeValue::Float(right)) => {
            left.partial_cmp(right).unwrap_or(Ordering::Equal)
        }
        (RuntimeValue::String(left), RuntimeValue::String(right)) => left.cmp(right),
        (RuntimeValue::Bytes(left), RuntimeValue::Bytes(right)) => left.cmp(right),
        (RuntimeValue::Datetime(left), RuntimeValue::Datetime(right)) => left.cmp(right),
        (RuntimeValue::List(left), RuntimeValue::List(right)) => left.len().cmp(&right.len()),
        (RuntimeValue::Map(left), RuntimeValue::Map(right)) => left.len().cmp(&right.len()),
        (RuntimeValue::Node(left), RuntimeValue::Node(right)) => left.get().cmp(&right.get()),
        (RuntimeValue::Edge(left), RuntimeValue::Edge(right)) => left.get().cmp(&right.get()),
        _ => Ordering::Equal,
    }
}

fn same_runtime_type(left: &RuntimeValue, right: &RuntimeValue) -> bool {
    std::mem::discriminant(left) == std::mem::discriminant(right)
}

fn runtime_type_name(value: &RuntimeValue) -> &'static str {
    match value {
        RuntimeValue::Null => "null",
        RuntimeValue::Bool(_) => "bool",
        RuntimeValue::Int(_) => "int",
        RuntimeValue::Float(_) => "float",
        RuntimeValue::String(_) => "string",
        RuntimeValue::Bytes(_) => "bytes",
        RuntimeValue::Datetime(_) => "datetime",
        RuntimeValue::List(_) => "list",
        RuntimeValue::Map(_) => "map",
        RuntimeValue::Node(_) => "node",
        RuntimeValue::Edge(_) => "edge",
    }
}

fn show_schema_result(rows: Vec<SchemaRow>) -> QueryResult {
    query_result(
        &["kind", "name", "description", "ddl"],
        rows.into_iter()
            .map(|row| {
                vec![
                    RuntimeValue::String(row.kind),
                    RuntimeValue::String(row.name),
                    row.description
                        .map(RuntimeValue::String)
                        .unwrap_or(RuntimeValue::Null),
                    RuntimeValue::String(row.ddl),
                ]
            })
            .collect(),
    )
}

fn show_indexes_result(rows: Vec<IndexRow>) -> QueryResult {
    query_result(
        &[
            "name",
            "target_kind",
            "target_name",
            "property",
            "unique",
            "status",
        ],
        rows.into_iter()
            .map(|row| {
                vec![
                    RuntimeValue::String(row.name),
                    RuntimeValue::String(row.target_kind),
                    RuntimeValue::String(row.target_name),
                    RuntimeValue::String(row.property),
                    RuntimeValue::Bool(row.unique),
                    RuntimeValue::String(row.status),
                ]
            })
            .collect(),
    )
}

fn show_constraints_result(rows: Vec<ConstraintRow>) -> QueryResult {
    query_result(
        &[
            "name",
            "target_kind",
            "target_name",
            "property",
            "constraint_type",
            "details",
        ],
        rows.into_iter()
            .map(|row| {
                vec![
                    RuntimeValue::String(row.name),
                    RuntimeValue::String(row.target_kind),
                    RuntimeValue::String(row.target_name),
                    RuntimeValue::String(row.property),
                    RuntimeValue::String(row.constraint_type),
                    RuntimeValue::String(row.details),
                ]
            })
            .collect(),
    )
}

fn show_stats_result(stats: GraphStats) -> QueryResult {
    single_row_result(
        &[
            "node_count",
            "edge_count",
            "label_count",
            "edge_type_count",
            "index_count",
            "constraint_count",
            "last_tx_id",
            "wal_bytes",
        ],
        vec![
            RuntimeValue::Int(stats.node_count as i64),
            RuntimeValue::Int(stats.edge_count as i64),
            RuntimeValue::Int(stats.label_count as i64),
            RuntimeValue::Int(stats.edge_type_count as i64),
            RuntimeValue::Int(stats.index_count as i64),
            RuntimeValue::Int(stats.constraint_count as i64),
            RuntimeValue::Int(stats.last_tx_id as i64),
            RuntimeValue::Int(stats.wal_bytes as i64),
        ],
    )
}

fn show_transactions_result(info: TransactionInfo) -> QueryResult {
    single_row_result(
        &["active", "failed", "savepoints", "last_tx_id"],
        vec![
            RuntimeValue::Bool(info.active),
            RuntimeValue::Bool(info.failed),
            RuntimeValue::Int(info.savepoints as i64),
            RuntimeValue::Int(info.last_tx_id as i64),
        ],
    )
}

fn build_explain_rows(
    statement: &Statement,
    parent_id: Option<i64>,
    next_id: &mut i64,
    rows: &mut Vec<Vec<RuntimeValue>>,
    match_plan: Option<&MatchPlan>,
) {
    let id = *next_id;
    *next_id += 1;
    let (operator, detail) = match statement {
        Statement::Show(kind) => ("Show", show_kind_detail(kind)),
        Statement::Explain(_) => ("Explain", String::new()),
        Statement::CreateLabel { name, .. } => ("CreateLabel", param_value_detail(name)),
        Statement::DropLabel { name, .. } => ("DropLabel", param_value_detail(name)),
        Statement::CreateEdgeType { name, .. } => ("CreateEdgeType", param_value_detail(name)),
        Statement::DropEdgeType { name, .. } => ("DropEdgeType", param_value_detail(name)),
        Statement::CreateIndex { property, .. } => ("CreateIndex", param_value_detail(property)),
        Statement::AlterIndex { name, status } => (
            "AlterIndex",
            format!("{} {}", param_value_detail(name), status.as_str()),
        ),
        Statement::DropIndex { name, .. } => ("DropIndex", param_value_detail(name)),
        Statement::CreateConstraint { constraint, .. } => {
            ("CreateConstraint", constraint_spec_detail(constraint))
        }
        Statement::AlterConstraint { name, rename_to } => (
            "AlterConstraint",
            format!(
                "{} -> {}",
                param_value_detail(name),
                param_value_detail(rename_to)
            ),
        ),
        Statement::DropConstraint { name, .. } => ("DropConstraint", param_value_detail(name)),
        Statement::Begin => ("Begin", String::new()),
        Statement::Commit => ("Commit", String::new()),
        Statement::Rollback => ("Rollback", String::new()),
        Statement::Savepoint(name) => ("Savepoint", name.clone()),
        Statement::RollbackToSavepoint(name) => ("RollbackToSavepoint", name.clone()),
        Statement::ReleaseSavepoint(name) => ("ReleaseSavepoint", name.clone()),
        Statement::Query(query) => {
            rows.push(vec![
                RuntimeValue::Int(id),
                parent_id
                    .map(RuntimeValue::Int)
                    .unwrap_or(RuntimeValue::Null),
                RuntimeValue::String("Query".to_owned()),
                RuntimeValue::String(String::new()),
            ]);
            if query.match_clause.is_some() {
                if let Some(match_plan) = match_plan {
                    add_explain_child(
                        next_id,
                        rows,
                        id,
                        match_plan_operator(match_plan),
                        &match_plan_detail(match_plan),
                    );
                } else {
                    add_explain_child(next_id, rows, id, "Match", "pattern");
                }
            }
            if query.where_clause.is_some() {
                add_explain_child(next_id, rows, id, "Filter", "WHERE");
            }
            if !query.with_clauses.is_empty() {
                add_explain_child(next_id, rows, id, "With", "WITH");
            }
            if query.merge_clause.is_some() {
                add_explain_child(next_id, rows, id, "Merge", "pattern");
            }
            if query.create_clause.is_some() {
                add_explain_child(next_id, rows, id, "Create", "pattern");
            }
            if !query.set_clause.is_empty() {
                add_explain_child(next_id, rows, id, "Set", "properties");
            }
            if !query.remove_clause.is_empty() {
                add_explain_child(next_id, rows, id, "Remove", "properties");
            }
            if !query.delete_clause.is_empty() {
                add_explain_child(next_id, rows, id, "Delete", "variables");
            }
            if query.return_all || !query.return_clause.is_empty() {
                add_explain_child(next_id, rows, id, "Project", "RETURN");
            }
            if !query.order_by.is_empty() {
                add_explain_child(next_id, rows, id, "Order", "ORDER BY");
            }
            if query.limit.is_some() {
                add_explain_child(next_id, rows, id, "Limit", "LIMIT");
            }
            return;
        }
    };

    rows.push(vec![
        RuntimeValue::Int(id),
        parent_id
            .map(RuntimeValue::Int)
            .unwrap_or(RuntimeValue::Null),
        RuntimeValue::String(operator.to_owned()),
        RuntimeValue::String(detail),
    ]);
}

fn add_explain_child(
    next_id: &mut i64,
    rows: &mut Vec<Vec<RuntimeValue>>,
    parent_id: i64,
    operator: &str,
    detail: &str,
) {
    let id = *next_id;
    *next_id += 1;
    rows.push(vec![
        RuntimeValue::Int(id),
        RuntimeValue::Int(parent_id),
        RuntimeValue::String(operator.to_owned()),
        RuntimeValue::String(detail.to_owned()),
    ]);
}

fn param_value_detail(value: &ParamValue) -> String {
    match value {
        ParamValue::Literal(value) => value.clone(),
        ParamValue::Parameter(name) => format!("${name}"),
    }
}

fn constraint_spec_detail(spec: &ConstraintSpec) -> String {
    match spec {
        ConstraintSpec::Unique { property } => format!("{} UNIQUE", param_value_detail(property)),
        ConstraintSpec::Required { property } => {
            format!("{} REQUIRED", param_value_detail(property))
        }
        ConstraintSpec::Type {
            property,
            value_type,
        } => format!("{} TYPE {}", param_value_detail(property), value_type),
        ConstraintSpec::Endpoints {
            from_label,
            to_label,
        } => format!(
            "ENDPOINTS :{} -> :{}",
            param_value_detail(from_label),
            param_value_detail(to_label)
        ),
        ConstraintSpec::MaxOutgoing(limit) => format!("MAX OUTGOING {limit}"),
    }
}

#[derive(Default)]
struct RangeConstraintSet {
    lower: Option<RangeBound>,
    upper: Option<RangeBound>,
}

fn fold_range_constraints(
    constraints: &[PropertyConstraint],
) -> BTreeMap<String, RangeConstraintSet> {
    let mut ranges = BTreeMap::<String, RangeConstraintSet>::new();
    for constraint in constraints {
        let slot = ranges.entry(constraint.property.clone()).or_default();
        match constraint.kind {
            ConstraintKind::Eq => {
                slot.lower = Some(RangeBound {
                    value: constraint.value.clone(),
                    inclusive: true,
                });
                slot.upper = Some(RangeBound {
                    value: constraint.value.clone(),
                    inclusive: true,
                });
            }
            ConstraintKind::Gt | ConstraintKind::Gte => {
                let candidate = RangeBound {
                    value: constraint.value.clone(),
                    inclusive: constraint.kind == ConstraintKind::Gte,
                };
                if slot
                    .lower
                    .as_ref()
                    .is_none_or(|existing| bound_is_tighter_lower(&candidate, existing))
                {
                    slot.lower = Some(candidate);
                }
            }
            ConstraintKind::Lt | ConstraintKind::Lte => {
                let candidate = RangeBound {
                    value: constraint.value.clone(),
                    inclusive: constraint.kind == ConstraintKind::Lte,
                };
                if slot
                    .upper
                    .as_ref()
                    .is_none_or(|existing| bound_is_tighter_upper(&candidate, existing))
                {
                    slot.upper = Some(candidate);
                }
            }
        }
    }
    ranges
}

fn bound_is_tighter_lower(candidate: &RangeBound, existing: &RangeBound) -> bool {
    let ordering = compare_runtime_values(&candidate.value, &existing.value);
    ordering == Ordering::Greater
        || (ordering == Ordering::Equal && !candidate.inclusive && existing.inclusive)
}

fn bound_is_tighter_upper(candidate: &RangeBound, existing: &RangeBound) -> bool {
    let ordering = compare_runtime_values(&candidate.value, &existing.value);
    ordering == Ordering::Less
        || (ordering == Ordering::Equal && !candidate.inclusive && existing.inclusive)
}

fn range_matches(
    value: &RuntimeValue,
    lower: Option<&RangeBound>,
    upper: Option<&RangeBound>,
) -> bool {
    if let Some(lower) = lower {
        if !same_runtime_type(value, &lower.value) {
            return false;
        }
        let ordering = compare_runtime_values(value, &lower.value);
        if ordering == Ordering::Less || (ordering == Ordering::Equal && !lower.inclusive) {
            return false;
        }
    }
    if let Some(upper) = upper {
        if !same_runtime_type(value, &upper.value) {
            return false;
        }
        let ordering = compare_runtime_values(value, &upper.value);
        if ordering == Ordering::Greater || (ordering == Ordering::Equal && !upper.inclusive) {
            return false;
        }
    }
    true
}

fn constraint_kind_from_binary(op: BinaryOp) -> Option<ConstraintKind> {
    match op {
        BinaryOp::Eq => Some(ConstraintKind::Eq),
        BinaryOp::Lt => Some(ConstraintKind::Lt),
        BinaryOp::Lte => Some(ConstraintKind::Lte),
        BinaryOp::Gt => Some(ConstraintKind::Gt),
        BinaryOp::Gte => Some(ConstraintKind::Gte),
        _ => None,
    }
}

fn reverse_constraint_op(op: BinaryOp) -> BinaryOp {
    match op {
        BinaryOp::Lt => BinaryOp::Gt,
        BinaryOp::Lte => BinaryOp::Gte,
        BinaryOp::Gt => BinaryOp::Lt,
        BinaryOp::Gte => BinaryOp::Lte,
        other => other,
    }
}

fn match_plan_operator(plan: &MatchPlan) -> &'static str {
    match plan.access {
        MatchAccessPath::NodeScan { .. } => "NodeScan",
        MatchAccessPath::NodeIndexSeek { .. } => "NodeIndexSeek",
        MatchAccessPath::NodeIndexRangeScan { .. } => "NodeIndexRangeScan",
    }
}

fn match_plan_detail(plan: &MatchPlan) -> String {
    match &plan.access {
        MatchAccessPath::NodeScan { detail } => detail.clone(),
        MatchAccessPath::NodeIndexSeek {
            target,
            property,
            value,
        } => format!("{}({}) = {:?}", target.display_target(), property, value),
        MatchAccessPath::NodeIndexRangeScan {
            target,
            property,
            lower,
            upper,
        } => format!(
            "{}({}) range {} {}",
            target.display_target(),
            property,
            lower
                .as_ref()
                .map(|bound| format_bound(">=", ">", bound))
                .unwrap_or_else(|| "-inf".to_owned()),
            upper
                .as_ref()
                .map(|bound| format_bound("<=", "<", bound))
                .unwrap_or_else(|| "+inf".to_owned())
        ),
    }
}

fn format_bound(inclusive: &str, exclusive: &str, bound: &RangeBound) -> String {
    format!(
        "{} {:?}",
        if bound.inclusive {
            inclusive
        } else {
            exclusive
        },
        bound.value
    )
}

fn projection_name_for_with(index: usize, item: &ReturnItem) -> String {
    item.alias.clone().unwrap_or_else(|| match &item.expr {
        Expr::Variable(name) => name.clone(),
        _ => format!("col_{}", index + 1),
    })
}

fn projection_name_for_return(index: usize, item: &ReturnItem) -> String {
    item.alias
        .clone()
        .unwrap_or_else(|| format!("col_{}", index + 1))
}

fn path_runtime_value(trace: &PathTrace) -> RuntimeValue {
    RuntimeValue::Map(vec![
        (
            "nodes".to_owned(),
            RuntimeValue::List(
                trace
                    .nodes
                    .iter()
                    .copied()
                    .map(RuntimeValue::Node)
                    .collect(),
            ),
        ),
        (
            "edges".to_owned(),
            RuntimeValue::List(
                trace
                    .edges
                    .iter()
                    .copied()
                    .map(RuntimeValue::Edge)
                    .collect(),
            ),
        ),
    ])
}

fn expr_contains_aggregate(expr: &Expr) -> bool {
    match expr {
        Expr::FunctionCall { name, .. } if is_aggregate_function(name) => true,
        Expr::Property(base, _) => expr_contains_aggregate(base),
        Expr::Index { target, index } => {
            expr_contains_aggregate(target) || expr_contains_aggregate(index)
        }
        Expr::List(values) => values.iter().any(expr_contains_aggregate),
        Expr::Map(entries) => entries
            .iter()
            .any(|(_, value)| expr_contains_aggregate(value)),
        Expr::Unary { expr, .. } => expr_contains_aggregate(expr),
        Expr::Binary { left, right, .. } => {
            expr_contains_aggregate(left) || expr_contains_aggregate(right)
        }
        Expr::IsNull { expr, .. } => expr_contains_aggregate(expr),
        Expr::FunctionCall { args, .. } => args.iter().any(expr_contains_aggregate),
        _ => false,
    }
}

fn aggregate_expression_is_invalid(expr: &Expr) -> bool {
    match expr {
        Expr::FunctionCall { name, args } if is_aggregate_function(name) => {
            args.iter().any(expr_contains_aggregate)
        }
        _ => expr_contains_aggregate(expr),
    }
}

fn is_aggregate_function(name: &str) -> bool {
    matches!(name, "count" | "sum" | "avg" | "min" | "max" | "collect")
}

fn sum_aggregate<F>(rows: &[Row], mut value_fn: F) -> Result<RuntimeValue, ExecutionError>
where
    F: FnMut(&Row) -> Result<RuntimeValue, ExecutionError>,
{
    let mut int_sum = 0i64;
    let mut float_sum = 0f64;
    let mut kind = None::<&'static str>;
    for row in rows {
        match value_fn(row)? {
            RuntimeValue::Null => {}
            RuntimeValue::Int(value) => match kind {
                None | Some("int") => {
                    int_sum += value;
                    kind = Some("int");
                }
                Some(_) => {
                    return Err(ExecutionError::new(
                        "function_error",
                        "sum() requires numeric values of one type",
                    ));
                }
            },
            RuntimeValue::Float(value) => match kind {
                None | Some("float") => {
                    float_sum += value;
                    kind = Some("float");
                }
                Some(_) => {
                    return Err(ExecutionError::new(
                        "function_error",
                        "sum() requires numeric values of one type",
                    ));
                }
            },
            _ => {
                return Err(ExecutionError::new(
                    "function_error",
                    "sum() requires numeric values",
                ));
            }
        }
    }
    Ok(match kind {
        Some("int") => RuntimeValue::Int(int_sum),
        Some("float") => RuntimeValue::Float(float_sum),
        _ => RuntimeValue::Null,
    })
}

fn avg_aggregate<F>(rows: &[Row], mut value_fn: F) -> Result<RuntimeValue, ExecutionError>
where
    F: FnMut(&Row) -> Result<RuntimeValue, ExecutionError>,
{
    let mut sum = 0f64;
    let mut count = 0usize;
    for row in rows {
        match value_fn(row)? {
            RuntimeValue::Null => {}
            RuntimeValue::Int(value) => {
                sum += value as f64;
                count += 1;
            }
            RuntimeValue::Float(value) => {
                sum += value;
                count += 1;
            }
            _ => {
                return Err(ExecutionError::new(
                    "function_error",
                    "avg() requires numeric values",
                ));
            }
        }
    }
    if count == 0 {
        Ok(RuntimeValue::Null)
    } else {
        Ok(RuntimeValue::Float(sum / count as f64))
    }
}

fn min_max_aggregate<F>(
    rows: &[Row],
    mut value_fn: F,
    min: bool,
) -> Result<RuntimeValue, ExecutionError>
where
    F: FnMut(&Row) -> Result<RuntimeValue, ExecutionError>,
{
    let mut selected = None::<RuntimeValue>;
    for row in rows {
        let value = value_fn(row)?;
        if value == RuntimeValue::Null {
            continue;
        }
        match &selected {
            None => selected = Some(value),
            Some(current)
                if same_runtime_type(&value, current)
                    && ((min && compare_runtime_values(&value, current) == Ordering::Less)
                        || (!min
                            && compare_runtime_values(&value, current) == Ordering::Greater)) =>
            {
                selected = Some(value);
            }
            Some(current) if !same_runtime_type(&value, current) => {
                return Err(ExecutionError::new(
                    "function_error",
                    "min()/max() require comparable values of one type",
                ));
            }
            _ => {}
        }
    }
    Ok(selected.unwrap_or(RuntimeValue::Null))
}

fn show_kind_detail(kind: &ShowKind) -> String {
    match kind {
        ShowKind::Schema => "SCHEMA".to_owned(),
        ShowKind::Indexes(Some(target)) => format!("INDEXES ON {}", target.display_target()),
        ShowKind::Indexes(None) => "INDEXES".to_owned(),
        ShowKind::Constraints(Some(target)) => {
            format!("CONSTRAINTS ON {}", target.display_target())
        }
        ShowKind::Constraints(None) => "CONSTRAINTS".to_owned(),
        ShowKind::Stats => "STATS".to_owned(),
        ShowKind::Transactions => "TRANSACTIONS".to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::fs;
    use std::path::PathBuf;

    use super::{RuntimeValue, Session};
    use crate::engine::Value;

    fn temp_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("{}_{}.cupld", name, std::process::id()))
    }

    #[test]
    fn executes_match_where_return_queries() {
        let mut session = Session::new_in_memory();
        session
            .execute_script(
                "CREATE (a:Person {name: 'Ada'})-[:KNOWS]->(b:Person {name: 'Grace'})",
                &BTreeMap::new(),
            )
            .unwrap();

        let results = session
            .execute_script(
                "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name AS source, b.name AS target",
                &BTreeMap::new(),
            )
            .unwrap();

        assert_eq!(results[0].columns, vec!["source", "target"]);
        assert_eq!(
            results[0].rows,
            vec![vec![
                RuntimeValue::String("Ada".to_owned()),
                RuntimeValue::String("Grace".to_owned())
            ]]
        );
    }

    #[test]
    fn supports_transactions_and_savepoints() {
        let mut session = Session::new_in_memory();
        session.execute_script("BEGIN", &BTreeMap::new()).unwrap();
        session
            .execute_script("CREATE (n:Person {name: 'Ada'})", &BTreeMap::new())
            .unwrap();
        session
            .execute_script("SAVEPOINT before_name", &BTreeMap::new())
            .unwrap();
        session
            .execute_script(
                "MATCH (n:Person) SET n.name = 'Grace' RETURN n.name",
                &BTreeMap::new(),
            )
            .unwrap();
        session
            .execute_script("ROLLBACK TO SAVEPOINT before_name", &BTreeMap::new())
            .unwrap();
        let results = session
            .execute_script("MATCH (n:Person) RETURN n.name", &BTreeMap::new())
            .unwrap();

        assert_eq!(
            results[0].rows,
            vec![vec![RuntimeValue::String("Ada".to_owned())]]
        );
    }

    #[test]
    fn exposes_show_and_explain_results() {
        let mut session = Session::new_in_memory();
        session
            .execute_script("CREATE LABEL Person", &BTreeMap::new())
            .unwrap();

        let show = session
            .execute_script("SHOW SCHEMA", &BTreeMap::new())
            .unwrap();
        let explain = session
            .execute_script(
                "EXPLAIN MATCH (n:Person) RETURN n.name ORDER BY n.name DESC LIMIT 2",
                &BTreeMap::new(),
            )
            .unwrap();

        assert_eq!(show[0].columns, vec!["kind", "name", "description", "ddl"]);
        assert_eq!(
            explain[0].columns,
            vec!["id", "parent_id", "operator", "detail"]
        );
        assert!(explain[0].rows.len() >= 3);
    }

    #[test]
    fn uses_named_parameters() {
        let mut session = Session::new_in_memory();
        let mut params = BTreeMap::new();
        params.insert("name".to_owned(), Value::from("Ada"));

        session
            .execute_script("CREATE (n:Person {name: $name})", &params)
            .unwrap();
        let result = session
            .execute_script("MATCH (n:Person {name: $name}) RETURN n.name", &params)
            .unwrap();

        assert_eq!(
            result[0].rows,
            vec![vec![RuntimeValue::String("Ada".to_owned())]]
        );
    }

    #[test]
    fn save_as_and_open_round_trip() {
        let path = temp_path("cupld_runtime_round_trip");
        let mut session = Session::new_in_memory();
        session
            .execute_script("CREATE (n:Person {name: 'Ada'})", &BTreeMap::new())
            .unwrap();
        assert!(session.is_dirty());

        session.save_as(&path).unwrap();
        assert!(!session.is_dirty());

        let mut reopened = Session::open(&path).unwrap();
        let result = reopened
            .execute_script("MATCH (n:Person) RETURN n.name", &BTreeMap::new())
            .unwrap();

        assert_eq!(
            result[0].rows,
            vec![vec![RuntimeValue::String("Ada".to_owned())]]
        );

        let _ = fs::remove_file(path);
    }
}
