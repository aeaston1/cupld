use std::fmt;

use crate::engine::{PropertyType, SchemaTarget};

#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, PartialEq)]
pub enum Statement {
    Begin,
    Commit,
    Rollback,
    Savepoint(String),
    RollbackToSavepoint(String),
    ReleaseSavepoint(String),
    CreateLabel {
        name: String,
        if_not_exists: bool,
    },
    DropLabel {
        name: String,
        if_exists: bool,
    },
    CreateEdgeType {
        name: String,
        if_not_exists: bool,
    },
    DropEdgeType {
        name: String,
        if_exists: bool,
    },
    CreateIndex {
        name: Option<String>,
        target: SchemaTarget,
        property: String,
        if_not_exists: bool,
    },
    DropIndex {
        name: String,
        if_exists: bool,
    },
    CreateConstraint {
        name: Option<String>,
        target: SchemaTarget,
        property: String,
        constraint: ConstraintSpec,
        if_not_exists: bool,
    },
    DropConstraint {
        name: String,
        if_exists: bool,
    },
    Show(ShowKind),
    Explain(Box<Statement>),
    Query(Query),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ShowKind {
    Schema,
    Indexes,
    Constraints,
    Stats,
    Transactions,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConstraintSpec {
    Unique,
    Required,
    Type(PropertyType),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Direction {
    Outgoing,
    Incoming,
    Undirected,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Query {
    pub match_clause: Option<Pattern>,
    pub where_clause: Option<Expr>,
    pub create_clause: Option<Pattern>,
    pub set_clause: Vec<SetAssignment>,
    pub remove_clause: Vec<PropertyTarget>,
    pub delete_clause: Vec<String>,
    pub return_clause: Vec<ReturnItem>,
    pub order_by: Vec<OrderItem>,
    pub limit: Option<usize>,
}

impl Query {
    fn is_empty(&self) -> bool {
        self.match_clause.is_none()
            && self.where_clause.is_none()
            && self.create_clause.is_none()
            && self.set_clause.is_empty()
            && self.remove_clause.is_empty()
            && self.delete_clause.is_empty()
            && self.return_clause.is_empty()
            && self.order_by.is_empty()
            && self.limit.is_none()
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Pattern {
    pub start: NodePattern,
    pub segments: Vec<PatternSegment>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PatternSegment {
    pub direction: Direction,
    pub edge: EdgePattern,
    pub node: NodePattern,
}

#[derive(Clone, Debug, PartialEq)]
pub struct NodePattern {
    pub variable: Option<String>,
    pub labels: Vec<String>,
    pub properties: Vec<(String, Expr)>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct EdgePattern {
    pub variable: Option<String>,
    pub edge_type: Option<String>,
    pub properties: Vec<(String, Expr)>,
    pub hops: Option<HopRange>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HopRange {
    pub min: u8,
    pub max: u8,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SetAssignment {
    pub target: PropertyTarget,
    pub value: Expr,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PropertyTarget {
    pub variable: String,
    pub property: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ReturnItem {
    pub expr: Expr,
    pub alias: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct OrderItem {
    pub expr: Expr,
    pub descending: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Expr {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    Parameter(String),
    Variable(String),
    Property(Box<Expr>, String),
    List(Vec<Expr>),
    Map(Vec<(String, Expr)>),
    Unary {
        op: UnaryOp,
        expr: Box<Expr>,
    },
    Binary {
        left: Box<Expr>,
        op: BinaryOp,
        right: Box<Expr>,
    },
    IsNull {
        expr: Box<Expr>,
        negated: bool,
    },
    FunctionCall {
        name: String,
        args: Vec<Expr>,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UnaryOp {
    Not,
    Negate,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BinaryOp {
    Or,
    And,
    Eq,
    NotEq,
    Lt,
    Lte,
    Gt,
    Gte,
    Add,
    Subtract,
    Multiply,
    Divide,
    In,
    Contains,
    StartsWith,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum QueryErrorKind {
    Character,
    CreateStatement,
    DropStatement,
    ExpectedIdentifier,
    ExpectedKeyword,
    ExpectedToken,
    HopRange,
    HopValue,
    Identifier,
    LimitValue,
    Number,
    Parameter,
    PropertyType,
    ShowKind,
    String,
    UnexpectedEof,
    UnexpectedToken,
}

impl QueryErrorKind {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Character => "parse_character",
            Self::CreateStatement => "parse_create_statement",
            Self::DropStatement => "parse_drop_statement",
            Self::ExpectedIdentifier => "parse_expected_identifier",
            Self::ExpectedKeyword => "parse_expected_keyword",
            Self::ExpectedToken => "parse_expected_token",
            Self::HopRange => "parse_hop_range",
            Self::HopValue => "parse_hop_value",
            Self::Identifier => "parse_identifier",
            Self::LimitValue => "parse_limit_value",
            Self::Number => "parse_number",
            Self::Parameter => "parse_parameter",
            Self::PropertyType => "parse_property_type",
            Self::ShowKind => "parse_show_kind",
            Self::String => "parse_string",
            Self::UnexpectedEof => "parse_unexpected_eof",
            Self::UnexpectedToken => "parse_unexpected_token",
        }
    }
}

impl From<&'static str> for QueryErrorKind {
    fn from(value: &'static str) -> Self {
        match value {
            "parse_character" => Self::Character,
            "parse_create_statement" => Self::CreateStatement,
            "parse_drop_statement" => Self::DropStatement,
            "parse_expected_identifier" => Self::ExpectedIdentifier,
            "parse_expected_keyword" => Self::ExpectedKeyword,
            "parse_expected_token" => Self::ExpectedToken,
            "parse_hop_range" => Self::HopRange,
            "parse_hop_value" => Self::HopValue,
            "parse_identifier" => Self::Identifier,
            "parse_limit_value" => Self::LimitValue,
            "parse_number" => Self::Number,
            "parse_parameter" => Self::Parameter,
            "parse_property_type" => Self::PropertyType,
            "parse_show_kind" => Self::ShowKind,
            "parse_string" => Self::String,
            "parse_unexpected_eof" => Self::UnexpectedEof,
            "parse_unexpected_token" => Self::UnexpectedToken,
            _ => panic!("unknown query error code: {value}"),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueryError {
    kind: QueryErrorKind,
    message: String,
    line: usize,
    column: usize,
}

impl QueryError {
    fn new(
        kind: impl Into<QueryErrorKind>,
        message: impl Into<String>,
        line: usize,
        column: usize,
    ) -> Self {
        Self {
            kind: kind.into(),
            message: message.into(),
            line,
            column,
        }
    }

    pub fn code(&self) -> &'static str {
        self.kind.as_str()
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn line(&self) -> usize {
        self.line
    }

    pub fn column(&self) -> usize {
        self.column
    }
}

impl fmt::Display for QueryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} at line {}, column {}: {}",
            self.code(),
            self.line,
            self.column,
            self.message
        )
    }
}

impl std::error::Error for QueryError {}

#[derive(Clone, Debug, PartialEq)]
struct Token {
    kind: TokenKind,
    line: usize,
    column: usize,
}

#[derive(Clone, Debug, PartialEq)]
enum TokenKind {
    Identifier(String),
    Parameter(String),
    String(String),
    Int(i64),
    Float(f64),
    LParen,
    RParen,
    LBracket,
    RBracket,
    LBrace,
    RBrace,
    Comma,
    Colon,
    Dot,
    Semicolon,
    Star,
    Plus,
    Minus,
    Slash,
    Eq,
    NotEq,
    Lt,
    Lte,
    Gt,
    Gte,
    ArrowRight,
    ArrowLeft,
    Range,
}

pub fn parse_script(input: &str) -> Result<Vec<Statement>, QueryError> {
    let tokens = tokenize(input)?;
    let mut parser = Parser::new(tokens);
    parser.parse_script()
}

struct Parser {
    tokens: Vec<Token>,
    idx: usize,
}

impl Parser {
    fn new(tokens: Vec<Token>) -> Self {
        Self { tokens, idx: 0 }
    }

    fn parse_script(&mut self) -> Result<Vec<Statement>, QueryError> {
        let mut statements = Vec::new();
        while !self.is_eof() {
            if self.consume_semicolon() {
                continue;
            }
            statements.push(self.parse_statement()?);
            self.consume_semicolon();
        }
        Ok(statements)
    }

    fn parse_statement(&mut self) -> Result<Statement, QueryError> {
        if self.consume_keyword("EXPLAIN")? {
            return Ok(Statement::Explain(Box::new(self.parse_statement()?)));
        }
        if self.consume_keyword("BEGIN")? {
            return Ok(Statement::Begin);
        }
        if self.consume_keyword("COMMIT")? {
            return Ok(Statement::Commit);
        }
        if self.consume_keyword("ROLLBACK")? {
            if self.consume_keyword("TO")? {
                self.expect_keyword("SAVEPOINT")?;
                let name = self.expect_identifier()?;
                return Ok(Statement::RollbackToSavepoint(name));
            }
            return Ok(Statement::Rollback);
        }
        if self.consume_keyword("SAVEPOINT")? {
            return Ok(Statement::Savepoint(self.expect_identifier()?));
        }
        if self.consume_keyword("RELEASE")? {
            self.expect_keyword("SAVEPOINT")?;
            return Ok(Statement::ReleaseSavepoint(self.expect_identifier()?));
        }
        if self.consume_keyword("SHOW")? {
            return Ok(Statement::Show(self.parse_show_kind()?));
        }
        if self.peek_keyword("CREATE") && self.peek_create_starts_query() {
            let query = self.parse_query()?;
            return Ok(Statement::Query(query));
        }
        if self.consume_keyword("CREATE")? {
            return self.parse_create_statement();
        }
        if self.consume_keyword("DROP")? {
            return self.parse_drop_statement();
        }

        let query = self.parse_query()?;
        if query.is_empty() {
            let token = self.peek().cloned().unwrap_or(Token {
                kind: TokenKind::Semicolon,
                line: 1,
                column: 1,
            });
            return Err(QueryError::new(
                "parse_unexpected_token",
                "expected a statement",
                token.line,
                token.column,
            ));
        }
        Ok(Statement::Query(query))
    }

    fn parse_show_kind(&mut self) -> Result<ShowKind, QueryError> {
        if self.consume_keyword("SCHEMA")? {
            return Ok(ShowKind::Schema);
        }
        if self.consume_keyword("INDEXES")? {
            return Ok(ShowKind::Indexes);
        }
        if self.consume_keyword("CONSTRAINTS")? {
            return Ok(ShowKind::Constraints);
        }
        if self.consume_keyword("STATS")? {
            return Ok(ShowKind::Stats);
        }
        if self.consume_keyword("TRANSACTIONS")? {
            return Ok(ShowKind::Transactions);
        }
        self.error_here(
            "parse_show_kind",
            "expected SCHEMA, INDEXES, CONSTRAINTS, STATS, or TRANSACTIONS",
        )
    }

    fn peek_create_starts_query(&self) -> bool {
        matches!(
            self.tokens.get(self.idx + 1).map(|token| &token.kind),
            Some(TokenKind::LParen)
        )
    }

    fn parse_create_statement(&mut self) -> Result<Statement, QueryError> {
        let if_not_exists = if self.consume_keyword("IF")? {
            self.expect_keyword("NOT")?;
            self.expect_keyword("EXISTS")?;
            true
        } else {
            false
        };

        if self.consume_keyword("LABEL")? {
            return Ok(Statement::CreateLabel {
                name: self.expect_identifier()?,
                if_not_exists,
            });
        }
        if self.consume_keyword("EDGE")? {
            self.expect_keyword("TYPE")?;
            return Ok(Statement::CreateEdgeType {
                name: self.expect_identifier()?,
                if_not_exists,
            });
        }
        if self.consume_keyword("INDEX")? {
            let name = self.parse_optional_identifier_before("ON")?;
            self.expect_keyword("ON")?;
            let target = self.parse_schema_target()?;
            self.expect_token(TokenDiscriminant::LParen)?;
            let property = self.expect_identifier()?;
            self.expect_token(TokenDiscriminant::RParen)?;
            return Ok(Statement::CreateIndex {
                name,
                target,
                property,
                if_not_exists,
            });
        }
        if self.consume_keyword("CONSTRAINT")? {
            let name = self.parse_optional_identifier_before("ON")?;
            self.expect_keyword("ON")?;
            let target = self.parse_schema_target()?;
            self.expect_keyword("REQUIRE")?;
            let property = self.expect_identifier()?;
            let constraint = if self.consume_keyword("UNIQUE")? {
                ConstraintSpec::Unique
            } else if self.consume_keyword("REQUIRED")? {
                ConstraintSpec::Required
            } else {
                self.expect_keyword("TYPE")?;
                ConstraintSpec::Type(self.parse_property_type()?)
            };
            return Ok(Statement::CreateConstraint {
                name,
                target,
                property,
                constraint,
                if_not_exists,
            });
        }
        self.error_here(
            "parse_create_statement",
            "expected LABEL, EDGE TYPE, INDEX, or CONSTRAINT",
        )
    }

    fn parse_drop_statement(&mut self) -> Result<Statement, QueryError> {
        let if_exists = if self.consume_keyword("IF")? {
            self.expect_keyword("EXISTS")?;
            true
        } else {
            false
        };
        if self.consume_keyword("LABEL")? {
            return Ok(Statement::DropLabel {
                name: self.expect_identifier()?,
                if_exists,
            });
        }
        if self.consume_keyword("EDGE")? {
            self.expect_keyword("TYPE")?;
            return Ok(Statement::DropEdgeType {
                name: self.expect_identifier()?,
                if_exists,
            });
        }
        if self.consume_keyword("INDEX")? {
            return Ok(Statement::DropIndex {
                name: self.expect_identifier()?,
                if_exists,
            });
        }
        if self.consume_keyword("CONSTRAINT")? {
            return Ok(Statement::DropConstraint {
                name: self.expect_identifier()?,
                if_exists,
            });
        }
        self.error_here(
            "parse_drop_statement",
            "expected LABEL, EDGE TYPE, INDEX, or CONSTRAINT",
        )
    }

    fn parse_query(&mut self) -> Result<Query, QueryError> {
        let mut query = Query {
            match_clause: None,
            where_clause: None,
            create_clause: None,
            set_clause: Vec::new(),
            remove_clause: Vec::new(),
            delete_clause: Vec::new(),
            return_clause: Vec::new(),
            order_by: Vec::new(),
            limit: None,
        };

        if self.consume_keyword("MATCH")? {
            query.match_clause = Some(self.parse_pattern()?);
        }
        if self.consume_keyword("WHERE")? {
            query.where_clause = Some(self.parse_expr()?);
        }
        if self.consume_keyword("CREATE")? {
            query.create_clause = Some(self.parse_pattern()?);
        }
        if self.consume_keyword("SET")? {
            query.set_clause = self.parse_set_clause()?;
        }
        if self.consume_keyword("REMOVE")? {
            query.remove_clause = self.parse_remove_clause()?;
        }
        if self.consume_keyword("DELETE")? {
            query.delete_clause = self.parse_delete_clause()?;
        }
        if self.consume_keyword("RETURN")? {
            query.return_clause = self.parse_return_clause()?;
        }
        if self.consume_keyword("ORDER")? {
            self.expect_keyword("BY")?;
            query.order_by = self.parse_order_clause()?;
        }
        if self.consume_keyword("LIMIT")? {
            query.limit = Some(self.expect_limit_value()?);
        }

        Ok(query)
    }

    fn parse_pattern(&mut self) -> Result<Pattern, QueryError> {
        let start = self.parse_node_pattern()?;
        let mut segments = Vec::new();

        loop {
            if self.check(TokenDiscriminant::Minus) {
                self.advance();
                let edge = self.parse_edge_pattern()?;
                let direction = if self.consume_token(TokenDiscriminant::ArrowRight) {
                    Direction::Outgoing
                } else {
                    self.expect_token(TokenDiscriminant::Minus)?;
                    Direction::Undirected
                };
                let node = self.parse_node_pattern()?;
                segments.push(PatternSegment {
                    direction,
                    edge,
                    node,
                });
                continue;
            }
            if self.consume_token(TokenDiscriminant::ArrowLeft) {
                let edge = self.parse_edge_pattern()?;
                self.expect_token(TokenDiscriminant::Minus)?;
                let node = self.parse_node_pattern()?;
                segments.push(PatternSegment {
                    direction: Direction::Incoming,
                    edge,
                    node,
                });
                continue;
            }
            break;
        }

        Ok(Pattern { start, segments })
    }

    fn parse_node_pattern(&mut self) -> Result<NodePattern, QueryError> {
        self.expect_token(TokenDiscriminant::LParen)?;
        let variable = self.try_identifier()?;
        let mut labels = Vec::new();
        while self.consume_token(TokenDiscriminant::Colon) {
            labels.push(self.expect_identifier()?);
        }
        let properties = if self.check(TokenDiscriminant::LBrace) {
            self.parse_property_map()?
        } else {
            Vec::new()
        };
        self.expect_token(TokenDiscriminant::RParen)?;
        Ok(NodePattern {
            variable,
            labels,
            properties,
        })
    }

    fn parse_edge_pattern(&mut self) -> Result<EdgePattern, QueryError> {
        self.expect_token(TokenDiscriminant::LBracket)?;
        let variable = self.try_identifier()?;
        let edge_type = if self.consume_token(TokenDiscriminant::Colon) {
            Some(self.expect_identifier()?)
        } else {
            None
        };
        let hops = if self.consume_token(TokenDiscriminant::Star) {
            let min = self.expect_u8()?;
            self.expect_token(TokenDiscriminant::Range)?;
            let max = self.expect_u8()?;
            if max > 10 || min > max {
                let token = self.previous_or_current();
                return Err(QueryError::new(
                    "parse_hop_range",
                    "bounded traversal must satisfy 0 <= min <= max <= 10",
                    token.line,
                    token.column,
                ));
            }
            Some(HopRange { min, max })
        } else {
            None
        };
        let properties = if self.check(TokenDiscriminant::LBrace) {
            self.parse_property_map()?
        } else {
            Vec::new()
        };
        self.expect_token(TokenDiscriminant::RBracket)?;
        Ok(EdgePattern {
            variable,
            edge_type,
            properties,
            hops,
        })
    }

    fn parse_property_map(&mut self) -> Result<Vec<(String, Expr)>, QueryError> {
        self.expect_token(TokenDiscriminant::LBrace)?;
        let mut entries = Vec::new();
        if self.consume_token(TokenDiscriminant::RBrace) {
            return Ok(entries);
        }
        loop {
            let key = self.expect_identifier()?;
            self.expect_token(TokenDiscriminant::Colon)?;
            let value = self.parse_expr()?;
            entries.push((key, value));
            if self.consume_token(TokenDiscriminant::Comma) {
                if self.consume_token(TokenDiscriminant::RBrace) {
                    break;
                }
                continue;
            }
            self.expect_token(TokenDiscriminant::RBrace)?;
            break;
        }
        Ok(entries)
    }

    fn parse_set_clause(&mut self) -> Result<Vec<SetAssignment>, QueryError> {
        let mut assignments = Vec::new();
        loop {
            assignments.push(SetAssignment {
                target: self.parse_property_target()?,
                value: {
                    self.expect_token(TokenDiscriminant::Eq)?;
                    self.parse_expr()?
                },
            });
            if !self.consume_token(TokenDiscriminant::Comma) {
                break;
            }
        }
        Ok(assignments)
    }

    fn parse_remove_clause(&mut self) -> Result<Vec<PropertyTarget>, QueryError> {
        let mut targets = Vec::new();
        loop {
            targets.push(self.parse_property_target()?);
            if !self.consume_token(TokenDiscriminant::Comma) {
                break;
            }
        }
        Ok(targets)
    }

    fn parse_delete_clause(&mut self) -> Result<Vec<String>, QueryError> {
        let mut variables = Vec::new();
        loop {
            variables.push(self.expect_identifier()?);
            if !self.consume_token(TokenDiscriminant::Comma) {
                break;
            }
        }
        Ok(variables)
    }

    fn parse_return_clause(&mut self) -> Result<Vec<ReturnItem>, QueryError> {
        let mut items = Vec::new();
        loop {
            let expr = self.parse_expr()?;
            let alias = if self.consume_keyword("AS")? {
                Some(self.expect_identifier()?)
            } else {
                None
            };
            items.push(ReturnItem { expr, alias });
            if !self.consume_token(TokenDiscriminant::Comma) {
                break;
            }
        }
        Ok(items)
    }

    fn parse_order_clause(&mut self) -> Result<Vec<OrderItem>, QueryError> {
        let mut items = Vec::new();
        loop {
            let expr = self.parse_expr()?;
            let descending = if self.consume_keyword("DESC")? {
                true
            } else {
                self.consume_keyword("ASC")?;
                false
            };
            items.push(OrderItem { expr, descending });
            if !self.consume_token(TokenDiscriminant::Comma) {
                break;
            }
        }
        Ok(items)
    }

    fn parse_property_target(&mut self) -> Result<PropertyTarget, QueryError> {
        let variable = self.expect_identifier()?;
        self.expect_token(TokenDiscriminant::Dot)?;
        let property = self.expect_identifier()?;
        Ok(PropertyTarget { variable, property })
    }

    fn parse_expr(&mut self) -> Result<Expr, QueryError> {
        self.parse_or_expr()
    }

    fn parse_or_expr(&mut self) -> Result<Expr, QueryError> {
        let mut expr = self.parse_and_expr()?;
        while self.consume_keyword("OR")? {
            expr = Expr::Binary {
                left: Box::new(expr),
                op: BinaryOp::Or,
                right: Box::new(self.parse_and_expr()?),
            };
        }
        Ok(expr)
    }

    fn parse_and_expr(&mut self) -> Result<Expr, QueryError> {
        let mut expr = self.parse_is_expr()?;
        while self.consume_keyword("AND")? {
            expr = Expr::Binary {
                left: Box::new(expr),
                op: BinaryOp::And,
                right: Box::new(self.parse_is_expr()?),
            };
        }
        Ok(expr)
    }

    fn parse_is_expr(&mut self) -> Result<Expr, QueryError> {
        let mut expr = self.parse_comparison_expr()?;
        if self.consume_keyword("IS")? {
            let negated = self.consume_keyword("NOT")?;
            self.expect_keyword("NULL")?;
            expr = Expr::IsNull {
                expr: Box::new(expr),
                negated,
            };
        }
        Ok(expr)
    }

    fn parse_comparison_expr(&mut self) -> Result<Expr, QueryError> {
        let mut expr = self.parse_additive_expr()?;
        loop {
            let op = if self.consume_token(TokenDiscriminant::Eq) {
                Some(BinaryOp::Eq)
            } else if self.consume_token(TokenDiscriminant::NotEq) {
                Some(BinaryOp::NotEq)
            } else if self.consume_token(TokenDiscriminant::Lte) {
                Some(BinaryOp::Lte)
            } else if self.consume_token(TokenDiscriminant::Gte) {
                Some(BinaryOp::Gte)
            } else if self.consume_token(TokenDiscriminant::Lt) {
                Some(BinaryOp::Lt)
            } else if self.consume_token(TokenDiscriminant::Gt) {
                Some(BinaryOp::Gt)
            } else if self.consume_keyword("IN")? {
                Some(BinaryOp::In)
            } else if self.consume_keyword("CONTAINS")? {
                Some(BinaryOp::Contains)
            } else if self.consume_keyword("STARTS")? {
                self.expect_keyword("WITH")?;
                Some(BinaryOp::StartsWith)
            } else {
                None
            };

            let Some(op) = op else {
                break;
            };
            expr = Expr::Binary {
                left: Box::new(expr),
                op,
                right: Box::new(self.parse_additive_expr()?),
            };
        }
        Ok(expr)
    }

    fn parse_additive_expr(&mut self) -> Result<Expr, QueryError> {
        let mut expr = self.parse_multiplicative_expr()?;
        loop {
            let op = if self.consume_token(TokenDiscriminant::Plus) {
                Some(BinaryOp::Add)
            } else if self.consume_token(TokenDiscriminant::Minus) {
                Some(BinaryOp::Subtract)
            } else {
                None
            };
            let Some(op) = op else {
                break;
            };
            expr = Expr::Binary {
                left: Box::new(expr),
                op,
                right: Box::new(self.parse_multiplicative_expr()?),
            };
        }
        Ok(expr)
    }

    fn parse_multiplicative_expr(&mut self) -> Result<Expr, QueryError> {
        let mut expr = self.parse_unary_expr()?;
        loop {
            let op = if self.consume_token(TokenDiscriminant::Star) {
                Some(BinaryOp::Multiply)
            } else if self.consume_token(TokenDiscriminant::Slash) {
                Some(BinaryOp::Divide)
            } else {
                None
            };
            let Some(op) = op else {
                break;
            };
            expr = Expr::Binary {
                left: Box::new(expr),
                op,
                right: Box::new(self.parse_unary_expr()?),
            };
        }
        Ok(expr)
    }

    fn parse_unary_expr(&mut self) -> Result<Expr, QueryError> {
        if self.consume_keyword("NOT")? {
            return Ok(Expr::Unary {
                op: UnaryOp::Not,
                expr: Box::new(self.parse_unary_expr()?),
            });
        }
        if self.consume_token(TokenDiscriminant::Minus) {
            return Ok(Expr::Unary {
                op: UnaryOp::Negate,
                expr: Box::new(self.parse_unary_expr()?),
            });
        }
        self.parse_postfix_expr()
    }

    fn parse_postfix_expr(&mut self) -> Result<Expr, QueryError> {
        let mut expr = self.parse_primary_expr()?;
        while self.consume_token(TokenDiscriminant::Dot) {
            expr = Expr::Property(Box::new(expr), self.expect_identifier()?);
        }
        Ok(expr)
    }

    fn parse_primary_expr(&mut self) -> Result<Expr, QueryError> {
        if self.consume_keyword("null")? {
            return Ok(Expr::Null);
        }
        if self.consume_keyword("true")? {
            return Ok(Expr::Bool(true));
        }
        if self.consume_keyword("false")? {
            return Ok(Expr::Bool(false));
        }
        if let Some(parameter) = self.try_parameter() {
            return Ok(Expr::Parameter(parameter));
        }
        if let Some(value) = self.try_int() {
            return Ok(Expr::Int(value));
        }
        if let Some(value) = self.try_float() {
            return Ok(Expr::Float(value));
        }
        if let Some(value) = self.try_string() {
            return Ok(Expr::String(value));
        }
        if self.consume_token(TokenDiscriminant::LParen) {
            let expr = self.parse_expr()?;
            self.expect_token(TokenDiscriminant::RParen)?;
            return Ok(expr);
        }
        if self.consume_token(TokenDiscriminant::LBracket) {
            return self.parse_list_literal();
        }
        if self.check(TokenDiscriminant::LBrace) {
            return Ok(Expr::Map(self.parse_property_map()?));
        }
        let ident = self.expect_identifier()?;
        if self.consume_token(TokenDiscriminant::LParen) {
            let mut args = Vec::new();
            if !self.consume_token(TokenDiscriminant::RParen) {
                loop {
                    args.push(self.parse_expr()?);
                    if self.consume_token(TokenDiscriminant::Comma) {
                        continue;
                    }
                    self.expect_token(TokenDiscriminant::RParen)?;
                    break;
                }
            }
            return Ok(Expr::FunctionCall { name: ident, args });
        }
        Ok(Expr::Variable(ident))
    }

    fn parse_list_literal(&mut self) -> Result<Expr, QueryError> {
        let mut values = Vec::new();
        if self.consume_token(TokenDiscriminant::RBracket) {
            return Ok(Expr::List(values));
        }
        loop {
            values.push(self.parse_expr()?);
            if self.consume_token(TokenDiscriminant::Comma) {
                if self.consume_token(TokenDiscriminant::RBracket) {
                    break;
                }
                continue;
            }
            self.expect_token(TokenDiscriminant::RBracket)?;
            break;
        }
        Ok(Expr::List(values))
    }

    fn parse_property_type(&mut self) -> Result<PropertyType, QueryError> {
        let token = self.expect_identifier()?;
        match token.as_str() {
            "string" => Ok(PropertyType::String),
            "int" => Ok(PropertyType::Int),
            "float" => Ok(PropertyType::Float),
            "bool" => Ok(PropertyType::Bool),
            "bytes" => Ok(PropertyType::Bytes),
            "datetime" => Ok(PropertyType::Datetime),
            "list" => Ok(PropertyType::List),
            "map" => Ok(PropertyType::Map),
            "null" => Ok(PropertyType::Null),
            _ => self.error_here("parse_property_type", "expected a property type"),
        }
    }

    fn parse_schema_target(&mut self) -> Result<SchemaTarget, QueryError> {
        if self.consume_token(TokenDiscriminant::Colon) {
            return Ok(SchemaTarget::label(self.expect_identifier()?));
        }
        self.expect_token(TokenDiscriminant::LBracket)?;
        self.expect_token(TokenDiscriminant::Colon)?;
        let name = self.expect_identifier()?;
        self.expect_token(TokenDiscriminant::RBracket)?;
        Ok(SchemaTarget::edge_type(name))
    }

    fn parse_optional_identifier_before(
        &mut self,
        sentinel_keyword: &str,
    ) -> Result<Option<String>, QueryError> {
        if self.peek_keyword(sentinel_keyword) {
            Ok(None)
        } else {
            Ok(Some(self.expect_identifier()?))
        }
    }

    fn expect_limit_value(&mut self) -> Result<usize, QueryError> {
        match self.peek().map(|token| &token.kind) {
            Some(TokenKind::Int(value)) if *value > 0 => {
                let value = *value as usize;
                self.advance();
                Ok(value)
            }
            Some(TokenKind::Parameter(_)) => {
                let token = self.advance().unwrap();
                let TokenKind::Parameter(name) = token.kind else {
                    unreachable!();
                };
                name.parse::<usize>().map_err(|_| {
                    QueryError::new(
                        "parse_limit_value",
                        "LIMIT parameters must be numeric names in v1",
                        token.line,
                        token.column,
                    )
                })
            }
            _ => self.error_here("parse_limit_value", "expected a positive integer LIMIT"),
        }
    }

    fn expect_u8(&mut self) -> Result<u8, QueryError> {
        let token = self.peek().cloned().ok_or_else(|| {
            QueryError::new("parse_unexpected_eof", "unexpected end of input", 1, 1)
        })?;
        match token.kind {
            TokenKind::Int(value) if (0..=10).contains(&value) => {
                self.advance();
                Ok(value as u8)
            }
            _ => Err(QueryError::new(
                "parse_hop_value",
                "expected a hop count between 0 and 10",
                token.line,
                token.column,
            )),
        }
    }

    fn expect_identifier(&mut self) -> Result<String, QueryError> {
        let token = self.peek().cloned().ok_or_else(|| {
            QueryError::new("parse_unexpected_eof", "unexpected end of input", 1, 1)
        })?;
        if let TokenKind::Identifier(value) = token.kind {
            self.advance();
            Ok(value)
        } else {
            Err(QueryError::new(
                "parse_expected_identifier",
                "expected an identifier",
                token.line,
                token.column,
            ))
        }
    }

    fn try_identifier(&mut self) -> Result<Option<String>, QueryError> {
        match self.peek().map(|token| &token.kind) {
            Some(TokenKind::Identifier(_)) => self.expect_identifier().map(Some),
            _ => Ok(None),
        }
    }

    fn try_parameter(&mut self) -> Option<String> {
        let token = self.peek()?.clone();
        if let TokenKind::Parameter(name) = token.kind {
            self.advance();
            Some(name)
        } else {
            None
        }
    }

    fn try_int(&mut self) -> Option<i64> {
        let token = self.peek()?.clone();
        if let TokenKind::Int(value) = token.kind {
            self.advance();
            Some(value)
        } else {
            None
        }
    }

    fn try_float(&mut self) -> Option<f64> {
        let token = self.peek()?.clone();
        if let TokenKind::Float(value) = token.kind {
            self.advance();
            Some(value)
        } else {
            None
        }
    }

    fn try_string(&mut self) -> Option<String> {
        let token = self.peek()?.clone();
        if let TokenKind::String(value) = token.kind {
            self.advance();
            Some(value)
        } else {
            None
        }
    }

    fn consume_keyword(&mut self, keyword: &str) -> Result<bool, QueryError> {
        if self.peek_keyword(keyword) {
            self.advance();
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn expect_keyword(&mut self, keyword: &str) -> Result<(), QueryError> {
        if self.consume_keyword(keyword)? {
            Ok(())
        } else {
            self.error_here("parse_expected_keyword", format!("expected {keyword}"))
        }
    }

    fn peek_keyword(&self, keyword: &str) -> bool {
        matches!(
            self.peek().map(|token| &token.kind),
            Some(TokenKind::Identifier(value)) if value == keyword
        )
    }

    fn consume_token(&mut self, expected: TokenDiscriminant) -> bool {
        if self.check(expected) {
            self.advance();
            true
        } else {
            false
        }
    }

    fn consume_semicolon(&mut self) -> bool {
        self.consume_token(TokenDiscriminant::Semicolon)
    }

    fn expect_token(&mut self, expected: TokenDiscriminant) -> Result<(), QueryError> {
        if self.consume_token(expected) {
            Ok(())
        } else {
            self.error_here(
                "parse_expected_token",
                format!("expected {}", expected.label()),
            )
        }
    }

    fn check(&self, expected: TokenDiscriminant) -> bool {
        self.peek()
            .map(|token| expected.matches(&token.kind))
            .unwrap_or(false)
    }

    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.idx)
    }

    fn advance(&mut self) -> Option<Token> {
        let token = self.tokens.get(self.idx).cloned();
        if token.is_some() {
            self.idx += 1;
        }
        token
    }

    fn previous_or_current(&self) -> Token {
        self.tokens
            .get(self.idx.saturating_sub(1))
            .or_else(|| self.tokens.get(self.idx))
            .cloned()
            .unwrap_or(Token {
                kind: TokenKind::Semicolon,
                line: 1,
                column: 1,
            })
    }

    fn error_here<T>(
        &self,
        code: &'static str,
        message: impl Into<String>,
    ) -> Result<T, QueryError> {
        let token = self.peek().cloned().unwrap_or(Token {
            kind: TokenKind::Semicolon,
            line: 1,
            column: 1,
        });
        Err(QueryError::new(code, message, token.line, token.column))
    }

    fn is_eof(&self) -> bool {
        self.idx >= self.tokens.len()
    }
}

#[derive(Clone, Copy)]
enum TokenDiscriminant {
    LParen,
    RParen,
    LBracket,
    RBracket,
    LBrace,
    RBrace,
    Comma,
    Colon,
    Dot,
    Semicolon,
    Star,
    Plus,
    Minus,
    Slash,
    Eq,
    NotEq,
    Lt,
    Lte,
    Gt,
    Gte,
    ArrowRight,
    ArrowLeft,
    Range,
}

impl TokenDiscriminant {
    fn matches(self, token: &TokenKind) -> bool {
        matches!(
            (self, token),
            (Self::LParen, TokenKind::LParen)
                | (Self::RParen, TokenKind::RParen)
                | (Self::LBracket, TokenKind::LBracket)
                | (Self::RBracket, TokenKind::RBracket)
                | (Self::LBrace, TokenKind::LBrace)
                | (Self::RBrace, TokenKind::RBrace)
                | (Self::Comma, TokenKind::Comma)
                | (Self::Colon, TokenKind::Colon)
                | (Self::Dot, TokenKind::Dot)
                | (Self::Semicolon, TokenKind::Semicolon)
                | (Self::Star, TokenKind::Star)
                | (Self::Plus, TokenKind::Plus)
                | (Self::Minus, TokenKind::Minus)
                | (Self::Slash, TokenKind::Slash)
                | (Self::Eq, TokenKind::Eq)
                | (Self::NotEq, TokenKind::NotEq)
                | (Self::Lt, TokenKind::Lt)
                | (Self::Lte, TokenKind::Lte)
                | (Self::Gt, TokenKind::Gt)
                | (Self::Gte, TokenKind::Gte)
                | (Self::ArrowRight, TokenKind::ArrowRight)
                | (Self::ArrowLeft, TokenKind::ArrowLeft)
                | (Self::Range, TokenKind::Range)
        )
    }

    fn label(self) -> &'static str {
        match self {
            Self::LParen => "(",
            Self::RParen => ")",
            Self::LBracket => "[",
            Self::RBracket => "]",
            Self::LBrace => "{",
            Self::RBrace => "}",
            Self::Comma => ",",
            Self::Colon => ":",
            Self::Dot => ".",
            Self::Semicolon => ";",
            Self::Star => "*",
            Self::Plus => "+",
            Self::Minus => "-",
            Self::Slash => "/",
            Self::Eq => "=",
            Self::NotEq => "!=",
            Self::Lt => "<",
            Self::Lte => "<=",
            Self::Gt => ">",
            Self::Gte => ">=",
            Self::ArrowRight => "->",
            Self::ArrowLeft => "<-",
            Self::Range => "..",
        }
    }
}

fn tokenize(input: &str) -> Result<Vec<Token>, QueryError> {
    let bytes = input.as_bytes();
    let mut idx = 0;
    let mut line = 1;
    let mut column = 1;
    let mut tokens = Vec::new();

    while idx < bytes.len() {
        let ch = bytes[idx] as char;
        match ch {
            ' ' | '\t' | '\r' => {
                idx += 1;
                column += 1;
            }
            '\n' => {
                idx += 1;
                line += 1;
                column = 1;
            }
            '-' if idx + 1 < bytes.len() && bytes[idx + 1] as char == '-' => {
                idx += 2;
                column += 2;
                while idx < bytes.len() && bytes[idx] as char != '\n' {
                    idx += 1;
                    column += 1;
                }
            }
            '(' => push_simple(
                &mut tokens,
                TokenKind::LParen,
                line,
                column,
                &mut idx,
                &mut column,
            ),
            ')' => push_simple(
                &mut tokens,
                TokenKind::RParen,
                line,
                column,
                &mut idx,
                &mut column,
            ),
            '[' => push_simple(
                &mut tokens,
                TokenKind::LBracket,
                line,
                column,
                &mut idx,
                &mut column,
            ),
            ']' => push_simple(
                &mut tokens,
                TokenKind::RBracket,
                line,
                column,
                &mut idx,
                &mut column,
            ),
            '{' => push_simple(
                &mut tokens,
                TokenKind::LBrace,
                line,
                column,
                &mut idx,
                &mut column,
            ),
            '}' => push_simple(
                &mut tokens,
                TokenKind::RBrace,
                line,
                column,
                &mut idx,
                &mut column,
            ),
            ',' => push_simple(
                &mut tokens,
                TokenKind::Comma,
                line,
                column,
                &mut idx,
                &mut column,
            ),
            ':' => push_simple(
                &mut tokens,
                TokenKind::Colon,
                line,
                column,
                &mut idx,
                &mut column,
            ),
            ';' => push_simple(
                &mut tokens,
                TokenKind::Semicolon,
                line,
                column,
                &mut idx,
                &mut column,
            ),
            '*' => push_simple(
                &mut tokens,
                TokenKind::Star,
                line,
                column,
                &mut idx,
                &mut column,
            ),
            '+' => push_simple(
                &mut tokens,
                TokenKind::Plus,
                line,
                column,
                &mut idx,
                &mut column,
            ),
            '/' => push_simple(
                &mut tokens,
                TokenKind::Slash,
                line,
                column,
                &mut idx,
                &mut column,
            ),
            '.' if idx + 1 < bytes.len() && bytes[idx + 1] as char == '.' => {
                tokens.push(Token {
                    kind: TokenKind::Range,
                    line,
                    column,
                });
                idx += 2;
                column += 2;
            }
            '.' => push_simple(
                &mut tokens,
                TokenKind::Dot,
                line,
                column,
                &mut idx,
                &mut column,
            ),
            '=' => push_simple(
                &mut tokens,
                TokenKind::Eq,
                line,
                column,
                &mut idx,
                &mut column,
            ),
            '!' if idx + 1 < bytes.len() && bytes[idx + 1] as char == '=' => {
                tokens.push(Token {
                    kind: TokenKind::NotEq,
                    line,
                    column,
                });
                idx += 2;
                column += 2;
            }
            '<' if idx + 1 < bytes.len() && bytes[idx + 1] as char == '=' => {
                tokens.push(Token {
                    kind: TokenKind::Lte,
                    line,
                    column,
                });
                idx += 2;
                column += 2;
            }
            '>' if idx + 1 < bytes.len() && bytes[idx + 1] as char == '=' => {
                tokens.push(Token {
                    kind: TokenKind::Gte,
                    line,
                    column,
                });
                idx += 2;
                column += 2;
            }
            '-' if idx + 1 < bytes.len() && bytes[idx + 1] as char == '>' => {
                tokens.push(Token {
                    kind: TokenKind::ArrowRight,
                    line,
                    column,
                });
                idx += 2;
                column += 2;
            }
            '<' if idx + 1 < bytes.len() && bytes[idx + 1] as char == '-' => {
                tokens.push(Token {
                    kind: TokenKind::ArrowLeft,
                    line,
                    column,
                });
                idx += 2;
                column += 2;
            }
            '-' => push_simple(
                &mut tokens,
                TokenKind::Minus,
                line,
                column,
                &mut idx,
                &mut column,
            ),
            '<' => push_simple(
                &mut tokens,
                TokenKind::Lt,
                line,
                column,
                &mut idx,
                &mut column,
            ),
            '>' => push_simple(
                &mut tokens,
                TokenKind::Gt,
                line,
                column,
                &mut idx,
                &mut column,
            ),
            '$' => {
                let start = idx;
                let start_col = column;
                idx += 1;
                column += 1;
                while idx < bytes.len() {
                    let ch = bytes[idx] as char;
                    if ch.is_ascii_alphanumeric() || ch == '_' {
                        idx += 1;
                        column += 1;
                    } else {
                        break;
                    }
                }
                if idx == start + 1 {
                    return Err(QueryError::new(
                        "parse_parameter",
                        "expected a parameter name",
                        line,
                        start_col,
                    ));
                }
                tokens.push(Token {
                    kind: TokenKind::Parameter(input[start + 1..idx].to_owned()),
                    line,
                    column: start_col,
                });
            }
            '\'' | '"' => {
                let quote = ch;
                let start_col = column;
                idx += 1;
                column += 1;
                let mut value = String::new();
                while idx < bytes.len() {
                    let current = bytes[idx] as char;
                    if current == quote {
                        idx += 1;
                        column += 1;
                        break;
                    }
                    if current == '\\' {
                        idx += 1;
                        column += 1;
                        if idx >= bytes.len() {
                            return Err(QueryError::new(
                                "parse_string",
                                "unterminated escape sequence",
                                line,
                                column,
                            ));
                        }
                        let escaped = bytes[idx] as char;
                        value.push(match escaped {
                            'n' => '\n',
                            'r' => '\r',
                            't' => '\t',
                            '\\' => '\\',
                            '\'' => '\'',
                            '"' => '"',
                            other => other,
                        });
                        idx += 1;
                        column += 1;
                        continue;
                    }
                    if current == '\n' {
                        return Err(QueryError::new(
                            "parse_string",
                            "unterminated string literal",
                            line,
                            start_col,
                        ));
                    }
                    value.push(current);
                    idx += 1;
                    column += 1;
                }
                tokens.push(Token {
                    kind: TokenKind::String(value),
                    line,
                    column: start_col,
                });
            }
            '`' => {
                let start_col = column;
                idx += 1;
                column += 1;
                let start = idx;
                while idx < bytes.len() && bytes[idx] as char != '`' {
                    if bytes[idx] as char == '\n' {
                        return Err(QueryError::new(
                            "parse_identifier",
                            "unterminated quoted identifier",
                            line,
                            start_col,
                        ));
                    }
                    idx += 1;
                    column += 1;
                }
                if idx >= bytes.len() {
                    return Err(QueryError::new(
                        "parse_identifier",
                        "unterminated quoted identifier",
                        line,
                        start_col,
                    ));
                }
                let value = input[start..idx].to_owned();
                idx += 1;
                column += 1;
                tokens.push(Token {
                    kind: TokenKind::Identifier(value),
                    line,
                    column: start_col,
                });
            }
            ch if ch.is_ascii_alphabetic() || ch == '_' => {
                let start = idx;
                let start_col = column;
                idx += 1;
                column += 1;
                while idx < bytes.len() {
                    let current = bytes[idx] as char;
                    if current.is_ascii_alphanumeric() || current == '_' {
                        idx += 1;
                        column += 1;
                    } else {
                        break;
                    }
                }
                tokens.push(Token {
                    kind: TokenKind::Identifier(input[start..idx].to_owned()),
                    line,
                    column: start_col,
                });
            }
            ch if ch.is_ascii_digit() => {
                let start = idx;
                let start_col = column;
                idx += 1;
                column += 1;
                while idx < bytes.len() && (bytes[idx] as char).is_ascii_digit() {
                    idx += 1;
                    column += 1;
                }
                let is_float = idx < bytes.len()
                    && bytes[idx] as char == '.'
                    && idx + 1 < bytes.len()
                    && (bytes[idx + 1] as char).is_ascii_digit();
                if is_float {
                    idx += 1;
                    column += 1;
                    while idx < bytes.len() && (bytes[idx] as char).is_ascii_digit() {
                        idx += 1;
                        column += 1;
                    }
                    let value = input[start..idx].parse::<f64>().map_err(|_| {
                        QueryError::new("parse_number", "invalid float literal", line, start_col)
                    })?;
                    tokens.push(Token {
                        kind: TokenKind::Float(value),
                        line,
                        column: start_col,
                    });
                } else {
                    let value = input[start..idx].parse::<i64>().map_err(|_| {
                        QueryError::new("parse_number", "invalid integer literal", line, start_col)
                    })?;
                    tokens.push(Token {
                        kind: TokenKind::Int(value),
                        line,
                        column: start_col,
                    });
                }
            }
            _ => {
                return Err(QueryError::new(
                    "parse_character",
                    format!("unexpected character {ch:?}"),
                    line,
                    column,
                ));
            }
        }
    }

    Ok(tokens)
}

fn push_simple(
    tokens: &mut Vec<Token>,
    kind: TokenKind,
    line: usize,
    column: usize,
    idx: &mut usize,
    current_column: &mut usize,
) {
    tokens.push(Token { kind, line, column });
    *idx += 1;
    *current_column += 1;
}

#[cfg(test)]
mod tests {
    use super::{ConstraintSpec, Expr, PropertyTarget, Query, ShowKind, Statement, parse_script};
    use crate::engine::{PropertyType, SchemaTarget};

    #[test]
    fn parses_schema_statements_and_show_commands() {
        let statements = parse_script(
            "CREATE LABEL Person;
             CREATE EDGE TYPE KNOWS;
             CREATE INDEX ON :Person(email);
             CREATE CONSTRAINT ON :Person REQUIRE age TYPE int;
             SHOW INDEXES;",
        )
        .unwrap();

        assert_eq!(
            statements,
            vec![
                Statement::CreateLabel {
                    name: "Person".to_owned(),
                    if_not_exists: false,
                },
                Statement::CreateEdgeType {
                    name: "KNOWS".to_owned(),
                    if_not_exists: false,
                },
                Statement::CreateIndex {
                    name: None,
                    target: SchemaTarget::label("Person"),
                    property: "email".to_owned(),
                    if_not_exists: false,
                },
                Statement::CreateConstraint {
                    name: None,
                    target: SchemaTarget::label("Person"),
                    property: "age".to_owned(),
                    constraint: ConstraintSpec::Type(PropertyType::Int),
                    if_not_exists: false,
                },
                Statement::Show(ShowKind::Indexes),
            ]
        );
    }

    #[test]
    fn parses_match_where_return_and_limit() {
        let statements = parse_script(
            "MATCH (n:Person {name: $name})-[:KNOWS*1..3]->(m)
             WHERE n.name = \"Ada\" AND m.name STARTS WITH 'G'
             RETURN n.name AS source, m.name
             ORDER BY m.name DESC
             LIMIT 5",
        )
        .unwrap();

        let Statement::Query(Query {
            match_clause,
            where_clause,
            return_clause,
            order_by,
            limit,
            ..
        }) = &statements[0]
        else {
            panic!("expected query");
        };

        assert!(match_clause.is_some());
        assert!(where_clause.is_some());
        assert_eq!(return_clause.len(), 2);
        assert_eq!(order_by.len(), 1);
        assert_eq!(*limit, Some(5));
    }

    #[test]
    fn parses_write_clauses() {
        let statements = parse_script(
            "MATCH (n:Person)
             CREATE (n)-[:KNOWS {since: 2020}]->(m:Person {name: 'Grace'})
             SET n.role = 'engineer'
             REMOVE n.old_field
             DELETE m",
        )
        .unwrap();

        let Statement::Query(query) = &statements[0] else {
            panic!("expected query");
        };
        assert!(query.match_clause.is_some());
        assert!(query.create_clause.is_some());
        assert_eq!(
            query.set_clause[0].target,
            PropertyTarget {
                variable: "n".to_owned(),
                property: "role".to_owned(),
            }
        );
        assert_eq!(query.remove_clause.len(), 1);
        assert_eq!(query.delete_clause, vec!["m"]);
    }

    #[test]
    fn parser_reports_line_and_column_for_errors() {
        let err = parse_script("MATCH (n {name 'Ada'})").unwrap_err();

        assert_eq!(err.code(), "parse_expected_token");
        assert_eq!(err.line(), 1);
        assert!(err.column() > 1);
    }

    #[test]
    fn supports_comments_and_trailing_commas() {
        let statements = parse_script(
            "-- hello
             MATCH (n {name: 'Ada',}) RETURN [1,2,], {a: 1,} AS payload",
        )
        .unwrap();

        let Statement::Query(query) = &statements[0] else {
            panic!("expected query");
        };
        assert_eq!(query.return_clause.len(), 2);
        assert_eq!(
            query.return_clause[1].expr,
            Expr::Map(vec![("a".to_owned(), Expr::Int(1))])
        );
    }
}
