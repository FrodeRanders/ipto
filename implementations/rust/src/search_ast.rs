//! Typed search AST matching Java's SearchExpression hierarchy.
//!
//! Replaces the previous JSON-based intermediate representation. The text
//! query parser produces this AST directly, and backend compilers walk it
//! to generate SQL/Cypher with full type awareness.

use serde_json::Value;

/// Boolean expression tree with typed search items at the leaves.
#[derive(Clone, Debug, PartialEq)]
pub enum SearchExpr {
    And(Box<SearchExpr>, Box<SearchExpr>),
    Or(Box<SearchExpr>, Box<SearchExpr>),
    Not(Box<SearchExpr>),
    Between(SearchItem, SearchItem),
    Leaf(SearchItem),
}

/// Typed search predicate — matches Java's SearchItem<?> hierarchy.
#[derive(Clone, Debug, PartialEq)]
pub enum SearchItem {
    /// Predicate on a unit-level column.
    Unit {
        column: String,
        operator: Operator,
        value: Value,
    },
    /// Predicate on a named attribute value.
    Attr {
        name: String,
        attr_id: Option<i64>,
        attr_type: AttrType,
        operator: Operator,
        value: Value,
    },
    /// Find units on one side of a relation.
    Rel {
        direction: Direction,
        rel_type: i64,
        tenant_id: i64,
        unit_id: i64,
    },
    /// Find units on one side of an external association.
    Assoc {
        direction: Direction,
        assoc_type: i64,
        ref_string: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Operator {
    Eq,
    Ne,
    Gt,
    Gte,
    Lt,
    Lte,
    Like,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AttrType {
    String,
    Integer,
    Long,
    Double,
    Bool,
    Time,
    Record,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Direction {
    Left,
    Right,
}

impl SearchExpr {
    /// Walk all leaves, invoking `f` on each SearchItem.
    pub fn collect_leaves(&self) -> Vec<&SearchItem> {
        let mut leaves = Vec::new();
        self.fold_leaves(&mut leaves);
        leaves
    }

    fn fold_leaves<'a>(&'a self, acc: &mut Vec<&'a SearchItem>) {
        match self {
            SearchExpr::And(l, r) | SearchExpr::Or(l, r) => {
                l.fold_leaves(acc);
                r.fold_leaves(acc);
            }
            SearchExpr::Not(inner) => inner.fold_leaves(acc),
            SearchExpr::Between(lower, upper) => {
                acc.push(lower);
                acc.push(upper);
            }
            SearchExpr::Leaf(item) => acc.push(item),
        }
    }

    pub fn is_unit_item(item: &SearchItem) -> bool {
        matches!(item, SearchItem::Unit { .. })
    }

    pub fn is_attr_item(item: &SearchItem) -> bool {
        matches!(item, SearchItem::Attr { .. })
    }

    pub fn is_rel_item(item: &SearchItem) -> bool {
        matches!(item, SearchItem::Rel { .. })
    }

    pub fn is_assoc_item(item: &SearchItem) -> bool {
        matches!(item, SearchItem::Assoc { .. })
    }

    pub fn is_unit_like(item: &SearchItem) -> bool {
        Self::is_unit_item(item) || Self::is_rel_item(item) || Self::is_assoc_item(item)
    }
}

impl Operator {
    pub fn to_sql(&self) -> &'static str {
        match self {
            Operator::Eq => "=",
            Operator::Ne => "<>",
            Operator::Gt => ">",
            Operator::Gte => ">=",
            Operator::Lt => "<",
            Operator::Lte => "<=",
            Operator::Like => "ILIKE",
        }
    }

    pub fn to_cypher(&self) -> &'static str {
        match self {
            Operator::Eq => "=",
            Operator::Ne => "<>",
            Operator::Gt => ">",
            Operator::Gte => ">=",
            Operator::Lt => "<",
            Operator::Lte => "<=",
            Operator::Like => "=~",
        }
    }
}

impl std::fmt::Display for Operator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Operator::Eq => write!(f, "eq"),
            Operator::Ne => write!(f, "ne"),
            Operator::Gt => write!(f, "gt"),
            Operator::Gte => write!(f, "gte"),
            Operator::Lt => write!(f, "lt"),
            Operator::Lte => write!(f, "lte"),
            Operator::Like => write!(f, "like"),
        }
    }
}

impl AttrType {
    pub fn to_vector_table(&self) -> &'static str {
        match self {
            AttrType::String => "repo_string_vector",
            AttrType::Integer => "repo_integer_vector",
            AttrType::Long => "repo_integer_vector",
            AttrType::Double => "repo_float_vector",
            AttrType::Bool => "repo_boolean_vector",
            AttrType::Time => "repo_time_vector",
            AttrType::Record => "repo_record_vector",
        }
    }

    pub fn to_value_column(&self) -> &'static str {
        match self {
            AttrType::String => "vv.vecthvalue",
            AttrType::Integer => "vv.vectivalue",
            AttrType::Long => "vv.vectlvalue",
            AttrType::Double => "vv.vectdvalue",
            AttrType::Bool => "vv.vectbvalue",
            AttrType::Time => "vv.vecttvalue",
            AttrType::Record => "vv.vectrvalue",
        }
    }
}
