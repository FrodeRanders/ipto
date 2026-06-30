//! Cypher compiler that walks the typed search AST and generates Neo4j Cypher.
//!
//! Produces MATCH/WHERE/RETURN/ORDER BY/SKIP/LIMIT clauses with support for
//! unit field constraints, attribute value matterns, relation EXISTS patterns,
//! and external association EXISTS patterns.

use crate::search_ast::{Direction, Operator, SearchExpr, SearchItem};

#[derive(Debug)]
pub struct CompiledCypher {
    pub query: String,
    pub count_query: String,
    pub params: serde_json::Map<String, serde_json::Value>,
}

pub fn compile(
    expr: &SearchExpr,
    order: &(String, String),
    limit: Option<i64>,
    offset: Option<i64>,
) -> CompiledCypher {
    let mut params = serde_json::Map::new();
    let mut param_idx = 0;

    let where_clause = compile_cypher_where(expr, &mut param_idx, &mut params);
    let attr_match = compile_attr_match(expr, &mut param_idx, &mut params);

    let where_sql = if where_clause.is_empty() {
        String::new()
    } else {
        format!(" AND {}", where_clause)
    };

    let match_clause = format!(
        "MATCH (k:UnitKernel)\nMATCH (k)-[:HAS_VERSION]->(v:UnitVersion)\nWHERE v.unitver = k.lastver{}",
        attr_match
    );

    let order_clause = cypher_order(order);
    let (page_clause, _) = cypher_paging(limit, offset);

    let data_query = format!(
        "{}{}{}\nRETURN k.tenantid, k.unitid, v.unitver, k.lastver, k.corrid, k.status, k.created, v.modified, v.unitname, v.payload\n{}{}",
        match_clause, where_sql, "", order_clause, page_clause,
    );

    let count_query = format!("{}{}{}\nRETURN count(*)", match_clause, where_sql, "");

    CompiledCypher {
        query: data_query,
        count_query,
        params,
    }
}

fn compile_cypher_where(
    expr: &SearchExpr,
    idx: &mut i64,
    params: &mut serde_json::Map<String, serde_json::Value>,
) -> String {
    match expr {
        SearchExpr::And(l, r) => {
            let left = compile_cypher_where(l, idx, params);
            let right = compile_cypher_where(r, idx, params);
            if left.is_empty() {
                return right;
            }
            if right.is_empty() {
                return left;
            }
            format!("({} AND {})", left, right)
        }
        SearchExpr::Or(l, r) => {
            let left = compile_cypher_where(l, idx, params);
            let right = compile_cypher_where(r, idx, params);
            if left.is_empty() {
                return right;
            }
            if right.is_empty() {
                return left;
            }
            format!("({} OR {})", left, right)
        }
        SearchExpr::Not(inner) => {
            let inner_sql = compile_cypher_where(inner, idx, params);
            if inner_sql.is_empty() {
                return String::new();
            }
            format!("NOT ({})", inner_sql)
        }
        SearchExpr::Between(lower, upper) => {
            let lower_sql = item_to_cypher_predicate(lower, idx, params);
            let upper_sql = item_to_cypher_predicate(upper, idx, params);
            if lower_sql.is_empty() || upper_sql.is_empty() {
                String::new()
            } else {
                format!("({} AND {})", lower_sql, upper_sql)
            }
        }
        SearchExpr::Leaf(item) => item_to_cypher_predicate(item, idx, params),
    }
}

fn item_to_cypher_predicate(
    item: &SearchItem,
    idx: &mut i64,
    params: &mut serde_json::Map<String, serde_json::Value>,
) -> String {
    match item {
        SearchItem::Unit {
            column,
            operator,
            value,
        } => {
            let col = match column.as_str() {
                "tenantid" => "k.tenantid",
                "unitid" => "k.unitid",
                "unitver" => "v.unitver",
                "status" => "k.status",
                "corrid" => "k.corrid",
                "unitname" | "name" => "v.unitname",
                "created" => "k.created",
                "modified" => "v.modified",
                _ => return String::new(),
            };
            let val = match operator {
                Operator::Like => {
                    serde_json::Value::String(like_to_regex(value.as_str().unwrap_or("")))
                }
                _ => value.clone(),
            };
            let pname = cypher_param(idx, params, &val);
            let op_str = match operator {
                Operator::Like => "=~",
                _ => operator.to_cypher(),
            };
            format!("{} {} {}", col, op_str, pname)
        }
        SearchItem::Rel {
            direction,
            rel_type,
            tenant_id,
            unit_id,
        } => {
            let p_rel = cypher_param(idx, params, &serde_json::json!(rel_type));
            let p_tid = cypher_param(idx, params, &serde_json::json!(tenant_id));
            let p_uid = cypher_param(idx, params, &serde_json::json!(unit_id));
            match direction {
                Direction::Left => {
                    format!(
                        "EXISTS {{ MATCH (k)-[:HAS_RELATION {{reltype: {}}}]->(:UnitKernel {{tenantid: {}, unitid: {}}}) }}",
                        p_rel, p_tid, p_uid
                    )
                }
                Direction::Right => {
                    format!(
                        "EXISTS {{ MATCH (k)<-[:HAS_RELATION {{reltype: {}}}]-(:UnitKernel {{tenantid: {}, unitid: {}}}) }}",
                        p_rel, p_tid, p_uid
                    )
                }
            }
        }
        SearchItem::Assoc {
            direction,
            assoc_type,
            ref_string,
        } => {
            let p_type = cypher_param(idx, params, &serde_json::json!(assoc_type));
            let p_ref = cypher_param(idx, params, &serde_json::json!(ref_string));
            match direction {
                Direction::Left => {
                    format!(
                        "EXISTS {{ MATCH (k)-[:HAS_ASSOC {{assoctype: {}, assocstring: {}}}]->(:ExternalRef) }}",
                        p_type, p_ref
                    )
                }
                Direction::Right => {
                    format!(
                        "EXISTS {{ MATCH (k)<-[:HAS_ASSOC {{assoctype: {}, assocstring: {}}}]-(:ExternalRef) }}",
                        p_type, p_ref
                    )
                }
            }
        }
        SearchItem::Attr { .. } => String::new(),
    }
}

fn compile_attr_match(
    expr: &SearchExpr,
    idx: &mut i64,
    params: &mut serde_json::Map<String, serde_json::Value>,
) -> String {
    let leaves = expr.collect_leaves();
    let attrs: Vec<&&SearchItem> = leaves
        .iter()
        .filter(|l| SearchExpr::is_attr_item(l))
        .collect();
    if attrs.is_empty() {
        return String::new();
    }

    let mut clauses = Vec::new();
    for (i, item) in attrs.iter().enumerate() {
        if let SearchItem::Attr {
            attr_id,
            operator,
            value,
            ..
        } = item
        {
            let var = format!("av{}", i);
            let id = attr_id.unwrap_or(0);
            let p_id = cypher_param(idx, params, &serde_json::json!(id));
            let val = match operator {
                Operator::Like => {
                    serde_json::Value::String(like_to_regex(value.as_str().unwrap_or("")))
                }
                _ => value.clone(),
            };
            let p_val = cypher_param(idx, params, &val);
            let op_str = match operator {
                Operator::Like => "=~",
                _ => operator.to_cypher(),
            };
            let match_line = format!(
                "OPTIONAL MATCH (v)-[:HAS_ATTR_VALUE]->({}:AttributeValue {{attrid: {}}})",
                var, p_id
            );
            let where_line = format!(
                "{} IS NOT NULL AND {}.attrvalue {} {}",
                var, var, op_str, p_val
            );
            clauses.push(format!("{}\nAND {}", match_line, where_line));
        }
    }
    if clauses.is_empty() {
        String::new()
    } else {
        format!("\n{}", clauses.join("\n"))
    }
}

fn cypher_param(
    idx: &mut i64,
    params: &mut serde_json::Map<String, serde_json::Value>,
    value: &serde_json::Value,
) -> String {
    let name = format!("p{}", idx);
    *idx += 1;
    params.insert(name.clone(), value.clone());
    format!("${}", name)
}

fn cypher_order(order: &(String, String)) -> String {
    let field = match order.0.as_str() {
        "created" => "k.created",
        "modified" => "v.modified",
        "unitid" => "k.unitid",
        "status" => "k.status",
        _ => "k.created",
    };
    let dir = if order.1.eq_ignore_ascii_case("asc") {
        "ASC"
    } else {
        "DESC"
    };
    format!("ORDER BY {} {}", field, dir)
}

fn cypher_paging(limit: Option<i64>, offset: Option<i64>) -> (String, Vec<String>) {
    let mut clauses = Vec::new();
    let mut params = Vec::new();
    if let Some(off) = offset {
        clauses.push(format!("SKIP {}", off));
        params.push(off.to_string());
    }
    if let Some(lim) = limit {
        clauses.push(format!("LIMIT {}", lim));
        params.push(lim.to_string());
    }
    (clauses.join("\n"), params)
}

fn like_to_regex(pattern: &str) -> String {
    let mut out = String::from("(?i)");
    for ch in pattern.chars() {
        match ch {
            '%' => out.push_str(".*"),
            '_' => out.push('.'),
            '.' | '+' | '*' | '?' | '^' | '$' | '(' | ')' | '[' | ']' | '{' | '}' | '|' | '\\' => {
                out.push('\\');
                out.push(ch);
            }
            _ => out.push(ch),
        }
    }
    out
}
