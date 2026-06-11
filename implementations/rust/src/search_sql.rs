//! SQL compiler that walks the typed search AST and generates PostgreSQL.
//!
//! Two strategies:
//! - **EXISTS** for unit-only queries (direct WHERE clauses with EXISTS subqueries
//!   for relations and associations).
//! - **SET_OPS** for attribute-constrained queries (WITH/CTE + INTERSECT/UNION).

use crate::search_ast::{SearchExpr, SearchItem};

#[derive(Debug)]
pub struct CompiledQuery {
    pub sql: String,
    pub params: Vec<String>,
    pub count_sql: String,
    pub count_params: Vec<String>,
}

pub fn compile(
    expr: &SearchExpr,
    order: &(String, String),
    limit: Option<i64>,
    offset: Option<i64>,
) -> CompiledQuery {
    let leaves = expr.collect_leaves();
    let has_attr = leaves.iter().any(|l| SearchExpr::is_attr_item(l));
    if has_attr {
        compile_set_ops(expr, order, limit, offset)
    } else {
        compile_exists(expr, order, limit, offset)
    }
}

fn compile_exists(
    expr: &SearchExpr,
    order: &(String, String),
    limit: Option<i64>,
    offset: Option<i64>,
) -> CompiledQuery {
    let mut param_idx = 1;
    let mut params: Vec<String> = Vec::new();
    let where_clause = compile_exists_where(expr, &mut param_idx, &mut params);
    let where_sql = if where_clause.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", where_clause)
    };

    let order_field = match order.0.as_str() {
        "created" => "uk.created",
        "modified" => "uv.modified",
        "unitid" => "uk.unitid",
        "status" => "uk.status",
        _ => "uk.created",
    };
    let order_dir = if order.1.eq_ignore_ascii_case("asc") {
        "ASC"
    } else {
        "DESC"
    };
    let order_sql = format!(" ORDER BY {} {}", order_field, order_dir);

    let (page_sql, page_params) = build_paging(limit, offset, &mut param_idx);

    let base_from = " FROM repo.repo_unit_kernel uk \
                     JOIN repo.repo_unit_version uv \
                       ON (uk.tenantid = uv.tenantid AND uk.unitid = uv.unitid AND uk.lastver = uv.unitver)";

    let data_sql = format!(
        "SELECT uk.tenantid, uk.unitid, uv.unitver, uk.corrid, uk.status, uk.created, uv.modified, uv.unitname{}{}{}{}",
        base_from, where_sql, order_sql, page_sql
    );

    let count_params = params.clone();
    let count_sql = format!("SELECT count(*){}{}", base_from, where_sql);

    let mut all_params = params;
    all_params.extend(page_params);

    CompiledQuery {
        sql: data_sql,
        params: all_params,
        count_sql,
        count_params,
    }
}

fn compile_exists_where(
    expr: &SearchExpr,
    idx: &mut i64,
    params: &mut Vec<String>,
) -> String {
    match expr {
        SearchExpr::And(l, r) => {
            let left = compile_exists_where(l, idx, params);
            let right = compile_exists_where(r, idx, params);
            if left.is_empty() { return right; }
            if right.is_empty() { return left; }
            format!("({} AND {})", left, right)
        }
        SearchExpr::Or(l, r) => {
            let left = compile_exists_where(l, idx, params);
            let right = compile_exists_where(r, idx, params);
            if left.is_empty() { return right; }
            if right.is_empty() { return left; }
            format!("({} OR {})", left, right)
        }
        SearchExpr::Not(inner) => {
            let inner_sql = compile_exists_where(inner, idx, params);
            if inner_sql.is_empty() { return String::new(); }
            format!("NOT ({})", inner_sql)
        }
        SearchExpr::Between(lower, upper) => {
            let (lower_sql, lower_p) = unit_item_to_sql(lower, idx);
            let (upper_sql, upper_p) = unit_item_to_sql(upper, idx);
            params.extend(lower_p);
            params.extend(upper_p);
            if lower_sql.is_empty() || upper_sql.is_empty() {
                String::new()
            } else {
                format!("({} AND {})", lower_sql, upper_sql)
            }
        }
        SearchExpr::Leaf(item) => {
            let (sql, item_params) = unit_item_to_sql(item, idx);
            params.extend(item_params);
            sql
        }
    }
}

fn compile_set_ops(
    expr: &SearchExpr,
    order: &(String, String),
    limit: Option<i64>,
    offset: Option<i64>,
) -> CompiledQuery {
    let leaves = expr.collect_leaves();
    let attr_leaves: Vec<&&SearchItem> =
        leaves.iter().filter(|l| SearchExpr::is_attr_item(l)).collect();

    let mut param_idx = 1;

    let mut unit_params: Vec<String> = Vec::new();
    let unit_where = compile_exists_where(expr, &mut param_idx, &mut unit_params);

    let mut cte_defs: Vec<String> = Vec::new();
    let mut cte_params: Vec<String> = Vec::new();
    let mut cte_names: Vec<(String, &&SearchItem)> = Vec::new();

    for (i, item) in attr_leaves.iter().enumerate() {
        let cte_name = format!("c{}", i + 1);
        let (cte_sql, item_params) = attr_item_to_cte(item, &cte_name, &mut param_idx);
        cte_defs.push(cte_sql);
        cte_params.extend(item_params);
        cte_names.push((cte_name, *item));
    }

    let attr_logic = build_set_ops_logic(expr, &cte_names);
    let order_sql = build_order_sql(order);
    let (page_sql, page_params) = build_paging(limit, offset, &mut param_idx);

    let cte_block = if cte_defs.is_empty() {
        String::new()
    } else {
        format!("WITH {}\n", cte_defs.join(",\n    "))
    };

    let unit_where_sql = if unit_where.is_empty() {
        String::new()
    } else {
        format!(" AND {}", unit_where)
    };

    let data_sql = format!(
        "{}final AS ({})\n\
         SELECT uk.tenantid, uk.unitid, uv.unitver, uk.corrid, uk.status, uk.created, uv.modified, uv.unitname\n\
         FROM repo.repo_unit_kernel uk\n\
         JOIN repo.repo_unit_version uv ON (uk.tenantid = uv.tenantid AND uk.unitid = uv.unitid AND uk.lastver = uv.unitver)\n\
         JOIN final f ON (uk.tenantid = f.tenantid AND uk.unitid = f.unitid)\n\
         WHERE 1=1{}{}{}",
        cte_block,
        attr_logic,
        unit_where_sql,
        order_sql,
        page_sql,
    );

    let count_sql = format!(
        "{}final AS ({})\n\
         SELECT count(*)\n\
         FROM repo.repo_unit_kernel uk\n\
         JOIN repo.repo_unit_version uv ON (uk.tenantid = uv.tenantid AND uk.unitid = uv.unitid AND uk.lastver = uv.unitver)\n\
         JOIN final f ON (uk.tenantid = f.tenantid AND uk.unitid = f.unitid)\n\
         WHERE 1=1{}",
        cte_block,
        attr_logic,
        unit_where_sql,
    );

    let mut count_params: Vec<String> = Vec::new();
    count_params.extend(unit_params.clone());
    count_params.extend(cte_params.clone());

    let mut all_params: Vec<String> = Vec::new();
    all_params.extend(unit_params);
    all_params.extend(cte_params);
    all_params.extend(page_params);

    CompiledQuery {
        sql: data_sql,
        params: all_params,
        count_sql,
        count_params,
    }
}

fn unit_item_to_sql(item: &SearchItem, idx: &mut i64) -> (String, Vec<String>) {
    let mut next = || {
        let i = *idx;
        *idx += 1;
        i
    };

    match item {
        SearchItem::Unit {
            column,
            operator,
            value,
        } => {
            let col_sql = match column.as_str() {
                "tenantid" => "uk.tenantid",
                "unitid" => "uk.unitid",
                "unitver" => "uk.unitver",
                "status" => "uk.status",
                "corrid" => "uk.corrid",
                "unitname" | "name" => "uv.unitname",
                "created" => "uk.created",
                "modified" => "uv.modified",
                _ => return (String::new(), vec![]),
            };
            let val = value_to_string(value);
            let i = next();
            (
                format!("{} {} ${}", col_sql, operator.to_sql(), i),
                vec![val],
            )
        }
        SearchItem::Rel {
            direction,
            rel_type,
            tenant_id,
            unit_id,
        } => {
            let i1 = next();
            let i2 = next();
            let i3 = next();
            match direction {
                crate::search_ast::Direction::Left => (
                    format!(
                        "EXISTS (SELECT 1 FROM repo.repo_internal_relation r \
                         WHERE uk.tenantid = r.tenantid AND uk.unitid = r.unitid \
                         AND r.reltype = ${} AND r.reltenantid = ${} AND r.relunitid = ${})",
                        i1, i2, i3
                    ),
                    vec![
                        rel_type.to_string(),
                        tenant_id.to_string(),
                        unit_id.to_string(),
                    ],
                ),
                crate::search_ast::Direction::Right => (
                    format!(
                        "EXISTS (SELECT 1 FROM repo.repo_internal_relation r \
                         WHERE uk.tenantid = r.reltenantid AND uk.unitid = r.relunitid \
                         AND r.reltype = ${} AND r.tenantid = ${} AND r.unitid = ${})",
                        i1, i2, i3
                    ),
                    vec![
                        rel_type.to_string(),
                        tenant_id.to_string(),
                        unit_id.to_string(),
                    ],
                ),
            }
        }
        SearchItem::Assoc {
            direction,
            assoc_type,
            ref_string,
        } => {
            let i1 = next();
            let i2 = next();
            match direction {
                crate::search_ast::Direction::Left => (
                    format!(
                        "EXISTS (SELECT 1 FROM repo.repo_external_assoc a \
                         WHERE uk.tenantid = a.tenantid AND uk.unitid = a.unitid \
                         AND a.assoctype = ${} AND a.assocstring = ${})",
                        i1, i2
                    ),
                    vec![assoc_type.to_string(), ref_string.clone()],
                ),
                crate::search_ast::Direction::Right => (
                    format!(
                        "EXISTS (SELECT 1 FROM repo.repo_external_assoc a \
                         WHERE a.assoctype = ${} AND a.assocstring = ${} \
                         AND uk.tenantid = a.tenantid AND uk.unitid = a.unitid)",
                        i1, i2
                    ),
                    vec![assoc_type.to_string(), ref_string.clone()],
                ),
            }
        }
        _ => (String::new(), vec![]),
    }
}

fn attr_item_to_cte(
    item: &SearchItem,
    cte_name: &str,
    idx: &mut i64,
) -> (String, Vec<String>) {
    if let SearchItem::Attr {
        attr_id,
        attr_type,
        operator,
        value,
        ..
    } = item
    {
        let attr_id = match attr_id {
            Some(id) => *id,
            None => return (String::new(), vec![]),
        };
        let table = attr_type.to_vector_table();
        let col = attr_type.to_value_column();

        let i1 = *idx;
        *idx += 1;
        let i2 = *idx;
        *idx += 1;

        let sql = format!(
            "{} AS (\n    SELECT av.tenantid, av.unitid\n    FROM repo.repo_attribute_value av\n    JOIN {} vv ON av.valueid = vv.valueid\n    WHERE av.attrid = ${} AND {} {} ${}\n)",
            cte_name, table, i1, col, operator.to_sql(), i2
        );
        (sql, vec![attr_id.to_string(), value_to_string(value)])
    } else {
        (String::new(), vec![])
    }
}

fn build_set_ops_logic(
    expr: &SearchExpr,
    cte_names: &[(String, &&SearchItem)],
) -> String {
    match expr {
        SearchExpr::And(l, r) => {
            format!(
                "({} INTERSECT {})",
                build_set_ops_logic(l, cte_names),
                build_set_ops_logic(r, cte_names)
            )
        }
        SearchExpr::Or(l, r) => {
            format!(
                "({} UNION {})",
                build_set_ops_logic(l, cte_names),
                build_set_ops_logic(r, cte_names)
            )
        }
        SearchExpr::Not(inner) => {
            format!(
                "(SELECT tenantid, unitid FROM repo.repo_unit_kernel EXCEPT {})",
                build_set_ops_logic(inner, cte_names)
            )
        }
        SearchExpr::Between(lower, upper) => {
            let (lower_cte, upper_cte) = (
                cte_name_for(cte_names, lower),
                cte_name_for(cte_names, upper),
            );
            match (lower_cte, upper_cte) {
                (Some(l), Some(u)) => {
                    format!("(SELECT tenantid, unitid FROM {}) INTERSECT (SELECT tenantid, unitid FROM {})", l, u)
                }
                _ => "(SELECT tenantid, unitid FROM repo.repo_unit_kernel)".to_string(),
            }
        }
        SearchExpr::Leaf(item) => {
            if SearchExpr::is_unit_like(item) {
                "(SELECT tenantid, unitid FROM repo.repo_unit_kernel)".to_string()
            } else if let Some(cte) = cte_name_for(cte_names, item) {
                format!("(SELECT tenantid, unitid FROM {})", cte)
            } else {
                "(SELECT tenantid, unitid FROM repo.repo_unit_kernel)".to_string()
            }
        }
    }
}

fn cte_name_for<'a>(
    cte_names: &'a [(String, &&SearchItem)],
    item: &SearchItem,
) -> Option<&'a str> {
    cte_names
        .iter()
        .find(|(_, i)| ***i == *item)
        .map(|(name, _)| name.as_str())
}

fn build_order_sql(order: &(String, String)) -> String {
    let field = match order.0.as_str() {
        "created" => "uk.created",
        "modified" => "uv.modified",
        "unitid" => "uk.unitid",
        "status" => "uk.status",
        _ => "uk.created",
    };
    let dir = if order.1.eq_ignore_ascii_case("asc") {
        "ASC"
    } else {
        "DESC"
    };
    format!(" ORDER BY {} {}", field, dir)
}

fn build_paging(
    limit: Option<i64>,
    offset: Option<i64>,
    idx: &mut i64,
) -> (String, Vec<String>) {
    let mut sql = String::new();
    let mut params = Vec::new();
    if let Some(lim) = limit {
        let i = *idx;
        *idx += 1;
        sql.push_str(&format!(" LIMIT ${}", i));
        params.push(lim.to_string());
    }
    if let Some(off) = offset {
        let i = *idx;
        *idx += 1;
        sql.push_str(&format!(" OFFSET ${}", i));
        params.push(off.to_string());
    }
    (sql, params)
}

fn value_to_string(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        _ => value.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::search_ast::{Direction, Operator, SearchExpr, SearchItem};
    use serde_json::json;

    fn leaf_unit(column: &str, op: Operator, value: serde_json::Value) -> SearchExpr {
        SearchExpr::Leaf(SearchItem::Unit {
            column: column.to_string(),
            operator: op,
            value,
        })
    }

    #[test]
    fn compile_simple_unit_eq() {
        let expr = leaf_unit("tenantid", Operator::Eq, json!(42));
        let order = ("created".to_string(), "DESC".to_string());
        let compiled = compile(&expr, &order, Some(10), None);
        assert!(compiled.sql.contains("tenantid = $1"));
        assert!(compiled.sql.contains("LIMIT $2"));
        assert!(!compiled.sql.contains("INTERSECT"));
        assert!(!compiled.sql.contains("UNION"));
    }

    #[test]
    fn compile_multi_unit_where() {
        let expr = SearchExpr::And(
            Box::new(leaf_unit("tenantid", Operator::Eq, json!(1))),
            Box::new(leaf_unit("status", Operator::Eq, json!(30))),
        );
        let order = ("created".to_string(), "DESC".to_string());
        let compiled = compile(&expr, &order, None, None);
        assert!(compiled.sql.contains("$1"));
        assert!(compiled.sql.contains("$2"));
        assert_eq!(compiled.params.len(), 2);
    }

    #[test]
    fn compile_unit_with_paging() {
        let expr = leaf_unit("status", Operator::Eq, json!(30));
        let order = ("created".to_string(), "DESC".to_string());
        let compiled = compile(&expr, &order, Some(5), Some(10));
        assert!(compiled.sql.contains("LIMIT $2"));
        assert!(compiled.sql.contains("OFFSET $3"));
        assert_eq!(compiled.params, vec!["30".to_string(), "5".to_string(), "10".to_string()]);
    }

    #[test]
    fn compile_rel_exists() {
        let expr = SearchExpr::Leaf(SearchItem::Rel {
            direction: Direction::Left,
            rel_type: 5,
            tenant_id: 1,
            unit_id: 42,
        });
        let order = ("created".to_string(), "DESC".to_string());
        let compiled = compile(&expr, &order, Some(10), None);
        assert!(compiled.sql.contains("EXISTS"));
        assert!(compiled.sql.contains("repo_internal_relation"));
        assert_eq!(compiled.params.len(), 4); // 3 rel params + 1 limit
    }

    #[test]
    fn compile_assoc_exists() {
        let expr = SearchExpr::Leaf(SearchItem::Assoc {
            direction: Direction::Right,
            assoc_type: 3,
            ref_string: "ref-123".to_string(),
        });
        let order = ("created".to_string(), "DESC".to_string());
        let compiled = compile(&expr, &order, Some(10), None);
        assert!(compiled.sql.contains("EXISTS"));
        assert!(compiled.sql.contains("repo_external_assoc"));
    }

    #[test]
    fn compile_mixed_triggers_set_ops() {
        let unit = leaf_unit("tenantid", Operator::Eq, json!(1));
        let attr = SearchExpr::Leaf(SearchItem::Attr {
            name: "color".to_string(),
            attr_id: Some(5),
            attr_type: crate::search_ast::AttrType::String,
            operator: Operator::Eq,
            value: json!("red"),
        });
        let expr = SearchExpr::And(Box::new(unit), Box::new(attr));
        let order = ("created".to_string(), "DESC".to_string());
        let compiled = compile(&expr, &order, None, None);
        assert!(compiled.sql.contains("c1 AS"));
        assert!(compiled.sql.contains("INTERSECT"));
        assert!(compiled.sql.contains("tenantid = $1"));
        assert!(compiled.sql.contains("av.attrid = $"));
    }

    #[test]
    fn compile_or_expression() {
        let left = leaf_unit("status", Operator::Eq, json!(10));
        let right = leaf_unit("status", Operator::Eq, json!(30));
        let expr = SearchExpr::Or(Box::new(left), Box::new(right));
        let order = ("created".to_string(), "DESC".to_string());
        let compiled = compile(&expr, &order, None, None);
        assert!(compiled.sql.contains("status = $1"));
        assert!(compiled.sql.contains("status = $2"));
    }

    #[test]
    fn compile_not_expression() {
        let inner = leaf_unit("status", Operator::Eq, json!(40));
        let expr = SearchExpr::Not(Box::new(inner));
        let order = ("created".to_string(), "DESC".to_string());
        let compiled = compile(&expr, &order, None, None);
        assert!(compiled.sql.contains("NOT ("));
        assert!(compiled.sql.contains("status = $1"));
    }

    #[test]
    fn compile_between_unit() {
        let expr = SearchExpr::Between(
            SearchItem::Unit { column: "unitid".to_string(), operator: Operator::Gte, value: json!(100) },
            SearchItem::Unit { column: "unitid".to_string(), operator: Operator::Lte, value: json!(200) },
        );
        let order = ("unitid".to_string(), "ASC".to_string());
        let compiled = compile(&expr, &order, Some(50), None);
        assert!(compiled.sql.contains("unitid >= $1"));
        assert!(compiled.sql.contains("unitid <= $2"));
        assert!(compiled.params.contains(&"100".to_string()));
        assert!(compiled.params.contains(&"200".to_string()));
    }

    #[test]
    fn compile_count_sql_differs_from_data_sql() {
        let expr = leaf_unit("status", Operator::Eq, json!(30));
        let order = ("created".to_string(), "DESC".to_string());
        let compiled = compile(&expr, &order, Some(10), None);
        assert!(compiled.count_sql.contains("count(*)"));
        assert!(compiled.sql.contains("uk.tenantid"));
        assert_ne!(compiled.count_sql, compiled.sql);
        assert_eq!(compiled.count_params.len(), 1); // just status
        assert_eq!(compiled.params.len(), 2); // status + limit
    }
}
