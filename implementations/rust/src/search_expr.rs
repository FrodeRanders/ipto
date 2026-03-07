use serde_json::{Map, Value};

use crate::backend::{RepoError, RepoResult};

#[derive(Clone)]
pub enum BoolExpr<L> {
    True,
    False,
    Leaf(Box<L>),
    And(Vec<BoolExpr<L>>),
    Or(Vec<BoolExpr<L>>),
    Not(Box<BoolExpr<L>>),
}

pub fn parse_bool_expression<L, FL, FT, FM>(
    expression: &Value,
    parse_leaf: FL,
    infer_tenant: FT,
    tenant_scope_leaf: FM,
    tenant_scope_error: &str,
) -> RepoResult<BoolExpr<L>>
where
    FL: Fn(&Map<String, Value>) -> RepoResult<Option<L>> + Copy,
    FT: Fn(&Map<String, Value>) -> Option<i64> + Copy,
    FM: Fn(i64) -> RepoResult<L> + Copy,
{
    let expr = expression
        .as_object()
        .ok_or_else(|| RepoError::InvalidInput("search expression must be object".to_string()))?;
    parse_bool_expression_object(
        expr,
        parse_leaf,
        infer_tenant,
        tenant_scope_leaf,
        tenant_scope_error,
    )
}

fn parse_bool_expression_object<L, FL, FT, FM>(
    expr: &Map<String, Value>,
    parse_leaf: FL,
    infer_tenant: FT,
    tenant_scope_leaf: FM,
    tenant_scope_error: &str,
) -> RepoResult<BoolExpr<L>>
where
    FL: Fn(&Map<String, Value>) -> RepoResult<Option<L>> + Copy,
    FT: Fn(&Map<String, Value>) -> Option<i64> + Copy,
    FM: Fn(i64) -> RepoResult<L> + Copy,
{
    let has_and = expr.contains_key("and");
    let has_or = expr.contains_key("or");
    let has_not = expr.contains_key("not");

    if has_and && has_or {
        return Err(RepoError::InvalidInput(
            "search expression cannot contain both 'and' and 'or' at same level".to_string(),
        ));
    }
    if (has_and || has_or) && has_not {
        return Err(RepoError::InvalidInput(
            "search expression cannot combine 'not' with 'and'/'or' at same level".to_string(),
        ));
    }

    let leaf = leaf_only_expression(expr);
    let leaf_constraint = parse_leaf(&leaf)?;
    let leaf_expr = leaf_constraint.map(|leaf| BoolExpr::Leaf(Box::new(leaf)));

    if has_and {
        let children = expr
            .get("and")
            .and_then(Value::as_array)
            .ok_or_else(|| RepoError::InvalidInput("search 'and' must be an array".to_string()))?;
        if children.is_empty() {
            return Ok(BoolExpr::False);
        }
        let mut parts = Vec::new();
        if let Some(leaf) = leaf_expr {
            parts.push(leaf);
        }
        for child in children {
            let child_obj = child.as_object().ok_or_else(|| {
                RepoError::InvalidInput("search 'and' children must be objects".to_string())
            })?;
            parts.push(parse_bool_expression_object(
                child_obj,
                parse_leaf,
                infer_tenant,
                tenant_scope_leaf,
                tenant_scope_error,
            )?);
        }
        return Ok(combine_and(parts));
    }

    if has_or {
        let children = expr
            .get("or")
            .and_then(Value::as_array)
            .ok_or_else(|| RepoError::InvalidInput("search 'or' must be an array".to_string()))?;
        if children.is_empty() {
            return Ok(BoolExpr::False);
        }
        let mut parts = Vec::new();
        for child in children {
            let child_obj = child.as_object().ok_or_else(|| {
                RepoError::InvalidInput("search 'or' children must be objects".to_string())
            })?;
            parts.push(parse_bool_expression_object(
                child_obj,
                parse_leaf,
                infer_tenant,
                tenant_scope_leaf,
                tenant_scope_error,
            )?);
        }
        let or_expr = combine_or(parts);
        return Ok(match leaf_expr {
            Some(leaf) => combine_and(vec![leaf, or_expr]),
            None => or_expr,
        });
    }

    if has_not {
        let not_obj = expr
            .get("not")
            .and_then(Value::as_object)
            .ok_or_else(|| RepoError::InvalidInput("search 'not' must be an object".to_string()))?;
        let not_expr = parse_bool_expression_object(
            not_obj,
            parse_leaf,
            infer_tenant,
            tenant_scope_leaf,
            tenant_scope_error,
        )?;
        let negate = BoolExpr::Not(Box::new(not_expr));

        if let Some(leaf) = leaf_expr {
            return Ok(combine_and(vec![leaf, negate]));
        }

        let tenant_id = infer_tenant(not_obj)
            .ok_or_else(|| RepoError::Unsupported(tenant_scope_error.to_string()))?;
        let scoped = tenant_scope_leaf(tenant_id)?;
        return Ok(combine_and(vec![BoolExpr::Leaf(Box::new(scoped)), negate]));
    }

    Ok(leaf_expr.unwrap_or(BoolExpr::True))
}

fn combine_and<L>(parts: Vec<BoolExpr<L>>) -> BoolExpr<L> {
    let mut out = Vec::new();
    for expr in parts {
        match expr {
            BoolExpr::True => {}
            BoolExpr::False => return BoolExpr::False,
            BoolExpr::And(children) => out.extend(children),
            other => out.push(other),
        }
    }
    match out.len() {
        0 => BoolExpr::True,
        1 => out.into_iter().next().unwrap_or(BoolExpr::True),
        _ => BoolExpr::And(out),
    }
}

fn combine_or<L>(parts: Vec<BoolExpr<L>>) -> BoolExpr<L> {
    let mut out = Vec::new();
    for expr in parts {
        match expr {
            BoolExpr::False => {}
            BoolExpr::True => return BoolExpr::True,
            BoolExpr::Or(children) => out.extend(children),
            other => out.push(other),
        }
    }
    match out.len() {
        0 => BoolExpr::False,
        1 => out.into_iter().next().unwrap_or(BoolExpr::False),
        _ => BoolExpr::Or(out),
    }
}

fn leaf_only_expression(expr: &Map<String, Value>) -> Map<String, Value> {
    let mut out = Map::new();
    for (key, value) in expr {
        if key == "and" || key == "or" || key == "not" {
            continue;
        }
        out.insert(key.clone(), value.clone());
    }
    out
}
