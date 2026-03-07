use serde_json::{json, Value};

use crate::backend::{RepoError, RepoResult};

#[derive(Clone, Copy, PartialEq, Eq)]
enum LogicalOp {
    And,
    Or,
}

#[derive(Clone)]
enum ExprNode {
    Leaf(Value),
    And(Box<ExprNode>, Box<ExprNode>),
    Or(Box<ExprNode>, Box<ExprNode>),
    Not(Box<ExprNode>),
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Side {
    Left,
    Right,
}

#[derive(Clone)]
enum Token {
    LParen,
    RParen,
    Comma,
    Operator(String),
    Word(String),
    String(String),
    Number(String),
    Bool(bool),
}

pub(crate) fn parse_search_query(query: &str) -> RepoResult<Value> {
    parse_search_query_with_mode(query, false)
}

pub(crate) fn parse_search_query_strict(query: &str) -> RepoResult<Value> {
    parse_search_query_with_mode(query, true)
}

fn parse_search_query_with_mode(query: &str, strict_fields: bool) -> RepoResult<Value> {
    // Parsing pipeline:
    // 1) tokenize raw query text
    // 2) build AST with operator precedence (NOT > AND > OR)
    // 3) lower AST to JSON expression format used by backends
    let tokens = tokenize(query)?;
    let mut parser = Parser {
        tokens,
        index: 0,
        strict_fields,
    };
    let expr = parser.parse_expression()?;
    if parser.peek().is_some() {
        return Err(RepoError::InvalidInput(
            "unexpected token after query expression".to_string(),
        ));
    }
    Ok(to_json_expr(expr))
}

struct Parser {
    tokens: Vec<Token>,
    index: usize,
    strict_fields: bool,
}

impl Parser {
    fn parse_expression(&mut self) -> RepoResult<ExprNode> {
        self.parse_or()
    }

    fn parse_or(&mut self) -> RepoResult<ExprNode> {
        // Lowest precedence.
        let mut expr = self.parse_and()?;
        while self.consume_keyword("or") {
            let right = self.parse_and()?;
            expr = ExprNode::Or(Box::new(expr), Box::new(right));
        }
        Ok(expr)
    }

    fn parse_and(&mut self) -> RepoResult<ExprNode> {
        // Middle precedence.
        let mut expr = self.parse_not()?;
        while self.consume_keyword("and") {
            let right = self.parse_not()?;
            expr = ExprNode::And(Box::new(expr), Box::new(right));
        }
        Ok(expr)
    }

    fn parse_not(&mut self) -> RepoResult<ExprNode> {
        // Highest logical precedence.
        if self.consume_keyword("not") {
            let inner = self.parse_not()?;
            return Ok(ExprNode::Not(Box::new(inner)));
        }
        self.parse_primary()
    }

    fn parse_primary(&mut self) -> RepoResult<ExprNode> {
        if self.consume_lparen() {
            let inner = self.parse_expression()?;
            self.expect_rparen()?;
            return Ok(inner);
        }
        self.parse_predicate()
    }

    fn parse_predicate(&mut self) -> RepoResult<ExprNode> {
        let field = self.take_word("field")?;
        if self.consume_keyword("between") {
            // Normalize BETWEEN into two comparisons so backend compilers only
            // need to handle primitive operators.
            let lower = self.take_value("lower bound")?;
            if !self.consume_keyword("and") {
                return Err(RepoError::InvalidInput(
                    "BETWEEN requires 'and' between lower and upper bounds".to_string(),
                ));
            }
            let upper = self.take_value("upper bound")?;
            return Ok(ExprNode::Leaf(json!({
                "and": [
                    predicate_json_for_field(&field, ">=", lower, self.strict_fields)?,
                    predicate_json_for_field(&field, "<=", upper, self.strict_fields)?
                ]
            })));
        }

        let mut negate_in = false;
        if self.consume_keyword("not") {
            negate_in = true;
        }
        if self.consume_keyword("in") {
            let values = self.take_value_list("IN list")?;
            if values.is_empty() {
                return Err(RepoError::InvalidInput(
                    "IN list must contain at least one value".to_string(),
                ));
            }
            let mut parts = Vec::with_capacity(values.len());
            for value in values {
                parts.push(predicate_json_for_field(
                    &field,
                    if negate_in { "!=" } else { "=" },
                    value,
                    self.strict_fields,
                )?);
            }
            // Normalize IN/NOT IN into OR/AND lists of simple predicates.
            return Ok(ExprNode::Leaf(if negate_in {
                json!({ "and": parts })
            } else {
                json!({ "or": parts })
            }));
        }
        if negate_in {
            return Err(RepoError::InvalidInput(
                "expected IN after NOT in predicate".to_string(),
            ));
        }

        let operator = self.take_operator()?;
        let value = self.take_value("value")?;
        Ok(ExprNode::Leaf(predicate_json_for_field(
            &field,
            &operator,
            value,
            self.strict_fields,
        )?))
    }

    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.index)
    }

    fn next(&mut self) -> Option<Token> {
        let out = self.tokens.get(self.index).cloned();
        if out.is_some() {
            self.index += 1;
        }
        out
    }

    fn consume_lparen(&mut self) -> bool {
        matches!(self.peek(), Some(Token::LParen)) && self.next().is_some()
    }

    fn consume_comma(&mut self) -> bool {
        matches!(self.peek(), Some(Token::Comma)) && self.next().is_some()
    }

    fn expect_rparen(&mut self) -> RepoResult<()> {
        match self.next() {
            Some(Token::RParen) => Ok(()),
            _ => Err(RepoError::InvalidInput("expected ')'".to_string())),
        }
    }

    fn consume_keyword(&mut self, expected: &str) -> bool {
        match self.peek() {
            Some(Token::Word(word)) if word.eq_ignore_ascii_case(expected) => {
                self.next();
                true
            }
            _ => false,
        }
    }

    fn take_word(&mut self, label: &str) -> RepoResult<String> {
        match self.next() {
            Some(Token::Word(word)) => Ok(word),
            _ => Err(RepoError::InvalidInput(format!("expected {label}"))),
        }
    }

    fn take_operator(&mut self) -> RepoResult<String> {
        match self.next() {
            Some(Token::Operator(op)) => Ok(op),
            Some(Token::Word(word)) if word.eq_ignore_ascii_case("like") => Ok("like".to_string()),
            _ => Err(RepoError::InvalidInput("expected operator".to_string())),
        }
    }

    fn take_value(&mut self, label: &str) -> RepoResult<Value> {
        match self.next() {
            Some(Token::String(text)) => Ok(Value::String(text)),
            Some(Token::Bool(value)) => Ok(Value::Bool(value)),
            Some(Token::Number(raw)) => {
                if raw.contains('.') {
                    let parsed = raw.parse::<f64>().map_err(|_| {
                        RepoError::InvalidInput(format!("invalid numeric {label}: {raw}"))
                    })?;
                    Ok(json!(parsed))
                } else {
                    let parsed = raw.parse::<i64>().map_err(|_| {
                        RepoError::InvalidInput(format!("invalid integer {label}: {raw}"))
                    })?;
                    Ok(json!(parsed))
                }
            }
            Some(Token::Word(word)) => Ok(Value::String(word)),
            _ => Err(RepoError::InvalidInput(format!("expected {label}"))),
        }
    }

    fn take_value_list(&mut self, label: &str) -> RepoResult<Vec<Value>> {
        if !self.consume_lparen() {
            return Err(RepoError::InvalidInput(format!(
                "expected '(' to start {label}"
            )));
        }
        let mut values = Vec::new();
        if matches!(self.peek(), Some(Token::RParen)) {
            self.expect_rparen()?;
            return Ok(values);
        }
        loop {
            values.push(self.take_value("value")?);
            if self.consume_comma() {
                continue;
            }
            self.expect_rparen()?;
            break;
        }
        Ok(values)
    }
}

fn predicate_json_for_field(
    field: &str,
    operator: &str,
    value: Value,
    strict_fields: bool,
) -> RepoResult<Value> {
    if let Some(spec) = parse_relation_field(field)? {
        if operator != "=" {
            return Err(RepoError::InvalidInput(
                "only '=' is supported for relation constraints".to_string(),
            ));
        }
        let rel_type = resolve_relation_type_token(&spec.type_token)?;
        let unit = value_to_unit_ref(&value)?;
        let mut rel = json!({
            "type": rel_type,
            "unit": unit
        });
        if let Some(side) = spec.side {
            rel["side"] = Value::String(match side {
                Side::Left => "left".to_string(),
                Side::Right => "right".to_string(),
            });
        }
        return Ok(json!({ "relation": rel }));
    }

    if let Some(spec) = parse_association_field(field)? {
        if operator != "=" {
            return Err(RepoError::InvalidInput(
                "only '=' is supported for association constraints".to_string(),
            ));
        }
        let assoc_type = resolve_association_type_token(&spec.type_token)?;
        let mut assoc = json!({
            "type": assoc_type,
            "reference": value_to_stringish(value),
        });
        if let Some(side) = spec.side {
            assoc["side"] = Value::String(match side {
                Side::Left => "left".to_string(),
                Side::Right => "right".to_string(),
            });
        }
        return Ok(json!({ "association": assoc }));
    }

    if let Some(unit_field) = normalize_unit_field(field) {
        let mut op = normalize_operator(operator);
        if op == "eq" && is_string_unit_field(&unit_field) && value_has_wildcard_pattern(&value) {
            op = "like".to_string();
        }
        let coerced = coerce_unit_field_value(&unit_field, value)?;
        return Ok(json!({
            "predicates": [{
                "field": unit_field,
                "op": op,
                "value": coerced
            }]
        }));
    }

    let attribute_field = if let Some(attr_name) = parse_attribute_field(field) {
        attr_name
    } else if strict_fields {
        return Err(RepoError::InvalidInput(format!(
            "unknown search field '{field}' in strict mode (use attr:<name> for attributes)"
        )));
    } else {
        field.to_string()
    };

    let attribute_op = normalize_operator(operator);
    Ok(json!({
        "attribute_cmp": {
            "name_or_id": attribute_field,
            "op": attribute_op,
            "value": value
        }
    }))
}

fn to_json_expr(expr: ExprNode) -> Value {
    match expr {
        ExprNode::Leaf(v) => v,
        ExprNode::Not(inner) => json!({ "not": to_json_expr(*inner) }),
        ExprNode::And(left, right) => {
            let mut nodes = Vec::new();
            flatten_logical(&mut nodes, LogicalOp::And, *left);
            flatten_logical(&mut nodes, LogicalOp::And, *right);
            json!({ "and": nodes })
        }
        ExprNode::Or(left, right) => {
            let mut nodes = Vec::new();
            flatten_logical(&mut nodes, LogicalOp::Or, *left);
            flatten_logical(&mut nodes, LogicalOp::Or, *right);
            json!({ "or": nodes })
        }
    }
}

fn flatten_logical(out: &mut Vec<Value>, target: LogicalOp, node: ExprNode) {
    match (target, node) {
        (LogicalOp::And, ExprNode::And(left, right)) => {
            flatten_logical(out, target, *left);
            flatten_logical(out, target, *right);
        }
        (LogicalOp::Or, ExprNode::Or(left, right)) => {
            flatten_logical(out, target, *left);
            flatten_logical(out, target, *right);
        }
        (_, other) => out.push(to_json_expr(other)),
    }
}

fn normalize_operator(op: &str) -> String {
    match op.to_ascii_lowercase().as_str() {
        "=" => "eq".to_string(),
        "!=" => "neq".to_string(),
        "<>" => "neq".to_string(),
        ">" => "gt".to_string(),
        ">=" => "gte".to_string(),
        "<" => "lt".to_string(),
        "<=" => "lte".to_string(),
        "like" => "like".to_string(),
        other => other.to_string(),
    }
}

fn normalize_unit_field(field: &str) -> Option<String> {
    let lower = field.to_ascii_lowercase();
    let normalized = match lower.as_str() {
        "tenantid" | "tenant_id" => "tenantid",
        "unitid" | "unit_id" => "unitid",
        "unitver" | "unit_ver" => "unitver",
        "status" => "status",
        "name" | "unitname" | "unit_name" => "name",
        "corrid" | "correlationid" | "corr_id" => "corrid",
        "created" => "created",
        "modified" => "modified",
        _ => return None,
    };
    Some(normalized.to_string())
}

fn is_string_unit_field(field: &str) -> bool {
    matches!(field, "name" | "corrid")
}

fn value_has_wildcard_pattern(value: &Value) -> bool {
    value
        .as_str()
        .map(|s| s.contains('*') || s.contains('%') || s.contains('_'))
        .unwrap_or(false)
}

fn coerce_unit_field_value(field: &str, value: Value) -> RepoResult<Value> {
    if field != "status" {
        return Ok(value);
    }
    if let Some(i) = value.as_i64() {
        return Ok(json!(i));
    }
    let raw = value.as_str().ok_or_else(|| {
        RepoError::InvalidInput(
            "status search value must be numeric or known status name".to_string(),
        )
    })?;
    parse_status_text(raw).map(|status| json!(status))
}

fn parse_status_text(raw: &str) -> RepoResult<i64> {
    if let Ok(parsed) = raw.parse::<i64>() {
        return Ok(parsed);
    }
    let mut normalized = raw.trim().to_ascii_uppercase().replace(['-', ' '], "_");
    if let Some(stripped) = normalized.strip_prefix("STATUS_") {
        normalized = stripped.to_string();
    }
    match normalized.as_str() {
        "PENDING_DISPOSITION" => Ok(1),
        "PENDING_DELETION" => Ok(10),
        "OBLITERATED" => Ok(20),
        "EFFECTIVE" => Ok(30),
        "ARCHIVED" => Ok(40),
        _ => Err(RepoError::InvalidInput(format!(
            "unknown status: \"{raw}\""
        ))),
    }
}

fn parse_attribute_field(field: &str) -> Option<String> {
    let (prefix, local) = field.split_once(':')?;
    let lower = prefix.to_ascii_lowercase();
    if lower == "attr" || lower == "attribute" {
        let trimmed = local.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    } else {
        None
    }
}

struct ParsedRelationField {
    type_token: String,
    side: Option<Side>,
}

fn parse_relation_field(field: &str) -> RepoResult<Option<ParsedRelationField>> {
    parse_prefixed_field(
        field,
        &["relation", "relations", "rel"],
        "relation type is missing in field",
    )
}

fn parse_association_field(field: &str) -> RepoResult<Option<ParsedRelationField>> {
    parse_prefixed_field(
        field,
        &["association", "associations", "assoc"],
        "association type is missing in field",
    )
}

fn parse_prefixed_field(
    field: &str,
    bases: &[&str],
    missing_msg: &str,
) -> RepoResult<Option<ParsedRelationField>> {
    let Some((prefix_raw, local_raw)) = field.split_once(':') else {
        return Ok(None);
    };
    let prefix = prefix_raw.to_ascii_lowercase();
    let mut prefix_side = None;
    let mut prefix_matches = false;
    for base in bases {
        if prefix == *base {
            prefix_matches = true;
            break;
        }
        if prefix == format!("{base}-left") || prefix == format!("{base}_left") {
            prefix_matches = true;
            prefix_side = Some(Side::Left);
            break;
        }
        if prefix == format!("{base}-right") || prefix == format!("{base}_right") {
            prefix_matches = true;
            prefix_side = Some(Side::Right);
            break;
        }
    }
    if !prefix_matches {
        return Ok(None);
    }

    let (local_side, remaining) = parse_local_side(local_raw);
    let side = match (prefix_side, local_side) {
        (Some(a), Some(b)) if a != b => {
            return Err(RepoError::InvalidInput(
                "conflicting relation/association side qualifiers".to_string(),
            ))
        }
        (Some(a), _) => Some(a),
        (_, Some(b)) => Some(b),
        _ => None,
    };

    if remaining.trim().is_empty() {
        return Err(RepoError::InvalidInput(format!("{missing_msg}: {field}")));
    }
    Ok(Some(ParsedRelationField {
        type_token: remaining.trim().to_string(),
        side,
    }))
}

fn parse_local_side(local: &str) -> (Option<Side>, &str) {
    let lower = local.to_ascii_lowercase();
    if lower.starts_with("left:") {
        return (Some(Side::Left), &local["left:".len()..]);
    }
    if lower.starts_with("right:") {
        return (Some(Side::Right), &local["right:".len()..]);
    }
    if lower.starts_with("left-") || lower.starts_with("left_") {
        return (Some(Side::Left), &local["left-".len()..]);
    }
    if lower.starts_with("right-") || lower.starts_with("right_") {
        return (Some(Side::Right), &local["right-".len()..]);
    }
    (None, local)
}

fn value_to_stringish(value: Value) -> String {
    match value {
        Value::String(s) => s,
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        other => other.to_string(),
    }
}

fn value_to_unit_ref(value: &Value) -> RepoResult<String> {
    let raw = value
        .as_str()
        .ok_or_else(|| {
            RepoError::InvalidInput("relation value must be unit reference".to_string())
        })?
        .trim();
    let mut split = raw.splitn(2, ':');
    let base = split.next().unwrap_or(raw);
    if let Some(version) = split.next() {
        if !version.is_empty() && !version.chars().all(|ch| ch.is_ascii_digit()) {
            return Err(RepoError::InvalidInput(format!(
                "invalid relation unit reference: {raw}"
            )));
        }
    }
    let mut parts = base.split('.');
    let tenant = parts.next();
    let unit = parts.next();
    if tenant.is_none() || unit.is_none() || parts.next().is_some() {
        return Err(RepoError::InvalidInput(format!(
            "invalid relation unit reference: {raw}"
        )));
    }
    if tenant
        .and_then(|t| t.parse::<i64>().ok())
        .zip(unit.and_then(|u| u.parse::<i64>().ok()))
        .is_none()
    {
        return Err(RepoError::InvalidInput(format!(
            "invalid relation unit reference: {raw}"
        )));
    }
    Ok(raw.to_string())
}

fn resolve_relation_type_token(token: &str) -> RepoResult<i64> {
    if let Ok(n) = token.parse::<i64>() {
        return Ok(n);
    }
    let normalized = normalize_type_token(token, "_RELATION");
    match normalized.as_str() {
        "PARENT_CHILD_RELATION" => Ok(1),
        "PARENTCHILD_RELATION" => Ok(1),
        "PARENT_RELATION" => Ok(1),
        "CHILD_RELATION" => Ok(1),
        "REPLACEMENT_RELATION" => Ok(3),
        "REPLACE_RELATION" => Ok(3),
        _ => Err(RepoError::InvalidInput(format!(
            "unknown relation type: {token}"
        ))),
    }
}

fn resolve_association_type_token(token: &str) -> RepoResult<i64> {
    if let Ok(n) = token.parse::<i64>() {
        return Ok(n);
    }
    let normalized = normalize_type_token(token, "_ASSOCIATION");
    match normalized.as_str() {
        "CASE_ASSOCIATION" => Ok(2),
        "CASEASSOC_ASSOCIATION" => Ok(2),
        "CASE_ASSOC_ASSOCIATION" => Ok(2),
        _ => Err(RepoError::InvalidInput(format!(
            "unknown association type: {token}"
        ))),
    }
}

fn normalize_type_token(raw: &str, suffix: &str) -> String {
    let mut s = raw.trim().replace(['-', ' '], "_").to_ascii_uppercase();
    if !s.ends_with(suffix) {
        s.push_str(suffix);
    }
    s
}

fn tokenize(query: &str) -> RepoResult<Vec<Token>> {
    let chars = query.chars().collect::<Vec<_>>();
    let mut out = Vec::new();
    let mut i = 0usize;
    while i < chars.len() {
        let ch = chars[i];
        if ch.is_whitespace() {
            i += 1;
            continue;
        }
        match ch {
            '(' => {
                out.push(Token::LParen);
                i += 1;
            }
            ')' => {
                out.push(Token::RParen);
                i += 1;
            }
            ',' => {
                out.push(Token::Comma);
                i += 1;
            }
            '=' => {
                out.push(Token::Operator("=".to_string()));
                i += 1;
            }
            '!' => {
                if i + 1 < chars.len() && chars[i + 1] == '=' {
                    out.push(Token::Operator("!=".to_string()));
                    i += 2;
                } else {
                    return Err(RepoError::InvalidInput(
                        "expected '!=' operator".to_string(),
                    ));
                }
            }
            '>' => {
                if i + 1 < chars.len() && chars[i + 1] == '=' {
                    out.push(Token::Operator(">=".to_string()));
                    i += 2;
                } else {
                    out.push(Token::Operator(">".to_string()));
                    i += 1;
                }
            }
            '<' => {
                if i + 1 < chars.len() && chars[i + 1] == '=' {
                    out.push(Token::Operator("<=".to_string()));
                    i += 2;
                } else if i + 1 < chars.len() && chars[i + 1] == '>' {
                    out.push(Token::Operator("<>".to_string()));
                    i += 2;
                } else {
                    out.push(Token::Operator("<".to_string()));
                    i += 1;
                }
            }
            '\'' | '"' => {
                let quote = ch;
                i += 1;
                let mut text = String::new();
                let mut closed = false;
                while i < chars.len() {
                    let curr = chars[i];
                    if curr == '\\' && i + 1 < chars.len() {
                        text.push(chars[i + 1]);
                        i += 2;
                        continue;
                    }
                    if curr == quote {
                        i += 1;
                        closed = true;
                        break;
                    }
                    text.push(curr);
                    i += 1;
                }
                if !closed {
                    return Err(RepoError::InvalidInput(
                        "unterminated string literal in search query".to_string(),
                    ));
                }
                out.push(Token::String(text));
            }
            _ => {
                let mut text = String::new();
                while i < chars.len() {
                    let c = chars[i];
                    if c.is_whitespace()
                        || c == '('
                        || c == ')'
                        || c == ','
                        || c == '='
                        || c == '<'
                        || c == '>'
                        || c == '!'
                        || c == '\''
                        || c == '"'
                    {
                        break;
                    }
                    text.push(c);
                    i += 1;
                }
                let lower = text.to_ascii_lowercase();
                if lower == "true" {
                    out.push(Token::Bool(true));
                } else if lower == "false" {
                    out.push(Token::Bool(false));
                } else if is_number_token(&text) {
                    out.push(Token::Number(text));
                } else {
                    out.push(Token::Word(text));
                }
            }
        }
    }
    if out.is_empty() {
        return Err(RepoError::InvalidInput(
            "search query cannot be empty".to_string(),
        ));
    }
    Ok(out)
}

fn is_number_token(text: &str) -> bool {
    if text.is_empty() {
        return false;
    }
    if let Some(rest) = text.strip_prefix('-') {
        return !rest.is_empty() && rest.chars().all(|c| c.is_ascii_digit() || c == '.');
    }
    text.chars().all(|c| c.is_ascii_digit() || c == '.')
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{parse_search_query, parse_search_query_strict};

    #[test]
    fn parse_basic_and_or_not_expression() {
        let expr = parse_search_query("tenantid = 7 and not (status = 10 or status = 20)")
            .expect("parse should succeed");
        assert!(expr.get("and").is_some());
    }

    #[test]
    fn parse_unit_and_attribute_predicates() {
        let expr =
            parse_search_query("unitname like '*foo*' and custom_attr >= 10").expect("parse");
        let and = expr["and"].as_array().expect("and array");
        assert!(and.iter().any(|leaf| leaf.get("predicates").is_some()));
        assert!(and.iter().any(|leaf| leaf.get("attribute_cmp").is_some()));
    }

    #[test]
    fn parse_relation_and_association_prefixes() {
        let expr =
            parse_search_query("relation-right:11 = '7.100' and association:21 = 'external-ref'")
                .expect("parse");
        assert_eq!(
            expr,
            json!({
                "and": [
                    {"relation": {"type": 11, "side": "right", "unit": "7.100"}},
                    {"association": {"type": 21, "reference": "external-ref"}}
                ]
            })
        );
    }

    #[test]
    fn parse_symbolic_relation_association_types() {
        let expr = parse_search_query(
            "relation:right-parent-child = '7.100' and association:case = 'external-ref'",
        )
        .expect("parse");
        assert_eq!(
            expr,
            json!({
                "and": [
                    {"relation": {"type": 1, "side": "right", "unit": "7.100"}},
                    {"association": {"type": 2, "reference": "external-ref"}}
                ]
            })
        );
    }

    #[test]
    fn parse_unknown_symbolic_relation_type_fails() {
        let err = parse_search_query("relation:unknown = '7.100'").expect_err("must fail");
        assert!(err.to_string().contains("unknown relation type"));
    }

    #[test]
    fn parse_unitver_alias_maps_to_unit_predicate() {
        let expr = parse_search_query("unit_ver = 2").expect("parse");
        assert_eq!(
            expr,
            json!({
                "predicates": [{
                    "field": "unitver",
                    "op": "eq",
                    "value": 2
                }]
            })
        );
    }

    #[test]
    fn strict_mode_requires_attr_prefix_for_unknown_fields() {
        let err = parse_search_query_strict("custom_attr = 10").expect_err("must fail");
        assert!(err.to_string().contains("strict mode"));

        let ok = parse_search_query_strict("attr:custom_attr = 10").expect("must parse");
        assert_eq!(
            ok,
            json!({
                "attribute_cmp": {
                    "name_or_id": "custom_attr",
                    "op": "eq",
                    "value": 10
                }
            })
        );
    }

    #[test]
    fn parse_name_eq_with_wildcard_becomes_like() {
        let expr = parse_search_query("name = '*foo*'").expect("parse");
        assert_eq!(
            expr,
            json!({
                "predicates": [{
                    "field": "name",
                    "op": "like",
                    "value": "*foo*"
                }]
            })
        );
    }

    #[test]
    fn parse_relation_type_synonym() {
        let expr = parse_search_query("relation:parentchild = '7.100'").expect("parse");
        assert_eq!(
            expr,
            json!({
                "relation": {"type": 1, "unit": "7.100"}
            })
        );
    }

    #[test]
    fn parse_relation_reference_with_invalid_version_fails() {
        let err = parse_search_query("relation:1 = '7.100:abc'").expect_err("must fail");
        assert!(err.to_string().contains("invalid relation unit reference"));
    }

    #[test]
    fn parse_unterminated_string_fails() {
        let err = parse_search_query("name = 'unterminated").expect_err("must fail");
        assert!(err.to_string().contains("unterminated string literal"));
    }

    #[test]
    fn parse_status_name_maps_to_numeric_status() {
        let expr = parse_search_query("status = EFFECTIVE").expect("parse");
        assert_eq!(
            expr,
            json!({
                "predicates": [{
                    "field": "status",
                    "op": "eq",
                    "value": 30
                }]
            })
        );
    }

    #[test]
    fn parse_attribute_like_and_neq_are_preserved() {
        let like = parse_search_query("attr:custom_attr like '*foo*'").expect("parse");
        assert_eq!(
            like,
            json!({
                "attribute_cmp": {
                    "name_or_id": "custom_attr",
                    "op": "like",
                    "value": "*foo*"
                }
            })
        );

        let neq = parse_search_query("attr:custom_attr != 10").expect("parse");
        assert_eq!(
            neq,
            json!({
                "attribute_cmp": {
                    "name_or_id": "custom_attr",
                    "op": "neq",
                    "value": 10
                }
            })
        );
    }

    #[test]
    fn parse_in_list_expands_to_or_expression() {
        let expr = parse_search_query("unitid in (1, 2, 3)").expect("parse");
        assert_eq!(
            expr,
            json!({
                "or": [
                    {"predicates": [{"field": "unitid", "op": "eq", "value": 1}]},
                    {"predicates": [{"field": "unitid", "op": "eq", "value": 2}]},
                    {"predicates": [{"field": "unitid", "op": "eq", "value": 3}]}
                ]
            })
        );
    }

    #[test]
    fn parse_in_list_strict_attribute_requires_prefix() {
        let err = parse_search_query_strict("custom_attr in (1, 2)").expect_err("must fail");
        assert!(err.to_string().contains("strict mode"));

        let ok = parse_search_query_strict("attr:custom_attr in (1, 2)").expect("parse");
        assert_eq!(
            ok,
            json!({
                "or": [
                    {"attribute_cmp": {"name_or_id": "custom_attr", "op": "eq", "value": 1}},
                    {"attribute_cmp": {"name_or_id": "custom_attr", "op": "eq", "value": 2}}
                ]
            })
        );
    }

    #[test]
    fn parse_in_list_requires_non_empty_values() {
        let err = parse_search_query("tenantid in ()").expect_err("must fail");
        assert!(err.to_string().contains("at least one value"));
    }

    #[test]
    fn parse_not_in_list_expands_to_and_expression() {
        let expr = parse_search_query("unitid not in (1, 2)").expect("parse");
        assert_eq!(
            expr,
            json!({
                "and": [
                    {"predicates": [{"field": "unitid", "op": "neq", "value": 1}]},
                    {"predicates": [{"field": "unitid", "op": "neq", "value": 2}]}
                ]
            })
        );
    }

    #[test]
    fn parse_not_requires_in_keyword() {
        let err = parse_search_query("unitid not 1").expect_err("must fail");
        assert!(err.to_string().contains("expected IN after NOT"));
    }

    #[test]
    fn parse_angle_bracket_neq_operator() {
        let expr = parse_search_query("unitid <> 7").expect("parse");
        assert_eq!(
            expr,
            json!({
                "predicates": [{
                    "field": "unitid",
                    "op": "neq",
                    "value": 7
                }]
            })
        );
    }

    #[test]
    fn parse_between_expands_to_and_expression() {
        let expr = parse_search_query("unitid between 10 and 20").expect("parse");
        assert_eq!(
            expr,
            json!({
                "and": [
                    {"predicates": [{"field": "unitid", "op": "gte", "value": 10}]},
                    {"predicates": [{"field": "unitid", "op": "lte", "value": 20}]}
                ]
            })
        );
    }

    #[test]
    fn parse_between_requires_and_keyword() {
        let err = parse_search_query("unitid between 10 20").expect_err("must fail");
        assert!(err.to_string().contains("BETWEEN requires 'and'"));
    }
}
