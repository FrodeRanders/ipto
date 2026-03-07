use chrono::{DateTime, Utc};
use serde_json::{Map, Value};
use uuid::Uuid;

use crate::backend::{RepoError, RepoResult};

#[derive(Clone)]
pub(crate) enum UnitField {
    TenantId,
    UnitId,
    UnitVer,
    Status,
    Name,
    Corrid,
    Created,
    Modified,
}

#[derive(Clone, Copy)]
pub(crate) enum UnitOp {
    Eq,
    Neq,
    Gt,
    Gte,
    Lt,
    Lte,
    Like,
}

#[derive(Clone)]
pub(crate) enum UnitValue {
    I64(i64),
    String(String),
    TimestampMillis(i64),
}

#[derive(Clone)]
pub(crate) struct UnitPredicate {
    pub(crate) field: UnitField,
    pub(crate) op: UnitOp,
    pub(crate) value: UnitValue,
}

#[derive(Clone, Default)]
pub(crate) struct AttributeCmpConstraint {
    pub(crate) attr_id: Option<i64>,
    pub(crate) attr_name: Option<String>,
    pub(crate) op: Option<String>,
    pub(crate) value_type: Option<String>,
    pub(crate) value_text: Option<String>,
    pub(crate) value_number: Option<f64>,
    pub(crate) value_bool: Option<bool>,
    pub(crate) value_time_millis: Option<i64>,
    pub(crate) value_wildcard: Option<bool>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum RelationSide {
    Left,
    Right,
}

#[derive(Clone, Default)]
pub(crate) struct RelationConstraint {
    pub(crate) relation_type: Option<i64>,
    pub(crate) related_tenantid: Option<i64>,
    pub(crate) related_unitid: Option<i64>,
    pub(crate) side: Option<RelationSide>,
}

#[derive(Clone, Default)]
pub(crate) struct AssociationConstraint {
    pub(crate) association_type: Option<i64>,
    pub(crate) association_reference: Option<String>,
    pub(crate) side: Option<RelationSide>,
}

pub(crate) fn extract_unit_predicates(
    expr: &Map<String, Value>,
    validate_corrid_uuid: bool,
) -> RepoResult<Vec<UnitPredicate>> {
    let Some(predicates_value) = expr.get("predicates") else {
        return Ok(Vec::new());
    };
    let predicates = predicates_value.as_array().ok_or_else(|| {
        RepoError::InvalidInput("predicates must be an array of objects".to_string())
    })?;

    let mut out = Vec::new();
    for raw in predicates {
        let obj = raw.as_object().ok_or_else(|| {
            RepoError::InvalidInput("predicates entries must be objects".to_string())
        })?;
        let field = obj
            .get("field")
            .and_then(Value::as_str)
            .ok_or_else(|| RepoError::InvalidInput("predicates.field is required".to_string()))?;
        let op = obj.get("op").and_then(Value::as_str).unwrap_or("eq");
        let value = obj
            .get("value")
            .ok_or_else(|| RepoError::InvalidInput("predicates.value is required".to_string()))?;
        out.push(parse_unit_predicate(
            field,
            op,
            value,
            validate_corrid_uuid,
        )?);
    }
    Ok(out)
}

pub(crate) fn extract_attribute_constraint(
    expr: &Map<String, Value>,
) -> RepoResult<AttributeCmpConstraint> {
    if expr.contains_key("attribute_eq") && expr.contains_key("attribute_cmp") {
        return Err(RepoError::InvalidInput(
            "cannot combine attribute_eq and attribute_cmp in same expression".to_string(),
        ));
    }
    let (raw, default_op) = if let Some(raw) = expr.get("attribute_cmp") {
        (raw, "eq")
    } else if let Some(raw) = expr.get("attribute_eq") {
        (raw, "eq")
    } else {
        return Ok(AttributeCmpConstraint::default());
    };
    let obj = raw.as_object().ok_or_else(|| {
        RepoError::InvalidInput("attribute_eq/attribute_cmp must be an object".to_string())
    })?;
    let value = obj.get("value").ok_or_else(|| {
        RepoError::InvalidInput("attribute_eq/attribute_cmp.value is required".to_string())
    })?;

    let mut attr_id = extract_obj_i64(obj, "attrid")?;
    let mut attr_name = extract_obj_string(obj, "name")
        .or_else(|| extract_obj_string(obj, "attrname"))
        .or_else(|| extract_obj_string(obj, "attribute_name"));

    if let Some(name_or_id) = extract_obj_string(obj, "name_or_id") {
        // Java-compatible selector that can be either id or name.
        if let Ok(parsed) = name_or_id.parse::<i64>() {
            attr_id = Some(parsed);
        } else {
            attr_name = Some(name_or_id);
        }
    }

    if attr_id.is_none() && attr_name.is_none() {
        return Err(RepoError::InvalidInput(
            "attribute_eq/attribute_cmp requires attrid/name/name_or_id selector".to_string(),
        ));
    }

    let op = extract_obj_string(obj, "op")
        .unwrap_or_else(|| default_op.to_string())
        .to_ascii_lowercase();
    if !matches!(
        op.as_str(),
        "eq" | "neq" | "gt" | "gte" | "lt" | "lte" | "like"
    ) {
        return Err(RepoError::InvalidInput(format!(
            "attribute_cmp.op must be one of eq|neq|gt|gte|lt|lte|like, got '{op}'"
        )));
    }

    let inferred_type = match value {
        Value::String(_) => "string".to_string(),
        Value::Number(_) => "number".to_string(),
        Value::Bool(_) => "boolean".to_string(),
        _ => {
            return Err(RepoError::InvalidInput(
                "attribute_eq/attribute_cmp.value must be string/number/boolean".to_string(),
            ))
        }
    };
    let value_type = extract_obj_string(obj, "value_type")
        .unwrap_or(inferred_type)
        .to_ascii_lowercase();
    if !matches!(
        value_type.as_str(),
        "string" | "number" | "boolean" | "time"
    ) {
        return Err(RepoError::InvalidInput(format!(
            "attribute_cmp.value_type must be one of string|number|boolean|time, got '{value_type}'"
        )));
    }
    if value_type == "boolean" && !matches!(op.as_str(), "eq" | "neq") {
        return Err(RepoError::InvalidInput(
            "attribute_cmp boolean comparisons support only eq|neq".to_string(),
        ));
    }
    if value_type != "string" && op == "like" {
        return Err(RepoError::InvalidInput(
            "attribute_cmp like comparisons support only string values".to_string(),
        ));
    }

    let (value_text, value_wildcard) = if value_type == "string" {
        let raw = value
            .as_str()
            .map(ToString::to_string)
            .or_else(|| value_to_text(value))
            .ok_or_else(|| {
                RepoError::InvalidInput(
                    "attribute_cmp.value with value_type=string must be string-like".to_string(),
                )
            })?;
        // Shared normalization keeps backend behavior aligned for case/wildcard matching.
        let (normalized, wildcard) = normalize_attribute_string(raw);
        (Some(normalized), Some(wildcard))
    } else {
        (None, None)
    };
    let value_number = if value_type == "number" {
        Some(value_to_f64(value).ok_or_else(|| {
            RepoError::InvalidInput(
                "attribute_cmp.value with value_type=number must be numeric".to_string(),
            )
        })?)
    } else {
        None
    };
    let value_bool = if value_type == "boolean" {
        Some(value.as_bool().ok_or_else(|| {
            RepoError::InvalidInput(
                "attribute_cmp.value with value_type=boolean must be boolean".to_string(),
            )
        })?)
    } else {
        None
    };
    let value_time_millis = if value_type == "time" {
        Some(parse_time_millis(value, "time value")?)
    } else {
        None
    };

    Ok(AttributeCmpConstraint {
        attr_id,
        attr_name,
        op: Some(op),
        value_type: Some(value_type),
        value_text,
        value_number,
        value_bool,
        value_time_millis,
        value_wildcard,
    })
}

pub(crate) fn extract_relation_constraint(
    expr: &Map<String, Value>,
) -> RepoResult<RelationConstraint> {
    let mut out = RelationConstraint {
        relation_type: first_i64(expr, &["relation_type", "relationtype", "reltype"])?,
        related_tenantid: first_i64(
            expr,
            &["related_tenantid", "relation_tenantid", "reltenantid"],
        )?,
        related_unitid: first_i64(expr, &["related_unitid", "relation_unitid", "relunitid"])?,
        side: first_side(expr, &["relation_side", "relside"])?,
    };

    if let Some(obj) = first_object(expr, &["relation", "relations", "rel"]) {
        merge_i64(
            &mut out.relation_type,
            first_obj_i64(obj, &["type", "relation_type", "relationtype", "reltype"])?,
            "relation_type",
        )?;
        merge_side(
            &mut out.side,
            first_obj_side(obj, &["side", "relation_side", "relside"])?,
            "relation_side",
        )?;

        if let Some(unit) = obj
            .get("unit")
            .or_else(|| obj.get("related"))
            .or_else(|| obj.get("unit_ref"))
        {
            let (tenant, unit_id) = parse_unit_ref(unit)?;
            merge_i64(&mut out.related_tenantid, Some(tenant), "related_tenantid")?;
            merge_i64(&mut out.related_unitid, Some(unit_id), "related_unitid")?;
        } else {
            merge_i64(
                &mut out.related_tenantid,
                first_obj_i64(
                    obj,
                    &["related_tenantid", "relation_tenantid", "reltenantid"],
                )?,
                "related_tenantid",
            )?;
            merge_i64(
                &mut out.related_unitid,
                first_obj_i64(obj, &["related_unitid", "relation_unitid", "relunitid"])?,
                "related_unitid",
            )?;
        }
    }

    Ok(out)
}

pub(crate) fn extract_association_constraint(
    expr: &Map<String, Value>,
) -> RepoResult<AssociationConstraint> {
    let mut out = AssociationConstraint {
        association_type: first_i64(expr, &["association_type", "associationtype", "assoctype"])?,
        association_reference: first_string(
            expr,
            &[
                "association_reference",
                "association_ref",
                "assocstring",
                "refstring",
            ],
        ),
        side: first_side(expr, &["association_side", "assocside"])?,
    };

    if let Some(obj) = first_object(expr, &["association", "associations", "assoc"]) {
        merge_i64(
            &mut out.association_type,
            first_obj_i64(
                obj,
                &["type", "association_type", "associationtype", "assoctype"],
            )?,
            "association_type",
        )?;
        merge_string(
            &mut out.association_reference,
            first_obj_string(
                obj,
                &[
                    "reference",
                    "association_reference",
                    "association_ref",
                    "assocstring",
                    "refstring",
                ],
            ),
            "association_reference",
        )?;
        merge_side(
            &mut out.side,
            first_obj_side(obj, &["side", "association_side", "assocside"])?,
            "association_side",
        )?;
    }

    Ok(out)
}

pub(crate) fn parse_time_millis(value: &Value, label: &str) -> RepoResult<i64> {
    match value {
        Value::Number(n) => {
            let millis = n.as_i64().ok_or_else(|| {
                RepoError::InvalidInput(format!("{label} must be integer millis"))
            })?;
            if DateTime::<Utc>::from_timestamp_millis(millis).is_none() {
                return Err(RepoError::InvalidInput(format!(
                    "{label} has out-of-range millis value: {millis}"
                )));
            }
            Ok(millis)
        }
        Value::String(s) => {
            // Accept both millis-as-string and RFC3339 timestamps.
            if let Ok(millis) = s.parse::<i64>() {
                if DateTime::<Utc>::from_timestamp_millis(millis).is_none() {
                    return Err(RepoError::InvalidInput(format!(
                        "{label} has out-of-range millis value: {millis}"
                    )));
                }
                return Ok(millis);
            }
            DateTime::parse_from_rfc3339(s)
                .map(|dt| dt.timestamp_millis())
                .map_err(|_| {
                    RepoError::InvalidInput(format!(
                        "{label} must be RFC3339 timestamp or millis integer"
                    ))
                })
        }
        other => Err(RepoError::InvalidInput(format!(
            "{label} must be timestamp string/millis, got {other}"
        ))),
    }
}

pub(crate) fn normalize_attribute_string(value: String) -> (String, bool) {
    // Sanitize quotes and normalize wildcard syntax used across query inputs.
    let sanitized = value.replace(['\'', '\"'], " ");
    let wildcarded = sanitized.replace('*', "%");
    let has_wildcard = wildcarded.contains('%') || wildcarded.contains('_');
    (wildcarded.to_ascii_lowercase(), has_wildcard)
}

fn parse_unit_predicate(
    field: &str,
    op: &str,
    value: &Value,
    validate_corrid_uuid: bool,
) -> RepoResult<UnitPredicate> {
    let field_norm = field.to_ascii_lowercase();
    let op = parse_unit_op(op)?;

    let field = match field_norm.as_str() {
        "tenantid" | "tenant_id" => UnitField::TenantId,
        "unitid" | "unit_id" => UnitField::UnitId,
        "unitver" | "unit_ver" => UnitField::UnitVer,
        "status" => UnitField::Status,
        "name" | "unitname" | "unit_name" => UnitField::Name,
        "corrid" | "corr_id" | "correlationid" => UnitField::Corrid,
        "created" => UnitField::Created,
        "modified" => UnitField::Modified,
        other => {
            return Err(RepoError::InvalidInput(format!(
                "unsupported predicates.field '{other}'"
            )))
        }
    };

    let typed_value = match field {
        UnitField::TenantId | UnitField::UnitId | UnitField::UnitVer => {
            UnitValue::I64(value_to_i64(value)?)
        }
        UnitField::Status => UnitValue::I64(value_to_status_i64(value)?),
        UnitField::Name => {
            let mut text = value_to_string(value)?;
            if matches!(op, UnitOp::Like) {
                // Allow both '*' and '%' as wildcard inputs.
                text = text.replace('*', "%");
            }
            UnitValue::String(text)
        }
        UnitField::Corrid => {
            let text = value_to_string(value)?;
            if validate_corrid_uuid {
                Uuid::parse_str(&text).map_err(|e| {
                    RepoError::InvalidInput(format!("predicates corrid must be UUID string: {e}"))
                })?;
            }
            UnitValue::String(text)
        }
        UnitField::Created | UnitField::Modified => {
            UnitValue::TimestampMillis(parse_time_millis(value, "predicate timestamp")?)
        }
    };

    if matches!(field, UnitField::Name) {
        if !matches!(op, UnitOp::Eq | UnitOp::Neq | UnitOp::Like) {
            return Err(RepoError::InvalidInput(
                "name predicates support only eq|neq|like".to_string(),
            ));
        }
    } else if matches!(field, UnitField::Corrid) {
        if !matches!(op, UnitOp::Eq | UnitOp::Neq) {
            return Err(RepoError::InvalidInput(
                "corrid predicates support only eq|neq".to_string(),
            ));
        }
    } else if matches!(op, UnitOp::Like) {
        return Err(RepoError::InvalidInput(
            "like predicate is supported only for name".to_string(),
        ));
    }

    Ok(UnitPredicate {
        field,
        op,
        value: typed_value,
    })
}

fn parse_unit_op(op: &str) -> RepoResult<UnitOp> {
    match op.to_ascii_lowercase().as_str() {
        "eq" => Ok(UnitOp::Eq),
        "neq" | "ne" => Ok(UnitOp::Neq),
        "gt" => Ok(UnitOp::Gt),
        "gte" | "ge" => Ok(UnitOp::Gte),
        "lt" => Ok(UnitOp::Lt),
        "lte" | "le" => Ok(UnitOp::Lte),
        "like" => Ok(UnitOp::Like),
        other => Err(RepoError::InvalidInput(format!(
            "unsupported predicates.op '{other}'"
        ))),
    }
}

fn value_to_i64(value: &Value) -> RepoResult<i64> {
    match value {
        Value::Number(n) => n
            .as_i64()
            .ok_or_else(|| RepoError::InvalidInput("predicate value must be integer".to_string())),
        Value::String(s) => s
            .parse::<i64>()
            .map_err(|_| RepoError::InvalidInput("predicate value must be integer".to_string())),
        _ => Err(RepoError::InvalidInput(
            "predicate value must be integer/string".to_string(),
        )),
    }
}

fn value_to_status_i64(value: &Value) -> RepoResult<i64> {
    if let Ok(parsed) = value_to_i64(value) {
        return Ok(parsed);
    }
    let raw = value.as_str().ok_or_else(|| {
        RepoError::InvalidInput(
            "predicate status value must be integer or known status name".to_string(),
        )
    })?;
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
            "Unknown status: \"{raw}\""
        ))),
    }
}

fn value_to_string(value: &Value) -> RepoResult<String> {
    match value {
        Value::String(s) => Ok(s.clone()),
        Value::Number(n) => Ok(n.to_string()),
        Value::Bool(b) => Ok(b.to_string()),
        _ => Err(RepoError::InvalidInput(
            "predicate value must be string-like".to_string(),
        )),
    }
}

fn value_to_text(value: &Value) -> Option<String> {
    match value {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

fn value_to_f64(value: &Value) -> Option<f64> {
    let parsed = match value {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.parse::<f64>().ok(),
        _ => None,
    }?;
    if parsed.is_finite() {
        Some(parsed)
    } else {
        None
    }
}

fn extract_obj_string(expr: &Map<String, Value>, key: &str) -> Option<String> {
    expr.get(key)
        .and_then(Value::as_str)
        .map(ToString::to_string)
}

fn extract_obj_i64(expr: &Map<String, Value>, key: &str) -> RepoResult<Option<i64>> {
    match expr.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(n)) => n
            .as_i64()
            .ok_or_else(|| RepoError::InvalidInput(format!("{key} must be an integer")))
            .map(Some),
        Some(Value::String(s)) => s
            .parse::<i64>()
            .map(Some)
            .map_err(|_| RepoError::InvalidInput(format!("{key} must be an integer"))),
        Some(other) => Err(RepoError::InvalidInput(format!(
            "{key} must be numeric/string, got {other}"
        ))),
    }
}

fn parse_side(value: &Value, key: &str) -> RepoResult<RelationSide> {
    let Some(raw) = value.as_str() else {
        return Err(RepoError::InvalidInput(format!(
            "{key} must be 'left' or 'right'"
        )));
    };
    match raw.to_ascii_lowercase().as_str() {
        "left" => Ok(RelationSide::Left),
        "right" => Ok(RelationSide::Right),
        _ => Err(RepoError::InvalidInput(format!(
            "{key} must be 'left' or 'right'"
        ))),
    }
}

fn parse_unit_ref(value: &Value) -> RepoResult<(i64, i64)> {
    if let Some(obj) = value.as_object() {
        let tenant = first_obj_i64(obj, &["tenantid", "tenant_id"]).and_then(|v| {
            v.ok_or_else(|| {
                RepoError::InvalidInput("relation.unit.tenantid is required".to_string())
            })
        })?;
        let unit = first_obj_i64(obj, &["unitid", "unit_id"]).and_then(|v| {
            v.ok_or_else(|| RepoError::InvalidInput("relation.unit.unitid is required".to_string()))
        })?;
        return Ok((tenant, unit));
    }

    let raw = value.as_str().ok_or_else(|| {
        RepoError::InvalidInput(
            "relation.unit must be object or 'tenantid.unitid[:version]'".to_string(),
        )
    })?;
    let trimmed = raw.trim();
    let mut split = trimmed.splitn(2, ':');
    let base = split.next().unwrap_or(trimmed);
    if let Some(version) = split.next() {
        if !version.is_empty() && !version.chars().all(|ch| ch.is_ascii_digit()) {
            return Err(RepoError::InvalidInput(format!(
                "invalid relation unit reference: {raw}"
            )));
        }
    }
    let mut parts = base.split('.');
    let tenant = parts
        .next()
        .ok_or_else(|| RepoError::InvalidInput(format!("invalid relation unit reference: {raw}")))?
        .parse::<i64>()
        .map_err(|_| RepoError::InvalidInput(format!("invalid relation unit reference: {raw}")))?;
    let unit = parts
        .next()
        .ok_or_else(|| RepoError::InvalidInput(format!("invalid relation unit reference: {raw}")))?
        .parse::<i64>()
        .map_err(|_| RepoError::InvalidInput(format!("invalid relation unit reference: {raw}")))?;
    if parts.next().is_some() {
        return Err(RepoError::InvalidInput(format!(
            "invalid relation unit reference: {raw}"
        )));
    }
    Ok((tenant, unit))
}

fn first_i64(expr: &Map<String, Value>, keys: &[&str]) -> RepoResult<Option<i64>> {
    for key in keys {
        if expr.contains_key(*key) {
            return extract_obj_i64(expr, key);
        }
    }
    Ok(None)
}

fn first_obj_i64(expr: &Map<String, Value>, keys: &[&str]) -> RepoResult<Option<i64>> {
    first_i64(expr, keys)
}

fn first_string(expr: &Map<String, Value>, keys: &[&str]) -> Option<String> {
    keys.iter().find_map(|key| extract_obj_string(expr, key))
}

fn first_obj_string(expr: &Map<String, Value>, keys: &[&str]) -> Option<String> {
    first_string(expr, keys)
}

fn first_side(expr: &Map<String, Value>, keys: &[&str]) -> RepoResult<Option<RelationSide>> {
    for key in keys {
        if let Some(value) = expr.get(*key) {
            return parse_side(value, key).map(Some);
        }
    }
    Ok(None)
}

fn first_obj_side(expr: &Map<String, Value>, keys: &[&str]) -> RepoResult<Option<RelationSide>> {
    first_side(expr, keys)
}

fn first_object<'a>(expr: &'a Map<String, Value>, keys: &[&str]) -> Option<&'a Map<String, Value>> {
    for key in keys {
        if let Some(v) = expr.get(*key) {
            if let Some(obj) = v.as_object() {
                return Some(obj);
            }
        }
    }
    None
}

fn merge_i64(target: &mut Option<i64>, incoming: Option<i64>, field: &str) -> RepoResult<()> {
    if let Some(value) = incoming {
        if let Some(existing) = target {
            if *existing != value {
                return Err(RepoError::InvalidInput(format!(
                    "conflicting values for {field}: {existing} vs {value}"
                )));
            }
        } else {
            *target = Some(value);
        }
    }
    Ok(())
}

fn merge_string(
    target: &mut Option<String>,
    incoming: Option<String>,
    field: &str,
) -> RepoResult<()> {
    if let Some(value) = incoming {
        if let Some(existing) = target {
            if existing != &value {
                return Err(RepoError::InvalidInput(format!(
                    "conflicting values for {field}: '{existing}' vs '{value}'"
                )));
            }
        } else {
            *target = Some(value);
        }
    }
    Ok(())
}

fn merge_side(
    target: &mut Option<RelationSide>,
    incoming: Option<RelationSide>,
    field: &str,
) -> RepoResult<()> {
    if let Some(value) = incoming {
        if let Some(existing) = target {
            if *existing != value {
                return Err(RepoError::InvalidInput(format!(
                    "conflicting values for {field}"
                )));
            }
        } else {
            *target = Some(value);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::search_filters::{extract_unit_predicates, UnitField, UnitPredicate, UnitValue};

    fn single_predicate(expr: serde_json::Value) -> UnitPredicate {
        let obj = expr.as_object().expect("object expression");
        let mut parsed = extract_unit_predicates(obj, false).expect("extract predicates");
        assert_eq!(parsed.len(), 1);
        parsed.remove(0)
    }

    #[test]
    fn extract_predicates_supports_unit_name_alias() {
        let predicate = single_predicate(json!({
            "predicates": [{
                "field": "unit_name",
                "op": "eq",
                "value": "alpha"
            }]
        }));
        assert!(matches!(predicate.field, UnitField::Name));
        assert!(matches!(predicate.value, UnitValue::String(v) if v == "alpha"));
    }

    #[test]
    fn extract_predicates_supports_correlationid_alias() {
        let predicate = single_predicate(json!({
            "predicates": [{
                "field": "correlationid",
                "op": "eq",
                "value": "00000000-0000-0000-0000-000000000000"
            }]
        }));
        assert!(matches!(predicate.field, UnitField::Corrid));
        assert!(
            matches!(predicate.value, UnitValue::String(v) if v == "00000000-0000-0000-0000-000000000000")
        );
    }
}
