use std::sync::Arc;
use std::{cmp::Ordering, collections::HashMap};

use serde_json::{json, Map, Number, Value};

use crate::backend::{Backend, RepoError, RepoResult};
use crate::graphql_sdl::{
    attribute_type_is_record, catalog_as_json, normalize_attribute_type, parse_graphql_sdl,
    validate_catalog_references,
};
use crate::model::{
    Association, Relation, SearchOrder, SearchPaging, SearchResult, UnitRef, VersionSelector,
};
use crate::search_query::{parse_search_query, parse_search_query_strict};

/// High-level service layer that enforces IPTO semantics and delegates persistence
/// to the selected backend implementation.
pub struct RepoService {
    backend: Arc<dyn Backend>,
}

pub const STATUS_PENDING_DISPOSITION: i32 = 1;
pub const STATUS_PENDING_DELETION: i32 = 10;
pub const STATUS_OBLITERATED: i32 = 20;
pub const STATUS_EFFECTIVE: i32 = 30;
pub const STATUS_ARCHIVED: i32 = 40;
const SEARCH_FETCH_CHUNK: i64 = 500;

impl RepoService {
    pub fn new(backend: Arc<dyn Backend>) -> Self {
        Self { backend }
    }

    pub fn get_unit_json(
        &self,
        tenant_id: i64,
        unit_id: i64,
        selector: VersionSelector,
    ) -> RepoResult<Option<Value>> {
        self.backend.get_unit_json(tenant_id, unit_id, selector)
    }

    pub fn get_unit_by_corrid_json(
        &self,
        corrid: &str,
    ) -> RepoResult<Option<Value>> {
        self.backend.get_unit_by_corrid_json(corrid)
    }

    pub fn unit_exists(&self, tenant_id: i64, unit_id: i64) -> RepoResult<bool> {
        self.backend.unit_exists(tenant_id, unit_id)
    }

    pub fn store_unit_json(&self, unit: Value) -> RepoResult<Value> {
        let incoming = unit
            .as_object()
            .ok_or_else(|| RepoError::InvalidInput("unit payload must be object".to_string()))?;

        let tenant_id = required_obj_i64(incoming, "tenantid")?;
        let unit_id = optional_obj_i64(incoming, "unitid");
        let Some(unit_id) = unit_id else {
            // Missing unit id means "new unit" path.
            return self.backend.store_unit_json(unit);
        };

        // Existing unit update path: load current state first so we can enforce
        // readonly/lock/status/version rules before writing.
        let selector = optional_obj_i64(incoming, "unitver")
            .map(VersionSelector::Exact)
            .unwrap_or(VersionSelector::Latest);
        let fetched = self
            .backend
            .get_unit_json(tenant_id, unit_id, selector)?
            .ok_or(RepoError::NotFound)?;
        let fetched_obj = fetched
            .as_object()
            .ok_or_else(|| RepoError::Backend("fetched unit payload must be object".to_string()))?;

        if fetched_obj
            .get("isreadonly")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            return Err(RepoError::InvalidInput(format!(
                "Unit is readonly: {tenant_id}.{unit_id}"
            )));
        }

        let unit_ref = UnitRef {
            tenant_id,
            unit_id,
            version: None,
        };
        if self.backend.is_unit_locked(unit_ref.clone())? {
            return Err(RepoError::AlreadyLocked);
        }

        let current_status = required_obj_i32(fetched_obj, "status")?;
        let requested_status = optional_obj_i64(incoming, "status")
            .map(|v| {
                i32::try_from(v)
                    .map_err(|_| RepoError::InvalidInput(format!("'status' must fit i32: {v}")))
            })
            .transpose()?;
        if let Some(status) = requested_status {
            if !is_known_status(status) {
                return Err(RepoError::InvalidInput(format!("unknown status: {status}")));
            }
        }

        let mut merged = fetched_obj.clone();
        merged.insert("tenantid".to_string(), Value::Number(tenant_id.into()));
        merged.insert("unitid".to_string(), Value::Number(unit_id.into()));
        merged.insert(
            "status".to_string(),
            Value::Number(requested_status.unwrap_or(current_status).into()),
        );

        let mut requires_new_version = false;
        if let Some(name_value) = incoming.get("unitname").filter(|v| !v.is_null()) {
            let name = name_value
                .as_str()
                .ok_or_else(|| RepoError::InvalidInput("unitname must be string".to_string()))?;
            let current = fetched_obj
                .get("unitname")
                .and_then(Value::as_str)
                .unwrap_or_default();
            if current != name {
                requires_new_version = true;
                merged.insert("unitname".to_string(), Value::String(name.to_string()));
            }
        }
        if let Some(attrs) = incoming.get("attributes").and_then(Value::as_array) {
            let new_attrs = Value::Array(attrs.clone());
            if fetched_obj.get("attributes") != Some(&new_attrs) {
                requires_new_version = true;
                merged.insert("attributes".to_string(), new_attrs);
            }
        }

        if !requires_new_version {
            // Preserve Java-style lifecycle behavior: status-only changes go through
            // transition policy and do not create a new unit version.
            let resulting_status = if let Some(status) = requested_status {
                self.request_status_transition(unit_ref.clone(), status)?
            } else {
                current_status
            };
            if resulting_status != current_status {
                return self
                    .backend
                    .get_unit_json(tenant_id, unit_id, VersionSelector::Latest)?
                    .ok_or(RepoError::NotFound);
            }
            return Ok(Value::Object(fetched_obj.clone()));
        }

        self.backend.store_unit_json(Value::Object(merged))
    }

    pub fn search_units(
        &self,
        expression: Value,
        order: SearchOrder,
        paging: SearchPaging,
    ) -> RepoResult<SearchResult> {
        if !contains_set_operation(&expression) {
            return self.backend.search_units(expression, order, paging);
        }
        validate_set_expression_shape(&expression)?;

        // Fast path: backends may implement native set-expression execution.
        match self
            .backend
            .search_units(expression.clone(), order.clone(), paging.clone())
        {
            Ok(native) => return Ok(native),
            Err(RepoError::Unsupported(_)) => {}
            Err(err) => return Err(err),
        }

        // Fallback path: evaluate boolean set operations in service layer by
        // composing backend leaf searches.
        let mut units = self
            .evaluate_set_expression(&expression, &order)?
            .into_values()
            .collect::<Vec<_>>();
        sort_units(&mut units, &order);

        let total_hits = i64::try_from(units.len())
            .map_err(|_| RepoError::Backend("search result size overflow".to_string()))?;
        let results = apply_paging(units, &paging);
        Ok(SearchResult {
            total_hits,
            results,
        })
    }

    pub fn search_units_query(
        &self,
        query: &str,
        order: SearchOrder,
        paging: SearchPaging,
    ) -> RepoResult<SearchResult> {
        let expression = parse_search_query(query)?;
        self.search_units(expression, order, paging)
    }

    pub fn search_units_query_strict(
        &self,
        query: &str,
        order: SearchOrder,
        paging: SearchPaging,
    ) -> RepoResult<SearchResult> {
        let expression = parse_search_query_strict(query)?;
        self.search_units(expression, order, paging)
    }

    pub fn set_status(&self, unit: UnitRef, status: i32) -> RepoResult<()> {
        self.backend.set_status(unit, status)
    }

    pub fn add_relation(
        &self,
        left: UnitRef,
        relation_type: i32,
        right: UnitRef,
    ) -> RepoResult<()> {
        self.backend.add_relation(left, relation_type, right)
    }

    pub fn remove_relation(
        &self,
        left: UnitRef,
        relation_type: i32,
        right: UnitRef,
    ) -> RepoResult<()> {
        self.backend.remove_relation(left, relation_type, right)
    }

    pub fn get_right_relation(
        &self,
        unit: UnitRef,
        relation_type: i32,
    ) -> RepoResult<Option<Relation>> {
        self.backend.get_right_relation(unit, relation_type)
    }

    pub fn get_right_relations(
        &self,
        unit: UnitRef,
        relation_type: i32,
    ) -> RepoResult<Vec<Relation>> {
        self.backend.get_right_relations(unit, relation_type)
    }

    pub fn get_left_relations(
        &self,
        unit: UnitRef,
        relation_type: i32,
    ) -> RepoResult<Vec<Relation>> {
        self.backend.get_left_relations(unit, relation_type)
    }

    pub fn count_right_relations(&self, unit: UnitRef, relation_type: i32) -> RepoResult<i64> {
        self.backend.count_right_relations(unit, relation_type)
    }

    pub fn count_left_relations(&self, unit: UnitRef, relation_type: i32) -> RepoResult<i64> {
        self.backend.count_left_relations(unit, relation_type)
    }

    pub fn add_association(
        &self,
        unit: UnitRef,
        association_type: i32,
        reference: &str,
    ) -> RepoResult<()> {
        self.backend
            .add_association(unit, association_type, reference)
    }

    pub fn remove_association(
        &self,
        unit: UnitRef,
        association_type: i32,
        reference: &str,
    ) -> RepoResult<()> {
        self.backend
            .remove_association(unit, association_type, reference)
    }

    pub fn get_right_association(
        &self,
        unit: UnitRef,
        association_type: i32,
    ) -> RepoResult<Option<Association>> {
        self.backend.get_right_association(unit, association_type)
    }

    pub fn get_right_associations(
        &self,
        unit: UnitRef,
        association_type: i32,
    ) -> RepoResult<Vec<Association>> {
        self.backend.get_right_associations(unit, association_type)
    }

    pub fn get_left_associations(
        &self,
        association_type: i32,
        reference: &str,
    ) -> RepoResult<Vec<Association>> {
        self.backend
            .get_left_associations(association_type, reference)
    }

    pub fn count_right_associations(
        &self,
        unit: UnitRef,
        association_type: i32,
    ) -> RepoResult<i64> {
        self.backend
            .count_right_associations(unit, association_type)
    }

    pub fn count_left_associations(
        &self,
        association_type: i32,
        reference: &str,
    ) -> RepoResult<i64> {
        self.backend
            .count_left_associations(association_type, reference)
    }

    pub fn lock_unit(&self, unit: UnitRef, lock_type: i32, purpose: &str) -> RepoResult<()> {
        self.backend.lock_unit(unit, lock_type, purpose)
    }

    pub fn unlock_unit(&self, unit: UnitRef) -> RepoResult<()> {
        self.backend.unlock_unit(unit)
    }

    pub fn is_unit_locked(&self, unit: UnitRef) -> RepoResult<bool> {
        self.backend.is_unit_locked(unit)
    }

    pub fn activate_unit(&self, unit: UnitRef) -> RepoResult<()> {
        let status = self.current_status(unit.clone())?;
        // Policy: lifecycle helpers are convenience operations and may perform
        // direct transitions even when request_status_transition would reject.
        if status == STATUS_PENDING_DELETION || status == STATUS_OBLITERATED {
            self.backend.set_status(unit, STATUS_EFFECTIVE)?;
        }
        Ok(())
    }

    pub fn inactivate_unit(&self, unit: UnitRef) -> RepoResult<()> {
        let status = self.current_status(unit.clone())?;
        if status == STATUS_EFFECTIVE {
            if self.backend.is_unit_locked(unit.clone())? {
                return Err(RepoError::AlreadyLocked);
            }
            self.backend.set_status(unit, STATUS_PENDING_DELETION)?;
        }
        Ok(())
    }

    pub fn request_status_transition(
        &self,
        unit: UnitRef,
        requested_status: i32,
    ) -> RepoResult<i32> {
        // Policy: this method implements the strict Java transition matrix.
        if !is_known_status(requested_status) {
            return Err(RepoError::InvalidInput(format!(
                "unknown status: {requested_status}"
            )));
        }

        let current_status = self.current_status(unit.clone())?;
        let allowed = match current_status {
            STATUS_ARCHIVED => false,
            STATUS_EFFECTIVE => {
                requested_status == STATUS_PENDING_DELETION
                    || requested_status == STATUS_PENDING_DISPOSITION
            }
            STATUS_PENDING_DELETION => requested_status == STATUS_PENDING_DISPOSITION,
            STATUS_OBLITERATED => requested_status == STATUS_PENDING_DISPOSITION,
            STATUS_PENDING_DISPOSITION => false,
            _ => false,
        };

        if !allowed {
            return Ok(current_status);
        }

        self.backend.set_status(unit, requested_status)?;
        Ok(requested_status)
    }

    pub fn create_attribute(
        &self,
        alias: &str,
        name: &str,
        qualname: &str,
        attribute_type: &str,
        is_array: bool,
    ) -> RepoResult<Value> {
        self.backend
            .create_attribute(alias, name, qualname, attribute_type, is_array)
    }

    pub fn inspect_graphql_sdl(&self, sdl: &str) -> RepoResult<Value> {
        let catalog = parse_graphql_sdl(sdl)?;
        validate_catalog_references(&catalog)?;
        Ok(json!({
            "catalog": catalog_as_json(&catalog),
            "counts": {
                "attributes": catalog.attributes.len(),
                "records": catalog.records.len(),
                "templates": catalog.templates.len()
            }
        }))
    }

    pub fn configure_graphql_sdl(&self, sdl: &str) -> RepoResult<Value> {
        let catalog = parse_graphql_sdl(sdl)?;
        validate_catalog_references(&catalog)?;

        let mut created = Vec::new();
        let mut existing = Vec::new();
        let mut persisted_records = 0usize;
        let mut persisted_templates = 0usize;
        let mut record_persistence_unsupported = false;
        let mut template_persistence_unsupported = false;
        let mut seen_aliases = HashMap::new();
        for attribute in &catalog.attributes {
            // SDL aliases are the stable lookup keys across record/template references.
            if seen_aliases.insert(attribute.alias.clone(), ()).is_some() {
                return Err(RepoError::InvalidInput(format!(
                    "duplicate attribute alias '{}' in SDL registry",
                    attribute.alias
                )));
            }

            if let Some(found) = self.find_existing_attribute(attribute)? {
                // Existing catalog entry must match SDL, otherwise we fail fast
                // rather than mutating incompatible metadata silently.
                self.ensure_attribute_compatible(attribute, &found)?;
                existing.push(found);
                continue;
            }

            let added = self.create_attribute(
                &attribute.alias,
                &attribute.name,
                &attribute.qualname,
                &attribute.attribute_type,
                attribute.is_array,
            )?;
            created.push(added);
        }

        for record in &catalog.records {
            let Some(attr) = self.get_attribute_info(&record.attribute_alias)? else {
                return Err(RepoError::InvalidInput(format!(
                    "record '{}' references unknown attribute '{}'",
                    record.type_name, record.attribute_alias
                )));
            };
            let attr_type = attribute_type_from_info(&attr);
            if !attribute_type_is_record(&attr_type) {
                return Err(RepoError::InvalidInput(format!(
                    "record '{}' requires attribute '{}' to be type RECORD, got '{}'",
                    record.type_name, record.attribute_alias, attr_type
                )));
            }
            for field in &record.fields {
                if self.get_attribute_info(&field.attribute_alias)?.is_none() {
                    return Err(RepoError::InvalidInput(format!(
                        "record '{}.{}' references unknown attribute '{}'",
                        record.type_name, field.field_name, field.attribute_alias
                    )));
                }
            }

            let record_attr_id = required_id_field(&attr, "id")
                .ok_or_else(|| RepoError::Backend("attribute metadata missing id".to_string()))?;
            let mut record_fields = Vec::with_capacity(record.fields.len());
            for field in &record.fields {
                let field_attr = self
                    .get_attribute_info(&field.attribute_alias)?
                    .ok_or_else(|| {
                        RepoError::InvalidInput(format!(
                            "record '{}.{}' references unknown attribute '{}'",
                            record.type_name, field.field_name, field.attribute_alias
                        ))
                    })?;
                let field_attr_id = required_id_field(&field_attr, "id").ok_or_else(|| {
                    RepoError::Backend("attribute metadata missing id".to_string())
                })?;
                record_fields.push((field_attr_id, field.field_name.clone()));
            }
            match self.backend.upsert_record_template(
                record_attr_id,
                &record.type_name,
                &record_fields,
            ) {
                Ok(()) => persisted_records += 1,
                // Some backends (currently Neo4j) do not persist template tables.
                Err(RepoError::Unsupported(_)) => record_persistence_unsupported = true,
                Err(err) => return Err(err),
            }
        }

        for template in &catalog.templates {
            let mut template_fields = Vec::with_capacity(template.fields.len());
            for field in &template.fields {
                let field_attr = self
                    .get_attribute_info(&field.attribute_alias)?
                    .ok_or_else(|| {
                        RepoError::InvalidInput(format!(
                            "template '{}.{}' references unknown attribute '{}'",
                            template.type_name, field.field_name, field.attribute_alias
                        ))
                    })?;
                let field_attr_id = required_id_field(&field_attr, "id").ok_or_else(|| {
                    RepoError::Backend("attribute metadata missing id".to_string())
                })?;
                template_fields.push((field_attr_id, field.field_name.clone()));
            }
            match self
                .backend
                .upsert_unit_template(&template.template_name, &template_fields)
            {
                Ok(()) => persisted_templates += 1,
                Err(RepoError::Unsupported(_)) => template_persistence_unsupported = true,
                Err(err) => return Err(err),
            }
        }

        Ok(json!({
            "catalog": catalog_as_json(&catalog),
            "summary": {
                "attributesCreated": created.len(),
                "attributesExisting": existing.len(),
                "recordSpecsValidated": catalog.records.len(),
                "templateSpecsValidated": catalog.templates.len(),
                "recordTemplatesPersisted": persisted_records,
                "unitTemplatesPersisted": persisted_templates
            },
            "persistence": {
                "recordTemplatesSupported": !record_persistence_unsupported,
                "unitTemplatesSupported": !template_persistence_unsupported
            },
            "createdAttributes": created,
            "existingAttributes": existing
        }))
    }

    pub fn instantiate_attribute(&self, name_or_id: &str) -> RepoResult<Option<Value>> {
        self.backend.instantiate_attribute(name_or_id)
    }

    pub fn can_change_attribute(&self, name_or_id: &str) -> RepoResult<bool> {
        self.backend.can_change_attribute(name_or_id)
    }

    pub fn get_attribute_info(&self, name_or_id: &str) -> RepoResult<Option<Value>> {
        self.backend.get_attribute_info(name_or_id)
    }

    pub fn get_tenant_info(&self, name_or_id: &str) -> RepoResult<Option<Value>> {
        self.backend.get_tenant_info(name_or_id)
    }

    pub fn attribute_name_to_id(&self, attribute_name: &str) -> RepoResult<Option<i64>> {
        let info = self.get_attribute_info(attribute_name)?;
        Ok(info.and_then(|v| get_id(&v)))
    }

    pub fn attribute_id_to_name(&self, attribute_id: i64) -> RepoResult<Option<String>> {
        let info = self.get_attribute_info(&attribute_id.to_string())?;
        Ok(info.and_then(|v| get_name(&v)))
    }

    pub fn tenant_name_to_id(&self, tenant_name: &str) -> RepoResult<Option<i64>> {
        let info = self.get_tenant_info(tenant_name)?;
        Ok(info.and_then(|v| get_id(&v)))
    }

    pub fn tenant_id_to_name(&self, tenant_id: i64) -> RepoResult<Option<String>> {
        let info = self.get_tenant_info(&tenant_id.to_string())?;
        Ok(info.and_then(|v| get_name(&v)))
    }

    pub fn health(&self) -> RepoResult<Value> {
        self.backend.health()
    }

    pub fn flush_cache(&self) -> RepoResult<()> {
        self.backend.flush_cache()
    }

    fn current_status(&self, unit: UnitRef) -> RepoResult<i32> {
        let payload = self
            .backend
            .get_unit_json(unit.tenant_id, unit.unit_id, VersionSelector::Latest)?
            .ok_or(RepoError::NotFound)?;
        payload
            .get("status")
            .and_then(Value::as_i64)
            .and_then(|v| i32::try_from(v).ok())
            .ok_or_else(|| RepoError::Backend("unit payload missing numeric status".to_string()))
    }

    fn find_existing_attribute(
        &self,
        attribute: &crate::graphql_sdl::SdlAttributeSpec,
    ) -> RepoResult<Option<Value>> {
        let mut candidates = vec![
            attribute.alias.as_str(),
            attribute.name.as_str(),
            attribute.qualname.as_str(),
        ];
        candidates.dedup();
        for candidate in candidates {
            if let Some(found) = self.get_attribute_info(candidate)? {
                return Ok(Some(found));
            }
        }
        Ok(None)
    }

    fn ensure_attribute_compatible(
        &self,
        expected: &crate::graphql_sdl::SdlAttributeSpec,
        actual: &Value,
    ) -> RepoResult<()> {
        let actual_alias = actual
            .get("alias")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let actual_name = actual
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let actual_qualname = actual
            .get("qualname")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let actual_type = normalize_attribute_type(&attribute_type_from_info(actual));
        let actual_is_array = actual
            .get("is_array")
            .and_then(Value::as_bool)
            .or_else(|| {
                actual
                    .get("forced_scalar")
                    .and_then(Value::as_bool)
                    .map(|forced_scalar| !forced_scalar)
            })
            .unwrap_or(false);

        let expected_type = normalize_attribute_type(&expected.attribute_type);
        let mismatch = actual_alias != expected.alias
            || actual_name != expected.name
            || actual_qualname != expected.qualname
            || actual_type != expected_type
            || actual_is_array != expected.is_array;
        if mismatch {
            return Err(RepoError::InvalidInput(format!(
                "existing attribute '{}' does not match SDL definition (expected alias='{}' name='{}' qualname='{}' type='{}' is_array={}, got alias='{}' name='{}' qualname='{}' type='{}' is_array={})",
                expected.alias,
                expected.alias,
                expected.name,
                expected.qualname,
                expected_type,
                expected.is_array,
                actual_alias,
                actual_name,
                actual_qualname,
                actual_type,
                actual_is_array
            )));
        }
        Ok(())
    }

    fn evaluate_set_expression(
        &self,
        expression: &Value,
        order: &SearchOrder,
    ) -> RepoResult<HashMap<(i64, i64), crate::model::Unit>> {
        let expr = expression.as_object().ok_or_else(|| {
            RepoError::InvalidInput("search expression must be object".to_string())
        })?;

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

        let leaf = leaf_expression(expr);

        if has_and {
            // AND -> intersection.
            let children = expr.get("and").and_then(Value::as_array).ok_or_else(|| {
                RepoError::InvalidInput("search 'and' must be an array".to_string())
            })?;
            if children.is_empty() {
                return Ok(HashMap::new());
            }

            let mut current = if let Some(leaf_expr) = leaf.clone() {
                self.search_leaf_all(leaf_expr, order)?
            } else {
                let mut iter = children.iter();
                let first = iter.next().ok_or_else(|| {
                    RepoError::InvalidInput("search 'and' must be non-empty".to_string())
                })?;
                let mut seeded = self.evaluate_set_expression(first, order)?;
                for child in iter {
                    let right = self.evaluate_set_expression(child, order)?;
                    seeded.retain(|key, _| right.contains_key(key));
                }
                return Ok(seeded);
            };
            for child in children {
                let right = self.evaluate_set_expression(child, order)?;
                current.retain(|key, _| right.contains_key(key));
            }
            return Ok(current);
        }

        if has_or {
            // OR -> union.
            let children = expr.get("or").and_then(Value::as_array).ok_or_else(|| {
                RepoError::InvalidInput("search 'or' must be an array".to_string())
            })?;
            let mut current = HashMap::new();
            for child in children {
                let right = self.evaluate_set_expression(child, order)?;
                for (key, unit) in right {
                    current.entry(key).or_insert(unit);
                }
            }
            if let Some(leaf_expr) = leaf {
                let limited = self.search_leaf_all(leaf_expr, order)?;
                current.retain(|key, _| limited.contains_key(key));
            }
            return Ok(current);
        }

        if has_not {
            // NOT -> base minus excluded. Base must be tenant-scoped to avoid
            // accidental global scans.
            let not_expr = expr.get("not").and_then(Value::as_object).ok_or_else(|| {
                RepoError::InvalidInput("search 'not' must be an object".to_string())
            })?;

            let base_expr = if let Some(leaf_expr) = leaf {
                leaf_expr
            } else if let Some(tenant_id) = infer_tenant_id(&Value::Object(not_expr.clone())) {
                tenant_only_expression(tenant_id)
            } else {
                return Err(RepoError::InvalidInput(
                    "search 'not' requires tenant scope (directly or in child expression)"
                        .to_string(),
                ));
            };

            let mut base = self.search_leaf_all(base_expr, order)?;
            let excluded = self.evaluate_set_expression(&Value::Object(not_expr.clone()), order)?;
            base.retain(|key, _| !excluded.contains_key(key));
            return Ok(base);
        }

        if let Some(leaf_expr) = leaf {
            return self.search_leaf_all(leaf_expr, order);
        }

        self.search_leaf_all(expression.clone(), order)
    }

    fn search_leaf_all(
        &self,
        expression: Value,
        order: &SearchOrder,
    ) -> RepoResult<HashMap<(i64, i64), crate::model::Unit>> {
        let mut offset = 0_i64;
        let mut seen: HashMap<(i64, i64), crate::model::Unit> = HashMap::new();

        loop {
            let page = self.backend.search_units(
                expression.clone(),
                order.clone(),
                SearchPaging {
                    limit: SEARCH_FETCH_CHUNK,
                    offset,
                },
            )?;

            for unit in page.results {
                seen.entry((unit.tenant_id, unit.unit_id)).or_insert(unit);
            }

            offset += SEARCH_FETCH_CHUNK;
            if offset >= page.total_hits || page.total_hits == 0 {
                break;
            }
        }

        Ok(seen)
    }
}

fn get_id(v: &Value) -> Option<i64> {
    required_id_field(v, "id")
}

fn required_id_field(v: &Value, field: &str) -> Option<i64> {
    v.get(field).and_then(|id| {
        id.as_i64()
            .or_else(|| id.as_u64().and_then(|u| i64::try_from(u).ok()))
            .or_else(|| id.as_str().and_then(|s| s.parse::<i64>().ok()))
    })
}

fn attribute_type_from_info(v: &Value) -> String {
    match v.get("type") {
        Some(raw) => raw
            .as_str()
            .map(str::to_string)
            .or_else(|| raw.as_i64().map(|n| n.to_string()))
            .or_else(|| raw.as_u64().map(|n| n.to_string()))
            .unwrap_or_default(),
        None => String::new(),
    }
}

fn get_name(v: &Value) -> Option<String> {
    v.get("name")
        .and_then(Value::as_str)
        .map(ToString::to_string)
}

fn required_obj_i64(obj: &Map<String, Value>, key: &str) -> RepoResult<i64> {
    optional_obj_i64(obj, key)
        .ok_or_else(|| RepoError::InvalidInput(format!("'{key}' is mandatory, but missing")))
}

fn required_obj_i32(obj: &Map<String, Value>, key: &str) -> RepoResult<i32> {
    let value = required_obj_i64(obj, key)?;
    i32::try_from(value)
        .map_err(|_| RepoError::InvalidInput(format!("'{key}' must fit i32: {value}")))
}

fn optional_obj_i64(obj: &Map<String, Value>, key: &str) -> Option<i64> {
    obj.get(key).and_then(|v| {
        v.as_i64()
            .or_else(|| v.as_u64().and_then(|u| i64::try_from(u).ok()))
            .or_else(|| v.as_str().and_then(|s| s.parse::<i64>().ok()))
    })
}

fn is_known_status(status: i32) -> bool {
    matches!(
        status,
        STATUS_PENDING_DISPOSITION
            | STATUS_PENDING_DELETION
            | STATUS_OBLITERATED
            | STATUS_EFFECTIVE
            | STATUS_ARCHIVED
    )
}

fn contains_set_operation(expression: &Value) -> bool {
    let Some(obj) = expression.as_object() else {
        return false;
    };
    obj.contains_key("and") || obj.contains_key("or") || obj.contains_key("not")
}

fn validate_set_expression_shape(expression: &Value) -> RepoResult<()> {
    let expr = expression
        .as_object()
        .ok_or_else(|| RepoError::InvalidInput("search expression must be object".to_string()))?;

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

    if let Some(children) = expr.get("and") {
        let children = children
            .as_array()
            .ok_or_else(|| RepoError::InvalidInput("search 'and' must be an array".to_string()))?;
        for child in children {
            validate_set_expression_shape(child)?;
        }
    }
    if let Some(children) = expr.get("or") {
        let children = children
            .as_array()
            .ok_or_else(|| RepoError::InvalidInput("search 'or' must be an array".to_string()))?;
        for child in children {
            validate_set_expression_shape(child)?;
        }
    }
    if let Some(not_expr) = expr.get("not") {
        let not_obj = not_expr
            .as_object()
            .ok_or_else(|| RepoError::InvalidInput("search 'not' must be an object".to_string()))?;
        validate_set_expression_shape(&Value::Object(not_obj.clone()))?;
    }
    Ok(())
}

fn leaf_expression(expr: &Map<String, Value>) -> Option<Value> {
    let mut leaf = Map::new();
    for (key, value) in expr {
        if key == "and" || key == "or" || key == "not" {
            continue;
        }
        leaf.insert(key.clone(), value.clone());
    }
    if leaf.is_empty() {
        None
    } else {
        Some(Value::Object(leaf))
    }
}

fn infer_tenant_id(expression: &Value) -> Option<i64> {
    let obj = expression.as_object()?;
    if obj.contains_key("tenantid") {
        if let Some(tenant) = obj.get("tenantid").and_then(Value::as_i64) {
            return Some(tenant);
        }
        if let Some(tenant) = obj
            .get("tenantid")
            .and_then(Value::as_str)
            .and_then(|s| s.parse::<i64>().ok())
        {
            return Some(tenant);
        }
    }
    if obj.contains_key("tenant_id") {
        if let Some(tenant) = obj.get("tenant_id").and_then(Value::as_i64) {
            return Some(tenant);
        }
        if let Some(tenant) = obj
            .get("tenant_id")
            .and_then(Value::as_str)
            .and_then(|s| s.parse::<i64>().ok())
        {
            return Some(tenant);
        }
    }

    if let Some(and_children) = obj.get("and").and_then(Value::as_array) {
        let mut tenant: Option<i64> = None;
        for child in and_children {
            let child_tenant = infer_tenant_id(child)?;
            if let Some(existing) = tenant {
                if existing != child_tenant {
                    return None;
                }
            } else {
                tenant = Some(child_tenant);
            }
        }
        return tenant;
    }

    if let Some(or_children) = obj.get("or").and_then(Value::as_array) {
        let mut tenant: Option<i64> = None;
        for child in or_children {
            let child_tenant = infer_tenant_id(child)?;
            if let Some(existing) = tenant {
                if existing != child_tenant {
                    return None;
                }
            } else {
                tenant = Some(child_tenant);
            }
        }
        return tenant;
    }

    obj.get("not").and_then(infer_tenant_id)
}

fn tenant_only_expression(tenant_id: i64) -> Value {
    let mut map = Map::new();
    map.insert(
        "tenantid".to_string(),
        Value::Number(Number::from(tenant_id)),
    );
    Value::Object(map)
}

fn apply_paging(units: Vec<crate::model::Unit>, paging: &SearchPaging) -> Vec<crate::model::Unit> {
    let start = if paging.offset <= 0 {
        0_usize
    } else {
        usize::try_from(paging.offset).unwrap_or(usize::MAX)
    };
    if start >= units.len() {
        return Vec::new();
    }
    if paging.limit <= 0 {
        return units.into_iter().skip(start).collect();
    }
    let limit = usize::try_from(paging.limit).unwrap_or(usize::MAX);
    units.into_iter().skip(start).take(limit).collect()
}

fn sort_units(units: &mut [crate::model::Unit], order: &SearchOrder) {
    units.sort_by(|left, right| compare_units(left, right, &order.field, order.descending));
}

fn compare_units(
    left: &crate::model::Unit,
    right: &crate::model::Unit,
    field: &str,
    descending: bool,
) -> Ordering {
    let cmp = match field {
        "unitid" => left.unit_id.cmp(&right.unit_id),
        "status" => left.status.cmp(&right.status),
        "modified" => payload_time_key(&left.payload, "modified")
            .cmp(&payload_time_key(&right.payload, "modified")),
        _ => payload_time_key(&left.payload, "created")
            .cmp(&payload_time_key(&right.payload, "created")),
    };

    let ordered = if descending { cmp.reverse() } else { cmp };
    if ordered == Ordering::Equal {
        (left.tenant_id, left.unit_id).cmp(&(right.tenant_id, right.unit_id))
    } else {
        ordered
    }
}

fn payload_time_key(payload: &Value, key: &str) -> i64 {
    let Some(value) = payload.get(key) else {
        return 0;
    };
    if let Some(ts) = value.as_i64() {
        return ts;
    }
    if let Some(s) = value.as_str() {
        if let Ok(v) = s.parse::<i64>() {
            return v;
        }
        if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
            return dt.timestamp_millis();
        }
    }
    0
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use serde_json::json;
    use uuid::Uuid;

    use crate::backend::{Backend, RepoError, RepoResult};
    use crate::model::{
        Association, Relation, SearchOrder, SearchPaging, SearchResult, Unit, UnitRef,
        VersionSelector,
    };

    use super::RepoService;

    #[derive(Clone, Copy)]
    enum ProbeMode {
        Fallback,
        Native,
    }

    struct ProbeBackend {
        mode: ProbeMode,
        seen_expressions: Arc<Mutex<Vec<serde_json::Value>>>,
    }

    impl ProbeBackend {
        fn new(mode: ProbeMode) -> Self {
            Self {
                mode,
                seen_expressions: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn seen(&self) -> Vec<serde_json::Value> {
            self.seen_expressions.lock().expect("probe lock").clone()
        }
    }

    impl Backend for ProbeBackend {
        fn get_unit_json(
            &self,
            _tenant_id: i64,
            _unit_id: i64,
            _selector: VersionSelector,
        ) -> RepoResult<Option<serde_json::Value>> {
            Err(RepoError::Unsupported("probe.get_unit_json".to_string()))
        }

        fn get_unit_by_corrid_json(
            &self,
            _corrid: &str,
        ) -> RepoResult<Option<serde_json::Value>> {
            Err(RepoError::Unsupported(
                "probe.get_unit_by_corrid_json".to_string(),
            ))
        }

        fn unit_exists(&self, _tenant_id: i64, _unit_id: i64) -> RepoResult<bool> {
            Err(RepoError::Unsupported("probe.unit_exists".to_string()))
        }

        fn store_unit_json(&self, _unit: serde_json::Value) -> RepoResult<serde_json::Value> {
            Err(RepoError::Unsupported("probe.store_unit_json".to_string()))
        }

        fn search_units(
            &self,
            expression: serde_json::Value,
            _order: SearchOrder,
            _paging: SearchPaging,
        ) -> RepoResult<SearchResult> {
            self.seen_expressions
                .lock()
                .expect("probe lock")
                .push(expression.clone());

            let has_set = expression
                .as_object()
                .map(|o| o.contains_key("and") || o.contains_key("or") || o.contains_key("not"))
                .unwrap_or(false);

            match (self.mode, has_set) {
                (ProbeMode::Native, true) => Ok(SearchResult {
                    total_hits: 1,
                    results: vec![mk_unit(42, 5001)],
                }),
                (ProbeMode::Fallback, true) => Err(RepoError::Unsupported(
                    "probe.native-set-missing".to_string(),
                )),
                _ => {
                    let unit_id = expression
                        .as_object()
                        .and_then(|o| {
                            o.get("unitid")
                                .and_then(serde_json::Value::as_i64)
                                .or_else(|| {
                                    o.get("predicates")
                                        .and_then(serde_json::Value::as_array)
                                        .and_then(|arr| {
                                            arr.iter().find_map(|entry| {
                                                let obj = entry.as_object()?;
                                                let field = obj.get("field")?.as_str()?;
                                                if field.eq_ignore_ascii_case("unitid")
                                                    || field.eq_ignore_ascii_case("unit_id")
                                                {
                                                    obj.get("value")?.as_i64()
                                                } else {
                                                    None
                                                }
                                            })
                                        })
                                })
                        })
                        .unwrap_or(1001);
                    Ok(SearchResult {
                        total_hits: 1,
                        results: vec![mk_unit(42, unit_id)],
                    })
                }
            }
        }

        fn add_relation(
            &self,
            _left: UnitRef,
            _relation_type: i32,
            _right: UnitRef,
        ) -> RepoResult<()> {
            Err(RepoError::Unsupported("probe.add_relation".to_string()))
        }

        fn remove_relation(
            &self,
            _left: UnitRef,
            _relation_type: i32,
            _right: UnitRef,
        ) -> RepoResult<()> {
            Err(RepoError::Unsupported("probe.remove_relation".to_string()))
        }

        fn get_right_relation(
            &self,
            _unit: UnitRef,
            _relation_type: i32,
        ) -> RepoResult<Option<Relation>> {
            Err(RepoError::Unsupported(
                "probe.get_right_relation".to_string(),
            ))
        }

        fn get_right_relations(
            &self,
            _unit: UnitRef,
            _relation_type: i32,
        ) -> RepoResult<Vec<Relation>> {
            Err(RepoError::Unsupported(
                "probe.get_right_relations".to_string(),
            ))
        }

        fn get_left_relations(
            &self,
            _unit: UnitRef,
            _relation_type: i32,
        ) -> RepoResult<Vec<Relation>> {
            Err(RepoError::Unsupported(
                "probe.get_left_relations".to_string(),
            ))
        }

        fn count_right_relations(&self, _unit: UnitRef, _relation_type: i32) -> RepoResult<i64> {
            Err(RepoError::Unsupported(
                "probe.count_right_relations".to_string(),
            ))
        }

        fn count_left_relations(&self, _unit: UnitRef, _relation_type: i32) -> RepoResult<i64> {
            Err(RepoError::Unsupported(
                "probe.count_left_relations".to_string(),
            ))
        }

        fn add_association(
            &self,
            _unit: UnitRef,
            _association_type: i32,
            _reference: &str,
        ) -> RepoResult<()> {
            Err(RepoError::Unsupported("probe.add_association".to_string()))
        }

        fn remove_association(
            &self,
            _unit: UnitRef,
            _association_type: i32,
            _reference: &str,
        ) -> RepoResult<()> {
            Err(RepoError::Unsupported(
                "probe.remove_association".to_string(),
            ))
        }

        fn get_right_association(
            &self,
            _unit: UnitRef,
            _association_type: i32,
        ) -> RepoResult<Option<Association>> {
            Err(RepoError::Unsupported(
                "probe.get_right_association".to_string(),
            ))
        }

        fn get_right_associations(
            &self,
            _unit: UnitRef,
            _association_type: i32,
        ) -> RepoResult<Vec<Association>> {
            Err(RepoError::Unsupported(
                "probe.get_right_associations".to_string(),
            ))
        }

        fn get_left_associations(
            &self,
            _association_type: i32,
            _reference: &str,
        ) -> RepoResult<Vec<Association>> {
            Err(RepoError::Unsupported(
                "probe.get_left_associations".to_string(),
            ))
        }

        fn count_right_associations(
            &self,
            _unit: UnitRef,
            _association_type: i32,
        ) -> RepoResult<i64> {
            Err(RepoError::Unsupported(
                "probe.count_right_associations".to_string(),
            ))
        }

        fn count_left_associations(
            &self,
            _association_type: i32,
            _reference: &str,
        ) -> RepoResult<i64> {
            Err(RepoError::Unsupported(
                "probe.count_left_associations".to_string(),
            ))
        }

        fn lock_unit(&self, _unit: UnitRef, _lock_type: i32, _purpose: &str) -> RepoResult<()> {
            Err(RepoError::Unsupported("probe.lock_unit".to_string()))
        }

        fn unlock_unit(&self, _unit: UnitRef) -> RepoResult<()> {
            Err(RepoError::Unsupported("probe.unlock_unit".to_string()))
        }

        fn is_unit_locked(&self, _unit: UnitRef) -> RepoResult<bool> {
            Err(RepoError::Unsupported("probe.is_unit_locked".to_string()))
        }

        fn set_status(&self, _unit: UnitRef, _status: i32) -> RepoResult<()> {
            Err(RepoError::Unsupported("probe.set_status".to_string()))
        }

        fn create_attribute(
            &self,
            _alias: &str,
            _name: &str,
            _qualname: &str,
            _attribute_type: &str,
            _is_array: bool,
        ) -> RepoResult<serde_json::Value> {
            Err(RepoError::Unsupported("probe.create_attribute".to_string()))
        }

        fn instantiate_attribute(
            &self,
            _name_or_id: &str,
        ) -> RepoResult<Option<serde_json::Value>> {
            Err(RepoError::Unsupported(
                "probe.instantiate_attribute".to_string(),
            ))
        }

        fn can_change_attribute(&self, _name_or_id: &str) -> RepoResult<bool> {
            Err(RepoError::Unsupported(
                "probe.can_change_attribute".to_string(),
            ))
        }

        fn get_attribute_info(&self, _name_or_id: &str) -> RepoResult<Option<serde_json::Value>> {
            Err(RepoError::Unsupported(
                "probe.get_attribute_info".to_string(),
            ))
        }

        fn get_tenant_info(&self, _name_or_id: &str) -> RepoResult<Option<serde_json::Value>> {
            Err(RepoError::Unsupported("probe.get_tenant_info".to_string()))
        }
    }

    struct StoreProbeBackend {
        state: Arc<Mutex<StoreProbeState>>,
    }

    #[derive(Clone)]
    struct StoreProbeState {
        latest: serde_json::Value,
        readonly: Option<serde_json::Value>,
        locked: bool,
        store_calls: usize,
        status_updates: usize,
    }

    impl StoreProbeBackend {
        fn new(
            latest: serde_json::Value,
            readonly: Option<serde_json::Value>,
            locked: bool,
        ) -> Self {
            Self {
                state: Arc::new(Mutex::new(StoreProbeState {
                    latest,
                    readonly,
                    locked,
                    store_calls: 0,
                    status_updates: 0,
                })),
            }
        }

        fn snapshot(&self) -> StoreProbeState {
            self.state.lock().expect("store probe lock").clone()
        }
    }

    impl Backend for StoreProbeBackend {
        fn get_unit_json(
            &self,
            _tenant_id: i64,
            _unit_id: i64,
            selector: VersionSelector,
        ) -> RepoResult<Option<serde_json::Value>> {
            let state = self.state.lock().expect("store probe lock");
            match selector {
                VersionSelector::Latest => Ok(Some(state.latest.clone())),
                VersionSelector::Exact(version) => {
                    if let Some(readonly) = &state.readonly {
                        let matches = readonly
                            .get("unitver")
                            .and_then(serde_json::Value::as_i64)
                            .is_some_and(|v| v == version);
                        if matches {
                            return Ok(Some(readonly.clone()));
                        }
                    }
                    let matches_latest = state
                        .latest
                        .get("unitver")
                        .and_then(serde_json::Value::as_i64)
                        .is_some_and(|v| v == version);
                    Ok(if matches_latest {
                        Some(state.latest.clone())
                    } else {
                        None
                    })
                }
            }
        }

        fn get_unit_by_corrid_json(
            &self,
            _corrid: &str,
        ) -> RepoResult<Option<serde_json::Value>> {
            Err(RepoError::Unsupported(
                "store_probe.get_unit_by_corrid_json".to_string(),
            ))
        }

        fn unit_exists(&self, _tenant_id: i64, _unit_id: i64) -> RepoResult<bool> {
            Ok(true)
        }

        fn store_unit_json(&self, unit: serde_json::Value) -> RepoResult<serde_json::Value> {
            let mut obj = unit
                .as_object()
                .cloned()
                .ok_or_else(|| RepoError::InvalidInput("unit must be object".to_string()))?;
            let mut state = self.state.lock().expect("store probe lock");
            state.store_calls += 1;
            let next_ver = state
                .latest
                .get("unitver")
                .and_then(serde_json::Value::as_i64)
                .unwrap_or(1)
                + 1;
            obj.insert("unitver".to_string(), json!(next_ver));
            obj.insert("isreadonly".to_string(), json!(false));
            let stored = serde_json::Value::Object(obj);
            state.latest = stored.clone();
            Ok(stored)
        }

        fn search_units(
            &self,
            _expression: serde_json::Value,
            _order: SearchOrder,
            _paging: SearchPaging,
        ) -> RepoResult<SearchResult> {
            Err(RepoError::Unsupported(
                "store_probe.search_units".to_string(),
            ))
        }

        fn add_relation(
            &self,
            _left: UnitRef,
            _relation_type: i32,
            _right: UnitRef,
        ) -> RepoResult<()> {
            Err(RepoError::Unsupported(
                "store_probe.add_relation".to_string(),
            ))
        }

        fn remove_relation(
            &self,
            _left: UnitRef,
            _relation_type: i32,
            _right: UnitRef,
        ) -> RepoResult<()> {
            Err(RepoError::Unsupported(
                "store_probe.remove_relation".to_string(),
            ))
        }

        fn get_right_relation(
            &self,
            _unit: UnitRef,
            _relation_type: i32,
        ) -> RepoResult<Option<Relation>> {
            Err(RepoError::Unsupported(
                "store_probe.get_right_relation".to_string(),
            ))
        }

        fn get_right_relations(
            &self,
            _unit: UnitRef,
            _relation_type: i32,
        ) -> RepoResult<Vec<Relation>> {
            Err(RepoError::Unsupported(
                "store_probe.get_right_relations".to_string(),
            ))
        }

        fn get_left_relations(
            &self,
            _unit: UnitRef,
            _relation_type: i32,
        ) -> RepoResult<Vec<Relation>> {
            Err(RepoError::Unsupported(
                "store_probe.get_left_relations".to_string(),
            ))
        }

        fn count_right_relations(&self, _unit: UnitRef, _relation_type: i32) -> RepoResult<i64> {
            Err(RepoError::Unsupported(
                "store_probe.count_right_relations".to_string(),
            ))
        }

        fn count_left_relations(&self, _unit: UnitRef, _relation_type: i32) -> RepoResult<i64> {
            Err(RepoError::Unsupported(
                "store_probe.count_left_relations".to_string(),
            ))
        }

        fn add_association(
            &self,
            _unit: UnitRef,
            _association_type: i32,
            _reference: &str,
        ) -> RepoResult<()> {
            Err(RepoError::Unsupported(
                "store_probe.add_association".to_string(),
            ))
        }

        fn remove_association(
            &self,
            _unit: UnitRef,
            _association_type: i32,
            _reference: &str,
        ) -> RepoResult<()> {
            Err(RepoError::Unsupported(
                "store_probe.remove_association".to_string(),
            ))
        }

        fn get_right_association(
            &self,
            _unit: UnitRef,
            _association_type: i32,
        ) -> RepoResult<Option<Association>> {
            Err(RepoError::Unsupported(
                "store_probe.get_right_association".to_string(),
            ))
        }

        fn get_right_associations(
            &self,
            _unit: UnitRef,
            _association_type: i32,
        ) -> RepoResult<Vec<Association>> {
            Err(RepoError::Unsupported(
                "store_probe.get_right_associations".to_string(),
            ))
        }

        fn get_left_associations(
            &self,
            _association_type: i32,
            _reference: &str,
        ) -> RepoResult<Vec<Association>> {
            Err(RepoError::Unsupported(
                "store_probe.get_left_associations".to_string(),
            ))
        }

        fn count_right_associations(
            &self,
            _unit: UnitRef,
            _association_type: i32,
        ) -> RepoResult<i64> {
            Err(RepoError::Unsupported(
                "store_probe.count_right_associations".to_string(),
            ))
        }

        fn count_left_associations(
            &self,
            _association_type: i32,
            _reference: &str,
        ) -> RepoResult<i64> {
            Err(RepoError::Unsupported(
                "store_probe.count_left_associations".to_string(),
            ))
        }

        fn lock_unit(&self, _unit: UnitRef, _lock_type: i32, _purpose: &str) -> RepoResult<()> {
            Err(RepoError::Unsupported("store_probe.lock_unit".to_string()))
        }

        fn unlock_unit(&self, _unit: UnitRef) -> RepoResult<()> {
            Err(RepoError::Unsupported(
                "store_probe.unlock_unit".to_string(),
            ))
        }

        fn is_unit_locked(&self, _unit: UnitRef) -> RepoResult<bool> {
            Ok(self.state.lock().expect("store probe lock").locked)
        }

        fn set_status(&self, _unit: UnitRef, status: i32) -> RepoResult<()> {
            let mut state = self.state.lock().expect("store probe lock");
            state.status_updates += 1;
            if let Some(obj) = state.latest.as_object_mut() {
                obj.insert("status".to_string(), json!(status));
            }
            Ok(())
        }

        fn create_attribute(
            &self,
            _alias: &str,
            _name: &str,
            _qualname: &str,
            _attribute_type: &str,
            _is_array: bool,
        ) -> RepoResult<serde_json::Value> {
            Err(RepoError::Unsupported(
                "store_probe.create_attribute".to_string(),
            ))
        }

        fn instantiate_attribute(
            &self,
            _name_or_id: &str,
        ) -> RepoResult<Option<serde_json::Value>> {
            Err(RepoError::Unsupported(
                "store_probe.instantiate_attribute".to_string(),
            ))
        }

        fn can_change_attribute(&self, _name_or_id: &str) -> RepoResult<bool> {
            Err(RepoError::Unsupported(
                "store_probe.can_change_attribute".to_string(),
            ))
        }

        fn get_attribute_info(&self, _name_or_id: &str) -> RepoResult<Option<serde_json::Value>> {
            Err(RepoError::Unsupported(
                "store_probe.get_attribute_info".to_string(),
            ))
        }

        fn get_tenant_info(&self, _name_or_id: &str) -> RepoResult<Option<serde_json::Value>> {
            Err(RepoError::Unsupported(
                "store_probe.get_tenant_info".to_string(),
            ))
        }
    }

    fn mk_unit(tenant_id: i64, unit_id: i64) -> Unit {
        Unit {
            tenant_id,
            unit_id,
            unit_ver: 1,
            status: 30,
            name: Some(format!("unit-{unit_id}")),
            corr_id: Uuid::nil(),
            payload: json!({
                "tenantid": tenant_id,
                "unitid": unit_id,
                "unitver": 1,
                "status": 30,
                "created": "2026-01-01T00:00:00Z",
                "modified": "2026-01-01T00:00:00Z"
            }),
        }
    }

    #[test]
    fn search_set_expression_uses_native_when_available() {
        let backend = Arc::new(ProbeBackend::new(ProbeMode::Native));
        let repo = RepoService::new(backend.clone());
        let result = repo
            .search_units(
                json!({"or": [{"unitid": 1001}, {"unitid": 1002}]}),
                SearchOrder {
                    field: "created".to_string(),
                    descending: true,
                },
                SearchPaging {
                    limit: 20,
                    offset: 0,
                },
            )
            .expect("native set expression search should succeed");
        assert_eq!(result.total_hits, 1);
        assert_eq!(result.results[0].unit_id, 5001);
        let seen = backend.seen();
        assert_eq!(seen.len(), 1);
        assert!(seen[0].get("or").is_some());
    }

    #[test]
    fn search_set_expression_falls_back_on_unsupported_native() {
        let backend = Arc::new(ProbeBackend::new(ProbeMode::Fallback));
        let repo = RepoService::new(backend.clone());
        let result = repo
            .search_units(
                json!({"or": [{"unitid": 1001}, {"unitid": 1002}]}),
                SearchOrder {
                    field: "created".to_string(),
                    descending: true,
                },
                SearchPaging {
                    limit: 20,
                    offset: 0,
                },
            )
            .expect("fallback set expression search should succeed");
        assert_eq!(result.total_hits, 2);
        assert!(result.results.iter().any(|u| u.unit_id == 1001));
        assert!(result.results.iter().any(|u| u.unit_id == 1002));

        let seen = backend.seen();
        assert!(seen.len() >= 3);
        assert!(seen[0].get("or").is_some());
        assert!(seen.iter().skip(1).all(|expr| {
            expr.as_object().is_some_and(|o| {
                !o.contains_key("and") && !o.contains_key("or") && !o.contains_key("not")
            })
        }));
    }

    #[test]
    fn search_set_expression_rejects_invalid_mixed_operators_before_backend() {
        let backend = Arc::new(ProbeBackend::new(ProbeMode::Native));
        let repo = RepoService::new(backend.clone());
        let err = repo
            .search_units(
                json!({"and": [{"unitid": 1001}], "or": [{"unitid": 1002}]}),
                SearchOrder {
                    field: "created".to_string(),
                    descending: true,
                },
                SearchPaging {
                    limit: 20,
                    offset: 0,
                },
            )
            .expect_err("mixed root operators should be rejected");
        assert!(matches!(err, RepoError::InvalidInput(_)));
        assert!(backend.seen().is_empty());
    }

    #[test]
    fn search_query_expression_falls_back_like_json_path() {
        let backend = Arc::new(ProbeBackend::new(ProbeMode::Fallback));
        let repo = RepoService::new(backend.clone());
        let parsed = crate::search_query::parse_search_query("unitid = 1001 or unitid = 1002")
            .expect("query should parse");
        assert!(parsed.get("or").is_some(), "parsed expression: {parsed}");
        let result = repo
            .search_units_query(
                "unitid = 1001 or unitid = 1002",
                SearchOrder {
                    field: "created".to_string(),
                    descending: true,
                },
                SearchPaging {
                    limit: 20,
                    offset: 0,
                },
            )
            .expect("query-based search should succeed");
        assert_eq!(result.total_hits, 2);
        assert!(result.results.iter().any(|u| u.unit_id == 1001));
        assert!(result.results.iter().any(|u| u.unit_id == 1002));
        let seen = backend.seen();
        assert!(!seen.is_empty());
        assert!(seen[0].get("or").is_some());
    }

    #[test]
    fn search_query_expression_rejects_invalid_input() {
        let backend = Arc::new(ProbeBackend::new(ProbeMode::Native));
        let repo = RepoService::new(backend.clone());
        let err = repo
            .search_units_query(
                "tenantid =",
                SearchOrder {
                    field: "created".to_string(),
                    descending: true,
                },
                SearchPaging {
                    limit: 20,
                    offset: 0,
                },
            )
            .expect_err("invalid query must fail");
        assert!(matches!(err, RepoError::InvalidInput(_)));
        assert!(backend.seen().is_empty());
    }

    #[test]
    fn search_query_strict_requires_attr_prefix_for_unknown_fields() {
        let backend = Arc::new(ProbeBackend::new(ProbeMode::Native));
        let repo = RepoService::new(backend.clone());

        let err = repo
            .search_units_query_strict(
                "custom_attr = 10",
                SearchOrder {
                    field: "created".to_string(),
                    descending: true,
                },
                SearchPaging {
                    limit: 20,
                    offset: 0,
                },
            )
            .expect_err("strict query must reject implicit attribute field");
        assert!(matches!(err, RepoError::InvalidInput(_)));
        assert!(backend.seen().is_empty());

        let _ = repo
            .search_units_query_strict(
                "attr:custom_attr = 10",
                SearchOrder {
                    field: "created".to_string(),
                    descending: true,
                },
                SearchPaging {
                    limit: 20,
                    offset: 0,
                },
            )
            .expect("strict query with attr prefix should pass");
        let seen = backend.seen();
        assert_eq!(seen.len(), 1);
        assert!(seen[0].get("attribute_cmp").is_some());
    }

    #[test]
    fn store_unit_json_rejects_readonly_exact_version() {
        let backend = Arc::new(StoreProbeBackend::new(
            json!({"tenantid": 42, "unitid": 1001, "unitver": 2, "status": 30, "isreadonly": false, "unitname": "latest"}),
            Some(
                json!({"tenantid": 42, "unitid": 1001, "unitver": 1, "status": 30, "isreadonly": true, "unitname": "old"}),
            ),
            false,
        ));
        let repo = RepoService::new(backend.clone());
        let err = repo
            .store_unit_json(json!({
                "tenantid": 42,
                "unitid": 1001,
                "unitver": 1,
                "unitname": "attempted-update"
            }))
            .expect_err("readonly version update should fail");
        assert!(matches!(err, RepoError::InvalidInput(_)));
        let state = backend.snapshot();
        assert_eq!(state.store_calls, 0);
        assert_eq!(state.status_updates, 0);
    }

    #[test]
    fn store_unit_json_rejects_locked_unit() {
        let backend = Arc::new(StoreProbeBackend::new(
            json!({"tenantid": 42, "unitid": 1001, "unitver": 2, "status": 30, "isreadonly": false, "unitname": "latest"}),
            None,
            true,
        ));
        let repo = RepoService::new(backend.clone());
        let err = repo
            .store_unit_json(json!({
                "tenantid": 42,
                "unitid": 1001,
                "unitname": "attempted-update"
            }))
            .expect_err("locked unit update should fail");
        assert!(matches!(err, RepoError::AlreadyLocked));
        let state = backend.snapshot();
        assert_eq!(state.store_calls, 0);
    }

    #[test]
    fn store_unit_json_status_only_does_not_create_new_version() {
        let backend = Arc::new(StoreProbeBackend::new(
            json!({"tenantid": 42, "unitid": 1001, "unitver": 2, "status": 30, "isreadonly": false, "unitname": "latest"}),
            None,
            false,
        ));
        let repo = RepoService::new(backend.clone());
        let stored = repo
            .store_unit_json(json!({
                "tenantid": 42,
                "unitid": 1001,
                "status": 10
            }))
            .expect("status-only update should succeed");
        assert_eq!(stored["unitver"], 2);
        assert_eq!(stored["status"], 10);
        let state = backend.snapshot();
        assert_eq!(state.status_updates, 1);
        assert_eq!(state.store_calls, 0);
    }

    #[test]
    fn store_unit_json_name_change_creates_new_version() {
        let backend = Arc::new(StoreProbeBackend::new(
            json!({"tenantid": 42, "unitid": 1001, "unitver": 2, "status": 30, "isreadonly": false, "unitname": "latest"}),
            None,
            false,
        ));
        let repo = RepoService::new(backend.clone());
        let stored = repo
            .store_unit_json(json!({
                "tenantid": 42,
                "unitid": 1001,
                "unitname": "renamed"
            }))
            .expect("name change should store new version");
        assert_eq!(stored["unitver"], 3);
        assert_eq!(stored["unitname"], "renamed");
        let state = backend.snapshot();
        assert_eq!(state.store_calls, 1);
    }
}
