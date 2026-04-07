use std::collections::HashSet;

use serde_json::Value;

use crate::backend::{RepoError, RepoResult};
use crate::model::{SearchOrder, SearchPaging, VersionSelector};
use crate::repo::RepoService;

#[derive(Clone, Copy, Eq, PartialEq)]
enum OperationKind {
    Query,
    Mutation,
}

impl OperationKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Query => "query",
            Self::Mutation => "mutation",
        }
    }
}

#[derive(Clone, Copy)]
struct OperationSpec {
    canonical: &'static str,
    display: &'static str,
    kind: OperationKind,
}

// Static operation registry used both for execution dispatch and
// allowlist/introspection behavior.
const SUPPORTED_OPERATION_SPECS: &[OperationSpec] = &[
    OperationSpec {
        canonical: "searchunits",
        display: "searchUnits",
        kind: OperationKind::Query,
    },
    OperationSpec {
        canonical: "search",
        display: "search",
        kind: OperationKind::Query,
    },
    OperationSpec {
        canonical: "units",
        display: "units",
        kind: OperationKind::Query,
    },
    OperationSpec {
        canonical: "searchunit",
        display: "searchUnit",
        kind: OperationKind::Query,
    },
    OperationSpec {
        canonical: "searchunitsquery",
        display: "searchUnitsQuery",
        kind: OperationKind::Query,
    },
    OperationSpec {
        canonical: "searchunitsquerystrict",
        display: "searchUnitsQueryStrict",
        kind: OperationKind::Query,
    },
    OperationSpec {
        canonical: "searchraw",
        display: "searchRaw",
        kind: OperationKind::Query,
    },
    OperationSpec {
        canonical: "searchrawpayload",
        display: "searchRawPayload",
        kind: OperationKind::Query,
    },
    OperationSpec {
        canonical: "unitsraw",
        display: "unitsRaw",
        kind: OperationKind::Query,
    },
    OperationSpec {
        canonical: "registeredoperations",
        display: "registeredOperations",
        kind: OperationKind::Query,
    },
    OperationSpec {
        canonical: "unitexists",
        display: "unitExists",
        kind: OperationKind::Query,
    },
    OperationSpec {
        canonical: "unitlifecycle",
        display: "unitLifecycle",
        kind: OperationKind::Query,
    },
    OperationSpec {
        canonical: "getunit",
        display: "getUnit",
        kind: OperationKind::Query,
    },
    OperationSpec {
        canonical: "unit",
        display: "unit",
        kind: OperationKind::Query,
    },
    OperationSpec {
        canonical: "unitraw",
        display: "unitRaw",
        kind: OperationKind::Query,
    },
    OperationSpec {
        canonical: "loadunit",
        display: "loadUnit",
        kind: OperationKind::Query,
    },
    OperationSpec {
        canonical: "loadunitraw",
        display: "loadUnitRaw",
        kind: OperationKind::Query,
    },
    OperationSpec {
        canonical: "getunitmeta",
        display: "getUnitMeta",
        kind: OperationKind::Query,
    },
    OperationSpec {
        canonical: "getunitstatus",
        display: "getUnitStatus",
        kind: OperationKind::Query,
    },
    OperationSpec {
        canonical: "getunitbycorrid",
        display: "getUnitByCorrid",
        kind: OperationKind::Query,
    },
    OperationSpec {
        canonical: "unitbycorrid",
        display: "unitByCorrid",
        kind: OperationKind::Query,
    },
    OperationSpec {
        canonical: "loadbycorrid",
        display: "loadByCorrId",
        kind: OperationKind::Query,
    },
    OperationSpec {
        canonical: "unitrawbycorrid",
        display: "unitRawByCorrid",
        kind: OperationKind::Query,
    },
    OperationSpec {
        canonical: "loadrawpayloadbycorrid",
        display: "loadRawPayloadByCorrId",
        kind: OperationKind::Query,
    },
    OperationSpec {
        canonical: "getattributeinfo",
        display: "getAttributeInfo",
        kind: OperationKind::Query,
    },
    OperationSpec {
        canonical: "instantiateattribute",
        display: "instantiateAttribute",
        kind: OperationKind::Query,
    },
    OperationSpec {
        canonical: "attributenametoid",
        display: "attributeNameToId",
        kind: OperationKind::Query,
    },
    OperationSpec {
        canonical: "attributeidtoname",
        display: "attributeIdToName",
        kind: OperationKind::Query,
    },
    OperationSpec {
        canonical: "gettenantinfo",
        display: "getTenantInfo",
        kind: OperationKind::Query,
    },
    OperationSpec {
        canonical: "isunitlocked",
        display: "isUnitLocked",
        kind: OperationKind::Query,
    },
    OperationSpec {
        canonical: "tenantnametoid",
        display: "tenantNameToId",
        kind: OperationKind::Query,
    },
    OperationSpec {
        canonical: "tenantidtoname",
        display: "tenantIdToName",
        kind: OperationKind::Query,
    },
    OperationSpec {
        canonical: "canchangeattribute",
        display: "canChangeAttribute",
        kind: OperationKind::Query,
    },
    OperationSpec {
        canonical: "getrightrelation",
        display: "getRightRelation",
        kind: OperationKind::Query,
    },
    OperationSpec {
        canonical: "getrightrelations",
        display: "getRightRelations",
        kind: OperationKind::Query,
    },
    OperationSpec {
        canonical: "getleftrelations",
        display: "getLeftRelations",
        kind: OperationKind::Query,
    },
    OperationSpec {
        canonical: "countrightrelations",
        display: "countRightRelations",
        kind: OperationKind::Query,
    },
    OperationSpec {
        canonical: "countleftrelations",
        display: "countLeftRelations",
        kind: OperationKind::Query,
    },
    OperationSpec {
        canonical: "getrightassociation",
        display: "getRightAssociation",
        kind: OperationKind::Query,
    },
    OperationSpec {
        canonical: "getrightassociations",
        display: "getRightAssociations",
        kind: OperationKind::Query,
    },
    OperationSpec {
        canonical: "getleftassociations",
        display: "getLeftAssociations",
        kind: OperationKind::Query,
    },
    OperationSpec {
        canonical: "countrightassociations",
        display: "countRightAssociations",
        kind: OperationKind::Query,
    },
    OperationSpec {
        canonical: "countleftassociations",
        display: "countLeftAssociations",
        kind: OperationKind::Query,
    },
    OperationSpec {
        canonical: "health",
        display: "health",
        kind: OperationKind::Query,
    },
    OperationSpec {
        canonical: "setstatus",
        display: "setStatus",
        kind: OperationKind::Mutation,
    },
    OperationSpec {
        canonical: "storeunit",
        display: "storeUnit",
        kind: OperationKind::Mutation,
    },
    OperationSpec {
        canonical: "lagraunitraw",
        display: "lagraUnitRaw",
        kind: OperationKind::Mutation,
    },
    OperationSpec {
        canonical: "storerawunit",
        display: "storeRawUnit",
        kind: OperationKind::Mutation,
    },
    OperationSpec {
        canonical: "createunit",
        display: "createUnit",
        kind: OperationKind::Mutation,
    },
    OperationSpec {
        canonical: "activateunit",
        display: "activateUnit",
        kind: OperationKind::Mutation,
    },
    OperationSpec {
        canonical: "inactivateunit",
        display: "inactivateUnit",
        kind: OperationKind::Mutation,
    },
    OperationSpec {
        canonical: "requeststatustransition",
        display: "requestStatusTransition",
        kind: OperationKind::Mutation,
    },
    OperationSpec {
        canonical: "transitionunitstatus",
        display: "transitionUnitStatus",
        kind: OperationKind::Mutation,
    },
    OperationSpec {
        canonical: "lockunit",
        display: "lockUnit",
        kind: OperationKind::Mutation,
    },
    OperationSpec {
        canonical: "unlockunit",
        display: "unlockUnit",
        kind: OperationKind::Mutation,
    },
    OperationSpec {
        canonical: "addrelation",
        display: "addRelation",
        kind: OperationKind::Mutation,
    },
    OperationSpec {
        canonical: "removerelation",
        display: "removeRelation",
        kind: OperationKind::Mutation,
    },
    OperationSpec {
        canonical: "addassociation",
        display: "addAssociation",
        kind: OperationKind::Mutation,
    },
    OperationSpec {
        canonical: "removeassociation",
        display: "removeAssociation",
        kind: OperationKind::Mutation,
    },
    OperationSpec {
        canonical: "createattribute",
        display: "createAttribute",
        kind: OperationKind::Mutation,
    },
    OperationSpec {
        canonical: "instantiateattributemutation",
        display: "instantiateAttributeMutation",
        kind: OperationKind::Mutation,
    },
    OperationSpec {
        canonical: "flushcache",
        display: "flushCache",
        kind: OperationKind::Mutation,
    },
];

pub struct GraphqlRuntime {
    repo: RepoService,
    allowed_operations: HashSet<&'static str>,
}

impl GraphqlRuntime {
    pub fn new(repo: RepoService) -> Self {
        Self {
            repo,
            allowed_operations: SUPPORTED_OPERATION_SPECS
                .iter()
                .map(|spec| spec.canonical)
                .collect(),
        }
    }

    pub fn with_operation_allowlist(repo: RepoService, allowlist: &[&str]) -> RepoResult<Self> {
        let mut allowed_operations = HashSet::new();
        for operation in allowlist {
            let canonical = canonical_operation_name(operation).ok_or_else(|| {
                RepoError::InvalidInput(format!(
                    "unknown graphql operation in allowlist: {operation}"
                ))
            })?;
            allowed_operations.insert(canonical);
        }
        Ok(Self {
            repo,
            allowed_operations,
        })
    }

    pub fn registered_operations(&self) -> Value {
        serde_json::json!({
            "queries": self.allowed_operations_for_kind(OperationKind::Query),
            "mutations": self.allowed_operations_for_kind(OperationKind::Mutation)
        })
    }

    pub fn execute(&self, query: &str, variables: Option<Value>) -> RepoResult<Value> {
        match self.execute_impl(query, variables) {
            Ok(response) => Ok(response),
            Err(err) => Ok(graphql_error_response(err)),
        }
    }

    fn execute_impl(&self, query: &str, variables: Option<Value>) -> RepoResult<Value> {
        // This runtime intentionally executes a constrained operation subset by
        // root-field dispatch, rather than full GraphQL schema execution.
        let operation_kind = detect_operation_kind(query);
        let operation_raw = extract_root_field(query).unwrap_or_default();
        let operation = match canonical_operation_name(&operation_raw) {
            Some(operation) => operation,
            None => {
                return Ok(graphql_unsupported_operation_response(
                    &operation_raw,
                    operation_kind,
                    self.allowed_operations_for_kind(operation_kind),
                ));
            }
        };
        if !self.allowed_operations.contains(operation)
            || operation_kind_for(operation) != Some(operation_kind)
        {
            return Ok(graphql_unsupported_operation_response(
                &operation_raw,
                operation_kind,
                self.allowed_operations_for_kind(operation_kind),
            ));
        }
        let vars = variables
            .as_ref()
            .and_then(Value::as_object)
            .cloned()
            .unwrap_or_default();

        if operation_kind == OperationKind::Mutation {
            // Mutations are handled explicitly to keep semantics and payload
            // shape aligned with Java compatibility aliases.
            if operation == "setstatus" {
                let (tenant_id, unit_id) = required_unit_identification_var(&vars)?;
                let status = required_i32_var(&vars, &["status"])?;
                self.repo.set_status(
                    crate::model::UnitRef {
                        tenant_id,
                        unit_id,
                        version: None,
                    },
                    status,
                )?;
                return Ok(serde_json::json!({
                    "data": {
                        "setStatus": {
                            "ok": true,
                            "status": status
                        }
                    }
                }));
            }

            if operation == "storeunit" {
                let unit = required_object_var(&vars, &["unit", "payload"])?;
                let stored = self.repo.store_unit_json(Value::Object(unit))?;
                return Ok(serde_json::json!({
                    "data": {
                        "storeUnit": stored
                    }
                }));
            }

            if operation == "lagraunitraw" || operation == "storerawunit" {
                let data = required_string_var(&vars, &["data"])?;
                let raw_bytes = decode_base64(&data, "data")?;
                let unit: Value = serde_json::from_slice(&raw_bytes).map_err(|err| {
                    RepoError::InvalidInput(format!(
                        "graphql var 'data' must decode to valid JSON: {err}"
                    ))
                })?;
                let stored = self.repo.store_unit_json(unit)?;
                let encoded = encode_json_base64(&stored)?;
                let root_field = if operation == "storerawunit" {
                    "storeRawUnit"
                } else {
                    "lagraUnitRaw"
                };
                return Ok(serde_json::json!({
                    "data": {
                        (root_field): encoded
                    }
                }));
            }

            if operation == "createunit" {
                let tenant_id = required_i64_var(&vars, &["tenantid", "tenantId"])?;
                let mut unit = serde_json::Map::new();
                unit.insert("tenantid".to_string(), Value::Number(tenant_id.into()));
                if let Some(unit_name) =
                    optional_string_var(&vars, &["unitname", "unitName", "name"])
                {
                    unit.insert("unitname".to_string(), Value::String(unit_name));
                }
                if let Some(corrid) = optional_string_var(&vars, &["corrid", "corrId"]) {
                    unit.insert("corrid".to_string(), Value::String(corrid));
                }
                if let Some(status) = optional_i64_var(&vars, &["status"]) {
                    unit.insert("status".to_string(), Value::Number(status.into()));
                }
                let created = self.repo.store_unit_json(Value::Object(unit))?;
                return Ok(serde_json::json!({
                    "data": {
                        "createUnit": created
                    }
                }));
            }

            if operation == "inactivateunit" {
                let (tenant_id, unit_id) = required_unit_identification_var(&vars)?;
                self.repo.inactivate_unit(crate::model::UnitRef {
                    tenant_id,
                    unit_id,
                    version: None,
                })?;
                return Ok(serde_json::json!({
                    "data": { "inactivateUnit": { "ok": true } }
                }));
            }

            if operation == "activateunit" {
                let (tenant_id, unit_id) = required_unit_identification_var(&vars)?;
                self.repo.activate_unit(crate::model::UnitRef {
                    tenant_id,
                    unit_id,
                    version: None,
                })?;
                return Ok(serde_json::json!({
                    "data": { "activateUnit": { "ok": true } }
                }));
            }

            if operation == "requeststatustransition" {
                let (tenant_id, unit_id) = required_unit_identification_var(&vars)?;
                let requested_status =
                    required_i32_var(&vars, &["requested_status", "requestedStatus"])?;
                let resulting_status = self.repo.request_status_transition(
                    crate::model::UnitRef {
                        tenant_id,
                        unit_id,
                        version: None,
                    },
                    requested_status,
                )?;
                return Ok(serde_json::json!({
                    "data": { "requestStatusTransition": { "status": resulting_status } }
                }));
            }

            if operation == "transitionunitstatus" {
                let (tenant_id, unit_id) = required_unit_identification_var(&vars)?;
                let requested_status =
                    required_i32_var(&vars, &["requested_status", "requestedStatus"])?;
                let resulting_status = self.repo.request_status_transition(
                    crate::model::UnitRef {
                        tenant_id,
                        unit_id,
                        version: None,
                    },
                    requested_status,
                )?;
                return Ok(serde_json::json!({
                    "data": { "transitionUnitStatus": { "status": resulting_status } }
                }));
            }

            if operation == "addrelation" {
                let (tenant_id, unit_id) = required_unit_identification_var(&vars)?;
                let relation_type = required_i32_var(&vars, &["relation_type", "relationType"])?;
                let other_tenant_id =
                    required_i64_var(&vars, &["other_tenant_id", "otherTenantId"])?;
                let other_unit_id = required_i64_var(&vars, &["other_unit_id", "otherUnitId"])?;
                self.repo.add_relation(
                    crate::model::UnitRef {
                        tenant_id,
                        unit_id,
                        version: None,
                    },
                    relation_type,
                    crate::model::UnitRef {
                        tenant_id: other_tenant_id,
                        unit_id: other_unit_id,
                        version: None,
                    },
                )?;
                return Ok(serde_json::json!({
                    "data": { "addRelation": { "ok": true } }
                }));
            }

            if operation == "removerelation" {
                let (tenant_id, unit_id) = required_unit_identification_var(&vars)?;
                let relation_type = required_i32_var(&vars, &["relation_type", "relationType"])?;
                let other_tenant_id =
                    required_i64_var(&vars, &["other_tenant_id", "otherTenantId"])?;
                let other_unit_id = required_i64_var(&vars, &["other_unit_id", "otherUnitId"])?;
                self.repo.remove_relation(
                    crate::model::UnitRef {
                        tenant_id,
                        unit_id,
                        version: None,
                    },
                    relation_type,
                    crate::model::UnitRef {
                        tenant_id: other_tenant_id,
                        unit_id: other_unit_id,
                        version: None,
                    },
                )?;
                return Ok(serde_json::json!({
                    "data": { "removeRelation": { "ok": true } }
                }));
            }

            if operation == "addassociation" {
                let (tenant_id, unit_id) = required_unit_identification_var(&vars)?;
                let association_type =
                    required_i32_var(&vars, &["association_type", "associationType"])?;
                let reference = required_string_var(&vars, &["reference"])?;
                self.repo.add_association(
                    crate::model::UnitRef {
                        tenant_id,
                        unit_id,
                        version: None,
                    },
                    association_type,
                    &reference,
                )?;
                return Ok(serde_json::json!({
                    "data": { "addAssociation": { "ok": true } }
                }));
            }

            if operation == "removeassociation" {
                let (tenant_id, unit_id) = required_unit_identification_var(&vars)?;
                let association_type =
                    required_i32_var(&vars, &["association_type", "associationType"])?;
                let reference = required_string_var(&vars, &["reference"])?;
                self.repo.remove_association(
                    crate::model::UnitRef {
                        tenant_id,
                        unit_id,
                        version: None,
                    },
                    association_type,
                    &reference,
                )?;
                return Ok(serde_json::json!({
                    "data": { "removeAssociation": { "ok": true } }
                }));
            }

            if operation == "unlockunit" {
                let (tenant_id, unit_id) = required_unit_identification_var(&vars)?;
                self.repo.unlock_unit(crate::model::UnitRef {
                    tenant_id,
                    unit_id,
                    version: None,
                })?;
                return Ok(serde_json::json!({
                    "data": {
                        "unlockUnit": {
                            "ok": true
                        }
                    }
                }));
            }

            if operation == "lockunit" {
                let (tenant_id, unit_id) = required_unit_identification_var(&vars)?;
                let lock_type = required_i32_var(&vars, &["lock_type", "lockType"])?;
                let purpose = required_string_var(&vars, &["purpose"])?;
                self.repo.lock_unit(
                    crate::model::UnitRef {
                        tenant_id,
                        unit_id,
                        version: None,
                    },
                    lock_type,
                    &purpose,
                )?;
                return Ok(serde_json::json!({
                    "data": {
                        "lockUnit": {
                            "ok": true
                        }
                    }
                }));
            }

            if operation == "createattribute" {
                let alias = required_string_var(&vars, &["alias"])?;
                let name = required_string_var(&vars, &["name"])?;
                let qualname = required_string_var(&vars, &["qualname"])?;
                let attribute_type =
                    required_string_var(&vars, &["attribute_type", "attributeType"])?;
                let is_array = optional_bool_var(&vars, &["is_array", "isArray"]).unwrap_or(false);
                let created = self.repo.create_attribute(
                    &alias,
                    &name,
                    &qualname,
                    &attribute_type,
                    is_array,
                )?;
                return Ok(serde_json::json!({
                    "data": { "createAttribute": created }
                }));
            }

            if operation == "instantiateattributemutation" {
                let name_or_id =
                    required_name_or_id_var(&vars, &["name_or_id", "nameOrId", "name", "id"])?;
                let attribute = self.repo.instantiate_attribute(&name_or_id)?;
                return Ok(serde_json::json!({
                    "data": { "instantiateAttributeMutation": attribute }
                }));
            }

            if operation == "flushcache" {
                self.repo.flush_cache()?;
                return Ok(serde_json::json!({
                    "data": { "flushCache": { "ok": true } }
                }));
            }

            return Ok(graphql_unsupported_operation_response(
                &operation_raw,
                operation_kind,
                self.allowed_operations_for_kind(operation_kind),
            ));
        }

        if operation == "registeredoperations" {
            return Ok(serde_json::json!({
                "data": {
                    "registeredOperations": self.registered_operations()
                }
            }));
        }

        if operation == "unitexists" {
            let (tenant_id, unit_id) = required_unit_identification_var(&vars)?;
            let exists = self.repo.unit_exists(tenant_id, unit_id)?;
            return Ok(serde_json::json!({
                "data": {
                    "unitExists": exists
                }
            }));
        }

        if operation == "unitlifecycle" {
            let (tenant_id, unit_id) = required_unit_identification_var(&vars)?;
            let exists = self.repo.unit_exists(tenant_id, unit_id)?;
            let (locked, status) = if exists {
                let locked = self.repo.is_unit_locked(crate::model::UnitRef {
                    tenant_id,
                    unit_id,
                    version: None,
                })?;
                let status = self
                    .repo
                    .get_unit_json(tenant_id, unit_id, VersionSelector::Latest)?
                    .and_then(|unit| unit.get("status").and_then(Value::as_i64));
                (locked, status)
            } else {
                (false, None)
            };
            return Ok(serde_json::json!({
                "data": {
                    "unitLifecycle": {
                        "exists": exists,
                        "locked": locked,
                        "status": status
                    }
                }
            }));
        }

        if operation == "getunitbycorrid"
            || operation == "unitbycorrid"
            || operation == "loadbycorrid"
        {
            let corrid = required_corrid_var(&vars)?;
            let unit = self.repo.get_unit_by_corrid_json(&corrid)?;
            let root_field = match operation {
                "unitbycorrid" => "unitByCorrid",
                "loadbycorrid" => "loadByCorrId",
                _ => "getUnitByCorrid",
            };
            return Ok(serde_json::json!({
                "data": {
                    (root_field): unit
                }
            }));
        }

        if operation == "getunit" || operation == "unit" || operation == "loadunit" {
            let (tenant_id, unit_id) = required_unit_identification_var(&vars)?;
            let selector = optional_version_var(&vars)
                .map(VersionSelector::Exact)
                .unwrap_or(VersionSelector::Latest);
            let unit = self.repo.get_unit_json(tenant_id, unit_id, selector)?;
            let root_field = match operation {
                "unit" => "unit",
                "loadunit" => "loadUnit",
                _ => "getUnit",
            };
            return Ok(serde_json::json!({
                "data": {
                    (root_field): unit
                }
            }));
        }

        if operation == "unitraw" || operation == "loadunitraw" {
            let (tenant_id, unit_id) = required_unit_identification_var(&vars)?;
            let selector = optional_version_var(&vars)
                .map(VersionSelector::Exact)
                .unwrap_or(VersionSelector::Latest);
            let encoded = self
                .repo
                .get_unit_json(tenant_id, unit_id, selector)?
                .map(|unit| encode_json_base64(&unit))
                .transpose()?;
            let root_field = if operation == "loadunitraw" {
                "loadUnitRaw"
            } else {
                "unitRaw"
            };
            return Ok(serde_json::json!({
                "data": {
                    (root_field): encoded
                }
            }));
        }

        if operation == "unitrawbycorrid" || operation == "loadrawpayloadbycorrid" {
            let corrid = required_corrid_var(&vars)?;
            let encoded = self
                .repo
                .get_unit_by_corrid_json(&corrid)?
                .map(|unit| encode_json_base64(&unit))
                .transpose()?;
            let root_field = if operation == "loadrawpayloadbycorrid" {
                "loadRawPayloadByCorrId"
            } else {
                "unitRawByCorrid"
            };
            return Ok(serde_json::json!({
                "data": {
                    (root_field): encoded
                }
            }));
        }

        if operation == "getunitmeta" {
            let (tenant_id, unit_id) = required_unit_identification_var(&vars)?;
            let unit = self
                .repo
                .get_unit_json(tenant_id, unit_id, VersionSelector::Latest)?;
            if let Some(unit) = unit {
                return Ok(serde_json::json!({
                    "data": {
                        "getUnitMeta": {
                            "exists": true,
                            "status": unit.get("status").and_then(Value::as_i64),
                            "unitver": unit.get("unitver").and_then(Value::as_i64),
                            "corrid": unit.get("corrid").and_then(Value::as_str),
                            "isreadonly": unit.get("isreadonly").and_then(Value::as_bool)
                        }
                    }
                }));
            }
            return Ok(serde_json::json!({
                "data": {
                    "getUnitMeta": {
                        "exists": false,
                        "status": Value::Null,
                        "unitver": Value::Null,
                        "corrid": Value::Null,
                        "isreadonly": Value::Null
                    }
                }
            }));
        }

        if operation == "getunitstatus" {
            let (tenant_id, unit_id) = required_unit_identification_var(&vars)?;
            let status = self
                .repo
                .get_unit_json(tenant_id, unit_id, VersionSelector::Latest)?
                .and_then(|unit| unit.get("status").and_then(Value::as_i64));
            return Ok(serde_json::json!({
                "data": {
                    "getUnitStatus": status
                }
            }));
        }

        if operation == "getattributeinfo" {
            let name_or_id =
                required_name_or_id_var(&vars, &["name_or_id", "nameOrId", "name", "id"])?;
            let attribute = self.repo.get_attribute_info(&name_or_id)?;
            return Ok(serde_json::json!({
                "data": {
                    "getAttributeInfo": attribute
                }
            }));
        }

        if operation == "instantiateattribute" {
            let name_or_id =
                required_name_or_id_var(&vars, &["name_or_id", "nameOrId", "name", "id"])?;
            let attribute = self.repo.instantiate_attribute(&name_or_id)?;
            return Ok(serde_json::json!({
                "data": {
                    "instantiateAttribute": attribute
                }
            }));
        }

        if operation == "attributenametoid" {
            let name = required_string_var(&vars, &["name", "attributeName"])?;
            let id = self.repo.attribute_name_to_id(&name)?;
            return Ok(serde_json::json!({
                "data": {
                    "attributeNameToId": id
                }
            }));
        }

        if operation == "attributeidtoname" {
            let id = required_i64_var(&vars, &["id", "attributeId"])?;
            let name = self.repo.attribute_id_to_name(id)?;
            return Ok(serde_json::json!({
                "data": {
                    "attributeIdToName": name
                }
            }));
        }

        if operation == "gettenantinfo" {
            let name_or_id =
                required_name_or_id_var(&vars, &["name_or_id", "nameOrId", "name", "id"])?;
            let tenant = self.repo.get_tenant_info(&name_or_id)?;
            return Ok(serde_json::json!({
                "data": {
                    "getTenantInfo": tenant
                }
            }));
        }

        if operation == "isunitlocked" {
            let (tenant_id, unit_id) = required_unit_identification_var(&vars)?;
            let locked = self.repo.is_unit_locked(crate::model::UnitRef {
                tenant_id,
                unit_id,
                version: None,
            })?;
            return Ok(serde_json::json!({
                "data": {
                    "isUnitLocked": locked
                }
            }));
        }

        if operation == "tenantnametoid" {
            let name = required_string_var(&vars, &["name", "tenantName"])?;
            let id = self.repo.tenant_name_to_id(&name)?;
            return Ok(serde_json::json!({
                "data": {
                    "tenantNameToId": id
                }
            }));
        }

        if operation == "tenantidtoname" {
            let id = required_i64_var(&vars, &["id", "tenantId"])?;
            let name = self.repo.tenant_id_to_name(id)?;
            return Ok(serde_json::json!({
                "data": {
                    "tenantIdToName": name
                }
            }));
        }

        if operation == "canchangeattribute" {
            let name_or_id =
                required_name_or_id_var(&vars, &["name_or_id", "nameOrId", "name", "id"])?;
            let can_change = self.repo.can_change_attribute(&name_or_id)?;
            return Ok(serde_json::json!({
                "data": {
                    "canChangeAttribute": can_change
                }
            }));
        }

        if operation == "getrightrelation" {
            let (tenant_id, unit_id) = required_unit_identification_var(&vars)?;
            let relation_type = required_i32_var(&vars, &["relation_type", "relationType"])?;
            let relation = self.repo.get_right_relation(
                crate::model::UnitRef {
                    tenant_id,
                    unit_id,
                    version: None,
                },
                relation_type,
            )?;
            return Ok(serde_json::json!({"data": {"getRightRelation": relation}}));
        }

        if operation == "getrightrelations" {
            let (tenant_id, unit_id) = required_unit_identification_var(&vars)?;
            let relation_type = required_i32_var(&vars, &["relation_type", "relationType"])?;
            let relations = self.repo.get_right_relations(
                crate::model::UnitRef {
                    tenant_id,
                    unit_id,
                    version: None,
                },
                relation_type,
            )?;
            return Ok(serde_json::json!({"data": {"getRightRelations": relations}}));
        }

        if operation == "getleftrelations" {
            let (tenant_id, unit_id) = required_unit_identification_var(&vars)?;
            let relation_type = required_i32_var(&vars, &["relation_type", "relationType"])?;
            let relations = self.repo.get_left_relations(
                crate::model::UnitRef {
                    tenant_id,
                    unit_id,
                    version: None,
                },
                relation_type,
            )?;
            return Ok(serde_json::json!({"data": {"getLeftRelations": relations}}));
        }

        if operation == "countrightrelations" {
            let (tenant_id, unit_id) = required_unit_identification_var(&vars)?;
            let relation_type = required_i32_var(&vars, &["relation_type", "relationType"])?;
            let count = self.repo.count_right_relations(
                crate::model::UnitRef {
                    tenant_id,
                    unit_id,
                    version: None,
                },
                relation_type,
            )?;
            return Ok(serde_json::json!({"data": {"countRightRelations": count}}));
        }

        if operation == "countleftrelations" {
            let (tenant_id, unit_id) = required_unit_identification_var(&vars)?;
            let relation_type = required_i32_var(&vars, &["relation_type", "relationType"])?;
            let count = self.repo.count_left_relations(
                crate::model::UnitRef {
                    tenant_id,
                    unit_id,
                    version: None,
                },
                relation_type,
            )?;
            return Ok(serde_json::json!({"data": {"countLeftRelations": count}}));
        }

        if operation == "getrightassociation" {
            let (tenant_id, unit_id) = required_unit_identification_var(&vars)?;
            let association_type =
                required_i32_var(&vars, &["association_type", "associationType"])?;
            let assoc = self.repo.get_right_association(
                crate::model::UnitRef {
                    tenant_id,
                    unit_id,
                    version: None,
                },
                association_type,
            )?;
            return Ok(serde_json::json!({"data": {"getRightAssociation": assoc}}));
        }

        if operation == "getrightassociations" {
            let (tenant_id, unit_id) = required_unit_identification_var(&vars)?;
            let association_type =
                required_i32_var(&vars, &["association_type", "associationType"])?;
            let assocs = self.repo.get_right_associations(
                crate::model::UnitRef {
                    tenant_id,
                    unit_id,
                    version: None,
                },
                association_type,
            )?;
            return Ok(serde_json::json!({"data": {"getRightAssociations": assocs}}));
        }

        if operation == "getleftassociations" {
            let association_type =
                required_i32_var(&vars, &["association_type", "associationType"])?;
            let reference = required_string_var(&vars, &["reference"])?;
            let assocs = self
                .repo
                .get_left_associations(association_type, &reference)?;
            return Ok(serde_json::json!({"data": {"getLeftAssociations": assocs}}));
        }

        if operation == "countrightassociations" {
            let (tenant_id, unit_id) = required_unit_identification_var(&vars)?;
            let association_type =
                required_i32_var(&vars, &["association_type", "associationType"])?;
            let count = self.repo.count_right_associations(
                crate::model::UnitRef {
                    tenant_id,
                    unit_id,
                    version: None,
                },
                association_type,
            )?;
            return Ok(serde_json::json!({"data": {"countRightAssociations": count}}));
        }

        if operation == "countleftassociations" {
            let association_type =
                required_i32_var(&vars, &["association_type", "associationType"])?;
            let reference = required_string_var(&vars, &["reference"])?;
            let count = self
                .repo
                .count_left_associations(association_type, &reference)?;
            return Ok(serde_json::json!({"data": {"countLeftAssociations": count}}));
        }

        if operation == "health" {
            let health = self.repo.health()?;
            return Ok(serde_json::json!({"data": {"health": health}}));
        }

        if operation != "searchunits"
            && operation != "search"
            && operation != "searchunit"
            && operation != "units"
            && operation != "unitsraw"
            && operation != "searchraw"
            && operation != "searchrawpayload"
            && operation != "searchunitsquery"
            && operation != "searchunitsquerystrict"
        {
            return Ok(graphql_unsupported_operation_response(
                &operation_raw,
                operation_kind,
                self.allowed_operations_for_kind(operation_kind),
            ));
        }

        let order = search_order_from_vars(&vars);
        let paging = search_paging_from_vars(&vars);

        if operation == "searchunits"
            || operation == "search"
            || operation == "searchunit"
            || operation == "units"
        {
            let expression = search_expression_from_vars(&vars);
            let result = self.repo.search_units(expression, order, paging)?;
            let root_field = match operation {
                "search" => "search",
                "searchunit" => "searchUnit",
                "units" => "units",
                _ => "searchUnits",
            };
            return Ok(serde_json::json!({
                "data": {
                    (root_field): {
                        "totalHits": result.total_hits,
                        "results": result.results
                    }
                }
            }));
        }

        if operation == "unitsraw" || operation == "searchraw" || operation == "searchrawpayload" {
            let tenant_scope = search_tenant_from_vars(&vars);
            let raw_query = optional_search_query_var(&vars);
            let raw_expression = optional_search_expression_from_vars(&vars);
            let result = if let Some(expression) = raw_expression {
                let scoped_expression = if let Some(tenant_id) = tenant_scope {
                    scoped_expression(expression, tenant_id)
                } else {
                    expression
                };
                self.repo.search_units(scoped_expression, order, paging)?
            } else if let Some(query) = raw_query {
                let scoped_query = tenant_scope
                    .map(|tenant_id| format!("tenantid = {tenant_id} and ({query})"))
                    .unwrap_or(query);
                self.repo.search_units_query(&scoped_query, order, paging)?
            } else {
                let expression = tenant_scope
                    .map(tenant_only_expression)
                    .unwrap_or_else(|| Value::Object(Default::default()));
                self.repo.search_units(expression, order, paging)?
            };
            let payloads: Vec<Value> = result
                .results
                .into_iter()
                .map(|unit| unit.payload)
                .collect();
            let encoded = encode_json_base64(&Value::Array(payloads))?;
            let root_field = match operation {
                "searchraw" => "searchRaw",
                "searchrawpayload" => "searchRawPayload",
                _ => "unitsRaw",
            };
            return Ok(serde_json::json!({
                "data": {
                    (root_field): encoded
                }
            }));
        }

        if operation == "searchunitsquery" {
            let query = required_search_query_var(&vars)?;
            let result = self.repo.search_units_query(&query, order, paging)?;
            return Ok(serde_json::json!({
                "data": {
                    "searchUnitsQuery": {
                        "totalHits": result.total_hits,
                        "results": result.results
                    }
                }
            }));
        }

        let query = required_search_query_var(&vars)?;
        let result = self.repo.search_units_query_strict(&query, order, paging)?;
        Ok(serde_json::json!({
            "data": {
                "searchUnitsQueryStrict": {
                    "totalHits": result.total_hits,
                    "results": result.results
                }
            }
        }))
    }

    fn allowed_operations_for_kind(&self, kind: OperationKind) -> Vec<&'static str> {
        SUPPORTED_OPERATION_SPECS
            .iter()
            .filter(|spec| spec.kind == kind && self.allowed_operations.contains(spec.canonical))
            .map(|spec| spec.display)
            .collect()
    }
}

fn canonical_operation_name(name: &str) -> Option<&'static str> {
    // Normalize API/client naming variants:
    // - case-insensitive
    // - underscores ignored
    let normalized = name.to_ascii_lowercase().replace('_', "");
    SUPPORTED_OPERATION_SPECS
        .iter()
        .find(|spec| spec.canonical == normalized)
        .map(|spec| spec.canonical)
}

fn operation_kind_for(canonical: &str) -> Option<OperationKind> {
    SUPPORTED_OPERATION_SPECS
        .iter()
        .find(|spec| spec.canonical == canonical)
        .map(|spec| spec.kind)
}

fn graphql_error_response(err: RepoError) -> Value {
    let (code, message) = match err {
        RepoError::NotFound => ("NOT_FOUND", "not found".to_string()),
        RepoError::AlreadyLocked => ("ALREADY_LOCKED", "already locked".to_string()),
        RepoError::InvalidInput(msg) => ("INVALID_INPUT", msg),
        RepoError::Backend(msg) => ("BACKEND_ERROR", msg),
        RepoError::Unsupported(msg) => ("UNSUPPORTED", msg),
    };

    serde_json::json!({
        "data": Value::Null,
        "errors": [
            {
                "message": message,
                "extensions": {
                    "code": code
                }
            }
        ]
    })
}

fn graphql_unsupported_operation_response(
    operation: &str,
    operation_kind: OperationKind,
    supported_operations: Vec<&'static str>,
) -> Value {
    serde_json::json!({
        "data": Value::Null,
        "errors": [
            {
                "message": format!(
                    "unsupported {} operation '{}'",
                    operation_kind.as_str(),
                    operation
                ),
                "extensions": {
                    "code": "UNSUPPORTED",
                    "operationType": operation_kind.as_str(),
                    "operation": operation,
                    "supportedOperations": supported_operations
                }
            }
        ]
    })
}

fn extract_root_field(query: &str) -> Option<String> {
    // Minimal root-field extraction that tolerates optional aliases:
    //   query { alias: realOperation(...) { ... } }
    // returns "realOperation" in that case.
    let bytes = query.as_bytes();
    let open = bytes.iter().position(|b| *b == b'{')?;
    let mut i = open + 1;
    while i < bytes.len() {
        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        if i >= bytes.len() || bytes[i] == b'}' {
            return None;
        }
        if let Some((first, next_i)) = parse_identifier(bytes, i) {
            i = next_i;
            while i < bytes.len() && bytes[i].is_ascii_whitespace() {
                i += 1;
            }
            if i < bytes.len() && bytes[i] == b':' {
                i += 1;
                while i < bytes.len() && bytes[i].is_ascii_whitespace() {
                    i += 1;
                }
                let (aliased, _) = parse_identifier(bytes, i)?;
                return Some(aliased);
            }
            return Some(first);
        }
        i += 1;
    }
    None
}

fn parse_identifier(bytes: &[u8], start: usize) -> Option<(String, usize)> {
    if start >= bytes.len() {
        return None;
    }
    let mut i = start;
    let is_ident = |b: u8| b.is_ascii_alphanumeric() || b == b'_';
    if !is_ident(bytes[i]) {
        return None;
    }
    while i < bytes.len() && is_ident(bytes[i]) {
        i += 1;
    }
    Some((String::from_utf8_lossy(&bytes[start..i]).to_string(), i))
}

fn detect_operation_kind(query: &str) -> OperationKind {
    let trimmed = query.trim_start();
    let mut first = String::new();
    for ch in trimmed.chars() {
        if ch.is_ascii_alphanumeric() || ch == '_' {
            first.push(ch.to_ascii_lowercase());
        } else {
            break;
        }
    }
    if first == "mutation" {
        OperationKind::Mutation
    } else {
        OperationKind::Query
    }
}

fn required_i64_var(vars: &serde_json::Map<String, Value>, keys: &[&str]) -> RepoResult<i64> {
    for key in keys {
        if let Some(v) = vars.get(*key) {
            if let Some(i) = v.as_i64() {
                return Ok(i);
            }
            if let Some(s) = v.as_str().and_then(|s| s.parse::<i64>().ok()) {
                return Ok(s);
            }
            return Err(RepoError::InvalidInput(format!(
                "graphql var '{key}' must be i64"
            )));
        }
    }
    Err(RepoError::InvalidInput(format!(
        "graphql missing required var: one of {:?}",
        keys
    )))
}

fn required_i32_var(vars: &serde_json::Map<String, Value>, keys: &[&str]) -> RepoResult<i32> {
    let value = required_i64_var(vars, keys)?;
    i32::try_from(value)
        .map_err(|_| RepoError::InvalidInput(format!("graphql var '{:?}' must fit i32", keys)))
}

fn required_string_var(vars: &serde_json::Map<String, Value>, keys: &[&str]) -> RepoResult<String> {
    for key in keys {
        if let Some(v) = vars.get(*key) {
            if let Some(s) = v.as_str() {
                return Ok(s.to_string());
            }
            return Err(RepoError::InvalidInput(format!(
                "graphql var '{key}' must be string"
            )));
        }
    }
    Err(RepoError::InvalidInput(format!(
        "graphql missing required var: one of {:?}",
        keys
    )))
}

fn required_name_or_id_var(
    vars: &serde_json::Map<String, Value>,
    keys: &[&str],
) -> RepoResult<String> {
    for key in keys {
        if let Some(v) = vars.get(*key) {
            if let Some(s) = v.as_str() {
                return Ok(s.to_string());
            }
            if let Some(i) = v.as_i64() {
                return Ok(i.to_string());
            }
            return Err(RepoError::InvalidInput(format!(
                "graphql var '{key}' must be string or i64"
            )));
        }
    }
    Err(RepoError::InvalidInput(format!(
        "graphql missing required var: one of {:?}",
        keys
    )))
}

fn required_object_var(
    vars: &serde_json::Map<String, Value>,
    keys: &[&str],
) -> RepoResult<serde_json::Map<String, Value>> {
    for key in keys {
        if let Some(v) = vars.get(*key) {
            if let Some(obj) = v.as_object() {
                return Ok(obj.clone());
            }
            return Err(RepoError::InvalidInput(format!(
                "graphql var '{key}' must be object"
            )));
        }
    }
    Err(RepoError::InvalidInput(format!(
        "graphql missing required var: one of {:?}",
        keys
    )))
}

fn required_unit_identification_var(
    vars: &serde_json::Map<String, Value>,
) -> RepoResult<(i64, i64)> {
    if let (Some(tenant_id), Some(unit_id)) = (
        optional_i64_var(vars, &["tenantid", "tenantId"]),
        optional_i64_var(vars, &["unitid", "unitId"]),
    ) {
        return Ok((tenant_id, unit_id));
    }
    if let Some(id) = nested_object_var(vars, &["id", "unit", "unit_id", "unitId"]) {
        if let (Some(tenant_id), Some(unit_id)) = (
            optional_i64_var(id, &["tenantid", "tenantId"]),
            optional_i64_var(id, &["unitid", "unitId"]),
        ) {
            return Ok((tenant_id, unit_id));
        }
        return Err(RepoError::InvalidInput(
            "graphql var 'id' must contain numeric tenantId and unitId".to_string(),
        ));
    }
    Err(RepoError::InvalidInput(
        "graphql missing required unit identification (tenantId/unitId or id object)".to_string(),
    ))
}

fn required_corrid_var(vars: &serde_json::Map<String, Value>) -> RepoResult<String> {
    if let Some(corrid) = optional_string_var(vars, &["corrid", "corrId"]) {
        return Ok(corrid);
    }
    if let Some(id) = nested_object_var(vars, &["id"]) {
        if let Some(corrid) = optional_string_var(id, &["corrid", "corrId"]) {
            return Ok(corrid);
        }
        return Err(RepoError::InvalidInput(
            "graphql var 'id' must contain corrId".to_string(),
        ));
    }
    Err(RepoError::InvalidInput(
        "graphql missing required corrid identification".to_string(),
    ))
}

fn optional_version_var(vars: &serde_json::Map<String, Value>) -> Option<i64> {
    if let Some(version) = optional_i64_var(vars, &["version", "unitver", "unit_ver"]) {
        return Some(version);
    }
    nested_object_var(vars, &["id"])
        .and_then(|id| optional_i64_var(id, &["version", "unitver", "unit_ver"]))
}

fn search_expression_from_vars(vars: &serde_json::Map<String, Value>) -> Value {
    if let Some(expression) = vars.get("expression") {
        return expression.clone();
    }
    if let Some(filter) = nested_object_var(vars, &["filter"]) {
        if let Some(expression) = filter.get("expression") {
            return expression.clone();
        }
    }
    Value::Object(Default::default())
}

fn required_search_query_var(vars: &serde_json::Map<String, Value>) -> RepoResult<String> {
    if let Some(query) = optional_string_var(vars, &["query", "search_query", "searchQuery"]) {
        return Ok(query);
    }
    if let Some(filter) = nested_object_var(vars, &["filter"]) {
        if let Some(query) = optional_string_var(filter, &["query", "search_query", "searchQuery"])
        {
            return Ok(query);
        }
        return Err(RepoError::InvalidInput(
            "graphql var 'filter' must contain query/searchQuery".to_string(),
        ));
    }
    Err(RepoError::InvalidInput(
        "graphql missing required var: query (or filter.query)".to_string(),
    ))
}

fn optional_search_query_var(vars: &serde_json::Map<String, Value>) -> Option<String> {
    optional_string_var(vars, &["query", "search_query", "searchQuery"]).or_else(|| {
        nested_object_var(vars, &["filter"]).and_then(|filter| {
            optional_string_var(filter, &["query", "search_query", "searchQuery", "where"])
        })
    })
}

fn optional_search_expression_from_vars(vars: &serde_json::Map<String, Value>) -> Option<Value> {
    vars.get("expression").cloned().or_else(|| {
        nested_object_var(vars, &["filter"]).and_then(|filter| filter.get("expression").cloned())
    })
}

fn search_tenant_from_vars(vars: &serde_json::Map<String, Value>) -> Option<i64> {
    optional_i64_var(vars, &["tenantid", "tenantId"]).or_else(|| {
        nested_object_var(vars, &["filter"])
            .and_then(|filter| optional_i64_var(filter, &["tenantid", "tenantId"]))
    })
}

fn tenant_only_expression(tenant_id: i64) -> Value {
    let mut map = serde_json::Map::new();
    map.insert("tenantid".to_string(), Value::Number(tenant_id.into()));
    Value::Object(map)
}

fn scoped_expression(expression: Value, tenant_id: i64) -> Value {
    let tenant = tenant_only_expression(tenant_id);
    if expression.get("tenantid").is_some() || expression.get("tenant_id").is_some() {
        expression
    } else {
        serde_json::json!({
            "and": [
                tenant,
                expression
            ]
        })
    }
}

fn encode_json_base64(value: &Value) -> RepoResult<String> {
    use base64::Engine as _;

    let data = serde_json::to_vec(value)
        .map_err(|err| RepoError::Backend(format!("failed to serialize json payload: {err}")))?;
    Ok(base64::engine::general_purpose::STANDARD.encode(data))
}

fn decode_base64(value: &str, var_name: &str) -> RepoResult<Vec<u8>> {
    use base64::Engine as _;

    base64::engine::general_purpose::STANDARD
        .decode(value)
        .map_err(|err| {
            RepoError::InvalidInput(format!("graphql var '{var_name}' must be base64: {err}"))
        })
}

fn search_order_from_vars(vars: &serde_json::Map<String, Value>) -> SearchOrder {
    if let Some(order_obj) = nested_object_var(vars, &["order"]) {
        SearchOrder {
            field: order_obj
                .get("field")
                .and_then(Value::as_str)
                .unwrap_or("created")
                .to_string(),
            descending: order_obj
                .get("descending")
                .and_then(Value::as_bool)
                .unwrap_or(true),
        }
    } else if let Some(filter_obj) = nested_object_var(vars, &["filter"]) {
        if let Some(order_obj) = filter_obj.get("order").and_then(Value::as_object) {
            SearchOrder {
                field: order_obj
                    .get("field")
                    .and_then(Value::as_str)
                    .unwrap_or("created")
                    .to_string(),
                descending: order_obj
                    .get("descending")
                    .and_then(Value::as_bool)
                    .unwrap_or(true),
            }
        } else {
            SearchOrder {
                field: "created".to_string(),
                descending: true,
            }
        }
    } else {
        SearchOrder {
            field: "created".to_string(),
            descending: true,
        }
    }
}

fn search_paging_from_vars(vars: &serde_json::Map<String, Value>) -> SearchPaging {
    if let Some(paging_obj) = nested_object_var(vars, &["paging"]) {
        SearchPaging {
            limit: paging_obj
                .get("limit")
                .and_then(Value::as_i64)
                .unwrap_or(50),
            offset: paging_obj
                .get("offset")
                .and_then(Value::as_i64)
                .unwrap_or(0),
        }
    } else if let Some(filter_obj) = nested_object_var(vars, &["filter"]) {
        if let Some(paging_obj) = filter_obj.get("paging").and_then(Value::as_object) {
            SearchPaging {
                limit: paging_obj
                    .get("limit")
                    .and_then(Value::as_i64)
                    .unwrap_or(50),
                offset: paging_obj
                    .get("offset")
                    .and_then(Value::as_i64)
                    .unwrap_or(0),
            }
        } else {
            SearchPaging {
                limit: 50,
                offset: 0,
            }
        }
    } else {
        SearchPaging {
            limit: 50,
            offset: 0,
        }
    }
}

fn nested_object_var<'a>(
    vars: &'a serde_json::Map<String, Value>,
    keys: &[&str],
) -> Option<&'a serde_json::Map<String, Value>> {
    for key in keys {
        if let Some(obj) = vars.get(*key).and_then(Value::as_object) {
            return Some(obj);
        }
    }
    None
}

fn optional_bool_var(vars: &serde_json::Map<String, Value>, keys: &[&str]) -> Option<bool> {
    for key in keys {
        if let Some(v) = vars.get(*key) {
            if let Some(value) = v.as_bool() {
                return Some(value);
            }
            if let Some(s) = v.as_str() {
                match s.to_ascii_lowercase().as_str() {
                    "true" | "1" | "yes" => return Some(true),
                    "false" | "0" | "no" => return Some(false),
                    _ => {}
                }
            }
            return None;
        }
    }
    None
}

fn optional_string_var(vars: &serde_json::Map<String, Value>, keys: &[&str]) -> Option<String> {
    for key in keys {
        if let Some(v) = vars.get(*key) {
            return v.as_str().map(ToString::to_string);
        }
    }
    None
}

fn optional_i64_var(vars: &serde_json::Map<String, Value>, keys: &[&str]) -> Option<i64> {
    for key in keys {
        if let Some(v) = vars.get(*key) {
            if let Some(i) = v.as_i64() {
                return Some(i);
            }
            if let Some(parsed) = v.as_str().and_then(|s| s.parse::<i64>().ok()) {
                return Some(parsed);
            }
            return None;
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde_json::{json, Value};
    use uuid::Uuid;

    use crate::backend::{Backend, RepoResult};
    use crate::model::{
        Association, Relation, SearchOrder, SearchPaging, SearchResult, Unit, UnitRef,
        VersionSelector,
    };
    use crate::repo::RepoService;

    use super::GraphqlRuntime;

    struct MockBackend;

    impl Backend for MockBackend {
        fn get_unit_json(
            &self,
            tenant_id: i64,
            unit_id: i64,
            _selector: VersionSelector,
        ) -> RepoResult<Option<Value>> {
            if tenant_id == 42 && unit_id == 1001 {
                Ok(Some(
                    json!({"tenantid": 42, "unitid": 1001, "unitver": 1, "status": 30}),
                ))
            } else {
                Ok(None)
            }
        }

        fn get_unit_by_corrid_json(
            &self,
            corrid: &str,
        ) -> RepoResult<Option<Value>> {
            if corrid == "00000000-0000-0000-0000-000000000000" {
                Ok(Some(
                    json!({"tenantid": 42, "unitid": 1001, "unitver": 1, "status": 30}),
                ))
            } else {
                Ok(None)
            }
        }

        fn unit_exists(&self, _tenant_id: i64, _unit_id: i64) -> RepoResult<bool> {
            Ok(true)
        }

        fn store_unit_json(&self, unit: Value) -> RepoResult<Value> {
            let mut obj = unit.as_object().cloned().unwrap_or_default();
            obj.entry("tenantid".to_string())
                .or_insert_with(|| Value::Number(42_i64.into()));
            obj.entry("unitid".to_string())
                .or_insert_with(|| Value::Number(2001_i64.into()));
            obj.entry("unitver".to_string())
                .or_insert_with(|| Value::Number(1_i64.into()));
            obj.entry("status".to_string())
                .or_insert_with(|| Value::Number(30_i64.into()));
            Ok(Value::Object(obj))
        }

        fn search_units(
            &self,
            _expression: Value,
            _order: SearchOrder,
            _paging: SearchPaging,
        ) -> RepoResult<SearchResult> {
            Ok(SearchResult {
                total_hits: 1,
                results: vec![Unit {
                    tenant_id: 42,
                    unit_id: 1001,
                    unit_ver: 1,
                    status: 30,
                    name: Some("graphql-unit".to_string()),
                    corr_id: Uuid::nil(),
                    payload: json!({"tenantid": 42, "unitid": 1001, "unitver": 1, "status": 30}),
                }],
            })
        }

        fn add_relation(
            &self,
            _left: UnitRef,
            _relation_type: i32,
            _right: UnitRef,
        ) -> RepoResult<()> {
            Ok(())
        }

        fn remove_relation(
            &self,
            _left: UnitRef,
            _relation_type: i32,
            _right: UnitRef,
        ) -> RepoResult<()> {
            Ok(())
        }

        fn get_right_relation(
            &self,
            _unit: UnitRef,
            _relation_type: i32,
        ) -> RepoResult<Option<Relation>> {
            Ok(Some(Relation {
                tenant_id: 42,
                unit_id: 1001,
                relation_type: 11,
                related_tenant_id: 42,
                related_unit_id: 1002,
            }))
        }

        fn get_right_relations(
            &self,
            _unit: UnitRef,
            _relation_type: i32,
        ) -> RepoResult<Vec<Relation>> {
            Ok(vec![Relation {
                tenant_id: 42,
                unit_id: 1001,
                relation_type: 11,
                related_tenant_id: 42,
                related_unit_id: 1002,
            }])
        }

        fn get_left_relations(
            &self,
            _unit: UnitRef,
            _relation_type: i32,
        ) -> RepoResult<Vec<Relation>> {
            Ok(vec![Relation {
                tenant_id: 42,
                unit_id: 1001,
                relation_type: 11,
                related_tenant_id: 42,
                related_unit_id: 1002,
            }])
        }

        fn count_right_relations(&self, _unit: UnitRef, _relation_type: i32) -> RepoResult<i64> {
            Ok(1)
        }

        fn count_left_relations(&self, _unit: UnitRef, _relation_type: i32) -> RepoResult<i64> {
            Ok(1)
        }

        fn add_association(
            &self,
            _unit: UnitRef,
            _association_type: i32,
            _reference: &str,
        ) -> RepoResult<()> {
            Ok(())
        }

        fn remove_association(
            &self,
            _unit: UnitRef,
            _association_type: i32,
            _reference: &str,
        ) -> RepoResult<()> {
            Ok(())
        }

        fn get_right_association(
            &self,
            _unit: UnitRef,
            _association_type: i32,
        ) -> RepoResult<Option<Association>> {
            Ok(Some(Association {
                tenant_id: 42,
                unit_id: 1001,
                association_type: 21,
                reference: "graphql-ref".to_string(),
            }))
        }

        fn get_right_associations(
            &self,
            _unit: UnitRef,
            _association_type: i32,
        ) -> RepoResult<Vec<Association>> {
            Ok(vec![Association {
                tenant_id: 42,
                unit_id: 1001,
                association_type: 21,
                reference: "graphql-ref".to_string(),
            }])
        }

        fn get_left_associations(
            &self,
            _association_type: i32,
            _reference: &str,
        ) -> RepoResult<Vec<Association>> {
            Ok(vec![Association {
                tenant_id: 42,
                unit_id: 1001,
                association_type: 21,
                reference: "graphql-ref".to_string(),
            }])
        }

        fn count_right_associations(
            &self,
            _unit: UnitRef,
            _association_type: i32,
        ) -> RepoResult<i64> {
            Ok(1)
        }

        fn count_left_associations(
            &self,
            _association_type: i32,
            _reference: &str,
        ) -> RepoResult<i64> {
            Ok(1)
        }

        fn lock_unit(&self, _unit: UnitRef, _lock_type: i32, _purpose: &str) -> RepoResult<()> {
            Ok(())
        }

        fn unlock_unit(&self, _unit: UnitRef) -> RepoResult<()> {
            Ok(())
        }

        fn is_unit_locked(&self, unit: UnitRef) -> RepoResult<bool> {
            Ok(unit.unit_id == 1002)
        }

        fn set_status(&self, _unit: UnitRef, _status: i32) -> RepoResult<()> {
            Ok(())
        }

        fn create_attribute(
            &self,
            alias: &str,
            name: &str,
            qualname: &str,
            attribute_type: &str,
            is_array: bool,
        ) -> RepoResult<Value> {
            Ok(json!({
                "id": 8,
                "alias": alias,
                "name": name,
                "qualname": qualname,
                "type": if attribute_type == "string" { 1 } else { 0 },
                "forced_scalar": !is_array
            }))
        }

        fn instantiate_attribute(&self, _name_or_id: &str) -> RepoResult<Option<Value>> {
            Ok(Some(json!({
                "id": 7,
                "name": "mock-attr",
                "qualname": "mock.attr",
                "type": 1,
                "forced_scalar": true
            })))
        }

        fn can_change_attribute(&self, _name_or_id: &str) -> RepoResult<bool> {
            Ok(false)
        }

        fn get_attribute_info(&self, _name_or_id: &str) -> RepoResult<Option<Value>> {
            Ok(Some(json!({
                "id": 7,
                "name": "mock-attr",
                "qualname": "mock.attr",
                "type": 1,
                "forced_scalar": true
            })))
        }

        fn get_tenant_info(&self, _name_or_id: &str) -> RepoResult<Option<Value>> {
            Ok(Some(json!({
                "id": 42,
                "name": "mock-tenant",
                "description": "mock",
                "created": "2026-01-01T00:00:00Z"
            })))
        }
    }

    #[test]
    fn graphql_execute_search_units_mvp() {
        let runtime = GraphqlRuntime::new(RepoService::new(Arc::new(MockBackend)));
        let result = runtime
            .execute(
                "query SearchUnits { searchUnits { totalHits results { unitId } } }",
                Some(json!({
                    "expression": {"tenantid": 42, "name_ilike": "%graphql%"},
                    "order": {"field": "created", "descending": true},
                    "paging": {"limit": 10, "offset": 0}
                })),
            )
            .expect("graphql search should succeed");
        assert_eq!(result["data"]["searchUnits"]["totalHits"], 1);
        assert_eq!(result["data"]["searchUnits"]["results"][0]["unit_id"], 1001);
    }

    #[test]
    fn graphql_execute_search_unit_alias_mvp() {
        let runtime = GraphqlRuntime::new(RepoService::new(Arc::new(MockBackend)));
        let result = runtime
            .execute(
                "query SearchUnit { searchUnit { totalHits results { unitId } } }",
                Some(json!({
                    "expression": {"tenantid": 42, "name_ilike": "%graphql%"},
                    "order": {"field": "created", "descending": true},
                    "paging": {"limit": 10, "offset": 0}
                })),
            )
            .expect("graphql searchUnit alias should succeed");
        assert_eq!(result["data"]["searchUnit"]["totalHits"], 1);
        assert_eq!(result["data"]["searchUnit"]["results"][0]["unit_id"], 1001);
    }

    #[test]
    fn graphql_execute_units_alias_mvp() {
        let runtime = GraphqlRuntime::new(RepoService::new(Arc::new(MockBackend)));
        let result = runtime
            .execute(
                "query Units { units { totalHits results { unitId } } }",
                Some(json!({
                    "expression": {"tenantid": 42, "name_ilike": "%graphql%"},
                    "order": {"field": "created", "descending": true},
                    "paging": {"limit": 10, "offset": 0}
                })),
            )
            .expect("graphql units alias should succeed");
        assert_eq!(result["data"]["units"]["totalHits"], 1);
        assert_eq!(result["data"]["units"]["results"][0]["unit_id"], 1001);
    }

    #[test]
    fn graphql_execute_search_units_accepts_filter_object_mvp() {
        let runtime = GraphqlRuntime::new(RepoService::new(Arc::new(MockBackend)));
        let result = runtime
            .execute(
                "query SearchUnits { searchUnits { totalHits results { unitId } } }",
                Some(json!({
                    "filter": {
                        "expression": {"tenantid": 42, "name_ilike": "%graphql%"},
                        "order": {"field": "created", "descending": true},
                        "paging": {"limit": 10, "offset": 0}
                    }
                })),
            )
            .expect("graphql search with filter object should succeed");
        assert_eq!(result["data"]["searchUnits"]["totalHits"], 1);
        assert_eq!(result["data"]["searchUnits"]["results"][0]["unit_id"], 1001);
    }

    #[test]
    fn graphql_execute_search_units_query_mvp() {
        let runtime = GraphqlRuntime::new(RepoService::new(Arc::new(MockBackend)));
        let result = runtime
            .execute(
                "query SearchUnitsQuery { searchUnitsQuery { totalHits results { unitId } } }",
                Some(json!({
                    "query": "tenantid = 42 and name = '*graphql*'",
                    "order": {"field": "created", "descending": true},
                    "paging": {"limit": 10, "offset": 0}
                })),
            )
            .expect("graphql searchUnitsQuery should succeed");
        assert_eq!(result["data"]["searchUnitsQuery"]["totalHits"], 1);
        assert_eq!(
            result["data"]["searchUnitsQuery"]["results"][0]["unit_id"],
            1001
        );
    }

    #[test]
    fn graphql_execute_search_units_query_accepts_filter_object_mvp() {
        let runtime = GraphqlRuntime::new(RepoService::new(Arc::new(MockBackend)));
        let result = runtime
            .execute(
                "query SearchUnitsQuery { searchUnitsQuery { totalHits results { unitId } } }",
                Some(json!({
                    "filter": {
                        "query": "tenantid = 42 and name = '*graphql*'",
                        "order": {"field": "created", "descending": true},
                        "paging": {"limit": 10, "offset": 0}
                    }
                })),
            )
            .expect("graphql searchUnitsQuery with filter object should succeed");
        assert_eq!(result["data"]["searchUnitsQuery"]["totalHits"], 1);
    }

    #[test]
    fn graphql_execute_units_raw_filter_where_mvp() {
        use base64::Engine as _;

        let runtime = GraphqlRuntime::new(RepoService::new(Arc::new(MockBackend)));
        let result = runtime
            .execute(
                "query UnitsRaw { unitsRaw }",
                Some(json!({
                    "filter": {
                        "tenantId": 42,
                        "where": "name = '*graphql*'",
                        "order": {"field": "created", "descending": true},
                        "paging": {"limit": 10, "offset": 0}
                    }
                })),
            )
            .expect("graphql unitsRaw should succeed");
        let encoded = result["data"]["unitsRaw"]
            .as_str()
            .expect("unitsRaw should be a base64 string");
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .expect("unitsRaw decode");
        let payloads: Value = serde_json::from_slice(&decoded).expect("unitsRaw json");
        assert!(payloads.as_array().is_some_and(|arr| !arr.is_empty()));
    }

    #[test]
    fn graphql_execute_search_raw_alias_mvp() {
        use base64::Engine as _;

        let runtime = GraphqlRuntime::new(RepoService::new(Arc::new(MockBackend)));
        let result = runtime
            .execute(
                "query SearchRaw { searchRaw }",
                Some(json!({
                    "filter": {
                        "tenantId": 42,
                        "where": "name = '*graphql*'",
                        "order": {"field": "created", "descending": true},
                        "paging": {"limit": 10, "offset": 0}
                    }
                })),
            )
            .expect("graphql searchRaw alias should succeed");
        let encoded = result["data"]["searchRaw"]
            .as_str()
            .expect("searchRaw should be a base64 string");
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .expect("searchRaw decode");
        let payloads: Value = serde_json::from_slice(&decoded).expect("searchRaw json");
        assert!(payloads.as_array().is_some_and(|arr| !arr.is_empty()));
    }

    #[test]
    fn graphql_execute_search_units_query_strict_mvp() {
        let runtime = GraphqlRuntime::new(RepoService::new(Arc::new(MockBackend)));
        let result = runtime
            .execute(
                "query SearchUnitsQueryStrict { searchUnitsQueryStrict { totalHits results { unitId } } }",
                Some(json!({
                    "query": "attr:mock_attr = 'value'",
                    "order": {"field": "created", "descending": true},
                    "paging": {"limit": 10, "offset": 0}
                })),
            )
            .expect("graphql searchUnitsQueryStrict should succeed");
        assert_eq!(result["data"]["searchUnitsQueryStrict"]["totalHits"], 1);
    }

    #[test]
    fn graphql_execute_rejects_unsupported_operations() {
        let runtime = GraphqlRuntime::new(RepoService::new(Arc::new(MockBackend)));
        let result = runtime
            .execute("query UnknownOp { frobnicate }", None)
            .expect("graphql envelope should be returned");
        assert_eq!(result["data"], Value::Null);
        assert_eq!(result["errors"][0]["extensions"]["code"], "UNSUPPORTED");
        assert_eq!(result["errors"][0]["extensions"]["operationType"], "query");
        assert_eq!(result["errors"][0]["extensions"]["operation"], "frobnicate");
        assert!(result["errors"][0]["extensions"]["supportedOperations"]
            .as_array()
            .is_some_and(|ops| ops.iter().any(|op| op == "searchUnits")));
    }

    #[test]
    fn graphql_execute_invalid_input_returns_error_envelope() {
        let runtime = GraphqlRuntime::new(RepoService::new(Arc::new(MockBackend)));
        let result = runtime
            .execute("query GetUnit { getUnit { unitid } }", Some(json!({})))
            .expect("graphql envelope should be returned");
        assert_eq!(result["data"], Value::Null);
        assert_eq!(result["errors"][0]["extensions"]["code"], "INVALID_INPUT");
    }

    #[test]
    fn graphql_execute_get_unit_mvp() {
        let runtime = GraphqlRuntime::new(RepoService::new(Arc::new(MockBackend)));
        let result = runtime
            .execute(
                "query GetUnit { getUnit { unitid } }",
                Some(json!({
                    "tenantid": 42,
                    "unitid": 1001,
                    "version": 1
                })),
            )
            .expect("graphql getUnit should succeed");
        assert_eq!(result["data"]["getUnit"]["unitid"], 1001);
    }

    #[test]
    fn graphql_execute_get_unit_accepts_id_object_mvp() {
        let runtime = GraphqlRuntime::new(RepoService::new(Arc::new(MockBackend)));
        let result = runtime
            .execute(
                "query GetUnit { getUnit { unitid } }",
                Some(json!({
                    "id": {
                        "tenantId": 42,
                        "unitId": 1001,
                        "unitver": 1
                    }
                })),
            )
            .expect("graphql getUnit with id object should succeed");
        assert_eq!(result["data"]["getUnit"]["unitid"], 1001);
    }

    #[test]
    fn graphql_execute_unit_alias_mvp() {
        let runtime = GraphqlRuntime::new(RepoService::new(Arc::new(MockBackend)));
        let result = runtime
            .execute(
                "query Unit { unit { unitid } }",
                Some(json!({
                    "id": {
                        "tenantId": 42,
                        "unitId": 1001,
                    }
                })),
            )
            .expect("graphql unit alias should succeed");
        assert_eq!(result["data"]["unit"]["unitid"], 1001);
    }

    #[test]
    fn graphql_execute_load_unit_alias_mvp() {
        let runtime = GraphqlRuntime::new(RepoService::new(Arc::new(MockBackend)));
        let result = runtime
            .execute(
                "query LoadUnit { loadUnit { unitid } }",
                Some(json!({
                    "id": {
                        "tenantId": 42,
                        "unitId": 1001
                    }
                })),
            )
            .expect("graphql loadUnit alias should succeed");
        assert_eq!(result["data"]["loadUnit"]["unitid"], 1001);
    }

    #[test]
    fn graphql_execute_unit_raw_mvp() {
        use base64::Engine as _;

        let runtime = GraphqlRuntime::new(RepoService::new(Arc::new(MockBackend)));
        let result = runtime
            .execute(
                "query UnitRaw { unitRaw }",
                Some(json!({
                    "id": {
                        "tenantId": 42,
                        "unitId": 1001
                    }
                })),
            )
            .expect("graphql unitRaw should succeed");
        let encoded = result["data"]["unitRaw"]
            .as_str()
            .expect("unitRaw should be a base64 string");
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .expect("unitRaw decode");
        let payload: Value = serde_json::from_slice(&decoded).expect("unitRaw json");
        assert_eq!(payload["unitid"], 1001);
    }

    #[test]
    fn graphql_execute_load_unit_raw_alias_mvp() {
        use base64::Engine as _;

        let runtime = GraphqlRuntime::new(RepoService::new(Arc::new(MockBackend)));
        let result = runtime
            .execute(
                "query LoadUnitRaw { loadUnitRaw }",
                Some(json!({
                    "id": {
                        "tenantId": 42,
                        "unitId": 1001
                    }
                })),
            )
            .expect("graphql loadUnitRaw alias should succeed");
        let encoded = result["data"]["loadUnitRaw"]
            .as_str()
            .expect("loadUnitRaw should be a base64 string");
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .expect("loadUnitRaw decode");
        let payload: Value = serde_json::from_slice(&decoded).expect("loadUnitRaw json");
        assert_eq!(payload["unitid"], 1001);
    }

    #[test]
    fn graphql_execute_get_unit_meta_mvp() {
        let runtime = GraphqlRuntime::new(RepoService::new(Arc::new(MockBackend)));
        let found = runtime
            .execute(
                "query GetUnitMeta { getUnitMeta { exists status unitver corrid isreadonly } }",
                Some(json!({
                    "tenantid": 42,
                    "unitid": 1001
                })),
            )
            .expect("graphql getUnitMeta should succeed");
        assert_eq!(found["data"]["getUnitMeta"]["exists"], true);
        assert_eq!(found["data"]["getUnitMeta"]["status"], 30);
        assert_eq!(found["data"]["getUnitMeta"]["unitver"], 1);

        let missing = runtime
            .execute(
                "query GetUnitMeta { getUnitMeta { exists status unitver corrid isreadonly } }",
                Some(json!({
                    "tenantid": 42,
                    "unitid": 999999
                })),
            )
            .expect("graphql getUnitMeta missing should succeed");
        assert_eq!(missing["data"]["getUnitMeta"]["exists"], false);
        assert!(missing["data"]["getUnitMeta"]["status"].is_null());
        assert!(missing["data"]["getUnitMeta"]["unitver"].is_null());
    }

    #[test]
    fn graphql_execute_get_unit_status_mvp() {
        let runtime = GraphqlRuntime::new(RepoService::new(Arc::new(MockBackend)));
        let result = runtime
            .execute(
                "query GetUnitStatus { getUnitStatus }",
                Some(json!({
                    "tenantid": 42,
                    "unitid": 1001
                })),
            )
            .expect("graphql getUnitStatus should succeed");
        assert_eq!(result["data"]["getUnitStatus"], 30);
    }

    #[test]
    fn graphql_execute_get_unit_by_corrid_mvp() {
        let runtime = GraphqlRuntime::new(RepoService::new(Arc::new(MockBackend)));
        let result = runtime
            .execute(
                "query GetUnitByCorrid { getUnitByCorrid { unitid } }",
                Some(json!({
                    "corrid": "00000000-0000-0000-0000-000000000000"
                })),
            )
            .expect("graphql getUnitByCorrid should succeed");
        assert_eq!(result["data"]["getUnitByCorrid"]["unitid"], 1001);
    }

    #[test]
    fn graphql_execute_get_unit_by_corrid_accepts_id_object_mvp() {
        let runtime = GraphqlRuntime::new(RepoService::new(Arc::new(MockBackend)));
        let result = runtime
            .execute(
                "query GetUnitByCorrid { getUnitByCorrid { unitid } }",
                Some(json!({
                    "id": {
                        "corrId": "00000000-0000-0000-0000-000000000000"
                    }
                })),
            )
            .expect("graphql getUnitByCorrid with id object should succeed");
        assert_eq!(result["data"]["getUnitByCorrid"]["unitid"], 1001);
    }

    #[test]
    fn graphql_execute_unit_by_corrid_alias_mvp() {
        let runtime = GraphqlRuntime::new(RepoService::new(Arc::new(MockBackend)));
        let result = runtime
            .execute(
                "query UnitByCorrid { unitByCorrid { unitid } }",
                Some(json!({
                    "id": {
                        "corrId": "00000000-0000-0000-0000-000000000000"
                    }
                })),
            )
            .expect("graphql unitByCorrid alias should succeed");
        assert_eq!(result["data"]["unitByCorrid"]["unitid"], 1001);
    }

    #[test]
    fn graphql_execute_load_by_corrid_alias_mvp() {
        let runtime = GraphqlRuntime::new(RepoService::new(Arc::new(MockBackend)));
        let result = runtime
            .execute(
                "query LoadByCorrId { loadByCorrId { unitid } }",
                Some(json!({
                    "id": {
                        "corrId": "00000000-0000-0000-0000-000000000000"
                    }
                })),
            )
            .expect("graphql loadByCorrId alias should succeed");
        assert_eq!(result["data"]["loadByCorrId"]["unitid"], 1001);
    }

    #[test]
    fn graphql_execute_unit_raw_by_corrid_alias_mvp() {
        use base64::Engine as _;

        let runtime = GraphqlRuntime::new(RepoService::new(Arc::new(MockBackend)));
        let result = runtime
            .execute(
                "query UnitRawByCorrid { unitRawByCorrid }",
                Some(json!({
                    "corrid": "00000000-0000-0000-0000-000000000000"
                })),
            )
            .expect("graphql unitRawByCorrid alias should succeed");
        let encoded = result["data"]["unitRawByCorrid"]
            .as_str()
            .expect("unitRawByCorrid should be a base64 string");
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .expect("unitRawByCorrid decode");
        let payload: Value = serde_json::from_slice(&decoded).expect("unitRawByCorrid json");
        assert_eq!(payload["unitid"], 1001);
    }

    #[test]
    fn graphql_execute_load_raw_payload_by_corrid_alias_mvp() {
        use base64::Engine as _;

        let runtime = GraphqlRuntime::new(RepoService::new(Arc::new(MockBackend)));
        let result = runtime
            .execute(
                "query LoadRawPayloadByCorrId { loadRawPayloadByCorrId }",
                Some(json!({
                    "corrid": "00000000-0000-0000-0000-000000000000"
                })),
            )
            .expect("graphql loadRawPayloadByCorrId alias should succeed");
        let encoded = result["data"]["loadRawPayloadByCorrId"]
            .as_str()
            .expect("loadRawPayloadByCorrId should be a base64 string");
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .expect("loadRawPayloadByCorrId decode");
        let payload: Value = serde_json::from_slice(&decoded).expect("loadRawPayloadByCorrId json");
        assert_eq!(payload["unitid"], 1001);
    }

    #[test]
    fn graphql_execute_unit_exists_mvp() {
        let runtime = GraphqlRuntime::new(RepoService::new(Arc::new(MockBackend)));
        let result = runtime
            .execute(
                "query UnitExists { unitExists }",
                Some(json!({
                    "tenantid": 42,
                    "unitid": 1001
                })),
            )
            .expect("graphql unitExists should succeed");
        assert_eq!(result["data"]["unitExists"], true);
    }

    #[test]
    fn graphql_execute_unit_lifecycle_mvp() {
        let runtime = GraphqlRuntime::new(RepoService::new(Arc::new(MockBackend)));
        let result = runtime
            .execute(
                "query UnitLifecycle { unitLifecycle { exists locked status } }",
                Some(json!({
                    "tenantid": 42,
                    "unitid": 1001
                })),
            )
            .expect("graphql unitLifecycle should succeed");
        assert_eq!(result["data"]["unitLifecycle"]["exists"], true);
        assert_eq!(result["data"]["unitLifecycle"]["locked"], false);
        assert_eq!(result["data"]["unitLifecycle"]["status"], 30);

        let locked_result = runtime
            .execute(
                "query UnitLifecycle { unitLifecycle { exists locked status } }",
                Some(json!({
                    "tenantid": 42,
                    "unitid": 1002
                })),
            )
            .expect("graphql unitLifecycle locked should succeed");
        assert_eq!(locked_result["data"]["unitLifecycle"]["exists"], true);
        assert_eq!(locked_result["data"]["unitLifecycle"]["locked"], true);
    }

    #[test]
    fn graphql_execute_store_unit_mutation_mvp() {
        let runtime = GraphqlRuntime::new(RepoService::new(Arc::new(MockBackend)));
        let result = runtime
            .execute(
                "mutation StoreUnit { storeUnit { tenantid unitid unitver status unitname } }",
                Some(json!({
                    "unit": {
                        "tenantid": 42,
                        "status": 30,
                        "unitname": "graphql-stored-unit"
                    }
                })),
            )
            .expect("graphql storeUnit should succeed");
        assert_eq!(result["data"]["storeUnit"]["tenantid"], 42);
        assert_eq!(result["data"]["storeUnit"]["unitver"], 1);
        assert_eq!(result["data"]["storeUnit"]["status"], 30);
        assert_eq!(
            result["data"]["storeUnit"]["unitname"],
            "graphql-stored-unit"
        );
    }

    #[test]
    fn graphql_execute_create_unit_mutation_mvp() {
        let runtime = GraphqlRuntime::new(RepoService::new(Arc::new(MockBackend)));
        let result = runtime
            .execute(
                "mutation CreateUnit { createUnit { tenantid unitid unitver status unitname } }",
                Some(json!({
                    "tenantid": 42,
                    "unitname": "graphql-created-unit"
                })),
            )
            .expect("graphql createUnit should succeed");
        assert_eq!(result["data"]["createUnit"]["tenantid"], 42);
        assert_eq!(result["data"]["createUnit"]["unitver"], 1);
        assert_eq!(result["data"]["createUnit"]["status"], 30);
        assert_eq!(
            result["data"]["createUnit"]["unitname"],
            "graphql-created-unit"
        );
    }

    #[test]
    fn graphql_execute_lagra_unit_raw_mutation_mvp() {
        use base64::Engine as _;

        let runtime = GraphqlRuntime::new(RepoService::new(Arc::new(MockBackend)));
        let payload = serde_json::json!({
            "tenantid": 42,
            "unitname": "raw-created-unit",
            "status": 30
        });
        let encoded_input = base64::engine::general_purpose::STANDARD
            .encode(serde_json::to_vec(&payload).expect("payload json"));
        let result = runtime
            .execute(
                "mutation LagraUnitRaw { lagraUnitRaw }",
                Some(json!({
                    "data": encoded_input
                })),
            )
            .expect("graphql lagraUnitRaw should succeed");
        let encoded_output = result["data"]["lagraUnitRaw"]
            .as_str()
            .expect("lagraUnitRaw should be a base64 string");
        let decoded_output = base64::engine::general_purpose::STANDARD
            .decode(encoded_output)
            .expect("lagraUnitRaw decode");
        let stored: Value =
            serde_json::from_slice(&decoded_output).expect("lagraUnitRaw stored json");
        assert_eq!(stored["tenantid"], 42);
        assert_eq!(stored["unitname"], "raw-created-unit");
    }

    #[test]
    fn graphql_execute_store_raw_unit_alias_mvp() {
        use base64::Engine as _;

        let runtime = GraphqlRuntime::new(RepoService::new(Arc::new(MockBackend)));
        let payload = serde_json::json!({
            "tenantid": 42,
            "unitname": "raw-created-unit-2",
            "status": 30
        });
        let encoded_input = base64::engine::general_purpose::STANDARD
            .encode(serde_json::to_vec(&payload).expect("payload json"));
        let result = runtime
            .execute(
                "mutation StoreRawUnit { storeRawUnit }",
                Some(json!({
                    "data": encoded_input
                })),
            )
            .expect("graphql storeRawUnit alias should succeed");
        let encoded_output = result["data"]["storeRawUnit"]
            .as_str()
            .expect("storeRawUnit should be a base64 string");
        let decoded_output = base64::engine::general_purpose::STANDARD
            .decode(encoded_output)
            .expect("storeRawUnit decode");
        let stored: Value = serde_json::from_slice(&decoded_output).expect("storeRawUnit json");
        assert_eq!(stored["tenantid"], 42);
        assert_eq!(stored["unitname"], "raw-created-unit-2");
    }

    #[test]
    fn graphql_execute_get_attribute_info_mvp() {
        let runtime = GraphqlRuntime::new(RepoService::new(Arc::new(MockBackend)));
        let result = runtime
            .execute(
                "query GetAttributeInfo { getAttributeInfo { id name } }",
                Some(json!({"nameOrId": "mock-attr"})),
            )
            .expect("graphql getAttributeInfo should succeed");
        assert_eq!(result["data"]["getAttributeInfo"]["id"], 7);
        assert_eq!(result["data"]["getAttributeInfo"]["name"], "mock-attr");
    }

    #[test]
    fn graphql_execute_get_attribute_info_accepts_numeric_id_var() {
        let runtime = GraphqlRuntime::new(RepoService::new(Arc::new(MockBackend)));
        let result = runtime
            .execute(
                "query GetAttributeInfo { getAttributeInfo { id name } }",
                Some(json!({"id": 7})),
            )
            .expect("graphql getAttributeInfo by numeric id should succeed");
        assert_eq!(result["data"]["getAttributeInfo"]["id"], 7);
    }

    #[test]
    fn graphql_execute_get_tenant_info_mvp() {
        let runtime = GraphqlRuntime::new(RepoService::new(Arc::new(MockBackend)));
        let result = runtime
            .execute(
                "query GetTenantInfo { getTenantInfo { id name } }",
                Some(json!({"nameOrId": "mock-tenant"})),
            )
            .expect("graphql getTenantInfo should succeed");
        assert_eq!(result["data"]["getTenantInfo"]["id"], 42);
        assert_eq!(result["data"]["getTenantInfo"]["name"], "mock-tenant");
    }

    #[test]
    fn graphql_execute_get_tenant_info_accepts_numeric_id_var() {
        let runtime = GraphqlRuntime::new(RepoService::new(Arc::new(MockBackend)));
        let result = runtime
            .execute(
                "query GetTenantInfo { getTenantInfo { id name } }",
                Some(json!({"id": 42})),
            )
            .expect("graphql getTenantInfo by numeric id should succeed");
        assert_eq!(result["data"]["getTenantInfo"]["id"], 42);
    }

    #[test]
    fn graphql_execute_set_status_mutation_mvp() {
        let runtime = GraphqlRuntime::new(RepoService::new(Arc::new(MockBackend)));
        let result = runtime
            .execute(
                "mutation SetStatus { setStatus { ok status } }",
                Some(json!({
                    "tenantid": 42,
                    "unitid": 1001,
                    "status": 30
                })),
            )
            .expect("graphql setStatus mutation should succeed");
        assert_eq!(result["data"]["setStatus"]["ok"], true);
        assert_eq!(result["data"]["setStatus"]["status"], 30);
    }

    #[test]
    fn graphql_execute_lock_unlock_mutations_mvp() {
        let runtime = GraphqlRuntime::new(RepoService::new(Arc::new(MockBackend)));
        let lock_result = runtime
            .execute(
                "mutation LockUnit { lockUnit { ok } }",
                Some(json!({
                    "tenantid": 42,
                    "unitid": 1001,
                    "lockType": 30,
                    "purpose": "graphql-test"
                })),
            )
            .expect("graphql lockUnit mutation should succeed");
        assert_eq!(lock_result["data"]["lockUnit"]["ok"], true);

        let unlock_result = runtime
            .execute(
                "mutation UnlockUnit { unlockUnit { ok } }",
                Some(json!({
                    "tenantid": 42,
                    "unitid": 1001
                })),
            )
            .expect("graphql unlockUnit mutation should succeed");
        assert_eq!(unlock_result["data"]["unlockUnit"]["ok"], true);

        let locked = runtime
            .execute(
                "query IsUnitLocked { isUnitLocked }",
                Some(json!({
                    "tenantid": 42,
                    "unitid": 1002
                })),
            )
            .expect("graphql isUnitLocked query should succeed");
        assert_eq!(locked["data"]["isUnitLocked"], true);
    }

    #[test]
    fn graphql_execute_relation_association_mutations_mvp() {
        let runtime = GraphqlRuntime::new(RepoService::new(Arc::new(MockBackend)));
        let add_relation = runtime
            .execute(
                "mutation AddRelation { addRelation { ok } }",
                Some(json!({
                    "tenantid": 42,
                    "unitid": 1001,
                    "relationType": 11,
                    "otherTenantId": 42,
                    "otherUnitId": 1002
                })),
            )
            .expect("graphql addRelation mutation should succeed");
        assert_eq!(add_relation["data"]["addRelation"]["ok"], true);

        let remove_relation = runtime
            .execute(
                "mutation RemoveRelation { removeRelation { ok } }",
                Some(json!({
                    "tenantid": 42,
                    "unitid": 1001,
                    "relationType": 11,
                    "otherTenantId": 42,
                    "otherUnitId": 1002
                })),
            )
            .expect("graphql removeRelation mutation should succeed");
        assert_eq!(remove_relation["data"]["removeRelation"]["ok"], true);

        let add_association = runtime
            .execute(
                "mutation AddAssociation { addAssociation { ok } }",
                Some(json!({
                    "tenantid": 42,
                    "unitid": 1001,
                    "associationType": 21,
                    "reference": "graphql-ref"
                })),
            )
            .expect("graphql addAssociation mutation should succeed");
        assert_eq!(add_association["data"]["addAssociation"]["ok"], true);

        let remove_association = runtime
            .execute(
                "mutation RemoveAssociation { removeAssociation { ok } }",
                Some(json!({
                    "tenantid": 42,
                    "unitid": 1001,
                    "associationType": 21,
                    "reference": "graphql-ref"
                })),
            )
            .expect("graphql removeAssociation mutation should succeed");
        assert_eq!(remove_association["data"]["removeAssociation"]["ok"], true);

        let create_attribute = runtime
            .execute(
                "mutation CreateAttribute { createAttribute { id name } }",
                Some(json!({
                    "alias": "mock-attr-alias",
                    "name": "mock-attr",
                    "qualname": "mock.attr",
                    "attributeType": "string",
                    "isArray": false
                })),
            )
            .expect("graphql createAttribute mutation should succeed");
        assert_eq!(create_attribute["data"]["createAttribute"]["id"], 8);
        assert_eq!(
            create_attribute["data"]["createAttribute"]["name"],
            "mock-attr"
        );
    }

    #[test]
    fn graphql_execute_relation_association_reads_and_health_mvp() {
        let runtime = GraphqlRuntime::new(RepoService::new(Arc::new(MockBackend)));

        let right_relation = runtime
            .execute(
                "query GetRightRelation { getRightRelation { related_unit_id } }",
                Some(json!({"tenantid": 42, "unitid": 1001, "relationType": 11})),
            )
            .expect("graphql getRightRelation should succeed");
        assert_eq!(
            right_relation["data"]["getRightRelation"]["related_unit_id"],
            1002
        );

        let right_rel_count = runtime
            .execute(
                "query CountRightRelations { countRightRelations }",
                Some(json!({"tenantid": 42, "unitid": 1001, "relationType": 11})),
            )
            .expect("graphql countRightRelations should succeed");
        assert_eq!(right_rel_count["data"]["countRightRelations"], 1);

        let right_assoc = runtime
            .execute(
                "query GetRightAssociation { getRightAssociation { reference } }",
                Some(json!({"tenantid": 42, "unitid": 1001, "associationType": 21})),
            )
            .expect("graphql getRightAssociation should succeed");
        assert_eq!(
            right_assoc["data"]["getRightAssociation"]["reference"],
            "graphql-ref"
        );

        let left_assoc_count = runtime
            .execute(
                "query CountLeftAssociations { countLeftAssociations }",
                Some(json!({"associationType": 21, "reference": "graphql-ref"})),
            )
            .expect("graphql countLeftAssociations should succeed");
        assert_eq!(left_assoc_count["data"]["countLeftAssociations"], 1);

        let health = runtime
            .execute("query Health { health { status } }", None)
            .expect("graphql health should succeed");
        assert_eq!(health["data"]["health"]["status"], "ok");
    }

    #[test]
    fn graphql_execute_dispatch_avoids_operation_name_collisions() {
        let runtime = GraphqlRuntime::new(RepoService::new(Arc::new(MockBackend)));

        let right_relations = runtime
            .execute(
                "query GetRightRelations { getRightRelations { related_unit_id } }",
                Some(json!({"tenantid": 42, "unitid": 1001, "relationType": 11})),
            )
            .expect("graphql getRightRelations should succeed");
        assert!(right_relations["data"]["getRightRelations"].is_array());

        let unlock = runtime
            .execute(
                "mutation UnlockUnit { unlockUnit { ok } }",
                Some(json!({"tenantid": 42, "unitid": 1001})),
            )
            .expect("graphql unlockUnit mutation should succeed");
        assert_eq!(unlock["data"]["unlockUnit"]["ok"], true);
    }

    #[test]
    fn graphql_allowlist_limits_operations() {
        let runtime = GraphqlRuntime::with_operation_allowlist(
            RepoService::new(Arc::new(MockBackend)),
            &["health"],
        )
        .expect("allowlist should be valid");

        let allowed = runtime
            .execute("query Health { health { status } }", None)
            .expect("graphql health should succeed");
        assert_eq!(allowed["data"]["health"]["status"], "ok");

        let blocked = runtime
            .execute(
                "query SearchUnits { searchUnits { totalHits } }",
                Some(json!({"expression": {"tenantid": 42}})),
            )
            .expect("graphql envelope should be returned");
        assert_eq!(blocked["data"], Value::Null);
        assert_eq!(blocked["errors"][0]["extensions"]["code"], "UNSUPPORTED");
        assert_eq!(
            blocked["errors"][0]["extensions"]["supportedOperations"],
            json!(["health"])
        );
    }

    #[test]
    fn graphql_registered_operations_reflect_allowlist() {
        let runtime = GraphqlRuntime::with_operation_allowlist(
            RepoService::new(Arc::new(MockBackend)),
            &["health", "set_status"],
        )
        .expect("allowlist should be valid");

        let registered = runtime.registered_operations();
        assert_eq!(registered["queries"], json!(["health"]));
        assert_eq!(registered["mutations"], json!(["setStatus"]));
    }

    #[test]
    fn graphql_execute_registered_operations_query() {
        let runtime = GraphqlRuntime::new(RepoService::new(Arc::new(MockBackend)));
        let result = runtime
            .execute(
                "query RegisteredOperations { registeredOperations { queries mutations } }",
                None,
            )
            .expect("graphql registeredOperations should succeed");
        assert!(result["data"]["registeredOperations"]["queries"]
            .as_array()
            .is_some_and(|ops| ops.iter().any(|op| op == "searchUnits")));
        assert!(result["data"]["registeredOperations"]["mutations"]
            .as_array()
            .is_some_and(|ops| ops.iter().any(|op| op == "setStatus")));
    }

    #[test]
    fn graphql_execute_registered_operations_query_respects_allowlist() {
        let runtime = GraphqlRuntime::with_operation_allowlist(
            RepoService::new(Arc::new(MockBackend)),
            &["registered_operations", "health", "set_status"],
        )
        .expect("allowlist should be valid");
        let result = runtime
            .execute(
                "query RegisteredOperations { registeredOperations { queries mutations } }",
                None,
            )
            .expect("graphql registeredOperations should succeed");
        assert_eq!(
            result["data"]["registeredOperations"]["queries"],
            json!(["registeredOperations", "health"])
        );
        assert_eq!(
            result["data"]["registeredOperations"]["mutations"],
            json!(["setStatus"])
        );
    }

    #[test]
    fn graphql_execute_lifecycle_and_mutability_mvp() {
        let runtime = GraphqlRuntime::new(RepoService::new(Arc::new(MockBackend)));

        let activate = runtime
            .execute(
                "mutation ActivateUnit { activateUnit { ok } }",
                Some(json!({"tenantid": 42, "unitid": 1001})),
            )
            .expect("graphql activateUnit mutation should succeed");
        assert_eq!(activate["data"]["activateUnit"]["ok"], true);

        let inactivate = runtime
            .execute(
                "mutation InactivateUnit { inactivateUnit { ok } }",
                Some(json!({"tenantid": 42, "unitid": 1001})),
            )
            .expect("graphql inactivateUnit mutation should succeed");
        assert_eq!(inactivate["data"]["inactivateUnit"]["ok"], true);

        let transition = runtime
            .execute(
                "mutation RequestStatusTransition { requestStatusTransition { status } }",
                Some(json!({
                    "tenantid": 42,
                    "unitid": 1001,
                    "requestedStatus": 10
                })),
            )
            .expect("graphql requestStatusTransition mutation should succeed");
        assert_eq!(transition["data"]["requestStatusTransition"]["status"], 10);

        let transition_alias = runtime
            .execute(
                "mutation TransitionUnitStatus { transitionUnitStatus { status } }",
                Some(json!({
                    "tenantid": 42,
                    "unitid": 1001,
                    "requestedStatus": 10
                })),
            )
            .expect("graphql transitionUnitStatus mutation should succeed");
        assert_eq!(
            transition_alias["data"]["transitionUnitStatus"]["status"],
            10
        );

        let can_change = runtime
            .execute(
                "query CanChangeAttribute { canChangeAttribute }",
                Some(json!({"nameOrId": "mock-attr"})),
            )
            .expect("graphql canChangeAttribute query should succeed");
        assert_eq!(can_change["data"]["canChangeAttribute"], false);

        let flush = runtime
            .execute("mutation FlushCache { flushCache { ok } }", None)
            .expect("graphql flushCache mutation should succeed");
        assert_eq!(flush["data"]["flushCache"]["ok"], true);
    }

    #[test]
    fn graphql_execute_mapping_and_instantiation_mvp() {
        let runtime = GraphqlRuntime::new(RepoService::new(Arc::new(MockBackend)));

        let instantiated = runtime
            .execute(
                "query InstantiateAttribute { instantiateAttribute { id name } }",
                Some(json!({"nameOrId": "mock-attr"})),
            )
            .expect("graphql instantiateAttribute should succeed");
        assert_eq!(instantiated["data"]["instantiateAttribute"]["id"], 7);

        let instantiated_by_id = runtime
            .execute(
                "query InstantiateAttribute { instantiateAttribute { id name } }",
                Some(json!({"id": 7})),
            )
            .expect("graphql instantiateAttribute by numeric id should succeed");
        assert_eq!(instantiated_by_id["data"]["instantiateAttribute"]["id"], 7);

        let attr_name_to_id = runtime
            .execute(
                "query AttributeNameToId { attributeNameToId }",
                Some(json!({"attributeName": "mock-attr"})),
            )
            .expect("graphql attributeNameToId should succeed");
        assert_eq!(attr_name_to_id["data"]["attributeNameToId"], 7);

        let attr_id_to_name = runtime
            .execute(
                "query AttributeIdToName { attributeIdToName }",
                Some(json!({"attributeId": 7})),
            )
            .expect("graphql attributeIdToName should succeed");
        assert_eq!(attr_id_to_name["data"]["attributeIdToName"], "mock-attr");

        let tenant_name_to_id = runtime
            .execute(
                "query TenantNameToId { tenantNameToId }",
                Some(json!({"tenantName": "mock-tenant"})),
            )
            .expect("graphql tenantNameToId should succeed");
        assert_eq!(tenant_name_to_id["data"]["tenantNameToId"], 42);

        let tenant_id_to_name = runtime
            .execute(
                "query TenantIdToName { tenantIdToName }",
                Some(json!({"tenantId": 42})),
            )
            .expect("graphql tenantIdToName should succeed");
        assert_eq!(tenant_id_to_name["data"]["tenantIdToName"], "mock-tenant");

        let instantiated_mutation = runtime
            .execute(
                "mutation InstantiateAttributeMutation { instantiateAttributeMutation { id name } }",
                Some(json!({"nameOrId": "mock-attr"})),
            )
            .expect("graphql instantiateAttributeMutation should succeed");
        assert_eq!(
            instantiated_mutation["data"]["instantiateAttributeMutation"]["id"],
            7
        );

        let instantiated_mutation_by_id = runtime
            .execute(
                "mutation InstantiateAttributeMutation { instantiateAttributeMutation { id name } }",
                Some(json!({"id": 7})),
            )
            .expect("graphql instantiateAttributeMutation by numeric id should succeed");
        assert_eq!(
            instantiated_mutation_by_id["data"]["instantiateAttributeMutation"]["id"],
            7
        );
    }
}
