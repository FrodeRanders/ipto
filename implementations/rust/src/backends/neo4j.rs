use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::time::{SystemTime, UNIX_EPOCH};

use chrono::{DateTime, SecondsFormat, Utc};
use serde_json::{json, Map, Value};
use uuid::Uuid;

use crate::backend::{Backend, RepoError, RepoResult};
use crate::model::{
    Association, Relation, SearchOrder, SearchPaging, SearchResult, Unit, UnitRef, VersionSelector,
};
use crate::search_expr::{parse_bool_expression, BoolExpr};
use crate::search_filters::{
    extract_association_constraint, extract_attribute_constraint, extract_relation_constraint,
    extract_unit_predicates, parse_time_millis, AssociationConstraint, AttributeCmpConstraint,
    RelationConstraint, RelationSide, UnitField, UnitOp, UnitPredicate, UnitValue,
};

pub struct Neo4jBackend {
    url: String,
    database: String,
    user: String,
    password: String,
}

static NEO4J_BOOTSTRAPPED: AtomicBool = AtomicBool::new(false);
impl Neo4jBackend {
    pub fn new() -> Self {
        Self {
            url: std::env::var("IPTO_NEO4J_URL")
                .unwrap_or_else(|_| "http://localhost:7474".to_string()),
            database: std::env::var("IPTO_NEO4J_DATABASE").unwrap_or_else(|_| "neo4j".to_string()),
            user: std::env::var("IPTO_NEO4J_USER").unwrap_or_else(|_| "neo4j".to_string()),
            password: std::env::var("IPTO_NEO4J_PASSWORD").unwrap_or_else(|_| "neo4j".to_string()),
        }
    }

    fn endpoint(&self) -> String {
        format!(
            "{}/db/{}/tx/commit",
            self.url.trim_end_matches('/'),
            self.database
        )
    }

    fn query_raw(&self, statement: &str, parameters: Value) -> RepoResult<Vec<Vec<Value>>> {
        let payload = json!({
            "statements": [
                {
                    "statement": statement,
                    "parameters": parameters
                }
            ]
        });

        let basic = {
            use base64::Engine as _;
            let creds = format!("{}:{}", self.user, self.password);
            format!(
                "Basic {}",
                base64::engine::general_purpose::STANDARD.encode(creds)
            )
        };

        let response = ureq::post(&self.endpoint())
            .set("Authorization", &basic)
            .set("Content-Type", "application/json")
            .send_json(payload)
            .map_err(|e| RepoError::Backend(format!("neo4j request failed: {e}")))?;

        let body: Value = response
            .into_json()
            .map_err(|e| RepoError::Backend(format!("neo4j response decode failed: {e}")))?;

        if let Some(errors) = body.get("errors").and_then(Value::as_array) {
            if !errors.is_empty() {
                let first = &errors[0];
                let code = first
                    .get("code")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown_code");
                let msg = first
                    .get("message")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown error");
                return Err(RepoError::Backend(format!("neo4j error {code}: {msg}")));
            }
        }

        let mut rows: Vec<Vec<Value>> = Vec::new();
        if let Some(results) = body.get("results").and_then(Value::as_array) {
            if let Some(first_result) = results.first() {
                if let Some(data) = first_result.get("data").and_then(Value::as_array) {
                    for entry in data {
                        if let Some(row) = entry.get("row").and_then(Value::as_array) {
                            rows.push(row.to_vec());
                        }
                    }
                }
            }
        }

        Ok(rows)
    }

    fn query(&self, statement: &str, parameters: Value) -> RepoResult<Vec<Vec<Value>>> {
        self.ensure_bootstrap()?;
        self.query_raw(statement, parameters)
    }

    fn ensure_bootstrap(&self) -> RepoResult<()> {
        if NEO4J_BOOTSTRAPPED.load(AtomicOrdering::Acquire) {
            return Ok(());
        }

        if !env_bool("IPTO_NEO4J_BOOTSTRAP", true) {
            NEO4J_BOOTSTRAPPED.store(true, AtomicOrdering::Release);
            return Ok(());
        }

        let statements = [
            "CREATE CONSTRAINT unit_kernel_pk IF NOT EXISTS FOR (k:UnitKernel) REQUIRE (k.tenantid, k.unitid) IS UNIQUE",
            "CREATE CONSTRAINT unit_kernel_corrid IF NOT EXISTS FOR (k:UnitKernel) REQUIRE k.corrid IS UNIQUE",
            "CREATE CONSTRAINT unit_version_pk IF NOT EXISTS FOR (v:UnitVersion) REQUIRE (v.tenantid, v.unitid, v.unitver) IS UNIQUE",
            "CREATE CONSTRAINT counter_name IF NOT EXISTS FOR (c:Counter) REQUIRE c.name IS UNIQUE",
            "CREATE CONSTRAINT attribute_id IF NOT EXISTS FOR (a:Attribute) REQUIRE a.id IS UNIQUE",
            "CREATE CONSTRAINT attribute_name IF NOT EXISTS FOR (a:Attribute) REQUIRE a.name IS UNIQUE",
            "CREATE CONSTRAINT tenant_id IF NOT EXISTS FOR (t:Tenant) REQUIRE t.id IS UNIQUE",
            "CREATE CONSTRAINT tenant_name IF NOT EXISTS FOR (t:Tenant) REQUIRE t.name IS UNIQUE",
            "CREATE CONSTRAINT assoc_ref_unique IF NOT EXISTS FOR (a:AssociationRef) REQUIRE (a.assoctype, a.refstring) IS UNIQUE",
            "CREATE INDEX unit_kernel_status_idx IF NOT EXISTS FOR (k:UnitKernel) ON (k.status)",
            "CREATE INDEX unit_version_name_idx IF NOT EXISTS FOR (v:UnitVersion) ON (v.unitname)",
            "CREATE INDEX assoc_ref_string_idx IF NOT EXISTS FOR (a:AssociationRef) ON (a.refstring)",
        ];

        for stmt in statements {
            self.query_raw(stmt, json!({}))?;
        }

        NEO4J_BOOTSTRAPPED.store(true, AtomicOrdering::Release);
        Ok(())
    }

    fn next_counter(&self, name: &str) -> RepoResult<i64> {
        let rows = self.query(
            "MERGE (c:Counter {name: $name}) ON CREATE SET c.value = 0 SET c.value = c.value + 1 RETURN c.value",
            json!({"name": name}),
        )?;
        extract_i64(rows.first().and_then(|r| r.first())).ok_or_else(|| {
            RepoError::Backend(format!("neo4j counter '{name}' returned invalid value"))
        })
    }

    fn link_unit_version_attributes(
        &self,
        tenant_id: i64,
        unit_id: i64,
        unit_ver: i64,
        attr_ids: &[i64],
        attr_names: &[String],
    ) -> RepoResult<()> {
        // Maintain usage links so can_change_attribute can remain usage-aware.
        if !attr_ids.is_empty() {
            self.query(
                "MATCH (v:UnitVersion {tenantid: $tenantid, unitid: $unitid, unitver: $unitver}) UNWIND $attr_ids AS attrid MATCH (a:Attribute {id: attrid}) MERGE (v)-[:USES_ATTRIBUTE]->(a) RETURN count(a)",
                json!({
                    "tenantid": tenant_id,
                    "unitid": unit_id,
                    "unitver": unit_ver,
                    "attr_ids": attr_ids
                }),
            )?;
        }

        if !attr_names.is_empty() {
            self.query(
                "MATCH (v:UnitVersion {tenantid: $tenantid, unitid: $unitid, unitver: $unitver}) UNWIND $attr_names AS attrname MATCH (a:Attribute {name: attrname}) MERGE (v)-[:USES_ATTRIBUTE]->(a) RETURN count(a)",
                json!({
                    "tenantid": tenant_id,
                    "unitid": unit_id,
                    "unitver": unit_ver,
                    "attr_names": attr_names
                }),
            )?;
        }

        Ok(())
    }

    fn link_unit_version_attribute_values(
        &self,
        tenant_id: i64,
        unit_id: i64,
        unit_ver: i64,
        entries: &[Value],
    ) -> RepoResult<()> {
        if entries.is_empty() {
            return Ok(());
        }

        // Persist flattened scalar values for attribute search, instead of scanning
        // nested payload JSON at query time.
        self.query(
            "MATCH (v:UnitVersion {tenantid: $tenantid, unitid: $unitid, unitver: $unitver}) \
             UNWIND $entries AS e \
             UNWIND coalesce(e.values, []) AS raw \
             WITH v, e, raw WHERE raw IS NOT NULL \
             CREATE (av:AttributeValue { \
               tenantid: v.tenantid, \
               unitid: v.unitid, \
               unitver: v.unitver, \
               attrid: e.attrid, \
               attrname: e.attrname, \
               attrtype: e.attrtype, \
               value: raw \
             }) \
             CREATE (v)-[:HAS_ATTR_VALUE]->(av) \
             RETURN count(av)",
            json!({
                "tenantid": tenant_id,
                "unitid": unit_id,
                "unitver": unit_ver,
                "entries": entries
            }),
        )?;
        Ok(())
    }
}

impl Default for Neo4jBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl Backend for Neo4jBackend {
    fn get_unit_json(
        &self,
        tenant_id: i64,
        unit_id: i64,
        selector: VersionSelector,
    ) -> RepoResult<Option<Value>> {
        let version = match selector {
            VersionSelector::Latest => -1,
            VersionSelector::Exact(v) => v,
        };

        let rows = self.query(
            "MATCH (k:UnitKernel {tenantid: $tenantid, unitid: $unitid}) WITH k, CASE WHEN $version = -1 THEN k.lastver ELSE $version END AS wanted MATCH (v:UnitVersion {tenantid: k.tenantid, unitid: k.unitid, unitver: wanted}) RETURN k.tenantid, k.unitid, v.unitver, k.lastver, k.corrid, k.status, k.created, v.modified, v.unitname, v.payload",
            json!({"tenantid": tenant_id, "unitid": unit_id, "version": version}),
        )?;

        if rows.is_empty() {
            return Ok(None);
        }

        let row = &rows[0];
        if row.len() < 10 {
            return Err(RepoError::Backend(format!(
                "invalid neo4j get_unit_json row: {:?}",
                row
            )));
        }

        let tenant = extract_i64(row.first()).unwrap_or(tenant_id);
        let unit = extract_i64(row.get(1)).unwrap_or(unit_id);
        let unit_ver = extract_i64(row.get(2)).unwrap_or(1);
        let last_ver = extract_i64(row.get(3)).unwrap_or(unit_ver);
        let corrid = row.get(4).and_then(Value::as_str).unwrap_or_default();
        let status = extract_i64(row.get(5)).unwrap_or(30);
        let created = extract_i64(row.get(6)).unwrap_or_default();
        let modified = extract_i64(row.get(7)).unwrap_or(created);
        let unitname = row.get(8).and_then(Value::as_str).map(ToString::to_string);

        let mut payload_obj = payload_to_object(row.get(9))?;
        payload_obj.insert("tenantid".to_string(), Value::Number(tenant.into()));
        payload_obj.insert("unitid".to_string(), Value::Number(unit.into()));
        payload_obj.insert("unitver".to_string(), Value::Number(unit_ver.into()));
        payload_obj.insert("corrid".to_string(), Value::String(corrid.to_string()));
        payload_obj.insert("status".to_string(), Value::Number(status.into()));
        payload_obj.insert(
            "created".to_string(),
            Value::String(millis_to_iso8601(created)),
        );
        payload_obj.insert(
            "modified".to_string(),
            Value::String(millis_to_iso8601(modified)),
        );
        payload_obj.insert("isreadonly".to_string(), Value::Bool(last_ver > unit_ver));
        if let Some(name) = unitname {
            payload_obj.insert("unitname".to_string(), Value::String(name));
        }

        Ok(Some(Value::Object(payload_obj)))
    }

    fn get_unit_by_corrid_json(&self, tenant_id: i64, corrid: &str) -> RepoResult<Option<Value>> {
        let rows = self.query(
            "MATCH (k:UnitKernel {tenantid: $tenantid, corrid: $corrid}) MATCH (v:UnitVersion {tenantid: k.tenantid, unitid: k.unitid, unitver: k.lastver}) RETURN k.tenantid, k.unitid, v.unitver, k.lastver, k.corrid, k.status, k.created, v.modified, v.unitname, v.payload",
            json!({"tenantid": tenant_id, "corrid": corrid}),
        )?;

        if rows.is_empty() {
            return Ok(None);
        }

        let row = &rows[0];
        if row.len() < 10 {
            return Err(RepoError::Backend(format!(
                "invalid neo4j get_unit_by_corrid_json row: {:?}",
                row
            )));
        }

        let tenant = extract_i64(row.first()).unwrap_or(tenant_id);
        let unit = extract_i64(row.get(1)).unwrap_or_default();
        let unit_ver = extract_i64(row.get(2)).unwrap_or(1);
        let last_ver = extract_i64(row.get(3)).unwrap_or(unit_ver);
        let corrid = row.get(4).and_then(Value::as_str).unwrap_or_default();
        let status = extract_i64(row.get(5)).unwrap_or(30);
        let created = extract_i64(row.get(6)).unwrap_or_default();
        let modified = extract_i64(row.get(7)).unwrap_or(created);
        let unitname = row.get(8).and_then(Value::as_str).map(ToString::to_string);

        let mut payload_obj = payload_to_object(row.get(9))?;
        payload_obj.insert("tenantid".to_string(), Value::Number(tenant.into()));
        payload_obj.insert("unitid".to_string(), Value::Number(unit.into()));
        payload_obj.insert("unitver".to_string(), Value::Number(unit_ver.into()));
        payload_obj.insert("corrid".to_string(), Value::String(corrid.to_string()));
        payload_obj.insert("status".to_string(), Value::Number(status.into()));
        payload_obj.insert(
            "created".to_string(),
            Value::String(millis_to_iso8601(created)),
        );
        payload_obj.insert(
            "modified".to_string(),
            Value::String(millis_to_iso8601(modified)),
        );
        payload_obj.insert("isreadonly".to_string(), Value::Bool(last_ver > unit_ver));
        if let Some(name) = unitname {
            payload_obj.insert("unitname".to_string(), Value::String(name));
        }

        Ok(Some(Value::Object(payload_obj)))
    }

    fn unit_exists(&self, tenant_id: i64, unit_id: i64) -> RepoResult<bool> {
        let rows = self.query(
            "MATCH (k:UnitKernel {tenantid: $tenantid, unitid: $unitid}) RETURN count(k) > 0",
            json!({"tenantid": tenant_id, "unitid": unit_id}),
        )?;
        Ok(rows
            .first()
            .and_then(|r| r.first())
            .and_then(Value::as_bool)
            .unwrap_or(false))
    }

    fn store_unit_json(&self, unit: Value) -> RepoResult<Value> {
        let mut obj = as_object(unit)?;
        let tenant_id = extract_obj_i64(&obj, "tenantid")?
            .ok_or_else(|| RepoError::InvalidInput("tenantid is required".to_string()))?;
        let (attr_ids, attr_names) = collect_attribute_refs(&obj);
        let attr_entries = collect_attribute_entries(&obj);

        let status = extract_obj_i64(&obj, "status")?.unwrap_or(30);
        let unit_name = obj
            .get("unitname")
            .and_then(Value::as_str)
            .map(ToString::to_string);

        if let Some(unit_id) = extract_obj_i64(&obj, "unitid")? {
            let modified = now_millis();
            let modified_iso = millis_to_iso8601(modified);
            obj.insert("status".to_string(), Value::Number(status.into()));
            obj.insert("modified".to_string(), Value::String(modified_iso));
            let payload = Value::Object(obj.clone()).to_string();

            let rows = self.query(
                "MATCH (k:UnitKernel {tenantid: $tenantid, unitid: $unitid}) SET k.lastver = coalesce(k.lastver, 0) + 1, k.status = $status WITH k, k.lastver AS nextver CREATE (v:UnitVersion {tenantid: k.tenantid, unitid: k.unitid, unitver: nextver, unitname: $unitname, modified: $modified, payload: $payload}) CREATE (k)-[:HAS_VERSION {unitver: nextver}]->(v) RETURN nextver, v.modified",
                json!({
                    "tenantid": tenant_id,
                    "unitid": unit_id,
                    "status": status,
                    "unitname": unit_name,
                    "modified": modified,
                    "payload": payload
                }),
            )?;

            let row = rows.first().ok_or(RepoError::NotFound)?;
            let unit_ver = extract_i64(row.first()).unwrap_or(1);
            let modified_out = extract_i64(row.get(1)).unwrap_or(modified);
            self.link_unit_version_attributes(
                tenant_id,
                unit_id,
                unit_ver,
                &attr_ids,
                &attr_names,
            )?;
            self.link_unit_version_attribute_values(tenant_id, unit_id, unit_ver, &attr_entries)?;
            obj.insert("unitver".to_string(), Value::Number(unit_ver.into()));
            obj.insert(
                "modified".to_string(),
                Value::String(millis_to_iso8601(modified_out)),
            );
            obj.insert("isreadonly".to_string(), Value::Bool(false));
            Ok(Value::Object(obj))
        } else {
            let unit_id = self.next_counter("unitid")?;
            let corrid = obj
                .get("corrid")
                .and_then(Value::as_str)
                .map(ToString::to_string)
                .unwrap_or_else(|| Uuid::now_v7().to_string());
            let now = now_millis();

            obj.insert("unitid".to_string(), Value::Number(unit_id.into()));
            obj.insert("unitver".to_string(), Value::Number(1.into()));
            obj.insert("corrid".to_string(), Value::String(corrid.clone()));
            obj.insert("status".to_string(), Value::Number(status.into()));
            obj.insert("created".to_string(), Value::String(millis_to_iso8601(now)));
            obj.insert(
                "modified".to_string(),
                Value::String(millis_to_iso8601(now)),
            );
            obj.insert("isreadonly".to_string(), Value::Bool(false));
            let payload = Value::Object(obj.clone()).to_string();

            let rows = self.query(
                "CREATE (k:UnitKernel {tenantid: $tenantid, unitid: $unitid, corrid: $corrid, status: $status, created: $created, lastver: 1}) CREATE (v:UnitVersion {tenantid: $tenantid, unitid: $unitid, unitver: 1, unitname: $unitname, modified: $modified, payload: $payload}) CREATE (k)-[:HAS_VERSION {unitver: 1}]->(v) RETURN k.unitid, 1, k.created, v.modified",
                json!({
                    "tenantid": tenant_id,
                    "unitid": unit_id,
                    "corrid": corrid,
                    "status": status,
                    "created": now,
                    "unitname": unit_name,
                    "modified": now,
                    "payload": payload
                }),
            )?;

            let row = rows.first().ok_or_else(|| {
                RepoError::Backend("neo4j create unit returned no rows".to_string())
            })?;
            let created = extract_i64(row.get(2)).unwrap_or(now);
            let modified = extract_i64(row.get(3)).unwrap_or(now);
            self.link_unit_version_attributes(tenant_id, unit_id, 1, &attr_ids, &attr_names)?;
            self.link_unit_version_attribute_values(tenant_id, unit_id, 1, &attr_entries)?;
            obj.insert(
                "created".to_string(),
                Value::String(millis_to_iso8601(created)),
            );
            obj.insert(
                "modified".to_string(),
                Value::String(millis_to_iso8601(modified)),
            );
            Ok(Value::Object(obj))
        }
    }

    fn search_units(
        &self,
        expression: Value,
        order: SearchOrder,
        paging: SearchPaging,
    ) -> RepoResult<SearchResult> {
        let search_expr = parse_search_expression(&expression)?;
        let mut params = CypherParams::new();
        let compiled_where = compile_search_expression(&search_expr, &mut params)?;
        let where_predicate = compiled_where.unwrap_or_else(|| "1 = 1".to_string());
        let (order_field, order_dir) = order_sql(&order);
        let limit = if paging.limit > 0 { paging.limit } else { 100 };
        let offset = if paging.offset >= 0 { paging.offset } else { 0 };
        let limit_ph = params.push(Value::Number(limit.into()));
        let offset_ph = params.push(Value::Number(offset.into()));
        let params_json = params.into_value();

        let count_cypher = format!(
            "MATCH (k:UnitKernel) MATCH (k)-[:HAS_VERSION]->(v:UnitVersion) WHERE v.unitver = k.lastver AND ({}) RETURN count(*)",
            where_predicate
        );
        let count_rows = self.query(&count_cypher, params_json.clone())?;
        let total_hits = extract_i64(count_rows.first().and_then(|r| r.first())).unwrap_or(0);

        let data_cypher = format!(
            "MATCH (k:UnitKernel) MATCH (k)-[:HAS_VERSION]->(v:UnitVersion) WHERE v.unitver = k.lastver AND ({}) RETURN k.tenantid, k.unitid, v.unitver, k.lastver, k.corrid, k.status, k.created, v.modified, v.unitname, v.payload ORDER BY {} {} SKIP {} LIMIT {}",
            where_predicate, order_field, order_dir, offset_ph, limit_ph
        );
        let data_rows = self.query(&data_cypher, params_json)?;

        let results = data_rows
            .iter()
            .map(|row| row_to_unit(row.as_slice()))
            .collect::<RepoResult<Vec<Unit>>>()?;

        Ok(SearchResult {
            total_hits,
            results,
        })
    }

    fn add_relation(&self, left: UnitRef, relation_type: i32, right: UnitRef) -> RepoResult<()> {
        let rows = self.query(
            "MATCH (a:UnitKernel {tenantid: $tenantid, unitid: $unitid}) MATCH (b:UnitKernel {tenantid: $reltenantid, unitid: $relunitid}) MERGE (a)-[:RELATED_TO {reltype: $reltype, reltenantid: $reltenantid, relunitid: $relunitid}]->(b) RETURN 1",
            json!({
                "tenantid": left.tenant_id,
                "unitid": left.unit_id,
                "reltype": relation_type,
                "reltenantid": right.tenant_id,
                "relunitid": right.unit_id
            }),
        )?;
        if rows.is_empty() {
            Err(RepoError::NotFound)
        } else {
            Ok(())
        }
    }

    fn remove_relation(&self, left: UnitRef, relation_type: i32, right: UnitRef) -> RepoResult<()> {
        self.query(
            "MATCH (:UnitKernel {tenantid: $tenantid, unitid: $unitid})-[r:RELATED_TO {reltype: $reltype, reltenantid: $reltenantid, relunitid: $relunitid}]->(:UnitKernel {tenantid: $reltenantid, unitid: $relunitid}) DELETE r RETURN count(r)",
            json!({
                "tenantid": left.tenant_id,
                "unitid": left.unit_id,
                "reltype": relation_type,
                "reltenantid": right.tenant_id,
                "relunitid": right.unit_id
            }),
        )?;
        Ok(())
    }

    fn get_right_relation(
        &self,
        unit: UnitRef,
        relation_type: i32,
    ) -> RepoResult<Option<Relation>> {
        let rows = self.query(
            "MATCH (a:UnitKernel {tenantid: $tenantid, unitid: $unitid})-[r:RELATED_TO {reltype: $reltype}]->(b:UnitKernel) RETURN {tenantid: a.tenantid, unitid: a.unitid, reltype: r.reltype, reltenantid: r.reltenantid, relunitid: r.relunitid} LIMIT 1",
            json!({"tenantid": unit.tenant_id, "unitid": unit.unit_id, "reltype": relation_type}),
        )?;
        rows.first()
            .and_then(|r| r.first())
            .map(relation_from_value)
            .transpose()
    }

    fn get_right_relations(&self, unit: UnitRef, relation_type: i32) -> RepoResult<Vec<Relation>> {
        let rows = self.query(
            "MATCH (a:UnitKernel {tenantid: $tenantid, unitid: $unitid})-[r:RELATED_TO {reltype: $reltype}]->(b:UnitKernel) RETURN {tenantid: a.tenantid, unitid: a.unitid, reltype: r.reltype, reltenantid: r.reltenantid, relunitid: r.relunitid}",
            json!({"tenantid": unit.tenant_id, "unitid": unit.unit_id, "reltype": relation_type}),
        )?;
        rows.into_iter()
            .filter_map(|r| r.first().cloned())
            .map(|v| relation_from_value(&v))
            .collect()
    }

    fn get_left_relations(&self, unit: UnitRef, relation_type: i32) -> RepoResult<Vec<Relation>> {
        let rows = self.query(
            "MATCH (a:UnitKernel)-[r:RELATED_TO {reltype: $reltype}]->(b:UnitKernel {tenantid: $reltenantid, unitid: $relunitid}) RETURN {tenantid: a.tenantid, unitid: a.unitid, reltype: r.reltype, reltenantid: r.reltenantid, relunitid: r.relunitid}",
            json!({"reltenantid": unit.tenant_id, "relunitid": unit.unit_id, "reltype": relation_type}),
        )?;
        rows.into_iter()
            .filter_map(|r| r.first().cloned())
            .map(|v| relation_from_value(&v))
            .collect()
    }

    fn count_right_relations(&self, unit: UnitRef, relation_type: i32) -> RepoResult<i64> {
        let rows = self.query(
            "MATCH (:UnitKernel {tenantid: $tenantid, unitid: $unitid})-[r:RELATED_TO {reltype: $reltype}]->(:UnitKernel) RETURN count(r)",
            json!({"tenantid": unit.tenant_id, "unitid": unit.unit_id, "reltype": relation_type}),
        )?;
        Ok(extract_i64(rows.first().and_then(|r| r.first())).unwrap_or(0))
    }

    fn count_left_relations(&self, unit: UnitRef, relation_type: i32) -> RepoResult<i64> {
        let rows = self.query(
            "MATCH (:UnitKernel)-[r:RELATED_TO {reltype: $reltype}]->(:UnitKernel {tenantid: $reltenantid, unitid: $relunitid}) RETURN count(r)",
            json!({"reltenantid": unit.tenant_id, "relunitid": unit.unit_id, "reltype": relation_type}),
        )?;
        Ok(extract_i64(rows.first().and_then(|r| r.first())).unwrap_or(0))
    }

    fn add_association(
        &self,
        unit: UnitRef,
        association_type: i32,
        reference: &str,
    ) -> RepoResult<()> {
        let rows = self.query(
            "MATCH (k:UnitKernel {tenantid: $tenantid, unitid: $unitid}) MERGE (a:AssociationRef {assoctype: $assoctype, refstring: $refstring}) MERGE (k)-[:ASSOCIATED_WITH {assoctype: $assoctype, refstring: $refstring}]->(a) RETURN 1",
            json!({"tenantid": unit.tenant_id, "unitid": unit.unit_id, "assoctype": association_type, "refstring": reference}),
        )?;
        if rows.is_empty() {
            Err(RepoError::NotFound)
        } else {
            Ok(())
        }
    }

    fn remove_association(
        &self,
        unit: UnitRef,
        association_type: i32,
        reference: &str,
    ) -> RepoResult<()> {
        self.query(
            "MATCH (:UnitKernel {tenantid: $tenantid, unitid: $unitid})-[r:ASSOCIATED_WITH {assoctype: $assoctype, refstring: $refstring}]->(:AssociationRef {assoctype: $assoctype, refstring: $refstring}) DELETE r RETURN count(r)",
            json!({"tenantid": unit.tenant_id, "unitid": unit.unit_id, "assoctype": association_type, "refstring": reference}),
        )?;
        Ok(())
    }

    fn get_right_association(
        &self,
        unit: UnitRef,
        association_type: i32,
    ) -> RepoResult<Option<Association>> {
        let rows = self.query(
            "MATCH (k:UnitKernel {tenantid: $tenantid, unitid: $unitid})-[r:ASSOCIATED_WITH {assoctype: $assoctype}]->(a:AssociationRef) RETURN {tenantid: k.tenantid, unitid: k.unitid, assoctype: r.assoctype, assocstring: r.refstring} LIMIT 1",
            json!({"tenantid": unit.tenant_id, "unitid": unit.unit_id, "assoctype": association_type}),
        )?;
        rows.first()
            .and_then(|r| r.first())
            .map(association_from_value)
            .transpose()
    }

    fn get_right_associations(
        &self,
        unit: UnitRef,
        association_type: i32,
    ) -> RepoResult<Vec<Association>> {
        let rows = self.query(
            "MATCH (k:UnitKernel {tenantid: $tenantid, unitid: $unitid})-[r:ASSOCIATED_WITH {assoctype: $assoctype}]->(a:AssociationRef) RETURN {tenantid: k.tenantid, unitid: k.unitid, assoctype: r.assoctype, assocstring: r.refstring}",
            json!({"tenantid": unit.tenant_id, "unitid": unit.unit_id, "assoctype": association_type}),
        )?;
        rows.into_iter()
            .filter_map(|r| r.first().cloned())
            .map(|v| association_from_value(&v))
            .collect()
    }

    fn get_left_associations(
        &self,
        association_type: i32,
        reference: &str,
    ) -> RepoResult<Vec<Association>> {
        let rows = self.query(
            "MATCH (k:UnitKernel)-[r:ASSOCIATED_WITH {assoctype: $assoctype, refstring: $refstring}]->(:AssociationRef {assoctype: $assoctype, refstring: $refstring}) RETURN {tenantid: k.tenantid, unitid: k.unitid, assoctype: r.assoctype, assocstring: r.refstring}",
            json!({"assoctype": association_type, "refstring": reference}),
        )?;
        rows.into_iter()
            .filter_map(|r| r.first().cloned())
            .map(|v| association_from_value(&v))
            .collect()
    }

    fn count_right_associations(&self, unit: UnitRef, association_type: i32) -> RepoResult<i64> {
        let rows = self.query(
            "MATCH (:UnitKernel {tenantid: $tenantid, unitid: $unitid})-[r:ASSOCIATED_WITH {assoctype: $assoctype}]->(:AssociationRef) RETURN count(r)",
            json!({"tenantid": unit.tenant_id, "unitid": unit.unit_id, "assoctype": association_type}),
        )?;
        Ok(extract_i64(rows.first().and_then(|r| r.first())).unwrap_or(0))
    }

    fn count_left_associations(&self, association_type: i32, reference: &str) -> RepoResult<i64> {
        let rows = self.query(
            "MATCH (:UnitKernel)-[r:ASSOCIATED_WITH {assoctype: $assoctype, refstring: $refstring}]->(:AssociationRef {assoctype: $assoctype, refstring: $refstring}) RETURN count(r)",
            json!({"assoctype": association_type, "refstring": reference}),
        )?;
        Ok(extract_i64(rows.first().and_then(|r| r.first())).unwrap_or(0))
    }

    fn lock_unit(&self, unit: UnitRef, lock_type: i32, purpose: &str) -> RepoResult<()> {
        let check_rows = self.query(
            "MATCH (k:UnitKernel {tenantid: $tenantid, unitid: $unitid}) OPTIONAL MATCH (k)-[:HAS_LOCK]->(l:UnitLock) RETURN count(l) > 0",
            json!({"tenantid": unit.tenant_id, "unitid": unit.unit_id}),
        )?;

        match check_rows
            .first()
            .and_then(|r| r.first())
            .and_then(Value::as_bool)
        {
            Some(true) => return Err(RepoError::AlreadyLocked),
            Some(false) => {}
            None => return Err(RepoError::NotFound),
        }

        let rows = self.query(
            "MATCH (k:UnitKernel {tenantid: $tenantid, unitid: $unitid}) CREATE (l:UnitLock {locktype: $locktype, purpose: $purpose, locked_at: timestamp()}) CREATE (k)-[:HAS_LOCK]->(l) RETURN 1",
            json!({"tenantid": unit.tenant_id, "unitid": unit.unit_id, "locktype": lock_type, "purpose": purpose}),
        )?;

        if rows.is_empty() {
            Err(RepoError::NotFound)
        } else {
            Ok(())
        }
    }

    fn unlock_unit(&self, unit: UnitRef) -> RepoResult<()> {
        let rows = self.query(
            "MATCH (k:UnitKernel {tenantid: $tenantid, unitid: $unitid}) OPTIONAL MATCH (k)-[r:HAS_LOCK]->(l:UnitLock) DELETE r, l RETURN 1",
            json!({"tenantid": unit.tenant_id, "unitid": unit.unit_id}),
        )?;
        if rows.is_empty() {
            Err(RepoError::NotFound)
        } else {
            Ok(())
        }
    }

    fn is_unit_locked(&self, unit: UnitRef) -> RepoResult<bool> {
        let rows = self.query(
            "MATCH (k:UnitKernel {tenantid: $tenantid, unitid: $unitid}) OPTIONAL MATCH (k)-[:HAS_LOCK]->(l:UnitLock) RETURN count(l) > 0",
            json!({"tenantid": unit.tenant_id, "unitid": unit.unit_id}),
        )?;
        Ok(rows
            .first()
            .and_then(|r| r.first())
            .and_then(Value::as_bool)
            .unwrap_or(false))
    }

    fn set_status(&self, unit: UnitRef, status: i32) -> RepoResult<()> {
        let rows = self.query(
            "MATCH (k:UnitKernel {tenantid: $tenantid, unitid: $unitid}) SET k.status = $status RETURN k.status",
            json!({"tenantid": unit.tenant_id, "unitid": unit.unit_id, "status": status}),
        )?;
        if rows.is_empty() {
            Err(RepoError::NotFound)
        } else {
            Ok(())
        }
    }

    fn create_attribute(
        &self,
        alias: &str,
        name: &str,
        qualname: &str,
        attribute_type: &str,
        is_array: bool,
    ) -> RepoResult<Value> {
        if let Some(existing) = self.get_attribute_info(name)? {
            return Ok(existing);
        }

        let id = self.next_counter("attrid")?;
        let attr_type = parse_attr_type(attribute_type)?;
        let forced_scalar = !is_array;
        let alias_value = if alias.is_empty() {
            Value::Null
        } else {
            Value::String(alias.to_string())
        };

        let rows = self.query(
            "CREATE (a:Attribute {id: $id, alias: $alias, name: $name, qualname: $qualname, type: $type, forced_scalar: $forced_scalar}) RETURN a.id, a.alias, a.name, a.qualname, a.type, a.forced_scalar",
            json!({
                "id": id,
                "alias": alias_value,
                "name": name,
                "qualname": qualname,
                "type": attr_type,
                "forced_scalar": forced_scalar
            }),
        )?;

        let row = rows.first().ok_or_else(|| {
            RepoError::Backend("neo4j create_attribute returned no rows".to_string())
        })?;
        attribute_info_from_row(row.as_slice())
    }

    fn instantiate_attribute(&self, name_or_id: &str) -> RepoResult<Option<Value>> {
        self.get_attribute_info(name_or_id)
    }

    fn can_change_attribute(&self, name_or_id: &str) -> RepoResult<bool> {
        let Some(attr) = self.get_attribute_info(name_or_id)? else {
            return Ok(true);
        };
        let attr_id = attr
            .get("id")
            .and_then(Value::as_i64)
            .ok_or_else(|| RepoError::Backend("attribute info missing id".to_string()))?;

        let rows = self.query(
            "MATCH (v:UnitVersion)-[:USES_ATTRIBUTE]->(a:Attribute {id: $id}) RETURN count(v) > 0",
            json!({"id": attr_id}),
        )?;
        let in_use = rows
            .first()
            .and_then(|r| r.first())
            .and_then(Value::as_bool)
            .unwrap_or(false);
        Ok(!in_use)
    }

    fn get_attribute_info(&self, name_or_id: &str) -> RepoResult<Option<Value>> {
        if let Ok(id) = name_or_id.parse::<i64>() {
            let rows = self.query(
                "MATCH (a:Attribute {id: $id}) RETURN a.id, a.alias, a.name, a.qualname, a.type, a.forced_scalar LIMIT 1",
                json!({"id": id}),
            )?;
            return rows
                .first()
                .map(|row| attribute_info_from_row(row.as_slice()))
                .transpose();
        }

        let rows = self.query(
            "MATCH (a:Attribute) WHERE a.name = $q OR a.alias = $q OR a.qualname = $q RETURN a.id, a.alias, a.name, a.qualname, a.type, a.forced_scalar LIMIT 1",
            json!({"q": name_or_id}),
        )?;
        rows.first()
            .map(|row| attribute_info_from_row(row.as_slice()))
            .transpose()
    }

    fn get_tenant_info(&self, name_or_id: &str) -> RepoResult<Option<Value>> {
        if let Ok(id) = name_or_id.parse::<i64>() {
            let rows = self.query(
                "MERGE (t:Tenant {id: $id}) ON CREATE SET t.name = $name, t.description = NULL, t.created = timestamp() RETURN t.id, t.name, t.description, t.created",
                json!({"id": id, "name": format!("tenant-{id}")}),
            )?;
            return rows
                .first()
                .map(|row| tenant_info_from_row(row.as_slice()))
                .transpose();
        }

        let rows = self.query(
            "MATCH (t:Tenant {name: $name}) RETURN t.id, t.name, t.description, t.created LIMIT 1",
            json!({"name": name_or_id}),
        )?;

        if let Some(row) = rows.first() {
            return tenant_info_from_row(row.as_slice()).map(Some);
        }

        let id = self.next_counter("tenantid")?;
        let rows = self.query(
            "CREATE (t:Tenant {id: $id, name: $name, description: NULL, created: timestamp()}) RETURN t.id, t.name, t.description, t.created",
            json!({"id": id, "name": name_or_id}),
        )?;
        rows.first()
            .map(|row| tenant_info_from_row(row.as_slice()))
            .transpose()
    }

    fn health(&self) -> RepoResult<Value> {
        let rows = self.query("RETURN 1", json!({}))?;
        let ok = rows.first().and_then(|r| r.first()).and_then(Value::as_i64) == Some(1);

        Ok(json!({
            "status": if ok { "ok" } else { "degraded" },
            "backend": "neo4j",
            "url": self.url,
            "database": self.database,
            "user": self.user,
            "implemented": [
                "unit_exists", "store_unit_json", "get_unit_json", "search_units", "set_status",
                "add_relation", "remove_relation", "get_right_relation", "get_right_relations", "get_left_relations", "count_right_relations", "count_left_relations",
                "add_association", "remove_association", "get_right_association", "get_right_associations", "get_left_associations", "count_right_associations", "count_left_associations",
                "lock_unit", "unlock_unit", "is_unit_locked",
                "create_attribute", "instantiate_attribute", "can_change_attribute",
                "get_attribute_info", "get_tenant_info"
            ]
        }))
    }
}

fn parse_attr_type(attribute_type: &str) -> RepoResult<i64> {
    if let Ok(value) = attribute_type.parse::<i64>() {
        return Ok(value);
    }

    let normalized = attribute_type.to_ascii_lowercase();
    let value = match normalized.as_str() {
        "string" => 1,
        "time" | "instant" | "timestamp" => 2,
        "int" | "integer" => 3,
        "long" => 4,
        "double" | "float" => 5,
        "bool" | "boolean" => 6,
        "data" | "bytes" | "blob" => 7,
        "record" => 99,
        _ => {
            return Err(RepoError::InvalidInput(format!(
                "unknown attribute type '{attribute_type}'"
            )));
        }
    };

    Ok(value)
}

fn env_bool(key: &str, default: bool) -> bool {
    match std::env::var(key) {
        Ok(v) => matches!(v.to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"),
        Err(_) => default,
    }
}

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

fn as_object(value: Value) -> RepoResult<Map<String, Value>> {
    match value {
        Value::Object(obj) => Ok(obj),
        _ => Err(RepoError::InvalidInput(
            "unit payload must be a JSON object".to_string(),
        )),
    }
}

fn extract_i64(value: Option<&Value>) -> Option<i64> {
    match value {
        Some(Value::Number(n)) => n.as_i64().or_else(|| n.as_f64().map(|f| f.trunc() as i64)),
        _ => None,
    }
}

fn extract_obj_i64(obj: &Map<String, Value>, key: &str) -> RepoResult<Option<i64>> {
    match obj.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(_)) => Ok(extract_i64(obj.get(key))),
        Some(Value::String(s)) => Ok(s.parse::<i64>().ok()),
        Some(other) => Err(RepoError::InvalidInput(format!(
            "{key} must be numeric, got {other}"
        ))),
    }
}

fn extract_map_i64(expr: &Map<String, Value>, key: &str) -> RepoResult<Option<i64>> {
    match expr.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(_)) => Ok(extract_i64(expr.get(key))),
        Some(Value::String(s)) => Ok(s.parse::<i64>().ok()),
        Some(other) => Err(RepoError::InvalidInput(format!(
            "{key} must be numeric/string, got {other}"
        ))),
    }
}

fn extract_map_status_i64(expr: &Map<String, Value>, key: &str) -> RepoResult<Option<i64>> {
    match expr.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(raw)) => parse_status_name(raw, key).map(Some),
        Some(_) => extract_map_i64(expr, key),
    }
}

fn parse_status_name(raw: &str, key: &str) -> RepoResult<i64> {
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
            "{key} has unknown status name '{raw}'"
        ))),
    }
}

fn extract_map_timestamp_millis(expr: &Map<String, Value>, key: &str) -> RepoResult<Option<i64>> {
    match expr.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(value) => parse_time_millis(value, key).map(Some),
    }
}

fn extract_map_string(expr: &Map<String, Value>, key: &str) -> Option<String> {
    expr.get(key)
        .and_then(Value::as_str)
        .map(ToString::to_string)
}

struct CypherParams {
    values: Map<String, Value>,
    index: usize,
}

impl CypherParams {
    fn new() -> Self {
        Self {
            values: Map::new(),
            index: 0,
        }
    }

    fn push(&mut self, value: Value) -> String {
        self.index += 1;
        let key = format!("p{}", self.index);
        self.values.insert(key.clone(), value);
        format!("${key}")
    }

    fn into_value(self) -> Value {
        Value::Object(self.values)
    }
}

type SearchExpr = BoolExpr<LeafConstraint>;

#[derive(Clone, Default)]
struct LeafConstraint {
    tenant_id: Option<i64>,
    unit_id: Option<i64>,
    unit_ver: Option<i64>,
    status: Option<i64>,
    corrid: Option<String>,
    relation: RelationConstraint,
    association: AssociationConstraint,
    attribute_cmp: AttributeCmpConstraint,
    name: Option<String>,
    name_regex: Option<String>,
    created_after: Option<i64>,
    created_before: Option<i64>,
    modified_after: Option<i64>,
    modified_before: Option<i64>,
    predicates: Vec<UnitPredicate>,
}

impl LeafConstraint {
    fn is_empty(&self) -> bool {
        self.tenant_id.is_none()
            && self.unit_id.is_none()
            && self.unit_ver.is_none()
            && self.status.is_none()
            && self.corrid.is_none()
            && self.relation.relation_type.is_none()
            && self.relation.related_tenantid.is_none()
            && self.relation.related_unitid.is_none()
            && self.association.association_type.is_none()
            && self.association.association_reference.is_none()
            && self.attribute_cmp.op.is_none()
            && self.name.is_none()
            && self.name_regex.is_none()
            && self.created_after.is_none()
            && self.created_before.is_none()
            && self.modified_after.is_none()
            && self.modified_before.is_none()
            && self.predicates.is_empty()
    }
}

fn parse_search_expression(expression: &Value) -> RepoResult<SearchExpr> {
    parse_bool_expression(
        expression,
        parse_leaf_constraint,
        infer_tenant_id,
        |tenant_id| {
            Ok(LeafConstraint {
                tenant_id: Some(tenant_id),
                ..LeafConstraint::default()
            })
        },
        "neo4j native not-expression search requires tenant scope",
    )
}

fn parse_leaf_constraint(expr: &Map<String, Value>) -> RepoResult<Option<LeafConstraint>> {
    let mut name = extract_map_string(expr, "name")
        .or_else(|| extract_map_string(expr, "unitname"))
        .or_else(|| extract_map_string(expr, "unit_name"));
    let mut name_regex = extract_map_string(expr, "name_ilike").map(|s| like_to_regex(&s));
    if name_regex.is_none() {
        if let Some(raw_name) = name.as_ref() {
            if has_name_wildcard(raw_name) {
                // Normalize wildcard name filters to regex path for Neo4j.
                name_regex = Some(like_to_regex(&raw_name.replace('*', "%")));
                name = None;
            }
        }
    }
    let predicates = extract_unit_predicates(expr, false)?;
    let unit_ver = if expr.contains_key("unitver") {
        extract_map_i64(expr, "unitver")?
    } else {
        extract_map_i64(expr, "unit_ver")?
    };
    let tenant_id = if expr.contains_key("tenantid") {
        extract_map_i64(expr, "tenantid")?
    } else {
        extract_map_i64(expr, "tenant_id")?
    };
    let unit_id = if expr.contains_key("unitid") {
        extract_map_i64(expr, "unitid")?
    } else {
        extract_map_i64(expr, "unit_id")?
    };
    let leaf = LeafConstraint {
        tenant_id,
        unit_id,
        unit_ver,
        status: extract_map_status_i64(expr, "status")?,
        corrid: extract_map_string(expr, "corrid")
            .or_else(|| extract_map_string(expr, "corr_id"))
            .or_else(|| extract_map_string(expr, "correlationid")),
        relation: extract_relation_constraint(expr)?,
        association: extract_association_constraint(expr)?,
        attribute_cmp: extract_attribute_constraint(expr)?,
        name,
        name_regex,
        created_after: extract_map_timestamp_millis(expr, "created_after")?,
        created_before: extract_map_timestamp_millis(expr, "created_before")?,
        modified_after: extract_map_timestamp_millis(expr, "modified_after")?,
        modified_before: extract_map_timestamp_millis(expr, "modified_before")?,
        predicates,
    };
    if leaf.is_empty() {
        Ok(None)
    } else {
        Ok(Some(leaf))
    }
}

fn compile_search_expression(
    expr: &SearchExpr,
    params: &mut CypherParams,
) -> RepoResult<Option<String>> {
    match expr {
        SearchExpr::True => Ok(None),
        SearchExpr::False => Ok(Some("false".to_string())),
        SearchExpr::Leaf(leaf) => compile_leaf_constraint(leaf.as_ref(), params).map(Some),
        SearchExpr::And(children) => {
            let mut parts = Vec::new();
            for child in children {
                if let Some(sql) = compile_search_expression(child, params)? {
                    parts.push(format!("({sql})"));
                }
            }
            if parts.is_empty() {
                Ok(None)
            } else {
                Ok(Some(parts.join(" AND ")))
            }
        }
        SearchExpr::Or(children) => {
            let mut parts = Vec::new();
            for child in children {
                match compile_search_expression(child, params)? {
                    Some(sql) => parts.push(format!("({sql})")),
                    None => return Ok(None),
                }
            }
            if parts.is_empty() {
                Ok(Some("false".to_string()))
            } else {
                Ok(Some(parts.join(" OR ")))
            }
        }
        SearchExpr::Not(child) => match compile_search_expression(child, params)? {
            Some(sql) => Ok(Some(format!("NOT ({sql})"))),
            None => Ok(Some("false".to_string())),
        },
    }
}

fn compile_leaf_constraint(leaf: &LeafConstraint, params: &mut CypherParams) -> RepoResult<String> {
    let mut clauses = Vec::new();

    if let Some(tenant_id) = leaf.tenant_id {
        let ph = params.push(Value::Number(tenant_id.into()));
        clauses.push(format!("k.tenantid = {ph}"));
    }
    if let Some(unit_id) = leaf.unit_id {
        let ph = params.push(Value::Number(unit_id.into()));
        clauses.push(format!("k.unitid = {ph}"));
    }
    if let Some(unit_ver) = leaf.unit_ver {
        let ph = params.push(Value::Number(unit_ver.into()));
        clauses.push(format!("v.unitver = {ph}"));
    }
    if let Some(status) = leaf.status {
        let ph = params.push(Value::Number(status.into()));
        clauses.push(format!("k.status = {ph}"));
    }
    if let Some(corrid) = leaf.corrid.as_ref() {
        let ph = params.push(Value::String(corrid.clone()));
        clauses.push(format!("k.corrid = {ph}"));
    }
    if let Some(name) = leaf.name.as_ref() {
        let ph = params.push(Value::String(name.clone()));
        clauses.push(format!("v.unitname = {ph}"));
    }
    if let Some(name_regex) = leaf.name_regex.as_ref() {
        let ph = params.push(Value::String(name_regex.clone()));
        clauses.push(format!("(v.unitname IS NOT NULL AND v.unitname =~ {ph})"));
    }
    if let Some(created_after) = leaf.created_after {
        let ph = params.push(Value::Number(created_after.into()));
        clauses.push(format!("k.created >= {ph}"));
    }
    if let Some(created_before) = leaf.created_before {
        let ph = params.push(Value::Number(created_before.into()));
        clauses.push(format!("k.created < {ph}"));
    }
    if let Some(modified_after) = leaf.modified_after {
        let ph = params.push(Value::Number(modified_after.into()));
        clauses.push(format!("v.modified >= {ph}"));
    }
    if let Some(modified_before) = leaf.modified_before {
        let ph = params.push(Value::Number(modified_before.into()));
        clauses.push(format!("v.modified < {ph}"));
    }

    if leaf.relation.relation_type.is_some()
        || leaf.relation.related_tenantid.is_some()
        || leaf.relation.related_unitid.is_some()
    {
        let side = leaf.relation.side.unwrap_or(RelationSide::Left);
        let mut rel = Vec::new();
        if let Some(relation_type) = leaf.relation.relation_type {
            let ph = params.push(Value::Number(relation_type.into()));
            rel.push(format!("r.reltype = {ph}"));
        }
        if let Some(related_tenantid) = leaf.relation.related_tenantid {
            let ph = params.push(Value::Number(related_tenantid.into()));
            match side {
                RelationSide::Left => rel.push(format!("r.reltenantid = {ph}")),
                RelationSide::Right => rel.push(format!("src.tenantid = {ph}")),
            }
        }
        if let Some(related_unitid) = leaf.relation.related_unitid {
            let ph = params.push(Value::Number(related_unitid.into()));
            match side {
                RelationSide::Left => rel.push(format!("r.relunitid = {ph}")),
                RelationSide::Right => rel.push(format!("src.unitid = {ph}")),
            }
        }
        let pattern = match side {
            RelationSide::Left => "MATCH (k)-[r:RELATED_TO]->(:UnitKernel)".to_string(),
            RelationSide::Right => "MATCH (src:UnitKernel)-[r:RELATED_TO]->(k)".to_string(),
        };
        let exists = if rel.is_empty() {
            format!("EXISTS {{ {pattern} }}")
        } else {
            format!("EXISTS {{ {pattern} WHERE {} }}", rel.join(" AND "))
        };
        clauses.push(exists);
    }

    if leaf.association.association_type.is_some()
        || leaf.association.association_reference.is_some()
    {
        let _association_side = leaf.association.side.unwrap_or(RelationSide::Left);
        let mut assoc = Vec::new();
        if let Some(association_type) = leaf.association.association_type {
            let ph = params.push(Value::Number(association_type.into()));
            assoc.push(format!("a.assoctype = {ph}"));
        }
        if let Some(reference) = leaf.association.association_reference.as_ref() {
            let ph = params.push(Value::String(reference.clone()));
            assoc.push(format!("a.refstring = {ph}"));
        }
        if assoc.is_empty() {
            clauses.push("EXISTS { MATCH (k)-[:ASSOCIATED_WITH]->(:AssociationRef) }".to_string());
        } else {
            clauses.push(format!(
                "EXISTS {{ MATCH (k)-[a:ASSOCIATED_WITH]->(:AssociationRef) WHERE {} }}",
                assoc.join(" AND ")
            ));
        }
    }

    if leaf.attribute_cmp.op.is_some() {
        clauses.push(compile_attribute_constraint(&leaf.attribute_cmp, params)?);
    }
    for predicate in &leaf.predicates {
        clauses.push(compile_unit_predicate(predicate, params)?);
    }

    if clauses.is_empty() {
        Ok("true".to_string())
    } else {
        Ok(clauses.join(" AND "))
    }
}

fn compile_unit_predicate(
    predicate: &UnitPredicate,
    params: &mut CypherParams,
) -> RepoResult<String> {
    let op = match predicate.op {
        UnitOp::Eq => "=",
        UnitOp::Neq => "<>",
        UnitOp::Gt => ">",
        UnitOp::Gte => ">=",
        UnitOp::Lt => "<",
        UnitOp::Lte => "<=",
        UnitOp::Like => "LIKE",
    };

    let sql = match (&predicate.field, &predicate.value) {
        (UnitField::TenantId, UnitValue::I64(value)) => {
            let ph = params.push(Value::Number((*value).into()));
            format!("k.tenantid {op} {ph}")
        }
        (UnitField::UnitId, UnitValue::I64(value)) => {
            let ph = params.push(Value::Number((*value).into()));
            format!("k.unitid {op} {ph}")
        }
        (UnitField::UnitVer, UnitValue::I64(value)) => {
            let ph = params.push(Value::Number((*value).into()));
            format!("v.unitver {op} {ph}")
        }
        (UnitField::Status, UnitValue::I64(value)) => {
            let ph = params.push(Value::Number((*value).into()));
            format!("k.status {op} {ph}")
        }
        (UnitField::Name, UnitValue::String(value)) => {
            let ph = params.push(Value::String(value.clone()));
            if matches!(predicate.op, UnitOp::Like) {
                let regex_ph = params.push(Value::String(like_to_regex(value)));
                format!("(v.unitname IS NOT NULL AND v.unitname =~ {regex_ph})")
            } else {
                format!("v.unitname {op} {ph}")
            }
        }
        (UnitField::Corrid, UnitValue::String(value)) => {
            let ph = params.push(Value::String(value.clone()));
            format!("k.corrid {op} {ph}")
        }
        (UnitField::Created, UnitValue::TimestampMillis(value)) => {
            let ph = params.push(Value::Number((*value).into()));
            format!("k.created {op} {ph}")
        }
        (UnitField::Modified, UnitValue::TimestampMillis(value)) => {
            let ph = params.push(Value::Number((*value).into()));
            format!("v.modified {op} {ph}")
        }
        _ => {
            return Err(RepoError::InvalidInput(
                "invalid predicates field/value combination".to_string(),
            ))
        }
    };
    Ok(sql)
}

fn compile_attribute_constraint(
    attribute_cmp: &AttributeCmpConstraint,
    params: &mut CypherParams,
) -> RepoResult<String> {
    let selector = if let Some(attr_id) = attribute_cmp.attr_id {
        let ph = params.push(Value::Number(attr_id.into()));
        format!("coalesce(toInteger(av.attrid), -1) = {ph}")
    } else if let Some(attr_name) = attribute_cmp.attr_name.as_ref() {
        let ph = params.push(Value::String(attr_name.clone()));
        format!(
            "(\
             av.attrname = {ph} \
             OR EXISTS {{ \
               MATCH (a:Attribute) \
               WHERE (a.name = av.attrname OR coalesce(toInteger(a.id), -1) = coalesce(toInteger(av.attrid), -2)) \
                 AND (a.name = {ph} OR a.alias = {ph} OR a.qualname = {ph}) \
             }}\
             )"
        )
    } else {
        return Err(RepoError::InvalidInput(
            "attribute_cmp requires attrid/name selector".to_string(),
        ));
    };

    let op = attribute_cmp.op.as_deref().unwrap_or("eq");
    let value_type = attribute_cmp.value_type.as_deref().unwrap_or("string");

    let value_pred = match value_type {
        "string" => {
            let text = attribute_cmp.value_text.as_ref().ok_or_else(|| {
                RepoError::InvalidInput("attribute_cmp string comparison missing value".to_string())
            })?;
            let text_ph = params.push(Value::String(text.clone()));
            let cmp = match op {
                "like" => {
                    let regex = like_to_regex(text);
                    let regex_ph = params.push(Value::String(regex));
                    format!("toLower(toString(av.value)) =~ {regex_ph}")
                }
                "eq" if attribute_cmp.value_wildcard == Some(true) => {
                    let regex = like_to_regex(text);
                    let regex_ph = params.push(Value::String(regex));
                    format!("toLower(toString(av.value)) =~ {regex_ph}")
                }
                "eq" => format!("toLower(toString(av.value)) = {text_ph}"),
                "neq" if attribute_cmp.value_wildcard == Some(true) => {
                    let regex = like_to_regex(text);
                    let regex_ph = params.push(Value::String(regex));
                    format!("NOT (toLower(toString(av.value)) =~ {regex_ph})")
                }
                "neq" => format!("toLower(toString(av.value)) <> {text_ph}"),
                "gt" => format!("toLower(toString(av.value)) > {text_ph}"),
                "gte" => format!("toLower(toString(av.value)) >= {text_ph}"),
                "lt" => format!("toLower(toString(av.value)) < {text_ph}"),
                "lte" => format!("toLower(toString(av.value)) <= {text_ph}"),
                other => {
                    return Err(RepoError::InvalidInput(format!(
                        "unsupported string comparison operator: {other}"
                    )))
                }
            };
            format!("coalesce(toInteger(av.attrtype), -1) = 1 AND {cmp}")
        }
        "number" => {
            let number = attribute_cmp.value_number.ok_or_else(|| {
                RepoError::InvalidInput("attribute_cmp number comparison missing value".to_string())
            })?;
            let num_ph = params.push(json!(number));
            let cmp = match op {
                "eq" => format!("toFloat(av.value) = {num_ph}"),
                "neq" => format!("toFloat(av.value) <> {num_ph}"),
                "gt" => format!("toFloat(av.value) > {num_ph}"),
                "gte" => format!("toFloat(av.value) >= {num_ph}"),
                "lt" => format!("toFloat(av.value) < {num_ph}"),
                "lte" => format!("toFloat(av.value) <= {num_ph}"),
                other => {
                    return Err(RepoError::InvalidInput(format!(
                        "unsupported number comparison operator: {other}"
                    )))
                }
            };
            format!("coalesce(toInteger(av.attrtype), -1) IN [3,4,5] AND {cmp}")
        }
        "boolean" => {
            if !matches!(op, "eq" | "neq") {
                return Err(RepoError::InvalidInput(
                    "attribute_cmp boolean comparisons support only eq|neq".to_string(),
                ));
            }
            let value = attribute_cmp.value_bool.ok_or_else(|| {
                RepoError::InvalidInput(
                    "attribute_cmp boolean comparison missing value".to_string(),
                )
            })?;
            let bool_ph = params.push(Value::Bool(value));
            let cmp = if op == "eq" {
                format!("av.value = {bool_ph}")
            } else {
                format!("av.value <> {bool_ph}")
            };
            format!("coalesce(toInteger(av.attrtype), -1) = 6 AND {cmp}")
        }
        "time" => {
            let time = attribute_cmp.value_time_millis.ok_or_else(|| {
                RepoError::InvalidInput("attribute_cmp time comparison missing value".to_string())
            })?;
            let time_ph = params.push(Value::String(millis_to_iso8601_checked(time)?));
            let cmp = match op {
                "eq" => format!("datetime(toString(av.value)) = datetime({time_ph})"),
                "neq" => format!("datetime(toString(av.value)) <> datetime({time_ph})"),
                "gt" => format!("datetime(toString(av.value)) > datetime({time_ph})"),
                "gte" => format!("datetime(toString(av.value)) >= datetime({time_ph})"),
                "lt" => format!("datetime(toString(av.value)) < datetime({time_ph})"),
                "lte" => format!("datetime(toString(av.value)) <= datetime({time_ph})"),
                other => {
                    return Err(RepoError::InvalidInput(format!(
                        "unsupported time comparison operator: {other}"
                    )))
                }
            };
            format!("coalesce(toInteger(av.attrtype), -1) = 2 AND {cmp}")
        }
        other => {
            return Err(RepoError::InvalidInput(format!(
                "unsupported attribute value_type: {other}"
            )))
        }
    };

    Ok(format!(
        // Attribute predicates are evaluated through HAS_ATTR_VALUE nodes.
        "EXISTS {{ MATCH (v)-[:HAS_ATTR_VALUE]->(av:AttributeValue) WHERE ({selector}) AND ({value_pred}) }}"
    ))
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

fn has_name_wildcard(value: &str) -> bool {
    value.contains('*') || value.contains('%') || value.contains('_')
}

fn payload_to_object(value: Option<&Value>) -> RepoResult<Map<String, Value>> {
    match value {
        Some(Value::String(s)) => match serde_json::from_str::<Value>(s) {
            Ok(Value::Object(obj)) => Ok(obj),
            Ok(other) => {
                let mut obj = Map::new();
                obj.insert("payload".to_string(), other);
                Ok(obj)
            }
            Err(_) => {
                let mut obj = Map::new();
                obj.insert("payload".to_string(), Value::String(s.clone()));
                Ok(obj)
            }
        },
        Some(Value::Object(obj)) => Ok(obj.clone()),
        Some(other) => {
            let mut obj = Map::new();
            obj.insert("payload".to_string(), other.clone());
            Ok(obj)
        }
        None => Ok(Map::new()),
    }
}

fn order_sql(order: &SearchOrder) -> (&'static str, &'static str) {
    let field = match order.field.as_str() {
        "created" => "k.created",
        "modified" => "v.modified",
        "unitid" => "k.unitid",
        "status" => "k.status",
        _ => "k.created",
    };

    let dir = if order.descending { "DESC" } else { "ASC" };
    (field, dir)
}

fn infer_tenant_id(expr: &Map<String, Value>) -> Option<i64> {
    if expr.contains_key("tenantid") {
        if let Some(v) = expr.get("tenantid").and_then(Value::as_i64) {
            return Some(v);
        }
        if let Some(v) = expr
            .get("tenantid")
            .and_then(Value::as_str)
            .and_then(|s| s.parse::<i64>().ok())
        {
            return Some(v);
        }
    }
    if expr.contains_key("tenant_id") {
        if let Some(v) = expr.get("tenant_id").and_then(Value::as_i64) {
            return Some(v);
        }
        if let Some(v) = expr
            .get("tenant_id")
            .and_then(Value::as_str)
            .and_then(|s| s.parse::<i64>().ok())
        {
            return Some(v);
        }
    }
    if let Some(and_children) = expr.get("and").and_then(Value::as_array) {
        let mut tenant = None;
        for child in and_children {
            let child_obj = child.as_object()?;
            let child_tenant = infer_tenant_id(child_obj)?;
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
    if let Some(or_children) = expr.get("or").and_then(Value::as_array) {
        let mut tenant = None;
        for child in or_children {
            let child_obj = child.as_object()?;
            let child_tenant = infer_tenant_id(child_obj)?;
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
    expr.get("not")
        .and_then(Value::as_object)
        .and_then(infer_tenant_id)
}

fn row_to_unit(row: &[Value]) -> RepoResult<Unit> {
    if row.len() < 10 {
        return Err(RepoError::Backend(format!(
            "invalid neo4j search row: {:?}",
            row
        )));
    }

    let tenant_id = extract_i64(row.first()).unwrap_or_default();
    let unit_id = extract_i64(row.get(1)).unwrap_or_default();
    let unit_ver = extract_i64(row.get(2)).unwrap_or(1);
    let last_ver = extract_i64(row.get(3)).unwrap_or(unit_ver);
    let corrid = row
        .get(4)
        .and_then(Value::as_str)
        .and_then(|s| Uuid::parse_str(s).ok())
        .unwrap_or_else(Uuid::now_v7);
    let status = i32::try_from(extract_i64(row.get(5)).unwrap_or(30)).unwrap_or(30);
    let created = extract_i64(row.get(6)).unwrap_or_default();
    let modified = extract_i64(row.get(7)).unwrap_or(created);
    let name = row.get(8).and_then(Value::as_str).map(ToString::to_string);

    let mut payload = payload_to_object(row.get(9))?;
    payload.insert("tenantid".to_string(), Value::Number(tenant_id.into()));
    payload.insert("unitid".to_string(), Value::Number(unit_id.into()));
    payload.insert("unitver".to_string(), Value::Number(unit_ver.into()));
    payload.insert(
        "status".to_string(),
        Value::Number(i64::from(status).into()),
    );
    payload.insert(
        "created".to_string(),
        Value::String(millis_to_iso8601(created)),
    );
    payload.insert(
        "modified".to_string(),
        Value::String(millis_to_iso8601(modified)),
    );
    payload.insert("isreadonly".to_string(), Value::Bool(last_ver > unit_ver));

    Ok(Unit {
        tenant_id,
        unit_id,
        unit_ver,
        status,
        name,
        corr_id: corrid,
        payload: Value::Object(payload),
    })
}

fn relation_from_value(value: &Value) -> RepoResult<Relation> {
    let obj = value
        .as_object()
        .ok_or_else(|| RepoError::Backend(format!("invalid neo4j relation row: {value}")))?;

    Ok(Relation {
        tenant_id: get_map_i64(obj, "tenantid")?,
        unit_id: get_map_i64(obj, "unitid")?,
        relation_type: get_map_i64(obj, "reltype")? as i32,
        related_tenant_id: get_map_i64(obj, "reltenantid")?,
        related_unit_id: get_map_i64(obj, "relunitid")?,
    })
}

fn association_from_value(value: &Value) -> RepoResult<Association> {
    let obj = value
        .as_object()
        .ok_or_else(|| RepoError::Backend(format!("invalid neo4j association row: {value}")))?;

    let reference = obj
        .get("assocstring")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();

    Ok(Association {
        tenant_id: get_map_i64(obj, "tenantid")?,
        unit_id: get_map_i64(obj, "unitid")?,
        association_type: get_map_i64(obj, "assoctype")? as i32,
        reference,
    })
}

fn get_map_i64(obj: &Map<String, Value>, key: &str) -> RepoResult<i64> {
    extract_i64(obj.get(key)).ok_or_else(|| {
        RepoError::Backend(format!(
            "missing/invalid key '{key}' in neo4j map: {}",
            Value::Object(obj.clone())
        ))
    })
}

fn attribute_info_from_row(row: &[Value]) -> RepoResult<Value> {
    if row.len() < 6 {
        return Err(RepoError::Backend(format!(
            "invalid neo4j attribute row: {:?}",
            row
        )));
    }

    Ok(json!({
        "id": extract_i64(row.first()).unwrap_or_default(),
        "alias": row.get(1).cloned().unwrap_or(Value::Null),
        "name": row.get(2).and_then(Value::as_str).unwrap_or_default(),
        "qualname": row.get(3).and_then(Value::as_str).unwrap_or_default(),
        "type": extract_i64(row.get(4)).unwrap_or_default(),
        "forced_scalar": row.get(5).and_then(Value::as_bool).unwrap_or(false)
    }))
}

fn tenant_info_from_row(row: &[Value]) -> RepoResult<Value> {
    if row.len() < 4 {
        return Err(RepoError::Backend(format!(
            "invalid neo4j tenant row: {:?}",
            row
        )));
    }

    Ok(json!({
        "id": extract_i64(row.first()).unwrap_or_default(),
        "name": row.get(1).and_then(Value::as_str).unwrap_or_default(),
        "description": row.get(2).cloned().unwrap_or(Value::Null),
        "created": millis_to_iso8601(extract_i64(row.get(3)).unwrap_or_default())
    }))
}

fn millis_to_iso8601(millis: i64) -> String {
    DateTime::<Utc>::from_timestamp_millis(millis)
        .unwrap_or_else(|| DateTime::<Utc>::from_timestamp(0, 0).expect("unix epoch"))
        .to_rfc3339_opts(SecondsFormat::Secs, true)
}

fn millis_to_iso8601_checked(millis: i64) -> RepoResult<String> {
    DateTime::<Utc>::from_timestamp_millis(millis)
        .map(|dt| dt.to_rfc3339_opts(SecondsFormat::Secs, true))
        .ok_or_else(|| {
            RepoError::InvalidInput(format!("time value has out-of-range millis: {millis}"))
        })
}

fn collect_attribute_refs(obj: &Map<String, Value>) -> (Vec<i64>, Vec<String>) {
    let mut ids = HashSet::new();
    let mut names = HashSet::new();

    let attrs = obj
        .get("attributes")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();

    for attr in attrs {
        let Some(map) = attr.as_object() else {
            continue;
        };

        if let Some(id) = map
            .get("attrid")
            .and_then(Value::as_i64)
            .or_else(|| map.get("id").and_then(Value::as_i64))
        {
            ids.insert(id);
        }

        if let Some(name) = map
            .get("attrname")
            .and_then(Value::as_str)
            .or_else(|| map.get("name").and_then(Value::as_str))
            .map(ToString::to_string)
        {
            names.insert(name);
        }
    }

    (ids.into_iter().collect(), names.into_iter().collect())
}

fn collect_attribute_entries(obj: &Map<String, Value>) -> Vec<Value> {
    let attrs = obj
        .get("attributes")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();

    let mut out = Vec::new();
    for attr in attrs {
        let Some(map) = attr.as_object() else {
            continue;
        };
        let attr_id = map
            .get("attrid")
            .and_then(Value::as_i64)
            .or_else(|| map.get("id").and_then(Value::as_i64));
        let attr_name = map
            .get("attrname")
            .and_then(Value::as_str)
            .or_else(|| map.get("name").and_then(Value::as_str))
            .map(ToString::to_string);
        let attr_type = map
            .get("attrtype")
            .and_then(Value::as_i64)
            .or_else(|| map.get("type").and_then(Value::as_i64));

        let raw_value = map.get("value").cloned().unwrap_or(Value::Null);
        let values = attribute_values_as_primitives(raw_value);
        if values.is_empty() {
            continue;
        }

        out.push(json!({
            "attrid": attr_id,
            "attrname": attr_name,
            "attrtype": attr_type,
            "values": values
        }));
    }
    out
}

fn attribute_values_as_primitives(raw: Value) -> Vec<Value> {
    match raw {
        // Only scalar primitives are indexed in AttributeValue nodes.
        Value::Array(items) => items
            .into_iter()
            .filter(|v| matches!(v, Value::String(_) | Value::Number(_) | Value::Bool(_)))
            .collect(),
        Value::String(_) | Value::Number(_) | Value::Bool(_) => vec![raw],
        _ => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parse_leaf_name_wildcard_promotes_to_name_regex() {
        let expr = json!({
            "tenantid": 1,
            "name": "alpha*"
        });
        let obj = expr.as_object().expect("object");
        let leaf = parse_leaf_constraint(obj)
            .expect("parse leaf")
            .expect("leaf exists");
        assert!(leaf.name.is_none());
        assert!(leaf.name_regex.is_some());
    }
}
