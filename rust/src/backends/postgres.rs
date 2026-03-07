use chrono::{DateTime, NaiveDateTime, Utc};
use postgres::types::ToSql;
use postgres::{Client, Config, NoTls, Row};
use serde_json::{json, Map, Number, Value};
use std::ops::{Deref, DerefMut};
use std::sync::{Mutex, MutexGuard};
use uuid::Uuid;

use crate::backend::{Backend, RepoError, RepoResult};
use crate::model::{
    Association, Relation, SearchOrder, SearchPaging, SearchResult, Unit, UnitRef, VersionSelector,
};
use crate::search_expr::{parse_bool_expression, BoolExpr};
use crate::search_filters::{
    extract_association_constraint, extract_attribute_constraint, extract_relation_constraint,
    extract_unit_predicates, AssociationConstraint, AttributeCmpConstraint, RelationConstraint,
    RelationSide, UnitField, UnitOp, UnitPredicate, UnitValue,
};

pub struct PostgresBackend {
    cfg: PostgresConfig,
    client: Mutex<Option<Client>>,
}

struct PostgresClientGuard<'a> {
    inner: MutexGuard<'a, Option<Client>>,
}

impl<'a> Deref for PostgresClientGuard<'a> {
    type Target = Client;

    fn deref(&self) -> &Self::Target {
        self.inner
            .as_ref()
            .expect("postgres client must be initialized")
    }
}

impl<'a> DerefMut for PostgresClientGuard<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.inner
            .as_mut()
            .expect("postgres client must be initialized")
    }
}

#[derive(Debug, Clone)]
struct PostgresConfig {
    host: String,
    port: u16,
    user: String,
    password: String,
    database: String,
}

impl PostgresConfig {
    fn from_env() -> Self {
        Self {
            host: std::env::var("IPTO_PG_HOST").unwrap_or_else(|_| "localhost".to_string()),
            port: std::env::var("IPTO_PG_PORT")
                .ok()
                .and_then(|s| s.parse::<u16>().ok())
                .unwrap_or(5432),
            user: std::env::var("IPTO_PG_USER").unwrap_or_else(|_| "repo".to_string()),
            password: std::env::var("IPTO_PG_PASSWORD").unwrap_or_else(|_| "repo".to_string()),
            database: std::env::var("IPTO_PG_DATABASE").unwrap_or_else(|_| "repo".to_string()),
        }
    }

    fn connect(&self) -> RepoResult<Client> {
        let mut cfg = Config::new();
        cfg.host(&self.host)
            .port(self.port)
            .user(&self.user)
            .password(&self.password)
            .dbname(&self.database);

        cfg.connect(NoTls)
            .map_err(|e| RepoError::Backend(format!("postgres connect failed: {e}")))
    }
}

impl PostgresBackend {
    pub fn new() -> Self {
        Self {
            cfg: PostgresConfig::from_env(),
            client: Mutex::new(None),
        }
    }

    fn client(&self) -> RepoResult<PostgresClientGuard<'_>> {
        let mut guard = self
            .client
            .lock()
            .map_err(|_| RepoError::Backend("postgres client lock poisoned".to_string()))?;
        // Keep a single lazily-initialized client and reconnect if the server
        // closes the connection between calls.
        let reconnect = guard
            .as_ref()
            .map(|client| client.is_closed())
            .unwrap_or(true);
        if reconnect {
            *guard = Some(self.cfg.connect()?);
        }
        Ok(PostgresClientGuard { inner: guard })
    }
}

impl Default for PostgresBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl Backend for PostgresBackend {
    fn get_unit_json(
        &self,
        tenant_id: i64,
        unit_id: i64,
        selector: VersionSelector,
    ) -> RepoResult<Option<Value>> {
        let mut client = self.client()?;
        let tenant = as_i32(tenant_id, "tenant_id")?;
        let version = match selector {
            VersionSelector::Latest => -1,
            VersionSelector::Exact(v) => as_i32(v, "version")?,
        };

        let row = client
            .query_one(
                "CALL repo.extract_unit_json($1::integer, $2::bigint, $3::integer, NULL::jsonb)",
                &[&tenant, &unit_id, &version],
            )
            .map_err(|e| RepoError::Backend(format!("extract_unit_json failed: {e}")))?;

        let payload: Option<Value> = row
            .try_get(0)
            .map_err(|e| RepoError::Backend(format!("extract_unit_json decode failed: {e}")))?;
        Ok(payload.map(normalize_payload_timestamps))
    }

    fn get_unit_by_corrid_json(&self, tenant_id: i64, corrid: &str) -> RepoResult<Option<Value>> {
        let mut client = self.client()?;
        let tenant = as_i32(tenant_id, "tenant_id")?;
        let corrid = Uuid::parse_str(corrid)
            .map_err(|e| RepoError::InvalidInput(format!("invalid corrid '{corrid}': {e}")))?;

        let row = client
            .query_opt(
                "SELECT unitid FROM repo.repo_unit_kernel WHERE tenantid = $1 AND corrid = $2",
                &[&tenant, &corrid],
            )
            .map_err(|e| RepoError::Backend(format!("lookup by corrid failed: {e}")))?;

        match row {
            Some(row) => {
                let unit_id: i64 = row.get(0);
                self.get_unit_json(tenant_id, unit_id, VersionSelector::Latest)
            }
            None => Ok(None),
        }
    }

    fn unit_exists(&self, tenant_id: i64, unit_id: i64) -> RepoResult<bool> {
        let mut client = self.client()?;
        let tenant = as_i32(tenant_id, "tenant_id")?;

        let rows = client
            .query(
                "SELECT 1 FROM repo.repo_unit_kernel WHERE tenantid = $1 AND unitid = $2",
                &[&tenant, &unit_id],
            )
            .map_err(|e| RepoError::Backend(format!("unit_exists query failed: {e}")))?;

        Ok(!rows.is_empty())
    }

    fn store_unit_json(&self, unit: Value) -> RepoResult<Value> {
        let mut client = self.client()?;
        let mut obj = as_object(unit)?;
        let unit_text = Value::Object(obj.clone()).to_string();

        if obj.contains_key("unitid") {
            let row = client
                .query_one(
                    "CALL repo.ingest_new_version_json($1::text, NULL, NULL)",
                    &[&unit_text],
                )
                .map_err(|e| RepoError::Backend(format!("ingest_new_version_json failed: {e}")))?;

            let unit_ver: i32 = row.get(0);
            let modified: NaiveDateTime = row.get(1);

            obj.insert("unitver".to_string(), Value::Number(Number::from(unit_ver)));
            obj.insert(
                "modified".to_string(),
                Value::String(format_pg_timestamp(modified)),
            );
            obj.insert("isreadonly".to_string(), Value::Bool(false));
            Ok(Value::Object(obj))
        } else {
            let row = client
                .query_one(
                    "CALL repo.ingest_new_unit_json($1::text, NULL, NULL, NULL, NULL)",
                    &[&unit_text],
                )
                .map_err(|e| RepoError::Backend(format!("ingest_new_unit_json failed: {e}")))?;

            let unit_id: i64 = row.get(0);
            let unit_ver: i32 = row.get(1);
            let created: NaiveDateTime = row.get(2);
            let modified: NaiveDateTime = row.get(3);

            obj.insert("unitid".to_string(), Value::Number(Number::from(unit_id)));
            obj.insert("unitver".to_string(), Value::Number(Number::from(unit_ver)));
            obj.insert(
                "created".to_string(),
                Value::String(format_pg_timestamp(created)),
            );
            obj.insert(
                "modified".to_string(),
                Value::String(format_pg_timestamp(modified)),
            );
            obj.insert("isreadonly".to_string(), Value::Bool(false));
            Ok(Value::Object(obj))
        }
    }

    fn search_units(
        &self,
        expression: Value,
        order: SearchOrder,
        paging: SearchPaging,
    ) -> RepoResult<SearchResult> {
        let mut client = self.client()?;
        let search_expr = parse_search_expression(&expression)?;
        let mut params = Vec::new();
        let where_clause = compile_search_expression(&search_expr, &mut params)?;
        let (order_field, order_dir) = order_sql(&order);
        let limit = if paging.limit > 0 { paging.limit } else { 100 };
        let offset = if paging.offset >= 0 { paging.offset } else { 0 };

        let base_from = " FROM repo.repo_unit_kernel uk JOIN repo.repo_unit_version uv ON (uk.tenantid = uv.tenantid AND uk.unitid = uv.unitid AND uk.lastver = uv.unitver)";
        let count_sql = if let Some(where_sql) = where_clause.as_ref() {
            format!("SELECT count(*){} WHERE {}", base_from, where_sql)
        } else {
            format!("SELECT count(*){}", base_from)
        };
        let count_refs = params_as_refs(&params);
        let count_row = client
            .query_one(&count_sql, &count_refs)
            .map_err(|e| RepoError::Backend(format!("search count query failed: {e}")))?;
        let total_hits: i64 = count_row.get(0);

        let mut select_params = params.clone();
        let limit_ph = push_param(&mut select_params, SqlParam::I64(limit));
        let offset_ph = push_param(&mut select_params, SqlParam::I64(offset));
        let select_sql = if let Some(where_sql) = where_clause.as_ref() {
            format!(
                "SELECT uk.tenantid, uk.unitid, uv.unitver, uk.corrid, uk.status, uk.created, uv.modified, uv.unitname{} WHERE {} ORDER BY {} {} LIMIT {} OFFSET {}",
                base_from, where_sql, order_field, order_dir, limit_ph, offset_ph
            )
        } else {
            format!(
                "SELECT uk.tenantid, uk.unitid, uv.unitver, uk.corrid, uk.status, uk.created, uv.modified, uv.unitname{} ORDER BY {} {} LIMIT {} OFFSET {}",
                base_from, order_field, order_dir, limit_ph, offset_ph
            )
        };
        let select_refs = params_as_refs(&select_params);

        let rows = client
            .query(&select_sql, &select_refs)
            .map_err(|e| RepoError::Backend(format!("search select query failed: {e}")))?;

        Ok(SearchResult {
            total_hits,
            results: rows
                .into_iter()
                .map(row_to_unit)
                .collect::<RepoResult<Vec<Unit>>>()?,
        })
    }

    fn add_relation(&self, left: UnitRef, relation_type: i32, right: UnitRef) -> RepoResult<()> {
        let mut client = self.client()?;
        let left_tenant = as_i32(left.tenant_id, "left.tenant_id")?;
        let right_tenant = as_i32(right.tenant_id, "right.tenant_id")?;

        client
            .execute(
                "INSERT INTO repo.repo_internal_relation (tenantid, unitid, reltype, reltenantid, relunitid) VALUES ($1, $2, $3, $4, $5) ON CONFLICT DO NOTHING",
                &[&left_tenant, &left.unit_id, &relation_type, &right_tenant, &right.unit_id],
            )
            .map_err(|e| RepoError::Backend(format!("add_relation failed: {e}")))?;
        Ok(())
    }

    fn remove_relation(&self, left: UnitRef, relation_type: i32, right: UnitRef) -> RepoResult<()> {
        let mut client = self.client()?;
        let left_tenant = as_i32(left.tenant_id, "left.tenant_id")?;
        let right_tenant = as_i32(right.tenant_id, "right.tenant_id")?;

        client
            .execute(
                "DELETE FROM repo.repo_internal_relation WHERE tenantid = $1 AND unitid = $2 AND reltype = $3 AND reltenantid = $4 AND relunitid = $5",
                &[&left_tenant, &left.unit_id, &relation_type, &right_tenant, &right.unit_id],
            )
            .map_err(|e| RepoError::Backend(format!("remove_relation failed: {e}")))?;
        Ok(())
    }

    fn get_right_relation(
        &self,
        unit: UnitRef,
        relation_type: i32,
    ) -> RepoResult<Option<Relation>> {
        let mut client = self.client()?;
        let tenant = as_i32(unit.tenant_id, "tenant_id")?;

        let row = client
            .query_opt(
                "SELECT tenantid, unitid, reltype, reltenantid, relunitid FROM repo.repo_internal_relation WHERE tenantid = $1 AND unitid = $2 AND reltype = $3 LIMIT 1",
                &[&tenant, &unit.unit_id, &relation_type],
            )
            .map_err(|e| RepoError::Backend(format!("get_right_relation failed: {e}")))?;

        row.map(relation_from_row).transpose()
    }

    fn get_right_relations(&self, unit: UnitRef, relation_type: i32) -> RepoResult<Vec<Relation>> {
        let mut client = self.client()?;
        let tenant = as_i32(unit.tenant_id, "tenant_id")?;

        let rows = client
            .query(
                "SELECT tenantid, unitid, reltype, reltenantid, relunitid FROM repo.repo_internal_relation WHERE tenantid = $1 AND unitid = $2 AND reltype = $3",
                &[&tenant, &unit.unit_id, &relation_type],
            )
            .map_err(|e| RepoError::Backend(format!("get_right_relations failed: {e}")))?;
        rows.into_iter().map(relation_from_row).collect()
    }

    fn get_left_relations(&self, unit: UnitRef, relation_type: i32) -> RepoResult<Vec<Relation>> {
        let mut client = self.client()?;
        let tenant = as_i32(unit.tenant_id, "tenant_id")?;

        let rows = client
            .query(
                "SELECT tenantid, unitid, reltype, reltenantid, relunitid FROM repo.repo_internal_relation WHERE reltenantid = $1 AND relunitid = $2 AND reltype = $3",
                &[&tenant, &unit.unit_id, &relation_type],
            )
            .map_err(|e| RepoError::Backend(format!("get_left_relations failed: {e}")))?;
        rows.into_iter().map(relation_from_row).collect()
    }

    fn count_right_relations(&self, unit: UnitRef, relation_type: i32) -> RepoResult<i64> {
        let mut client = self.client()?;
        let tenant = as_i32(unit.tenant_id, "tenant_id")?;
        let row = client
            .query_one(
                "SELECT COUNT(*) FROM repo.repo_internal_relation WHERE tenantid = $1 AND unitid = $2 AND reltype = $3",
                &[&tenant, &unit.unit_id, &relation_type],
            )
            .map_err(|e| RepoError::Backend(format!("count_right_relations failed: {e}")))?;
        Ok(row.get(0))
    }

    fn count_left_relations(&self, unit: UnitRef, relation_type: i32) -> RepoResult<i64> {
        let mut client = self.client()?;
        let tenant = as_i32(unit.tenant_id, "tenant_id")?;
        let row = client
            .query_one(
                "SELECT COUNT(*) FROM repo.repo_internal_relation WHERE reltenantid = $1 AND relunitid = $2 AND reltype = $3",
                &[&tenant, &unit.unit_id, &relation_type],
            )
            .map_err(|e| RepoError::Backend(format!("count_left_relations failed: {e}")))?;
        Ok(row.get(0))
    }

    fn add_association(
        &self,
        unit: UnitRef,
        association_type: i32,
        reference: &str,
    ) -> RepoResult<()> {
        let mut client = self.client()?;
        let tenant = as_i32(unit.tenant_id, "tenant_id")?;

        client
            .execute(
                "INSERT INTO repo.repo_external_assoc (tenantid, unitid, assoctype, assocstring) VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING",
                &[&tenant, &unit.unit_id, &association_type, &reference],
            )
            .map_err(|e| RepoError::Backend(format!("add_association failed: {e}")))?;
        Ok(())
    }

    fn remove_association(
        &self,
        unit: UnitRef,
        association_type: i32,
        reference: &str,
    ) -> RepoResult<()> {
        let mut client = self.client()?;
        let tenant = as_i32(unit.tenant_id, "tenant_id")?;

        client
            .execute(
                "DELETE FROM repo.repo_external_assoc WHERE tenantid = $1 AND unitid = $2 AND assoctype = $3 AND assocstring = $4",
                &[&tenant, &unit.unit_id, &association_type, &reference],
            )
            .map_err(|e| RepoError::Backend(format!("remove_association failed: {e}")))?;
        Ok(())
    }

    fn get_right_association(
        &self,
        unit: UnitRef,
        association_type: i32,
    ) -> RepoResult<Option<Association>> {
        let mut client = self.client()?;
        let tenant = as_i32(unit.tenant_id, "tenant_id")?;
        let row = client
            .query_opt(
                "SELECT tenantid, unitid, assoctype, assocstring FROM repo.repo_external_assoc WHERE tenantid = $1 AND unitid = $2 AND assoctype = $3 LIMIT 1",
                &[&tenant, &unit.unit_id, &association_type],
            )
            .map_err(|e| RepoError::Backend(format!("get_right_association failed: {e}")))?;
        row.map(association_from_row).transpose()
    }

    fn get_right_associations(
        &self,
        unit: UnitRef,
        association_type: i32,
    ) -> RepoResult<Vec<Association>> {
        let mut client = self.client()?;
        let tenant = as_i32(unit.tenant_id, "tenant_id")?;
        let rows = client
            .query(
                "SELECT tenantid, unitid, assoctype, assocstring FROM repo.repo_external_assoc WHERE tenantid = $1 AND unitid = $2 AND assoctype = $3",
                &[&tenant, &unit.unit_id, &association_type],
            )
            .map_err(|e| RepoError::Backend(format!("get_right_associations failed: {e}")))?;
        rows.into_iter().map(association_from_row).collect()
    }

    fn get_left_associations(
        &self,
        association_type: i32,
        reference: &str,
    ) -> RepoResult<Vec<Association>> {
        let mut client = self.client()?;
        let rows = client
            .query(
                "SELECT tenantid, unitid, assoctype, assocstring FROM repo.repo_external_assoc WHERE assoctype = $1 AND assocstring = $2",
                &[&association_type, &reference],
            )
            .map_err(|e| RepoError::Backend(format!("get_left_associations failed: {e}")))?;
        rows.into_iter().map(association_from_row).collect()
    }

    fn count_right_associations(&self, unit: UnitRef, association_type: i32) -> RepoResult<i64> {
        let mut client = self.client()?;
        let tenant = as_i32(unit.tenant_id, "tenant_id")?;
        let row = client
            .query_one(
                "SELECT COUNT(*) FROM repo.repo_external_assoc WHERE tenantid = $1 AND unitid = $2 AND assoctype = $3",
                &[&tenant, &unit.unit_id, &association_type],
            )
            .map_err(|e| RepoError::Backend(format!("count_right_associations failed: {e}")))?;
        Ok(row.get(0))
    }

    fn count_left_associations(&self, association_type: i32, reference: &str) -> RepoResult<i64> {
        let mut client = self.client()?;
        let row = client
            .query_one(
                "SELECT COUNT(*) FROM repo.repo_external_assoc WHERE assoctype = $1 AND assocstring = $2",
                &[&association_type, &reference],
            )
            .map_err(|e| RepoError::Backend(format!("count_left_associations failed: {e}")))?;
        Ok(row.get(0))
    }

    fn lock_unit(&self, unit: UnitRef, lock_type: i32, purpose: &str) -> RepoResult<()> {
        let mut client = self.client()?;
        let tenant = as_i32(unit.tenant_id, "tenant_id")?;

        let existing = client
            .query_opt(
                "SELECT 1 FROM repo.repo_lock WHERE tenantid = $1 AND unitid = $2 LIMIT 1",
                &[&tenant, &unit.unit_id],
            )
            .map_err(|e| RepoError::Backend(format!("lock_unit pre-check failed: {e}")))?;
        if existing.is_some() {
            return Err(RepoError::AlreadyLocked);
        }

        client
            .execute(
                "INSERT INTO repo.repo_lock (tenantid, unitid, purpose, locktype, expire) VALUES ($1, $2, $3, $4, $5)",
                &[&tenant, &unit.unit_id, &purpose, &lock_type, &Option::<NaiveDateTime>::None],
            )
            .map_err(|e| RepoError::Backend(format!("lock_unit insert failed: {e}")))?;
        Ok(())
    }

    fn unlock_unit(&self, unit: UnitRef) -> RepoResult<()> {
        let mut client = self.client()?;
        let tenant = as_i32(unit.tenant_id, "tenant_id")?;
        client
            .execute(
                "DELETE FROM repo.repo_lock WHERE tenantid = $1 AND unitid = $2",
                &[&tenant, &unit.unit_id],
            )
            .map_err(|e| RepoError::Backend(format!("unlock_unit failed: {e}")))?;
        Ok(())
    }

    fn is_unit_locked(&self, unit: UnitRef) -> RepoResult<bool> {
        let mut client = self.client()?;
        let tenant = as_i32(unit.tenant_id, "tenant_id")?;
        let row = client
            .query_one(
                "SELECT COUNT(*) > 0 FROM repo.repo_lock WHERE tenantid = $1 AND unitid = $2",
                &[&tenant, &unit.unit_id],
            )
            .map_err(|e| RepoError::Backend(format!("is_unit_locked failed: {e}")))?;
        Ok(row.get(0))
    }

    fn set_status(&self, unit: UnitRef, status: i32) -> RepoResult<()> {
        let mut client = self.client()?;
        let tenant = as_i32(unit.tenant_id, "tenant_id")?;

        let changed = client
            .execute(
                "UPDATE repo.repo_unit_kernel SET status = $1 WHERE tenantid = $2 AND unitid = $3",
                &[&status, &tenant, &unit.unit_id],
            )
            .map_err(|e| RepoError::Backend(format!("set_status failed: {e}")))?;

        if changed > 0 {
            Ok(())
        } else {
            Err(RepoError::NotFound)
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
        let mut client = self.client()?;
        let attr_type = parse_attr_type(attribute_type)?;
        let scalar = !is_array;
        let alias_param: Option<&str> = if alias.is_empty() { None } else { Some(alias) };
        match client.query_one(
            "INSERT INTO repo.repo_attribute (attrtype, scalar, attrname, qualname, alias) VALUES ($1, $2, $3, $4, $5) RETURNING attrid, alias, attrname, qualname, attrtype, scalar",
            &[&attr_type, &scalar, &name, &qualname, &alias_param],
        ) {
            Ok(row) => attribute_info_from_row(row),
            Err(_) => self
                .get_attribute_info(name)?
                .ok_or(RepoError::Backend("attribute insert failed".to_string())),
        }
    }

    fn instantiate_attribute(&self, name_or_id: &str) -> RepoResult<Option<Value>> {
        self.get_attribute_info(name_or_id)
    }

    fn can_change_attribute(&self, name_or_id: &str) -> RepoResult<bool> {
        let mut client = self.client()?;

        let attr_id = if let Ok(id) = name_or_id.parse::<i32>() {
            Some(id)
        } else {
            self.get_attribute_info(name_or_id)?
                .and_then(|v| v.get("id").and_then(Value::as_i64))
                .and_then(|v| i32::try_from(v).ok())
        };

        let Some(attr_id) = attr_id else {
            return Ok(true);
        };

        let row = client
            .query_opt(
                "SELECT 1 FROM repo.repo_attribute_value WHERE attrid = $1 LIMIT 1",
                &[&attr_id],
            )
            .map_err(|e| RepoError::Backend(format!("can_change_attribute failed: {e}")))?;
        Ok(row.is_none())
    }

    fn get_attribute_info(&self, name_or_id: &str) -> RepoResult<Option<Value>> {
        let mut client = self.client()?;
        if let Ok(id) = name_or_id.parse::<i32>() {
            let row = client
                .query_opt(
                    "SELECT attrid, alias, attrname, qualname, attrtype, scalar FROM repo.repo_attribute WHERE attrid = $1",
                    &[&id],
                )
                .map_err(|e| RepoError::Backend(format!("get_attribute_info by id failed: {e}")))?;
            row.map(attribute_info_from_row).transpose()
        } else {
            let row = client
                .query_opt(
                    "SELECT attrid, alias, attrname, qualname, attrtype, scalar FROM repo.repo_attribute WHERE attrname = $1 OR alias = $1 OR qualname = $1",
                    &[&name_or_id],
                )
                .map_err(|e| RepoError::Backend(format!("get_attribute_info by name failed: {e}")))?;
            row.map(attribute_info_from_row).transpose()
        }
    }

    fn upsert_record_template(
        &self,
        record_attribute_id: i64,
        record_type_name: &str,
        fields: &[(i64, String)],
    ) -> RepoResult<()> {
        let mut client = self.client()?;
        let record_id = as_i32(record_attribute_id, "record_attribute_id")?;
        let mut tx = client
            .transaction()
            .map_err(|e| RepoError::Backend(format!("begin upsert_record_template failed: {e}")))?;

        if let Some(existing) = tx
            .query_opt(
                "SELECT recordid FROM repo.repo_record_template WHERE lower(name) = lower($1)",
                &[&record_type_name],
            )
            .map_err(|e| {
                RepoError::Backend(format!("record template lookup by name failed: {e}"))
            })?
        {
            let existing_record_id: i32 = existing.get(0);
            if existing_record_id != record_id {
                return Err(RepoError::InvalidInput(format!(
                    "record template name '{}' is already bound to record attribute id {}, cannot rebind to {}",
                    record_type_name, existing_record_id, record_id
                )));
            }
        }

        tx.execute(
            "INSERT INTO repo.repo_record_template (recordid, name) VALUES ($1, $2)
             ON CONFLICT (recordid) DO UPDATE SET name = EXCLUDED.name",
            &[&record_id, &record_type_name],
        )
        .map_err(|e| RepoError::Backend(format!("upsert record template failed: {e}")))?;

        // Replace element rows as an ordered snapshot so repeated SDL applies are
        // deterministic and idempotent.
        tx.execute(
            "DELETE FROM repo.repo_record_template_elements WHERE recordid = $1",
            &[&record_id],
        )
        .map_err(|e| RepoError::Backend(format!("clear record template elements failed: {e}")))?;

        for (idx, (attr_id, field_alias)) in fields.iter().enumerate() {
            let field_attr_id = as_i32(*attr_id, "record_field_attribute_id")?;
            let position = i32::try_from(idx + 1)
                .map_err(|_| RepoError::Backend("record field index overflow".to_string()))?;
            tx.execute(
                "INSERT INTO repo.repo_record_template_elements (recordid, attrid, idx, alias)
                 VALUES ($1, $2, $3, $4)",
                &[&record_id, &field_attr_id, &position, &field_alias],
            )
            .map_err(|e| {
                RepoError::Backend(format!("insert record template element failed: {e}"))
            })?;
        }

        tx.commit().map_err(|e| {
            RepoError::Backend(format!("commit upsert_record_template failed: {e}"))
        })?;
        Ok(())
    }

    fn upsert_unit_template(
        &self,
        template_name: &str,
        fields: &[(i64, String)],
    ) -> RepoResult<()> {
        let mut client = self.client()?;
        let mut tx = client
            .transaction()
            .map_err(|e| RepoError::Backend(format!("begin upsert_unit_template failed: {e}")))?;

        // Keep one template row per name; reuse template id on conflict.
        let row = tx
            .query_one(
                "INSERT INTO repo.repo_unit_template (name) VALUES ($1)
                 ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
                 RETURNING templateid",
                &[&template_name],
            )
            .map_err(|e| RepoError::Backend(format!("upsert unit template failed: {e}")))?;
        let template_id: i32 = row.get(0);

        tx.execute(
            "DELETE FROM repo.repo_unit_template_elements WHERE templateid = $1",
            &[&template_id],
        )
        .map_err(|e| RepoError::Backend(format!("clear unit template elements failed: {e}")))?;

        for (idx, (attr_id, field_alias)) in fields.iter().enumerate() {
            let field_attr_id = as_i32(*attr_id, "template_field_attribute_id")?;
            let position = i32::try_from(idx + 1)
                .map_err(|_| RepoError::Backend("template field index overflow".to_string()))?;
            tx.execute(
                "INSERT INTO repo.repo_unit_template_elements (templateid, attrid, idx, alias)
                 VALUES ($1, $2, $3, $4)",
                &[&template_id, &field_attr_id, &position, &field_alias],
            )
            .map_err(|e| RepoError::Backend(format!("insert unit template element failed: {e}")))?;
        }

        tx.commit()
            .map_err(|e| RepoError::Backend(format!("commit upsert_unit_template failed: {e}")))?;
        Ok(())
    }

    fn get_tenant_info(&self, name_or_id: &str) -> RepoResult<Option<Value>> {
        let mut client = self.client()?;
        if let Ok(id) = name_or_id.parse::<i32>() {
            let row = client
                .query_opt(
                    "SELECT tenantid, name, description, created FROM repo.repo_tenant WHERE tenantid = $1",
                    &[&id],
                )
                .map_err(|e| RepoError::Backend(format!("get_tenant_info by id failed: {e}")))?;
            row.map(tenant_info_from_row).transpose()
        } else {
            let row = client
                .query_opt(
                    "SELECT tenantid, name, description, created FROM repo.repo_tenant WHERE lower(name) = lower($1)",
                    &[&name_or_id],
                )
                .map_err(|e| RepoError::Backend(format!("get_tenant_info by name failed: {e}")))?;
            row.map(tenant_info_from_row).transpose()
        }
    }

    fn health(&self) -> RepoResult<Value> {
        let mut client = self.client()?;
        let row = client
            .query_one("SELECT current_database(), current_user", &[])
            .map_err(|e| RepoError::Backend(format!("postgres health query failed: {e}")))?;
        let database: String = row.get(0);
        let user: String = row.get(1);

        Ok(json!({
            "status": "ok",
            "backend": "postgres",
            "database": database,
            "user": user,
            "implemented": [
                "get_unit_json", "unit_exists", "store_unit_json", "search_units", "set_status",
                "add_relation", "remove_relation", "get_right_relation", "get_right_relations",
                "get_left_relations", "count_right_relations", "count_left_relations",
                "add_association", "remove_association", "get_right_association",
                "get_right_associations", "get_left_associations",
                "count_right_associations", "count_left_associations",
                "lock_unit", "unlock_unit", "is_unit_locked",
                "create_attribute", "instantiate_attribute", "can_change_attribute",
                "get_attribute_info", "get_tenant_info",
                "upsert_record_template", "upsert_unit_template"
            ]
        }))
    }
}

#[derive(Clone)]
enum SqlParam {
    I32(i32),
    I64(i64),
    F64(f64),
    Bool(bool),
    String(String),
    Uuid(Uuid),
    Timestamp(NaiveDateTime),
}

fn push_param(params: &mut Vec<SqlParam>, value: SqlParam) -> String {
    params.push(value);
    format!("${}", params.len())
}

fn params_as_refs(params: &[SqlParam]) -> Vec<&(dyn ToSql + Sync)> {
    params
        .iter()
        .map(|param| match param {
            SqlParam::I32(v) => v as &(dyn ToSql + Sync),
            SqlParam::I64(v) => v as &(dyn ToSql + Sync),
            SqlParam::F64(v) => v as &(dyn ToSql + Sync),
            SqlParam::Bool(v) => v as &(dyn ToSql + Sync),
            SqlParam::String(v) => v as &(dyn ToSql + Sync),
            SqlParam::Uuid(v) => v as &(dyn ToSql + Sync),
            SqlParam::Timestamp(v) => v as &(dyn ToSql + Sync),
        })
        .collect()
}

type SearchExpr = BoolExpr<LeafConstraint>;

#[derive(Clone, Default)]
struct LeafConstraint {
    tenant_id: Option<i32>,
    unit_id: Option<i64>,
    unit_ver: Option<i32>,
    status: Option<i32>,
    name: Option<String>,
    name_ilike: Option<String>,
    corrid: Option<Uuid>,
    created_after: Option<NaiveDateTime>,
    created_before: Option<NaiveDateTime>,
    modified_after: Option<NaiveDateTime>,
    modified_before: Option<NaiveDateTime>,
    relation: RelationConstraint,
    association: AssociationConstraint,
    attribute_cmp: AttributeCmpConstraint,
    predicates: Vec<UnitPredicate>,
}

impl LeafConstraint {
    fn is_empty(&self) -> bool {
        self.tenant_id.is_none()
            && self.unit_id.is_none()
            && self.unit_ver.is_none()
            && self.status.is_none()
            && self.name.is_none()
            && self.name_ilike.is_none()
            && self.corrid.is_none()
            && self.created_after.is_none()
            && self.created_before.is_none()
            && self.modified_after.is_none()
            && self.modified_before.is_none()
            && self.relation.relation_type.is_none()
            && self.relation.related_tenantid.is_none()
            && self.relation.related_unitid.is_none()
            && self.association.association_type.is_none()
            && self.association.association_reference.is_none()
            && self.attribute_cmp.op.is_none()
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
                tenant_id: Some(as_i32(tenant_id, "tenantid")?),
                ..LeafConstraint::default()
            })
        },
        "postgres native not-expression search requires tenant scope",
    )
}

fn parse_leaf_constraint(expr: &Map<String, Value>) -> RepoResult<Option<LeafConstraint>> {
    let tenant_id = if expr.contains_key("tenantid") {
        extract_i32(expr, "tenantid")?
    } else {
        extract_i32(expr, "tenant_id")?
    };
    let unit_id = if expr.contains_key("unitid") {
        extract_i64(expr, "unitid")?
    } else {
        extract_i64(expr, "unit_id")?
    };
    let unit_ver = if expr.contains_key("unitver") {
        extract_i32(expr, "unitver")?
    } else {
        extract_i32(expr, "unit_ver")?
    };
    let status = extract_status_i32(expr, "status")?;
    let mut name = extract_string(expr, "name")
        .or_else(|| extract_string(expr, "unitname"))
        .or_else(|| extract_string(expr, "unit_name"));
    let mut name_ilike = extract_string(expr, "name_ilike");
    if name_ilike.is_none() {
        if let Some(raw_name) = name.as_ref() {
            if has_name_wildcard(raw_name) {
                name_ilike = Some(raw_name.replace('*', "%"));
                name = None;
            }
        }
    }
    let corrid = if expr.contains_key("corrid") {
        extract_uuid(expr, "corrid")?
    } else if expr.contains_key("corr_id") {
        extract_uuid(expr, "corr_id")?
    } else {
        extract_uuid(expr, "correlationid")?
    };
    let created_after = extract_timestamp(expr, "created_after")?;
    let created_before = extract_timestamp(expr, "created_before")?;
    let modified_after = extract_timestamp(expr, "modified_after")?;
    let modified_before = extract_timestamp(expr, "modified_before")?;
    let relation = extract_relation_constraint(expr)?;
    let association = extract_association_constraint(expr)?;
    let attribute_cmp = extract_attribute_constraint(expr)?;
    let predicates = extract_unit_predicates(expr, true)?;

    let leaf = LeafConstraint {
        tenant_id,
        unit_id,
        unit_ver,
        status,
        name,
        name_ilike,
        corrid,
        created_after,
        created_before,
        modified_after,
        modified_before,
        relation,
        association,
        attribute_cmp,
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
    params: &mut Vec<SqlParam>,
) -> RepoResult<Option<String>> {
    match expr {
        SearchExpr::True => Ok(None),
        SearchExpr::False => Ok(Some("1 = 0".to_string())),
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
                Ok(Some("1 = 0".to_string()))
            } else {
                Ok(Some(parts.join(" OR ")))
            }
        }
        SearchExpr::Not(child) => match compile_search_expression(child, params)? {
            Some(sql) => Ok(Some(format!("NOT ({sql})"))),
            None => Ok(Some("1 = 0".to_string())),
        },
    }
}

fn has_name_wildcard(value: &str) -> bool {
    value.contains('*') || value.contains('%') || value.contains('_')
}

fn compile_leaf_constraint(
    leaf: &LeafConstraint,
    params: &mut Vec<SqlParam>,
) -> RepoResult<String> {
    let mut clauses = Vec::new();

    if let Some(tenant_id) = leaf.tenant_id {
        let ph = push_param(params, SqlParam::I32(tenant_id));
        clauses.push(format!("uk.tenantid = {ph}"));
    }
    if let Some(unit_id) = leaf.unit_id {
        let ph = push_param(params, SqlParam::I64(unit_id));
        clauses.push(format!("uk.unitid = {ph}"));
    }
    if let Some(unit_ver) = leaf.unit_ver {
        let ph = push_param(params, SqlParam::I32(unit_ver));
        clauses.push(format!("uv.unitver = {ph}"));
    }
    if let Some(status) = leaf.status {
        let ph = push_param(params, SqlParam::I32(status));
        clauses.push(format!("uk.status = {ph}"));
    }
    if let Some(name) = leaf.name.as_ref() {
        let ph = push_param(params, SqlParam::String(name.clone()));
        clauses.push(format!("uv.unitname = {ph}"));
    }
    if let Some(name_ilike) = leaf.name_ilike.as_ref() {
        let ph = push_param(params, SqlParam::String(name_ilike.clone()));
        clauses.push(format!("uv.unitname ILIKE {ph}"));
    }
    if let Some(created_after) = leaf.created_after {
        let ph = push_param(params, SqlParam::Timestamp(created_after));
        clauses.push(format!("uk.created >= {ph}"));
    }
    if let Some(created_before) = leaf.created_before {
        let ph = push_param(params, SqlParam::Timestamp(created_before));
        clauses.push(format!("uk.created < {ph}"));
    }
    if let Some(corrid) = leaf.corrid {
        let ph = push_param(params, SqlParam::Uuid(corrid));
        clauses.push(format!("uk.corrid = {ph}"));
    }
    if let Some(modified_after) = leaf.modified_after {
        let ph = push_param(params, SqlParam::Timestamp(modified_after));
        clauses.push(format!("uv.modified >= {ph}"));
    }
    if let Some(modified_before) = leaf.modified_before {
        let ph = push_param(params, SqlParam::Timestamp(modified_before));
        clauses.push(format!("uv.modified < {ph}"));
    }

    if leaf.relation.relation_type.is_some()
        || leaf.relation.related_tenantid.is_some()
        || leaf.relation.related_unitid.is_some()
    {
        let side = leaf.relation.side.unwrap_or(RelationSide::Left);
        let mut relation = match side {
            RelationSide::Left => vec![
                "rir.tenantid = uk.tenantid".to_string(),
                "rir.unitid = uk.unitid".to_string(),
            ],
            RelationSide::Right => vec![
                "rir.reltenantid = uk.tenantid".to_string(),
                "rir.relunitid = uk.unitid".to_string(),
            ],
        };
        if let Some(relation_type) = leaf.relation.relation_type {
            let ph = push_param(
                params,
                SqlParam::I32(as_i32(relation_type, "relation.relation_type")?),
            );
            relation.push(format!("rir.reltype = {ph}"));
        }
        if let Some(related_tenantid) = leaf.relation.related_tenantid {
            let ph = push_param(
                params,
                SqlParam::I32(as_i32(related_tenantid, "relation.related_tenantid")?),
            );
            match side {
                RelationSide::Left => relation.push(format!("rir.reltenantid = {ph}")),
                RelationSide::Right => relation.push(format!("rir.tenantid = {ph}")),
            }
        }
        if let Some(related_unitid) = leaf.relation.related_unitid {
            let ph = push_param(params, SqlParam::I64(related_unitid));
            match side {
                RelationSide::Left => relation.push(format!("rir.relunitid = {ph}")),
                RelationSide::Right => relation.push(format!("rir.unitid = {ph}")),
            }
        }
        clauses.push(format!(
            "EXISTS (SELECT 1 FROM repo.repo_internal_relation rir WHERE {})",
            relation.join(" AND ")
        ));
    }

    if leaf.association.association_type.is_some()
        || leaf.association.association_reference.is_some()
    {
        let _association_side = leaf.association.side.unwrap_or(RelationSide::Left);
        let mut association = vec![
            "rea.tenantid = uk.tenantid".to_string(),
            "rea.unitid = uk.unitid".to_string(),
        ];
        if let Some(association_type) = leaf.association.association_type {
            let ph = push_param(
                params,
                SqlParam::I32(as_i32(association_type, "association.association_type")?),
            );
            association.push(format!("rea.assoctype = {ph}"));
        }
        if let Some(reference) = leaf.association.association_reference.as_ref() {
            let ph = push_param(params, SqlParam::String(reference.clone()));
            association.push(format!("rea.assocstring = {ph}"));
        }
        clauses.push(format!(
            "EXISTS (SELECT 1 FROM repo.repo_external_assoc rea WHERE {})",
            association.join(" AND ")
        ));
    }

    if leaf.attribute_cmp.op.is_some() {
        clauses.push(compile_attribute_constraint(&leaf.attribute_cmp, params)?);
    }
    for predicate in &leaf.predicates {
        clauses.push(compile_unit_predicate(predicate, params)?);
    }

    if clauses.is_empty() {
        Ok("1 = 1".to_string())
    } else {
        Ok(clauses.join(" AND "))
    }
}

fn compile_unit_predicate(
    predicate: &UnitPredicate,
    params: &mut Vec<SqlParam>,
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
            let ph = push_param(params, SqlParam::I32(as_i32(*value, "predicates.value")?));
            format!("uk.tenantid {op} {ph}")
        }
        (UnitField::UnitId, UnitValue::I64(value)) => {
            let ph = push_param(params, SqlParam::I64(*value));
            format!("uk.unitid {op} {ph}")
        }
        (UnitField::UnitVer, UnitValue::I64(value)) => {
            let ph = push_param(params, SqlParam::I32(as_i32(*value, "predicates.value")?));
            format!("uv.unitver {op} {ph}")
        }
        (UnitField::Status, UnitValue::I64(value)) => {
            let ph = push_param(params, SqlParam::I32(as_i32(*value, "predicates.value")?));
            format!("uk.status {op} {ph}")
        }
        (UnitField::Name, UnitValue::String(value)) => {
            let ph = push_param(params, SqlParam::String(value.clone()));
            if matches!(predicate.op, UnitOp::Like) {
                format!("lower(uv.unitname) LIKE lower({ph})")
            } else {
                format!("uv.unitname {op} {ph}")
            }
        }
        (UnitField::Corrid, UnitValue::String(value)) => {
            let corrid = Uuid::parse_str(value).map_err(|e| {
                RepoError::InvalidInput(format!("predicates corrid must be UUID string: {e}"))
            })?;
            let ph = push_param(params, SqlParam::Uuid(corrid));
            format!("uk.corrid {op} {ph}")
        }
        (UnitField::Created, UnitValue::TimestampMillis(value)) => {
            let ph = push_param(params, SqlParam::Timestamp(millis_to_naive(*value)?));
            format!("uk.created {op} {ph}")
        }
        (UnitField::Modified, UnitValue::TimestampMillis(value)) => {
            let ph = push_param(params, SqlParam::Timestamp(millis_to_naive(*value)?));
            format!("uv.modified {op} {ph}")
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
    params: &mut Vec<SqlParam>,
) -> RepoResult<String> {
    let mut where_parts = vec![
        "av.tenantid = uk.tenantid".to_string(),
        "av.unitid = uk.unitid".to_string(),
        "uk.lastver BETWEEN av.unitverfrom AND av.unitverto".to_string(),
    ];

    if let Some(attr_id) = attribute_cmp.attr_id {
        let ph = push_param(
            params,
            SqlParam::I32(as_i32(attr_id, "attribute_cmp.attrid")?),
        );
        where_parts.push(format!("av.attrid = {ph}"));
    } else if let Some(attr_name) = attribute_cmp.attr_name.as_ref() {
        let ph = push_param(params, SqlParam::String(attr_name.clone()));
        where_parts.push(format!(
            "(a.attrname = {ph} OR a.alias = {ph} OR a.qualname = {ph})"
        ));
    } else {
        return Err(RepoError::InvalidInput(
            "attribute_cmp requires attrid/name selector".to_string(),
        ));
    }

    let op = attribute_cmp.op.as_deref().unwrap_or("eq");
    let value_type = attribute_cmp.value_type.as_deref().unwrap_or("string");
    let cmp = match value_type {
        "string" => {
            let value = attribute_cmp.value_text.clone().ok_or_else(|| {
                RepoError::InvalidInput("attribute_cmp string comparison missing value".to_string())
            })?;
            let ph = push_param(params, SqlParam::String(value));
            match op {
                "like" => format!("lower(sv.value) LIKE {ph}"),
                "eq" if attribute_cmp.value_wildcard == Some(true) => {
                    format!("lower(sv.value) LIKE {ph}")
                }
                "eq" => format!("lower(sv.value) = {ph}"),
                "neq" if attribute_cmp.value_wildcard == Some(true) => {
                    format!("lower(sv.value) NOT LIKE {ph}")
                }
                "neq" => format!("lower(sv.value) <> {ph}"),
                "gt" => format!("lower(sv.value) > {ph}"),
                "gte" => format!("lower(sv.value) >= {ph}"),
                "lt" => format!("lower(sv.value) < {ph}"),
                "lte" => format!("lower(sv.value) <= {ph}"),
                other => {
                    return Err(RepoError::InvalidInput(format!(
                        "unsupported string comparison operator: {other}"
                    )))
                }
            }
        }
        "number" => {
            let value = attribute_cmp.value_number.ok_or_else(|| {
                RepoError::InvalidInput("attribute_cmp number comparison missing value".to_string())
            })?;
            let ph = push_param(params, SqlParam::F64(value));
            match op {
                "eq" => format!(
                    "(iv.value::double precision = {ph} OR lv.value::double precision = {ph} OR dv.value = {ph})"
                ),
                "neq" => format!(
                    "(iv.value::double precision <> {ph} OR lv.value::double precision <> {ph} OR dv.value <> {ph})"
                ),
                "gt" => format!(
                    "(iv.value::double precision > {ph} OR lv.value::double precision > {ph} OR dv.value > {ph})"
                ),
                "gte" => format!(
                    "(iv.value::double precision >= {ph} OR lv.value::double precision >= {ph} OR dv.value >= {ph})"
                ),
                "lt" => format!(
                    "(iv.value::double precision < {ph} OR lv.value::double precision < {ph} OR dv.value < {ph})"
                ),
                "lte" => format!(
                    "(iv.value::double precision <= {ph} OR lv.value::double precision <= {ph} OR dv.value <= {ph})"
                ),
                other => {
                    return Err(RepoError::InvalidInput(format!(
                        "unsupported number comparison operator: {other}"
                    )))
                }
            }
        }
        "boolean" => {
            let value = attribute_cmp.value_bool.ok_or_else(|| {
                RepoError::InvalidInput(
                    "attribute_cmp boolean comparison missing value".to_string(),
                )
            })?;
            if !matches!(op, "eq" | "neq") {
                return Err(RepoError::InvalidInput(
                    "attribute_cmp boolean comparisons support only eq|neq".to_string(),
                ));
            }
            let ph = push_param(params, SqlParam::Bool(value));
            if op == "eq" {
                format!("bv.value = {ph}")
            } else {
                format!("bv.value <> {ph}")
            }
        }
        "time" => {
            let value_millis = attribute_cmp.value_time_millis.ok_or_else(|| {
                RepoError::InvalidInput("attribute_cmp time comparison missing value".to_string())
            })?;
            let value = millis_to_naive(value_millis)?;
            let ph = push_param(params, SqlParam::Timestamp(value));
            match op {
                "eq" => format!("tv.value = {ph}"),
                "neq" => format!("tv.value <> {ph}"),
                "gt" => format!("tv.value > {ph}"),
                "gte" => format!("tv.value >= {ph}"),
                "lt" => format!("tv.value < {ph}"),
                "lte" => format!("tv.value <= {ph}"),
                other => {
                    return Err(RepoError::InvalidInput(format!(
                        "unsupported time comparison operator: {other}"
                    )))
                }
            }
        }
        other => {
            return Err(RepoError::InvalidInput(format!(
                "unsupported attribute value_type: {other}"
            )))
        }
    };
    where_parts.push(cmp);

    Ok(format!(
        "EXISTS (\
         SELECT 1 \
         FROM repo.repo_attribute_value av \
         JOIN repo.repo_attribute a ON a.attrid = av.attrid \
         LEFT JOIN repo.repo_string_vector sv ON sv.valueid = av.valueid \
         LEFT JOIN repo.repo_time_vector tv ON tv.valueid = av.valueid \
         LEFT JOIN repo.repo_integer_vector iv ON iv.valueid = av.valueid \
         LEFT JOIN repo.repo_long_vector lv ON lv.valueid = av.valueid \
         LEFT JOIN repo.repo_double_vector dv ON dv.valueid = av.valueid \
         LEFT JOIN repo.repo_boolean_vector bv ON bv.valueid = av.valueid \
         WHERE {}\
         )",
        where_parts.join(" AND ")
    ))
}

fn as_object(value: Value) -> RepoResult<Map<String, Value>> {
    match value {
        Value::Object(obj) => Ok(obj),
        _ => Err(RepoError::InvalidInput(
            "unit payload must be a JSON object".to_string(),
        )),
    }
}

fn as_i32(value: i64, field_name: &str) -> RepoResult<i32> {
    i32::try_from(value)
        .map_err(|_| RepoError::InvalidInput(format!("{field_name} does not fit in i32")))
}

fn extract_i32(expr: &Map<String, Value>, key: &str) -> RepoResult<Option<i32>> {
    match expr.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(n)) => n
            .as_i64()
            .ok_or_else(|| RepoError::InvalidInput(format!("{key} must be an integer")))
            .and_then(|v| as_i32(v, key).map(Some)),
        Some(Value::String(s)) => s
            .parse::<i64>()
            .map_err(|_| RepoError::InvalidInput(format!("{key} must be an integer")))
            .and_then(|v| as_i32(v, key).map(Some)),
        Some(other) => Err(RepoError::InvalidInput(format!(
            "{key} must be numeric/string, got {other}"
        ))),
    }
}

fn extract_status_i32(expr: &Map<String, Value>, key: &str) -> RepoResult<Option<i32>> {
    match expr.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(raw)) => {
            let status = parse_status_name(raw, key)?;
            as_i32(status, key).map(Some)
        }
        Some(_) => extract_i32(expr, key),
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

fn extract_i64(expr: &Map<String, Value>, key: &str) -> RepoResult<Option<i64>> {
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

fn extract_string(expr: &Map<String, Value>, key: &str) -> Option<String> {
    expr.get(key)
        .and_then(Value::as_str)
        .map(ToString::to_string)
}

fn extract_uuid(expr: &Map<String, Value>, key: &str) -> RepoResult<Option<Uuid>> {
    match expr.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(s)) => Uuid::parse_str(s)
            .map(Some)
            .map_err(|e| RepoError::InvalidInput(format!("{key} must be UUID string: {e}"))),
        Some(other) => Err(RepoError::InvalidInput(format!(
            "{key} must be UUID string, got {other}"
        ))),
    }
}

fn extract_timestamp(expr: &Map<String, Value>, key: &str) -> RepoResult<Option<NaiveDateTime>> {
    match expr.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(n)) => {
            let millis = n
                .as_i64()
                .ok_or_else(|| RepoError::InvalidInput(format!("{key} must be integer millis")))?;
            DateTime::<Utc>::from_timestamp_millis(millis)
                .map(|dt| Some(dt.naive_utc()))
                .ok_or_else(|| {
                    RepoError::InvalidInput(format!(
                        "{key} has out-of-range millis value: {millis}"
                    ))
                })
        }
        Some(Value::String(s)) => {
            if let Ok(millis) = s.parse::<i64>() {
                return DateTime::<Utc>::from_timestamp_millis(millis)
                    .map(|dt| Some(dt.naive_utc()))
                    .ok_or_else(|| {
                        RepoError::InvalidInput(format!(
                            "{key} has out-of-range millis value: {millis}"
                        ))
                    });
            }
            DateTime::parse_from_rfc3339(s)
                .map(|dt| Some(dt.naive_utc()))
                .map_err(|_| {
                    RepoError::InvalidInput(format!(
                        "{key} must be RFC3339 timestamp or millis integer"
                    ))
                })
        }
        Some(other) => Err(RepoError::InvalidInput(format!(
            "{key} must be timestamp string/millis, got {other}"
        ))),
    }
}

fn millis_to_naive(millis: i64) -> RepoResult<NaiveDateTime> {
    DateTime::<Utc>::from_timestamp_millis(millis)
        .map(|dt| dt.naive_utc())
        .ok_or_else(|| {
            RepoError::InvalidInput(format!("time value has out-of-range millis: {millis}"))
        })
}

fn order_sql(order: &SearchOrder) -> (&'static str, &'static str) {
    let field = match order.field.as_str() {
        "created" => "uk.created",
        "modified" => "uv.modified",
        "unitid" => "uk.unitid",
        "status" => "uk.status",
        _ => "uk.created",
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

fn row_to_unit(row: Row) -> RepoResult<Unit> {
    let tenant_id: i32 = row.get(0);
    let unit_id: i64 = row.get(1);
    let unit_ver: i32 = row.get(2);
    let corr_id: Uuid = row.get(3);
    let status: i32 = row.get(4);
    let created: NaiveDateTime = row.get(5);
    let modified: NaiveDateTime = row.get(6);
    let unit_name: Option<String> = row.get(7);

    Ok(Unit {
        tenant_id: i64::from(tenant_id),
        unit_id,
        unit_ver: i64::from(unit_ver),
        status,
        name: unit_name.clone(),
        corr_id,
        payload: json!({
            "tenantid": tenant_id,
            "unitid": unit_id,
            "unitver": unit_ver,
            "status": status,
            "corrid": corr_id,
            "unitname": unit_name,
            "created": format_pg_timestamp(created),
            "modified": format_pg_timestamp(modified)
        }),
    })
}

fn relation_from_row(row: Row) -> RepoResult<Relation> {
    let tenant_id: i32 = row.get(0);
    let unit_id: i64 = row.get(1);
    let relation_type: i32 = row.get(2);
    let related_tenant_id: i32 = row.get(3);
    let related_unit_id: i64 = row.get(4);

    Ok(Relation {
        tenant_id: i64::from(tenant_id),
        unit_id,
        relation_type,
        related_tenant_id: i64::from(related_tenant_id),
        related_unit_id,
    })
}

fn association_from_row(row: Row) -> RepoResult<Association> {
    let tenant_id: i32 = row.get(0);
    let unit_id: i64 = row.get(1);
    let association_type: i32 = row.get(2);
    let reference: String = row.get(3);

    Ok(Association {
        tenant_id: i64::from(tenant_id),
        unit_id,
        association_type,
        reference,
    })
}

fn parse_attr_type(attribute_type: &str) -> RepoResult<i32> {
    if let Ok(value) = attribute_type.parse::<i32>() {
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

fn attribute_info_from_row(row: Row) -> RepoResult<Value> {
    let id: i32 = row.get(0);
    let alias: Option<String> = row.get(1);
    let name: String = row.get(2);
    let qualname: String = row.get(3);
    let attr_type: i32 = row.get(4);
    let scalar: bool = row.get(5);

    Ok(json!({
        "id": id,
        "alias": alias,
        "name": name,
        "qualname": qualname,
        "type": attr_type,
        "forced_scalar": scalar
    }))
}

fn tenant_info_from_row(row: Row) -> RepoResult<Value> {
    let id: i32 = row.get(0);
    let name: String = row.get(1);
    let description: Option<String> = row.get(2);
    let created: NaiveDateTime = row.get(3);

    Ok(json!({
        "id": id,
        "name": name,
        "description": description,
        "created": format_pg_timestamp(created)
    }))
}

fn format_pg_timestamp(value: NaiveDateTime) -> String {
    format!("{}Z", value.format("%Y-%m-%dT%H:%M:%S"))
}

fn normalize_payload_timestamps(payload: Value) -> Value {
    let mut obj = match payload {
        Value::Object(obj) => obj,
        other => return other,
    };
    for key in ["created", "modified"] {
        if let Some(value) = obj.get_mut(key) {
            normalize_timestamp_value(value);
        }
    }
    Value::Object(obj)
}

fn normalize_timestamp_value(value: &mut Value) {
    let Some(ts) = value.as_str() else {
        return;
    };
    if ts.ends_with('Z') || ts.contains('+') {
        return;
    }
    *value = Value::String(format!("{ts}Z"));
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parse_leaf_name_wildcard_promotes_to_name_ilike() {
        let expr = json!({
            "tenantid": 1,
            "name": "alpha*"
        });
        let obj = expr.as_object().expect("object");
        let leaf = parse_leaf_constraint(obj)
            .expect("parse leaf")
            .expect("leaf exists");
        assert!(leaf.name.is_none());
        assert_eq!(leaf.name_ilike.as_deref(), Some("alpha%"));
    }
}
