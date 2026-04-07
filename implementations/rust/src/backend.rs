use crate::model::{
    Association, Relation, SearchOrder, SearchPaging, SearchResult, Unit, UnitRef, VersionSelector,
};
use serde_json::Value;
use thiserror::Error;

pub type RepoResult<T> = Result<T, RepoError>;

#[derive(Debug, Error)]
pub enum RepoError {
    #[error("not found")]
    NotFound,
    #[error("already locked")]
    AlreadyLocked,
    #[error("invalid input: {0}")]
    InvalidInput(String),
    #[error("backend error: {0}")]
    Backend(String),
    #[error("unsupported operation: {0}")]
    Unsupported(String),
}

pub trait Backend: Send + Sync {
    fn get_unit_json(
        &self,
        tenant_id: i64,
        unit_id: i64,
        selector: VersionSelector,
    ) -> RepoResult<Option<Value>>;
    fn get_unit_by_corrid_json(&self, corrid: &str) -> RepoResult<Option<Value>>;
    fn unit_exists(&self, tenant_id: i64, unit_id: i64) -> RepoResult<bool>;
    fn store_unit_json(&self, unit: Value) -> RepoResult<Value>;
    fn search_units(
        &self,
        expression: Value,
        order: SearchOrder,
        paging: SearchPaging,
    ) -> RepoResult<SearchResult>;

    fn add_relation(&self, left: UnitRef, relation_type: i32, right: UnitRef) -> RepoResult<()>;
    fn remove_relation(&self, left: UnitRef, relation_type: i32, right: UnitRef) -> RepoResult<()>;
    fn get_right_relation(&self, unit: UnitRef, relation_type: i32)
        -> RepoResult<Option<Relation>>;
    fn get_right_relations(&self, unit: UnitRef, relation_type: i32) -> RepoResult<Vec<Relation>>;
    fn get_left_relations(&self, unit: UnitRef, relation_type: i32) -> RepoResult<Vec<Relation>>;
    fn count_right_relations(&self, unit: UnitRef, relation_type: i32) -> RepoResult<i64>;
    fn count_left_relations(&self, unit: UnitRef, relation_type: i32) -> RepoResult<i64>;

    fn add_association(
        &self,
        unit: UnitRef,
        association_type: i32,
        reference: &str,
    ) -> RepoResult<()>;
    fn remove_association(
        &self,
        unit: UnitRef,
        association_type: i32,
        reference: &str,
    ) -> RepoResult<()>;
    fn get_right_association(
        &self,
        unit: UnitRef,
        association_type: i32,
    ) -> RepoResult<Option<Association>>;
    fn get_right_associations(
        &self,
        unit: UnitRef,
        association_type: i32,
    ) -> RepoResult<Vec<Association>>;
    fn get_left_associations(
        &self,
        association_type: i32,
        reference: &str,
    ) -> RepoResult<Vec<Association>>;
    fn count_right_associations(&self, unit: UnitRef, association_type: i32) -> RepoResult<i64>;
    fn count_left_associations(&self, association_type: i32, reference: &str) -> RepoResult<i64>;

    fn lock_unit(&self, unit: UnitRef, lock_type: i32, purpose: &str) -> RepoResult<()>;
    fn unlock_unit(&self, unit: UnitRef) -> RepoResult<()>;
    fn is_unit_locked(&self, unit: UnitRef) -> RepoResult<bool>;
    fn set_status(&self, unit: UnitRef, status: i32) -> RepoResult<()>;

    fn create_attribute(
        &self,
        alias: &str,
        name: &str,
        qualname: &str,
        attribute_type: &str,
        is_array: bool,
    ) -> RepoResult<Value>;

    fn instantiate_attribute(&self, name_or_id: &str) -> RepoResult<Option<Value>>;
    fn can_change_attribute(&self, name_or_id: &str) -> RepoResult<bool>;

    fn get_attribute_info(&self, name_or_id: &str) -> RepoResult<Option<Value>>;
    fn get_tenant_info(&self, name_or_id: &str) -> RepoResult<Option<Value>>;
    fn upsert_record_template(
        &self,
        _record_attribute_id: i64,
        _record_type_name: &str,
        _fields: &[(i64, String)],
    ) -> RepoResult<()> {
        Err(RepoError::Unsupported("upsert_record_template".to_string()))
    }
    fn upsert_unit_template(
        &self,
        _template_name: &str,
        _fields: &[(i64, String)],
    ) -> RepoResult<()> {
        Err(RepoError::Unsupported("upsert_unit_template".to_string()))
    }
    fn flush_cache(&self) -> RepoResult<()> {
        Ok(())
    }

    fn health(&self) -> RepoResult<Value> {
        Ok(serde_json::json!({"status": "ok"}))
    }

    fn parse_unit(&self, raw: Value) -> RepoResult<Unit> {
        serde_json::from_value(raw)
            .map_err(|e| RepoError::Backend(format!("invalid unit payload: {e}")))
    }
}
