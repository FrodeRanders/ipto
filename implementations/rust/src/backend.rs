//! Backend abstraction for IPTO persistence.
//!
//! [`Backend`] is the narrow storage contract behind [`crate::repo::RepoService`].
//! Implementations are responsible for translating the repository model to a
//! concrete database while preserving the same observable behavior. The service
//! layer deliberately owns cross-backend policy, such as update/version rules,
//! SDL validation, and fallback search composition.

use crate::model::{
    Association, Relation, SearchOrder, SearchPaging, SearchResult, Unit, UnitRef, VersionSelector,
};
use crate::search_ast::SearchExpr;
use serde_json::Value;
use thiserror::Error;

/// Standard result type used throughout the Rust implementation.
pub type RepoResult<T> = Result<T, RepoError>;

/// Error type shared by the service layer, backends, parsers, and bindings.
///
/// Variants separate domain outcomes (`NotFound`, `AlreadyLocked`,
/// `InvalidInput`) from storage/runtime failures (`Backend`) and explicit
/// capability gaps (`Unsupported`).
#[derive(Debug, Error)]
pub enum RepoError {
    /// A requested unit, attribute, tenant, or relation was not found.
    #[error("not found")]
    NotFound,
    /// A write operation was rejected because the unit is locked.
    #[error("already locked")]
    AlreadyLocked,
    /// Caller supplied malformed or semantically invalid input.
    #[error("invalid input: {0}")]
    InvalidInput(String),
    /// Backend or transport failure.
    #[error("backend error: {0}")]
    Backend(String),
    /// Operation is valid in the service contract but not implemented by this backend.
    #[error("unsupported operation: {0}")]
    Unsupported(String),
}

/// Storage contract implemented by concrete repository backends.
///
/// The trait accepts `serde_json::Value` for unit payloads and metadata because
/// the IPTO schema is dynamic and the Rust implementation must interoperate with
/// existing Java/GraphQL payload shapes. Methods should return normalized JSON
/// envelopes with the common keys used by [`crate::repo::RepoService`]:
/// `tenantid`, `unitid`, `unitver`, `status`, `corrid`, `created`, `modified`,
/// `unitname`, and `isreadonly` where applicable.
///
/// Relation direction is always `left -> right`. For example, a parent/child
/// relation stores the parent directory on the left and the child file or
/// directory on the right. "Right" relation methods start from the left unit and
/// follow outgoing links, which is the directory-contents view. "Left" relation
/// methods start from the right unit and find incoming links, which is the
/// "where is this file located?" view.
///
/// Associations use the same language even though the right side is a string:
/// the unit is left, the external reference is right. Right-association methods
/// list references attached to a unit; left-association methods locate units by
/// a reference.
pub trait Backend: Send + Sync {
    /// Load a unit by tenant/id using either the latest or an exact version.
    fn get_unit_json(
        &self,
        tenant_id: i64,
        unit_id: i64,
        selector: VersionSelector,
    ) -> RepoResult<Option<Value>>;
    /// Load the latest unit version by correlation id.
    fn get_unit_by_corrid_json(&self, corrid: &str) -> RepoResult<Option<Value>>;
    /// Return whether a unit kernel exists.
    fn unit_exists(&self, tenant_id: i64, unit_id: i64) -> RepoResult<bool>;
    /// Create a new unit or append a new version to an existing unit.
    fn store_unit_json(&self, unit: Value) -> RepoResult<Value>;
    /// Search latest unit versions using the normalized JSON expression format.
    fn search_units(
        &self,
        expression: Value,
        order: SearchOrder,
        paging: SearchPaging,
    ) -> RepoResult<SearchResult>;

    /// Add a directed relation from `left` to `right`.
    ///
    /// In a parent/child relation, `left` is the parent directory and `right` is
    /// the child entry.
    fn add_relation(&self, left: UnitRef, relation_type: i32, right: UnitRef) -> RepoResult<()>;
    /// Remove a directed relation from `left` to `right`.
    fn remove_relation(&self, left: UnitRef, relation_type: i32, right: UnitRef) -> RepoResult<()>;
    /// Return one outgoing/right-side relation for the unit and type.
    ///
    /// For parent/child, call this on a parent to get one child.
    fn get_right_relation(&self, unit: UnitRef, relation_type: i32)
    -> RepoResult<Option<Relation>>;
    /// Return all outgoing/right-side relations for the unit and type.
    ///
    /// For parent/child, call this on a directory to list its children.
    fn get_right_relations(&self, unit: UnitRef, relation_type: i32) -> RepoResult<Vec<Relation>>;
    /// Return all incoming/left-side relations pointing at the unit for the type.
    ///
    /// For parent/child, call this on a file or child directory to locate its
    /// parent directories.
    fn get_left_relations(&self, unit: UnitRef, relation_type: i32) -> RepoResult<Vec<Relation>>;
    /// Count outgoing/right-side relations for the unit and type.
    fn count_right_relations(&self, unit: UnitRef, relation_type: i32) -> RepoResult<i64>;
    /// Count incoming/left-side relations pointing at the unit for the type.
    fn count_left_relations(&self, unit: UnitRef, relation_type: i32) -> RepoResult<i64>;

    /// Add an association from a left-side unit to a right-side external reference.
    fn add_association(
        &self,
        unit: UnitRef,
        association_type: i32,
        reference: &str,
    ) -> RepoResult<()>;
    /// Remove an association from a left-side unit to a right-side external reference.
    fn remove_association(
        &self,
        unit: UnitRef,
        association_type: i32,
        reference: &str,
    ) -> RepoResult<()>;
    /// Return one right-side external reference from the unit for the type.
    fn get_right_association(
        &self,
        unit: UnitRef,
        association_type: i32,
    ) -> RepoResult<Option<Association>>;
    /// Return all right-side external references from the unit for the type.
    fn get_right_associations(
        &self,
        unit: UnitRef,
        association_type: i32,
    ) -> RepoResult<Vec<Association>>;
    /// Return all left-side units associated with the external reference for the type.
    fn get_left_associations(
        &self,
        association_type: i32,
        reference: &str,
    ) -> RepoResult<Vec<Association>>;
    /// Count right-side external references from the unit for the type.
    fn count_right_associations(&self, unit: UnitRef, association_type: i32) -> RepoResult<i64>;
    /// Count left-side units associated with the external reference for the type.
    fn count_left_associations(&self, association_type: i32, reference: &str) -> RepoResult<i64>;

    /// Lock a unit for a purpose.
    fn lock_unit(&self, unit: UnitRef, lock_type: i32, purpose: &str) -> RepoResult<()>;
    /// Remove any lock from a unit.
    fn unlock_unit(&self, unit: UnitRef) -> RepoResult<()>;
    /// Return whether the unit is currently locked.
    fn is_unit_locked(&self, unit: UnitRef) -> RepoResult<bool>;
    /// Set the lifecycle status directly.
    fn set_status(&self, unit: UnitRef, status: i32) -> RepoResult<()>;

    /// Create an attribute metadata record.
    fn create_attribute(
        &self,
        alias: &str,
        name: &str,
        qualname: &str,
        attribute_type: &str,
        is_array: bool,
    ) -> RepoResult<Value>;

    /// Instantiate an attribute by name or id, returning backend-specific metadata.
    fn instantiate_attribute(&self, name_or_id: &str) -> RepoResult<Option<Value>>;
    /// Return whether an attribute can still be changed without breaking usage.
    fn can_change_attribute(&self, name_or_id: &str) -> RepoResult<bool>;

    /// Look up attribute metadata by name, alias, qualified name, or id.
    fn get_attribute_info(&self, name_or_id: &str) -> RepoResult<Option<Value>>;
    /// Look up tenant metadata by name or id.
    fn get_tenant_info(&self, name_or_id: &str) -> RepoResult<Option<Value>>;
    /// Persist a record-template definition when the backend supports it.
    fn upsert_record_template(
        &self,
        _record_attribute_id: i64,
        _record_type_name: &str,
        _fields: &[(i64, String)],
    ) -> RepoResult<()> {
        Err(RepoError::Unsupported("upsert_record_template".to_string()))
    }
    /// Persist a unit-template definition when the backend supports it.
    fn upsert_unit_template(
        &self,
        _template_name: &str,
        _fields: &[(i64, String)],
    ) -> RepoResult<()> {
        Err(RepoError::Unsupported("upsert_unit_template".to_string()))
    }
    /// Clear backend-local caches.
    fn flush_cache(&self) -> RepoResult<()> {
        Ok(())
    }

    /// Search latest unit versions using a typed search AST.
    ///
    /// Default implementation converts the AST to the legacy JSON format and
    /// delegates to [`Backend::search_units`]. Backends with native AST
    /// support override this for better type safety and SQL generation.
    fn search_units_ast(
        &self,
        expr: &SearchExpr,
        order: SearchOrder,
        paging: SearchPaging,
    ) -> RepoResult<SearchResult> {
        let expression = crate::search_query::search_expr_to_json(expr);
        self.search_units(expression, order, paging)
    }

    /// Return a lightweight health payload.
    fn health(&self) -> RepoResult<Value> {
        Ok(serde_json::json!({"status": "ok"}))
    }

    /// Decode a raw JSON unit into the typed [`Unit`] envelope.
    fn parse_unit(&self, raw: Value) -> RepoResult<Unit> {
        serde_json::from_value(raw)
            .map_err(|e| RepoError::Backend(format!("invalid unit payload: {e}")))
    }
}
