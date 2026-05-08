//! Domain model shared by the repository service, backend adapters, tests, and
//! optional language bindings.
//!
//! The repository's unit payload is deliberately flexible, but these structs
//! capture the stable envelope: tenant/unit/version identity, lifecycle status,
//! correlation id, and graph-like links between units and external references.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

/// Version selector used when loading a unit.
///
/// `Latest` follows the unit kernel's current version pointer. `Exact` reads a
/// specific historical version and may therefore return a read-only unit
/// representation.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VersionSelector {
    /// Load the latest version currently attached to the unit kernel.
    Latest,
    /// Load one explicit version number.
    Exact(i64),
}

/// Stable unit identity used by relation, association, lock, and status APIs.
///
/// Most operations act on the latest version and leave `version` as `None`.
/// Version-aware callers can populate it when they need to preserve a precise
/// historical reference in an external protocol.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnitRef {
    /// Tenant namespace identifier.
    pub tenant_id: i64,
    /// Unit identifier within the tenant.
    pub unit_id: i64,
    /// Optional explicit unit version.
    pub version: Option<i64>,
}

/// Search result row for a repository unit.
///
/// The `payload` field contains the dynamic unit attributes exactly as returned
/// by the backend after repository envelope fields have been normalized.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Unit {
    /// Tenant namespace identifier.
    pub tenant_id: i64,
    /// Unit identifier within the tenant.
    pub unit_id: i64,
    /// Concrete unit version number.
    pub unit_ver: i64,
    /// Lifecycle status code.
    pub status: i32,
    /// Optional human-facing unit name.
    pub name: Option<String>,
    /// Correlation id used for idempotent/external lookup.
    pub corr_id: Uuid,
    /// Dynamic unit payload.
    pub payload: Value,
}

/// Sort order for unit searches.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchOrder {
    /// Field name understood by the backend search compiler.
    pub field: String,
    /// `true` for descending order, `false` for ascending order.
    pub descending: bool,
}

/// Limit/offset paging for unit searches.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchPaging {
    /// Maximum number of rows to return.
    pub limit: i64,
    /// Number of rows to skip from the ordered result.
    pub offset: i64,
}

/// Result envelope returned by search APIs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchResult {
    /// Total matches before paging.
    pub total_hits: i64,
    /// Page of matching units.
    pub results: Vec<Unit>,
}

/// Directed relation between two units.
///
/// Relation types are numeric to stay compatible with the database schema and
/// the Java implementation.
///
/// Direction is expressed as `left -> right`. For a parent/child relation, store
/// the parent unit on the left and the child unit on the right. A directory
/// listing is therefore a "right" lookup from the parent: "which children are to
/// my right?" Locating a file's containing directory is a "left" lookup from the
/// child: "which parents point to me from the left?"
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Relation {
    /// Left/source unit tenant.
    pub tenant_id: i64,
    /// Left/source unit id.
    pub unit_id: i64,
    /// Numeric relation type.
    pub relation_type: i32,
    /// Right/target unit tenant.
    pub related_tenant_id: i64,
    /// Right/target unit id.
    pub related_unit_id: i64,
}

/// Association from a unit to an external reference string.
///
/// Associations model links where the other side is not necessarily another
/// repository unit, such as a case number or domain-specific reference.
///
/// Association direction uses the same lookup vocabulary as relations: the unit
/// is the left side and the external reference is the right side. A "right"
/// association lookup starts with a unit and returns its references. A "left"
/// association lookup starts with a reference and returns the units that point to
/// it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Association {
    /// Left/source unit tenant.
    pub tenant_id: i64,
    /// Left/source unit id.
    pub unit_id: i64,
    /// Numeric association type.
    pub association_type: i32,
    /// Right/target external reference value.
    pub reference: String,
}
