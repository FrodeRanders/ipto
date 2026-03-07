use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VersionSelector {
    Latest,
    Exact(i64),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnitRef {
    pub tenant_id: i64,
    pub unit_id: i64,
    pub version: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Unit {
    pub tenant_id: i64,
    pub unit_id: i64,
    pub unit_ver: i64,
    pub status: i32,
    pub name: Option<String>,
    pub corr_id: Uuid,
    pub payload: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchOrder {
    pub field: String,
    pub descending: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchPaging {
    pub limit: i64,
    pub offset: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchResult {
    pub total_hits: i64,
    pub results: Vec<Unit>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Relation {
    pub tenant_id: i64,
    pub unit_id: i64,
    pub relation_type: i32,
    pub related_tenant_id: i64,
    pub related_unit_id: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Association {
    pub tenant_id: i64,
    pub unit_id: i64,
    pub association_type: i32,
    pub reference: String,
}
