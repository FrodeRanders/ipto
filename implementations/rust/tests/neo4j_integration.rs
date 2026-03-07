use std::sync::Arc;

use ipto_rust::backends::neo4j::Neo4jBackend;
use ipto_rust::model::{SearchOrder, SearchPaging, VersionSelector};
use ipto_rust::repo::RepoService;
use serde_json::json;
use uuid::Uuid;

fn neo4j_integration_enabled() -> bool {
    matches!(
        std::env::var("IPTO_NEO4J_INTEGRATION").ok().as_deref(),
        Some("1") | Some("true") | Some("TRUE") | Some("yes") | Some("YES")
    )
}

#[test]
fn neo4j_mvp_metadata_and_health() {
    if !neo4j_integration_enabled() {
        return;
    }

    let repo = RepoService::new(Arc::new(Neo4jBackend::new()));

    let health = repo.health().expect("neo4j health");
    assert_eq!(health["backend"], "neo4j");

    let tenant = repo
        .get_tenant_info("rust-neo4j-tenant")
        .expect("tenant info")
        .expect("tenant exists/created");
    assert_eq!(tenant["name"], "rust-neo4j-tenant");

    let attr_suffix = Uuid::now_v7().simple().to_string();
    let attr_alias = format!("neo4j_attr_alias_{attr_suffix}");
    let attr_name = format!("neo4j_attr_name_{attr_suffix}");
    let attr_qualname = format!("neo4j.attr.qualname.{attr_suffix}");

    let attr = repo
        .create_attribute(&attr_alias, &attr_name, &attr_qualname, "string", false)
        .expect("create attribute");
    assert_eq!(attr["name"], attr_name);

    let by_name = repo
        .get_attribute_info(&attr_name)
        .expect("attribute info by name")
        .expect("attribute exists");
    assert_eq!(by_name["name"], attr_name);
    let instantiated = repo
        .instantiate_attribute(&attr_name)
        .expect("instantiate_attribute by name");
    assert!(instantiated.is_some());
    assert!(repo
        .can_change_attribute(&attr_name)
        .expect("can_change_attribute by name"));
    let attr_id = by_name["id"].as_i64().expect("attribute id");
    assert_eq!(
        repo.attribute_name_to_id(&attr_name)
            .expect("attribute_name_to_id"),
        Some(attr_id)
    );
    assert_eq!(
        repo.attribute_id_to_name(attr_id)
            .expect("attribute_id_to_name"),
        Some(attr_name.clone())
    );
    assert_eq!(
        repo.tenant_name_to_id("rust-neo4j-tenant")
            .expect("tenant_name_to_id"),
        tenant["id"].as_i64()
    );
    assert_eq!(
        repo.tenant_id_to_name(tenant["id"].as_i64().expect("tenant id"))
            .expect("tenant_id_to_name"),
        Some("rust-neo4j-tenant".to_string())
    );

    let unit = repo
        .store_unit_json(json!({
            "tenantid": tenant["id"],
            "corrid": Uuid::now_v7(),
            "status": 30,
            "unitname": "neo4j-unit",
            "attributes": [
                {
                    "attrid": attr_id,
                    "attrname": attr_name,
                    "attrtype": 1,
                    "value": ["integration-value"]
                }
            ]
        }))
        .expect("store unit");
    let unit_id = unit["unitid"].as_i64().expect("unit id");

    let loaded = repo
        .get_unit_json(
            tenant["id"].as_i64().expect("tenant id"),
            unit_id,
            VersionSelector::Latest,
        )
        .expect("get unit")
        .expect("unit exists");
    assert_eq!(loaded["unitid"], unit_id);

    let found = repo
        .search_units(
            json!({
                "tenantid": tenant["id"],
                "name_ilike": "%neo4j-unit%"
            }),
            SearchOrder {
                field: "created".to_string(),
                descending: true,
            },
            SearchPaging {
                limit: 20,
                offset: 0,
            },
        )
        .expect("search units");
    assert!(found.total_hits >= 1);

    assert!(!repo
        .can_change_attribute(&attr_name)
        .expect("can_change_attribute false after usage"));
    assert!(!repo
        .can_change_attribute(&attr_id.to_string())
        .expect("can_change_attribute false by id after usage"));
}
