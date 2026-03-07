use std::sync::Arc;

use ipto_rust::backend::RepoError;
use ipto_rust::backends::neo4j::Neo4jBackend;
use ipto_rust::backends::postgres::PostgresBackend;
use ipto_rust::model::{SearchOrder, SearchPaging, UnitRef, VersionSelector};
use ipto_rust::repo::RepoService;
use postgres::{Client, Config, NoTls};
use serde_json::{json, Value};
use uuid::Uuid;

fn pg_integration_enabled() -> bool {
    matches!(
        std::env::var("IPTO_PG_INTEGRATION").ok().as_deref(),
        Some("1") | Some("true") | Some("TRUE") | Some("yes") | Some("YES")
    )
}

fn neo4j_integration_enabled() -> bool {
    matches!(
        std::env::var("IPTO_NEO4J_INTEGRATION").ok().as_deref(),
        Some("1") | Some("true") | Some("TRUE") | Some("yes") | Some("YES")
    )
}

fn pg_client() -> Result<Client, postgres::Error> {
    let host = std::env::var("IPTO_PG_HOST").unwrap_or_else(|_| "localhost".to_string());
    let port = std::env::var("IPTO_PG_PORT")
        .ok()
        .and_then(|s| s.parse::<u16>().ok())
        .unwrap_or(5432);
    let user = std::env::var("IPTO_PG_USER").unwrap_or_else(|_| "repo".to_string());
    let password = std::env::var("IPTO_PG_PASSWORD").unwrap_or_else(|_| "repo".to_string());
    let database = std::env::var("IPTO_PG_DATABASE").unwrap_or_else(|_| "repo".to_string());

    let mut cfg = Config::new();
    cfg.host(&host)
        .port(port)
        .user(&user)
        .password(&password)
        .dbname(&database);
    cfg.connect(NoTls)
}

fn ensure_pg_tenant(tenant_id: i32) {
    let mut client = pg_client().expect("postgres connection for tenant bootstrap");
    let name = format!("rust_parity_{tenant_id}");
    client
        .execute(
            "INSERT INTO repo.repo_tenant (tenantid, name, description) VALUES ($1, $2, $3) ON CONFLICT (tenantid) DO NOTHING",
            &[&tenant_id, &name, &"rust parity tenant"],
        )
        .expect("insert tenant");
}

fn make_unit(tenant_id: i64, name: &str) -> Value {
    json!({
        "tenantid": tenant_id,
        "corrid": Uuid::now_v7(),
        "status": 30,
        "unitname": name,
        "attributes": []
    })
}

fn ref_of(tenant_id: i64, unit_id: i64) -> UnitRef {
    UnitRef {
        tenant_id,
        unit_id,
        version: None,
    }
}

fn run_parity_scenario(repo: RepoService, tenant_id: i64, prefix: &str) {
    repo.flush_cache().expect("flush cache pre-flight");

    let unit_a = repo
        .store_unit_json(make_unit(tenant_id, &format!("{prefix}-a")))
        .expect("store unit a");
    let unit_b = repo
        .store_unit_json(make_unit(tenant_id, &format!("{prefix}-b")))
        .expect("store unit b");

    let unit_a_id = unit_a["unitid"].as_i64().expect("unit a id");
    let unit_b_id = unit_b["unitid"].as_i64().expect("unit b id");
    let unit_a_corrid = unit_a["corrid"].as_str().expect("unit a corrid");

    let a_ref = ref_of(tenant_id, unit_a_id);
    let b_ref = ref_of(tenant_id, unit_b_id);

    assert!(repo.unit_exists(tenant_id, unit_a_id).expect("unit exists"));

    let latest = repo
        .get_unit_json(tenant_id, unit_a_id, VersionSelector::Latest)
        .expect("get latest")
        .expect("unit found");
    assert_eq!(latest["unitid"], unit_a_id);
    assert_iso_timestamp_field(&latest, "created");
    assert_iso_timestamp_field(&latest, "modified");

    let by_corrid = repo
        .get_unit_by_corrid_json(tenant_id, unit_a_corrid)
        .expect("get by corrid")
        .expect("unit found by corrid");
    assert_eq!(by_corrid["unitid"], unit_a_id);
    assert_iso_timestamp_field(&by_corrid, "created");
    assert_iso_timestamp_field(&by_corrid, "modified");

    let predicate_search = repo
        .search_units(
            json!({
                "predicates": [
                    {"field": "tenantid", "op": "eq", "value": tenant_id},
                    {"field": "status", "op": "gte", "value": 30},
                    {"field": "name", "op": "like", "value": format!("{prefix}-a*")}
                ]
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
        .expect("predicate search");
    assert!(predicate_search
        .results
        .iter()
        .any(|u| u.unit_id == unit_a_id));
    assert!(!predicate_search
        .results
        .iter()
        .any(|u| u.unit_id == unit_b_id));

    repo.add_relation(a_ref.clone(), 11, b_ref.clone())
        .expect("add relation");

    let corrid_search = repo
        .search_units(
            json!({"tenantid": tenant_id, "corrid": unit_a_corrid}),
            SearchOrder {
                field: "created".to_string(),
                descending: true,
            },
            SearchPaging {
                limit: 20,
                offset: 0,
            },
        )
        .expect("search by corrid");
    assert_eq!(corrid_search.total_hits, 1);
    assert_eq!(corrid_search.results[0].unit_id, unit_a_id);

    let corrid_alias_search = repo
        .search_units(
            json!({"tenantid": tenant_id, "correlationid": unit_a_corrid}),
            SearchOrder {
                field: "created".to_string(),
                descending: true,
            },
            SearchPaging {
                limit: 20,
                offset: 0,
            },
        )
        .expect("search by correlationid alias");
    assert_eq!(corrid_alias_search.total_hits, 1);
    assert_eq!(corrid_alias_search.results[0].unit_id, unit_a_id);

    let corrid_underscore_alias_search = repo
        .search_units(
            json!({"tenantid": tenant_id, "corr_id": unit_a_corrid}),
            SearchOrder {
                field: "created".to_string(),
                descending: true,
            },
            SearchPaging {
                limit: 20,
                offset: 0,
            },
        )
        .expect("search by corr_id alias");
    assert_eq!(corrid_underscore_alias_search.total_hits, 1);
    assert_eq!(corrid_underscore_alias_search.results[0].unit_id, unit_a_id);

    let string_numeric_leaf_search = repo
        .search_units(
            json!({"tenantid": tenant_id.to_string(), "unitid": unit_a_id.to_string()}),
            SearchOrder {
                field: "created".to_string(),
                descending: true,
            },
            SearchPaging {
                limit: 20,
                offset: 0,
            },
        )
        .expect("search by string numeric ids");
    assert_eq!(string_numeric_leaf_search.total_hits, 1);
    assert_eq!(string_numeric_leaf_search.results[0].unit_id, unit_a_id);

    let underscored_alias_leaf_search = repo
        .search_units(
            json!({"tenant_id": tenant_id, "unit_id": unit_a_id}),
            SearchOrder {
                field: "created".to_string(),
                descending: true,
            },
            SearchPaging {
                limit: 20,
                offset: 0,
            },
        )
        .expect("search by tenant_id/unit_id aliases");
    assert_eq!(underscored_alias_leaf_search.total_hits, 1);
    assert_eq!(underscored_alias_leaf_search.results[0].unit_id, unit_a_id);

    let modified_search = repo
        .search_units(
            json!({"tenantid": tenant_id, "modified_after": 0, "modified_before": "2100-01-01T00:00:00Z"}),
            SearchOrder {
                field: "modified".to_string(),
                descending: true,
            },
            SearchPaging {
                limit: 20,
                offset: 0,
            },
        )
        .expect("search by modified range");
    assert!(modified_search.total_hits >= 2);

    let status_name_leaf_search = repo
        .search_units(
            json!({"tenantid": tenant_id, "status": "EFFECTIVE"}),
            SearchOrder {
                field: "created".to_string(),
                descending: true,
            },
            SearchPaging {
                limit: 20,
                offset: 0,
            },
        )
        .expect("search by status name in leaf expression");
    assert!(status_name_leaf_search
        .results
        .iter()
        .any(|unit| unit.unit_id == unit_a_id));

    assert_eq!(
        repo.count_right_relations(a_ref.clone(), 11)
            .expect("count right relations"),
        1
    );
    assert_eq!(
        repo.count_left_relations(b_ref.clone(), 11)
            .expect("count left relations"),
        1
    );

    repo.add_association(a_ref.clone(), 21, &format!("{prefix}-external"))
        .expect("add association");
    repo.add_relation(a_ref.clone(), 1, b_ref.clone())
        .expect("add relation with java symbolic type id");
    let symbolic_assoc_ref = format!("{prefix}-case");
    repo.add_association(a_ref.clone(), 2, &symbolic_assoc_ref)
        .expect("add association with java symbolic type id");
    assert_eq!(
        repo.count_right_associations(a_ref.clone(), 21)
            .expect("count right assoc"),
        1
    );

    let relation_search = repo
        .search_units(
            json!({
                "tenantid": tenant_id,
                "relation_type": 11,
                "related_tenantid": tenant_id,
                "related_unitid": unit_b_id
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
        .expect("search units by relation");
    assert!(relation_search
        .results
        .iter()
        .any(|unit| unit.unit_id == unit_a_id));

    let relation_alias_search = repo
        .search_units(
            json!({
                "tenantid": tenant_id,
                "reltype": 11,
                "reltenantid": tenant_id,
                "relunitid": unit_b_id
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
        .expect("search units by relation aliases");
    assert!(relation_alias_search
        .results
        .iter()
        .any(|unit| unit.unit_id == unit_a_id));

    let relation_right_side_search = repo
        .search_units(
            json!({
                "tenantid": tenant_id,
                "relation": {
                    "type": 11,
                    "side": "right",
                    "unit": format!("{tenant_id}.{unit_a_id}")
                }
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
        .expect("search units by relation right side object");
    assert!(relation_right_side_search
        .results
        .iter()
        .any(|unit| unit.unit_id == unit_b_id));

    let association_ref = format!("{prefix}-external");
    let association_search = repo
        .search_units(
            json!({
                "tenantid": tenant_id,
                "association_type": 21,
                "association_reference": association_ref
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
        .expect("search units by association");
    assert!(association_search
        .results
        .iter()
        .any(|unit| unit.unit_id == unit_a_id));

    let association_object_search = repo
        .search_units(
            json!({
                "tenantid": tenant_id,
                "association": {
                    "type": 21,
                    "reference": association_ref
                }
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
        .expect("search units by association object");
    assert!(association_object_search
        .results
        .iter()
        .any(|unit| unit.unit_id == unit_a_id));

    let relation_symbolic_query = repo
        .search_units_query(
            &format!("tenantid = {tenant_id} and relation:right-parent-child = '{tenant_id}.{unit_a_id}'"),
            SearchOrder {
                field: "created".to_string(),
                descending: true,
            },
            SearchPaging {
                limit: 20,
                offset: 0,
            },
        )
        .expect("search units by symbolic relation query");
    assert!(relation_symbolic_query
        .results
        .iter()
        .any(|unit| unit.unit_id == unit_b_id));

    let relation_symbolic_synonym_query = repo
        .search_units_query(
            &format!(
                "tenantid = {tenant_id} and relation:right-parentchild = '{tenant_id}.{unit_a_id}'"
            ),
            SearchOrder {
                field: "created".to_string(),
                descending: true,
            },
            SearchPaging {
                limit: 20,
                offset: 0,
            },
        )
        .expect("search units by symbolic relation synonym query");
    assert!(relation_symbolic_synonym_query
        .results
        .iter()
        .any(|unit| unit.unit_id == unit_b_id));

    let association_symbolic_query = repo
        .search_units_query(
            &format!("tenantid = {tenant_id} and association:case = '{symbolic_assoc_ref}'"),
            SearchOrder {
                field: "created".to_string(),
                descending: true,
            },
            SearchPaging {
                limit: 20,
                offset: 0,
            },
        )
        .expect("search units by symbolic association query");
    assert!(association_symbolic_query
        .results
        .iter()
        .any(|unit| unit.unit_id == unit_a_id));

    let and_search = repo
        .search_units(
            json!({
                "and": [
                    {"tenantid": tenant_id},
                    {"association_type": 21, "association_reference": association_ref}
                ]
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
        .expect("search units with and");
    assert!(and_search
        .results
        .iter()
        .any(|unit| unit.unit_id == unit_a_id));

    let or_search = repo
        .search_units(
            json!({
                "or": [
                    {"tenantid": tenant_id, "corrid": unit_a_corrid},
                    {"tenantid": tenant_id, "unitid": unit_b_id}
                ]
            }),
            SearchOrder {
                field: "created".to_string(),
                descending: true,
            },
            SearchPaging {
                limit: 50,
                offset: 0,
            },
        )
        .expect("search units with or");
    assert!(or_search
        .results
        .iter()
        .any(|unit| unit.unit_id == unit_a_id));
    assert!(or_search
        .results
        .iter()
        .any(|unit| unit.unit_id == unit_b_id));

    let not_search = repo
        .search_units(
            json!({
                "tenantid": tenant_id,
                "not": {"tenantid": tenant_id, "corrid": unit_a_corrid}
            }),
            SearchOrder {
                field: "created".to_string(),
                descending: true,
            },
            SearchPaging {
                limit: 50,
                offset: 0,
            },
        )
        .expect("search units with not");
    assert!(!not_search
        .results
        .iter()
        .any(|unit| unit.unit_id == unit_a_id));
    assert!(not_search
        .results
        .iter()
        .any(|unit| unit.unit_id == unit_b_id));

    let nested_not_search = repo
        .search_units(
            json!({
                "and": [
                    {"tenantid": tenant_id},
                    {"not": {"tenantid": tenant_id, "unitid": unit_a_id}}
                ]
            }),
            SearchOrder {
                field: "created".to_string(),
                descending: true,
            },
            SearchPaging {
                limit: 50,
                offset: 0,
            },
        )
        .expect("search units with nested not");
    assert!(!nested_not_search
        .results
        .iter()
        .any(|unit| unit.unit_id == unit_a_id));
    assert!(nested_not_search
        .results
        .iter()
        .any(|unit| unit.unit_id == unit_b_id));

    repo.lock_unit(a_ref.clone(), 30, "parity-test")
        .expect("lock unit");
    let lock_again = repo.lock_unit(a_ref.clone(), 30, "parity-test-again");
    assert!(matches!(lock_again, Err(RepoError::AlreadyLocked)));
    repo.unlock_unit(a_ref.clone()).expect("unlock unit");

    repo.set_status(a_ref.clone(), 20)
        .expect("set status to 20");
    repo.activate_unit(a_ref.clone()).expect("activate from 20");
    let after_activate = repo
        .get_unit_json(tenant_id, unit_a_id, VersionSelector::Latest)
        .expect("get after activate")
        .expect("unit exists");
    assert_eq!(after_activate["status"], 30);

    repo.lock_unit(a_ref.clone(), 31, "lifecycle-guard")
        .expect("lock for inactivate guard");
    let inactivate_locked = repo.inactivate_unit(a_ref.clone());
    assert!(matches!(inactivate_locked, Err(RepoError::AlreadyLocked)));
    repo.unlock_unit(a_ref.clone())
        .expect("unlock for inactivate path");

    repo.inactivate_unit(a_ref.clone())
        .expect("inactivate from effective");
    let after_inactivate = repo
        .get_unit_json(tenant_id, unit_a_id, VersionSelector::Latest)
        .expect("get after inactivate")
        .expect("unit exists");
    assert_eq!(after_inactivate["status"], 10);
    repo.activate_unit(a_ref.clone())
        .expect("activate from pending deletion");

    repo.set_status(a_ref.clone(), 30)
        .expect("set status to effective for transition matrix");
    let t1 = repo
        .request_status_transition(a_ref.clone(), 10)
        .expect("effective -> pending_deletion");
    assert_eq!(t1, 10);

    let t2 = repo
        .request_status_transition(a_ref.clone(), 40)
        .expect("pending_deletion -> archived rejected");
    assert_eq!(t2, 10);
    let t2b = repo
        .request_status_transition(a_ref.clone(), 30)
        .expect("pending_deletion -> effective rejected by strict matrix");
    assert_eq!(t2b, 10);
    repo.activate_unit(a_ref.clone())
        .expect("activate helper applies direct transition");
    let after_activate_helper = repo
        .get_unit_json(tenant_id, unit_a_id, VersionSelector::Latest)
        .expect("get after activate helper")
        .expect("unit exists");
    assert_eq!(after_activate_helper["status"], 30);
    repo.set_status(a_ref.clone(), 10)
        .expect("restore pending deletion");

    let t3 = repo
        .request_status_transition(a_ref.clone(), 1)
        .expect("pending_deletion -> pending_disposition");
    assert_eq!(t3, 1);

    let t4 = repo
        .request_status_transition(a_ref.clone(), 30)
        .expect("pending_disposition rejects transitions");
    assert_eq!(t4, 1);

    let invalid = repo.request_status_transition(a_ref.clone(), 999);
    assert!(matches!(invalid, Err(RepoError::InvalidInput(_))));

    repo.set_status(a_ref.clone(), 20)
        .expect("set status to obliterated");
    let t5 = repo
        .request_status_transition(a_ref.clone(), 1)
        .expect("obliterated -> pending_disposition");
    assert_eq!(t5, 1);

    let mut v2 = unit_a.clone();
    v2["unitname"] = Value::String(format!("{prefix}-a-v2"));
    let v2_stored = repo.store_unit_json(v2).expect("store v2");
    let v2_no = v2_stored["unitver"].as_i64().expect("v2 number");
    assert!(v2_no >= 2);

    let old = repo
        .get_unit_json(tenant_id, unit_a_id, VersionSelector::Exact(1))
        .expect("get version 1")
        .expect("version 1 exists");
    assert_eq!(old["unitver"], 1);

    let mut old_update = old.clone();
    old_update["unitname"] = Value::String(format!("{prefix}-old-version-update-attempt"));
    let readonly_update = repo.store_unit_json(old_update);
    assert!(
        matches!(readonly_update, Err(RepoError::InvalidInput(_))),
        "exact readonly version update should be rejected"
    );

    repo.lock_unit(a_ref.clone(), 32, "store-guard")
        .expect("lock for store guard");
    let locked_update = repo.store_unit_json(json!({
        "tenantid": tenant_id,
        "unitid": unit_a_id,
        "unitname": format!("{prefix}-locked-update-attempt")
    }));
    assert!(
        matches!(locked_update, Err(RepoError::AlreadyLocked)),
        "locked store update should be rejected"
    );
    repo.unlock_unit(a_ref.clone())
        .expect("unlock after store guard");

    let latest_before_status_only = repo
        .get_unit_json(tenant_id, unit_a_id, VersionSelector::Latest)
        .expect("latest before status-only update")
        .expect("latest unit exists");
    let before_status_only_ver = latest_before_status_only["unitver"]
        .as_i64()
        .expect("latest unitver");
    let status_only = repo
        .store_unit_json(json!({
            "tenantid": tenant_id,
            "unitid": unit_a_id,
            "status": 10
        }))
        .expect("status-only store should succeed");
    assert_eq!(status_only["unitver"], before_status_only_ver);
    assert_eq!(status_only["status"], 10);
    repo.set_status(a_ref.clone(), 30)
        .expect("restore status after status-only store");

    let unitver_alias_query = repo
        .search_units_query(
            "unit_ver = 2",
            SearchOrder {
                field: "created".to_string(),
                descending: true,
            },
            SearchPaging {
                limit: 20,
                offset: 0,
            },
        )
        .expect("search by unit_ver alias query");
    assert!(unitver_alias_query
        .results
        .iter()
        .any(|unit| unit.unit_id == unit_a_id));

    let unitver_leaf_search = repo
        .search_units(
            json!({"tenantid": tenant_id, "unit_ver": 2}),
            SearchOrder {
                field: "created".to_string(),
                descending: true,
            },
            SearchPaging {
                limit: 20,
                offset: 0,
            },
        )
        .expect("search by unit_ver in leaf expression");
    assert!(unitver_leaf_search
        .results
        .iter()
        .any(|unit| unit.unit_id == unit_a_id && unit.unit_ver >= 2));

    let status_name_query = repo
        .search_units_query(
            "status = EFFECTIVE",
            SearchOrder {
                field: "created".to_string(),
                descending: true,
            },
            SearchPaging {
                limit: 20,
                offset: 0,
            },
        )
        .expect("search by status name query");
    assert!(status_name_query
        .results
        .iter()
        .any(|unit| unit.unit_id == unit_a_id));

    let in_list_query = repo
        .search_units_query(
            &format!("tenantid = {tenant_id} and unitid in ({unit_a_id}, {unit_b_id})"),
            SearchOrder {
                field: "created".to_string(),
                descending: true,
            },
            SearchPaging {
                limit: 20,
                offset: 0,
            },
        )
        .expect("search by IN-list query");
    assert!(in_list_query
        .results
        .iter()
        .any(|unit| unit.unit_id == unit_a_id));
    assert!(in_list_query
        .results
        .iter()
        .any(|unit| unit.unit_id == unit_b_id));

    let not_in_list_query = repo
        .search_units_query(
            &format!("tenantid = {tenant_id} and unitid not in ({unit_a_id})"),
            SearchOrder {
                field: "created".to_string(),
                descending: true,
            },
            SearchPaging {
                limit: 20,
                offset: 0,
            },
        )
        .expect("search by NOT IN-list query");
    assert!(!not_in_list_query
        .results
        .iter()
        .any(|unit| unit.unit_id == unit_a_id));
    assert!(not_in_list_query
        .results
        .iter()
        .any(|unit| unit.unit_id == unit_b_id));

    let neq_angle_query = repo
        .search_units_query(
            &format!("tenantid = {tenant_id} and unitid <> {unit_a_id}"),
            SearchOrder {
                field: "created".to_string(),
                descending: true,
            },
            SearchPaging {
                limit: 20,
                offset: 0,
            },
        )
        .expect("search by <> neq operator");
    assert!(!neq_angle_query
        .results
        .iter()
        .any(|unit| unit.unit_id == unit_a_id));
    assert!(neq_angle_query
        .results
        .iter()
        .any(|unit| unit.unit_id == unit_b_id));

    let (min_id, max_id) = if unit_a_id <= unit_b_id {
        (unit_a_id, unit_b_id)
    } else {
        (unit_b_id, unit_a_id)
    };
    let between_query = repo
        .search_units_query(
            &format!("tenantid = {tenant_id} and unitid between {min_id} and {max_id}"),
            SearchOrder {
                field: "created".to_string(),
                descending: true,
            },
            SearchPaging {
                limit: 20,
                offset: 0,
            },
        )
        .expect("search by BETWEEN operator");
    assert!(between_query
        .results
        .iter()
        .any(|unit| unit.unit_id == unit_a_id));
    assert!(between_query
        .results
        .iter()
        .any(|unit| unit.unit_id == unit_b_id));

    let search = repo
        .search_units(
            json!({"tenantid": tenant_id, "name_ilike": format!("%{prefix}-a-v2%")}),
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
    assert!(search.total_hits >= 1);

    let wildcard_name_leaf_search = repo
        .search_units(
            json!({"tenantid": tenant_id, "name": format!("{prefix}-a-v2*")}),
            SearchOrder {
                field: "created".to_string(),
                descending: true,
            },
            SearchPaging {
                limit: 20,
                offset: 0,
            },
        )
        .expect("search units by wildcard name leaf");
    assert!(wildcard_name_leaf_search
        .results
        .iter()
        .any(|unit| unit.unit_id == unit_a_id));

    let attr_suffix = Uuid::now_v7().simple().to_string();
    let attr_name = format!("{prefix}_attr_{attr_suffix}");
    let attr_info = repo
        .create_attribute(
            &format!("{prefix}_alias_{attr_suffix}"),
            &attr_name,
            &format!("{prefix}.qual.{attr_suffix}"),
            "string",
            false,
        )
        .expect("create attribute for parity");
    let attr_id = attr_info["id"].as_i64().expect("attribute id");
    let attr_alias = attr_info["alias"]
        .as_str()
        .expect("attribute alias")
        .to_string();
    let attr_qualname = attr_info["qualname"]
        .as_str()
        .expect("attribute qualname")
        .to_string();
    assert!(repo
        .instantiate_attribute(&attr_name)
        .expect("instantiate_attribute")
        .is_some());
    assert!(repo
        .can_change_attribute(&attr_name)
        .expect("can_change_attribute by name"));
    assert!(repo
        .can_change_attribute(&attr_id.to_string())
        .expect("can_change_attribute by id"));

    let attr_value = format!("{prefix}-Attr-Value");
    let attr_value_lower = attr_value.to_ascii_lowercase();
    let attr_unit = repo
        .store_unit_json(json!({
            "tenantid": tenant_id,
            "corrid": Uuid::now_v7(),
            "status": 30,
            "unitname": format!("{prefix}-attr-unit"),
            "attributes": [
                {
                    "attrid": attr_id,
                    "attrname": attr_name.clone(),
                    "attrtype": 1,
                    "value": [attr_value.clone()]
                }
            ]
        }))
        .expect("store unit with attribute value");
    let attr_unit_id = attr_unit["unitid"].as_i64().expect("attribute unit id");

    let attr_search_by_id = repo
        .search_units(
            json!({
                "tenantid": tenant_id,
                "attribute_eq": {
                    "name_or_id": attr_id.to_string(),
                    "value": attr_value.clone()
                }
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
        .expect("search by attribute_eq id");
    assert!(attr_search_by_id
        .results
        .iter()
        .any(|unit| unit.unit_id == attr_unit_id));

    let attr_search_by_name = repo
        .search_units(
            json!({
                "tenantid": tenant_id,
                "attribute_eq": {
                    "name_or_id": attr_name.clone(),
                    "value": attr_value_lower
                }
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
        .expect("search by attribute_eq name");
    assert!(attr_search_by_name
        .results
        .iter()
        .any(|unit| unit.unit_id == attr_unit_id));

    let attr_search_by_alias = repo
        .search_units(
            json!({
                "tenantid": tenant_id,
                "attribute_eq": {
                    "name_or_id": attr_alias.clone(),
                    "value": attr_value.clone()
                }
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
        .expect("search by attribute_eq alias");
    assert!(attr_search_by_alias
        .results
        .iter()
        .any(|unit| unit.unit_id == attr_unit_id));

    let attr_search_by_qualname = repo
        .search_units(
            json!({
                "tenantid": tenant_id,
                "attribute_eq": {
                    "name_or_id": attr_qualname.clone(),
                    "value": attr_value.clone()
                }
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
        .expect("search by attribute_eq qualname");
    assert!(attr_search_by_qualname
        .results
        .iter()
        .any(|unit| unit.unit_id == attr_unit_id));

    let attr_search_wildcard = repo
        .search_units(
            json!({
                "tenantid": tenant_id,
                "attribute_eq": {
                    "name_or_id": attr_name.clone(),
                    "value": "*attr-value"
                }
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
        .expect("search by attribute_eq wildcard");
    assert!(attr_search_wildcard
        .results
        .iter()
        .any(|unit| unit.unit_id == attr_unit_id));

    let attr_cmp_like = repo
        .search_units(
            json!({
                "tenantid": tenant_id,
                "attribute_cmp": {
                    "name_or_id": attr_name.clone(),
                    "op": "like",
                    "value_type": "string",
                    "value": "*attr-value"
                }
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
        .expect("search by attribute_cmp like");
    assert!(attr_cmp_like
        .results
        .iter()
        .any(|unit| unit.unit_id == attr_unit_id));

    let numeric_attr_name = format!("{prefix}_numeric_attr_{attr_suffix}");
    let numeric_attr = repo
        .create_attribute(
            &format!("{prefix}_numeric_alias_{attr_suffix}"),
            &numeric_attr_name,
            &format!("{prefix}.qual.numeric.{attr_suffix}"),
            "integer",
            false,
        )
        .expect("create numeric attribute for compare search");
    let numeric_attr_id = numeric_attr["id"].as_i64().expect("numeric attribute id");

    let low_unit = repo
        .store_unit_json(json!({
            "tenantid": tenant_id,
            "corrid": Uuid::now_v7(),
            "status": 30,
            "unitname": format!("{prefix}-numeric-low"),
            "attributes": [
                {
                    "attrid": numeric_attr_id,
                    "attrname": numeric_attr_name.clone(),
                    "attrtype": 3,
                    "value": [10]
                }
            ]
        }))
        .expect("store numeric low unit");
    let low_unit_id = low_unit["unitid"].as_i64().expect("numeric low id");

    let high_unit = repo
        .store_unit_json(json!({
            "tenantid": tenant_id,
            "corrid": Uuid::now_v7(),
            "status": 30,
            "unitname": format!("{prefix}-numeric-high"),
            "attributes": [
                {
                    "attrid": numeric_attr_id,
                    "attrname": numeric_attr_name.clone(),
                    "attrtype": 3,
                    "value": [20]
                }
            ]
        }))
        .expect("store numeric high unit");
    let high_unit_id = high_unit["unitid"].as_i64().expect("numeric high id");

    let cmp_gt = repo
        .search_units(
            json!({
                "tenantid": tenant_id,
                "attribute_cmp": {
                    "name_or_id": numeric_attr_id.to_string(),
                    "op": "gt",
                    "value_type": "number",
                    "value": 15
                }
            }),
            SearchOrder {
                field: "created".to_string(),
                descending: true,
            },
            SearchPaging {
                limit: 50,
                offset: 0,
            },
        )
        .expect("search by attribute_cmp gt");
    assert!(cmp_gt
        .results
        .iter()
        .any(|unit| unit.unit_id == high_unit_id));
    assert!(!cmp_gt
        .results
        .iter()
        .any(|unit| unit.unit_id == low_unit_id));

    let cmp_gt_string_number = repo
        .search_units(
            json!({
                "tenantid": tenant_id,
                "attribute_cmp": {
                    "name_or_id": numeric_attr_name.clone(),
                    "op": "gt",
                    "value_type": "number",
                    "value": "15"
                }
            }),
            SearchOrder {
                field: "created".to_string(),
                descending: true,
            },
            SearchPaging {
                limit: 50,
                offset: 0,
            },
        )
        .expect("search by attribute_cmp gt with numeric string input");
    assert!(cmp_gt_string_number
        .results
        .iter()
        .any(|unit| unit.unit_id == high_unit_id));
    assert!(!cmp_gt_string_number
        .results
        .iter()
        .any(|unit| unit.unit_id == low_unit_id));

    let cmp_neq_number = repo
        .search_units(
            json!({
                "tenantid": tenant_id,
                "attribute_cmp": {
                    "name_or_id": numeric_attr_name.clone(),
                    "op": "neq",
                    "value_type": "number",
                    "value": 10
                }
            }),
            SearchOrder {
                field: "created".to_string(),
                descending: true,
            },
            SearchPaging {
                limit: 50,
                offset: 0,
            },
        )
        .expect("search by attribute_cmp neq");
    assert!(cmp_neq_number
        .results
        .iter()
        .any(|unit| unit.unit_id == high_unit_id));
    assert!(!cmp_neq_number
        .results
        .iter()
        .any(|unit| unit.unit_id == low_unit_id));

    let invalid_boolean_op = repo.search_units(
        json!({
            "tenantid": tenant_id,
            "attribute_cmp": {
                "name_or_id": numeric_attr_name.clone(),
                "op": "gt",
                "value_type": "boolean",
                "value": true
            }
        }),
        SearchOrder {
            field: "created".to_string(),
            descending: true,
        },
        SearchPaging {
            limit: 50,
            offset: 0,
        },
    );
    assert!(matches!(
        invalid_boolean_op,
        Err(RepoError::InvalidInput(_))
    ));

    let invalid_numeric_value = repo.search_units(
        json!({
            "tenantid": tenant_id,
            "attribute_cmp": {
                "name_or_id": numeric_attr_name.clone(),
                "op": "gt",
                "value_type": "number",
                "value": "NaN"
            }
        }),
        SearchOrder {
            field: "created".to_string(),
            descending: true,
        },
        SearchPaging {
            limit: 50,
            offset: 0,
        },
    );
    assert!(matches!(
        invalid_numeric_value,
        Err(RepoError::InvalidInput(_))
    ));

    let time_attr_name = format!("{prefix}_time_attr_{attr_suffix}");
    let time_attr = repo
        .create_attribute(
            &format!("{prefix}_time_alias_{attr_suffix}"),
            &time_attr_name,
            &format!("{prefix}.qual.time.{attr_suffix}"),
            "time",
            false,
        )
        .expect("create time attribute for compare search");
    let time_attr_id = time_attr["id"].as_i64().expect("time attribute id");

    let time_low = repo
        .store_unit_json(json!({
            "tenantid": tenant_id,
            "corrid": Uuid::now_v7(),
            "status": 30,
            "unitname": format!("{prefix}-time-low"),
            "attributes": [
                {
                    "attrid": time_attr_id,
                    "attrname": time_attr_name.clone(),
                    "attrtype": 2,
                    "value": ["2025-01-01T00:00:00Z"]
                }
            ]
        }))
        .expect("store time low unit");
    let time_low_id = time_low["unitid"].as_i64().expect("time low id");

    let time_high = repo
        .store_unit_json(json!({
            "tenantid": tenant_id,
            "corrid": Uuid::now_v7(),
            "status": 30,
            "unitname": format!("{prefix}-time-high"),
            "attributes": [
                {
                    "attrid": time_attr_id,
                    "attrname": time_attr_name.clone(),
                    "attrtype": 2,
                    "value": ["2025-01-02T00:00:00Z"]
                }
            ]
        }))
        .expect("store time high unit");
    let time_high_id = time_high["unitid"].as_i64().expect("time high id");

    let cmp_time_gt_millis_string = repo
        .search_units(
            json!({
                "tenantid": tenant_id,
                "attribute_cmp": {
                    "name_or_id": time_attr_name.clone(),
                    "op": "gt",
                    "value_type": "time",
                    "value": "1735732800000"
                }
            }),
            SearchOrder {
                field: "created".to_string(),
                descending: true,
            },
            SearchPaging {
                limit: 50,
                offset: 0,
            },
        )
        .expect("search by attribute_cmp gt with millis-string time input");
    assert!(cmp_time_gt_millis_string
        .results
        .iter()
        .any(|unit| unit.unit_id == time_high_id));
    assert!(!cmp_time_gt_millis_string
        .results
        .iter()
        .any(|unit| unit.unit_id == time_low_id));

    let invalid_time_millis = repo.search_units(
        json!({
            "tenantid": tenant_id,
            "attribute_cmp": {
                "name_or_id": time_attr_name.clone(),
                "op": "gt",
                "value_type": "time",
                "value": "999999999999999999"
            }
        }),
        SearchOrder {
            field: "created".to_string(),
            descending: true,
        },
        SearchPaging {
            limit: 50,
            offset: 0,
        },
    );
    assert!(matches!(
        invalid_time_millis,
        Err(RepoError::InvalidInput(_))
    ));

    assert!(!repo
        .can_change_attribute(&attr_name)
        .expect("can_change_attribute false after usage by name"));
    assert!(!repo
        .can_change_attribute(&attr_id.to_string())
        .expect("can_change_attribute false after usage by id"));

    repo.flush_cache().expect("flush cache post-flight");
}

fn assert_iso_timestamp_field(payload: &Value, field: &str) {
    let ts = payload
        .get(field)
        .and_then(Value::as_str)
        .expect("timestamp field must be string");
    assert!(ts.contains('T'), "{field} should contain 'T': {ts}");
    assert!(
        ts.ends_with('Z') || ts.contains('+'),
        "{field} should contain timezone: {ts}"
    );
}

#[test]
fn parity_postgres_core_flow() {
    if !pg_integration_enabled() {
        return;
    }

    let tenant_id = 994_i32;
    ensure_pg_tenant(tenant_id);

    let repo = RepoService::new(Arc::new(PostgresBackend::new()));
    run_parity_scenario(repo, i64::from(tenant_id), "parity-pg");
}

#[test]
fn parity_neo4j_core_flow() {
    if !neo4j_integration_enabled() {
        return;
    }

    let repo = RepoService::new(Arc::new(Neo4jBackend::new()));
    let tenant = repo
        .get_tenant_info("parity-neo4j-tenant")
        .expect("get/create tenant")
        .expect("tenant exists");
    let tenant_id = tenant["id"].as_i64().expect("tenant id");

    run_parity_scenario(repo, tenant_id, "parity-neo4j");
}
