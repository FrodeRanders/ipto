use std::sync::Arc;

use ipto_rust::backend::RepoError;
use ipto_rust::backends::postgres::PostgresBackend;
use ipto_rust::model::{SearchOrder, SearchPaging, UnitRef, VersionSelector};
use ipto_rust::repo::RepoService;
use postgres::{Client, Config, NoTls};
use serde_json::json;
use uuid::Uuid;

fn pg_integration_enabled() -> bool {
    matches!(
        std::env::var("IPTO_PG_INTEGRATION").ok().as_deref(),
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

fn ensure_tenant(tenant_id: i32) {
    let mut client = pg_client().expect("postgres connection for tenant bootstrap");
    let name = format!("rust_it_{tenant_id}");
    let _ = client
        .execute(
            "INSERT INTO repo.repo_tenant (tenantid, name, description) VALUES ($1, $2, $3) ON CONFLICT (tenantid) DO NOTHING",
            &[&tenant_id, &name, &"rust integration tenant"],
        )
        .expect("insert tenant");
}

fn make_unit(tenant_id: i32, name: &str) -> serde_json::Value {
    json!({
        "tenantid": tenant_id,
        "corrid": Uuid::now_v7(),
        "status": 30,
        "unitname": name,
        "attributes": []
    })
}

#[test]
fn postgres_mvp_crud_and_links() {
    if !pg_integration_enabled() {
        return;
    }

    let tenant_id = 991_i32;
    ensure_tenant(tenant_id);

    let repo = RepoService::new(Arc::new(PostgresBackend::new()));

    let unit1 = repo
        .store_unit_json(make_unit(tenant_id, "rust-it-unit-1"))
        .expect("store unit 1");
    let unit2 = repo
        .store_unit_json(make_unit(tenant_id, "rust-it-unit-2"))
        .expect("store unit 2");

    let unit1_id = unit1["unitid"].as_i64().expect("unit1 id");
    let unit2_id = unit2["unitid"].as_i64().expect("unit2 id");

    assert!(repo
        .unit_exists(i64::from(tenant_id), unit1_id)
        .expect("unit_exists"));

    let loaded = repo
        .get_unit_json(i64::from(tenant_id), unit1_id, VersionSelector::Latest)
        .expect("get_unit_json");
    assert!(loaded.is_some());

    let left = UnitRef {
        tenant_id: i64::from(tenant_id),
        unit_id: unit1_id,
        version: None,
    };
    let right = UnitRef {
        tenant_id: i64::from(tenant_id),
        unit_id: unit2_id,
        version: None,
    };

    repo.add_relation(left.clone(), 10, right.clone())
        .expect("add relation");
    let right_rel = repo
        .get_right_relation(left.clone(), 10)
        .expect("get right relation");
    assert!(right_rel.is_some());
    let right_rels = repo
        .get_right_relations(left.clone(), 10)
        .expect("get right relations");
    assert!(!right_rels.is_empty());
    let left_rels = repo
        .get_left_relations(right.clone(), 10)
        .expect("get left relations");
    assert!(!left_rels.is_empty());
    assert_eq!(
        repo.count_right_relations(left.clone(), 10)
            .expect("count right relations"),
        1
    );
    assert_eq!(
        repo.count_left_relations(right.clone(), 10)
            .expect("count left relations"),
        1
    );
    repo.remove_relation(left.clone(), 10, right.clone())
        .expect("remove relation");

    repo.add_association(left.clone(), 20, "external-ref-123")
        .expect("add association");
    let right_assoc = repo
        .get_right_association(left.clone(), 20)
        .expect("get right association");
    assert!(right_assoc.is_some());
    let right_assocs = repo
        .get_right_associations(left.clone(), 20)
        .expect("get right associations");
    assert!(!right_assocs.is_empty());
    let left_assocs = repo
        .get_left_associations(20, "external-ref-123")
        .expect("get left associations");
    assert!(!left_assocs.is_empty());
    assert_eq!(
        repo.count_right_associations(left.clone(), 20)
            .expect("count right associations"),
        1
    );
    assert_eq!(
        repo.count_left_associations(20, "external-ref-123")
            .expect("count left associations"),
        1
    );
    repo.remove_association(left.clone(), 20, "external-ref-123")
        .expect("remove association");

    repo.lock_unit(left.clone(), 30, "rust-it-test")
        .expect("lock unit");
    let lock_again = repo.lock_unit(left.clone(), 30, "rust-it-test-2");
    assert!(matches!(lock_again, Err(RepoError::AlreadyLocked)));
    repo.unlock_unit(left.clone()).expect("unlock unit");

    let search = repo
        .search_units(
            json!({"tenantid": tenant_id, "name_ilike": "rust-it-unit-%"}),
            SearchOrder {
                field: "created".to_string(),
                descending: true,
            },
            SearchPaging {
                limit: 50,
                offset: 0,
            },
        )
        .expect("search units");
    assert!(search.total_hits >= 2);

    let attr_suffix = Uuid::now_v7().simple().to_string();
    let attr_alias = format!("rust_it_attr_alias_{attr_suffix}");
    let attr_name = format!("rust_it_attr_name_{attr_suffix}");
    let attr_qualname = format!("rust.it.attr.qualname.{attr_suffix}");

    let created_attr = repo
        .create_attribute(&attr_alias, &attr_name, &attr_qualname, "string", false)
        .expect("create attribute");
    let attr_id = created_attr["id"].as_i64().expect("attribute id");
    let attr_by_id = repo
        .get_attribute_info(&attr_id.to_string())
        .expect("get attr by id");
    assert!(attr_by_id.is_some());
    let attr_by_name = repo
        .get_attribute_info(&attr_name)
        .expect("get attr by name");
    assert!(attr_by_name.is_some());
    let instantiated = repo
        .instantiate_attribute(&attr_name)
        .expect("instantiate_attribute by name");
    assert!(instantiated.is_some());
    assert!(repo
        .can_change_attribute(&attr_name)
        .expect("can_change_attribute by name"));
    assert!(repo
        .can_change_attribute(&attr_id.to_string())
        .expect("can_change_attribute by id"));
    assert_eq!(
        repo.attribute_name_to_id(&attr_name)
            .expect("attribute_name_to_id"),
        Some(attr_id)
    );
    assert_eq!(
        repo.attribute_id_to_name(attr_id)
            .expect("attribute_id_to_name"),
        Some(attr_name)
    );

    let tenant_info = repo
        .get_tenant_info(&tenant_id.to_string())
        .expect("tenant info by id");
    assert!(tenant_info.is_some());
    assert_eq!(
        repo.tenant_name_to_id(&format!("rust_it_{tenant_id}"))
            .expect("tenant_name_to_id"),
        Some(i64::from(tenant_id))
    );
    assert_eq!(
        repo.tenant_id_to_name(i64::from(tenant_id))
            .expect("tenant_id_to_name"),
        Some(format!("rust_it_{tenant_id}"))
    );
}

#[test]
fn postgres_configure_graphql_sdl_persists_record_and_unit_templates() {
    if !pg_integration_enabled() {
        return;
    }

    let repo = RepoService::new(Arc::new(PostgresBackend::new()));
    let suffix = Uuid::now_v7().simple().to_string();

    let record_alias = format!("it_record_{suffix}");
    let record_name = format!("it:record:{suffix}");
    let record_uri = format!("urn:it:record:{suffix}");

    let field_alias = format!("it_field_{suffix}");
    let field_name = format!("it:field:{suffix}");
    let field_uri = format!("urn:it:field:{suffix}");

    let scalar_alias = format!("it_scalar_{suffix}");
    let scalar_name = format!("it:scalar:{suffix}");
    let scalar_uri = format!("urn:it:scalar:{suffix}");

    let record_type_name = format!("ItRecord{suffix}");
    let template_type_name = format!("ItTemplate{suffix}");
    let template_name = format!("ItTemplateName{suffix}");

    let sdl = format!(
        r#"
        enum Attributes @attributeRegistry {{
            {record_alias} @attribute(datatype: RECORD, name: "{record_name}", uri: "{record_uri}")
            {field_alias} @attribute(datatype: STRING, name: "{field_name}", uri: "{field_uri}")
            {scalar_alias} @attribute(datatype: STRING, name: "{scalar_name}", uri: "{scalar_uri}")
        }}

        type {record_type_name} @record(attribute: {record_alias}) {{
            fieldValue: String @use(attribute: {field_alias})
        }}

        type {template_type_name} @template(name: "{template_name}") {{
            scalarValue: String @use(attribute: {scalar_alias})
            recordValue: {record_type_name} @use(attribute: {record_alias})
        }}
        "#
    );

    let report = repo
        .configure_graphql_sdl(&sdl)
        .expect("configure_graphql_sdl should succeed");
    assert_eq!(
        report["summary"]["recordTemplatesPersisted"].as_u64(),
        Some(1)
    );
    assert_eq!(
        report["summary"]["unitTemplatesPersisted"].as_u64(),
        Some(1)
    );
    assert_eq!(
        report["persistence"]["recordTemplatesSupported"].as_bool(),
        Some(true)
    );
    assert_eq!(
        report["persistence"]["unitTemplatesSupported"].as_bool(),
        Some(true)
    );

    let mut client = pg_client().expect("postgres connection for verification");
    let record_row = client
        .query_one(
            "SELECT rt.recordid, rt.name
             FROM repo.repo_record_template rt
             JOIN repo.repo_attribute a ON a.attrid = rt.recordid
             WHERE a.alias = $1",
            &[&record_alias],
        )
        .expect("record template row");
    let record_id: i32 = record_row.get(0);
    let stored_record_name: String = record_row.get(1);
    assert_eq!(stored_record_name, record_type_name);

    let record_elements = client
        .query(
            "SELECT rte.idx, rte.alias, a.alias
             FROM repo.repo_record_template_elements rte
             JOIN repo.repo_attribute a ON a.attrid = rte.attrid
             WHERE rte.recordid = $1
             ORDER BY rte.idx",
            &[&record_id],
        )
        .expect("record template elements");
    assert_eq!(record_elements.len(), 1);
    let record_field_alias: String = record_elements[0].get(1);
    let record_attr_alias: String = record_elements[0].get(2);
    assert_eq!(record_field_alias, "fieldValue");
    assert_eq!(record_attr_alias, field_alias);

    let template_row = client
        .query_one(
            "SELECT templateid FROM repo.repo_unit_template WHERE name = $1",
            &[&template_name],
        )
        .expect("unit template row");
    let template_id: i32 = template_row.get(0);

    let template_elements = client
        .query(
            "SELECT ute.idx, ute.alias, a.alias
             FROM repo.repo_unit_template_elements ute
             JOIN repo.repo_attribute a ON a.attrid = ute.attrid
             WHERE ute.templateid = $1
             ORDER BY ute.idx",
            &[&template_id],
        )
        .expect("unit template elements");
    assert_eq!(template_elements.len(), 2);

    let tpl0_alias: String = template_elements[0].get(1);
    let tpl0_attr: String = template_elements[0].get(2);
    assert_eq!(tpl0_alias, "scalarValue");
    assert_eq!(tpl0_attr, scalar_alias);

    let tpl1_alias: String = template_elements[1].get(1);
    let tpl1_attr: String = template_elements[1].get(2);
    assert_eq!(tpl1_alias, "recordValue");
    assert_eq!(tpl1_attr, record_alias);
}
