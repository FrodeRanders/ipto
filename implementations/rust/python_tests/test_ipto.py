import json
import os
import uuid

import pytest

try:
    import ipto_rust
except Exception as exc:  # pragma: no cover
    ipto_rust = None
    IMPORT_ERROR = exc
else:
    IMPORT_ERROR = None


def _pg_enabled() -> bool:
    return os.getenv("IPTO_PG_INTEGRATION", "").lower() in {"1", "true", "yes"}


def _neo4j_enabled() -> bool:
    return os.getenv("IPTO_NEO4J_INTEGRATION", "").lower() in {"1", "true", "yes"}


def _require_module():
    if ipto_rust is None:
        pytest.skip(f"ipto_rust import failed: {IMPORT_ERROR}")


def _unit_payload(tenant_id: int, name: str) -> str:
    return json.dumps({
        "tenantid": tenant_id,
        "corrid": str(uuid.uuid4()),
        "status": 30,
        "unitname": name,
        "attributes": [],
    })


@pytest.fixture
def pg_client():
    _require_module()
    return ipto_rust.PyIpto("postgres")


@pytest.fixture
def neo4j_client():
    _require_module()
    return ipto_rust.PyIpto("neo4j")


@pytest.mark.skipif(
    not _pg_enabled(),
    reason="Set IPTO_PG_INTEGRATION=1 to run PostgreSQL Python integration test",
)
def test_postgres_store_and_search_roundtrip(pg_client):
    pg_client.flush_cache()

    tenant_id = int(os.getenv("IPTO_PG_TEST_TENANT", "1"))
    unit_name = f"py-it-unit-pg-{uuid.uuid4().hex[:8]}"

    raw = pg_client.store_unit_json(_unit_payload(tenant_id, unit_name))
    stored = json.loads(raw)
    unit_id = stored["unitid"]
    corrid = stored["corrid"]

    assert pg_client.unit_exists(tenant_id, unit_id)

    loaded = pg_client.get_unit_json(tenant_id, unit_id)
    assert loaded is not None
    loaded_by_corrid = pg_client.get_unit_by_corrid_json(tenant_id, corrid)
    assert loaded_by_corrid is not None
    assert json.loads(loaded_by_corrid)["unitid"] == unit_id
    corrid_search = json.loads(
        pg_client.search_units(
            json.dumps({"tenantid": tenant_id, "corrid": corrid}),
            "created",
            True,
            10,
            0,
        )
    )
    assert corrid_search["total_hits"] == 1

    tenant_name = pg_client.tenant_id_to_name(tenant_id)
    assert tenant_name is not None
    assert pg_client.tenant_name_to_id(tenant_name) == tenant_id

    assoc_ref = f"py-it-assoc-pg-{uuid.uuid4().hex[:8]}"
    pg_client.add_association(tenant_id, unit_id, 21, assoc_ref)
    assoc_search = json.loads(
        pg_client.search_units(
            json.dumps(
                {
                    "tenantid": tenant_id,
                    "association_type": 21,
                    "association_reference": assoc_ref,
                }
            ),
            "created",
            True,
            10,
            0,
        )
    )
    assert assoc_search["total_hits"] >= 1

    pg_client.set_status(tenant_id, unit_id, 20)
    pg_client.activate_unit(tenant_id, unit_id)

    attr_suffix = uuid.uuid4().hex[:8]
    attr_name = f"py_it_attr_pg_{attr_suffix}"
    created_attr = json.loads(
        pg_client.create_attribute(
            f"py_it_attr_alias_pg_{attr_suffix}",
            attr_name,
            f"py.it.attr.pg.{attr_suffix}",
            "string",
            False,
        )
    )
    assert pg_client.can_change_attribute(attr_name)
    instantiated_attr = pg_client.instantiate_attribute(attr_name)
    assert instantiated_attr is not None
    assert json.loads(instantiated_attr)["id"] == created_attr["id"]

    wildcard_unit = json.loads(
        pg_client.store_unit_json(
            json.dumps(
                {
                    "tenantid": tenant_id,
                    "corrid": str(uuid.uuid4()),
                    "status": 30,
                    "unitname": f"py-it-attr-wild-pg-{uuid.uuid4().hex[:8]}",
                    "attributes": [
                        {
                            "attrid": created_attr["id"],
                            "attrname": attr_name,
                            "attrtype": 1,
                            "value": ["Pg-Wild-Value"],
                        }
                    ],
                }
            )
        )
    )
    wildcard_search = json.loads(
        pg_client.search_units(
            json.dumps(
                {
                    "tenantid": tenant_id,
                    "attribute_eq": {
                        "name_or_id": attr_name,
                        "value": "*wild-value",
                    },
                }
            ),
            "created",
            True,
            50,
            0,
        )
    )
    wildcard_ids = {u["unit_id"] for u in wildcard_search["results"]}
    assert wildcard_unit["unitid"] in wildcard_ids

    query_search = json.loads(
        pg_client.search_units_query(
            f"tenantid = {tenant_id} and corrid = '{corrid}'",
            "created",
            True,
            10,
            0,
        )
    )
    assert query_search["total_hits"] == 1

    with pytest.raises(RuntimeError):
        pg_client.search_units_query_strict(
            "custom_attr = 'x'",
            "created",
            True,
            10,
            0,
        )

    strict_query_search = json.loads(
        pg_client.search_units_query_strict(
            f"tenantid = {tenant_id} and attr:{attr_name} = '*wild-value'",
            "created",
            True,
            10,
            0,
        )
    )
    strict_ids = {u["unit_id"] for u in strict_query_search["results"]}
    assert wildcard_unit["unitid"] in strict_ids

    numeric_attr_suffix = uuid.uuid4().hex[:8]
    numeric_attr_name = f"py_it_attr_num_pg_{numeric_attr_suffix}"
    numeric_attr = json.loads(
        pg_client.create_attribute(
            f"py_it_attr_num_alias_pg_{numeric_attr_suffix}",
            numeric_attr_name,
            f"py.it.attr.num.pg.{numeric_attr_suffix}",
            "integer",
            False,
        )
    )
    numeric_attr_id = numeric_attr["id"]
    numeric_low = json.loads(
        pg_client.store_unit_json(
            json.dumps(
                {
                    "tenantid": tenant_id,
                    "corrid": str(uuid.uuid4()),
                    "status": 30,
                    "unitname": f"py-it-attr-num-low-pg-{uuid.uuid4().hex[:8]}",
                    "attributes": [
                        {
                            "attrid": numeric_attr_id,
                            "attrname": numeric_attr_name,
                            "attrtype": 3,
                            "value": [10],
                        }
                    ],
                }
            )
        )
    )
    numeric_high = json.loads(
        pg_client.store_unit_json(
            json.dumps(
                {
                    "tenantid": tenant_id,
                    "corrid": str(uuid.uuid4()),
                    "status": 30,
                    "unitname": f"py-it-attr-num-high-pg-{uuid.uuid4().hex[:8]}",
                    "attributes": [
                        {
                            "attrid": numeric_attr_id,
                            "attrname": numeric_attr_name,
                            "attrtype": 3,
                            "value": [20],
                        }
                    ],
                }
            )
        )
    )
    numeric_cmp = json.loads(
        pg_client.search_units(
            json.dumps(
                {
                    "tenantid": tenant_id,
                    "attribute_cmp": {
                        "name_or_id": str(numeric_attr_id),
                        "op": "gt",
                        "value_type": "number",
                        "value": "15",
                    },
                }
            ),
            "created",
            True,
            50,
            0,
        )
    )
    numeric_ids = {u["unit_id"] for u in numeric_cmp["results"]}
    assert numeric_high["unitid"] in numeric_ids
    assert numeric_low["unitid"] not in numeric_ids

    time_attr_suffix = uuid.uuid4().hex[:8]
    time_attr_name = f"py_it_attr_time_pg_{time_attr_suffix}"
    time_attr = json.loads(
        pg_client.create_attribute(
            f"py_it_attr_time_alias_pg_{time_attr_suffix}",
            time_attr_name,
            f"py.it.attr.time.pg.{time_attr_suffix}",
            "time",
            False,
        )
    )
    time_attr_id = time_attr["id"]
    time_low = json.loads(
        pg_client.store_unit_json(
            json.dumps(
                {
                    "tenantid": tenant_id,
                    "corrid": str(uuid.uuid4()),
                    "status": 30,
                    "unitname": f"py-it-attr-time-low-pg-{uuid.uuid4().hex[:8]}",
                    "attributes": [
                        {
                            "attrid": time_attr_id,
                            "attrname": time_attr_name,
                            "attrtype": 2,
                            "value": ["2025-01-01T00:00:00Z"],
                        }
                    ],
                }
            )
        )
    )
    time_high = json.loads(
        pg_client.store_unit_json(
            json.dumps(
                {
                    "tenantid": tenant_id,
                    "corrid": str(uuid.uuid4()),
                    "status": 30,
                    "unitname": f"py-it-attr-time-high-pg-{uuid.uuid4().hex[:8]}",
                    "attributes": [
                        {
                            "attrid": time_attr_id,
                            "attrname": time_attr_name,
                            "attrtype": 2,
                            "value": ["2025-01-02T00:00:00Z"],
                        }
                    ],
                }
            )
        )
    )
    time_cmp = json.loads(
        pg_client.search_units(
            json.dumps(
                {
                    "tenantid": tenant_id,
                    "attribute_cmp": {
                        "name_or_id": str(time_attr_id),
                        "op": "gt",
                        "value_type": "time",
                        "value": "1735732800000",
                    },
                }
            ),
            "created",
            True,
            50,
            0,
        )
    )
    time_ids = {u["unit_id"] for u in time_cmp["results"]}
    assert time_high["unitid"] in time_ids
    assert time_low["unitid"] not in time_ids

    with pytest.raises(RuntimeError):
        pg_client.search_units(
            json.dumps(
                {
                    "tenantid": tenant_id,
                    "attribute_cmp": {
                        "name_or_id": str(numeric_attr_id),
                        "op": "gt",
                        "value_type": "boolean",
                        "value": True,
                    },
                }
            ),
            "created",
            True,
            50,
            0,
        )

    with pytest.raises(RuntimeError):
        pg_client.search_units(
            json.dumps(
                {
                    "tenantid": tenant_id,
                    "attribute_cmp": {
                        "name_or_id": str(numeric_attr_id),
                        "op": "gt",
                        "value_type": "number",
                        "value": "NaN",
                    },
                }
            ),
            "created",
            True,
            50,
            0,
        )

    with pytest.raises(RuntimeError):
        pg_client.search_units(
            json.dumps(
                {
                    "tenantid": tenant_id,
                    "attribute_cmp": {
                        "name_or_id": str(time_attr_id),
                        "op": "gt",
                        "value_type": "time",
                        "value": "999999999999999999",
                    },
                }
            ),
            "created",
            True,
            50,
            0,
        )

    pg_client.set_status(tenant_id, unit_id, 30)
    assert pg_client.request_status_transition(tenant_id, unit_id, 10) == 10
    assert pg_client.request_status_transition(tenant_id, unit_id, 30) == 10
    pg_client.activate_unit(tenant_id, unit_id)
    assert json.loads(pg_client.get_unit_json(tenant_id, unit_id))["status"] == 30
    pg_client.set_status(tenant_id, unit_id, 10)
    assert pg_client.request_status_transition(tenant_id, unit_id, 1) == 1
    assert pg_client.request_status_transition(tenant_id, unit_id, 30) == 1
    with pytest.raises(RuntimeError):
        pg_client.request_status_transition(tenant_id, unit_id, 999)
    assert json.loads(pg_client.get_unit_json(tenant_id, unit_id))["status"] == 1
    assert not pg_client.is_unit_locked(tenant_id, unit_id)
    pg_client.lock_unit(tenant_id, unit_id, 30, "python-lifecycle")
    assert pg_client.is_unit_locked(tenant_id, unit_id)
    with pytest.raises(RuntimeError):
        pg_client.inactivate_unit(tenant_id, unit_id)
    pg_client.unlock_unit(tenant_id, unit_id)
    assert not pg_client.is_unit_locked(tenant_id, unit_id)
    pg_client.inactivate_unit(tenant_id, unit_id)
    assert json.loads(pg_client.get_unit_json(tenant_id, unit_id))["status"] == 10
    pg_client.activate_unit(tenant_id, unit_id)

    search = json.loads(
        pg_client.search_units(
            json.dumps({"tenantid": tenant_id, "name_ilike": f"%{unit_name}%"}),
            "created",
            True,
            20,
            0,
        )
    )
    assert search["total_hits"] >= 1

    gql_exists = json.loads(
        pg_client.graphql_execute(
            "query UnitExists { unitExists }",
            json.dumps({"tenantid": tenant_id, "unitid": unit_id}),
        )
    )
    assert gql_exists["data"]["unitExists"] is True
    gql_lock = json.loads(
        pg_client.graphql_execute(
            "query IsUnitLocked { isUnitLocked }",
            json.dumps({"tenantid": tenant_id, "unitid": unit_id}),
        )
    )
    assert gql_lock["data"]["isUnitLocked"] is False
    gql_flush = json.loads(
        pg_client.graphql_execute("mutation FlushCache { flushCache { ok } }")
    )
    assert gql_flush["data"]["flushCache"]["ok"] is True
    gql_store_name = f"py-it-gql-store-pg-{uuid.uuid4().hex[:8]}"
    gql_store = json.loads(
        pg_client.graphql_execute(
            "mutation StoreUnit { storeUnit { tenantid unitid unitver status unitname } }",
            json.dumps(
                {
                    "unit": json.loads(_unit_payload(tenant_id, gql_store_name)),
                }
            ),
        )
    )
    assert gql_store["data"]["storeUnit"]["tenantid"] == tenant_id
    assert gql_store["data"]["storeUnit"]["unitver"] == 1
    assert gql_store["data"]["storeUnit"]["unitname"] == gql_store_name
    gql_attr_suffix = uuid.uuid4().hex[:8]
    gql_attr_name = f"py_it_attr_gql_pg_{gql_attr_suffix}"
    gql_create_attr = json.loads(
        pg_client.graphql_execute(
            "mutation CreateAttribute { createAttribute { id name alias qualname } }",
            json.dumps(
                {
                    "alias": f"py_it_attr_gql_alias_pg_{gql_attr_suffix}",
                    "name": gql_attr_name,
                    "qualname": f"py.it.attr.gql.pg.{gql_attr_suffix}",
                    "attributeType": "string",
                    "isArray": False,
                }
            ),
        )
    )
    assert gql_create_attr["data"]["createAttribute"]["name"] == gql_attr_name
    gql_allow_health = json.loads(
        pg_client.graphql_execute_allowlist(
            "query Health { health { status } }",
            ["health"],
        )
    )
    assert gql_allow_health["data"]["health"]["status"] == "ok"
    gql_allow_block = json.loads(
        pg_client.graphql_execute_allowlist(
            "query UnitExists { unitExists }",
            ["health"],
            json.dumps({"tenantid": tenant_id, "unitid": unit_id}),
        )
    )
    assert gql_allow_block["data"] is None
    assert gql_allow_block["errors"][0]["extensions"]["code"] == "UNSUPPORTED"
    gql_registered = json.loads(
        pg_client.graphql_execute(
            "query RegisteredOperations { registeredOperations { queries mutations } }"
        )
    )
    assert "searchUnits" in gql_registered["data"]["registeredOperations"]["queries"]
    gql_registered_allow = json.loads(
        pg_client.graphql_execute_allowlist(
            "query RegisteredOperations { registeredOperations { queries mutations } }",
            ["registered_operations", "health"],
        )
    )
    assert gql_registered_allow["data"]["registeredOperations"]["queries"] == [
        "registeredOperations",
        "health",
    ]

    gql_search_query = json.loads(
        pg_client.graphql_execute(
            "query SearchUnitsQuery { searchUnitsQuery { totalHits } }",
            json.dumps(
                {
                    "query": f"tenantid = {tenant_id} and corrid = '{corrid}'",
                    "order": {"field": "created", "descending": True},
                    "paging": {"limit": 10, "offset": 0},
                }
            ),
        )
    )
    assert gql_search_query["data"]["searchUnitsQuery"]["totalHits"] == 1

    gql_search_query_allow = json.loads(
        pg_client.graphql_execute_allowlist(
            "query SearchUnitsQuery { searchUnitsQuery { totalHits } }",
            ["search_units_query"],
            json.dumps(
                {
                    "query": f"tenantid = {tenant_id} and corrid = '{corrid}'",
                    "order": {"field": "created", "descending": True},
                    "paging": {"limit": 10, "offset": 0},
                }
            ),
        )
    )
    assert gql_search_query_allow["data"]["searchUnitsQuery"]["totalHits"] == 1

    gql_search_query_allow_block = json.loads(
        pg_client.graphql_execute_allowlist(
            "query SearchUnitsQuery { searchUnitsQuery { totalHits } }",
            ["health"],
            json.dumps(
                {
                    "query": f"tenantid = {tenant_id} and corrid = '{corrid}'",
                }
            ),
        )
    )
    assert gql_search_query_allow_block["data"] is None
    gql_search_query_allow_block_ext = gql_search_query_allow_block["errors"][0]["extensions"]
    assert gql_search_query_allow_block_ext["code"] == "UNSUPPORTED"
    assert gql_search_query_allow_block_ext["operationType"] == "query"
    assert gql_search_query_allow_block_ext["operation"] == "searchUnitsQuery"
    assert "health" in gql_search_query_allow_block_ext["supportedOperations"]

    gql_search_query_strict = json.loads(
        pg_client.graphql_execute(
            "query SearchUnitsQueryStrict { searchUnitsQueryStrict { totalHits } }",
            json.dumps(
                {
                    "query": f"tenantid = {tenant_id} and attr:{attr_name} = '*wild-value'",
                    "order": {"field": "created", "descending": True},
                    "paging": {"limit": 10, "offset": 0},
                }
            ),
        )
    )
    assert gql_search_query_strict["data"]["searchUnitsQueryStrict"]["totalHits"] >= 1

    gql_instantiate_mut = json.loads(
        pg_client.graphql_execute(
            "mutation InstantiateAttributeMutation { instantiateAttributeMutation { id name } }",
            json.dumps({"nameOrId": attr_name}),
        )
    )
    assert gql_instantiate_mut["data"]["instantiateAttributeMutation"]["id"] == created_attr["id"]

    gql_instantiate_mut_allow_block = json.loads(
        pg_client.graphql_execute_allowlist(
            "mutation InstantiateAttributeMutation { instantiateAttributeMutation { id name } }",
            ["health"],
            json.dumps({"nameOrId": attr_name}),
        )
    )
    assert gql_instantiate_mut_allow_block["data"] is None
    gql_instantiate_mut_allow_block_ext = gql_instantiate_mut_allow_block["errors"][0]["extensions"]
    assert gql_instantiate_mut_allow_block_ext["code"] == "UNSUPPORTED"
    assert gql_instantiate_mut_allow_block_ext["operationType"] == "mutation"
    assert gql_instantiate_mut_allow_block_ext["operation"] == "instantiateAttributeMutation"
    assert "health" in gql_instantiate_mut_allow_block_ext["supportedOperations"]


@pytest.mark.skipif(
    not _neo4j_enabled(),
    reason="Set IPTO_NEO4J_INTEGRATION=1 to run Neo4j Python integration test",
)
def test_neo4j_store_get_and_search_roundtrip(neo4j_client):
    neo4j_client.flush_cache()

    tenant_name = os.getenv("IPTO_NEO4J_TEST_TENANT", "py-it-tenant-neo4j")
    tenant_info_raw = neo4j_client.get_tenant_info(tenant_name)
    assert tenant_info_raw is not None
    tenant_info = json.loads(tenant_info_raw)
    tenant_id = int(tenant_info["id"])
    unit_name = f"py-it-unit-neo4j-{uuid.uuid4().hex[:8]}"

    raw = neo4j_client.store_unit_json(_unit_payload(tenant_id, unit_name))
    stored = json.loads(raw)
    unit_id = stored["unitid"]
    corrid = stored["corrid"]

    assert neo4j_client.unit_exists(tenant_id, unit_id)

    loaded = neo4j_client.get_unit_json(tenant_id, unit_id)
    assert loaded is not None
    loaded_json = json.loads(loaded)
    assert loaded_json["unitid"] == unit_id
    loaded_by_corrid = neo4j_client.get_unit_by_corrid_json(tenant_id, corrid)
    assert loaded_by_corrid is not None
    assert json.loads(loaded_by_corrid)["unitid"] == unit_id
    corrid_search = json.loads(
        neo4j_client.search_units(
            json.dumps({"tenantid": tenant_id, "corrid": corrid}),
            "created",
            True,
            10,
            0,
        )
    )
    assert corrid_search["total_hits"] == 1

    assert neo4j_client.tenant_name_to_id(tenant_name) == tenant_id
    assert neo4j_client.tenant_id_to_name(tenant_id) == tenant_name

    assoc_ref = f"py-it-assoc-neo4j-{uuid.uuid4().hex[:8]}"
    neo4j_client.add_association(tenant_id, unit_id, 21, assoc_ref)
    assoc_search = json.loads(
        neo4j_client.search_units(
            json.dumps(
                {
                    "tenantid": tenant_id,
                    "association_type": 21,
                    "association_reference": assoc_ref,
                }
            ),
            "created",
            True,
            10,
            0,
        )
    )
    assert assoc_search["total_hits"] >= 1

    neo4j_client.set_status(tenant_id, unit_id, 20)
    neo4j_client.activate_unit(tenant_id, unit_id)
    assert json.loads(neo4j_client.get_unit_json(tenant_id, unit_id))["status"] == 30
    neo4j_client.lock_unit(tenant_id, unit_id, 30, "python-lifecycle")
    with pytest.raises(RuntimeError):
        neo4j_client.inactivate_unit(tenant_id, unit_id)
    neo4j_client.unlock_unit(tenant_id, unit_id)
    neo4j_client.inactivate_unit(tenant_id, unit_id)
    assert json.loads(neo4j_client.get_unit_json(tenant_id, unit_id))["status"] == 10
    neo4j_client.activate_unit(tenant_id, unit_id)

    attr_suffix = uuid.uuid4().hex[:8]
    attr_name = f"py_it_attr_neo4j_{attr_suffix}"
    created_attr = json.loads(
        neo4j_client.create_attribute(
            f"py_it_attr_alias_neo4j_{attr_suffix}",
            attr_name,
            f"py.it.attr.neo4j.{attr_suffix}",
            "string",
            False,
        )
    )
    assert neo4j_client.can_change_attribute(attr_name)
    instantiated_attr = neo4j_client.instantiate_attribute(attr_name)
    assert instantiated_attr is not None
    assert json.loads(instantiated_attr)["id"] == created_attr["id"]

    wildcard_unit = json.loads(
        neo4j_client.store_unit_json(
            json.dumps(
                {
                    "tenantid": tenant_id,
                    "corrid": str(uuid.uuid4()),
                    "status": 30,
                    "unitname": f"py-it-attr-wild-neo4j-{uuid.uuid4().hex[:8]}",
                    "attributes": [
                        {
                            "attrid": created_attr["id"],
                            "attrname": attr_name,
                            "attrtype": 1,
                            "value": ["Neo4j-Wild-Value"],
                        }
                    ],
                }
            )
        )
    )
    wildcard_search = json.loads(
        neo4j_client.search_units(
            json.dumps(
                {
                    "tenantid": tenant_id,
                    "attribute_eq": {
                        "name_or_id": attr_name,
                        "value": "*wild-value",
                    },
                }
            ),
            "created",
            True,
            50,
            0,
        )
    )
    wildcard_ids = {u["unit_id"] for u in wildcard_search["results"]}
    assert wildcard_unit["unitid"] in wildcard_ids

    query_search = json.loads(
        neo4j_client.search_units_query(
            f"tenantid = {tenant_id} and corrid = '{corrid}'",
            "created",
            True,
            10,
            0,
        )
    )
    assert query_search["total_hits"] == 1

    with pytest.raises(RuntimeError):
        neo4j_client.search_units_query_strict(
            "custom_attr = 'x'",
            "created",
            True,
            10,
            0,
        )

    strict_query_search = json.loads(
        neo4j_client.search_units_query_strict(
            f"tenantid = {tenant_id} and attr:{attr_name} = '*wild-value'",
            "created",
            True,
            10,
            0,
        )
    )
    strict_ids = {u["unit_id"] for u in strict_query_search["results"]}
    assert wildcard_unit["unitid"] in strict_ids

    numeric_attr_suffix = uuid.uuid4().hex[:8]
    numeric_attr_name = f"py_it_attr_num_neo4j_{numeric_attr_suffix}"
    numeric_attr = json.loads(
        neo4j_client.create_attribute(
            f"py_it_attr_num_alias_neo4j_{numeric_attr_suffix}",
            numeric_attr_name,
            f"py.it.attr.num.neo4j.{numeric_attr_suffix}",
            "integer",
            False,
        )
    )
    numeric_attr_id = numeric_attr["id"]
    numeric_low = json.loads(
        neo4j_client.store_unit_json(
            json.dumps(
                {
                    "tenantid": tenant_id,
                    "corrid": str(uuid.uuid4()),
                    "status": 30,
                    "unitname": f"py-it-attr-num-low-neo4j-{uuid.uuid4().hex[:8]}",
                    "attributes": [
                        {
                            "attrid": numeric_attr_id,
                            "attrname": numeric_attr_name,
                            "attrtype": 3,
                            "value": [10],
                        }
                    ],
                }
            )
        )
    )
    numeric_high = json.loads(
        neo4j_client.store_unit_json(
            json.dumps(
                {
                    "tenantid": tenant_id,
                    "corrid": str(uuid.uuid4()),
                    "status": 30,
                    "unitname": f"py-it-attr-num-high-neo4j-{uuid.uuid4().hex[:8]}",
                    "attributes": [
                        {
                            "attrid": numeric_attr_id,
                            "attrname": numeric_attr_name,
                            "attrtype": 3,
                            "value": [20],
                        }
                    ],
                }
            )
        )
    )
    numeric_cmp = json.loads(
        neo4j_client.search_units(
            json.dumps(
                {
                    "tenantid": tenant_id,
                    "attribute_cmp": {
                        "name_or_id": str(numeric_attr_id),
                        "op": "gt",
                        "value_type": "number",
                        "value": "15",
                    },
                }
            ),
            "created",
            True,
            50,
            0,
        )
    )
    numeric_ids = {u["unit_id"] for u in numeric_cmp["results"]}
    assert numeric_high["unitid"] in numeric_ids
    assert numeric_low["unitid"] not in numeric_ids

    time_attr_suffix = uuid.uuid4().hex[:8]
    time_attr_name = f"py_it_attr_time_neo4j_{time_attr_suffix}"
    time_attr = json.loads(
        neo4j_client.create_attribute(
            f"py_it_attr_time_alias_neo4j_{time_attr_suffix}",
            time_attr_name,
            f"py.it.attr.time.neo4j.{time_attr_suffix}",
            "time",
            False,
        )
    )
    time_attr_id = time_attr["id"]
    time_low = json.loads(
        neo4j_client.store_unit_json(
            json.dumps(
                {
                    "tenantid": tenant_id,
                    "corrid": str(uuid.uuid4()),
                    "status": 30,
                    "unitname": f"py-it-attr-time-low-neo4j-{uuid.uuid4().hex[:8]}",
                    "attributes": [
                        {
                            "attrid": time_attr_id,
                            "attrname": time_attr_name,
                            "attrtype": 2,
                            "value": ["2025-01-01T00:00:00Z"],
                        }
                    ],
                }
            )
        )
    )
    time_high = json.loads(
        neo4j_client.store_unit_json(
            json.dumps(
                {
                    "tenantid": tenant_id,
                    "corrid": str(uuid.uuid4()),
                    "status": 30,
                    "unitname": f"py-it-attr-time-high-neo4j-{uuid.uuid4().hex[:8]}",
                    "attributes": [
                        {
                            "attrid": time_attr_id,
                            "attrname": time_attr_name,
                            "attrtype": 2,
                            "value": ["2025-01-02T00:00:00Z"],
                        }
                    ],
                }
            )
        )
    )
    time_cmp = json.loads(
        neo4j_client.search_units(
            json.dumps(
                {
                    "tenantid": tenant_id,
                    "attribute_cmp": {
                        "name_or_id": str(time_attr_id),
                        "op": "gt",
                        "value_type": "time",
                        "value": "1735732800000",
                    },
                }
            ),
            "created",
            True,
            50,
            0,
        )
    )
    time_ids = {u["unit_id"] for u in time_cmp["results"]}
    assert time_high["unitid"] in time_ids
    assert time_low["unitid"] not in time_ids

    with pytest.raises(RuntimeError):
        neo4j_client.search_units(
            json.dumps(
                {
                    "tenantid": tenant_id,
                    "attribute_cmp": {
                        "name_or_id": str(numeric_attr_id),
                        "op": "gt",
                        "value_type": "boolean",
                        "value": True,
                    },
                }
            ),
            "created",
            True,
            50,
            0,
        )

    with pytest.raises(RuntimeError):
        neo4j_client.search_units(
            json.dumps(
                {
                    "tenantid": tenant_id,
                    "attribute_cmp": {
                        "name_or_id": str(numeric_attr_id),
                        "op": "gt",
                        "value_type": "number",
                        "value": "NaN",
                    },
                }
            ),
            "created",
            True,
            50,
            0,
        )

    with pytest.raises(RuntimeError):
        neo4j_client.search_units(
            json.dumps(
                {
                    "tenantid": tenant_id,
                    "attribute_cmp": {
                        "name_or_id": str(time_attr_id),
                        "op": "gt",
                        "value_type": "time",
                        "value": "999999999999999999",
                    },
                }
            ),
            "created",
            True,
            50,
            0,
        )

    neo4j_client.set_status(tenant_id, unit_id, 30)
    assert neo4j_client.request_status_transition(tenant_id, unit_id, 10) == 10
    assert neo4j_client.request_status_transition(tenant_id, unit_id, 30) == 10
    neo4j_client.activate_unit(tenant_id, unit_id)
    assert json.loads(neo4j_client.get_unit_json(tenant_id, unit_id))["status"] == 30
    neo4j_client.set_status(tenant_id, unit_id, 10)
    assert neo4j_client.request_status_transition(tenant_id, unit_id, 1) == 1
    assert neo4j_client.request_status_transition(tenant_id, unit_id, 30) == 1
    with pytest.raises(RuntimeError):
        neo4j_client.request_status_transition(tenant_id, unit_id, 999)
    assert json.loads(neo4j_client.get_unit_json(tenant_id, unit_id))["status"] == 1
    assert not neo4j_client.is_unit_locked(tenant_id, unit_id)
    neo4j_client.lock_unit(tenant_id, unit_id, 31, "python-lifecycle-check")
    assert neo4j_client.is_unit_locked(tenant_id, unit_id)
    neo4j_client.unlock_unit(tenant_id, unit_id)
    assert not neo4j_client.is_unit_locked(tenant_id, unit_id)

    search = json.loads(
        neo4j_client.search_units(
            json.dumps({"tenantid": tenant_id, "name_ilike": f"%{unit_name}%"}),
            "created",
            True,
            20,
            0,
        )
    )
    assert search["total_hits"] >= 1

    gql_exists = json.loads(
        neo4j_client.graphql_execute(
            "query UnitExists { unitExists }",
            json.dumps({"tenantid": tenant_id, "unitid": unit_id}),
        )
    )
    assert gql_exists["data"]["unitExists"] is True
    gql_lock = json.loads(
        neo4j_client.graphql_execute(
            "query IsUnitLocked { isUnitLocked }",
            json.dumps({"tenantid": tenant_id, "unitid": unit_id}),
        )
    )
    assert gql_lock["data"]["isUnitLocked"] is False
    gql_flush = json.loads(
        neo4j_client.graphql_execute("mutation FlushCache { flushCache { ok } }")
    )
    assert gql_flush["data"]["flushCache"]["ok"] is True
    gql_store_name = f"py-it-gql-store-neo4j-{uuid.uuid4().hex[:8]}"
    gql_store = json.loads(
        neo4j_client.graphql_execute(
            "mutation StoreUnit { storeUnit { tenantid unitid unitver status unitname } }",
            json.dumps(
                {
                    "unit": json.loads(_unit_payload(tenant_id, gql_store_name)),
                }
            ),
        )
    )
    assert gql_store["data"]["storeUnit"]["tenantid"] == tenant_id
    assert gql_store["data"]["storeUnit"]["unitver"] == 1
    assert gql_store["data"]["storeUnit"]["unitname"] == gql_store_name
    gql_attr_suffix = uuid.uuid4().hex[:8]
    gql_attr_name = f"py_it_attr_gql_neo4j_{gql_attr_suffix}"
    gql_create_attr = json.loads(
        neo4j_client.graphql_execute(
            "mutation CreateAttribute { createAttribute { id name alias qualname } }",
            json.dumps(
                {
                    "alias": f"py_it_attr_gql_alias_neo4j_{gql_attr_suffix}",
                    "name": gql_attr_name,
                    "qualname": f"py.it.attr.gql.neo4j.{gql_attr_suffix}",
                    "attributeType": "string",
                    "isArray": False,
                }
            ),
        )
    )
    assert gql_create_attr["data"]["createAttribute"]["name"] == gql_attr_name
    gql_allow_health = json.loads(
        neo4j_client.graphql_execute_allowlist(
            "query Health { health { status } }",
            ["health"],
        )
    )
    assert gql_allow_health["data"]["health"]["status"] == "ok"
    gql_allow_block = json.loads(
        neo4j_client.graphql_execute_allowlist(
            "query UnitExists { unitExists }",
            ["health"],
            json.dumps({"tenantid": tenant_id, "unitid": unit_id}),
        )
    )
    assert gql_allow_block["data"] is None
    assert gql_allow_block["errors"][0]["extensions"]["code"] == "UNSUPPORTED"
    gql_registered = json.loads(
        neo4j_client.graphql_execute(
            "query RegisteredOperations { registeredOperations { queries mutations } }"
        )
    )
    assert "searchUnits" in gql_registered["data"]["registeredOperations"]["queries"]
    gql_registered_allow = json.loads(
        neo4j_client.graphql_execute_allowlist(
            "query RegisteredOperations { registeredOperations { queries mutations } }",
            ["registered_operations", "health"],
        )
    )
    assert gql_registered_allow["data"]["registeredOperations"]["queries"] == [
        "registeredOperations",
        "health",
    ]

    gql_search_query = json.loads(
        neo4j_client.graphql_execute(
            "query SearchUnitsQuery { searchUnitsQuery { totalHits } }",
            json.dumps(
                {
                    "query": f"tenantid = {tenant_id} and corrid = '{corrid}'",
                    "order": {"field": "created", "descending": True},
                    "paging": {"limit": 10, "offset": 0},
                }
            ),
        )
    )
    assert gql_search_query["data"]["searchUnitsQuery"]["totalHits"] == 1

    gql_search_query_allow = json.loads(
        neo4j_client.graphql_execute_allowlist(
            "query SearchUnitsQuery { searchUnitsQuery { totalHits } }",
            ["search_units_query"],
            json.dumps(
                {
                    "query": f"tenantid = {tenant_id} and corrid = '{corrid}'",
                    "order": {"field": "created", "descending": True},
                    "paging": {"limit": 10, "offset": 0},
                }
            ),
        )
    )
    assert gql_search_query_allow["data"]["searchUnitsQuery"]["totalHits"] == 1

    gql_search_query_allow_block = json.loads(
        neo4j_client.graphql_execute_allowlist(
            "query SearchUnitsQuery { searchUnitsQuery { totalHits } }",
            ["health"],
            json.dumps(
                {
                    "query": f"tenantid = {tenant_id} and corrid = '{corrid}'",
                }
            ),
        )
    )
    assert gql_search_query_allow_block["data"] is None
    gql_search_query_allow_block_ext = gql_search_query_allow_block["errors"][0]["extensions"]
    assert gql_search_query_allow_block_ext["code"] == "UNSUPPORTED"
    assert gql_search_query_allow_block_ext["operationType"] == "query"
    assert gql_search_query_allow_block_ext["operation"] == "searchUnitsQuery"
    assert "health" in gql_search_query_allow_block_ext["supportedOperations"]

    gql_search_query_strict = json.loads(
        neo4j_client.graphql_execute(
            "query SearchUnitsQueryStrict { searchUnitsQueryStrict { totalHits } }",
            json.dumps(
                {
                    "query": f"tenantid = {tenant_id} and attr:{attr_name} = '*wild-value'",
                    "order": {"field": "created", "descending": True},
                    "paging": {"limit": 10, "offset": 0},
                }
            ),
        )
    )
    assert gql_search_query_strict["data"]["searchUnitsQueryStrict"]["totalHits"] >= 1

    gql_instantiate_mut = json.loads(
        neo4j_client.graphql_execute(
            "mutation InstantiateAttributeMutation { instantiateAttributeMutation { id name } }",
            json.dumps({"nameOrId": attr_name}),
        )
    )
    assert gql_instantiate_mut["data"]["instantiateAttributeMutation"]["id"] == created_attr["id"]

    gql_instantiate_mut_allow_block = json.loads(
        neo4j_client.graphql_execute_allowlist(
            "mutation InstantiateAttributeMutation { instantiateAttributeMutation { id name } }",
            ["health"],
            json.dumps({"nameOrId": attr_name}),
        )
    )
    assert gql_instantiate_mut_allow_block["data"] is None
    gql_instantiate_mut_allow_block_ext = gql_instantiate_mut_allow_block["errors"][0]["extensions"]
    assert gql_instantiate_mut_allow_block_ext["code"] == "UNSUPPORTED"
    assert gql_instantiate_mut_allow_block_ext["operationType"] == "mutation"
    assert gql_instantiate_mut_allow_block_ext["operation"] == "instantiateAttributeMutation"
    assert "health" in gql_instantiate_mut_allow_block_ext["supportedOperations"]
