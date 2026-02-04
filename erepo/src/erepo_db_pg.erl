-module(erepo_db_pg).
-behaviour(erepo_backend).

-export([
    get_unit_json/3,
    unit_exists/2,
    store_unit_json/1,
    search_units/3,
    add_relation/3,
    remove_relation/3,
    add_association/3,
    remove_association/3,
    lock_unit/3,
    unlock_unit/1,
    set_status/2,
    create_attribute/5,
    get_attribute_info/1,
    get_tenant_info/1
]).

get_unit_json(TenantId, UnitId, Version) ->
    erepo_db:pg_get_unit_json_backend(TenantId, UnitId, Version).

unit_exists(TenantId, UnitId) ->
    erepo_db:pg_unit_exists_backend(TenantId, UnitId).

store_unit_json(UnitMap) ->
    erepo_db:pg_store_unit_json_backend(UnitMap).

search_units(Expression, Order, PagingOrLimit) ->
    erepo_db:pg_search_units_backend(Expression, Order, PagingOrLimit).

add_relation(UnitRef, RelType, OtherUnitRef) ->
    erepo_db:pg_add_relation_backend(UnitRef, RelType, OtherUnitRef).

remove_relation(UnitRef, RelType, OtherUnitRef) ->
    erepo_db:pg_remove_relation_backend(UnitRef, RelType, OtherUnitRef).

add_association(UnitRef, AssocType, RefString) ->
    erepo_db:pg_add_association_backend(UnitRef, AssocType, RefString).

remove_association(UnitRef, AssocType, RefString) ->
    erepo_db:pg_remove_association_backend(UnitRef, AssocType, RefString).

lock_unit(UnitRef, LockType, Purpose) ->
    erepo_db:pg_lock_unit_backend(UnitRef, LockType, Purpose).

unlock_unit(UnitRef) ->
    erepo_db:pg_unlock_unit_backend(UnitRef).

set_status(UnitRef, Status) ->
    erepo_db:pg_set_status_backend(UnitRef, Status).

create_attribute(Alias, Name, QualName, Type, IsArray) ->
    erepo_db:pg_create_attribute_backend(Alias, Name, QualName, Type, IsArray).

get_attribute_info(NameOrId) ->
    erepo_db:pg_get_attribute_info_backend(NameOrId).

get_tenant_info(NameOrId) ->
    erepo_db:pg_get_tenant_info_backend(NameOrId).
