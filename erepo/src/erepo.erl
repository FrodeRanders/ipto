-module(erepo).

-export([
    start_link/0,
    create_unit/1,
    create_unit/2,
    get_unit/2,
    get_unit/3,
    unit_exists/2,
    store_unit/1,
    store_unit_json/1,
    search_units/3,
    add_relation/3,
    remove_relation/3,
    add_association/3,
    remove_association/3,
    lock_unit/3,
    unlock_unit/1,
    activate_unit/1,
    inactivate_unit/1,
    create_attribute/5,
    get_attribute_info/1,
    get_tenant_info/1,
    sync/0
]).

start_link() ->
    case application:ensure_all_started(erepo) of
        {ok, _} = Ok ->
            Ok;
        {error, {erepo, {"no such file or directory", "erepo.app"}}} ->
            case erepo_sup:start_link() of
                {ok, _} = Ok ->
                    Ok;
                {error, {already_started, _Pid}} ->
                    {ok, erepo};
                Error ->
                    Error
            end;
        {error, {already_started, erepo}} ->
            {ok, erepo};
        Error ->
            Error
    end.

create_unit(TenantId) ->
    erepo_repo:create_unit(TenantId).

create_unit(TenantId, NameOrCorrId) ->
    erepo_repo:create_unit(TenantId, NameOrCorrId).

get_unit(TenantId, UnitId) ->
    erepo_repo:get_unit(TenantId, UnitId).

get_unit(TenantId, UnitId, Version) ->
    erepo_repo:get_unit(TenantId, UnitId, Version).

unit_exists(TenantId, UnitId) ->
    erepo_repo:unit_exists(TenantId, UnitId).

store_unit(Unit) ->
    erepo_repo:store_unit(Unit).

store_unit_json(UnitJsonMap) ->
    erepo_repo:store_unit_json(UnitJsonMap).

search_units(Expression, Order, PagingOrLimit) ->
    erepo_repo:search_units(Expression, Order, PagingOrLimit).

add_relation(UnitRef, RelType, OtherUnitRef) ->
    erepo_repo:add_relation(UnitRef, RelType, OtherUnitRef).

remove_relation(UnitRef, RelType, OtherUnitRef) ->
    erepo_repo:remove_relation(UnitRef, RelType, OtherUnitRef).

add_association(UnitRef, AssocType, RefString) ->
    erepo_repo:add_association(UnitRef, AssocType, RefString).

remove_association(UnitRef, AssocType, RefString) ->
    erepo_repo:remove_association(UnitRef, AssocType, RefString).

lock_unit(UnitRef, LockType, Purpose) ->
    erepo_repo:lock_unit(UnitRef, LockType, Purpose).

unlock_unit(UnitRef) ->
    erepo_repo:unlock_unit(UnitRef).

activate_unit(UnitRef) ->
    erepo_repo:activate_unit(UnitRef).

inactivate_unit(UnitRef) ->
    erepo_repo:inactivate_unit(UnitRef).

create_attribute(Alias, Name, QualName, Type, IsArray) ->
    erepo_repo:create_attribute(Alias, Name, QualName, Type, IsArray).

get_attribute_info(NameOrId) ->
    erepo_repo:get_attribute_info(NameOrId).

get_tenant_info(NameOrId) ->
    erepo_repo:get_tenant_info(NameOrId).

sync() ->
    erepo_repo:sync().
