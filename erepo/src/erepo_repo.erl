-module(erepo_repo).

-include("erepo.hrl").

-export([
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

create_unit(TenantId) when is_integer(TenantId), TenantId > 0 ->
    {ok, erepo_unit:new(TenantId, undefined, erepo_unit:make_corrid())};
create_unit(_TenantId) ->
    {error, invalid_tenantid}.

create_unit(TenantId, NameOrCorrId) when is_integer(TenantId), TenantId > 0 ->
    case NameOrCorrId of
        {corrid, Corr} when is_binary(Corr) ->
            {ok, erepo_unit:new(TenantId, undefined, Corr)};
        Name when is_binary(Name); is_list(Name) ->
            {ok, erepo_unit:new(TenantId, normalize_name(Name), erepo_unit:make_corrid())};
        _ ->
            {error, invalid_create_unit_argument}
    end;
create_unit(_TenantId, _NameOrCorrId) ->
    {error, invalid_tenantid}.

get_unit(TenantId, UnitId) ->
    erepo_db:get_unit_json(TenantId, UnitId, latest).

get_unit(TenantId, UnitId, Version) ->
    erepo_db:get_unit_json(TenantId, UnitId, Version).

unit_exists(TenantId, UnitId) ->
    erepo_db:unit_exists(TenantId, UnitId).

store_unit(Unit = #unit{}) ->
    store_unit_json(erepo_unit:to_persist_map(Unit));
store_unit(_Unit) ->
    {error, invalid_unit}.

store_unit_json(UnitMap) when is_map(UnitMap) ->
    case erepo_db:store_unit_json(UnitMap) of
        {ok, StoredMap} ->
            maybe_emit_event(updated, StoredMap),
            {ok, StoredMap};
        Error ->
            Error
    end;
store_unit_json(_UnitMap) ->
    {error, invalid_unit_json}.

search_units(Expression, Order, PagingOrLimit) ->
    erepo_search:search(Expression, Order, PagingOrLimit).

add_relation(UnitRef, RelType, OtherUnitRef) ->
    erepo_rel:add(UnitRef, RelType, OtherUnitRef).

remove_relation(UnitRef, RelType, OtherUnitRef) ->
    erepo_rel:remove(UnitRef, RelType, OtherUnitRef).

add_association(UnitRef, AssocType, RefString) ->
    erepo_assoc:add(UnitRef, AssocType, RefString).

remove_association(UnitRef, AssocType, RefString) ->
    erepo_assoc:remove(UnitRef, AssocType, RefString).

lock_unit(UnitRef, LockType, Purpose) ->
    erepo_lock:lock(UnitRef, LockType, Purpose).

unlock_unit(UnitRef) ->
    erepo_lock:unlock(UnitRef).

activate_unit(UnitRef) ->
    transition_status(UnitRef, ?STATUS_EFFECTIVE).

inactivate_unit(UnitRef) ->
    transition_status(UnitRef, ?STATUS_PENDING_DELETION).

create_attribute(Alias, Name, QualName, Type, IsArray) ->
    erepo_attr:create_attribute(Alias, Name, QualName, Type, IsArray).

get_attribute_info(NameOrId) ->
    erepo_attr:get_attribute_info(NameOrId).

get_tenant_info(NameOrId) ->
    erepo_db:get_tenant_info(NameOrId).

sync() ->
    erepo_attr:sync().

normalize_name(Name) when is_binary(Name) ->
    Name;
normalize_name(Name) when is_list(Name) ->
    unicode:characters_to_binary(Name).

maybe_emit_event(Action, Payload) ->
    erepo_event:emit(Action, Payload).

transition_status(#{tenantid := TenantId, unitid := UnitId}, RequestedStatus) ->
    transition_status({TenantId, UnitId}, RequestedStatus);
transition_status({TenantId, UnitId}, RequestedStatus) ->
    case erepo_db:get_unit_json(TenantId, UnitId, latest) of
        {ok, UnitMap} ->
            CurrentStatus = maps:get(status, UnitMap, ?STATUS_EFFECTIVE),
            case allowed_transition(CurrentStatus, RequestedStatus) of
                true -> erepo_db:set_status({TenantId, UnitId}, RequestedStatus);
                false -> ok
            end;
        not_found ->
            {error, not_found};
        Error ->
            Error
    end;
transition_status(_UnitRef, _RequestedStatus) ->
    {error, invalid_unit_ref}.

allowed_transition(?STATUS_ARCHIVED, _Requested) ->
    false;
allowed_transition(?STATUS_EFFECTIVE, ?STATUS_PENDING_DELETION) ->
    true;
allowed_transition(?STATUS_EFFECTIVE, ?STATUS_PENDING_DISPOSITION) ->
    true;
allowed_transition(?STATUS_PENDING_DELETION, ?STATUS_PENDING_DISPOSITION) ->
    true;
allowed_transition(?STATUS_PENDING_DELETION, ?STATUS_EFFECTIVE) ->
    true;
allowed_transition(?STATUS_OBLITERATED, ?STATUS_PENDING_DISPOSITION) ->
    true;
allowed_transition(?STATUS_OBLITERATED, ?STATUS_EFFECTIVE) ->
    true;
allowed_transition(_Current, _Requested) ->
    false.
