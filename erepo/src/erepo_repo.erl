%%% Copyright (C) 2026 Frode Randers
%%% All rights reserved
%%%
%%% This file is part of IPTO.
%%%
%%% Licensed under the Apache License, Version 2.0 (the "License");
%%% you may not use this file except in compliance with the License.
%%% You may obtain a copy of the License at
%%%
%%%    http://www.apache.org/licenses/LICENSE-2.0
%%%
%%% Unless required by applicable law or agreed to in writing, software
%%% distributed under the License is distributed on an "AS IS" BASIS,
%%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%% See the License for the specific language governing permissions and
%%% limitations under the License.
%%%
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

%% --------------------------------------------------------------------
%% create_unit/{1,2}
%%
%% Creates an in-memory unit record to be stored later.
%% --------------------------------------------------------------------
-spec create_unit(tenantid()) -> {ok, #unit{}} | {error, invalid_tenantid}.
create_unit(TenantId) when is_integer(TenantId), TenantId > 0 ->
    {ok, erepo_unit:new(TenantId, undefined, erepo_unit:make_corrid())};
create_unit(_TenantId) ->
    {error, invalid_tenantid}.

-spec create_unit(tenantid(), binary() | string() | {corrid, binary()}) ->
    {ok, #unit{}} | {error, invalid_tenantid | invalid_create_unit_argument}.
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

-spec get_unit(tenantid(), unitid()) -> unit_lookup_result().
get_unit(TenantId, UnitId) ->
    erepo_db:get_unit_json(TenantId, UnitId, latest).

-spec get_unit(tenantid(), unitid(), version_selector()) -> unit_lookup_result().
get_unit(TenantId, UnitId, Version) ->
    erepo_db:get_unit_json(TenantId, UnitId, Version).

-spec unit_exists(tenantid(), unitid()) -> boolean().
unit_exists(TenantId, UnitId) ->
    erepo_db:unit_exists(TenantId, UnitId).

-spec store_unit(#unit{}) -> erepo_result(unit_map()) | {error, invalid_unit}.
store_unit(Unit = #unit{}) ->
    store_unit_json(erepo_unit:to_persist_map(Unit));
store_unit(_Unit) ->
    {error, invalid_unit}.

-spec store_unit_json(unit_map()) -> erepo_result(unit_map()) | {error, invalid_unit_json}.
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

-spec search_units(search_expression() | map(), search_order(), search_paging()) -> erepo_result(search_result()).
search_units(Expression, Order, PagingOrLimit) ->
    erepo_search:search(Expression, Order, PagingOrLimit).

-spec add_relation(unit_ref_value(), relation_type(), unit_ref_value()) -> ok | {error, erepo_reason()}.
add_relation(UnitRef, RelType, OtherUnitRef) ->
    erepo_rel:add(UnitRef, RelType, OtherUnitRef).

-spec remove_relation(unit_ref_value(), relation_type(), unit_ref_value()) -> ok | {error, erepo_reason()}.
remove_relation(UnitRef, RelType, OtherUnitRef) ->
    erepo_rel:remove(UnitRef, RelType, OtherUnitRef).

-spec add_association(unit_ref_value(), association_type(), ref_string()) -> ok | {error, erepo_reason()}.
add_association(UnitRef, AssocType, RefString) ->
    erepo_assoc:add(UnitRef, AssocType, RefString).

-spec remove_association(unit_ref_value(), association_type(), ref_string()) -> ok | {error, erepo_reason()}.
remove_association(UnitRef, AssocType, RefString) ->
    erepo_assoc:remove(UnitRef, AssocType, RefString).

-spec lock_unit(unit_ref_value(), lock_type(), ref_string()) -> ok | already_locked | {error, erepo_reason()}.
lock_unit(UnitRef, LockType, Purpose) ->
    erepo_lock:lock(UnitRef, LockType, Purpose).

-spec unlock_unit(unit_ref_value()) -> ok | {error, erepo_reason()}.
unlock_unit(UnitRef) ->
    erepo_lock:unlock(UnitRef).

-spec activate_unit(unit_ref_value()) -> ok | {error, erepo_reason()}.
activate_unit(UnitRef) ->
    transition_status(UnitRef, ?STATUS_EFFECTIVE).

-spec inactivate_unit(unit_ref_value()) -> ok | {error, erepo_reason()}.
inactivate_unit(UnitRef) ->
    transition_status(UnitRef, ?STATUS_PENDING_DELETION).

-spec create_attribute(attribute_alias(), attribute_name(), attribute_qualname(), attribute_type(), boolean()) ->
    erepo_result(attribute_info()).
create_attribute(Alias, Name, QualName, Type, IsArray) ->
    erepo_attr:create_attribute(Alias, Name, QualName, Type, IsArray).

-spec get_attribute_info(name_or_id()) -> {ok, attribute_info()} | not_found | {error, erepo_reason()}.
get_attribute_info(NameOrId) ->
    erepo_attr:get_attribute_info(NameOrId).

-spec get_tenant_info(name_or_id()) -> {ok, tenant_info()} | not_found | {error, erepo_reason()}.
get_tenant_info(NameOrId) ->
    erepo_db:get_tenant_info(NameOrId).

-spec sync() -> ok.
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
