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
-module(ipto_repo).

-include("ipto.hrl").

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
    get_right_relation/2,
    get_right_relations/2,
    get_left_relations/2,
    count_right_relations/2,
    count_left_relations/2,
    add_association/3,
    remove_association/3,
    get_right_association/2,
    get_right_associations/2,
    get_left_associations/2,
    count_right_associations/2,
    count_left_associations/2,
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
    {ok, ipto_unit:new(TenantId, undefined, ipto_unit:make_corrid())};
create_unit(_TenantId) ->
    {error, invalid_tenantid}.

-spec create_unit(tenantid(), binary() | string() | {corrid, binary()}) ->
    {ok, #unit{}} | {error, invalid_tenantid | invalid_create_unit_argument}.
create_unit(TenantId, NameOrCorrId) when is_integer(TenantId), TenantId > 0 ->
    case NameOrCorrId of
        {corrid, Corr} when is_binary(Corr) ->
            {ok, ipto_unit:new(TenantId, undefined, Corr)};
        Name when is_binary(Name); is_list(Name) ->
            {ok, ipto_unit:new(TenantId, normalize_name(Name), ipto_unit:make_corrid())};
        _ ->
            {error, invalid_create_unit_argument}
    end;
create_unit(_TenantId, _NameOrCorrId) ->
    {error, invalid_tenantid}.

-spec get_unit(tenantid(), unitid()) -> unit_lookup_result().
get_unit(TenantId, UnitId) ->
    ipto_db:get_unit_json(TenantId, UnitId, latest).

-spec get_unit(tenantid(), unitid(), version_selector()) -> unit_lookup_result().
get_unit(TenantId, UnitId, Version) ->
    ipto_db:get_unit_json(TenantId, UnitId, Version).

-spec unit_exists(tenantid(), unitid()) -> boolean().
unit_exists(TenantId, UnitId) ->
    ipto_db:unit_exists(TenantId, UnitId).

-spec store_unit(#unit{}) -> ipto_result(unit_map()) | {error, invalid_unit}.
store_unit(Unit = #unit{}) ->
    store_unit_json(ipto_unit:to_persist_map(Unit));
store_unit(_Unit) ->
    {error, invalid_unit}.

-spec store_unit_json(unit_map()) -> ipto_result(unit_map()) | {error, invalid_unit_json}.
store_unit_json(UnitMap) when is_map(UnitMap) ->
    case ipto_db:store_unit_json(UnitMap) of
        {ok, StoredMap} ->
            maybe_emit_event(updated, StoredMap),
            {ok, StoredMap};
        Error ->
            Error
    end;
store_unit_json(_UnitMap) ->
    {error, invalid_unit_json}.

-spec search_units(search_expression() | map(), search_order(), search_paging()) -> ipto_result(search_result()).
search_units(Expression, Order, PagingOrLimit) ->
    ipto_search:search(Expression, Order, PagingOrLimit).

-spec add_relation(unit_ref_value(), relation_type(), unit_ref_value()) -> ok | {error, ipto_reason()}.
add_relation(UnitRef, RelType, OtherUnitRef) ->
    ipto_rel:add(UnitRef, RelType, OtherUnitRef).

-spec remove_relation(unit_ref_value(), relation_type(), unit_ref_value()) -> ok | {error, ipto_reason()}.
remove_relation(UnitRef, RelType, OtherUnitRef) ->
    ipto_rel:remove(UnitRef, RelType, OtherUnitRef).

-spec get_right_relation(unit_ref_value(), relation_type()) -> relation_lookup_result().
get_right_relation(UnitRef, RelType) ->
    ipto_rel:get_right_one(UnitRef, RelType).

-spec get_right_relations(unit_ref_value(), relation_type()) -> ipto_result([relation()]).
get_right_relations(UnitRef, RelType) ->
    ipto_rel:get_right(UnitRef, RelType).

-spec get_left_relations(unit_ref_value(), relation_type()) -> ipto_result([relation()]).
get_left_relations(UnitRef, RelType) ->
    ipto_rel:get_left(UnitRef, RelType).

-spec count_right_relations(unit_ref_value(), relation_type()) -> ipto_result(non_neg_integer()).
count_right_relations(UnitRef, RelType) ->
    ipto_rel:count_right(UnitRef, RelType).

-spec count_left_relations(unit_ref_value(), relation_type()) -> ipto_result(non_neg_integer()).
count_left_relations(UnitRef, RelType) ->
    ipto_rel:count_left(UnitRef, RelType).

-spec add_association(unit_ref_value(), association_type(), ref_string()) -> ok | {error, ipto_reason()}.
add_association(UnitRef, AssocType, RefString) ->
    ipto_assoc:add(UnitRef, AssocType, RefString).

-spec remove_association(unit_ref_value(), association_type(), ref_string()) -> ok | {error, ipto_reason()}.
remove_association(UnitRef, AssocType, RefString) ->
    ipto_assoc:remove(UnitRef, AssocType, RefString).

-spec get_right_association(unit_ref_value(), association_type()) -> association_lookup_result().
get_right_association(UnitRef, AssocType) ->
    ipto_assoc:get_right_one(UnitRef, AssocType).

-spec get_right_associations(unit_ref_value(), association_type()) -> ipto_result([association()]).
get_right_associations(UnitRef, AssocType) ->
    ipto_assoc:get_right(UnitRef, AssocType).

-spec get_left_associations(association_type(), ref_string()) -> ipto_result([association()]).
get_left_associations(AssocType, RefString) ->
    ipto_assoc:get_left(AssocType, RefString).

-spec count_right_associations(unit_ref_value(), association_type()) -> ipto_result(non_neg_integer()).
count_right_associations(UnitRef, AssocType) ->
    ipto_assoc:count_right(UnitRef, AssocType).

-spec count_left_associations(association_type(), ref_string()) -> ipto_result(non_neg_integer()).
count_left_associations(AssocType, RefString) ->
    ipto_assoc:count_left(AssocType, RefString).

-spec lock_unit(unit_ref_value(), lock_type(), ref_string()) -> ok | already_locked | {error, ipto_reason()}.
lock_unit(UnitRef, LockType, Purpose) ->
    ipto_lock:lock(UnitRef, LockType, Purpose).

-spec unlock_unit(unit_ref_value()) -> ok | {error, ipto_reason()}.
unlock_unit(UnitRef) ->
    ipto_lock:unlock(UnitRef).

-spec activate_unit(unit_ref_value()) -> ok | {error, ipto_reason()}.
activate_unit(UnitRef) ->
    transition_status(UnitRef, ?STATUS_EFFECTIVE).

-spec inactivate_unit(unit_ref_value()) -> ok | {error, ipto_reason()}.
inactivate_unit(UnitRef) ->
    transition_status(UnitRef, ?STATUS_PENDING_DELETION).

-spec create_attribute(attribute_alias(), attribute_name(), attribute_qualname(), attribute_type(), boolean()) ->
    ipto_result(attribute_info()).
create_attribute(Alias, Name, QualName, Type, IsArray) ->
    ipto_attr:create_attribute(Alias, Name, QualName, Type, IsArray).

-spec get_attribute_info(name_or_id()) -> {ok, attribute_info()} | not_found | {error, ipto_reason()}.
get_attribute_info(NameOrId) ->
    ipto_attr:get_attribute_info(NameOrId).

-spec get_tenant_info(name_or_id()) -> {ok, tenant_info()} | not_found | {error, ipto_reason()}.
get_tenant_info(NameOrId) ->
    ipto_db:get_tenant_info(NameOrId).

-spec sync() -> ok.
sync() ->
    ipto_attr:sync().

normalize_name(Name) when is_binary(Name) ->
    Name;
normalize_name(Name) when is_list(Name) ->
    unicode:characters_to_binary(Name).

maybe_emit_event(Action, Payload) ->
    ipto_event:emit(Action, Payload).

transition_status(#{tenantid := TenantId, unitid := UnitId}, RequestedStatus) ->
    transition_status({TenantId, UnitId}, RequestedStatus);
transition_status({TenantId, UnitId}, RequestedStatus) ->
    case ipto_db:get_unit_json(TenantId, UnitId, latest) of
        {ok, UnitMap} ->
            CurrentStatus = maps:get(status, UnitMap, ?STATUS_EFFECTIVE),
            case allowed_transition(CurrentStatus, RequestedStatus) of
                true -> ipto_db:set_status({TenantId, UnitId}, RequestedStatus);
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
