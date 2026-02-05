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
-module(erepo).

-include("erepo.hrl").

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
%% start_link/0
%%
%% Starts the erepo application or its supervisor in embedded contexts.
%% --------------------------------------------------------------------
-spec start_link() -> {ok, pid() | erepo} | {error, term()}.
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

-spec create_unit(tenantid()) -> {ok, #unit{}} | {error, invalid_tenantid}.
create_unit(TenantId) ->
    erepo_repo:create_unit(TenantId).

-spec create_unit(tenantid(), unit_name() | string() | {corrid, corrid()}) ->
    {ok, #unit{}} | {error, invalid_tenantid | invalid_create_unit_argument}.
create_unit(TenantId, NameOrCorrId) ->
    erepo_repo:create_unit(TenantId, NameOrCorrId).

-spec get_unit(tenantid(), unitid()) -> unit_lookup_result().
get_unit(TenantId, UnitId) ->
    erepo_repo:get_unit(TenantId, UnitId).

-spec get_unit(tenantid(), unitid(), version_selector()) -> unit_lookup_result().
get_unit(TenantId, UnitId, Version) ->
    erepo_repo:get_unit(TenantId, UnitId, Version).

-spec unit_exists(tenantid(), unitid()) -> boolean().
unit_exists(TenantId, UnitId) ->
    erepo_repo:unit_exists(TenantId, UnitId).

-spec store_unit(#unit{}) -> erepo_result(unit_map()) | {error, invalid_unit}.
store_unit(Unit) ->
    erepo_repo:store_unit(Unit).

-spec store_unit_json(unit_map()) -> erepo_result(unit_map()) | {error, invalid_unit_json}.
store_unit_json(UnitJsonMap) ->
    erepo_repo:store_unit_json(UnitJsonMap).

-spec search_units(search_expression() | map(), search_order(), search_paging()) -> erepo_result(search_result()).
search_units(Expression, Order, PagingOrLimit) ->
    erepo_repo:search_units(Expression, Order, PagingOrLimit).

-spec add_relation(unit_ref_value(), relation_type(), unit_ref_value()) -> ok | {error, erepo_reason()}.
add_relation(UnitRef, RelType, OtherUnitRef) ->
    erepo_repo:add_relation(UnitRef, RelType, OtherUnitRef).

-spec remove_relation(unit_ref_value(), relation_type(), unit_ref_value()) -> ok | {error, erepo_reason()}.
remove_relation(UnitRef, RelType, OtherUnitRef) ->
    erepo_repo:remove_relation(UnitRef, RelType, OtherUnitRef).

-spec get_right_relation(unit_ref_value(), relation_type()) -> relation_lookup_result().
get_right_relation(UnitRef, RelType) ->
    erepo_repo:get_right_relation(UnitRef, RelType).

-spec get_right_relations(unit_ref_value(), relation_type()) -> erepo_result([relation()]).
get_right_relations(UnitRef, RelType) ->
    erepo_repo:get_right_relations(UnitRef, RelType).

-spec get_left_relations(unit_ref_value(), relation_type()) -> erepo_result([relation()]).
get_left_relations(UnitRef, RelType) ->
    erepo_repo:get_left_relations(UnitRef, RelType).

-spec count_right_relations(unit_ref_value(), relation_type()) -> erepo_result(non_neg_integer()).
count_right_relations(UnitRef, RelType) ->
    erepo_repo:count_right_relations(UnitRef, RelType).

-spec count_left_relations(unit_ref_value(), relation_type()) -> erepo_result(non_neg_integer()).
count_left_relations(UnitRef, RelType) ->
    erepo_repo:count_left_relations(UnitRef, RelType).

-spec add_association(unit_ref_value(), association_type(), ref_string()) -> ok | {error, erepo_reason()}.
add_association(UnitRef, AssocType, RefString) ->
    erepo_repo:add_association(UnitRef, AssocType, RefString).

-spec remove_association(unit_ref_value(), association_type(), ref_string()) -> ok | {error, erepo_reason()}.
remove_association(UnitRef, AssocType, RefString) ->
    erepo_repo:remove_association(UnitRef, AssocType, RefString).

-spec get_right_association(unit_ref_value(), association_type()) -> association_lookup_result().
get_right_association(UnitRef, AssocType) ->
    erepo_repo:get_right_association(UnitRef, AssocType).

-spec get_right_associations(unit_ref_value(), association_type()) -> erepo_result([association()]).
get_right_associations(UnitRef, AssocType) ->
    erepo_repo:get_right_associations(UnitRef, AssocType).

-spec get_left_associations(association_type(), ref_string()) -> erepo_result([association()]).
get_left_associations(AssocType, RefString) ->
    erepo_repo:get_left_associations(AssocType, RefString).

-spec count_right_associations(unit_ref_value(), association_type()) -> erepo_result(non_neg_integer()).
count_right_associations(UnitRef, AssocType) ->
    erepo_repo:count_right_associations(UnitRef, AssocType).

-spec count_left_associations(association_type(), ref_string()) -> erepo_result(non_neg_integer()).
count_left_associations(AssocType, RefString) ->
    erepo_repo:count_left_associations(AssocType, RefString).

-spec lock_unit(unit_ref_value(), lock_type(), ref_string()) -> ok | already_locked | {error, erepo_reason()}.
lock_unit(UnitRef, LockType, Purpose) ->
    erepo_repo:lock_unit(UnitRef, LockType, Purpose).

-spec unlock_unit(unit_ref_value()) -> ok | {error, erepo_reason()}.
unlock_unit(UnitRef) ->
    erepo_repo:unlock_unit(UnitRef).

-spec activate_unit(unit_ref_value()) -> ok | {error, erepo_reason()}.
activate_unit(UnitRef) ->
    erepo_repo:activate_unit(UnitRef).

-spec inactivate_unit(unit_ref_value()) -> ok | {error, erepo_reason()}.
inactivate_unit(UnitRef) ->
    erepo_repo:inactivate_unit(UnitRef).

-spec create_attribute(attribute_alias(), attribute_name(), attribute_qualname(), attribute_type(), boolean()) ->
    erepo_result(attribute_info()).
create_attribute(Alias, Name, QualName, Type, IsArray) ->
    erepo_repo:create_attribute(Alias, Name, QualName, Type, IsArray).

-spec get_attribute_info(name_or_id()) -> {ok, attribute_info()} | not_found | {error, erepo_reason()}.
get_attribute_info(NameOrId) ->
    erepo_repo:get_attribute_info(NameOrId).

-spec get_tenant_info(name_or_id()) -> {ok, tenant_info()} | not_found | {error, erepo_reason()}.
get_tenant_info(NameOrId) ->
    erepo_repo:get_tenant_info(NameOrId).

-spec sync() -> ok.
sync() ->
    erepo_repo:sync().
