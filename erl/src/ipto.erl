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
-module(ipto).

-include("ipto.hrl").

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
%% Starts the ipto application or its supervisor in embedded contexts.
%% --------------------------------------------------------------------
-spec start_link() -> {ok, pid() | ipto} | {error, term()}.
start_link() ->
    case application:ensure_all_started(ipto) of
        {ok, _} = Ok ->
            Ok;
        {error, {ipto, {"no such file or directory", "ipto.app"}}} ->
            case ipto_sup:start_link() of
                {ok, _} = Ok ->
                    Ok;
                {error, {already_started, _Pid}} ->
                    {ok, ipto};
                Error ->
                    Error
            end;
        {error, {already_started, ipto}} ->
            {ok, ipto};
        Error ->
            Error
    end.

-spec create_unit(tenantid()) -> {ok, #unit{}} | {error, invalid_tenantid}.
create_unit(TenantId) ->
    ipto_repo:create_unit(TenantId).

-spec create_unit(tenantid(), unit_name() | string() | {corrid, corrid()}) ->
    {ok, #unit{}} | {error, invalid_tenantid | invalid_create_unit_argument}.
create_unit(TenantId, NameOrCorrId) ->
    ipto_repo:create_unit(TenantId, NameOrCorrId).

-spec get_unit(tenantid(), unitid()) -> unit_lookup_result().
get_unit(TenantId, UnitId) ->
    ipto_repo:get_unit(TenantId, UnitId).

-spec get_unit(tenantid(), unitid(), version_selector()) -> unit_lookup_result().
get_unit(TenantId, UnitId, Version) ->
    ipto_repo:get_unit(TenantId, UnitId, Version).

-spec unit_exists(tenantid(), unitid()) -> boolean().
unit_exists(TenantId, UnitId) ->
    ipto_repo:unit_exists(TenantId, UnitId).

-spec store_unit(#unit{}) -> ipto_result(unit_map()) | {error, invalid_unit}.
store_unit(Unit) ->
    ipto_repo:store_unit(Unit).

-spec store_unit_json(unit_map()) -> ipto_result(unit_map()) | {error, invalid_unit_json}.
store_unit_json(UnitJsonMap) ->
    ipto_repo:store_unit_json(UnitJsonMap).

-spec search_units(search_expression() | map(), search_order(), search_paging()) -> ipto_result(search_result()).
search_units(Expression, Order, PagingOrLimit) ->
    ipto_repo:search_units(Expression, Order, PagingOrLimit).

-spec add_relation(unit_ref_value(), relation_type(), unit_ref_value()) -> ok | {error, ipto_reason()}.
add_relation(UnitRef, RelType, OtherUnitRef) ->
    ipto_repo:add_relation(UnitRef, RelType, OtherUnitRef).

-spec remove_relation(unit_ref_value(), relation_type(), unit_ref_value()) -> ok | {error, ipto_reason()}.
remove_relation(UnitRef, RelType, OtherUnitRef) ->
    ipto_repo:remove_relation(UnitRef, RelType, OtherUnitRef).

-spec get_right_relation(unit_ref_value(), relation_type()) -> relation_lookup_result().
get_right_relation(UnitRef, RelType) ->
    ipto_repo:get_right_relation(UnitRef, RelType).

-spec get_right_relations(unit_ref_value(), relation_type()) -> ipto_result([relation()]).
get_right_relations(UnitRef, RelType) ->
    ipto_repo:get_right_relations(UnitRef, RelType).

-spec get_left_relations(unit_ref_value(), relation_type()) -> ipto_result([relation()]).
get_left_relations(UnitRef, RelType) ->
    ipto_repo:get_left_relations(UnitRef, RelType).

-spec count_right_relations(unit_ref_value(), relation_type()) -> ipto_result(non_neg_integer()).
count_right_relations(UnitRef, RelType) ->
    ipto_repo:count_right_relations(UnitRef, RelType).

-spec count_left_relations(unit_ref_value(), relation_type()) -> ipto_result(non_neg_integer()).
count_left_relations(UnitRef, RelType) ->
    ipto_repo:count_left_relations(UnitRef, RelType).

-spec add_association(unit_ref_value(), association_type(), ref_string()) -> ok | {error, ipto_reason()}.
add_association(UnitRef, AssocType, RefString) ->
    ipto_repo:add_association(UnitRef, AssocType, RefString).

-spec remove_association(unit_ref_value(), association_type(), ref_string()) -> ok | {error, ipto_reason()}.
remove_association(UnitRef, AssocType, RefString) ->
    ipto_repo:remove_association(UnitRef, AssocType, RefString).

-spec get_right_association(unit_ref_value(), association_type()) -> association_lookup_result().
get_right_association(UnitRef, AssocType) ->
    ipto_repo:get_right_association(UnitRef, AssocType).

-spec get_right_associations(unit_ref_value(), association_type()) -> ipto_result([association()]).
get_right_associations(UnitRef, AssocType) ->
    ipto_repo:get_right_associations(UnitRef, AssocType).

-spec get_left_associations(association_type(), ref_string()) -> ipto_result([association()]).
get_left_associations(AssocType, RefString) ->
    ipto_repo:get_left_associations(AssocType, RefString).

-spec count_right_associations(unit_ref_value(), association_type()) -> ipto_result(non_neg_integer()).
count_right_associations(UnitRef, AssocType) ->
    ipto_repo:count_right_associations(UnitRef, AssocType).

-spec count_left_associations(association_type(), ref_string()) -> ipto_result(non_neg_integer()).
count_left_associations(AssocType, RefString) ->
    ipto_repo:count_left_associations(AssocType, RefString).

-spec lock_unit(unit_ref_value(), lock_type(), ref_string()) -> ok | already_locked | {error, ipto_reason()}.
lock_unit(UnitRef, LockType, Purpose) ->
    ipto_repo:lock_unit(UnitRef, LockType, Purpose).

-spec unlock_unit(unit_ref_value()) -> ok | {error, ipto_reason()}.
unlock_unit(UnitRef) ->
    ipto_repo:unlock_unit(UnitRef).

-spec activate_unit(unit_ref_value()) -> ok | {error, ipto_reason()}.
activate_unit(UnitRef) ->
    ipto_repo:activate_unit(UnitRef).

-spec inactivate_unit(unit_ref_value()) -> ok | {error, ipto_reason()}.
inactivate_unit(UnitRef) ->
    ipto_repo:inactivate_unit(UnitRef).

-spec create_attribute(attribute_alias(), attribute_name(), attribute_qualname(), attribute_type(), boolean()) ->
    ipto_result(attribute_info()).
create_attribute(Alias, Name, QualName, Type, IsArray) ->
    ipto_repo:create_attribute(Alias, Name, QualName, Type, IsArray).

-spec get_attribute_info(name_or_id()) -> {ok, attribute_info()} | not_found | {error, ipto_reason()}.
get_attribute_info(NameOrId) ->
    ipto_repo:get_attribute_info(NameOrId).

-spec get_tenant_info(name_or_id()) -> {ok, tenant_info()} | not_found | {error, ipto_reason()}.
get_tenant_info(NameOrId) ->
    ipto_repo:get_tenant_info(NameOrId).

-spec sync() -> ok.
sync() ->
    ipto_repo:sync().
