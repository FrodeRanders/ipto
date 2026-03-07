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
    sync/0,
    reset_timing_data/0,
    get_timing_data/0,
    timing_report/0
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
            ipto_log:warning(ipto, "application .app missing, starting supervisor directly", []),
            case ipto_sup:start_link() of
                {ok, _} = Ok ->
                    ipto_log:notice(ipto, "started supervisor directly (embedded mode)", []),
                    Ok;
                {error, {already_started, _Pid}} ->
                    {ok, ipto};
                Error ->
                    ipto_log:error(ipto, "failed starting supervisor directly error=~p", [Error]),
                    Error
            end;
        {error, {already_started, ipto}} ->
            {ok, ipto};
        Error ->
            ipto_log:error(ipto, "failed starting application error=~p", [Error]),
            Error
    end.

-spec create_unit(tenantid()) -> {ok, #unit{}} | {error, invalid_tenantid}.
create_unit(TenantId) ->
    timed(create_unit, fun() -> ipto_repo:create_unit(TenantId) end).

-spec create_unit(tenantid(), unit_name() | string() | {corrid, corrid()}) ->
    {ok, #unit{}} | {error, invalid_tenantid | invalid_create_unit_argument}.
create_unit(TenantId, NameOrCorrId) ->
    timed(create_unit_with_name_or_corrid, fun() -> ipto_repo:create_unit(TenantId, NameOrCorrId) end).

-spec get_unit(tenantid(), unitid()) -> unit_lookup_result().
get_unit(TenantId, UnitId) ->
    timed(get_unit, fun() -> ipto_repo:get_unit(TenantId, UnitId) end).

-spec get_unit(tenantid(), unitid(), version_selector()) -> unit_lookup_result().
get_unit(TenantId, UnitId, Version) ->
    timed(get_unit_with_version, fun() -> ipto_repo:get_unit(TenantId, UnitId, Version) end).

-spec unit_exists(tenantid(), unitid()) -> boolean().
unit_exists(TenantId, UnitId) ->
    timed(unit_exists, fun() -> ipto_repo:unit_exists(TenantId, UnitId) end).

-spec store_unit(#unit{}) -> ipto_result(unit_map()) | {error, invalid_unit}.
store_unit(Unit) ->
    timed(store_unit, fun() -> ipto_repo:store_unit(Unit) end).

-spec store_unit_json(unit_map()) -> ipto_result(unit_map()) | {error, invalid_unit_json}.
store_unit_json(UnitJsonMap) ->
    timed(store_unit_json, fun() -> ipto_repo:store_unit_json(UnitJsonMap) end).

-spec search_units(search_expression() | map(), search_order(), search_paging()) -> ipto_result(search_result()).
search_units(Expression, Order, PagingOrLimit) ->
    timed(search_units, fun() -> ipto_repo:search_units(Expression, Order, PagingOrLimit) end).

-spec add_relation(unit_ref_value(), relation_type(), unit_ref_value()) -> ok | {error, ipto_reason()}.
add_relation(UnitRef, RelType, OtherUnitRef) ->
    timed(add_relation, fun() -> ipto_repo:add_relation(UnitRef, RelType, OtherUnitRef) end).

-spec remove_relation(unit_ref_value(), relation_type(), unit_ref_value()) -> ok | {error, ipto_reason()}.
remove_relation(UnitRef, RelType, OtherUnitRef) ->
    timed(remove_relation, fun() -> ipto_repo:remove_relation(UnitRef, RelType, OtherUnitRef) end).

-spec get_right_relation(unit_ref_value(), relation_type()) -> relation_lookup_result().
get_right_relation(UnitRef, RelType) ->
    timed(get_right_relation, fun() -> ipto_repo:get_right_relation(UnitRef, RelType) end).

-spec get_right_relations(unit_ref_value(), relation_type()) -> ipto_result([relation()]).
get_right_relations(UnitRef, RelType) ->
    timed(get_right_relations, fun() -> ipto_repo:get_right_relations(UnitRef, RelType) end).

-spec get_left_relations(unit_ref_value(), relation_type()) -> ipto_result([relation()]).
get_left_relations(UnitRef, RelType) ->
    timed(get_left_relations, fun() -> ipto_repo:get_left_relations(UnitRef, RelType) end).

-spec count_right_relations(unit_ref_value(), relation_type()) -> ipto_result(non_neg_integer()).
count_right_relations(UnitRef, RelType) ->
    timed(count_right_relations, fun() -> ipto_repo:count_right_relations(UnitRef, RelType) end).

-spec count_left_relations(unit_ref_value(), relation_type()) -> ipto_result(non_neg_integer()).
count_left_relations(UnitRef, RelType) ->
    timed(count_left_relations, fun() -> ipto_repo:count_left_relations(UnitRef, RelType) end).

-spec add_association(unit_ref_value(), association_type(), ref_string()) -> ok | {error, ipto_reason()}.
add_association(UnitRef, AssocType, RefString) ->
    timed(add_association, fun() -> ipto_repo:add_association(UnitRef, AssocType, RefString) end).

-spec remove_association(unit_ref_value(), association_type(), ref_string()) -> ok | {error, ipto_reason()}.
remove_association(UnitRef, AssocType, RefString) ->
    timed(remove_association, fun() -> ipto_repo:remove_association(UnitRef, AssocType, RefString) end).

-spec get_right_association(unit_ref_value(), association_type()) -> association_lookup_result().
get_right_association(UnitRef, AssocType) ->
    timed(get_right_association, fun() -> ipto_repo:get_right_association(UnitRef, AssocType) end).

-spec get_right_associations(unit_ref_value(), association_type()) -> ipto_result([association()]).
get_right_associations(UnitRef, AssocType) ->
    timed(get_right_associations, fun() -> ipto_repo:get_right_associations(UnitRef, AssocType) end).

-spec get_left_associations(association_type(), ref_string()) -> ipto_result([association()]).
get_left_associations(AssocType, RefString) ->
    timed(get_left_associations, fun() -> ipto_repo:get_left_associations(AssocType, RefString) end).

-spec count_right_associations(unit_ref_value(), association_type()) -> ipto_result(non_neg_integer()).
count_right_associations(UnitRef, AssocType) ->
    timed(count_right_associations, fun() -> ipto_repo:count_right_associations(UnitRef, AssocType) end).

-spec count_left_associations(association_type(), ref_string()) -> ipto_result(non_neg_integer()).
count_left_associations(AssocType, RefString) ->
    timed(count_left_associations, fun() -> ipto_repo:count_left_associations(AssocType, RefString) end).

-spec lock_unit(unit_ref_value(), lock_type(), ref_string()) -> ok | already_locked | {error, ipto_reason()}.
lock_unit(UnitRef, LockType, Purpose) ->
    timed(lock_unit, fun() -> ipto_repo:lock_unit(UnitRef, LockType, Purpose) end).

-spec unlock_unit(unit_ref_value()) -> ok | {error, ipto_reason()}.
unlock_unit(UnitRef) ->
    timed(unlock_unit, fun() -> ipto_repo:unlock_unit(UnitRef) end).

-spec activate_unit(unit_ref_value()) -> ok | {error, ipto_reason()}.
activate_unit(UnitRef) ->
    timed(activate_unit, fun() -> ipto_repo:activate_unit(UnitRef) end).

-spec inactivate_unit(unit_ref_value()) -> ok | {error, ipto_reason()}.
inactivate_unit(UnitRef) ->
    timed(inactivate_unit, fun() -> ipto_repo:inactivate_unit(UnitRef) end).

-spec create_attribute(attribute_alias(), attribute_name(), attribute_qualname(), attribute_type(), boolean()) ->
    ipto_result(attribute_info()).
create_attribute(Alias, Name, QualName, Type, IsArray) ->
    timed(create_attribute, fun() -> ipto_repo:create_attribute(Alias, Name, QualName, Type, IsArray) end).

-spec get_attribute_info(name_or_id()) -> {ok, attribute_info()} | not_found | {error, ipto_reason()}.
get_attribute_info(NameOrId) ->
    timed(get_attribute_info, fun() -> ipto_repo:get_attribute_info(NameOrId) end).

-spec get_tenant_info(name_or_id()) -> {ok, tenant_info()} | not_found | {error, ipto_reason()}.
get_tenant_info(NameOrId) ->
    timed(get_tenant_info, fun() -> ipto_repo:get_tenant_info(NameOrId) end).

-spec sync() -> ok.
sync() ->
    timed(sync, fun() -> ipto_repo:sync() end).

-spec reset_timing_data() -> ok.
reset_timing_data() ->
    ipto_timing_data:reset().

-spec get_timing_data() -> map().
get_timing_data() ->
    ipto_timing_data:get_stats().

-spec timing_report() -> binary().
timing_report() ->
    ipto_timing_data:report().

-spec timed(atom(), fun(() -> T)) -> T.
timed(Name, Fun) ->
    ipto_timed_execution:run(Name, Fun).
