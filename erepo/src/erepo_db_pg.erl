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
-module(erepo_db_pg).
-behaviour(erepo_backend).

-include("erepo.hrl").

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

-spec get_unit_json(tenantid(), unitid(), version_selector()) -> unit_lookup_result().
get_unit_json(TenantId, UnitId, Version) ->
    erepo_db:pg_get_unit_json_backend(TenantId, UnitId, Version).

-spec unit_exists(tenantid(), unitid()) -> boolean().
unit_exists(TenantId, UnitId) ->
    erepo_db:pg_unit_exists_backend(TenantId, UnitId).

-spec store_unit_json(unit_map()) -> erepo_result(unit_map()).
store_unit_json(UnitMap) ->
    erepo_db:pg_store_unit_json_backend(UnitMap).

-spec search_units(search_expression() | map(), search_order(), search_paging()) -> erepo_result(search_result()).
search_units(Expression, Order, PagingOrLimit) ->
    erepo_db:pg_search_units_backend(Expression, Order, PagingOrLimit).

-spec add_relation(unit_ref_value(), relation_type(), unit_ref_value()) -> ok | {error, erepo_reason()}.
add_relation(UnitRef, RelType, OtherUnitRef) ->
    erepo_db:pg_add_relation_backend(UnitRef, RelType, OtherUnitRef).

-spec remove_relation(unit_ref_value(), relation_type(), unit_ref_value()) -> ok | {error, erepo_reason()}.
remove_relation(UnitRef, RelType, OtherUnitRef) ->
    erepo_db:pg_remove_relation_backend(UnitRef, RelType, OtherUnitRef).

-spec add_association(unit_ref_value(), association_type(), ref_string()) -> ok | {error, erepo_reason()}.
add_association(UnitRef, AssocType, RefString) ->
    erepo_db:pg_add_association_backend(UnitRef, AssocType, RefString).

-spec remove_association(unit_ref_value(), association_type(), ref_string()) -> ok | {error, erepo_reason()}.
remove_association(UnitRef, AssocType, RefString) ->
    erepo_db:pg_remove_association_backend(UnitRef, AssocType, RefString).

-spec lock_unit(unit_ref_value(), lock_type(), ref_string()) -> ok | already_locked | {error, erepo_reason()}.
lock_unit(UnitRef, LockType, Purpose) ->
    erepo_db:pg_lock_unit_backend(UnitRef, LockType, Purpose).

-spec unlock_unit(unit_ref_value()) -> ok | {error, erepo_reason()}.
unlock_unit(UnitRef) ->
    erepo_db:pg_unlock_unit_backend(UnitRef).

-spec set_status(unit_ref_value(), unit_status()) -> ok | {error, erepo_reason()}.
set_status(UnitRef, Status) ->
    erepo_db:pg_set_status_backend(UnitRef, Status).

-spec create_attribute(attribute_alias(), attribute_name(), attribute_qualname(), attribute_type(), boolean()) ->
    erepo_result(attribute_info()).
create_attribute(Alias, Name, QualName, Type, IsArray) ->
    erepo_db:pg_create_attribute_backend(Alias, Name, QualName, Type, IsArray).

-spec get_attribute_info(name_or_id()) -> {ok, attribute_info()} | not_found | {error, erepo_reason()}.
get_attribute_info(NameOrId) ->
    erepo_db:pg_get_attribute_info_backend(NameOrId).

-spec get_tenant_info(name_or_id()) -> {ok, tenant_info()} | not_found | {error, erepo_reason()}.
get_tenant_info(NameOrId) ->
    erepo_db:pg_get_tenant_info_backend(NameOrId).
