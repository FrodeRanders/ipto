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
-module(ipto_db).

-include("ipto.hrl").

-export([
    get_unit_json/3,
    unit_exists/2,
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
    is_unit_locked/1,
    set_status/2,
    create_attribute/5,
    get_attribute_info/1,
    get_tenant_info/1,
    upsert_record_template/3,
    upsert_unit_template/2
]).

-spec get_unit_json(tenantid(), unitid(), version_selector()) -> unit_lookup_result().
get_unit_json(TenantId, UnitId, Version) ->
    call_backend(get_unit_json, [TenantId, UnitId, Version]).

-spec unit_exists(tenantid(), unitid()) -> boolean().
unit_exists(TenantId, UnitId) ->
    call_backend(unit_exists, [TenantId, UnitId]).

-spec store_unit_json(unit_map()) -> ipto_result(unit_map()).
store_unit_json(UnitMap) when is_map(UnitMap) ->
    case ipto_db_utils:validate_store_input(UnitMap) of
        ok ->
            call_backend(store_unit_json, [UnitMap]);
        Error ->
            Error
    end;
store_unit_json(_UnitMap) ->
    {error, invalid_unit_map}.

-spec search_units(search_expression() | map(), search_order(), search_paging()) -> ipto_result(search_result()).
search_units(Expression, Order, PagingOrLimit) ->
    call_backend(search_units, [Expression, Order, PagingOrLimit]).

-spec add_relation(unit_ref_value(), relation_type(), unit_ref_value()) -> ok | {error, ipto_reason()}.
add_relation(UnitRef, RelType, OtherUnitRef) ->
    call_backend(add_relation, [UnitRef, RelType, OtherUnitRef]).

-spec remove_relation(unit_ref_value(), relation_type(), unit_ref_value()) -> ok | {error, ipto_reason()}.
remove_relation(UnitRef, RelType, OtherUnitRef) ->
    call_backend(remove_relation, [UnitRef, RelType, OtherUnitRef]).

-spec get_right_relation(unit_ref_value(), relation_type()) -> relation_lookup_result().
get_right_relation(UnitRef, RelType) ->
    call_backend(get_right_relation, [UnitRef, RelType]).

-spec get_right_relations(unit_ref_value(), relation_type()) -> ipto_result([relation()]).
get_right_relations(UnitRef, RelType) ->
    call_backend(get_right_relations, [UnitRef, RelType]).

-spec get_left_relations(unit_ref_value(), relation_type()) -> ipto_result([relation()]).
get_left_relations(UnitRef, RelType) ->
    call_backend(get_left_relations, [UnitRef, RelType]).

-spec count_right_relations(unit_ref_value(), relation_type()) -> ipto_result(non_neg_integer()).
count_right_relations(UnitRef, RelType) ->
    call_backend(count_right_relations, [UnitRef, RelType]).

-spec count_left_relations(unit_ref_value(), relation_type()) -> ipto_result(non_neg_integer()).
count_left_relations(UnitRef, RelType) ->
    call_backend(count_left_relations, [UnitRef, RelType]).

-spec add_association(unit_ref_value(), association_type(), ref_string()) -> ok | {error, ipto_reason()}.
add_association(UnitRef, AssocType, RefString) ->
    call_backend(add_association, [UnitRef, AssocType, RefString]).

-spec remove_association(unit_ref_value(), association_type(), ref_string()) -> ok | {error, ipto_reason()}.
remove_association(UnitRef, AssocType, RefString) ->
    call_backend(remove_association, [UnitRef, AssocType, RefString]).

-spec get_right_association(unit_ref_value(), association_type()) -> association_lookup_result().
get_right_association(UnitRef, AssocType) ->
    call_backend(get_right_association, [UnitRef, AssocType]).

-spec get_right_associations(unit_ref_value(), association_type()) -> ipto_result([association()]).
get_right_associations(UnitRef, AssocType) ->
    call_backend(get_right_associations, [UnitRef, AssocType]).

-spec get_left_associations(association_type(), ref_string()) -> ipto_result([association()]).
get_left_associations(AssocType, RefString) ->
    call_backend(get_left_associations, [AssocType, RefString]).

-spec count_right_associations(unit_ref_value(), association_type()) -> ipto_result(non_neg_integer()).
count_right_associations(UnitRef, AssocType) ->
    call_backend(count_right_associations, [UnitRef, AssocType]).

-spec count_left_associations(association_type(), ref_string()) -> ipto_result(non_neg_integer()).
count_left_associations(AssocType, RefString) ->
    call_backend(count_left_associations, [AssocType, RefString]).

-spec lock_unit(unit_ref_value(), lock_type(), ref_string()) -> ok | already_locked | {error, ipto_reason()}.
lock_unit(UnitRef, LockType, Purpose) ->
    call_backend(lock_unit, [UnitRef, LockType, Purpose]).

-spec unlock_unit(unit_ref_value()) -> ok | {error, ipto_reason()}.
unlock_unit(UnitRef) ->
    call_backend(unlock_unit, [UnitRef]).

-spec is_unit_locked(unit_ref_value()) -> boolean().
is_unit_locked(UnitRef) ->
    call_backend(is_unit_locked, [UnitRef]).

-spec set_status(unit_ref_value(), unit_status()) -> ok | {error, ipto_reason()}.
set_status(UnitRef, Status) when is_integer(Status) ->
    call_backend(set_status, [UnitRef, Status]);
set_status(_UnitRef, _Status) ->
    {error, invalid_status}.

-spec create_attribute(attribute_alias(), attribute_name(), attribute_qualname(), attribute_type(), boolean()) ->
    ipto_result(attribute_info()).
create_attribute(Alias, Name, QualName, Type, IsArray) ->
    call_backend(create_attribute, [Alias, Name, QualName, Type, IsArray]).

-spec get_attribute_info(name_or_id()) -> {ok, attribute_info()} | not_found | {error, ipto_reason()}.
get_attribute_info(NameOrId) ->
    call_backend(get_attribute_info, [NameOrId]).

-spec get_tenant_info(name_or_id()) -> {ok, tenant_info()} | not_found | {error, ipto_reason()}.
get_tenant_info(NameOrId) ->
    call_backend(get_tenant_info, [NameOrId]).

-spec upsert_record_template(integer(), binary(), [{integer(), binary()}]) -> ok | {error, ipto_reason()}.
upsert_record_template(RecordId, RecordName, Fields)
  when is_integer(RecordId), RecordId > 0, is_binary(RecordName), is_list(Fields) ->
    call_backend(upsert_record_template, [RecordId, RecordName, Fields]);
upsert_record_template(_RecordId, _RecordName, _Fields) ->
    {error, invalid_record_template}.

-spec upsert_unit_template(binary(), [{integer(), binary()}]) -> ok | {error, ipto_reason()}.
upsert_unit_template(TemplateName, Fields) when is_binary(TemplateName), is_list(Fields) ->
    call_backend(upsert_unit_template, [TemplateName, Fields]);
upsert_unit_template(_TemplateName, _Fields) ->
    {error, invalid_unit_template}.

-spec backend() -> pg | neo4j | memory.
backend() ->
    case application:get_env(ipto, backend) of
        {ok, pg} -> pg;
        {ok, postgres} -> pg;
        {ok, neo4j} -> neo4j;
        _ -> memory
    end.

-spec backend_module() -> module().
backend_module() ->
    case backend() of
        pg -> ipto_db_pg;
        neo4j -> ipto_db_neo4j;
        memory -> ipto_db_memory
    end.

-spec call_backend(atom(), list()) -> term().
call_backend(Fun, Args) ->
    Module = backend_module(),
    try
        Result = apply(Module, Fun, Args),
        case Result of
            {error, ErrorReason} ->
                ipto_log:warning(ipto_db, "backend call failed backend=~p fun=~p reason=~p", [Module, Fun, ErrorReason]);
            _ ->
                ok
        end,
        Result
    catch
        Class:Reason:Stacktrace ->
            ipto_log:error(
                ipto_db,
                "backend call crashed backend=~p fun=~p class=~p reason=~p stacktrace=~p",
                [Module, Fun, Class, Reason, Stacktrace]
            ),
            erlang:raise(Class, Reason, Stacktrace)
    end.
