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

%% Central orchestration layer for the Erlang implementation.
%%
%% The facade in `ipto` calls into this module, which then coordinates unit
%% helpers, persistence backends, relation primitives, SDL parsing, and event
%% emission.

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
    is_unit_locked/1,
    request_status_transition/2,
    activate_unit/1,
    inactivate_unit/1,
    create_attribute/5,
    get_attribute_info/1,
    get_tenant_info/1,
    inspect_graphql_sdl/1,
    configure_graphql_sdl/1,
    configure_graphql_sdl_file/1,
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
            %% Events are best-effort notifications; persistence is authoritative.
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

-spec is_unit_locked(unit_ref_value()) -> boolean().
is_unit_locked(UnitRef) ->
    ipto_lock:is_locked(UnitRef).

-spec request_status_transition(unit_ref_value(), unit_status()) ->
    {ok, unit_status()} | {error, ipto_reason()}.
request_status_transition(UnitRef, RequestedStatus) when is_integer(RequestedStatus) ->
    transition_status(UnitRef, RequestedStatus);
request_status_transition(_UnitRef, _RequestedStatus) ->
    {error, invalid_status}.

-spec activate_unit(unit_ref_value()) -> ok | {error, ipto_reason()}.
%% Convenience helpers matching the Java surface.
activate_unit(UnitRef) ->
    case request_status_transition(UnitRef, ?STATUS_EFFECTIVE) of
        {ok, _Status} -> ok;
        Error -> Error
    end.

-spec inactivate_unit(unit_ref_value()) -> ok | {error, ipto_reason()}.
inactivate_unit(UnitRef) ->
    case request_status_transition(UnitRef, ?STATUS_PENDING_DELETION) of
        {ok, _Status} -> ok;
        Error -> Error
    end.

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

-spec inspect_graphql_sdl(binary() | string()) -> map().
inspect_graphql_sdl(Sdl) ->
    ipto_graphql_sdl:inspect(Sdl).

-spec configure_graphql_sdl(binary() | string()) -> {ok, map()} | {error, ipto_reason()}.
configure_graphql_sdl(Sdl) ->
    case ipto_graphql_sdl:parse(Sdl) of
        {ok, Catalog} ->
            %% Parsing only validates and catalogs the SDL. Applying it is a
            %% separate step because attribute/template persistence may fail.
            apply_sdl_catalog(Catalog);
        {error, {invalid_sdl, Errors, _Catalog}} ->
            {error, {invalid_sdl, Errors}}
    end.

-spec configure_graphql_sdl_file(binary() | string()) -> {ok, map()} | {error, ipto_reason()}.
configure_graphql_sdl_file(Path) when is_binary(Path); is_list(Path) ->
    case file:read_file(Path) of
        {ok, Sdl} ->
            configure_graphql_sdl(Sdl);
        {error, Reason} ->
            {error, {configure_graphql_sdl_file_failed, Reason}}
    end;
configure_graphql_sdl_file(_Path) ->
    {error, invalid_sdl_file_path}.

-spec sync() -> ok.
sync() ->
    ipto_attr:sync().

-spec normalize_name(binary() | string()) -> binary().
normalize_name(Name) when is_binary(Name) ->
    Name;
normalize_name(Name) when is_list(Name) ->
    unicode:characters_to_binary(Name).

-spec maybe_emit_event(atom(), term()) -> ok.
maybe_emit_event(Action, Payload) ->
    ipto_event:emit(Action, Payload).

-spec transition_status(unit_ref_value() | unit_ref_tuple() | unit_ref_map(), unit_status()) ->
    {ok, unit_status()} | {error, ipto_reason()}.
transition_status(#{tenantid := TenantId, unitid := UnitId}, RequestedStatus) ->
    transition_status({TenantId, UnitId}, RequestedStatus);
transition_status({TenantId, UnitId}, RequestedStatus) ->
    case ipto_db:get_unit_json(TenantId, UnitId, latest) of
        {ok, UnitMap} ->
            CurrentStatus = maps:get(status, UnitMap, ?STATUS_EFFECTIVE),
            case allowed_transition(CurrentStatus, RequestedStatus) of
                true ->
                    case ipto_db:set_status({TenantId, UnitId}, RequestedStatus) of
                        ok -> {ok, RequestedStatus};
                        Error -> Error
                    end;
                false ->
                    {ok, CurrentStatus}
            end;
        not_found ->
            {error, not_found};
        Error ->
            Error
    end;
transition_status(_UnitRef, _RequestedStatus) ->
    {error, invalid_unit_ref}.

-spec allowed_transition(unit_status(), unit_status()) -> boolean().
%% Status rules are intentionally conservative. Unsupported transitions are
%% treated as no-ops by returning the current status from `transition_status/2`.
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

-spec apply_sdl_catalog(map()) -> {ok, map()} | {error, ipto_reason()}.
apply_sdl_catalog(Catalog) ->
    Registry = maps:get(attribute_registry, Catalog, #{}),
    AttrDefs = maps:get(attribute_defs, Registry, []),
    case ensure_attributes(AttrDefs, #{created => 0, existing => 0, failed => []}) of
        {ok, AttrSummary} ->
            %% Templates refer to attributes by SDL symbol, so ids must be
            %% resolved after the attribute pass has converged.
            AttrIdMap = build_attribute_id_map(AttrDefs),
            {RecordSummary, TemplateSummary} = persist_templates(Catalog, AttrIdMap),
            Result = #{
                attributes => AttrSummary,
                records => RecordSummary,
                templates => TemplateSummary
            },
            {ok, Result};
        {error, Reason, AttrSummary} ->
            {error, {configure_graphql_sdl_failed, Reason, AttrSummary}}
    end.

-spec build_attribute_id_map([map()]) -> map().
build_attribute_id_map(AttrDefs) ->
    lists:foldl(
        fun(AttrDef, Acc) ->
            Symbol = maps:get(symbol, AttrDef),
            Name = attribute_name(AttrDef, Symbol),
            case get_attribute_info(Symbol) of
                {ok, Info} ->
                    Acc#{Symbol => maps:get(id, Info)};
                _ ->
                    case get_attribute_info(Name) of
                        {ok, Info2} -> Acc#{Symbol => maps:get(id, Info2)};
                        _ -> Acc
                    end
            end
        end,
        #{},
        AttrDefs
    ).

-spec persist_templates(map(), map()) -> {map(), map()}.
persist_templates(Catalog, AttrIdMap) ->
    case current_backend() of
        pg ->
            persist_templates_pg(Catalog, AttrIdMap);
        _ ->
            %% Template persistence is currently only backed by PostgreSQL.
            {
                #{
                    supported => false,
                    reason => <<"record template persistence unsupported by current backend">>,
                    count => maps:get(records, maps:get(counts, Catalog, #{}), 0)
                },
                #{
                    supported => false,
                    reason => <<"unit template persistence unsupported by current backend">>,
                    count => maps:get(templates, maps:get(counts, Catalog, #{}), 0)
                }
            }
    end.

-spec current_backend() -> memory | pg | neo4j.
current_backend() ->
    case application:get_env(ipto, backend) of
        {ok, pg} -> pg;
        {ok, postgres} -> pg;
        {ok, neo4j} -> neo4j;
        _ -> memory
    end.

-spec persist_templates_pg(map(), map()) -> {map(), map()}.
persist_templates_pg(Catalog, AttrIdMap) ->
    Records = maps:get(records, Catalog, []),
    Templates = maps:get(templates, Catalog, []),
    RecordResult = persist_record_templates_pg(Records, AttrIdMap),
    TemplateResult = persist_unit_templates_pg(Templates, AttrIdMap),
    {RecordResult, TemplateResult}.

-spec persist_record_templates_pg([map()], map()) -> map().
persist_record_templates_pg(Records, AttrIdMap) ->
    {Persisted, Failed} = lists:foldl(
        fun(Record, {P0, F0}) ->
            RecordAttr = maps:get(attribute, Record),
            RecordName = maps:get(type_name, Record),
            Uses = maps:get(uses, Record, []),
            case maps:get(RecordAttr, AttrIdMap, undefined) of
                undefined ->
                    {P0, [#{record => RecordName, reason => unresolved_record_attribute, attribute => RecordAttr} | F0]};
                RecordId ->
                    case resolve_template_fields(Uses, AttrIdMap) of
                        {ok, Fields} ->
                            case ipto_db:upsert_record_template(RecordId, RecordName, Fields) of
                                ok -> {P0 + 1, F0};
                                {ok, _} -> {P0 + 1, F0};
                                {error, Reason} ->
                                    {P0, [#{record => RecordName, reason => Reason} | F0]}
                            end;
                        {error, Reason} ->
                            {P0, [#{record => RecordName, reason => Reason} | F0]}
                    end
            end
        end,
        {0, []},
        Records
    ),
    #{
        supported => true,
        persisted => Persisted,
        count => length(Records),
        failed => lists:reverse(Failed)
    }.

-spec persist_unit_templates_pg([map()], map()) -> map().
persist_unit_templates_pg(Templates, AttrIdMap) ->
    {Persisted, Failed} = lists:foldl(
        fun(Template, {P0, F0}) ->
            TemplateTypeName = maps:get(type_name, Template),
            TemplateName0 = maps:get(name, Template, <<>>),
            TemplateName = case TemplateName0 of
                <<>> -> TemplateTypeName;
                _ -> TemplateName0
            end,
            Uses = maps:get(uses, Template, []),
            case resolve_template_fields(Uses, AttrIdMap) of
                {ok, Fields} ->
                    case ipto_db:upsert_unit_template(TemplateName, Fields) of
                        ok -> {P0 + 1, F0};
                        {ok, _} -> {P0 + 1, F0};
                        {error, Reason} ->
                            {P0, [#{template => TemplateName, reason => Reason} | F0]}
                    end;
                {error, Reason} ->
                    {P0, [#{template => TemplateName, reason => Reason} | F0]}
            end
        end,
        {0, []},
        Templates
    ),
    #{
        supported => true,
        persisted => Persisted,
        count => length(Templates),
        failed => lists:reverse(Failed)
    }.

-spec resolve_template_fields([map()], map()) -> {ok, [{integer(), binary()}]} | {error, term()}.
resolve_template_fields(Uses, AttrIdMap) ->
    resolve_template_fields(Uses, AttrIdMap, []).

-spec resolve_template_fields([map()], map(), [{integer(), binary()}]) -> {ok, [{integer(), binary()}]} | {error, term()}.
resolve_template_fields([], _AttrIdMap, Acc) ->
    {ok, lists:reverse(Acc)};
resolve_template_fields([Use | Rest], AttrIdMap, Acc) ->
    AttrSymbol = maps:get(attribute, Use),
    Alias = maps:get(field_name, Use),
    case maps:get(AttrSymbol, AttrIdMap, undefined) of
        undefined ->
            {error, {unresolved_use_attribute, AttrSymbol}};
        AttrId ->
            resolve_template_fields(Rest, AttrIdMap, [{AttrId, Alias} | Acc])
    end.

-spec ensure_attributes([map()], map()) -> {ok, map()} | {error, term(), map()}.
ensure_attributes([], Summary) ->
    case maps:get(failed, Summary, []) of
        [] -> {ok, Summary};
        Failed -> {error, {attribute_setup_failed, Failed}, Summary}
    end;
ensure_attributes([AttrDef | Rest], Summary0) ->
    Symbol = maps:get(symbol, AttrDef),
    Name = attribute_name(AttrDef, Symbol),
    Alias = Symbol,
    QualName = attribute_qualname(AttrDef, Name),
    IsArray = maps:get(is_array, AttrDef, true),
    case datatype_to_id(maps:get(datatype, AttrDef, <<"STRING">>)) of
        {ok, TypeId} ->
            Summary1 = ensure_attribute(Alias, Name, QualName, TypeId, IsArray, Summary0),
            ensure_attributes(Rest, Summary1);
        {error, invalid_datatype} ->
            Failed = #{
                attribute => Name,
                reason => invalid_datatype,
                datatype => maps:get(datatype, AttrDef, undefined)
            },
            ensure_attributes(Rest, Summary0#{failed => [Failed | maps:get(failed, Summary0, [])]})
    end.

-spec ensure_attribute(binary(), binary(), binary(), integer(), boolean(), map()) -> map().
ensure_attribute(Alias, Name, QualName, TypeId, IsArray, Summary0) ->
    case get_attribute_info(Name) of
        {ok, _Info} ->
            Summary0#{existing => maps:get(existing, Summary0, 0) + 1};
        not_found ->
            case create_attribute(Alias, Name, QualName, TypeId, IsArray) of
                {ok, _Created} ->
                    Summary0#{created => maps:get(created, Summary0, 0) + 1};
                {error, Reason} ->
                    Failed = #{attribute => Name, reason => Reason},
                    Summary0#{failed => [Failed | maps:get(failed, Summary0, [])]}
            end;
        {error, Reason} ->
            Failed = #{attribute => Name, reason => Reason},
            Summary0#{failed => [Failed | maps:get(failed, Summary0, [])]}
    end.

-spec datatype_to_id(binary() | undefined) -> {ok, integer()} | {error, invalid_datatype}.
datatype_to_id(<<"STRING">>) -> {ok, 1};
datatype_to_id(<<"TIME">>) -> {ok, 2};
datatype_to_id(<<"INTEGER">>) -> {ok, 3};
datatype_to_id(<<"LONG">>) -> {ok, 4};
datatype_to_id(<<"DOUBLE">>) -> {ok, 5};
datatype_to_id(<<"BOOLEAN">>) -> {ok, 6};
datatype_to_id(<<"DATA">>) -> {ok, 7};
datatype_to_id(<<"RECORD">>) -> {ok, 99};
datatype_to_id(_) -> {error, invalid_datatype}.

-spec attribute_name(map(), binary()) -> binary().
attribute_name(AttrDef, Symbol) ->
    case maps:get(name, AttrDef, undefined) of
        undefined -> Symbol;
        <<>> -> Symbol;
        Name when is_binary(Name) -> Name;
        _ -> Symbol
    end.

-spec attribute_qualname(map(), binary()) -> binary().
attribute_qualname(AttrDef, Name) ->
    case maps:get(qualname, AttrDef, undefined) of
        undefined -> Name;
        <<>> -> Name;
        QualName when is_binary(QualName) -> QualName;
        _ -> Name
    end.
