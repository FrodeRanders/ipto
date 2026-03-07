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
-module(ipto_graphql_resolvers).

-include("ipto.hrl").

-export([resolve/4]).

-spec resolve(query | mutation, atom(), map(), map()) ->
    {ok, unit_map() | search_result() | boolean() | integer() | map() | [relation()] | [association()] | null} |
    {error, ipto_reason()}.
resolve(query, unit, Args, _Context) ->
    TenantId = maps:get(tenantid, Args),
    UnitId = maps:get(unitid, Args),
    case ipto:get_unit(TenantId, UnitId) of
        {ok, Unit} -> {ok, Unit};
        not_found -> {ok, null};
        Error -> Error
    end;
resolve(query, unitByCorrid, Args, _Context) ->
    CorrId = maps:get(corrid, Args),
    Expr0 = #{corrid => CorrId},
    Expr = put_if_defined(tenantid, maps:get(tenantid, Args, undefined), Expr0),
    case ipto:search_units(Expr, {created, desc}, #{limit => 1, offset => 0}) of
        {ok, #{results := [Unit | _]}} -> {ok, Unit};
        {ok, #{results := []}} -> {ok, null};
        Error -> Error
    end;
resolve(query, unitExists, Args, _Context) ->
    TenantId = maps:get(tenantid, Args),
    UnitId = maps:get(unitid, Args),
    {ok, ipto:unit_exists(TenantId, UnitId)};
resolve(query, unitLocked, Args, _Context) ->
    UnitRef = unit_ref_from_args(Args),
    {ok, ipto:is_unit_locked(UnitRef)};
resolve(query, units, Args, _Context) ->
    Query = maps:get(query, Args, <<>>),
    Limit = maps:get(limit, Args, 50),
    Offset = maps:get(offset, Args, 0),
    Order = parse_order(Args),
    ipto:search_units(Query, Order, #{limit => Limit, offset => Offset});
resolve(query, unitsByExpression, Args, _Context) ->
    Expr = expression_from_args(Args),
    Limit = maps:get(limit, Args, 50),
    Offset = maps:get(offset, Args, 0),
    Order = parse_order(Args),
    ipto:search_units(Expr, Order, #{limit => Limit, offset => Offset});
resolve(query, rightRelation, Args, _Context) ->
    UnitRef = unit_ref_from_args(Args),
    RelType = maps:get(reltype, Args),
    case ipto:get_right_relation(UnitRef, RelType) of
        {ok, Relation} -> {ok, Relation};
        not_found -> {ok, null};
        Error -> Error
    end;
resolve(query, rightRelations, Args, _Context) ->
    UnitRef = unit_ref_from_args(Args),
    RelType = maps:get(reltype, Args),
    ipto:get_right_relations(UnitRef, RelType);
resolve(query, leftRelations, Args, _Context) ->
    UnitRef = unit_ref_from_args(Args),
    RelType = maps:get(reltype, Args),
    ipto:get_left_relations(UnitRef, RelType);
resolve(query, countRightRelations, Args, _Context) ->
    UnitRef = unit_ref_from_args(Args),
    RelType = maps:get(reltype, Args),
    ipto:count_right_relations(UnitRef, RelType);
resolve(query, countLeftRelations, Args, _Context) ->
    UnitRef = unit_ref_from_args(Args),
    RelType = maps:get(reltype, Args),
    ipto:count_left_relations(UnitRef, RelType);
resolve(query, rightAssociation, Args, _Context) ->
    UnitRef = unit_ref_from_args(Args),
    AssocType = maps:get(assoctype, Args),
    case ipto:get_right_association(UnitRef, AssocType) of
        {ok, Association} -> {ok, Association};
        not_found -> {ok, null};
        Error -> Error
    end;
resolve(query, rightAssociations, Args, _Context) ->
    UnitRef = unit_ref_from_args(Args),
    AssocType = maps:get(assoctype, Args),
    ipto:get_right_associations(UnitRef, AssocType);
resolve(query, leftAssociations, Args, _Context) ->
    AssocType = maps:get(assoctype, Args),
    AssocString = to_text(maps:get(assocstring, Args)),
    ipto:get_left_associations(AssocType, AssocString);
resolve(query, countRightAssociations, Args, _Context) ->
    UnitRef = unit_ref_from_args(Args),
    AssocType = maps:get(assoctype, Args),
    ipto:count_right_associations(UnitRef, AssocType);
resolve(query, countLeftAssociations, Args, _Context) ->
    AssocType = maps:get(assoctype, Args),
    AssocString = to_text(maps:get(assocstring, Args)),
    ipto:count_left_associations(AssocType, AssocString);
resolve(query, tenantInfo, Args, _Context) ->
    case resolve_name_or_id(Args) of
        {ok, NameOrId} ->
            case ipto:get_tenant_info(NameOrId) of
                {ok, Tenant} -> {ok, to_graphql_tenant_info(Tenant)};
                not_found -> {ok, null};
                Error -> Error
            end;
        Error ->
            Error
    end;
resolve(query, attributeInfo, Args, _Context) ->
    case resolve_name_or_id(Args) of
        {ok, NameOrId} ->
            case ipto:get_attribute_info(NameOrId) of
                {ok, Attribute} -> {ok, to_graphql_attribute_info(Attribute)};
                not_found -> {ok, null};
                Error -> Error
            end;
        Error ->
            Error
    end;
resolve(query, inspectGraphqlSdl, Args, _Context) ->
    Sdl = maps:get(sdl, Args),
    Catalog = ipto:inspect_graphql_sdl(Sdl),
    {ok, to_graphql_catalog(Catalog)};
resolve(query, registeredOperations, _Args, _Context) ->
    {ok, to_graphql_registered_operations(ipto_graphql_operations:registered())};
resolve(mutation, createUnit, Args, _Context) ->
    TenantId = maps:get(tenantid, Args),
    Name = maps:get(name, Args, undefined),
    case Name of
        undefined ->
            case ipto:create_unit(TenantId) of
                {ok, Unit} -> ipto:store_unit(Unit);
                Error -> Error
            end;
        _ ->
            case ipto:create_unit(TenantId, Name) of
                {ok, Unit} -> ipto:store_unit(Unit);
                Error -> Error
            end
    end;
resolve(mutation, inactivateUnit, Args, _Context) ->
    UnitRef = #{tenantid => maps:get(tenantid, Args), unitid => maps:get(unitid, Args)},
    case ipto:inactivate_unit(UnitRef) of
        ok -> {ok, true};
        Error -> Error
    end;
resolve(mutation, activateUnit, Args, _Context) ->
    UnitRef = #{tenantid => maps:get(tenantid, Args), unitid => maps:get(unitid, Args)},
    case ipto:activate_unit(UnitRef) of
        ok -> {ok, true};
        Error -> Error
    end;
resolve(mutation, requestStatusTransition, Args, _Context) ->
    UnitRef = unit_ref_from_args(Args),
    RequestedStatus = maps:get(status, Args),
    case ipto:request_status_transition(UnitRef, RequestedStatus) of
        {ok, Status} -> {ok, #{status => Status}};
        Error -> Error
    end;
resolve(mutation, transitionUnitStatus, Args, Context) ->
    resolve(mutation, requestStatusTransition, Args, Context);
resolve(mutation, lockUnit, Args, _Context) ->
    UnitRef = #{tenantid => maps:get(tenantid, Args), unitid => maps:get(unitid, Args)},
    LockType = maps:get(locktype, Args),
    Purpose = to_text(maps:get(purpose, Args)),
    case ipto:lock_unit(UnitRef, LockType, Purpose) of
        ok -> {ok, true};
        already_locked -> {ok, false};
        Error -> Error
    end;
resolve(mutation, unlockUnit, Args, _Context) ->
    UnitRef = #{tenantid => maps:get(tenantid, Args), unitid => maps:get(unitid, Args)},
    case ipto:unlock_unit(UnitRef) of
        ok -> {ok, true};
        Error -> Error
    end;
resolve(mutation, addRelation, Args, _Context) ->
    UnitRef = #{tenantid => maps:get(tenantid, Args), unitid => maps:get(unitid, Args)},
    OtherRef = #{tenantid => maps:get(reltenantid, Args), unitid => maps:get(relunitid, Args)},
    RelType = maps:get(reltype, Args),
    case ipto:add_relation(UnitRef, RelType, OtherRef) of
        ok -> {ok, true};
        Error -> Error
    end;
resolve(mutation, removeRelation, Args, _Context) ->
    UnitRef = #{tenantid => maps:get(tenantid, Args), unitid => maps:get(unitid, Args)},
    OtherRef = #{tenantid => maps:get(reltenantid, Args), unitid => maps:get(relunitid, Args)},
    RelType = maps:get(reltype, Args),
    case ipto:remove_relation(UnitRef, RelType, OtherRef) of
        ok -> {ok, true};
        Error -> Error
    end;
resolve(mutation, addAssociation, Args, _Context) ->
    UnitRef = #{tenantid => maps:get(tenantid, Args), unitid => maps:get(unitid, Args)},
    AssocType = maps:get(assoctype, Args),
    RefString = to_text(maps:get(assocstring, Args)),
    case ipto:add_association(UnitRef, AssocType, RefString) of
        ok -> {ok, true};
        Error -> Error
    end;
resolve(mutation, removeAssociation, Args, _Context) ->
    UnitRef = #{tenantid => maps:get(tenantid, Args), unitid => maps:get(unitid, Args)},
    AssocType = maps:get(assoctype, Args),
    RefString = to_text(maps:get(assocstring, Args)),
    case ipto:remove_association(UnitRef, AssocType, RefString) of
        ok -> {ok, true};
        Error -> Error
    end;
resolve(mutation, configureGraphqlSdl, Args, _Context) ->
    Sdl = maps:get(sdl, Args),
    case ipto:configure_graphql_sdl(Sdl) of
        {ok, Summary} -> {ok, to_graphql_configure_result(Summary)};
        Error -> Error
    end;
resolve(mutation, configureGraphqlSdlFile, Args, _Context) ->
    Path = maps:get(path, Args),
    case ipto:configure_graphql_sdl_file(Path) of
        {ok, Summary} -> {ok, to_graphql_configure_result(Summary)};
        Error -> Error
    end;
resolve(Type, Field, _Args, _Context) ->
    {error, {unsupported_resolver, {Type, Field}}}.

-spec to_graphql_catalog(map()) -> map().
to_graphql_catalog(Catalog) ->
    Registry0 = maps:get(attribute_registry, Catalog, #{}),
    Counts0 = maps:get(counts, Catalog, #{}),
    #{
        attribute_registry => #{
            found => maps:get(found, Registry0, false),
            enum_name => nullable_text(maps:get(enum_name, Registry0, undefined)),
            attributes => [to_text(V) || V <- maps:get(attributes, Registry0, [])],
            attribute_defs => [to_graphql_attr_def(D) || D <- maps:get(attribute_defs, Registry0, [])]
        },
        records => [to_graphql_record(R) || R <- maps:get(records, Catalog, [])],
        templates => [to_graphql_template(T) || T <- maps:get(templates, Catalog, [])],
        counts => #{
            attributes => maps:get(attributes, Counts0, 0),
            records => maps:get(records, Counts0, 0),
            templates => maps:get(templates, Counts0, 0)
        },
        errors => [to_graphql_error(E) || E <- maps:get(errors, Catalog, [])]
    }.

-spec to_graphql_configure_result(map()) -> map().
to_graphql_configure_result(Summary) ->
    Attributes0 = maps:get(attributes, Summary, #{}),
    Records0 = maps:get(records, Summary, #{}),
    Templates0 = maps:get(templates, Summary, #{}),
    #{
        attributes => #{
            created => maps:get(created, Attributes0, 0),
            existing => maps:get(existing, Attributes0, 0),
            failed => [to_graphql_failure(F) || F <- maps:get(failed, Attributes0, [])]
        },
        records => to_graphql_persist_summary(Records0),
        templates => to_graphql_persist_summary(Templates0)
    }.

-spec to_graphql_attr_def(map()) -> map().
to_graphql_attr_def(Def) ->
    #{
        symbol => to_text(maps:get(symbol, Def)),
        datatype => nullable_text(maps:get(datatype, Def, undefined)),
        is_array => maps:get(is_array, Def, true),
        name => nullable_text(maps:get(name, Def, undefined)),
        qualname => nullable_text(maps:get(qualname, Def, undefined)),
        uri => nullable_text(maps:get(uri, Def, undefined)),
        description => nullable_text(maps:get(description, Def, undefined))
    }.

-spec to_graphql_record(map()) -> map().
to_graphql_record(Record0) ->
    #{
        type_name => to_text(maps:get(type_name, Record0)),
        attribute => nullable_text(maps:get(attribute, Record0, undefined)),
        uses => [to_graphql_use(U) || U <- maps:get(uses, Record0, [])]
    }.

-spec to_graphql_template(map()) -> map().
to_graphql_template(Template0) ->
    #{
        type_name => to_text(maps:get(type_name, Template0)),
        name => nullable_text(maps:get(name, Template0, undefined)),
        uses => [to_graphql_use(U) || U <- maps:get(uses, Template0, [])]
    }.

-spec to_graphql_tenant_info(map()) -> map().
to_graphql_tenant_info(Tenant) ->
    #{
        id => maps:get(id, Tenant, 0),
        name => nullable_text(maps:get(name, Tenant, undefined)),
        description => nullable_text(maps:get(description, Tenant, undefined)),
        created => nullable_text(maps:get(created, Tenant, undefined))
    }.

-spec to_graphql_attribute_info(map()) -> map().
to_graphql_attribute_info(Attribute) ->
    #{
        id => maps:get(id, Attribute, 0),
        alias => nullable_text(maps:get(alias, Attribute, undefined)),
        name => nullable_text(maps:get(name, Attribute, undefined)),
        qualname => nullable_text(maps:get(qualname, Attribute, undefined)),
        type => nullable_int(maps:get(type, Attribute, undefined)),
        forced_scalar => nullable_bool(maps:get(forced_scalar, Attribute, undefined))
    }.

-spec to_graphql_use(map()) -> map().
to_graphql_use(Use0) ->
    #{
        field_name => to_text(maps:get(field_name, Use0)),
        attribute => to_text(maps:get(attribute, Use0))
    }.

-spec to_graphql_error(map()) -> map().
to_graphql_error(Error0) ->
    Context = maps:get(context, Error0, #{}),
    #{
        code => to_text(maps:get(code, Error0, <<"unknown">>)),
        message => to_text(maps:get(message, Error0, <<"">>)),
        context_json => to_text(json:encode(normalize_json(Context)))
    }.

-spec to_graphql_persist_summary(map()) -> map().
to_graphql_persist_summary(Summary0) ->
    #{
        supported => maps:get(supported, Summary0, false),
        reason => nullable_text(maps:get(reason, Summary0, undefined)),
        count => maps:get(count, Summary0, 0),
        persisted => nullable_int(maps:get(persisted, Summary0, undefined)),
        failed => [to_graphql_failure(F) || F <- maps:get(failed, Summary0, [])]
    }.

-spec to_graphql_registered_operations(map()) -> map().
to_graphql_registered_operations(Ops) ->
    #{
        queries => [{ok, #{name => Name}} || Name <- maps:get(queries, Ops, [])],
        mutations => [{ok, #{name => Name}} || Name <- maps:get(mutations, Ops, [])]
    }.

-spec to_graphql_failure(map()) -> map().
to_graphql_failure(Failure0) ->
    #{
        attribute => nullable_text(maps:get(attribute, Failure0, undefined)),
        record => nullable_text(maps:get(record, Failure0, undefined)),
        template => nullable_text(maps:get(template, Failure0, undefined)),
        datatype => nullable_text(maps:get(datatype, Failure0, undefined)),
        reason => to_text(maps:get(reason, Failure0, <<"unknown">>))
    }.

-spec nullable_int(term()) -> integer() | null.
nullable_int(undefined) -> null;
nullable_int(Value) when is_integer(Value) -> Value;
nullable_int(_) -> null.

-spec nullable_bool(term()) -> boolean() | null.
nullable_bool(undefined) -> null;
nullable_bool(Value) when is_boolean(Value) -> Value;
nullable_bool(_) -> null.

-spec nullable_text(term()) -> binary() | null.
nullable_text(undefined) -> null;
nullable_text(Value) when Value =:= null -> null;
nullable_text(Value) -> to_text(Value).

-spec resolve_name_or_id(map()) -> {ok, name_or_id()} | {error, ipto_reason()}.
resolve_name_or_id(Args) ->
    case maps:get(id, Args, undefined) of
        Id when is_integer(Id), Id > 0 ->
            {ok, Id};
        _ ->
            case maps:get(name, Args, undefined) of
                Name when is_binary(Name); is_list(Name) ->
                    {ok, to_text(Name)};
                _ ->
                    {error, {invalid_arguments, expected_id_or_name}}
            end
    end.

-spec unit_ref_from_args(map()) -> unit_ref_map().
unit_ref_from_args(Args) ->
    #{tenantid => maps:get(tenantid, Args), unitid => maps:get(unitid, Args)}.

-spec to_text(term()) -> binary().
to_text(Value) when is_binary(Value) ->
    Value;
to_text(Value) when is_atom(Value) ->
    atom_to_binary(Value, utf8);
to_text(Value) when is_list(Value) ->
    unicode:characters_to_binary(Value);
to_text(Value) when is_integer(Value); is_float(Value); is_boolean(Value) ->
    unicode:characters_to_binary(io_lib:format("~p", [Value]));
to_text(Value) ->
    unicode:characters_to_binary(io_lib:format("~p", [Value])).

-spec normalize_json(term()) -> term().
normalize_json(Value) when is_map(Value) ->
    maps:from_list([{json_key(K), normalize_json(V)} || {K, V} <- maps:to_list(Value)]);
normalize_json(Value) when is_list(Value) ->
    [normalize_json(V) || V <- Value];
normalize_json(Value) when is_atom(Value) ->
    atom_to_binary(Value, utf8);
normalize_json(Value) ->
    Value.

-spec json_key(term()) -> binary() | term().
json_key(Key) when is_atom(Key) ->
    atom_to_binary(Key, utf8);
json_key(Key) when is_binary(Key) ->
    Key;
json_key(Key) ->
    unicode:characters_to_binary(io_lib:format("~p", [Key])).

-spec parse_order(map()) -> search_order().
parse_order(Args) ->
    Field = parse_order_field(maps:get(orderField, Args, <<"created">>)),
    Dir = parse_order_dir(maps:get(orderDir, Args, <<"desc">>)),
    {Field, Dir}.

-spec parse_order_field(term()) -> atom().
parse_order_field(Value0) ->
    Value = to_text(Value0),
    case string:lowercase(binary_to_list(Value)) of
        "modified" -> modified;
        "unitid" -> unitid;
        "status" -> status;
        _ -> created
    end.

-spec parse_order_dir(term()) -> asc | desc.
parse_order_dir(Value0) ->
    Value = to_text(Value0),
    case string:lowercase(binary_to_list(Value)) of
        "asc" -> asc;
        _ -> desc
    end.

-spec expression_from_args(map()) -> map().
expression_from_args(Args) ->
    Expr0 = #{},
    Expr1 = put_if_defined(tenantid, maps:get(tenantid, Args, undefined), Expr0),
    Expr2 = put_if_defined(unitid, maps:get(unitid, Args, undefined), Expr1),
    Expr3 = put_if_defined(status, maps:get(status, Args, undefined), Expr2),
    Expr4 = put_if_defined(name, maps:get(name, Args, undefined), Expr3),
    Expr5 = put_if_defined(name_ilike, maps:get(nameLike, Args, undefined), Expr4),
    put_if_defined(corrid, maps:get(corrid, Args, undefined), Expr5).

-spec put_if_defined(atom(), term(), map()) -> map().
put_if_defined(_Key, undefined, Expr) ->
    Expr;
put_if_defined(_Key, null, Expr) ->
    Expr;
put_if_defined(Key, Value, Expr) ->
    Expr#{Key => Value}.
