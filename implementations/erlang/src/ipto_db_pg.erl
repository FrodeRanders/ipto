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
-module(ipto_db_pg).
-behaviour(ipto_backend).

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

%% Backend API

-spec get_unit_json(tenantid(), unitid(), version_selector()) -> unit_lookup_result().
get_unit_json(TenantId, UnitId, latest) ->
    get_unit_json(TenantId, UnitId, -1);
get_unit_json(TenantId, UnitId, Version) when is_integer(Version), Version /= 0 ->
    with_pg(fun(Conn) ->
        case pg_get_unit_json_proc(Conn, TenantId, UnitId, Version) of
            {ok, _} = Ok ->
                Ok;
            _ ->
                pg_get_unit_json_sql(Conn, TenantId, UnitId, Version)
        end
    end);
get_unit_json(_TenantId, _UnitId, _Version) ->
    {error, invalid_version}.

-spec unit_exists(tenantid(), unitid()) -> boolean().
unit_exists(TenantId, UnitId) ->
    with_pg(fun(Conn) ->
        Sql = "SELECT 1 FROM repo.repo_unit_kernel WHERE tenantid = $1 AND unitid = $2",
        case query_rows(epgsql:equery(Conn, Sql, [TenantId, UnitId])) of
            {ok, []} -> false;
            {ok, _Rows} -> true;
            _ -> false
        end
    end).

-spec store_unit_json(unit_map()) -> ipto_result(unit_map()).
store_unit_json(UnitMap) ->
    with_pg(fun(Conn) ->
        case pg_store_unit_json_proc(Conn, UnitMap) of
            {ok, _} = Ok ->
                Ok;
            _ ->
                case maps:get(unitid, UnitMap, undefined) of
                    undefined ->
                        pg_store_new_unit(Conn, UnitMap);
                    _ ->
                        pg_store_new_version(Conn, UnitMap)
                end
        end
    end).

-spec search_units(term(), search_order(), search_paging()) -> ipto_result(search_result()).
search_units(Expression, Order, PagingOrLimit) when is_tuple(Expression) ->
    search_units_ast(resolve_attr_expr(Expression), Order, PagingOrLimit);
search_units(Expression, Order, PagingOrLimit) when is_map(Expression) ->
    Expr = ipto_search_parser:map_to_ast(Expression),
    search_units_ast(resolve_attr_expr(Expr), Order, PagingOrLimit);
search_units(Expression, Order, PagingOrLimit) when is_binary(Expression); is_list(Expression) ->
    case ipto_search_parser:parse_ast(Expression) of
        {ok, Expr} -> search_units_ast(resolve_attr_expr(Expr), Order, PagingOrLimit);
        Error -> Error
    end;
search_units(_Expression, _Order, _PagingOrLimit) ->
    {error, invalid_query}.

-spec search_units_ast(ipto_search_ast:search_expr(), search_order(), search_paging()) -> ipto_result(search_result()).
search_units_ast(Expr, Order, Paging) ->
    with_pg(fun(Conn) ->
        Compiled = ipto_search_sql:compile(Expr, Order, Paging),
        Sql = maps:get(sql, Compiled),
        Params = maps:get(params, Compiled),
        CountSql = maps:get(count_sql, Compiled),
        CountParams = maps:get(count_params, Compiled),
        case query_rows(epgsql:equery(Conn, CountSql, CountParams)) of
            {ok, [CountRow | _]} ->
                [Total] = ipto_db_utils:row_values(CountRow),
                case query_rows(epgsql:equery(Conn, Sql, Params)) of
                    {ok, Rows} ->
                        Results = [row_to_unit_map(Row) || Row <- Rows],
                        {ok, #{results => Results, total => Total}};
                    Error2 -> {error, Error2}
                end;
            Error1 -> {error, Error1}
        end
    end).

-spec resolve_attr_expr(ipto_search_ast:search_expr()) -> ipto_search_ast:search_expr().
resolve_attr_expr({'$and', Left, Right}) ->
    ipto_search_ast:and_expr(resolve_attr_expr(Left), resolve_attr_expr(Right));
resolve_attr_expr({'$or', Left, Right}) ->
    ipto_search_ast:or_expr(resolve_attr_expr(Left), resolve_attr_expr(Right));
resolve_attr_expr({'$not', Inner}) ->
    ipto_search_ast:not_expr(resolve_attr_expr(Inner));
resolve_attr_expr({'$between', Item1, Item2}) ->
    ipto_search_ast:between_expr(resolve_attr_item(Item1), resolve_attr_item(Item2));
resolve_attr_expr({leaf, Item}) ->
    {leaf, resolve_attr_item(Item)}.

-spec resolve_attr_item(ipto_search_ast:search_item()) -> ipto_search_ast:search_item().
resolve_attr_item({attr, Name, undefined, Type, Op, Value}) ->
    case get_attribute_info(Name) of
        {ok, Info} ->
            AttrId = maps:get(id, Info),
            ResolvedType = case maps:get(type, Info) of
                T when is_integer(T) -> attr_type_from_int(T);
                _ -> Type
            end,
            {attr, Name, AttrId, ResolvedType, Op, Value};
        _ ->
            {attr, Name, undefined, Type, Op, Value}
    end;
resolve_attr_item(Item) ->
    Item.

-spec attr_type_from_int(integer()) -> ipto_search_ast:attribute_type().
attr_type_from_int(1) -> record;
attr_type_from_int(2) -> string;
attr_type_from_int(3) -> integer;
attr_type_from_int(4) -> long;
attr_type_from_int(5) -> double;
attr_type_from_int(6) -> boolean;
attr_type_from_int(7) -> time;
attr_type_from_int(_) -> string.

-spec add_relation(unit_ref_value(), relation_type(), unit_ref_value()) -> ok | {error, ipto_reason()}.
add_relation(UnitRef, RelType, OtherUnitRef) when is_integer(RelType) ->
    case {ipto_db_utils:normalize_ref(UnitRef), ipto_db_utils:normalize_ref(OtherUnitRef)} of
        {{TenantId, UnitId}, {RelTenantId, RelUnitId}} ->
            with_pg(fun(Conn) ->
                Sql = "INSERT INTO repo.repo_internal_relation (tenantid, unitid, reltype, reltenantid, relunitid) VALUES ($1, $2, $3, $4, $5) ON CONFLICT DO NOTHING",
                case epgsql:equery(Conn, Sql, [TenantId, UnitId, RelType, RelTenantId, RelUnitId]) of
                    {ok, _Count} -> ok;
                    {ok, _Cols, _Rows} -> ok;
                    Error -> {error, Error}
                end
            end);
        _ ->
            {error, invalid_relation_ref}
    end;
add_relation(_UnitRef, _RelType, _OtherUnitRef) ->
    {error, invalid_relation_type}.

-spec remove_relation(unit_ref_value(), relation_type(), unit_ref_value()) -> ok | {error, ipto_reason()}.
remove_relation(UnitRef, RelType, OtherUnitRef) when is_integer(RelType) ->
    case {ipto_db_utils:normalize_ref(UnitRef), ipto_db_utils:normalize_ref(OtherUnitRef)} of
        {{TenantId, UnitId}, {RelTenantId, RelUnitId}} ->
            with_pg(fun(Conn) ->
                Sql = "DELETE FROM repo.repo_internal_relation WHERE tenantid = $1 AND unitid = $2 AND reltype = $3 AND reltenantid = $4 AND relunitid = $5",
                case epgsql:equery(Conn, Sql, [TenantId, UnitId, RelType, RelTenantId, RelUnitId]) of
                    {ok, _Count} -> ok;
                    {ok, _Cols, _Rows} -> ok;
                    Error -> {error, Error}
                end
            end);
        _ ->
            {error, invalid_relation_ref}
    end;
remove_relation(_UnitRef, _RelType, _OtherUnitRef) ->
    {error, invalid_relation_type}.

-spec get_right_relation(unit_ref_value(), relation_type()) -> relation_lookup_result().
get_right_relation(UnitRef, RelType) when is_integer(RelType) ->
    case ipto_db_utils:normalize_ref(UnitRef) of
        {TenantId, UnitId} ->
            with_pg(fun(Conn) ->
                Sql = "SELECT tenantid, unitid, reltype, reltenantid, relunitid FROM repo.repo_internal_relation WHERE tenantid = $1 AND unitid = $2 AND reltype = $3 LIMIT 1",
                case query_rows(epgsql:equery(Conn, Sql, [TenantId, UnitId, RelType])) of
                    {ok, []} ->
                        not_found;
                    {ok, [Row | _]} ->
                        case relation_from_row(ipto_db_utils:row_values(Row)) of
                            Rel when is_map(Rel), map_size(Rel) =:= 0 ->
                                {error, {invalid_relation_row, Row}};
                            Rel ->
                                {ok, Rel}
                        end;
                    Error ->
                        {error, Error}
                end
            end);
        _ ->
            {error, invalid_unit_ref}
    end;
get_right_relation(_UnitRef, _RelType) ->
    {error, invalid_relation_type}.

-spec get_right_relations(unit_ref_value(), relation_type()) -> ipto_result([relation()]).
get_right_relations(UnitRef, RelType) when is_integer(RelType) ->
    case ipto_db_utils:normalize_ref(UnitRef) of
        {TenantId, UnitId} ->
            with_pg(fun(Conn) ->
                Sql = "SELECT tenantid, unitid, reltype, reltenantid, relunitid FROM repo.repo_internal_relation WHERE tenantid = $1 AND unitid = $2 AND reltype = $3",
                case query_rows(epgsql:equery(Conn, Sql, [TenantId, UnitId, RelType])) of
                    {ok, Rows} ->
                        Relations = [relation_from_row(ipto_db_utils:row_values(Row)) || Row <- Rows],
                        {ok, [R || R <- Relations, R =/= #{}]};
                    Error ->
                        {error, Error}
                end
            end);
        _ ->
            {error, invalid_unit_ref}
    end;
get_right_relations(_UnitRef, _RelType) ->
    {error, invalid_relation_type}.

-spec get_left_relations(unit_ref_value(), relation_type()) -> ipto_result([relation()]).
get_left_relations(UnitRef, RelType) when is_integer(RelType) ->
    case ipto_db_utils:normalize_ref(UnitRef) of
        {TenantId, UnitId} ->
            with_pg(fun(Conn) ->
                Sql = "SELECT tenantid, unitid, reltype, reltenantid, relunitid FROM repo.repo_internal_relation WHERE reltenantid = $1 AND relunitid = $2 AND reltype = $3",
                case query_rows(epgsql:equery(Conn, Sql, [TenantId, UnitId, RelType])) of
                    {ok, Rows} ->
                        Relations = [relation_from_row(ipto_db_utils:row_values(Row)) || Row <- Rows],
                        {ok, [R || R <- Relations, R =/= #{}]};
                    Error ->
                        {error, Error}
                end
            end);
        _ ->
            {error, invalid_unit_ref}
    end;
get_left_relations(_UnitRef, _RelType) ->
    {error, invalid_relation_type}.

-spec count_right_relations(unit_ref_value(), relation_type()) -> ipto_result(non_neg_integer()).
count_right_relations(UnitRef, RelType) when is_integer(RelType) ->
    case ipto_db_utils:normalize_ref(UnitRef) of
        {TenantId, UnitId} ->
            with_pg(fun(Conn) ->
                Sql = "SELECT COUNT(*) FROM repo.repo_internal_relation WHERE tenantid = $1 AND unitid = $2 AND reltype = $3",
                case query_rows(epgsql:equery(Conn, Sql, [TenantId, UnitId, RelType])) of
                    {ok, []} ->
                        {ok, 0};
                    {ok, [Row | _]} ->
                        [Count] = ipto_db_utils:row_values(Row),
                        {ok, Count};
                    Error ->
                        {error, Error}
                end
            end);
        _ ->
            {error, invalid_unit_ref}
    end;
count_right_relations(_UnitRef, _RelType) ->
    {error, invalid_relation_type}.

-spec count_left_relations(unit_ref_value(), relation_type()) -> ipto_result(non_neg_integer()).
count_left_relations(UnitRef, RelType) when is_integer(RelType) ->
    case ipto_db_utils:normalize_ref(UnitRef) of
        {TenantId, UnitId} ->
            with_pg(fun(Conn) ->
                Sql = "SELECT COUNT(*) FROM repo.repo_internal_relation WHERE reltenantid = $1 AND relunitid = $2 AND reltype = $3",
                case query_rows(epgsql:equery(Conn, Sql, [TenantId, UnitId, RelType])) of
                    {ok, []} ->
                        {ok, 0};
                    {ok, [Row | _]} ->
                        [Count] = ipto_db_utils:row_values(Row),
                        {ok, Count};
                    Error ->
                        {error, Error}
                end
            end);
        _ ->
            {error, invalid_unit_ref}
    end;
count_left_relations(_UnitRef, _RelType) ->
    {error, invalid_relation_type}.

-spec add_association(unit_ref_value(), association_type(), ref_string()) -> ok | {error, ipto_reason()}.
add_association(UnitRef, AssocType, RefString) when is_integer(AssocType), (is_binary(RefString) orelse is_list(RefString)) ->
    case ipto_db_utils:normalize_ref(UnitRef) of
        {TenantId, UnitId} ->
            AssocString = ipto_db_utils:normalize_string(RefString),
            with_pg(fun(Conn) ->
                Sql = "INSERT INTO repo.repo_external_assoc (tenantid, unitid, assoctype, assocstring) VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING",
                case epgsql:equery(Conn, Sql, [TenantId, UnitId, AssocType, AssocString]) of
                    {ok, _Count} -> ok;
                    {ok, _Cols, _Rows} -> ok;
                    Error -> {error, Error}
                end
            end);
        _ ->
            {error, invalid_association_ref}
    end;
add_association(_UnitRef, _AssocType, _RefString) ->
    {error, invalid_association}.

-spec remove_association(unit_ref_value(), association_type(), ref_string()) -> ok | {error, ipto_reason()}.
remove_association(UnitRef, AssocType, RefString) when is_integer(AssocType), (is_binary(RefString) orelse is_list(RefString)) ->
    case ipto_db_utils:normalize_ref(UnitRef) of
        {TenantId, UnitId} ->
            AssocString = ipto_db_utils:normalize_string(RefString),
            with_pg(fun(Conn) ->
                Sql = "DELETE FROM repo.repo_external_assoc WHERE tenantid = $1 AND unitid = $2 AND assoctype = $3 AND assocstring = $4",
                case epgsql:equery(Conn, Sql, [TenantId, UnitId, AssocType, AssocString]) of
                    {ok, _Count} -> ok;
                    {ok, _Cols, _Rows} -> ok;
                    Error -> {error, Error}
                end
            end);
        _ ->
            {error, invalid_association_ref}
    end;
remove_association(_UnitRef, _AssocType, _RefString) ->
    {error, invalid_association}.

-spec get_right_association(unit_ref_value(), association_type()) -> association_lookup_result().
get_right_association(UnitRef, AssocType) when is_integer(AssocType) ->
    case ipto_db_utils:normalize_ref(UnitRef) of
        {TenantId, UnitId} ->
            with_pg(fun(Conn) ->
                Sql = "SELECT tenantid, unitid, assoctype, assocstring FROM repo.repo_external_assoc WHERE tenantid = $1 AND unitid = $2 AND assoctype = $3 LIMIT 1",
                case query_rows(epgsql:equery(Conn, Sql, [TenantId, UnitId, AssocType])) of
                    {ok, []} ->
                        not_found;
                    {ok, [Row | _]} ->
                        case association_from_row(ipto_db_utils:row_values(Row)) of
                            Assoc when is_map(Assoc), map_size(Assoc) =:= 0 ->
                                {error, {invalid_association_row, Row}};
                            Assoc -> {ok, Assoc}
                        end;
                    Error ->
                        {error, Error}
                end
            end);
        _ ->
            {error, invalid_unit_ref}
    end;
get_right_association(_UnitRef, _AssocType) ->
    {error, invalid_association}.

-spec get_right_associations(unit_ref_value(), association_type()) -> ipto_result([association()]).
get_right_associations(UnitRef, AssocType) when is_integer(AssocType) ->
    case ipto_db_utils:normalize_ref(UnitRef) of
        {TenantId, UnitId} ->
            with_pg(fun(Conn) ->
                Sql = "SELECT tenantid, unitid, assoctype, assocstring FROM repo.repo_external_assoc WHERE tenantid = $1 AND unitid = $2 AND assoctype = $3",
                case query_rows(epgsql:equery(Conn, Sql, [TenantId, UnitId, AssocType])) of
                    {ok, Rows} ->
                        Assocs = [association_from_row(ipto_db_utils:row_values(Row)) || Row <- Rows],
                        {ok, [A || A <- Assocs, A =/= #{}]};
                    Error ->
                        {error, Error}
                end
            end);
        _ ->
            {error, invalid_unit_ref}
    end;
get_right_associations(_UnitRef, _AssocType) ->
    {error, invalid_association}.

-spec get_left_associations(association_type(), ref_string()) -> ipto_result([association()]).
get_left_associations(AssocType, RefString) when is_integer(AssocType), (is_binary(RefString) orelse is_list(RefString)) ->
    AssocString = ipto_db_utils:normalize_string(RefString),
    with_pg(fun(Conn) ->
        Sql = "SELECT tenantid, unitid, assoctype, assocstring FROM repo.repo_external_assoc WHERE assoctype = $1 AND assocstring = $2",
        case query_rows(epgsql:equery(Conn, Sql, [AssocType, AssocString])) of
            {ok, Rows} ->
                Assocs = [association_from_row(ipto_db_utils:row_values(Row)) || Row <- Rows],
                {ok, [A || A <- Assocs, A =/= #{}]};
            Error ->
                {error, Error}
        end
    end);
get_left_associations(_AssocType, _RefString) ->
    {error, invalid_association}.

-spec count_right_associations(unit_ref_value(), association_type()) -> ipto_result(non_neg_integer()).
count_right_associations(UnitRef, AssocType) when is_integer(AssocType) ->
    case ipto_db_utils:normalize_ref(UnitRef) of
        {TenantId, UnitId} ->
            with_pg(fun(Conn) ->
                Sql = "SELECT COUNT(*) FROM repo.repo_external_assoc WHERE tenantid = $1 AND unitid = $2 AND assoctype = $3",
                case query_rows(epgsql:equery(Conn, Sql, [TenantId, UnitId, AssocType])) of
                    {ok, []} ->
                        {ok, 0};
                    {ok, [Row | _]} ->
                        [Count] = ipto_db_utils:row_values(Row),
                        {ok, Count};
                    Error ->
                        {error, Error}
                end
            end);
        _ ->
            {error, invalid_unit_ref}
    end;
count_right_associations(_UnitRef, _AssocType) ->
    {error, invalid_association}.

-spec count_left_associations(association_type(), ref_string()) -> ipto_result(non_neg_integer()).
count_left_associations(AssocType, RefString) when is_integer(AssocType), (is_binary(RefString) orelse is_list(RefString)) ->
    AssocString = ipto_db_utils:normalize_string(RefString),
    with_pg(fun(Conn) ->
        Sql = "SELECT COUNT(*) FROM repo.repo_external_assoc WHERE assoctype = $1 AND assocstring = $2",
        case query_rows(epgsql:equery(Conn, Sql, [AssocType, AssocString])) of
            {ok, []} ->
                {ok, 0};
            {ok, [Row | _]} ->
                [Count] = ipto_db_utils:row_values(Row),
                {ok, Count};
            Error ->
                {error, Error}
        end
    end);
count_left_associations(_AssocType, _RefString) ->
    {error, invalid_association}.

-spec lock_unit(unit_ref_value(), lock_type(), ref_string()) -> ok | already_locked | {error, ipto_reason()}.
lock_unit(UnitRef, LockType, Purpose) when is_integer(LockType) ->
    case ipto_db_utils:normalize_ref(UnitRef) of
        {TenantId, UnitId} ->
            with_pg(fun(Conn) ->
                ExistsSql = "SELECT 1 FROM repo.repo_lock WHERE tenantid = $1 AND unitid = $2 FETCH FIRST 1 ROWS ONLY",
                case query_rows(epgsql:equery(Conn, ExistsSql, [TenantId, UnitId])) of
                    {ok, [_ | _]} ->
                        already_locked;
                    {ok, []} ->
                        InsertSql = "INSERT INTO repo.repo_lock (tenantid, unitid, purpose, locktype, expire) VALUES ($1, $2, $3, $4, $5)",
                        case epgsql:equery(Conn, InsertSql, [TenantId, UnitId, ipto_db_utils:normalize_purpose(Purpose), LockType, null]) of
                            {ok, _Count} -> ok;
                            {ok, _InsertCols, _InsertRows} -> ok;
                            Error -> {error, Error}
                        end;
                    Error ->
                        {error, Error}
                end
            end);
        _ ->
            {error, invalid_unit_ref}
    end;
lock_unit(_UnitRef, _LockType, _Purpose) ->
    {error, invalid_lock_type}.

-spec unlock_unit(unit_ref_value()) -> ok | {error, ipto_reason()}.
unlock_unit(UnitRef) ->
    case ipto_db_utils:normalize_ref(UnitRef) of
        {TenantId, UnitId} ->
            with_pg(fun(Conn) ->
                Sql = "DELETE FROM repo.repo_lock WHERE tenantid = $1 AND unitid = $2",
                case epgsql:equery(Conn, Sql, [TenantId, UnitId]) of
                    {ok, _Count} -> ok;
                    {ok, _Cols, _Rows} -> ok;
                    Error -> {error, Error}
                end
            end);
        _ ->
            {error, invalid_unit_ref}
    end.

-spec is_unit_locked(unit_ref_value()) -> boolean().
is_unit_locked(UnitRef) ->
    case ipto_db_utils:normalize_ref(UnitRef) of
        {TenantId, UnitId} ->
            case with_pg(fun(Conn) ->
                Sql = "SELECT 1 FROM repo.repo_lock WHERE tenantid = $1 AND unitid = $2 FETCH FIRST 1 ROWS ONLY",
                case query_rows(epgsql:equery(Conn, Sql, [TenantId, UnitId])) of
                    {ok, [_ | _]} -> true;
                    {ok, []} -> false;
                    _ -> false
                end
            end) of
                true -> true;
                _ -> false
            end;
        _ ->
            false
    end.

-spec set_status(unit_ref_value(), unit_status()) -> ok | {error, ipto_reason()}.
set_status(UnitRef, Status) ->
    case ipto_db_utils:normalize_ref(UnitRef) of
        {TenantId, UnitId} ->
            with_pg(fun(Conn) ->
                Sql = "UPDATE repo.repo_unit_kernel SET status = $1 WHERE tenantid = $2 AND unitid = $3",
                case epgsql:equery(Conn, Sql, [Status, TenantId, UnitId]) of
                    {ok, Count} when is_integer(Count), Count > 0 -> ok;
                    {ok, _} -> {error, not_found};
                    Error -> {error, Error}
                end
            end);
        _ ->
            {error, invalid_unit_ref}
    end.

-spec create_attribute(attribute_alias(), attribute_name(), attribute_qualname(), attribute_type(), boolean()) ->
    ipto_result(attribute_info()).
create_attribute(Alias, Name, QualName, Type, IsArray)
  when is_binary(Name), is_binary(QualName), is_integer(Type), is_boolean(IsArray) ->
    with_pg(fun(Conn) ->
        Sql = "INSERT INTO repo.repo_attribute (attrtype, scalar, attrname, qualname, alias) VALUES ($1, $2, $3, $4, $5) RETURNING attrid, alias, attrname, qualname, attrtype, scalar",
        Params = [Type, not IsArray, to_pg_text(Name), to_pg_text(QualName), maybe_null_text(Alias)],
        case query_rows(epgsql:equery(Conn, Sql, Params)) of
            {ok, [Row | _]} ->
                {ok, attr_row_to_info(Row)};
            {error, _} ->
                get_attribute_info(Name);
            Error ->
                {error, Error}
        end
    end);
create_attribute(_Alias, _Name, _QualName, _Type, _IsArray) ->
    {error, invalid_attribute_definition}.

-spec get_attribute_info(name_or_id()) -> {ok, attribute_info()} | not_found | {error, ipto_reason()}.
get_attribute_info(NameOrId) when is_binary(NameOrId) ->
    with_pg(fun(Conn) ->
        Sql = "SELECT attrid, alias, attrname, qualname, attrtype, scalar FROM repo.repo_attribute WHERE attrname = $1 OR alias = $1 OR qualname = $1",
        case query_rows(epgsql:equery(Conn, Sql, [to_pg_text(NameOrId)])) of
            {ok, [Row | _]} -> {ok, attr_row_to_info(Row)};
            {ok, []} -> not_found;
            Error -> {error, Error}
        end
    end);
get_attribute_info(NameOrId) when is_integer(NameOrId), NameOrId > 0 ->
    with_pg(fun(Conn) ->
        Sql = "SELECT attrid, alias, attrname, qualname, attrtype, scalar FROM repo.repo_attribute WHERE attrid = $1",
        case query_rows(epgsql:equery(Conn, Sql, [NameOrId])) of
            {ok, [Row | _]} -> {ok, attr_row_to_info(Row)};
            {ok, []} -> not_found;
            Error -> {error, Error}
        end
    end);
get_attribute_info(_NameOrId) ->
    {error, invalid_attribute_id_or_name}.

-spec get_tenant_info(name_or_id()) -> {ok, tenant_info()} | not_found | {error, ipto_reason()}.
get_tenant_info(NameOrId) when is_integer(NameOrId), NameOrId > 0 ->
    with_pg(fun(Conn) ->
        Sql = "SELECT tenantid, name, description, created FROM repo.repo_tenant WHERE tenantid = $1",
        case query_rows(epgsql:equery(Conn, Sql, [NameOrId])) of
            {ok, [Row | _]} ->
                [Id, Name, Description, Created] = ipto_db_utils:row_values(Row),
                {ok, #{id => Id, name => Name, description => Description, created => Created}};
            {ok, []} ->
                not_found;
            Error ->
                {error, Error}
        end
    end);
get_tenant_info(NameOrId) when is_binary(NameOrId) ->
    with_pg(fun(Conn) ->
        Sql = "SELECT tenantid, name, description, created FROM repo.repo_tenant WHERE lower(name) = lower($1)",
        case query_rows(epgsql:equery(Conn, Sql, [to_pg_text(NameOrId)])) of
            {ok, [Row | _]} ->
                [Id, Name, Description, Created] = ipto_db_utils:row_values(Row),
                {ok, #{id => Id, name => Name, description => Description, created => Created}};
            {ok, []} ->
                not_found;
            Error ->
                {error, Error}
        end
    end);
get_tenant_info(_NameOrId) ->
    {error, invalid_tenant_id_or_name}.

-spec upsert_record_template(integer(), binary(), [{integer(), binary()}]) -> ok | {error, ipto_reason()}.
upsert_record_template(RecordId, RecordName, Fields)
  when is_integer(RecordId), RecordId > 0, is_binary(RecordName), is_list(Fields) ->
    with_pg(fun(Conn) ->
        pg_tx(Conn, fun() ->
            UpsertSql = "INSERT INTO repo.repo_record_template (recordid, name) VALUES ($1, $2) "
                        "ON CONFLICT (recordid) DO UPDATE SET name = EXCLUDED.name",
            case epgsql:equery(Conn, UpsertSql, [RecordId, to_pg_text(RecordName)]) of
                {ok, _} ->
                    DeleteSql = "DELETE FROM repo.repo_record_template_elements WHERE recordid = $1",
                    case epgsql:equery(Conn, DeleteSql, [RecordId]) of
                        {ok, _} ->
                            insert_record_template_elements(Conn, RecordId, Fields, 1);
                        DeleteError ->
                            {error, DeleteError}
                    end;
                UpsertError ->
                    {error, UpsertError}
            end
        end)
    end);
upsert_record_template(_RecordId, _RecordName, _Fields) ->
    {error, invalid_record_template}.

-spec upsert_unit_template(binary(), [{integer(), binary()}]) -> ok | {error, ipto_reason()}.
upsert_unit_template(TemplateName, Fields) when is_binary(TemplateName), is_list(Fields) ->
    with_pg(fun(Conn) ->
        pg_tx(Conn, fun() ->
            UpsertSql = "INSERT INTO repo.repo_unit_template (name) VALUES ($1) "
                        "ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name "
                        "RETURNING templateid",
            case query_rows(epgsql:equery(Conn, UpsertSql, [to_pg_text(TemplateName)])) of
                {ok, [Row | _]} ->
                    [TemplateId] = ipto_db_utils:row_values(Row),
                    DeleteSql = "DELETE FROM repo.repo_unit_template_elements WHERE templateid = $1",
                    case epgsql:equery(Conn, DeleteSql, [TemplateId]) of
                        {ok, _} ->
                            insert_unit_template_elements(Conn, TemplateId, Fields, 1);
                        DeleteError ->
                            {error, DeleteError}
                    end;
                {ok, []} ->
                    {error, missing_template_id};
                UpsertError ->
                    {error, UpsertError}
            end
        end)
    end);
upsert_unit_template(_TemplateName, _Fields) ->
    {error, invalid_unit_template}.

%% Internal helpers

-spec pg_get_unit_json_proc(term(), tenantid(), unitid(), integer()) -> unit_lookup_result().
pg_get_unit_json_proc(Conn, TenantId, UnitId, Version) ->
    Sql = "CALL repo.extract_unit_json($1::integer, $2::bigint, $3::integer, NULL::jsonb)",
    case query_rows(epgsql:equery(Conn, Sql, [TenantId, UnitId, Version])) of
        {ok, []} ->
            not_found;
        {ok, [Row | _]} ->
            [Json] = ipto_db_utils:row_values(Row),
            if
                Json =:= null -> not_found;
                true -> {ok, json_to_map(Json)}
            end;
        Error ->
            {error, Error}
    end.

-spec pg_get_unit_json_sql(term(), tenantid(), unitid(), integer()) -> unit_lookup_result().
pg_get_unit_json_sql(Conn, TenantId, UnitId, Version) ->
    Sql =
        "SELECT uk.tenantid, uk.unitid, uv.unitver, uk.lastver, uk.corrid, uk.status, uk.created, uv.modified, uv.unitname"
        "  FROM repo.repo_unit_kernel uk"
        "  JOIN repo.repo_unit_version uv"
        "    ON (uk.tenantid = uv.tenantid AND uk.unitid = uv.unitid"
        "        AND uv.unitver = CASE WHEN $3 = -1 THEN uk.lastver ELSE $3 END)"
        " WHERE uk.tenantid = $1 AND uk.unitid = $2",
    case query_rows(epgsql:equery(Conn, Sql, [TenantId, UnitId, Version])) of
        {ok, []} -> not_found;
        {ok, [Row | _]} -> {ok, row_to_unit_map_with_lastver(Row)};
        Error -> {error, Error}
    end.

-spec pg_store_unit_json_proc(term(), unit_map()) -> ipto_result(unit_map()).
pg_store_unit_json_proc(Conn, UnitMap) ->
    JsonText = map_to_json(UnitMap),
    case maps:get(unitid, UnitMap, undefined) of
        undefined ->
            Sql = "CALL repo.ingest_new_unit_json($1::text, NULL, NULL, NULL, NULL)",
            case query_rows(epgsql:equery(Conn, Sql, [JsonText])) of
                {ok, [Row | _]} ->
                    [UnitId, UnitVer, Created, Modified] = ipto_db_utils:row_values(Row),
                    {ok, UnitMap#{
                        unitid => UnitId,
                        unitver => UnitVer,
                        created => Created,
                        modified => Modified,
                        isreadonly => false
                    }};
                Error ->
                    {error, Error}
            end;
        _ ->
            Sql = "CALL repo.ingest_new_version_json($1::text, NULL, NULL)",
            case query_rows(epgsql:equery(Conn, Sql, [JsonText])) of
                {ok, [Row | _]} ->
                    [UnitVer, Modified] = ipto_db_utils:row_values(Row),
                    {ok, UnitMap#{
                        unitver => UnitVer,
                        modified => Modified,
                        isreadonly => false
                    }};
                Error ->
                    {error, Error}
            end
    end.

-spec pg_store_new_unit(term(), unit_map()) -> ipto_result(unit_map()).
pg_store_new_unit(Conn, UnitMap) ->
    pg_tx(Conn, fun() ->
        TenantId = maps:get(tenantid, UnitMap),
        CorrId = to_pg_text(maps:get(corrid, UnitMap, ipto_unit:make_corrid())),
        Status = maps:get(status, UnitMap, 30),
        UnitName = maybe_null_text(maps:get(unitname, UnitMap, undefined)),

        KernelSql = "INSERT INTO repo.repo_unit_kernel (tenantid, corrid, status) VALUES ($1, $2, $3) RETURNING unitid, lastver, created",
        case query_rows(epgsql:equery(Conn, KernelSql, [TenantId, CorrId, Status])) of
            {ok, [Row1 | _]} ->
                [UnitId, UnitVer, Created] = ipto_db_utils:row_values(Row1),
                VersionSql = "INSERT INTO repo.repo_unit_version (tenantid, unitid, unitver, unitname) VALUES ($1, $2, $3, $4) RETURNING modified",
                case query_rows(epgsql:equery(Conn, VersionSql, [TenantId, UnitId, UnitVer, UnitName])) of
                    {ok, [Row2 | _]} ->
                        [Modified] = ipto_db_utils:row_values(Row2),
                        {ok, UnitMap#{
                            unitid => UnitId,
                            unitver => UnitVer,
                            created => Created,
                            modified => Modified,
                            isreadonly => false
                        }};
                    Error2 ->
                        {error, Error2}
                end;
            {ok, []} ->
                {error, no_inserted_unit};
            Error1 ->
                {error, Error1}
        end
    end).

-spec pg_store_new_version(term(), unit_map()) -> ipto_result(unit_map()).
pg_store_new_version(Conn, UnitMap) ->
    pg_tx(Conn, fun() ->
        TenantId = maps:get(tenantid, UnitMap),
        UnitId = maps:get(unitid, UnitMap),
        Status = maps:get(status, UnitMap, 30),
        UnitName = maybe_null_text(maps:get(unitname, UnitMap, undefined)),

        UpdateKernelSql = "UPDATE repo.repo_unit_kernel SET status = $1, lastver = lastver + 1 WHERE tenantid = $2 AND unitid = $3 RETURNING lastver",
        case query_rows(epgsql:equery(Conn, UpdateKernelSql, [Status, TenantId, UnitId])) of
            {ok, [Row1 | _]} ->
                [UnitVer] = ipto_db_utils:row_values(Row1),
                InsertVersionSql = "INSERT INTO repo.repo_unit_version (tenantid, unitid, unitver, unitname) VALUES ($1, $2, $3, $4) RETURNING modified",
                case query_rows(epgsql:equery(Conn, InsertVersionSql, [TenantId, UnitId, UnitVer, UnitName])) of
                    {ok, [Row2 | _]} ->
                        [Modified] = ipto_db_utils:row_values(Row2),
                        {ok, UnitMap#{
                            unitver => UnitVer,
                            modified => Modified,
                            isreadonly => false
                        }};
                    Error2 ->
                        {error, Error2}
                end;
            {ok, []} ->
                {error, not_found};
            Error1 ->
                {error, Error1}
        end
    end).

%% Connection and transaction helpers

-spec with_pg(fun((term()) -> term())) -> term().
with_pg(Fun) ->
    case ipto_pg_pool:with_connection(Fun) of
        {error, pool_unavailable} ->
            with_pg_direct(Fun);
        Result ->
            Result
    end.

-spec with_pg_direct(fun((term()) -> term())) -> term().
with_pg_direct(Fun) ->
    case code:ensure_loaded(epgsql) of
        {module, epgsql} ->
            Opts = pg_conn_opts(),
            Host = maps:get(host, Opts),
            User = maps:get(user, Opts),
            Pass = maps:get(pass, Opts),
            ConnOpts = [{database, maps:get(db, Opts)}, {port, maps:get(port, Opts)}],
            case epgsql:connect(Host, User, Pass, ConnOpts) of
                {ok, Conn} ->
                    try
                        Fun(Conn)
                    catch
                        Class:Reason:Stacktrace ->
                            ipto_log:error(
                                ipto_db_pg,
                                "pg operation crashed class=~p reason=~p stacktrace=~p",
                                [Class, Reason, Stacktrace]
                            ),
                            erlang:raise(Class, Reason, Stacktrace)
                    after
                        epgsql:close(Conn)
                    end;
                Error ->
                    ipto_log:warning(
                        ipto_db_pg,
                        "failed to connect to postgres host=~s db=~s port=~p error=~p",
                        [Host, maps:get(db, Opts), maps:get(port, Opts), Error]
                    ),
                    Error
            end;
        _ ->
            ipto_log:error(ipto_db_pg, "missing dependency epgsql; compile/run using profile 'pg'", []),
            {error, {missing_dependency, epgsql}}
    end.

-spec pg_conn_opts() -> map().
pg_conn_opts() ->
    Host = env_str("IPTO_PG_HOST", "localhost"),
    User = env_str("IPTO_PG_USER", "repo"),
    Pass = env_str("IPTO_PG_PASSWORD", "repo"),
    Db = env_str("IPTO_PG_DATABASE", "repo"),
    Port = env_int("IPTO_PG_PORT", 5432),
    #{host => Host, user => User, pass => Pass, db => Db, port => Port}.

-spec env_str(string() | binary(), string()) -> string().
env_str(Name, Default) ->
    case os:getenv(Name) of
        false -> Default;
        Value -> Value
    end.

-spec env_int(string() | binary(), integer()) -> integer().
env_int(Name, Default) ->
    case os:getenv(Name) of
        false -> Default;
        Value ->
            case string:to_integer(Value) of
                {I, _} -> I;
                _ -> Default
            end
    end.

-spec pg_tx(term(), fun(() -> {ok, term()} | {error, term()})) -> {ok, term()} | {error, term()}.
pg_tx(Conn, Fun) ->
    case epgsql:squery(Conn, "BEGIN") of
        {ok, _, _} ->
            case Fun() of
                {ok, _} = Ok ->
                    _ = epgsql:squery(Conn, "COMMIT"),
                    Ok;
                Error ->
                    _ = epgsql:squery(Conn, "ROLLBACK"),
                    Error
            end;
        Error ->
            {error, Error}
    end.

%% Row mapping

-spec query_rows(term()) -> {ok, list()} | {error, term()}.
query_rows({ok, Cols, Rows}) when is_list(Cols), is_list(Rows) ->
    {ok, Rows};
query_rows({ok, _Count, Cols, Rows}) when is_list(Cols), is_list(Rows) ->
    {ok, Rows};
query_rows({error, _} = Error) ->
    Error;
query_rows(Other) ->
    {error, Other}.

-spec row_to_unit_map(list() | tuple()) -> unit_map().
row_to_unit_map(Row) ->
    [TenantId, UnitId, UnitVer, CorrId, Status, Created, Modified, UnitName] = ipto_db_utils:row_values(Row),
    #{
        tenantid => TenantId,
        unitid => UnitId,
        unitver => UnitVer,
        corrid => CorrId,
        status => Status,
        created => Created,
        modified => Modified,
        unitname => UnitName,
        isreadonly => false
    }.

-spec row_to_unit_map_with_lastver(list() | tuple()) -> unit_map().
row_to_unit_map_with_lastver(Row) ->
    [TenantId, UnitId, UnitVer, LastVer, CorrId, Status, Created, Modified, UnitName] = ipto_db_utils:row_values(Row),
    #{
        tenantid => TenantId,
        unitid => UnitId,
        unitver => UnitVer,
        corrid => CorrId,
        status => Status,
        created => Created,
        modified => Modified,
        unitname => UnitName,
        isreadonly => LastVer > UnitVer
    }.

-spec relation_from_row(list()) -> relation() | #{}.
relation_from_row([TenantId, UnitId, RelType, RelTenantId, RelUnitId]) ->
    #{
        tenantid => TenantId,
        unitid => UnitId,
        reltype => RelType,
        reltenantid => RelTenantId,
        relunitid => RelUnitId
    };
relation_from_row([TenantId, UnitId, RelType, RelTenantId, RelUnitId | _]) ->
    relation_from_row([TenantId, UnitId, RelType, RelTenantId, RelUnitId]);
relation_from_row([Inner]) when is_list(Inner); is_tuple(Inner); is_map(Inner) ->
    relation_from_row(Inner);
relation_from_row([Map]) when is_map(Map) ->
    relation_from_row(Map);
relation_from_row(Tuple) when is_tuple(Tuple) ->
    relation_from_row(tuple_to_list(Tuple));
relation_from_row(Map) when is_map(Map) ->
    case {
        map_get_any(Map, [tenantid, <<"tenantid">>, <<"tenant_id">>]),
        map_get_any(Map, [unitid, <<"unitid">>, <<"unit_id">>]),
        map_get_any(Map, [reltype, <<"reltype">>, <<"rel_type">>]),
        map_get_any(Map, [reltenantid, <<"reltenantid">>, <<"rel_tenantid">>, <<"rel_tenant_id">>]),
        map_get_any(Map, [relunitid, <<"relunitid">>, <<"rel_unitid">>, <<"rel_unit_id">>])
    } of
        {undefined, _, _, _, _} ->
            #{};
        {TenantId, UnitId, RelType, RelTenantId, RelUnitId} ->
            relation_from_row([TenantId, UnitId, RelType, RelTenantId, RelUnitId])
    end;
relation_from_row(_) ->
    #{}.

-spec map_get_any(map(), [term()]) -> term().
map_get_any(_Map, []) ->
    undefined;
map_get_any(Map, [Key | Rest]) ->
    case maps:get(Key, Map, undefined) of
        undefined -> map_get_any(Map, Rest);
        Value -> Value
    end.

-spec association_from_row(list()) -> association() | #{}.
association_from_row([TenantId, UnitId, AssocType, AssocString]) ->
    #{
        tenantid => TenantId,
        unitid => UnitId,
        assoctype => AssocType,
        assocstring => ipto_db_utils:normalize_string(AssocString)
    };
association_from_row([TenantId, UnitId, AssocType, AssocString | _]) ->
    association_from_row([TenantId, UnitId, AssocType, AssocString]);
association_from_row([Inner]) when is_list(Inner); is_tuple(Inner); is_map(Inner) ->
    association_from_row(Inner);
association_from_row(Tuple) when is_tuple(Tuple) ->
    association_from_row(tuple_to_list(Tuple));
association_from_row(Map) when is_map(Map) ->
    case {
        map_get_any(Map, [tenantid, <<"tenantid">>, <<"tenant_id">>]),
        map_get_any(Map, [unitid, <<"unitid">>, <<"unit_id">>]),
        map_get_any(Map, [assoctype, <<"assoctype">>, <<"assoc_type">>]),
        map_get_any(Map, [assocstring, <<"assocstring">>, <<"assoc_string">>, refstring, <<"refstring">>, <<"ref_string">>])
    } of
        {undefined, _, _, _} ->
            #{};
        {TenantId, UnitId, AssocType, AssocString} ->
            association_from_row([TenantId, UnitId, AssocType, AssocString])
    end;
association_from_row(_) ->
    #{}.

-spec attr_row_to_info(list() | tuple()) -> attribute_info().
attr_row_to_info(Row) ->
    [Id, Alias, Name, QualName, Type, Scalar] = ipto_db_utils:row_values(Row),
    #{
        id => Id,
        alias => Alias,
        name => Name,
        qualname => QualName,
        type => Type,
        forced_scalar => Scalar
    }.

%% JSON helpers

-spec map_to_json(map()) -> binary().
map_to_json(Map) ->
    unicode:characters_to_binary(json:encode(normalize_json(Map))).

-spec normalize_json(term()) -> term().
normalize_json(Value) when is_map(Value) ->
    maps:from_list([{json_key(K), normalize_json(V)} || {K, V} <- maps:to_list(Value)]);
normalize_json(Value) when is_list(Value) ->
    [normalize_json(V) || V <- Value];
normalize_json(Value) ->
    Value.

-spec json_key(term()) -> term().
json_key(Key) when is_atom(Key) ->
    atom_to_binary(Key, utf8);
json_key(Key) when is_list(Key) ->
    unicode:characters_to_binary(Key);
json_key(Key) ->
    Key.

-spec json_to_map(term()) -> term().
json_to_map(JsonBin) when is_binary(JsonBin) ->
    to_atom_keys(json:decode(JsonBin));
json_to_map(JsonText) when is_list(JsonText) ->
    json_to_map(unicode:characters_to_binary(JsonText));
json_to_map(Other) ->
    Other.

-spec to_atom_keys(term()) -> term().
to_atom_keys(Value) when is_map(Value) ->
    maps:from_list([{to_atom_key(K), to_atom_keys(V)} || {K, V} <- maps:to_list(Value)]);
to_atom_keys(Value) when is_list(Value) ->
    [to_atom_keys(V) || V <- Value];
to_atom_keys(Value) ->
    Value.

-spec to_atom_key(term()) -> term().
to_atom_key(Key) when is_binary(Key) ->
    binary_to_atom(Key, utf8);
to_atom_key(Key) ->
    Key.

%% Type conversion

-spec to_pg_text(term()) -> string().
to_pg_text(Value) when is_binary(Value) -> unicode:characters_to_list(Value);
to_pg_text(Value) when is_list(Value) -> Value;
to_pg_text(Value) when is_atom(Value) -> atom_to_list(Value);
to_pg_text(Value) -> unicode:characters_to_list(io_lib:format("~p", [Value])).

-spec maybe_null_text(term()) -> null | string().
maybe_null_text(undefined) -> null;
maybe_null_text(null) -> null;
maybe_null_text(Value) -> to_pg_text(Value).

%% Template element inserters

-spec insert_record_template_elements(term(), integer(), [{integer(), binary()}], integer()) ->
    {ok, ok} | {error, term()}.
insert_record_template_elements(_Conn, _RecordId, [], _Idx) ->
    {ok, ok};
insert_record_template_elements(Conn, RecordId, [{AttrId, Alias} | Rest], Idx) ->
    Sql = "INSERT INTO repo.repo_record_template_elements (recordid, attrid, idx, alias) "
          "VALUES ($1, $2, $3, $4)",
    case epgsql:equery(Conn, Sql, [RecordId, AttrId, Idx, to_pg_text(Alias)]) of
        {ok, _} ->
            insert_record_template_elements(Conn, RecordId, Rest, Idx + 1);
        Error ->
            {error, Error}
    end.

-spec insert_unit_template_elements(term(), integer(), [{integer(), binary()}], integer()) ->
    {ok, ok} | {error, term()}.
insert_unit_template_elements(_Conn, _TemplateId, [], _Idx) ->
    {ok, ok};
insert_unit_template_elements(Conn, TemplateId, [{AttrId, Alias} | Rest], Idx) ->
    Sql = "INSERT INTO repo.repo_unit_template_elements (templateid, attrid, idx, alias) "
          "VALUES ($1, $2, $3, $4)",
    case epgsql:equery(Conn, Sql, [TemplateId, AttrId, Idx, to_pg_text(Alias)]) of
        {ok, _} ->
            insert_unit_template_elements(Conn, TemplateId, Rest, Idx + 1);
        Error ->
            {error, Error}
    end.


