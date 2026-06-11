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
-module(ipto_search_sql).

-include("ipto.hrl").

-export([compile/3]).

-type compiled_query() :: #{
    sql := string(),
    params := list(),
    count_sql := string(),
    count_params := list()
}.
-export_type([compiled_query/0]).

-define(TABLE_STRING, "repo_string_vector").
-define(TABLE_INTEGER, "repo_integer_vector").
-define(TABLE_LONG, "repo_integer_vector").
-define(TABLE_DOUBLE, "repo_float_vector").
-define(TABLE_BOOLEAN, "repo_boolean_vector").
-define(TABLE_TIME, "repo_time_vector").
-define(TABLE_RECORD, "repo_record_vector").

-spec compile(ipto_search_ast:search_expr(), search_order(), search_paging()) -> compiled_query().
compile(Expr, Order, Paging) ->
    case classify_search(Expr) of
        unit_only ->
            compile_exists(Expr, Order, Paging);
        mixed ->
            compile_set_ops(Expr, Order, Paging)
    end.

%% SET_OPS strategy — uses WITH/CTE + INTERSECT/UNION for attribute-constrained searches

-spec compile_set_ops(ipto_search_ast:search_expr(), search_order(), search_paging()) -> compiled_query().
compile_set_ops(Expr, Order, Paging) ->
    {UnitLeaves, AttrLeaves} = split_leaves(Expr),
    UnitWhere = build_unit_where(UnitLeaves, 1),
    NextIdx1 = proplists:get_value(next_idx, UnitWhere, 1),
    UnitParams = proplists:get_value(params, UnitWhere, []),

    {Ctes, AttrCteNames} = build_ctes(AttrLeaves, NextIdx1),
    NextIdx2 = proplists:get_value(next_idx, Ctes, NextIdx1),
    CteParams = proplists:get_value(params, Ctes, []),

    AttrLogic = build_set_ops_logic(Expr, AttrCteNames),
    OrderSql = build_order_sql(Order),
    {PageSql, PageParams, _NextIdx3} = build_paging_sql(Paging, NextIdx2),

    BaseSql =
        "WITH " ++ proplists:get_value(sql, Ctes, "") ++
        "final AS (" ++ AttrLogic ++ ") "
        "SELECT uk.tenantid, uk.unitid, uv.unitver, uk.corrid, uk.status, uk.created, uv.modified, uv.unitname "
        "FROM repo.repo_unit_kernel uk "
        "JOIN repo.repo_unit_version uv ON (uk.tenantid = uv.tenantid AND uk.unitid = uv.unitid AND uk.lastver = uv.unitver) "
        "JOIN final f ON (uk.tenantid = f.tenantid AND uk.unitid = f.unitid) ",

    WhereSql = proplists:get_value(where_sql, UnitWhere, ""),
    FullSql = BaseSql ++ WhereSql ++ OrderSql ++ PageSql,
    Params = UnitParams ++ CteParams ++ PageParams,

    CountSql =
        "WITH " ++ proplists:get_value(sql, Ctes, "") ++
        "final AS (" ++ AttrLogic ++ ") "
        "SELECT count(*) "
        "FROM repo.repo_unit_kernel uk "
        "JOIN repo.repo_unit_version uv ON (uk.tenantid = uv.tenantid AND uk.unitid = uv.unitid AND uk.lastver = uv.unitver) "
        "JOIN final f ON (uk.tenantid = f.tenantid AND uk.unitid = f.unitid) " ++
        WhereSql,
    CountParams = UnitParams ++ CteParams,

    #{sql => FullSql, params => Params, count_sql => CountSql, count_params => CountParams}.

%% EXISTS strategy — for unit-only searches (no attribute constraints)

-spec compile_exists(ipto_search_ast:search_expr(), search_order(), search_paging()) -> compiled_query().
compile_exists(Expr, Order, Paging) ->
    {UnitLeaves, _AttrLeaves} = split_leaves(Expr),
    UnitWhere = build_unit_where(UnitLeaves, 1),
    NextIdx = proplists:get_value(next_idx, UnitWhere, 1),
    UnitParams = proplists:get_value(params, UnitWhere, []),

    OrderSql = build_order_sql(Order),
    {PageSql, PageParams, _NextIdx2} = build_paging_sql(Paging, NextIdx),

    BaseFrom = " FROM repo.repo_unit_kernel uk"
               " JOIN repo.repo_unit_version uv"
               "   ON (uk.tenantid = uv.tenantid AND uk.unitid = uv.unitid AND uk.lastver = uv.unitver)",

    WhereSql = proplists:get_value(where_sql, UnitWhere, ""),
    SelectSql = "SELECT uk.tenantid, uk.unitid, uv.unitver, uk.corrid, uk.status, uk.created, uv.modified, uv.unitname" ++
                BaseFrom ++ WhereSql ++ OrderSql ++ PageSql,
    Params = UnitParams ++ PageParams,

    CountSql = "SELECT count(*)" ++ BaseFrom ++ WhereSql,
    CountParams = UnitParams,

    #{sql => SelectSql, params => Params, count_sql => CountSql, count_params => CountParams}.

%% Leaf classification

-spec split_leaves(ipto_search_ast:search_expr()) -> {[ipto_search_ast:search_item()], [ipto_search_ast:search_item()]}.
split_leaves(Expr) ->
    Leaves = ipto_search_ast:collect_leaves(Expr),
    UnitLike = [L || L <- Leaves, is_unit_like_item(L)],
    AttrOnly = [L || L <- Leaves, ipto_search_ast:is_attr_item(L)],
    {UnitLike, AttrOnly}.

-spec classify_search(ipto_search_ast:search_expr()) -> unit_only | mixed.
classify_search(Expr) ->
    Leaves = ipto_search_ast:collect_leaves(Expr),
    case lists:all(fun is_unit_like_item/1, Leaves) of
        true -> unit_only;
        false -> mixed
    end.

-spec is_unit_like_item(ipto_search_ast:search_item()) -> boolean().
is_unit_like_item(Item) ->
    ipto_search_ast:is_unit_item(Item) orelse
    ipto_search_ast:is_rel_item(Item) orelse
    ipto_search_ast:is_assoc_item(Item).

%% Unit WHERE clause compilation

-spec build_unit_where([ipto_search_ast:search_item()], pos_integer()) -> proplists:proplist().
build_unit_where([], NextIdx) ->
    [{where_sql, ""}, {params, []}, {next_idx, NextIdx}];
build_unit_where(Leaves, StartIdx) ->
    build_unit_where_loop(Leaves, StartIdx, [], []).

-spec build_unit_where_loop([ipto_search_ast:search_item()], pos_integer(), [string()], [[term()]]) -> proplists:proplist().
build_unit_where_loop([], NextIdx, SqlFragsRev, ParamsRev) ->
    SqlFrags = lists:reverse(SqlFragsRev),
    Params = lists:reverse(ParamsRev),
    WhereSql = case SqlFrags of
        [] -> "";
        _ -> " WHERE " ++ string:join(SqlFrags, " AND ")
    end,
    FlatParams = lists:flatten(Params),
    [{where_sql, WhereSql}, {params, FlatParams}, {next_idx, NextIdx}];
build_unit_where_loop([Item | Rest], Idx, SqlFrags, Params) ->
    {Sql, ItemParams} = unit_item_to_sql(Item, Idx),
    build_unit_where_loop(Rest, Idx + length(ItemParams), [Sql | SqlFrags], [ItemParams | Params]).

-spec unit_item_to_sql(ipto_search_ast:search_item(), pos_integer()) -> {string(), [term()]}.
unit_item_to_sql({unit, tenantid, Op, Value}, Idx) ->
    {op_sql("uk.tenantid", Op, "$" ++ integer_to_list(Idx)), [Value]};
unit_item_to_sql({unit, unitid, Op, Value}, Idx) ->
    {op_sql("uk.unitid", Op, "$" ++ integer_to_list(Idx)), [Value]};
unit_item_to_sql({unit, status, Op, Value}, Idx) ->
    {op_sql("uk.status", Op, "$" ++ integer_to_list(Idx)), [Value]};
unit_item_to_sql({unit, corrid, eq, Value}, Idx) ->
    {"uk.corrid = $" ++ integer_to_list(Idx), [ipto_db_utils:normalize_string(Value)]};
unit_item_to_sql({unit, corrid, ne, Value}, Idx) ->
    {"uk.corrid <> $" ++ integer_to_list(Idx), [ipto_db_utils:normalize_string(Value)]};
unit_item_to_sql({unit, corrid, like, Value}, Idx) ->
    {"uk.corrid ILIKE $" ++ integer_to_list(Idx), [ipto_db_utils:normalize_string(Value)]};
unit_item_to_sql({unit, created, Op, Value}, Idx) ->
    {op_sql("uk.created", Op, "$" ++ integer_to_list(Idx)), [Value]};
unit_item_to_sql({unit, modified, Op, Value}, Idx) ->
    {op_sql("uv.modified", Op, "$" ++ integer_to_list(Idx)), [Value]};
unit_item_to_sql({unit, unitname, eq, Value}, Idx) ->
    {"uv.unitname = $" ++ integer_to_list(Idx), [ipto_db_utils:normalize_string(Value)]};
unit_item_to_sql({unit, unitname, ne, Value}, Idx) ->
    {"uv.unitname <> $" ++ integer_to_list(Idx), [ipto_db_utils:normalize_string(Value)]};
unit_item_to_sql({unit, unitname, like, Value}, Idx) ->
    {"uv.unitname ILIKE $" ++ integer_to_list(Idx), [ipto_db_utils:normalize_string(Value)]};
unit_item_to_sql({rel, Direction, RelType, {TenantId, UnitId}}, Idx) ->
    case Direction of
        left ->
            {"EXISTS (SELECT 1 FROM repo.repo_internal_relation r "
             "WHERE uk.tenantid = r.tenantid AND uk.unitid = r.unitid "
             "AND r.reltype = $" ++ integer_to_list(Idx) ++ " "
             "AND r.reltenantid = $" ++ integer_to_list(Idx + 1) ++ " "
             "AND r.relunitid = $" ++ integer_to_list(Idx + 2) ++ ")",
             [RelType, TenantId, UnitId]};
        right ->
            {"EXISTS (SELECT 1 FROM repo.repo_internal_relation r "
             "WHERE uk.tenantid = r.reltenantid AND uk.unitid = r.relunitid "
             "AND r.reltype = $" ++ integer_to_list(Idx) ++ " "
             "AND r.tenantid = $" ++ integer_to_list(Idx + 1) ++ " "
             "AND r.unitid = $" ++ integer_to_list(Idx + 2) ++ ")",
             [RelType, TenantId, UnitId]}
    end;
unit_item_to_sql({assoc, Direction, AssocType, RefString}, Idx) ->
    case Direction of
        left ->
            {"EXISTS (SELECT 1 FROM repo.repo_external_assoc a "
             "WHERE uk.tenantid = a.tenantid AND uk.unitid = a.unitid "
             "AND a.assoctype = $" ++ integer_to_list(Idx) ++ " "
             "AND a.assocstring = $" ++ integer_to_list(Idx + 1) ++ ")",
             [AssocType, RefString]};
        right ->
            {"EXISTS (SELECT 1 FROM repo.repo_external_assoc a "
             "WHERE a.assoctype = $" ++ integer_to_list(Idx) ++ " "
             "AND a.assocstring = $" ++ integer_to_list(Idx + 1) ++ " "
             "AND uk.tenantid = a.tenantid AND uk.unitid = a.unitid)",
             [AssocType, RefString]}
    end.

-spec op_sql(string(), ipto_search_ast:operator(), string()) -> string().
op_sql(Col, eq, Place) -> Col ++ " = " ++ Place;
op_sql(Col, ne, Place) -> Col ++ " <> " ++ Place;
op_sql(Col, gt, Place) -> Col ++ " > " ++ Place;
op_sql(Col, gte, Place) -> Col ++ " >= " ++ Place;
op_sql(Col, lt, Place) -> Col ++ " < " ++ Place;
op_sql(Col, lte, Place) -> Col ++ " <= " ++ Place;
op_sql(Col, like, Place) -> Col ++ " ILIKE " ++ Place.

%% CTE generation for attribute constraints

-spec build_ctes([ipto_search_ast:search_item()], pos_integer()) -> {proplists:proplist(), [{string(), ipto_search_ast:search_item()}]}.
build_ctes(AttrLeaves, NextIdx) ->
    {CteDefs, CteParams, FinalIdx, CteNames} =
        lists:foldl(
            fun(Leaf, {Defs, ParamsAcc, Idx, Names}) ->
                Name = "c" ++ integer_to_list(length(Names) + 1),
                case attr_item_to_cte(Leaf, Name, Idx) of
                    {ok, CteSql, CteP, NewIdx} ->
                        {Defs ++ [CteSql], ParamsAcc ++ CteP, NewIdx,
                         Names ++ [{Name, Leaf}]};
                    {error, _} ->
                        {Defs, ParamsAcc, Idx, Names}
                end
            end,
            {[], [], NextIdx, []},
            AttrLeaves
        ),
    CteSql = string:join(CteDefs, ", "),
    Result = [{sql, CteSql}, {params, CteParams}, {next_idx, FinalIdx}],
    {Result, CteNames}.

-spec attr_item_to_cte(ipto_search_ast:search_item(), string(), pos_integer()) -> {ok, string(), [term()], pos_integer()} | {error, term()}.
attr_item_to_cte({attr, _Name, AttrId, Type, Op, Value}, CteName, Idx) ->
    VectorTable = type_to_vector_table(Type),
    ValueColumn = type_to_value_column(Type),
    Sql = CteName ++ " AS ("
          "SELECT av.tenantid, av.unitid "
          "FROM repo.repo_attribute_value av "
          "JOIN " ++ VectorTable ++ " vv ON av.valueid = vv.valueid "
          "WHERE av.attrid = $" ++ integer_to_list(Idx) ++ " "
          "AND " ++ ValueColumn ++ " " ++ op_sql("", Op, "$" ++ integer_to_list(Idx + 1)) ++ ")",
    {ok, Sql, [AttrId, Value], Idx + 2};
attr_item_to_cte(_, _CteName, Idx) ->
    {error, {invalid_cte, Idx}}.

-spec type_to_vector_table(ipto_search_ast:attribute_type()) -> string().
type_to_vector_table(string) -> ?TABLE_STRING;
type_to_vector_table(integer) -> ?TABLE_INTEGER;
type_to_vector_table(long) -> ?TABLE_LONG;
type_to_vector_table(double) -> ?TABLE_DOUBLE;
type_to_vector_table(boolean) -> ?TABLE_BOOLEAN;
type_to_vector_table(time) -> ?TABLE_TIME;
type_to_vector_table(record) -> ?TABLE_RECORD;
type_to_vector_table(_) -> ?TABLE_STRING.

-spec type_to_value_column(ipto_search_ast:attribute_type()) -> string().
type_to_value_column(string) -> "vv.vecthvalue";
type_to_value_column(integer) -> "vv.vectivalue";
type_to_value_column(long) -> "vv.vectlvalue";
type_to_value_column(double) -> "vv.vectdvalue";
type_to_value_column(boolean) -> "vv.vectbvalue";
type_to_value_column(time) -> "vv.vecttvalue";
type_to_value_column(record) -> "vv.vectrvalue";
type_to_value_column(_) -> "vv.vecthvalue".

%% Boolean tree → SET_OPS logic

-spec build_set_ops_logic(ipto_search_ast:search_expr(), [{string(), ipto_search_ast:search_item()}]) -> string().
build_set_ops_logic({'$and', Left, Right}, CteNames) ->
    "(" ++ build_set_ops_logic(Left, CteNames) ++ " INTERSECT " ++ build_set_ops_logic(Right, CteNames) ++ ")";
build_set_ops_logic({'$or', Left, Right}, CteNames) ->
    "(" ++ build_set_ops_logic(Left, CteNames) ++ " UNION " ++ build_set_ops_logic(Right, CteNames) ++ ")";
build_set_ops_logic({'$not', Inner}, CteNames) ->
    "(SELECT tenantid, unitid FROM repo.repo_unit_kernel "
    "EXCEPT " ++ build_set_ops_logic(Inner, CteNames) ++ ")";
build_set_ops_logic({leaf, Item}, CteNames) ->
    case ipto_search_ast:is_unit_item(Item) of
        true -> "(SELECT tenantid, unitid FROM repo.repo_unit_kernel)";
        false ->
            case lists:keyfind(Item, 2, CteNames) of
                {CteName, _} -> "(SELECT tenantid, unitid FROM " ++ CteName ++ ")";
                false -> "(SELECT tenantid, unitid FROM repo.repo_unit_kernel)"
            end
    end.

%% Order and paging

-spec build_order_sql(search_order() | map() | tuple()) -> string().
build_order_sql(#{field := Field, dir := Dir}) ->
    build_order_sql({Field, Dir});
build_order_sql({Field, Dir}) ->
    FieldSql = case Field of
        created -> "uk.created";
        modified -> "uv.modified";
        unitid -> "uk.unitid";
        status -> "uk.status";
        _ -> "uk.created"
    end,
    DirSql = case Dir of
        asc -> "ASC";
        'ASC' -> "ASC";
        _ -> "DESC"
    end,
    " ORDER BY " ++ FieldSql ++ " " ++ DirSql;
build_order_sql(_) ->
    " ORDER BY uk.created DESC".

-spec build_paging_sql(search_paging() | map() | integer(), pos_integer()) -> {string(), [term()], pos_integer()}.
build_paging_sql(#{limit := Limit, offset := Offset}, Idx)
  when is_integer(Limit), Limit > 0, is_integer(Offset), Offset >= 0 ->
    Sql = string:join([
        " LIMIT $" ++ integer_to_list(Idx),
        " OFFSET $" ++ integer_to_list(Idx + 1)
    ], " "),
    {Sql, [Limit, Offset], Idx + 2};
build_paging_sql(#{limit := Limit}, Idx) when is_integer(Limit), Limit > 0 ->
    Sql = " LIMIT $" ++ integer_to_list(Idx),
    {Sql, [Limit], Idx + 1};
build_paging_sql(Limit, Idx) when is_integer(Limit), Limit > 0 ->
    Sql = " LIMIT $" ++ integer_to_list(Idx),
    {Sql, [Limit], Idx + 1};
build_paging_sql(_, Idx) ->
    {"", [], Idx}.
