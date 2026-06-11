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
-module(ipto_search_pipeline_tests).

-include_lib("eunit/include/eunit.hrl").

parse_unit_eq_test() ->
    {ok, Expr} = ipto_search_parser:parse_ast("tenantid = 42"),
    ?assert(ipto_search_ast:is_leaf(Expr)),
    Leaves = ipto_search_ast:collect_leaves(Expr),
    ?assertEqual(1, length(Leaves)),
    [Item] = Leaves,
    ?assert(ipto_search_ast:is_unit_item(Item)),
    ?assertEqual(tenantid, ipto_search_ast:item_unit_column(Item)),
    ?assertEqual(eq, ipto_search_ast:item_operator(Item)),
    ?assertEqual(42, ipto_search_ast:item_value(Item)).

parse_unit_with_operator_test() ->
    {ok, Expr} = ipto_search_parser:parse_ast("unitid >= 10 and unitid < 100"),
    Leaves = ipto_search_ast:collect_leaves(Expr),
    ?assertEqual(2, length(Leaves)),
    Values = [{ipto_search_ast:item_unit_column(I), ipto_search_ast:item_operator(I), ipto_search_ast:item_value(I)} || I <- Leaves],
    ?assert(lists:member({unitid, gte, 10}, Values)),
    ?assert(lists:member({unitid, lt, 100}, Values)).

parse_string_unit_test() ->
    {ok, Expr} = ipto_search_parser:parse_ast("name = \"hello\""),
    [Item] = ipto_search_ast:collect_leaves(Expr),
    ?assert(ipto_search_ast:is_unit_item(Item)),
    ?assertEqual(unitname, ipto_search_ast:item_unit_column(Item)),
    ?assertEqual(<<"hello">>, ipto_search_ast:item_value(Item)).

parse_string_like_test() ->
    {ok, Expr} = ipto_search_parser:parse_ast("name ~ \"%world%\""),
    [Item] = ipto_search_ast:collect_leaves(Expr),
    ?assertEqual(like, ipto_search_ast:item_operator(Item)).

parse_status_literal_test() ->
    {ok, Expr} = ipto_search_parser:parse_ast("status = effective"),
    [Item] = ipto_search_ast:collect_leaves(Expr),
    ?assertEqual(30, ipto_search_ast:item_value(Item)).

parse_attr_colon_test() ->
    {ok, Expr} = ipto_search_parser:parse_ast("demo:color = \"red\""),
    [Item] = ipto_search_ast:collect_leaves(Expr),
    ?assert(ipto_search_ast:is_attr_item(Item)),
    ?assertEqual(<<"color">>, ipto_search_ast:item_attribute_name(Item)),
    ?assertEqual(eq, ipto_search_ast:item_operator(Item)),
    ?assertEqual(<<"red">>, ipto_search_ast:item_value(Item)).

parse_attr_wildcard_test() ->
    {ok, Expr} = ipto_search_parser:parse_ast("demo:tag ~ \"*urgent*\""),
    [Item] = ipto_search_ast:collect_leaves(Expr),
    ?assert(ipto_search_ast:is_attr_item(Item)),
    ?assertEqual(like, ipto_search_ast:item_operator(Item)).

parse_rel_spec_test() ->
    {ok, Expr} = ipto_search_parser:parse_ast("relation:left:5 = 1.42"),
    [Item] = ipto_search_ast:collect_leaves(Expr),
    ?assert(ipto_search_ast:is_rel_item(Item)),
    ?assertEqual(left, ipto_search_ast:item_rel_direction(Item)),
    ?assertEqual(5, ipto_search_ast:item_rel_type(Item)),
    ?assertEqual({1, 42}, ipto_search_ast:item_rel_ref(Item)).

parse_assoc_spec_test() ->
    {ok, Expr} = ipto_search_parser:parse_ast("association:right:3 = \"ref-123\""),
    [Item] = ipto_search_ast:collect_leaves(Expr),
    ?assert(ipto_search_ast:is_assoc_item(Item)),
    ?assertEqual(right, ipto_search_ast:item_assoc_direction(Item)),
    ?assertEqual(3, ipto_search_ast:item_assoc_type(Item)),
    ?assertEqual(<<"ref-123">>, ipto_search_ast:item_assoc_string(Item)).

parse_complex_boolean_test() ->
    {ok, Expr} = ipto_search_parser:parse_ast(
        "tenantid = 1 and (status = 30 or status = 10) and not name = \"skip\""
    ),
    Leaves = ipto_search_ast:collect_leaves(Expr),
    ?assertEqual(4, length(Leaves)).

parse_between_test() ->
    {ok, Expr} = ipto_search_parser:parse_ast("unitid between 100 and 200"),
    Leaves = ipto_search_ast:collect_leaves(Expr),
    ?assertEqual(2, length(Leaves)),
    Values = [{ipto_search_ast:item_operator(I), ipto_search_ast:item_value(I)} || I <- Leaves],
    ?assert(lists:member({gte, 100}, Values)),
    ?assert(lists:member({lte, 200}, Values)).

parse_in_clause_test() ->
    {ok, Expr} = ipto_search_parser:parse_ast("status in (10, 20, 30)"),
    Leaves = ipto_search_ast:collect_leaves(Expr),
    ?assertEqual(3, length(Leaves)).

parse_mixed_attr_unit_test() ->
    {ok, Expr} = ipto_search_parser:parse_ast("tenantid = 1 and demo:color = \"red\""),
    Leaves = ipto_search_ast:collect_leaves(Expr),
    UnitLeaves = [L || L <- Leaves, ipto_search_ast:is_unit_item(L)],
    AttrLeaves = [L || L <- Leaves, ipto_search_ast:is_attr_item(L)],
    ?assertEqual(1, length(UnitLeaves)),
    ?assertEqual(1, length(AttrLeaves)).

sql_unit_only_uses_exists_test() ->
    {ok, Expr} = ipto_search_parser:parse_ast("tenantid = 1 and status = 30"),
    Compiled = ipto_search_sql:compile(Expr, {created, desc}, #{limit => 10}),
    Sql = maps:get(sql, Compiled, ""),
    ?assert(string:str(Sql, "tenantid = $") > 0),
    ?assert(string:str(Sql, "status = $") > 0),
    Params = maps:get(params, Compiled, []),
    ?assert(length(Params) >= 2).

sql_mixed_triggers_set_ops_strategy_test() ->
    {ok, Expr} = ipto_search_parser:parse_ast("tenantid = 1 and demo:color = \"red\""),
    Leaves = ipto_search_ast:collect_leaves(Expr),
    HasAttr = lists:any(fun ipto_search_ast:is_attr_item/1, Leaves),
    ?assert(HasAttr).

sql_rel_search_produces_exists_test() ->
    {ok, Expr} = ipto_search_parser:parse_ast("relation:left:5 = 1.42"),
    Compiled = ipto_search_sql:compile(Expr, {created, desc}, 10),
    Sql = maps:get(sql, Compiled, ""),
    ?assert(string:str(Sql, "EXISTS") > 0),
    ?assert(string:str(Sql, "repo_internal_relation") > 0).

sql_assoc_search_produces_exists_test() ->
    {ok, Expr} = ipto_search_parser:parse_ast("association:right:3 = \"ref-123\""),
    Compiled = ipto_search_sql:compile(Expr, {created, desc}, 10),
    Sql = maps:get(sql, Compiled, ""),
    ?assert(string:str(Sql, "EXISTS") > 0),
    ?assert(string:str(Sql, "repo_external_assoc") > 0).

sql_paging_test() ->
    {ok, Expr} = ipto_search_parser:parse_ast("status = 30"),
    Compiled = ipto_search_sql:compile(Expr, {created, desc}, #{limit => 5, offset => 10}),
    Sql = maps:get(sql, Compiled, ""),
    ?assert(string:str(Sql, "LIMIT") > 0),
    ?assert(string:str(Sql, "OFFSET") > 0).

parse_invalid_field_returns_error_test() ->
    ?assertMatch({error, _}, ipto_search_parser:parse_ast("bogus = 42")).

parse_invalid_operator_for_rel_test() ->
    {error, _} = ipto_search_parser:parse_ast("relation:left:5 > 1.42").

parse_symbolic_rel_type_test() ->
    {ok, Expr} = ipto_search_parser:parse_ast("relation:left:parent-child = 1.42"),
    [Item] = ipto_search_ast:collect_leaves(Expr),
    ?assertEqual(1, ipto_search_ast:item_rel_type(Item)).

parse_symbolic_assoc_type_test() ->
    {ok, Expr} = ipto_search_parser:parse_ast("association:right:case = \"ref-123\""),
    [Item] = ipto_search_ast:collect_leaves(Expr),
    ?assertEqual(2, ipto_search_ast:item_assoc_type(Item)).

sql_set_ops_produces_sequential_params_test() ->
    {ok, Expr} = ipto_search_parser:parse_ast("tenantid = 1 and demo:color = \"red\""),
    Compiled = ipto_search_sql:compile(Expr, {created, desc}, #{limit => 5}),
    Sql = maps:get(sql, Compiled, ""),
    Params = maps:get(params, Compiled, []),
    CountParams = maps:get(count_params, Compiled, []),
    ?assert(string:str(Sql, "$1") > 0),
    ?assertEqual(1, hd(Params)),
    ?assert(length(Params) >= 3),
    ?assert(length(CountParams) >= 3).

sql_multi_unit_where_sequential_test() ->
    {ok, Expr} = ipto_search_parser:parse_ast("tenantid = 1 and status = 30 and unitid > 0"),
    Compiled = ipto_search_sql:compile(Expr, {created, desc}, 10),
    Sql = maps:get(sql, Compiled, ""),
    Params = maps:get(params, Compiled, []),
    ?assert(string:str(Sql, "$1") > 0),
    ?assert(string:str(Sql, "$2") > 0),
    ?assert(string:str(Sql, "$3") > 0),
    ?assert(length(Params) >= 4),
    ParamsWithoutPaging = lists:sublist(Params, 1, length(Params) - 1),
    ?assertEqual([0, 30, 1], ParamsWithoutPaging).

sql_rel_item_produces_sequential_params_test() ->
    {ok, Expr} = ipto_search_parser:parse_ast("tenantid = 1 and relation:left:5 = 1.42"),
    Compiled = ipto_search_sql:compile(Expr, {created, desc}, 10),
    Sql = maps:get(sql, Compiled, ""),
    ?assert(string:str(Sql, "$1") > 0),
    ?assert(string:str(Sql, "$2") > 0),
    ?assert(string:str(Sql, "$3") > 0),
    ?assert(string:str(Sql, "$4") > 0).

sql_count_and_data_sql_have_correct_params_test() ->
    {ok, Expr} = ipto_search_parser:parse_ast("status = 30"),
    Compiled = ipto_search_sql:compile(Expr, {created, desc}, 10),
    CountSql = maps:get(count_sql, Compiled, ""),
    DataSql = maps:get(sql, Compiled, ""),
    CountParams = maps:get(count_params, Compiled, []),
    DataParams = maps:get(params, Compiled, []),
    ?assertNotEqual(CountSql, DataSql),
    ?assert(string:str(CountSql, "count(*)") > 0),
    ?assert(string:str(DataSql, "uk.tenantid") > 0),
    ?assertEqual([30], CountParams),
    ?assertEqual([30, 10], DataParams).
