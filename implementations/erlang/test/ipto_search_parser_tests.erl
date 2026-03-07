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
-module(ipto_search_parser_tests).

-include_lib("eunit/include/eunit.hrl").

parse_basic_query_test() ->
    {ok, Expr} = ipto_search_parser:parse("tenantid=1 and status=30 and name~\"%foo%\""),
    1 = maps:get(tenantid, Expr),
    30 = maps:get(status, Expr),
    <<"%foo%">> = maps:get(name_ilike, Expr).

parse_created_query_test() ->
    {ok, Expr} = ipto_search_parser:parse("created>=\"2026-01-01 00:00:00\" and created<\"2027-01-01 00:00:00\""),
    <<"2026-01-01 00:00:00">> = maps:get(created_after, Expr),
    <<"2027-01-01 00:00:00">> = maps:get(created_before, Expr).

parse_invalid_query_test() ->
    {error, _} = ipto_search_parser:parse("unsupported=1 and tenantid=abc").

parse_alias_and_status_literal_test() ->
    {ok, Expr} = ipto_search_parser:parse("tenant_id=7 and unit_name=\"foo\" and status=EFFECTIVE"),
    7 = maps:get(tenantid, Expr),
    <<"foo">> = maps:get(name, Expr),
    30 = maps:get(status, Expr).

parse_extended_operators_test() ->
    {ok, Expr} = ipto_search_parser:parse(
        "unitid>=10 and unitid<20 and corr_id~\"abc%\" and name!=\"x\" and modified<=\"2027-01-01 00:00:00\""
    ),
    10 = maps:get(unitid_gte, Expr),
    20 = maps:get(unitid_lt, Expr),
    <<"abc%">> = maps:get(corrid_ilike, Expr),
    <<"x">> = maps:get(name_ne, Expr),
    <<"2027-01-01 00:00:00">> = maps:get(modified_lte, Expr).

parse_boolean_grouping_test() ->
    {ok, Expr} = ipto_search_parser:parse("tenantid=1 and (status=30 or status=40) and not name=\"z\""),
    true = maps:is_key('$and', Expr),
    AndExprs = maps:get('$and', Expr),
    3 = length(AndExprs).

parse_boolean_precedence_test() ->
    {ok, Expr} = ipto_search_parser:parse("status=30 or status=40 and tenantid=1"),
    true = maps:is_key('$or', Expr),
    [Left, Right] = maps:get('$or', Expr),
    30 = maps:get(status, Left),
    40 = maps:get(status, Right),
    1 = maps:get(tenantid, Right).

parse_in_not_in_test() ->
    {ok, Expr} = ipto_search_parser:parse("status in (30, 40) and tenantid not in (2,3)"),
    true = maps:is_key('$and', Expr),
    [InExpr, NotInExpr] = maps:get('$and', Expr),
    true = maps:is_key('$or', InExpr),
    true = maps:is_key('$not', NotInExpr).

parse_between_test() ->
    {ok, Expr} = ipto_search_parser:parse("unitid between 10 and 20"),
    true = maps:is_key('$and', Expr),
    [Lower, Upper] = maps:get('$and', Expr),
    10 = maps:get(unitid_gte, Lower),
    20 = maps:get(unitid_lte, Upper).
