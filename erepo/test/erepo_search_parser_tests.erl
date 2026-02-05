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
-module(erepo_search_parser_tests).

-include_lib("eunit/include/eunit.hrl").

parse_basic_query_test() ->
    {ok, Expr} = erepo_search_parser:parse("tenantid=1 and status=30 and name~\"%foo%\""),
    1 = maps:get(tenantid, Expr),
    30 = maps:get(status, Expr),
    <<"%foo%">> = maps:get(name_ilike, Expr).

parse_created_query_test() ->
    {ok, Expr} = erepo_search_parser:parse("created>=\"2026-01-01 00:00:00\" and created<\"2027-01-01 00:00:00\""),
    <<"2026-01-01 00:00:00">> = maps:get(created_after, Expr),
    <<"2027-01-01 00:00:00">> = maps:get(created_before, Expr).

parse_invalid_query_test() ->
    {error, _} = erepo_search_parser:parse("unsupported=1 and tenantid=abc").
