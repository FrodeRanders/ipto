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
-module(ipto_search_parser_prop_tests).

-include_lib("eunit/include/eunit.hrl").

-define(N, 250).

parse_status_literal_queries_test() ->
    Statuses = ["effective", "archived", "pending_deletion", "obliterated", "pending_disposition"],
    [
        ?_assertMatch({ok, #{status := S}} when is_integer(S), ipto_search_parser:parse(iolist_to_binary(["status = ", St])))
        || St <- Statuses
    ].

parse_boolean_and_query_test_() ->
    [
        ?_assertMatch({ok, _}, ipto_search_parser:parse("status = 30 and name = \"foo\"")),
        ?_assertMatch({ok, _}, ipto_search_parser:parse("status = 30 and name = \"foo\" and unitid = 1")),
        ?_assertMatch({ok, _}, ipto_search_parser:parse("status = 30 or status = 10")),
        ?_assertMatch({ok, _}, ipto_search_parser:parse("not status = 30"))
    ].

parse_boolean_precedence_test() ->
    ParsedOr = ipto_search_parser:parse("status = 30 or status = 10 and name = \"foo\""),
    ParsedParen = ipto_search_parser:parse("status = 30 or (status = 10 and name = \"foo\")"),
    [
        ?_assertMatch({ok, _}, ParsedOr),
        ?_assertMatch({ok, _}, ParsedParen),
        ?_assertEqual(ParsedOr, ParsedParen)
    ].

parse_in_and_between_test_() ->
    [
        ?_assertMatch({ok, _}, ipto_search_parser:parse("status in (10, 20, 30)")),
        ?_assertMatch({ok, _}, ipto_search_parser:parse("status not in (40)")),
        ?_assertMatch({ok, _}, ipto_search_parser:parse("status between 10 and 30")),
        ?_assertMatch({ok, _}, ipto_search_parser:parse("unitid between 1 and 100"))
    ].

parse_invalid_queries_produce_errors_test_() ->
    InvalidQueries = [
        "",
        "   ",
        "=",
        "= 42",
        "unknown_field = 42",
        "status in ()",
        "status not in ()",
        "status between 10",
        "between 10 and 20",
        "and status = 30",
        "(status = 30",
        "status = 30)",
        "status = 30 name = \"foo\""
    ],
    [
        ?_assertMatch({error, _}, ipto_search_parser:parse(Q))
        || Q <- InvalidQueries
    ].

parse_roundtrip_stable_test_() ->
    Queries = [
        "status = 30",
        "name = \"foo\"",
        "corrid ~ \"hello\"",
        "unitid > 5",
        "status in (10, 20)",
        "status between 10 and 30",
        "status = effective",
        "status = 30 and name = \"bar\"",
        "not status = 40"
    ],
    [
        begin
            {ok, P1} = ipto_search_parser:parse(Q),
            {ok, P2} = ipto_search_parser:parse(Q),
            ?_assertEqual(P1, P2)
        end
        || Q <- Queries
    ].

parse_field_alias_test_() ->
    [
        ?_assertMatch({ok, #{tenantid := _}}, ipto_search_parser:parse("tenant_id = 1")),
        ?_assertMatch({ok, #{unitid := _}}, ipto_search_parser:parse("unit_id = 1")),
        ?_assertMatch({ok, #{corrid := _}}, ipto_search_parser:parse("corr_id = \"abc\"")),
        ?_assertMatch({ok, #{corrid := _}}, ipto_search_parser:parse("correlationid = \"abc\"")),
        ?_assertMatch({ok, #{name := _}}, ipto_search_parser:parse("unitname = \"foo\"")),
        ?_assertMatch({ok, #{name := _}}, ipto_search_parser:parse("unit_name = \"foo\""))
    ].

parse_temporal_aliases_test_() ->
    [
        ?_assertMatch({ok, #{created_after := _}}, ipto_search_parser:parse("created_after = 100")),
        ?_assertMatch({ok, #{created_before := _}}, ipto_search_parser:parse("created_before = 200")),
        ?_assertMatch({ok, #{modified_after := _}}, ipto_search_parser:parse("modified_after = 100")),
        ?_assertMatch({ok, #{modified_before := _}}, ipto_search_parser:parse("modified_before = 200"))
    ].

parse_random_valid_queries_never_crash_test_() ->
    Queries = generate_random_valid_queries(?N),
    [
        begin
            case ipto_search_parser:parse(Q) of
                {ok, Map} -> ?_assert(is_map(Map));
                {error, _} -> ?_assert(true)
            end
        end
        || Q <- Queries
    ].

parse_random_known_valid_queries_test_() ->
    Queries = generate_known_valid_queries(?N),
    [
        begin
            {ok, Map} = ipto_search_parser:parse(Q),
            ?_assert(is_map(Map))
        end
        || Q <- Queries
    ].

parse_random_malformed_queries_error_test_() ->
    Queries = generate_known_malformed_queries(?N),
    [
        ?_assertMatch({error, _}, ipto_search_parser:parse(Q))
        || Q <- Queries
    ].

parse_nested_boolean_roundtrip_test_() ->
    Queries = [
        "status = 30 and (name = \"a\" or name = \"b\")",
        "(status = 10 or status = 20) and unitid > 0",
        "not (status = 40 or status = 20)",
        "status = 30 and not name = \"\"",
        "(status = 10 and name = \"x\") or (status = 30 and name = \"y\")"
    ],
    [
        begin
            {ok, P1} = ipto_search_parser:parse(Q),
            {ok, P2} = ipto_search_parser:parse(Q),
            ?_assertEqual(P1, P2)
        end
        || Q <- Queries
    ].

%% Generators

generate_random_valid_queries(N) ->
    [string:join([random_field(), random_op(), random_value()], " ") || _ <- lists:seq(1, N)].

generate_known_valid_queries(N) ->
    Field = fun() -> element(rand:uniform(2), {"status", "unitid"}) end,
    Op = fun() -> element(rand:uniform(3), {"=", ">=", "<"}) end,
    Val = fun() -> integer_to_list(rand:uniform(100)) end,
    [string:join([Field(), Op(), Val()], " ") || _ <- lists:seq(1, N)].

generate_known_malformed_queries(N) ->
    Malformed = [
        fun() -> "" end,
        fun() -> lists:duplicate(rand:uniform(10), $\s) end,
        fun() -> "unknown_field_" ++ integer_to_list(rand:uniform(100)) ++ " = 42" end,
        fun() -> integer_to_list(rand:uniform(100)) end,
        fun() -> "and status = 30" end
    ],
    [(lists:nth(rand:uniform(length(Malformed)), Malformed))() || _ <- lists:seq(1, N)].

random_field() ->
    Fields = ["tenantid", "unitid", "status", "name", "corrid", "created", "modified"],
    lists:nth(rand:uniform(length(Fields)), Fields).

random_op() ->
    Ops = ["=", "!=", ">=", "<=", ">", "<", "~"],
    lists:nth(rand:uniform(length(Ops)), Ops).

random_value() ->
    case rand:uniform(2) of
        1 -> integer_to_list(rand:uniform(1000));
        2 -> "\"" ++ random_string(rand:uniform(8)) ++ "\""
    end.

random_string(0) -> "";
random_string(N) ->
    Chars = "abcdefghijklmnopqrstuvwxyz0123456789",
    [lists:nth(rand:uniform(length(Chars)), Chars) | random_string(N - 1)].
