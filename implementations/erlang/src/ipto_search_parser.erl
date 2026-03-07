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
-module(ipto_search_parser).

-export([parse/1]).

-spec parse(binary() | string()) -> {ok, map()} | {error, {invalid_clause, string()} | invalid_query | {unsupported_field, string()} | {invalid_integer, {atom(), string()}}}.
parse(Query) when is_binary(Query); is_list(Query) ->
    Clauses = split_clauses(Query),
    parse_clauses(Clauses, #{});
parse(_Query) ->
    {error, invalid_query}.

-spec split_clauses(binary() | string()) -> [string()].
split_clauses(Query) when is_binary(Query) ->
    split_clauses(unicode:characters_to_list(Query));
split_clauses(Query) when is_list(Query) ->
    [C || C <- re:split(Query, "\\s+[Aa][Nn][Dd]\\s+", [{return, list}]), C =/= []].

-spec parse_clauses([string()], map()) ->
    {ok, map()} | {error, {invalid_clause, string()} | invalid_query | {unsupported_field, string()} | {invalid_integer, {atom(), string()}}}.
parse_clauses([], Expr) ->
    {ok, Expr};
parse_clauses([Clause | Rest], Expr) ->
    case parse_clause(Clause) of
        {ok, {Key, Value}} ->
            parse_clauses(Rest, Expr#{Key => Value});
        Error ->
            Error
    end.

-spec parse_clause(string()) ->
    {ok, {atom(), string()}} | {error, {invalid_clause, string()} | {unsupported_field, string()} | {invalid_integer, {atom(), string()}}}.
parse_clause(Clause) ->
    case re:run(Clause, "^\\s*([A-Za-z_][A-Za-z0-9_]*)\\s*(=|~|>=|<)\\s*(.+?)\\s*$", [{capture, all_but_first, list}]) of
        {match, [Field0, Op, Value0]} ->
            Field = string:lowercase(Field0),
            Value = normalize_value(Value0),
            to_filter(Field, Op, Value);
        nomatch ->
            {error, {invalid_clause, Clause}}
    end.

-spec to_filter(string(), string(), string()) ->
    {ok, {atom(), binary() | integer()}} | {error, {unsupported_field, string()} | {invalid_integer, {atom(), string()}}}.
to_filter("tenantid", "=", Value) ->
    int_filter(tenantid, Value);
to_filter("unitid", "=", Value) ->
    int_filter(unitid, Value);
to_filter("status", "=", Value) ->
    int_filter(status, Value);
to_filter("name", "=", Value) ->
    {ok, {name, to_binary(Value)}};
to_filter("name", "~", Value) ->
    {ok, {name_ilike, to_binary(Value)}};
to_filter("created", ">=", Value) ->
    {ok, {created_after, to_binary(Value)}};
to_filter("created", "<", Value) ->
    {ok, {created_before, to_binary(Value)}};
to_filter("created_after", "=", Value) ->
    {ok, {created_after, to_binary(Value)}};
to_filter("created_before", "=", Value) ->
    {ok, {created_before, to_binary(Value)}};
to_filter(Field, _Op, _Value) ->
    {error, {unsupported_field, Field}}.

-spec int_filter(atom(), string()) -> {ok, {atom(), integer()}} | {error, {invalid_integer, {atom(), string()}}}.
int_filter(Key, Value) ->
    case string:to_integer(Value) of
        {I, []} -> {ok, {Key, I}};
        _ -> {error, {invalid_integer, {Key, Value}}}
    end.

-spec normalize_value(string()) -> string().
normalize_value(Value0) ->
    Value1 = string:trim(Value0),
    case Value1 of
        [] ->
            [];
        _ ->
            case {lists:nth(1, Value1), lists:nth(length(Value1), Value1)} of
                {$", $"} when length(Value1) >= 2 ->
                    lists:sublist(Value1, 2, length(Value1) - 2);
                {$', $'} when length(Value1) >= 2 ->
                    lists:sublist(Value1, 2, length(Value1) - 2);
                _ ->
                    Value1
            end
    end.

-spec to_binary(binary() | string()) -> binary().
to_binary(Value) when is_binary(Value) ->
    Value;
to_binary(Value) when is_list(Value) ->
    unicode:characters_to_binary(Value).
