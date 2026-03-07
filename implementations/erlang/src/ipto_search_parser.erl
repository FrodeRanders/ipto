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

-spec parse(binary() | string()) -> {ok, map()} | {error, term()}.
parse(Query) when is_binary(Query); is_list(Query) ->
    case tokenize(Query) of
        {ok, Tokens} ->
            case parse_or(Tokens) of
                {ok, Expr, []} ->
                    {ok, Expr};
                {ok, _Expr, Rest} ->
                    {error, {unexpected_tokens, Rest}};
                Error ->
                    Error
            end;
        Error ->
            Error
    end;
parse(_Query) ->
    {error, invalid_query}.

-spec tokenize(binary() | string()) -> {ok, [term()]} | {error, term()}.
tokenize(Query) when is_binary(Query) ->
    tokenize(unicode:characters_to_list(Query));
tokenize(Query) when is_list(Query) ->
    tokenize_chars(Query, []);
tokenize(_) ->
    {error, invalid_query}.

-spec tokenize_chars(string(), [term()]) -> {ok, [term()]} | {error, term()}.
tokenize_chars([], Acc) ->
    {ok, lists:reverse(Acc)};
tokenize_chars([C | Rest], Acc) when C =:= $\s; C =:= $\t; C =:= $\n; C =:= $\r ->
    tokenize_chars(Rest, Acc);
tokenize_chars([$( | Rest], Acc) ->
    tokenize_chars(Rest, [lparen | Acc]);
tokenize_chars([$) | Rest], Acc) ->
    tokenize_chars(Rest, [rparen | Acc]);
tokenize_chars([$, | Rest], Acc) ->
    tokenize_chars(Rest, [comma | Acc]);
tokenize_chars([$" | Rest], Acc) ->
    case read_quoted(Rest, $", []) of
        {ok, Str, Rest2} -> tokenize_chars(Rest2, [{string, Str} | Acc]);
        Error -> Error
    end;
tokenize_chars([$' | Rest], Acc) ->
    case read_quoted(Rest, $', []) of
        {ok, Str, Rest2} -> tokenize_chars(Rest2, [{string, Str} | Acc]);
        Error -> Error
    end;
tokenize_chars([C | _] = Input, Acc) when C =:= $!; C =:= $<; C =:= $>; C =:= $=; C =:= $~ ->
    case read_operator(Input) of
        {ok, Op, Rest} -> tokenize_chars(Rest, [{op, Op} | Acc]);
        Error -> Error
    end;
tokenize_chars(Input, Acc) ->
    {Word, Rest} = read_word(Input, []),
    Token = classify_word(Word),
    tokenize_chars(Rest, [Token | Acc]).

-spec read_quoted(string(), char(), string()) -> {ok, string(), string()} | {error, term()}.
read_quoted([], _Quote, _Acc) ->
    {error, unterminated_string};
read_quoted([$\\, Quote | Rest], Quote, Acc) ->
    read_quoted(Rest, Quote, [Quote | Acc]);
read_quoted([$\\, C | Rest], Quote, Acc) ->
    read_quoted(Rest, Quote, [C | Acc]);
read_quoted([Quote | Rest], Quote, Acc) ->
    {ok, lists:reverse(Acc), Rest};
read_quoted([C | Rest], Quote, Acc) ->
    read_quoted(Rest, Quote, [C | Acc]).

-spec read_operator(string()) -> {ok, string(), string()} | {error, term()}.
read_operator([$!, $= | Rest]) -> {ok, "!=", Rest};
read_operator([$<, $> | Rest]) -> {ok, "<>", Rest};
read_operator([$>, $= | Rest]) -> {ok, ">=", Rest};
read_operator([$<, $= | Rest]) -> {ok, "<=", Rest};
read_operator([$= | Rest]) -> {ok, "=", Rest};
read_operator([$> | Rest]) -> {ok, ">", Rest};
read_operator([$< | Rest]) -> {ok, "<", Rest};
read_operator([$~ | Rest]) -> {ok, "~", Rest};
read_operator(_) -> {error, invalid_operator}.

-spec read_word(string(), string()) -> {string(), string()}.
read_word([], Acc) ->
    {lists:reverse(Acc), []};
read_word([C | Rest], Acc) when C =:= $\s; C =:= $\t; C =:= $\n; C =:= $\r ->
    {lists:reverse(Acc), [C | Rest]};
read_word([C | Rest], Acc) when C =:= $(; C =:= $); C =:= $,; C =:= $!; C =:= $<; C =:= $>; C =:= $=; C =:= $~; C =:= $"; C =:= $' ->
    {lists:reverse(Acc), [C | Rest]};
read_word([C | Rest], Acc) ->
    read_word(Rest, [C | Acc]).

-spec classify_word(string()) -> term().
classify_word(Word) ->
    Lower = string:lowercase(Word),
    case Lower of
        "and" -> and_kw;
        "or" -> or_kw;
        "not" -> not_kw;
        "in" -> in_kw;
        "between" -> between_kw;
        "like" -> {op, "like"};
        _ -> {word, Word}
    end.

-spec parse_or([term()]) -> {ok, map(), [term()]} | {error, term()}.
parse_or(Tokens) ->
    case parse_and(Tokens) of
        {ok, Left, Rest} ->
            parse_or_tail(Left, Rest);
        Error ->
            Error
    end.

-spec parse_or_tail(map(), [term()]) -> {ok, map(), [term()]} | {error, term()}.
parse_or_tail(Left, [or_kw | Rest]) ->
    case parse_and(Rest) of
        {ok, Right, Rest2} ->
            parse_or_tail(combine_or(Left, Right), Rest2);
        Error ->
            Error
    end;
parse_or_tail(Left, Rest) ->
    {ok, Left, Rest}.

-spec parse_and([term()]) -> {ok, map(), [term()]} | {error, term()}.
parse_and(Tokens) ->
    case parse_not(Tokens) of
        {ok, Left, Rest} ->
            parse_and_tail(Left, Rest);
        Error ->
            Error
    end.

-spec parse_and_tail(map(), [term()]) -> {ok, map(), [term()]} | {error, term()}.
parse_and_tail(Left, [and_kw | Rest]) ->
    case parse_not(Rest) of
        {ok, Right, Rest2} ->
            parse_and_tail(combine_and(Left, Right), Rest2);
        Error ->
            Error
    end;
parse_and_tail(Left, Rest) ->
    {ok, Left, Rest}.

-spec parse_not([term()]) -> {ok, map(), [term()]} | {error, term()}.
parse_not([not_kw | Rest]) ->
    case parse_not(Rest) of
        {ok, Expr, Rest2} ->
            {ok, #{'$not' => Expr}, Rest2};
        Error ->
            Error
    end;
parse_not(Tokens) ->
    parse_primary(Tokens).

-spec parse_primary([term()]) -> {ok, map(), [term()]} | {error, term()}.
parse_primary([lparen | Rest]) ->
    case parse_or(Rest) of
        {ok, Expr, [rparen | Rest2]} ->
            {ok, Expr, Rest2};
        {ok, _Expr, _Rest2} ->
            {error, missing_rparen};
        Error ->
            Error
    end;
parse_primary(Tokens) ->
    parse_clause_tokens(Tokens).

-spec parse_clause_tokens([term()]) -> {ok, map(), [term()]} | {error, term()}.
parse_clause_tokens([{word, Field0}, {op, Op0}, ValueToken | Rest]) ->
    case token_value(ValueToken) of
        {ok, Value0} ->
            Field = normalize_field(Field0),
            Value = normalize_value(Value0),
            Op = normalize_operator(Op0),
            case to_filter(Field, Op, Value) of
                {ok, {Key, Value1}} -> {ok, #{Key => Value1}, Rest};
                Error -> Error
            end;
        error ->
            {error, {invalid_clause, Field0}}
    end;
parse_clause_tokens([{word, Field0}, in_kw, lparen | Rest]) ->
    parse_in_clause(Field0, false, Rest);
parse_clause_tokens([{word, Field0}, not_kw, in_kw, lparen | Rest]) ->
    parse_in_clause(Field0, true, Rest);
parse_clause_tokens([{word, Field0}, between_kw, V1Token, and_kw, V2Token | Rest]) ->
    case {token_value(V1Token), token_value(V2Token)} of
        {{ok, V10}, {ok, V20}} ->
            Field = normalize_field(Field0),
            V1 = normalize_value(V10),
            V2 = normalize_value(V20),
            case build_between_expr(Field, V1, V2) of
                {ok, Expr} -> {ok, Expr, Rest};
                Error -> Error
            end;
        _ ->
            {error, {invalid_clause, Field0}}
    end;
parse_clause_tokens(Tokens) ->
    {error, {invalid_clause_tokens, Tokens}}.

-spec token_value(term()) -> {ok, string()} | error.
token_value({word, Value}) -> {ok, Value};
token_value({string, Value}) -> {ok, Value};
token_value(_) -> error.

-spec parse_in_clause(string(), boolean(), [term()]) -> {ok, map(), [term()]} | {error, term()}.
parse_in_clause(Field0, IsNegated, Tokens) ->
    case parse_in_values(Tokens, []) of
        {ok, Values0, Rest} ->
            Field = normalize_field(Field0),
            Values = [normalize_value(V) || V <- Values0],
            case build_in_expr(Field, Values, IsNegated) of
                {ok, Expr} -> {ok, Expr, Rest};
                Error -> Error
            end;
        Error ->
            Error
    end.

-spec parse_in_values([term()], [string()]) -> {ok, [string()], [term()]} | {error, term()}.
parse_in_values([rparen | Rest], Acc) ->
    {ok, lists:reverse(Acc), Rest};
parse_in_values(Tokens, Acc) ->
    case parse_single_in_value(Tokens) of
        {ok, Value, [comma | Rest]} ->
            parse_in_values(Rest, [Value | Acc]);
        {ok, Value, [rparen | Rest]} ->
            {ok, lists:reverse([Value | Acc]), Rest};
        {ok, _Value, Rest} ->
            {error, {invalid_in_list_tail, Rest}};
        Error ->
            Error
    end.

-spec parse_single_in_value([term()]) -> {ok, string(), [term()]} | {error, term()}.
parse_single_in_value([Token | Rest]) ->
    case token_value(Token) of
        {ok, Value} -> {ok, Value, Rest};
        error -> {error, {invalid_in_value, Token}}
    end;
parse_single_in_value([]) ->
    {error, invalid_in_list}.

-spec normalize_operator(string()) -> string().
normalize_operator(Op) ->
    case string:lowercase(Op) of
        "like" -> "like";
        Other -> Other
    end.

-spec build_in_expr(string(), [string()], boolean()) -> {ok, map()} | {error, term()}.
build_in_expr(_Field, [], _Negated) ->
    {error, empty_in_list};
build_in_expr(Field, Values, Negated) ->
    case [to_clause_expr(Field, V) || V <- Values] of
        [] ->
            {error, empty_in_list};
        Exprs ->
            OrExpr = #{'$or' => [E || {ok, E} <- Exprs]},
            case lists:all(fun(E) -> element(1, E) =:= ok end, Exprs) of
                true ->
                    case Negated of
                        true -> {ok, #{'$not' => OrExpr}};
                        false -> {ok, OrExpr}
                    end;
                false ->
                    {error, invalid_in_value}
            end
    end.

-spec build_between_expr(string(), string(), string()) -> {ok, map()} | {error, term()}.
build_between_expr(Field, V1, V2) ->
    case {to_cmp_clause_expr(Field, gte, V1), to_cmp_clause_expr(Field, lte, V2)} of
        {{ok, E1}, {ok, E2}} ->
            {ok, #{'$and' => [E1, E2]}};
        _ ->
            {error, invalid_between_clause}
    end.

-spec to_clause_expr(string(), string()) -> {ok, map()} | {error, term()}.
to_clause_expr(Field, Value) ->
    case to_filter(Field, "=", Value) of
        {ok, {Key, Value1}} -> {ok, #{Key => Value1}};
        Error -> Error
    end.

-spec to_cmp_clause_expr(string(), gte | lte, string()) -> {ok, map()} | {error, term()}.
to_cmp_clause_expr(Field, gte, Value) ->
    case to_filter(Field, ">=", Value) of
        {ok, {Key, Value1}} -> {ok, #{Key => Value1}};
        Error -> Error
    end;
to_cmp_clause_expr(Field, lte, Value) ->
    case to_filter(Field, "<=", Value) of
        {ok, {Key, Value1}} -> {ok, #{Key => Value1}};
        Error -> Error
    end.

-spec combine_and(map(), map()) -> map().
combine_and(A, B) ->
    case {is_plain_filter_map(A), is_plain_filter_map(B)} of
        {true, true} ->
            maps:merge(A, B);
        _ ->
            #{'$and' => expr_list('and', A) ++ expr_list('and', B)}
    end.

-spec combine_or(map(), map()) -> map().
combine_or(A, B) ->
    #{'$or' => expr_list('or', A) ++ expr_list('or', B)}.

-spec expr_list('and' | 'or', map()) -> [map()].
expr_list('and', Expr) ->
    case Expr of
        #{'$and' := Items} when is_list(Items) -> Items;
        _ -> [Expr]
    end;
expr_list('or', Expr) ->
    case Expr of
        #{'$or' := Items} when is_list(Items) -> Items;
        _ -> [Expr]
    end.

-spec is_plain_filter_map(map()) -> boolean().
is_plain_filter_map(Expr) ->
    not maps:is_key('$and', Expr) andalso
    not maps:is_key('$or', Expr) andalso
    not maps:is_key('$not', Expr).

normalize_field(Field0) ->
    Field = string:lowercase(Field0),
    case Field of
        "tenant_id" -> "tenantid";
        "unit_id" -> "unitid";
        "corr_id" -> "corrid";
        "correlationid" -> "corrid";
        "unitname" -> "name";
        "unit_name" -> "name";
        _ -> Field
    end.

-spec to_filter(string(), string(), string()) -> {ok, {atom(), term()}} | {error, term()}.
to_filter("tenantid", Op, Value) ->
    int_filter_op(tenantid, Op, Value);
to_filter("unitid", Op, Value) ->
    int_filter_op(unitid, Op, Value);
to_filter("status", Op, Value) ->
    status_filter_op(Op, Value);
to_filter("name", Op, Value) ->
    string_filter_op(name, Op, Value);
to_filter("corrid", Op, Value) ->
    string_filter_op(corrid, Op, Value);
to_filter("created", Op, Value) ->
    temporal_filter_op(created, Op, Value);
to_filter("modified", Op, Value) ->
    temporal_filter_op(modified, Op, Value);
to_filter("created_after", "=", Value) ->
    {ok, {created_after, to_binary(Value)}};
to_filter("created_before", "=", Value) ->
    {ok, {created_before, to_binary(Value)}};
to_filter("modified_after", "=", Value) ->
    {ok, {modified_after, to_binary(Value)}};
to_filter("modified_before", "=", Value) ->
    {ok, {modified_before, to_binary(Value)}};
to_filter(Field, _Op, _Value) ->
    {error, {unsupported_field, Field}}.

-spec int_filter_op(atom(), string(), string()) -> {ok, {atom(), integer()}} | {error, term()}.
int_filter_op(Key, "=", Value) ->
    int_filter(Key, Value);
int_filter_op(Key, "!=", Value) ->
    int_filter(key_with_op(Key, ne), Value);
int_filter_op(Key, "<>", Value) ->
    int_filter(key_with_op(Key, ne), Value);
int_filter_op(Key, ">", Value) ->
    int_filter(key_with_op(Key, gt), Value);
int_filter_op(Key, ">=", Value) ->
    int_filter(key_with_op(Key, gte), Value);
int_filter_op(Key, "<", Value) ->
    int_filter(key_with_op(Key, lt), Value);
int_filter_op(Key, "<=", Value) ->
    int_filter(key_with_op(Key, lte), Value);
int_filter_op(_Key, Op, _Value) ->
    {error, {unsupported_operator, Op}}.

-spec status_filter_op(string(), string()) -> {ok, {atom(), integer()}} | {error, term()}.
status_filter_op(Op, Value) ->
    case normalize_status_value(Value) of
        {ok, Status} ->
            int_filter_op(status, Op, integer_to_list(Status));
        {error, _} = Error ->
            Error
    end.

-spec normalize_status_value(string()) -> {ok, integer()} | {error, term()}.
normalize_status_value(Value0) ->
    Value = string:trim(Value0),
    Lower = string:lowercase(Value),
    case Lower of
        "pending_disposition" -> {ok, 1};
        "pending_deletion" -> {ok, 10};
        "obliterated" -> {ok, 20};
        "effective" -> {ok, 30};
        "archived" -> {ok, 40};
        _ ->
            case string:to_integer(Value) of
                {I, []} -> {ok, I};
                _ -> {error, {invalid_status, Value}}
            end
    end.

-spec string_filter_op(atom(), string(), string()) -> {ok, {atom(), binary()}} | {error, term()}.
string_filter_op(Key, "=", Value) ->
    {ok, {Key, to_binary(Value)}};
string_filter_op(Key, "!=", Value) ->
    {ok, {key_with_op(Key, ne), to_binary(Value)}};
string_filter_op(Key, "<>", Value) ->
    {ok, {key_with_op(Key, ne), to_binary(Value)}};
string_filter_op(name, "~", Value) ->
    {ok, {name_ilike, to_binary(Value)}};
string_filter_op(name, Op, Value) when Op =:= "like"; Op =:= "LIKE"; Op =:= "Like" ->
    {ok, {name_ilike, to_binary(Value)}};
string_filter_op(corrid, "~", Value) ->
    {ok, {corrid_ilike, to_binary(Value)}};
string_filter_op(corrid, Op, Value) when Op =:= "like"; Op =:= "LIKE"; Op =:= "Like" ->
    {ok, {corrid_ilike, to_binary(Value)}};
string_filter_op(_Key, Op, _Value) ->
    {error, {unsupported_operator, Op}}.

-spec temporal_filter_op(atom(), string(), string()) -> {ok, {atom(), binary()}} | {error, term()}.
temporal_filter_op(Key, "=", Value) ->
    {ok, {Key, to_binary(Value)}};
temporal_filter_op(Key, "!=", Value) ->
    {ok, {key_with_op(Key, ne), to_binary(Value)}};
temporal_filter_op(Key, "<>", Value) ->
    {ok, {key_with_op(Key, ne), to_binary(Value)}};
temporal_filter_op(Key, ">", Value) ->
    {ok, {key_with_op(Key, gt), to_binary(Value)}};
temporal_filter_op(Key, ">=", Value) ->
    {ok, {temporal_after_key(Key), to_binary(Value)}};
temporal_filter_op(Key, "<", Value) ->
    {ok, {temporal_before_key(Key), to_binary(Value)}};
temporal_filter_op(Key, "<=", Value) ->
    {ok, {key_with_op(Key, lte), to_binary(Value)}};
temporal_filter_op(_Key, Op, _Value) ->
    {error, {unsupported_operator, Op}}.

-spec int_filter(atom(), string()) -> {ok, {atom(), integer()}} | {error, term()}.
int_filter(Key, Value) ->
    case string:to_integer(Value) of
        {I, []} -> {ok, {Key, I}};
        _ -> {error, {invalid_integer, {Key, Value}}}
    end.

-spec key_with_op(atom(), ne | gt | gte | lt | lte) -> atom().
key_with_op(tenantid, ne) -> tenantid_ne;
key_with_op(tenantid, gt) -> tenantid_gt;
key_with_op(tenantid, gte) -> tenantid_gte;
key_with_op(tenantid, lt) -> tenantid_lt;
key_with_op(tenantid, lte) -> tenantid_lte;
key_with_op(unitid, ne) -> unitid_ne;
key_with_op(unitid, gt) -> unitid_gt;
key_with_op(unitid, gte) -> unitid_gte;
key_with_op(unitid, lt) -> unitid_lt;
key_with_op(unitid, lte) -> unitid_lte;
key_with_op(status, ne) -> status_ne;
key_with_op(status, gt) -> status_gt;
key_with_op(status, gte) -> status_gte;
key_with_op(status, lt) -> status_lt;
key_with_op(status, lte) -> status_lte;
key_with_op(name, ne) -> name_ne;
key_with_op(corrid, ne) -> corrid_ne;
key_with_op(created, ne) -> created_ne;
key_with_op(created, gt) -> created_gt;
key_with_op(created, gte) -> created_gte;
key_with_op(created, lt) -> created_lt;
key_with_op(created, lte) -> created_lte;
key_with_op(modified, ne) -> modified_ne;
key_with_op(modified, gt) -> modified_gt;
key_with_op(modified, gte) -> modified_gte;
key_with_op(modified, lt) -> modified_lt;
key_with_op(modified, lte) -> modified_lte.

-spec temporal_after_key(atom()) -> atom().
temporal_after_key(created) -> created_after;
temporal_after_key(modified) -> modified_after.

-spec temporal_before_key(atom()) -> atom().
temporal_before_key(created) -> created_before;
temporal_before_key(modified) -> modified_before.

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
