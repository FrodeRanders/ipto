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

-export([parse/1, parse_ast/1, map_to_ast/1]).

-define(STATUS_PENDING_DISPOSITION, 1).
-define(STATUS_PENDING_DELETION, 10).
-define(STATUS_OBLITERATED, 20).
-define(STATUS_EFFECTIVE, 30).
-define(STATUS_ARCHIVED, 40).

-spec parse(binary() | string()) -> {ok, map()} | {error, term()}.
parse(Query) ->
    case parse_ast(Query) of
        {ok, Expr} -> {ok, ast_to_map(Expr)};
        Error -> Error
    end.

-spec parse_ast(binary() | string()) -> {ok, ipto_search_ast:search_expr()} | {error, term()}.
parse_ast(Query) when is_binary(Query); is_list(Query) ->
    case tokenize(Query) of
        {ok, Tokens} ->
            case parse_or_ast(Tokens) of
                {ok, Expr, []} -> {ok, Expr};
                {ok, _Expr, Rest} -> {error, {unexpected_tokens, Rest}};
                Error -> Error
            end;
        Error -> Error
    end;
parse_ast(_Query) ->
    {error, invalid_query}.

%% Tokenizer

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
read_quoted([], _Quote, _Acc) -> {error, unterminated_string};
read_quoted([$\\, Quote | Rest], Quote, Acc) -> read_quoted(Rest, Quote, [Quote | Acc]);
read_quoted([$\\, C | Rest], Quote, Acc) -> read_quoted(Rest, Quote, [C | Acc]);
read_quoted([Quote | Rest], Quote, Acc) -> {ok, lists:reverse(Acc), Rest};
read_quoted([C | Rest], Quote, Acc) -> read_quoted(Rest, Quote, [C | Acc]).

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
read_word([], Acc) -> {lists:reverse(Acc), []};
read_word([C | Rest], Acc) when C =:= $\s; C =:= $\t; C =:= $\n; C =:= $\r ->
    {lists:reverse(Acc), [C | Rest]};
read_word([C | Rest], Acc) when C =:= $(; C =:= $); C =:= $,; C =:= $!; C =:= $<; C =:= $>; C =:= $=; C =:= $~; C =:= $"; C =:= $' ->
    {lists:reverse(Acc), [C | Rest]};
read_word([C | Rest], Acc) -> read_word(Rest, [C | Acc]).

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

%% AST-based recursive-descent parser

-spec parse_or_ast([term()]) -> {ok, ipto_search_ast:search_expr(), [term()]} | {error, term()}.
parse_or_ast(Tokens) ->
    case parse_and_ast(Tokens) of
        {ok, Left, Rest} -> parse_or_tail_ast(Left, Rest);
        Error -> Error
    end.

-spec parse_or_tail_ast(ipto_search_ast:search_expr(), [term()]) -> {ok, ipto_search_ast:search_expr(), [term()]} | {error, term()}.
parse_or_tail_ast(Left, [or_kw | Rest]) ->
    case parse_and_ast(Rest) of
        {ok, Right, Rest2} -> parse_or_tail_ast(ipto_search_ast:or_expr(Left, Right), Rest2);
        Error -> Error
    end;
parse_or_tail_ast(Left, Rest) ->
    {ok, Left, Rest}.

-spec parse_and_ast([term()]) -> {ok, ipto_search_ast:search_expr(), [term()]} | {error, term()}.
parse_and_ast(Tokens) ->
    case parse_not_ast(Tokens) of
        {ok, Left, Rest} -> parse_and_tail_ast(Left, Rest);
        Error -> Error
    end.

-spec parse_and_tail_ast(ipto_search_ast:search_expr(), [term()]) -> {ok, ipto_search_ast:search_expr(), [term()]} | {error, term()}.
parse_and_tail_ast(Left, [and_kw | Rest]) ->
    case parse_not_ast(Rest) of
        {ok, Right, Rest2} -> parse_and_tail_ast(ipto_search_ast:and_expr(Left, Right), Rest2);
        Error -> Error
    end;
parse_and_tail_ast(Left, Rest) ->
    {ok, Left, Rest}.

-spec parse_not_ast([term()]) -> {ok, ipto_search_ast:search_expr(), [term()]} | {error, term()}.
parse_not_ast([not_kw | Rest]) ->
    case parse_not_ast(Rest) of
        {ok, Expr, Rest2} -> {ok, ipto_search_ast:not_expr(Expr), Rest2};
        Error -> Error
    end;
parse_not_ast(Tokens) ->
    parse_primary_ast(Tokens).

-spec parse_primary_ast([term()]) -> {ok, ipto_search_ast:search_expr(), [term()]} | {error, term()}.
parse_primary_ast([lparen | Rest]) ->
    case parse_or_ast(Rest) of
        {ok, Expr, [rparen | Rest2]} -> {ok, Expr, Rest2};
        {ok, _Expr, _Rest2} -> {error, missing_rparen};
        Error -> Error
    end;
parse_primary_ast(Tokens) ->
    parse_clause_tokens_ast(Tokens).

-spec parse_clause_tokens_ast([term()]) -> {ok, ipto_search_ast:search_expr(), [term()]} | {error, term()}.
parse_clause_tokens_ast([{word, Field0}, {op, Op0}, ValueToken | Rest]) ->
    case token_value(ValueToken) of
        {ok, Value0} ->
            Field = string:lowercase(Field0),
            case classify_field_ast(Field) of
                {ok, Builder} ->
                    Op = normalize_operator_ast(Op0),
                    Value = normalize_value(Value0),
                    case Builder(Field, Op, Value) of
                        {ok, Item} -> {ok, ipto_search_ast:leaf(Item), Rest};
                        Error -> Error
                    end;
                {error, _} = Error -> Error
            end;
        error -> {error, {invalid_clause, Field0}}
    end;
parse_clause_tokens_ast([{word, Field0}, in_kw, lparen | Rest]) ->
    parse_in_clause_ast(Field0, false, Rest);
parse_clause_tokens_ast([{word, Field0}, not_kw, in_kw, lparen | Rest]) ->
    parse_in_clause_ast(Field0, true, Rest);
parse_clause_tokens_ast([{word, Field0}, between_kw, V1Token, and_kw, V2Token | Rest]) ->
    Field = string:lowercase(Field0),
    case {token_value(V1Token), token_value(V2Token)} of
        {{ok, V10}, {ok, V20}} ->
            case classify_field_ast(Field) of
                {ok, Builder} ->
                    V1 = normalize_value(V10),
                    V2 = normalize_value(V20),
                    case Builder(Field, gte, V1) of
                        {ok, Item1} ->
                            case Builder(Field, lte, V2) of
                                {ok, Item2} ->
                                    {ok, ipto_search_ast:between_expr(Item1, Item2), Rest};
                                Error -> Error
                            end;
                        Error -> Error
                    end;
                {error, _} = Error -> Error
            end;
        _ -> {error, {invalid_clause, Field0}}
    end;
parse_clause_tokens_ast(Tokens) ->
    {error, {invalid_clause_tokens, Tokens}}.

-spec parse_in_clause_ast(string(), boolean(), [term()]) -> {ok, ipto_search_ast:search_expr(), [term()]} | {error, term()}.
parse_in_clause_ast(Field, IsNegated, Tokens) ->
    case parse_in_values_ast(Tokens, []) of
        {ok, Values, Rest} ->
            case classify_field_ast(Field) of
                {ok, Builder} ->
                    Items = [Item || V <- Values, {ok, Item} <- [Builder(Field, eq, V)]],
                    case Items of
                        [] -> {error, empty_in_list};
                        _ ->
                            OrExpr = lists:foldl(
                                fun(Item, Acc) -> ipto_search_ast:or_expr(Acc, ipto_search_ast:leaf(Item)) end,
                                ipto_search_ast:leaf(hd(Items)),
                                tl(Items)
                            ),
                            case IsNegated of
                                true -> {ok, ipto_search_ast:not_expr(OrExpr), Rest};
                                false -> {ok, OrExpr, Rest}
                            end
                    end;
                {error, _} = Error -> Error
            end;
        Error -> Error
    end.

-spec parse_in_values_ast([term()], [string()]) -> {ok, [string()], [term()]} | {error, term()}.
parse_in_values_ast([rparen | Rest], Acc) ->
    {ok, lists:reverse(Acc), Rest};
parse_in_values_ast(Tokens, Acc) ->
    case parse_single_in_value(Tokens) of
        {ok, Value, [comma | Rest]} -> parse_in_values_ast(Rest, [Value | Acc]);
        {ok, Value, [rparen | Rest]} -> {ok, lists:reverse([Value | Acc]), Rest};
        {ok, _Value, Rest} -> {error, {invalid_in_list_tail, Rest}};
        Error -> Error
    end.

-spec parse_single_in_value([term()]) -> {ok, string(), [term()]} | {error, term()}.
parse_single_in_value([Token | Rest]) ->
    case token_value(Token) of
        {ok, Value} -> {ok, Value, Rest};
        error -> {error, {invalid_in_value, Token}}
    end;
parse_single_in_value([]) -> {error, invalid_in_list}.

-spec token_value(term()) -> {ok, string()} | error.
token_value({word, Value}) -> {ok, Value};
token_value({string, Value}) -> {ok, Value};
token_value(_) -> error.

%% Field classification

-type item_builder() :: fun((string(), ipto_search_ast:operator(), string()) -> {ok, ipto_search_ast:search_item()} | {error, term()}).

-spec classify_field_ast(string()) -> {ok, item_builder()} | {error, term()}.
classify_field_ast(Field) ->
    case classify_relation_spec(Field) of
        {ok, _} = Ok -> Ok;
        undefined ->
            case classify_association_spec(Field) of
                {ok, _} = Ok -> Ok;
                undefined ->
                    case classify_unit_field_ast(Field) of
                        {ok, _} = Ok -> Ok;
                        undefined ->
                            case has_attribute_colon(Field) of
                                true -> {ok, fun build_attr_item/3};
                                false -> {error, {unknown_field, Field}}
                            end
                    end
            end
    end.

-spec classify_relation_spec(string()) -> {ok, item_builder()} | undefined.
classify_relation_spec(Field) ->
    case split_spec_field(Field) of
        [Prefix, Direction, TypeStr | _] when Prefix =:= "relation"; Prefix =:= "rel" ->
            case {resolve_direction(Direction), string:to_integer(TypeStr)} of
                {{ok, Dir}, {RT, []}} ->
                    {ok, fun(_F, eq, RefString) ->
                        case parse_unit_ref(RefString) of
                            {ok, Ref} -> {ok, ipto_search_ast:rel_item(Dir, RT, Ref)};
                            Error -> Error
                        end;
                        (_F, _Op, _V) -> {error, {unsupported_operator_for_relation, _Op}}
                    end};
                _ -> undefined
            end;
        _ -> undefined
    end.

-spec classify_association_spec(string()) -> {ok, item_builder()} | undefined.
classify_association_spec(Field) ->
    case split_spec_field(Field) of
        [Prefix, Direction, TypeStr | _] when Prefix =:= "association"; Prefix =:= "assoc" ->
            case {resolve_direction(Direction), string:to_integer(TypeStr)} of
                {{ok, Dir}, {AT, []}} ->
                    {ok, fun(_F, eq, RefString) ->
                        {ok, ipto_search_ast:assoc_item(Dir, AT, to_binary(RefString))};
                        (_F, _Op, _V) -> {error, {unsupported_operator_for_association, _Op}}
                    end};
                _ -> undefined
            end;
        _ -> undefined
    end.

-spec split_spec_field(string()) -> [string()].
split_spec_field(Field) ->
    Normalized = string:replace(Field, "_", ":", all),
    string:split(Normalized, ":", all).

-spec resolve_direction(string()) -> {ok, left | right} | error.
resolve_direction("left") -> {ok, left};
resolve_direction("right") -> {ok, right};
resolve_direction(_) -> error.

-spec has_attribute_colon(string()) -> boolean().
has_attribute_colon(Field) ->
    case string:find(Field, ":") of
        Field -> false;
        nomatch -> false;
        _ -> true
    end.

-spec classify_unit_field_ast(string()) -> {ok, item_builder()} | undefined.
classify_unit_field_ast(Field) ->
    Known = [
        {"tenantid", fun build_int_unit_item/3},
        {"tenant_id", fun build_int_unit_item/3},
        {"unitid", fun build_int_unit_item/3},
        {"unit_id", fun build_int_unit_item/3},
        {"unitver", fun build_int_unit_item/3},
        {"status", fun build_status_item/3},
        {"name", fun build_string_unit_item/3},
        {"unitname", fun build_string_unit_item/3},
        {"unit_name", fun build_string_unit_item/3},
        {"corrid", fun build_string_unit_item/3},
        {"correlationid", fun build_string_unit_item/3},
        {"corr_id", fun build_string_unit_item/3},
        {"created", fun build_temporal_unit_item/3},
        {"modified", fun build_temporal_unit_item/3},
        {"created_after", fun build_created_after_item/3},
        {"created_before", fun build_created_before_item/3},
        {"modified_after", fun build_modified_after_item/3},
        {"modified_before", fun build_modified_before_item/3}
    ],
    case lists:keyfind(Field, 1, Known) of
        {Field, Builder} -> {ok, Builder};
        false -> undefined
    end.

-spec parse_unit_ref(string()) -> {ok, {integer(), integer()}} | {error, term()}.
parse_unit_ref(RefString) ->
    case string:split(RefString, ".") of
        [T, U] ->
            case {string:to_integer(T), string:to_integer(U)} of
                {{Tid, []}, {Uid, []}} -> {ok, {Tid, Uid}};
                _ -> {error, {invalid_unit_ref, RefString}}
            end;
        [RefString] ->
            case string:to_integer(RefString) of
                {Uid, []} -> {ok, {1, Uid}};
                _ -> {error, {invalid_unit_ref, RefString}}
            end
    end.

%% Item builders

-spec build_int_unit_item(string(), ipto_search_ast:operator(), string()) -> {ok, ipto_search_ast:search_item()} | {error, term()}.
build_int_unit_item(Field, Op, Value) ->
    NormalizedField = normalize_field(Field),
    case string:to_integer(Value) of
        {I, []} -> {ok, ipto_search_ast:unit_item(list_to_atom(NormalizedField), Op, I)};
        _ -> {error, {invalid_integer, {Field, Value}}}
    end.

-spec build_string_unit_item(string(), ipto_search_ast:operator(), string()) -> {ok, ipto_search_ast:search_item()} | {error, term()}.
build_string_unit_item(Field, Op, Value) ->
    NormalizedField = case Field of
        "correlationid" -> corrid; "corr_id" -> corrid;
        "unitname" -> unitname; "unit_name" -> unitname; "name" -> unitname;
        _ -> list_to_atom(Field)
    end,
    case has_wildcard(Value) of
        true -> {ok, ipto_search_ast:unit_item(NormalizedField, like, to_binary(to_like_pattern(Value)))};
        false when Op =:= like -> {ok, ipto_search_ast:unit_item(NormalizedField, like, to_binary(Value))};
        false -> {ok, ipto_search_ast:unit_item(NormalizedField, Op, to_binary(Value))}
    end.

-spec build_status_item(string(), ipto_search_ast:operator(), string()) -> {ok, ipto_search_ast:search_item()} | {error, term()}.
build_status_item(_Field, Op, Value) ->
    case normalize_status_value(Value) of
        {ok, Status} -> {ok, ipto_search_ast:unit_item(status, Op, Status)};
        Error -> Error
    end.

-spec build_temporal_unit_item(string(), ipto_search_ast:operator(), string()) -> {ok, ipto_search_ast:search_item()} | {error, term()}.
build_temporal_unit_item(Field, Op, Value) ->
    NormalizedField = case Field of "created" -> created; "modified" -> modified; _ -> list_to_atom(Field) end,
    {ok, ipto_search_ast:unit_item(NormalizedField, Op, to_binary(Value))}.

-spec build_created_after_item(string(), ipto_search_ast:operator(), string()) -> {ok, ipto_search_ast:search_item()} | {error, term()}.
build_created_after_item(_Field, _Op, Value) ->
    {ok, ipto_search_ast:unit_item(created, gte, to_binary(Value))}.

-spec build_created_before_item(string(), ipto_search_ast:operator(), string()) -> {ok, ipto_search_ast:search_item()} | {error, term()}.
build_created_before_item(_Field, _Op, Value) ->
    {ok, ipto_search_ast:unit_item(created, lt, to_binary(Value))}.

-spec build_modified_after_item(string(), ipto_search_ast:operator(), string()) -> {ok, ipto_search_ast:search_item()} | {error, term()}.
build_modified_after_item(_Field, _Op, Value) ->
    {ok, ipto_search_ast:unit_item(modified, gte, to_binary(Value))}.

-spec build_modified_before_item(string(), ipto_search_ast:operator(), string()) -> {ok, ipto_search_ast:search_item()} | {error, term()}.
build_modified_before_item(_Field, _Op, Value) ->
    {ok, ipto_search_ast:unit_item(modified, lt, to_binary(Value))}.

-spec build_attr_item(string(), ipto_search_ast:operator(), string()) -> {ok, ipto_search_ast:search_item()} | {error, term()}.
build_attr_item(Field, Op, Value) ->
    AttrName = to_binary(strip_attr_prefix(Field)),
    case has_wildcard(Value) of
        true ->
            {ok, ipto_search_ast:attr_item(AttrName, undefined, string, like, to_binary(to_like_pattern(Value)))};
        false ->
            {ok, ipto_search_ast:attr_item(AttrName, undefined, string, Op, to_binary(Value))}
    end.

-spec strip_attr_prefix(string()) -> string().
strip_attr_prefix(Field) ->
    case string:split(Field, ":", trailing) of
        [_Prefix, Local] -> Local;
        _ -> Field
    end.

%% Helpers

-spec normalize_field(string()) -> string().
normalize_field("tenant_id") -> "tenantid";
normalize_field("unit_id") -> "unitid";
normalize_field("corr_id") -> "corrid";
normalize_field("correlationid") -> "corrid";
normalize_field("unitname") -> "name";
normalize_field("unit_name") -> "name";
normalize_field(Other) -> Other.

-spec normalize_operator_ast(string()) -> ipto_search_ast:operator().
normalize_operator_ast("=") -> eq;
normalize_operator_ast("!=") -> ne;
normalize_operator_ast("<>") -> ne;
normalize_operator_ast(">") -> gt;
normalize_operator_ast(">=") -> gte;
normalize_operator_ast("<") -> lt;
normalize_operator_ast("<=") -> lte;
normalize_operator_ast("~") -> like;
normalize_operator_ast(Op) ->
    case string:lowercase(Op) of "like" -> like; _ -> eq end.

-spec normalize_status_value(string()) -> {ok, integer()} | {error, term()}.
normalize_status_value(Value0) ->
    Value = string:trim(Value0),
    Lower = string:lowercase(Value),
    case Lower of
        "pending_disposition" -> {ok, ?STATUS_PENDING_DISPOSITION};
        "pending_deletion" -> {ok, ?STATUS_PENDING_DELETION};
        "obliterated" -> {ok, ?STATUS_OBLITERATED};
        "effective" -> {ok, ?STATUS_EFFECTIVE};
        "archived" -> {ok, ?STATUS_ARCHIVED};
        _ ->
            case string:to_integer(Value) of
                {I, []} -> {ok, I};
                _ -> {error, {invalid_status, Value}}
            end
    end.

-spec has_wildcard(string()) -> boolean().
has_wildcard(Value) -> lists:member($*, Value) orelse lists:member($%, Value).

-spec to_like_pattern(string()) -> string().
to_like_pattern(Value) -> lists:map(fun($*) -> $%; (C) -> C end, Value).

-spec normalize_value(string()) -> string().
normalize_value(Value0) ->
    Value1 = string:trim(Value0),
    case Value1 of
        [] -> [];
        _ ->
            case {lists:nth(1, Value1), lists:nth(length(Value1), Value1)} of
                {$", $"} when length(Value1) >= 2 -> lists:sublist(Value1, 2, length(Value1) - 2);
                {$', $'} when length(Value1) >= 2 -> lists:sublist(Value1, 2, length(Value1) - 2);
                _ -> Value1
            end
    end.

-spec to_binary(binary() | string()) -> binary().
to_binary(Value) when is_binary(Value) -> Value;
to_binary(Value) when is_list(Value) -> unicode:characters_to_binary(Value).

%% Backward-compatible map conversion

-spec ast_to_map(ipto_search_ast:search_expr()) -> map().
ast_to_map({'$and', Left, Right}) ->
    LM = ast_to_map(Left),
    RM = ast_to_map(Right),
    Items = map_expr_list(LM) ++ map_expr_list(RM),
    case lists:all(fun is_plain_map/1, Items) of
        true -> maps_merge_all(Items, #{});
        false -> #{'$and' => Items}
    end;
ast_to_map({'$between', Item1, Item2}) ->
    #{'$and' => [item_to_map(Item1), item_to_map(Item2)]};
ast_to_map({'$or', Left, Right}) ->
    #{'$or' => map_expr_list(ast_to_map(Left)) ++ map_expr_list(ast_to_map(Right))};
ast_to_map({'$not', Inner}) ->
    #{'$not' => ast_to_map(Inner)};
ast_to_map({leaf, {unit, Col, Op, Val}}) ->
    #{normalize_map_key(Col, Op) => Val};
ast_to_map({leaf, {attr, _Name, _AttrId, _Type, _Op, _Val}}) ->
    #{};
ast_to_map({leaf, _}) ->
    #{}.

-spec map_expr_list(map()) -> [map()].
map_expr_list(#{'$and' := Items}) when is_list(Items) -> lists:flatmap(fun map_expr_list/1, Items);
map_expr_list(M) -> [M].

-spec maps_merge_all([map()], map()) -> map().
maps_merge_all([], Acc) -> Acc;
maps_merge_all([M | Rest], Acc) -> maps_merge_all(Rest, maps:merge(Acc, M)).

-spec is_plain_map(map()) -> boolean().
is_plain_map(M) when map_size(M) =:= 0 -> true;
is_plain_map(M) ->
    not maps:is_key('$and', M) andalso
    not maps:is_key('$or', M) andalso
    not maps:is_key('$not', M).

-spec normalize_map_key(atom(), ipto_search_ast:operator()) -> atom().
normalize_map_key(tenantid, eq) -> tenantid;
normalize_map_key(tenantid, ne) -> tenantid_ne;
normalize_map_key(tenantid, gt) -> tenantid_gt;
normalize_map_key(tenantid, gte) -> tenantid_gte;
normalize_map_key(tenantid, lt) -> tenantid_lt;
normalize_map_key(tenantid, lte) -> tenantid_lte;
normalize_map_key(unitid, eq) -> unitid;
normalize_map_key(unitid, ne) -> unitid_ne;
normalize_map_key(unitid, gt) -> unitid_gt;
normalize_map_key(unitid, gte) -> unitid_gte;
normalize_map_key(unitid, lt) -> unitid_lt;
normalize_map_key(unitid, lte) -> unitid_lte;
normalize_map_key(status, eq) -> status;
normalize_map_key(status, ne) -> status_ne;
normalize_map_key(status, gt) -> status_gt;
normalize_map_key(status, gte) -> status_gte;
normalize_map_key(status, lt) -> status_lt;
normalize_map_key(status, lte) -> status_lte;
normalize_map_key(unitname, eq) -> name;
normalize_map_key(unitname, ne) -> name_ne;
normalize_map_key(unitname, like) -> name_ilike;
normalize_map_key(corrid, eq) -> corrid;
normalize_map_key(corrid, ne) -> corrid_ne;
normalize_map_key(corrid, like) -> corrid_ilike;
normalize_map_key(created, eq) -> created;
normalize_map_key(created, ne) -> created_ne;
normalize_map_key(created, gt) -> created_gt;
normalize_map_key(created, gte) -> created_after;
normalize_map_key(created, lt) -> created_before;
normalize_map_key(created, lte) -> created_lte;
normalize_map_key(modified, eq) -> modified;
normalize_map_key(modified, ne) -> modified_ne;
normalize_map_key(modified, gt) -> modified_gt;
normalize_map_key(modified, gte) -> modified_after;
normalize_map_key(modified, lt) -> modified_before;
normalize_map_key(modified, lte) -> modified_lte;
normalize_map_key(_, _) -> undefined_key.

-spec map_to_ast(map()) -> ipto_search_ast:search_expr().
map_to_ast(Expr) when map_size(Expr) =:= 0 ->
    ipto_search_ast:leaf(ipto_search_ast:unit_item(tenantid, eq, 1));
map_to_ast(#{'$and' := Items}) when is_list(Items) ->
    lists:foldl(
        fun(Item, Acc) -> ipto_search_ast:and_expr(Acc, map_to_ast(Item)) end,
        map_to_ast(hd(Items)),
        tl(Items)
    );
map_to_ast(#{'$or' := Items}) when is_list(Items) ->
    lists:foldl(
        fun(Item, Acc) -> ipto_search_ast:or_expr(Acc, map_to_ast(Item)) end,
        map_to_ast(hd(Items)),
        tl(Items)
    );
map_to_ast(#{'$not' := Inner}) ->
    ipto_search_ast:not_expr(map_to_ast(Inner));
map_to_ast(M) ->
    Items = lists:flatmap(
        fun({Key, Value}) -> map_key_to_item(Key, Value) end,
        maps:to_list(M)
    ),
    case Items of
        [Single] -> ipto_search_ast:leaf(Single);
        [] -> ipto_search_ast:leaf(ipto_search_ast:unit_item(tenantid, eq, 1));
        [First | Rest] ->
            lists:foldl(
                fun(Item, Acc) -> ipto_search_ast:and_expr(Acc, ipto_search_ast:leaf(Item)) end,
                ipto_search_ast:leaf(First),
                Rest
            )
    end.

-spec map_key_to_item(atom(), term()) -> [ipto_search_ast:search_item()].
map_key_to_item('$and', _) -> [];
map_key_to_item('$or', _) -> [];
map_key_to_item('$not', _) -> [];
map_key_to_item(tenantid, V) -> [{unit, tenantid, eq, V}];
map_key_to_item(tenantid_ne, V) -> [{unit, tenantid, ne, V}];
map_key_to_item(tenantid_gt, V) -> [{unit, tenantid, gt, V}];
map_key_to_item(tenantid_gte, V) -> [{unit, tenantid, gte, V}];
map_key_to_item(tenantid_lt, V) -> [{unit, tenantid, lt, V}];
map_key_to_item(tenantid_lte, V) -> [{unit, tenantid, lte, V}];
map_key_to_item(unitid, V) -> [{unit, unitid, eq, V}];
map_key_to_item(unitid_ne, V) -> [{unit, unitid, ne, V}];
map_key_to_item(unitid_gt, V) -> [{unit, unitid, gt, V}];
map_key_to_item(unitid_gte, V) -> [{unit, unitid, gte, V}];
map_key_to_item(unitid_lt, V) -> [{unit, unitid, lt, V}];
map_key_to_item(unitid_lte, V) -> [{unit, unitid, lte, V}];
map_key_to_item(status, V) -> [{unit, status, eq, V}];
map_key_to_item(status_ne, V) -> [{unit, status, ne, V}];
map_key_to_item(status_gt, V) -> [{unit, status, gt, V}];
map_key_to_item(status_gte, V) -> [{unit, status, gte, V}];
map_key_to_item(status_lt, V) -> [{unit, status, lt, V}];
map_key_to_item(status_lte, V) -> [{unit, status, lte, V}];
map_key_to_item(name, V) -> [{unit, unitname, eq, V}];
map_key_to_item(name_ne, V) -> [{unit, unitname, ne, V}];
map_key_to_item(name_ilike, V) -> [{unit, unitname, like, V}];
map_key_to_item(corrid, V) -> [{unit, corrid, eq, V}];
map_key_to_item(corrid_ne, V) -> [{unit, corrid, ne, V}];
map_key_to_item(corrid_ilike, V) -> [{unit, corrid, like, V}];
map_key_to_item(created, V) -> [{unit, created, eq, V}];
map_key_to_item(created_ne, V) -> [{unit, created, ne, V}];
map_key_to_item(created_gt, V) -> [{unit, created, gt, V}];
map_key_to_item(created_gte, V) -> [{unit, created, gte, V}];
map_key_to_item(created_lt, V) -> [{unit, created, lt, V}];
map_key_to_item(created_lte, V) -> [{unit, created, lte, V}];
map_key_to_item(created_after, V) -> [{unit, created, gte, V}];
map_key_to_item(created_before, V) -> [{unit, created, lt, V}];
map_key_to_item(modified, V) -> [{unit, modified, eq, V}];
map_key_to_item(modified_ne, V) -> [{unit, modified, ne, V}];
map_key_to_item(modified_gt, V) -> [{unit, modified, gt, V}];
map_key_to_item(modified_gte, V) -> [{unit, modified, gte, V}];
map_key_to_item(modified_lt, V) -> [{unit, modified, lt, V}];
map_key_to_item(modified_lte, V) -> [{unit, modified, lte, V}];
map_key_to_item(modified_after, V) -> [{unit, modified, gte, V}];
map_key_to_item(modified_before, V) -> [{unit, modified, lt, V}];
map_key_to_item(unitver, V) -> [{unit, unitver, eq, V}];
map_key_to_item(_, _) -> [].

-spec item_to_map(ipto_search_ast:search_item()) -> map().
item_to_map({unit, Col, Op, Val}) ->
    #{normalize_map_key(Col, Op) => Val};
item_to_map(_) ->
    #{}.
