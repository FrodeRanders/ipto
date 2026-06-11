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
-module(ipto_db_utils).

-include("ipto.hrl").

-export([
    normalize_ref/1,
    normalize_string/1,
    normalize_purpose/1,
    row_values/1,
    like_match/2,
    like_pattern_to_regex/1,
    like_pattern_char/1,
    compare_created/3,
    to_int_maybe/1,
    compare_int/3,
    compare_text/3,
    validate_store_input/1,
    normalize_order/1,
    normalize_search_expression/1,
    is_proplist/1,
    drop_n/2,
    take_n/2,
    get_meta_map/1,
    next_attr_id/0
]).

-define(ATTR_SEQ_KEY, {meta, attr_seq}).
-define(ATTR_BY_NAME_KEY, {meta, attrs_by_name}).
-define(ATTR_BY_ID_KEY, {meta, attrs_by_id}).
-define(REL_KEY, {meta, relations}).
-define(ASSOC_KEY, {meta, associations}).
-define(LOCK_KEY, {meta, locks}).

-spec normalize_ref(unit_ref_value() | unit_ref_tuple() | unit_ref_map() | term()) -> unit_ref_tuple() | invalid_ref.
normalize_ref(#unit_ref{tenantid = TenantId, unitid = UnitId}) ->
    {TenantId, UnitId};
normalize_ref(#{tenantid := TenantId, unitid := UnitId}) ->
    {TenantId, UnitId};
normalize_ref({TenantId, UnitId}) ->
    {TenantId, UnitId};
normalize_ref(_) ->
    invalid_ref.

-spec normalize_string(term()) -> binary().
normalize_string(Value) when is_binary(Value) -> Value;
normalize_string(Value) when is_list(Value) -> unicode:characters_to_binary(Value);
normalize_string(Value) -> unicode:characters_to_binary(io_lib:format("~p", [Value])).

-spec normalize_purpose(term()) -> binary().
normalize_purpose(undefined) -> <<>>;
normalize_purpose(null) -> <<>>;
normalize_purpose(Value) when is_binary(Value) -> Value;
normalize_purpose(Value) when is_list(Value) -> unicode:characters_to_binary(Value);
normalize_purpose(Value) -> unicode:characters_to_binary(io_lib:format("~p", [Value])).

-spec row_values(list() | tuple()) -> list().
row_values(Row) when is_tuple(Row) -> tuple_to_list(Row);
row_values(Row) when is_list(Row) -> Row.

-spec like_match(binary() | string(), binary() | string()) -> boolean().
like_match(Text0, Pattern0) ->
    Text = unicode:characters_to_list(normalize_string(Text0)),
    Pattern = unicode:characters_to_list(normalize_string(Pattern0)),
    case re:run(Text, like_pattern_to_regex(Pattern), [{capture, none}, unicode]) of
        match -> true;
        nomatch -> false
    end.

-spec like_pattern_to_regex(string()) -> string().
like_pattern_to_regex(Pattern) ->
    "^" ++ lists:flatten([like_pattern_char(C) || C <- Pattern]) ++ "$".

-spec like_pattern_char(char()) -> string().
like_pattern_char($%) ->
    ".*";
like_pattern_char($_) ->
    ".";
like_pattern_char(C) when C =:= $.; C =:= $^; C =:= $$; C =:= $*; C =:= $+; C =:= $?; C =:= $(; C =:= $); C =:= $[; C =:= $]; C =:= ${; C =:= $}; C =:= $|; C =:= $\\ ->
    [$\\, C];
like_pattern_char(C) ->
    [C].

-spec compare_created(term(), term(), eq | ne | gt | ge | lt | le) -> boolean().
compare_created(Created, FilterValue, Op) ->
    case to_int_maybe(FilterValue) of
        {ok, N} ->
            compare_int(Created, N, Op);
        error ->
            compare_text(normalize_string(Created), normalize_string(FilterValue), Op)
    end.

-spec to_int_maybe(term()) -> {ok, integer()} | error.
to_int_maybe(Value) when is_integer(Value) ->
    {ok, Value};
to_int_maybe(Value) when is_binary(Value) ->
    to_int_maybe(binary_to_list(Value));
to_int_maybe(Value) when is_list(Value) ->
    case string:to_integer(string:trim(Value)) of
        {I, _} -> {ok, I};
        _ -> error
    end;
to_int_maybe(_) ->
    error.

-spec compare_int(integer(), integer(), eq | ne | gt | ge | lt | le) -> boolean().
compare_int(A, B, eq) -> A =:= B;
compare_int(A, B, ne) -> A =/= B;
compare_int(A, B, gt) -> A > B;
compare_int(A, B, ge) -> A >= B;
compare_int(A, B, lt) -> A < B;
compare_int(A, B, le) -> A =< B.

-spec compare_text(term(), term(), eq | ne | gt | ge | lt | le) -> boolean().
compare_text(A, B, eq) -> A =:= B;
compare_text(A, B, ne) -> A =/= B;
compare_text(A, B, gt) -> A > B;
compare_text(A, B, ge) -> A >= B;
compare_text(A, B, lt) -> A < B;
compare_text(A, B, le) -> A =< B.

-spec validate_store_input(unit_map()) -> ok | {error, ipto_reason()}.
validate_store_input(UnitMap) ->
    case maps:is_key(tenantid, UnitMap) of
        true -> ok;
        false -> {error, missing_tenantid}
    end.

-spec normalize_order(search_order() | map() | tuple()) -> {atom(), asc | desc}.
normalize_order(#{field := Field, dir := Dir}) ->
    normalize_order({Field, Dir});
normalize_order({Field, Dir}) ->
    NormField =
        case Field of
            created -> created;
            modified -> modified;
            unitid -> unitid;
            status -> status;
            _ -> created
        end,
    NormDir =
        case Dir of
            asc -> asc;
            'ASC' -> asc;
            _ -> desc
        end,
    {NormField, NormDir};
normalize_order(_) ->
    {created, desc}.

-spec normalize_search_expression(search_expression() | map() | list() | undefined) ->
    {ok, map()} | {error, ipto_reason()}.
normalize_search_expression(undefined) ->
    {ok, #{}};
normalize_search_expression(Expression) when is_map(Expression) ->
    {ok, Expression};
normalize_search_expression(Expression) when is_list(Expression) ->
    case is_proplist(Expression) of
        true -> {ok, maps:from_list(Expression)};
        false ->
            case ipto_search_parser:parse(Expression) of
                {ok, Parsed} -> {ok, Parsed};
                Error -> Error
            end
    end;
normalize_search_expression(Expression) when is_binary(Expression) ->
    ipto_search_parser:parse(Expression);
normalize_search_expression(_) ->
    {error, invalid_query}.

-spec is_proplist(term()) -> boolean().
is_proplist([]) ->
    true;
is_proplist([{Key, _Value} | Rest]) when is_atom(Key); is_binary(Key) ->
    is_proplist(Rest);
is_proplist(_) ->
    false.

-spec drop_n([term()], integer()) -> [term()].
drop_n(List, N) when N =< 0 ->
    List;
drop_n([], _N) ->
    [];
drop_n([_ | Rest], N) ->
    drop_n(Rest, N - 1).

-spec take_n([term()], integer()) -> [term()].
take_n(_List, N) when N =< 0 ->
    [];
take_n([], _N) ->
    [];
take_n([H | T], N) ->
    [H | take_n(T, N - 1)].

-spec get_meta_map(term()) -> map().
get_meta_map(Key) ->
    case ipto_cache:get(Key) of
        undefined -> #{};
        Map when is_map(Map) -> Map
    end.

-spec next_attr_id() -> pos_integer().
next_attr_id() ->
    Seq0 =
        case ipto_cache:get(?ATTR_SEQ_KEY) of
            undefined -> 0;
            ExistingSeq -> ExistingSeq
        end,
    Seq = Seq0 + 1,
    ipto_cache:put(?ATTR_SEQ_KEY, Seq),
    Seq.
