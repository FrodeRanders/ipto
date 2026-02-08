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
-module(ipto_timing_data).
-behaviour(gen_server).

-export([start_link/0, add_sample/2, reset/0, get_stats/0, report/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-type measurement() :: atom() | binary() | string().
-type state() :: #{binary() => map()}.

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec add_sample(measurement(), number()) -> ok.
add_sample(Name, Sample) ->
    try
        gen_server:cast(?MODULE, {add_sample, normalize_name(Name), Sample}),
        ok
    catch
        _:_ ->
            ok
    end.

-spec reset() -> ok.
reset() ->
    case whereis(?MODULE) of
        undefined -> ok;
        _ -> gen_server:call(?MODULE, reset)
    end.

-spec get_stats() -> map().
get_stats() ->
    case whereis(?MODULE) of
        undefined -> #{};
        _ -> gen_server:call(?MODULE, get)
    end.

-spec report() -> binary().
report() ->
    Stats = get_stats(),
    unicode:characters_to_binary(render_table(Stats)).

-spec init(list()) -> {ok, state()}.
init([]) ->
    {ok, #{}}.

-spec handle_call(term(), gen_server:from(), state()) -> {reply, term(), state()}.
handle_call(reset, _From, _State) ->
    {reply, ok, #{}};
handle_call(get, _From, State) ->
    Result = maps:fold(
        fun(Key, RunningStats, Acc) ->
            Acc#{Key => ipto_running_statistics:to_map(RunningStats)}
        end,
        #{},
        State
    ),
    {reply, Result, State};
handle_call(_Request, _From, State) ->
    {reply, {error, bad_request}, State}.

-spec handle_cast(term(), state()) -> {noreply, state()}.
handle_cast({add_sample, Name, Sample}, State0) ->
    Stats0 = maps:get(Name, State0, ipto_running_statistics:new()),
    Stats = ipto_running_statistics:add_sample(Stats0, Sample),
    {noreply, State0#{Name => Stats}};
handle_cast(_Msg, State) ->
    {noreply, State}.

-spec handle_info(term(), state()) -> {noreply, state()}.
handle_info(_Info, State) ->
    {noreply, State}.

-spec terminate(term(), state()) -> ok.
terminate(_Reason, _State) ->
    ok.

-spec code_change(term(), state(), term()) -> {ok, state()}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

-spec normalize_name(measurement()) -> binary().
normalize_name(Name) when is_atom(Name) ->
    atom_to_binary(Name, utf8);
normalize_name(Name) when is_binary(Name) ->
    Name;
normalize_name(Name) when is_list(Name) ->
    unicode:characters_to_binary(Name).

-spec render_table(map()) -> string().
render_table(Stats) when map_size(Stats) =:= 0 ->
    "";
render_table(Stats) ->
    Headers = ["Measurement", "Count", "Min", "Max", "Mean", "CV(%)", "Total time", "i.e."],
    Rows = [row_data(Key, maps:get(Key, Stats)) || Key <- lists:sort(maps:keys(Stats))],
    Widths = compute_widths_like_java(Headers, Rows),
    Separator = separator_line_like_java(Widths),
    HeaderLine = format_header_like_java(Headers, Widths),
    DataLines = [format_data_row_like_java(Row, Widths) || Row <- Rows],
    lists:flatten([Separator, HeaderLine, Separator, DataLines, "\n"]).

-spec row_data(binary(), map()) -> [string()].
row_data(Name, Stats) ->
    Total = maps:get(total, Stats),
    [
        binary_to_list(Name),
        integer_to_list(maps:get(count, Stats)),
        fmt_fixed(maps:get(min, Stats)),
        fmt_fixed(maps:get(max, Stats)),
        fmt_fixed(maps:get(mean, Stats)),
        fmt_fixed(maps:get(cv, Stats)),
        integer_to_list(Total),
        human_duration(Total)
    ].

-spec compute_widths_like_java([string()], [[string()]]) -> [non_neg_integer()].
compute_widths_like_java(Headers, Rows) ->
    Fluff = 5,
    Initial = [length(H) + 1 || H <- Headers],
    lists:foldl(
        fun(Row, Acc0) ->
            Acc1 = lists:zipwith(fun(Col, W) -> erlang:max(W, length(Col)) end, Row, Acc0),
            [W1, W2, W3, W4, W5, W6, W7, W8] = Acc1,
            [
                W1,
                erlang:max(W2, 1),
                erlang:max(W3, Fluff),
                erlang:max(W4, Fluff),
                erlang:max(W5, Fluff),
                erlang:max(W6, Fluff),
                erlang:max(W7, 1),
                erlang:max(W8, 1)
            ]
        end,
        Initial,
        Rows
    ).

-spec separator_line_like_java([non_neg_integer()]) -> string().
separator_line_like_java(Widths) ->
    Parts = [lists:duplicate(W + 1, $-) || W <- Widths],
    string:join(Parts, "+-") ++ "\n".

-spec format_header_like_java([string()], [non_neg_integer()]) -> string().
format_header_like_java(Headers, Widths) ->
    [H1, H2, H3, H4, H5, H6, H7, H8] = Headers,
    [W1, W2, W3, W4, W5, W6, W7, W8] = Widths,
    lists:flatten(io_lib:format(
        "~*s | ~*s | ~*s | ~*s | ~*s | ~*s | ~*s~*s~n",
        [W1, H1, W2, H2, W3, H3, W4, H4, W5, H5, W6, H6, W7, H7, W8, H8]
    )).

-spec format_data_row_like_java([string()], [non_neg_integer()]) -> string().
format_data_row_like_java(Row, Widths) ->
    [C1, C2, C3, C4, C5, C6, C7, C8] = Row,
    [W1, W2, W3, W4, W5, W6, W7, W8] = Widths,
    lists:flatten(io_lib:format(
        "~*s | ~*s | ~*s | ~*s | ~*s | ~*s | ~*s | ~*s~n",
        [W1, C1, W2, C2, W3, C3, W4, C4, W5, C5, W6, C6, W7, C7, W8, C8]
    )).

-spec fmt_fixed(number() | undefined) -> string().
fmt_fixed(undefined) ->
    "n/a";
fmt_fixed(Value) ->
    lists:flatten(io_lib:format("~.2f", [number_or_zero(Value)])).

-spec human_duration(integer()) -> string().
human_duration(Ms) when Ms < 1000 ->
    integer_to_list(Ms) ++ " ms";
human_duration(Ms) when Ms < 60000 ->
    lists:flatten(io_lib:format("~.2f s", [Ms / 1000.0]));
human_duration(Ms) when Ms < 3600000 ->
    Min = Ms div 60000,
    Sec = (Ms rem 60000) / 1000.0,
    lists:flatten(io_lib:format("~p min ~.1f s", [Min, Sec]));
human_duration(Ms) ->
    Hours = Ms div 3600000,
    Rem = Ms rem 3600000,
    Min = Rem div 60000,
    Sec = (Rem rem 60000) div 1000,
    lists:flatten(io_lib:format("~p h ~p min ~p s", [Hours, Min, Sec])).

-spec number_or_zero(number() | undefined) -> float().
number_or_zero(undefined) ->
    0.0;
number_or_zero(Value) when is_integer(Value) ->
    float(Value);
number_or_zero(Value) when is_float(Value) ->
    Value.
