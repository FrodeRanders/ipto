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
-module(ipto_pg_pool).
-behaviour(gen_server).

-export([start_link/0, with_connection/1, get_stats/0, reset_stats/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-type waiter() :: {gen_server:from(), integer()}.
-type telemetry() :: #{
    checkouts := non_neg_integer(),
    immediate_checkouts := non_neg_integer(),
    queued_checkouts := non_neg_integer(),
    checkins := non_neg_integer(),
    max_queue_depth := non_neg_integer(),
    wait_time := map()
}.
-type state() :: #{
    enabled := boolean(),
    available := [term()],
    waiters := queue:queue(waiter()),
    size := non_neg_integer(),
    telemetry := telemetry()
}.

-spec start_link() -> gen_server:start_ret().
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec with_connection(fun((term()) -> T)) -> T | {error, pool_unavailable} | {error, term()}.
with_connection(Fun) when is_function(Fun, 1) ->
    case whereis(?MODULE) of
        undefined ->
            {error, pool_unavailable};
        _ ->
            case gen_server:call(?MODULE, checkout, 60000) of
                {ok, Conn} ->
                    try
                        Fun(Conn)
                    catch
                        Class:Reason:Stacktrace ->
                            ipto_log:error(
                                ipto_pg_pool,
                                "pooled pg operation crashed class=~p reason=~p stacktrace=~p",
                                [Class, Reason, Stacktrace]
                            ),
                            erlang:raise(Class, Reason, Stacktrace)
                    after
                        gen_server:cast(?MODULE, {checkin, Conn})
                    end;
                Error ->
                    Error
            end
    end.

-spec get_stats() -> map().
get_stats() ->
    case whereis(?MODULE) of
        undefined ->
            #{enabled => false};
        _ ->
            gen_server:call(?MODULE, get_stats)
    end.

-spec reset_stats() -> ok.
reset_stats() ->
    case whereis(?MODULE) of
        undefined ->
            ok;
        _ ->
            gen_server:call(?MODULE, reset_stats)
    end.

-spec init(list()) -> {ok, state()}.
init([]) ->
    case maybe_enable(new_state()) of
        {ok, State} -> {ok, State};
        {error, _} -> {ok, new_state()}
    end.

-spec handle_call(term(), gen_server:from(), state()) -> {reply, term(), state()} | {noreply, state()}.
handle_call(checkout, From, State0) ->
    State1 = ensure_enabled(State0),
    case State1 of
        #{enabled := false} ->
            {reply, {error, pool_unavailable}, State1};
        #{available := [Conn | Rest]} ->
            Telemetry0 = maps:get(telemetry, State1),
            Telemetry = inc_telemetry(immediate_checkouts, inc_telemetry(checkouts, Telemetry0)),
            {reply, {ok, Conn}, State1#{available => Rest, telemetry => Telemetry}};
        _ ->
            Waiters0 = maps:get(waiters, State1),
            EnqueuedAt = erlang:monotonic_time(nanosecond),
            Waiters = queue:in({From, EnqueuedAt}, Waiters0),
            Depth = queue:len(Waiters),
            Telemetry0 = maps:get(telemetry, State1),
            Telemetry1 = inc_telemetry(queued_checkouts, Telemetry0),
            Telemetry = update_max_queue_depth(Depth, Telemetry1),
            {noreply, State1#{waiters => Waiters, telemetry => Telemetry}}
    end;
handle_call(get_stats, _From, State) ->
    {reply, format_stats(State), State};
handle_call(reset_stats, _From, State0) ->
    {reply, ok, State0#{telemetry => fresh_telemetry()}};
handle_call(_Request, _From, State) ->
    {reply, {error, bad_request}, State}.

-spec handle_cast(term(), state()) -> {noreply, state()}.
handle_cast({checkin, Conn}, State0 = #{enabled := false}) ->
    safe_close(Conn),
    {noreply, State0};
handle_cast({checkin, Conn}, State0) ->
    Waiters0 = maps:get(waiters, State0),
    Telemetry0 = inc_telemetry(checkins, maps:get(telemetry, State0)),
    case queue:out(Waiters0) of
        {{value, {NextFrom, EnqueuedAt}}, Waiters} ->
            WaitMs = wait_ms_since(EnqueuedAt),
            Telemetry = add_wait_sample(WaitMs, Telemetry0),
            gen_server:reply(NextFrom, {ok, Conn}),
            {noreply, State0#{waiters => Waiters, telemetry => Telemetry}};
        {empty, _} ->
            Available = maps:get(available, State0),
            {noreply, State0#{available => [Conn | Available], telemetry => Telemetry0}}
    end;
handle_cast(_Msg, State) ->
    {noreply, State}.

-spec handle_info(term(), state()) -> {noreply, state()}.
handle_info(_Info, State) ->
    {noreply, State}.

-spec terminate(term(), state()) -> ok.
terminate(_Reason, State) ->
    Available = maps:get(available, State, []),
    [safe_close(Conn) || Conn <- Available],
    ok.

-spec code_change(term(), state(), term()) -> {ok, state()}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

-spec new_state() -> state().
new_state() ->
    # {
        enabled => false,
        available => [],
        waiters => queue:new(),
        size => 0,
        telemetry => fresh_telemetry()
    }.

-spec ensure_enabled(state()) -> state().
ensure_enabled(State = #{enabled := true}) ->
    State;
ensure_enabled(State0) ->
    case maybe_enable(State0) of
        {ok, State} -> State;
        {error, _} -> State0
    end.

-spec maybe_enable(state()) -> {ok, state()} | {error, term()}.
maybe_enable(State0) ->
    case pg_backend_enabled() of
        false ->
            {error, backend_not_pg};
        true ->
            case code:ensure_loaded(epgsql) of
                {module, epgsql} ->
                    case connect_many(pool_size(), []) of
                        [] ->
                            ipto_log:warning(ipto_pg_pool, "postgres pool unavailable, no connections created", []),
                            {error, no_connections};
                        Conns ->
                            Size = length(Conns),
                            ipto_log:notice(ipto_pg_pool, "postgres pool enabled size=~p", [Size]),
                            {ok, State0#{enabled => true, available => Conns, size => Size}}
                    end;
                _ ->
                    ipto_log:warning(ipto_pg_pool, "postgres pool disabled, epgsql dependency not loaded", []),
                    {error, missing_epgsql}
            end
    end.

-spec pg_backend_enabled() -> boolean().
pg_backend_enabled() ->
    case application:get_env(ipto, backend) of
        {ok, pg} -> true;
        {ok, postgres} -> true;
        _ -> false
    end.

-spec pool_size() -> pos_integer().
pool_size() ->
    Default = erlang:max(2, erlang:system_info(schedulers_online)),
    case os:getenv("IPTO_PG_POOL_SIZE") of
        false ->
            Default;
        Value ->
            case string:to_integer(Value) of
                {I, _} when I > 0 -> I;
                _ -> Default
            end
    end.

-spec connect_many(non_neg_integer(), [term()]) -> [term()].
connect_many(0, Acc) ->
    Acc;
connect_many(N, Acc) ->
    case connect_one() of
        {ok, Conn} ->
            connect_many(N - 1, [Conn | Acc]);
        {error, Reason} ->
            ipto_log:warning(ipto_pg_pool, "failed creating pooled postgres connection reason=~p", [Reason]),
            connect_many(N - 1, Acc)
    end.

-spec connect_one() -> {ok, term()} | {error, term()}.
connect_one() ->
    Opts = pg_conn_opts(),
    Host = maps:get(host, Opts),
    User = maps:get(user, Opts),
    Pass = maps:get(pass, Opts),
    ConnOpts = [{database, maps:get(db, Opts)}, {port, maps:get(port, Opts)}],
    case epgsql:connect(Host, User, Pass, ConnOpts) of
        {ok, Conn} ->
            {ok, Conn};
        Error ->
            {error, Error}
    end.

-spec safe_close(term()) -> ok.
safe_close(Conn) ->
    _ = catch epgsql:close(Conn),
    ok.

-spec fresh_telemetry() -> telemetry().
fresh_telemetry() ->
    # {
        checkouts => 0,
        immediate_checkouts => 0,
        queued_checkouts => 0,
        checkins => 0,
        max_queue_depth => 0,
        wait_time => ipto_running_statistics:new()
    }.

-spec inc_telemetry(atom(), telemetry()) -> telemetry().
inc_telemetry(Key, Telemetry0) ->
    Telemetry0#{Key => maps:get(Key, Telemetry0, 0) + 1}.

-spec update_max_queue_depth(non_neg_integer(), telemetry()) -> telemetry().
update_max_queue_depth(Depth, Telemetry0) ->
    Current = maps:get(max_queue_depth, Telemetry0, 0),
    Telemetry0#{max_queue_depth => erlang:max(Depth, Current)}.

-spec add_wait_sample(number(), telemetry()) -> telemetry().
add_wait_sample(Ms, Telemetry0) ->
    WaitStats0 = maps:get(wait_time, Telemetry0),
    WaitStats = ipto_running_statistics:add_sample(WaitStats0, Ms),
    Telemetry0#{wait_time => WaitStats}.

-spec wait_ms_since(integer()) -> float().
wait_ms_since(EnqueuedAtNs) ->
    (erlang:monotonic_time(nanosecond) - EnqueuedAtNs) / 1000000.0.

-spec format_stats(state()) -> map().
format_stats(State) ->
    Telemetry = maps:get(telemetry, State),
    Waiters = maps:get(waiters, State),
    Available = maps:get(available, State),
    Size = maps:get(size, State),
    # {
        enabled => maps:get(enabled, State),
        size => Size,
        available => length(Available),
        in_use => erlang:max(0, Size - length(Available)),
        queue_depth => queue:len(Waiters),
        max_queue_depth => maps:get(max_queue_depth, Telemetry),
        checkouts => maps:get(checkouts, Telemetry),
        immediate_checkouts => maps:get(immediate_checkouts, Telemetry),
        queued_checkouts => maps:get(queued_checkouts, Telemetry),
        checkins => maps:get(checkins, Telemetry),
        checkout_wait_ms => ipto_running_statistics:to_map(maps:get(wait_time, Telemetry))
    }.

-spec pg_conn_opts() -> map().
pg_conn_opts() ->
    Host = env_str("IPTO_PG_HOST", "localhost"),
    User = env_str("IPTO_PG_USER", "repo"),
    Pass = env_str("IPTO_PG_PASSWORD", "repo"),
    Db = env_str("IPTO_PG_DATABASE", "repo"),
    Port = env_int("IPTO_PG_PORT", 5432),
    #{host => Host, user => User, pass => Pass, db => Db, port => Port}.

-spec env_str(string() | binary(), string()) -> string().
env_str(Name, Default) ->
    case os:getenv(Name) of
        false -> Default;
        Value -> Value
    end.

-spec env_int(string() | binary(), integer()) -> integer().
env_int(Name, Default) ->
    case os:getenv(Name) of
        false -> Default;
        Value ->
            case string:to_integer(Value) of
                {I, _} -> I;
                _ -> Default
            end
    end.
