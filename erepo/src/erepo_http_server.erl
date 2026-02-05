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
-module(erepo_http_server).
-behaviour(gen_server).

-include("erepo.hrl").

-export([start_link/0, refresh/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3, handle_continue/2]).

-type http_server_state() :: #{running := boolean(), port := non_neg_integer() | undefined}.
-type http_reply() :: ok | {error, erepo_reason()}.
-type http_call_msg() :: refresh.

-spec start_link() -> gen_server:start_ret().
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec refresh() -> http_reply().
refresh() ->
    gen_server:call(?MODULE, refresh).

-spec init(list()) -> {ok, http_server_state(), {continue, ensure_http}}.
init([]) ->
    {ok, #{running => false, port => undefined}, {continue, ensure_http}}.

-spec handle_call(http_call_msg() | any(), gen_server:from(), http_server_state()) ->
    {reply, http_reply() | {error, bad_request}, http_server_state()}.
handle_call(refresh, _From, State0) ->
    {Reply, State} = ensure_http_state(State0),
    {reply, Reply, State};
handle_call(_Request, _From, State) ->
    {reply, {error, bad_request}, State}.

-spec handle_cast(any(), http_server_state()) -> {noreply, http_server_state()}.
handle_cast(_Msg, State) ->
    {noreply, State}.

-spec handle_info(any(), http_server_state()) -> {noreply, http_server_state()}.
handle_info(_Info, State) ->
    {noreply, State}.

-spec terminate(any(), http_server_state()) -> ok.
terminate(_Reason, State) ->
    case maps:get(running, State, false) of
        true -> _ = erepo_http:stop();
        false -> ok
    end,
    ok.

-spec code_change(any(), http_server_state(), any()) -> {ok, http_server_state()}.
code_change(_OldVsn, State, _Extra) ->
    {_Reply, NewState} = ensure_http_state(State),
    {ok, NewState}.

-spec handle_continue(ensure_http, http_server_state()) -> {noreply, http_server_state()}.
handle_continue(ensure_http, State0) ->
    {_Reply, State} = ensure_http_state(State0),
    {noreply, State}.

ensure_http_state(State0) ->
    Enabled = http_enabled(),
    TargetPort = http_port(),
    Running = maps:get(running, State0, false),
    CurrentPort = maps:get(port, State0, undefined),
    case {Enabled, Running, CurrentPort =:= TargetPort} of
        {false, false, _} ->
            {ok, State0#{running => false, port => undefined}};
        {false, true, _} ->
            _ = erepo_http:stop(),
            {ok, State0#{running => false, port => undefined}};
        {true, true, true} ->
            {ok, State0};
        {true, true, false} ->
            _ = erepo_http:stop(),
            case erepo_http:start(#{port => TargetPort}) of
                {ok, _} ->
                    {ok, State0#{running => true, port => TargetPort}};
                Error ->
                    logger:warning("erepo http restart failed: ~p", [Error]),
                    {Error, State0#{running => false, port => undefined}}
            end;
        {true, false, _} ->
            case erepo_http:start(#{port => TargetPort}) of
                {ok, _} ->
                    {ok, State0#{running => true, port => TargetPort}};
                Error ->
                    logger:warning("erepo http start failed: ~p", [Error]),
                    {Error, State0#{running => false, port => undefined}}
            end
    end.

http_enabled() ->
    case os:getenv("EREPO_HTTP_ENABLED") of
        false ->
            application:get_env(erepo, http_enabled, false);
        Value ->
            to_bool(Value, false)
    end.

http_port() ->
    case os:getenv("EREPO_HTTP_PORT") of
        false ->
            application:get_env(erepo, http_port, 8080);
        Value ->
            to_int(Value, application:get_env(erepo, http_port, 8080))
    end.

to_bool(V, Default) when is_binary(V) ->
    to_bool(binary_to_list(V), Default);
to_bool(V, _Default) when is_list(V) ->
    Lower = string:lowercase(string:trim(V)),
    case Lower of
        "1" -> true;
        "true" -> true;
        "yes" -> true;
        "on" -> true;
        "0" -> false;
        "false" -> false;
        "no" -> false;
        "off" -> false;
        _ -> false
    end;
to_bool(_, Default) ->
    Default.

to_int(V, Default) when is_binary(V) ->
    to_int(binary_to_list(V), Default);
to_int(V, Default) when is_list(V) ->
    case string:to_integer(string:trim(V)) of
        {I, _} -> I;
        _ -> Default
    end;
to_int(_, Default) ->
    Default.
