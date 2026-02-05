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
-module(erepo_http).

-export([start/0, start/1, stop/0]).

-spec start() -> {ok, map()} | {error, term()}.
start() ->
    Port = env_int("EREPO_HTTP_PORT", 8080),
    start(#{port => Port}).

-spec start(map()) -> {ok, map() | already_started} | {error, term()}.
start(Opts) when is_map(Opts) ->
    case code:ensure_loaded(cowboy) of
        {module, cowboy} ->
            Port = maps:get(port, Opts, 8080),
            Dispatch = cowboy_router:compile([
                {'_', [
                    {"/graphql", erepo_http_graphql_handler, #{}},
                    {"/health", erepo_http_health_handler, #{}}
                ]}
            ]),
            TransOpts = [{ip, {0, 0, 0, 0}}, {port, Port}],
            ProtoOpts = #{env => #{dispatch => Dispatch}},
            case cowboy:start_clear(erepo_http_listener, TransOpts, ProtoOpts) of
                {ok, _Pid} -> {ok, #{port => Port}};
                {error, {already_started, _Pid}} -> {ok, already_started};
                Error -> Error
            end;
        _ ->
            {error, {missing_dependency, cowboy}}
    end;
start(_Opts) ->
    {error, invalid_options}.

-spec stop() -> ok | {error, term()}.
stop() ->
    case code:ensure_loaded(cowboy) of
        {module, cowboy} ->
            cowboy:stop_listener(erepo_http_listener);
        _ ->
            ok
    end.

env_int(Name, Default) ->
    case os:getenv(Name) of
        false -> Default;
        Value ->
            case string:to_integer(Value) of
                {I, _} -> I;
                _ -> Default
            end
    end.
