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
-module(ipto_sup).
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec init(list()) -> {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.
init([]) ->
    TimingDataChild = #{
        id => ipto_timing_data,
        start => {ipto_timing_data, start_link, []},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [ipto_timing_data]
    },
    PgPoolChild = #{
        id => ipto_pg_pool,
        start => {ipto_pg_pool, start_link, []},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [ipto_pg_pool]
    },
    CacheChild = #{
        id => ipto_cache,
        start => {ipto_cache, start_link, []},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [ipto_cache]
    },
    EventChild = #{
        id => ipto_event,
        start => {ipto_event, start_link, []},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [ipto_event]
    },
    HttpChild = #{
        id => ipto_http_server,
        start => {ipto_http_server, start_link, []},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [ipto_http_server]
    },
    {ok, {{one_for_one, 5, 10}, [TimingDataChild, PgPoolChild, CacheChild, EventChild, HttpChild]}}.
