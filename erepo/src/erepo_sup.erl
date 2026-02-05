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
-module(erepo_sup).
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec init(list()) -> {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.
init([]) ->
    CacheChild = #{
        id => erepo_cache,
        start => {erepo_cache, start_link, []},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [erepo_cache]
    },
    EventChild = #{
        id => erepo_event,
        start => {erepo_event, start_link, []},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [erepo_event]
    },
    HttpChild = #{
        id => erepo_http_server,
        start => {erepo_http_server, start_link, []},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [erepo_http_server]
    },
    {ok, {{one_for_one, 5, 10}, [CacheChild, EventChild, HttpChild]}}.
