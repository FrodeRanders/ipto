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
-module(ipto_cache).
-behaviour(gen_server).

-export([start_link/0, get/1, put/2, flush/0, all/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-type cache_key() :: any().
-type cache_value() :: any().
-type cache_state() :: #{cache_key() => cache_value()}.
-type cache_call_msg() :: {get, cache_key()} | {put, cache_key(), cache_value()} | flush | all.
-type cache_reply() :: cache_value() | undefined | ok | cache_state() | {error, bad_request}.

-spec start_link() -> gen_server:start_ret().
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec get(cache_key()) -> cache_value() | undefined.
get(Key) ->
    gen_server:call(?MODULE, {get, Key}).

-spec put(cache_key(), cache_value()) -> ok.
put(Key, Value) ->
    gen_server:call(?MODULE, {put, Key, Value}).

-spec flush() -> ok.
flush() ->
    gen_server:call(?MODULE, flush).

-spec all() -> map().
all() ->
    gen_server:call(?MODULE, all).

-spec init(list()) -> {ok, cache_state()}.
init([]) ->
    {ok, #{}}.

-spec handle_call(cache_call_msg() | any(), gen_server:from(), cache_state()) ->
    {reply, cache_reply(), cache_state()}.
handle_call({get, Key}, _From, State) ->
    {reply, maps:get(Key, State, undefined), State};
handle_call({put, Key, Value}, _From, State) ->
    {reply, ok, State#{Key => Value}};
handle_call(flush, _From, _State) ->
    {reply, ok, #{}};
handle_call(all, _From, State) ->
    {reply, State, State};
handle_call(_Request, _From, State) ->
    {reply, {error, bad_request}, State}.

-spec handle_cast(any(), cache_state()) -> {noreply, cache_state()}.
handle_cast(_Msg, State) ->
    {noreply, State}.

-spec handle_info(any(), cache_state()) -> {noreply, cache_state()}.
handle_info(_Info, State) ->
    {noreply, State}.

-spec terminate(any(), cache_state()) -> ok.
terminate(_Reason, _State) ->
    ok.

-spec code_change(any(), cache_state(), any()) -> {ok, cache_state()}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
