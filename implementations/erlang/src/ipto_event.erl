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
-module(ipto_event).
-behaviour(gen_server).

-export([start_link/0, emit/2, add_listener/1, remove_listener/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-type listener_list() :: [pid()].
-type event_state() :: #{listeners := listener_list()}.
-type event_action() :: atom() | binary() | string().
-type event_payload() :: any().
-type event_call_msg() :: {add_listener, pid()} | {remove_listener, pid()}.
-type event_cast_msg() :: {emit, event_action(), event_payload()}.
-type event_reply() :: ok | {error, bad_request}.

-spec start_link() -> gen_server:start_ret().
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec emit(event_action(), event_payload()) -> ok.
emit(Action, Payload) ->
    gen_server:cast(?MODULE, {emit, Action, Payload}).

-spec add_listener(pid()) -> ok | {error, invalid_listener}.
add_listener(ListenerPid) when is_pid(ListenerPid) ->
    gen_server:call(?MODULE, {add_listener, ListenerPid});
add_listener(_ListenerPid) ->
    {error, invalid_listener}.

-spec remove_listener(pid()) -> ok | {error, invalid_listener}.
remove_listener(ListenerPid) when is_pid(ListenerPid) ->
    gen_server:call(?MODULE, {remove_listener, ListenerPid});
remove_listener(_ListenerPid) ->
    {error, invalid_listener}.

-spec init(list()) -> {ok, event_state()}.
init([]) ->
    {ok, #{listeners => []}}.

-spec handle_call(event_call_msg() | any(), gen_server:from(), event_state()) ->
    {reply, event_reply(), event_state()}.
handle_call({add_listener, ListenerPid}, _From, State) ->
    Listeners = maps:get(listeners, State),
    NewListeners = lists:usort([ListenerPid | Listeners]),
    {reply, ok, State#{listeners => NewListeners}};
handle_call({remove_listener, ListenerPid}, _From, State) ->
    Listeners = maps:get(listeners, State),
    NewListeners = [Pid || Pid <- Listeners, Pid =/= ListenerPid],
    {reply, ok, State#{listeners => NewListeners}};
handle_call(_Request, _From, State) ->
    {reply, {error, bad_request}, State}.

-spec handle_cast(event_cast_msg() | any(), event_state()) -> {noreply, event_state()}.
handle_cast({emit, Action, Payload}, State) ->
    Event = #{action => Action, payload => Payload},
    [Pid ! {ipto_event, Event} || Pid <- maps:get(listeners, State)],
    {noreply, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

-spec handle_info(any(), event_state()) -> {noreply, event_state()}.
handle_info(_Info, State) ->
    {noreply, State}.

-spec terminate(any(), event_state()) -> ok.
terminate(_Reason, _State) ->
    ok.

-spec code_change(any(), event_state(), any()) -> {ok, event_state()}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
