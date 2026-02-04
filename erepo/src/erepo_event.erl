-module(erepo_event).
-behaviour(gen_server).

-export([start_link/0, emit/2, add_listener/1, remove_listener/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

emit(Action, Payload) ->
    gen_server:cast(?MODULE, {emit, Action, Payload}).

add_listener(ListenerPid) when is_pid(ListenerPid) ->
    gen_server:call(?MODULE, {add_listener, ListenerPid});
add_listener(_ListenerPid) ->
    {error, invalid_listener}.

remove_listener(ListenerPid) when is_pid(ListenerPid) ->
    gen_server:call(?MODULE, {remove_listener, ListenerPid});
remove_listener(_ListenerPid) ->
    {error, invalid_listener}.

init([]) ->
    {ok, #{listeners => []}}.

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

handle_cast({emit, Action, Payload}, State) ->
    Event = #{action => Action, payload => Payload},
    [Pid ! {erepo_event, Event} || Pid <- maps:get(listeners, State)],
    {noreply, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
