-module(erepo_cache).
-behaviour(gen_server).

-export([start_link/0, get/1, put/2, flush/0, all/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

get(Key) ->
    gen_server:call(?MODULE, {get, Key}).

put(Key, Value) ->
    gen_server:call(?MODULE, {put, Key, Value}).

flush() ->
    gen_server:call(?MODULE, flush).

all() ->
    gen_server:call(?MODULE, all).

init([]) ->
    {ok, #{}}.

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

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
