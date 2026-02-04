-module(erepo_sup).
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

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
