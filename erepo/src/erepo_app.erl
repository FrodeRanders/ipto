-module(erepo_app).
-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    ok = erepo_graphql_config:init(),
    erepo_sup:start_link().

stop(_State) ->
    ok.
