-module(erepo_http_health_handler).

-export([init/2]).

init(Req0, State) ->
    Body = unicode:characters_to_binary(json:encode(#{status => <<"ok">>})),
    Req1 = cowboy_req:reply(200, #{<<"content-type">> => <<"application/json">>}, Body, Req0),
    {ok, Req1, State}.
