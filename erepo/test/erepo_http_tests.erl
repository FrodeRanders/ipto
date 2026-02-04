-module(erepo_http_tests).

-include_lib("eunit/include/eunit.hrl").

parse_payload_ok_test() ->
    Payload = json:encode(#{query => <<"{ __typename }">>, variables => #{}}),
    {ok, <<"{ __typename }">>, Vars, Ctx} = erepo_http_graphql_handler:parse_payload(unicode:characters_to_binary(Payload)),
    #{} = Vars,
    #{} = Ctx.

parse_payload_missing_query_test() ->
    Payload = json:encode(#{variables => #{}}),
    {error, missing_query} = erepo_http_graphql_handler:parse_payload(unicode:characters_to_binary(Payload)).

graphql_http_roundtrip_test_() ->
    case {code:ensure_loaded(cowboy), code:ensure_loaded(graphql), can_bind_socket()} of
        {{module, cowboy}, {module, graphql}, true} ->
            {setup,
                fun setup_http_graphql/0,
                fun cleanup_http_graphql/1,
                fun graphql_http_roundtrip/1};
        _ ->
            {"cowboy/graphql deps or local socket bind unavailable in this profile", fun() -> ok end}
    end.

setup_http_graphql() ->
    ok = ensure_inets_started(),
    application:set_env(erepo, backend, memory),
    ok = erepo_graphql:reload_schema(),
    Port = reserve_free_port(),
    case erepo_http:start(#{port => Port}) of
        {ok, already_started} ->
            ok = erepo_http:stop(),
            {ok, _} = erepo_http:start(#{port => Port}),
            Port;
        {ok, _} -> Port;
        Error ->
            erlang:error({http_start_failed, Error})
    end.

cleanup_http_graphql(_Port) ->
    _ = erepo_http:stop(),
    ok.

graphql_http_roundtrip(Port) ->
    Body = unicode:characters_to_binary(json:encode(#{query => <<"{ __typename }">>, variables => #{}})),
    Url = "http://127.0.0.1:" ++ integer_to_list(Port) ++ "/graphql",
    Req = {Url, [{"content-type", "application/json"}], "application/json", binary_to_list(Body)},
    {ok, {{_HttpVsn, 200, _Reason}, _Headers, RespBody0}} = httpc:request(post, Req, [], []),
    RespBody = to_binary(RespBody0),
    Decoded = json:decode(RespBody),
    ?assertMatch(#{<<"data">> := _}, Decoded).

ensure_inets_started() ->
    case application:ensure_all_started(inets) of
        {ok, _} -> ok;
        {error, {already_started, inets}} -> ok;
        {error, {already_started, _Other}} -> ok;
        Error -> erlang:error({inets_start_failed, Error})
    end.

reserve_free_port() ->
    {ok, Socket} = gen_tcp:listen(0, [binary, {active, false}, {reuseaddr, true}]),
    {ok, {_Ip, Port}} = inet:sockname(Socket),
    ok = gen_tcp:close(Socket),
    Port.

can_bind_socket() ->
    case gen_tcp:listen(0, [binary, {active, false}, {reuseaddr, true}]) of
        {ok, Socket} ->
            ok = gen_tcp:close(Socket),
            true;
        _ ->
            false
    end.

to_binary(V) when is_binary(V) -> V;
to_binary(V) when is_list(V) -> unicode:characters_to_binary(V).
