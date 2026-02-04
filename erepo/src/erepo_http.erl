-module(erepo_http).

-export([start/0, start/1, stop/0]).

start() ->
    Port = env_int("EREPO_HTTP_PORT", 8080),
    start(#{port => Port}).

start(Opts) when is_map(Opts) ->
    case code:ensure_loaded(cowboy) of
        {module, cowboy} ->
            Port = maps:get(port, Opts, 8080),
            Dispatch = cowboy_router:compile([
                {'_', [
                    {"/graphql", erepo_http_graphql_handler, #{}},
                    {"/health", erepo_http_health_handler, #{}}
                ]}
            ]),
            TransOpts = [{ip, {0, 0, 0, 0}}, {port, Port}],
            ProtoOpts = #{env => #{dispatch => Dispatch}},
            case cowboy:start_clear(erepo_http_listener, TransOpts, ProtoOpts) of
                {ok, _Pid} -> {ok, #{port => Port}};
                {error, {already_started, _Pid}} -> {ok, already_started};
                Error -> Error
            end;
        _ ->
            {error, {missing_dependency, cowboy}}
    end;
start(_Opts) ->
    {error, invalid_options}.

stop() ->
    case code:ensure_loaded(cowboy) of
        {module, cowboy} ->
            cowboy:stop_listener(erepo_http_listener);
        _ ->
            ok
    end.

env_int(Name, Default) ->
    case os:getenv(Name) of
        false -> Default;
        Value ->
            case string:to_integer(Value) of
                {I, _} -> I;
                _ -> Default
            end
    end.
