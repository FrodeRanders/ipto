-module(erepo_http_graphql_handler).

-export([init/2, parse_payload/1]).

init(Req0, State) ->
    Method = cowboy_req:method(Req0),
    case Method of
        <<"POST">> ->
            {ok, Body, Req1} = cowboy_req:read_body(Req0),
            case parse_payload(Body) of
                {ok, Query, Variables, Context} ->
                    case erepo_graphql:execute(Query, Variables, Context) of
                        {ok, Result} ->
                            Json = encode_json(#{data => Result}),
                            Req2 = cowboy_req:reply(200, #{<<"content-type">> => <<"application/json">>}, Json, Req1),
                            {ok, Req2, State};
                        {error, Reason} ->
                            Json = encode_json(error_response(Reason)),
                            Req2 = cowboy_req:reply(400, #{<<"content-type">> => <<"application/json">>}, Json, Req1),
                            {ok, Req2, State}
                    end;
                {error, Reason} ->
                    Json = encode_json(error_response(Reason)),
                    Req2 = cowboy_req:reply(400, #{<<"content-type">> => <<"application/json">>}, Json, Req1),
                    {ok, Req2, State}
            end;
        _ ->
            Req1 = cowboy_req:reply(405, #{<<"content-type">> => <<"application/json">>}, encode_json(error_response(method_not_allowed)), Req0),
            {ok, Req1, State}
    end.

parse_payload(Body) when is_binary(Body) ->
    try
        Payload = json:decode(Body),
        Query = maps:get(<<"query">>, Payload, maps:get(query, Payload, undefined)),
        Variables = maps:get(<<"variables">>, Payload, maps:get(variables, Payload, #{})),
        Context = maps:get(<<"context">>, Payload, maps:get(context, Payload, #{})),
        case Query of
            undefined ->
                {error, missing_query};
            _ ->
                {ok, Query, normalize_map(Variables), normalize_map(Context)}
        end
    catch
        _:Reason -> {error, {invalid_json, Reason}}
    end;
parse_payload(_Body) ->
    {error, invalid_body}.

normalize_map(M) when is_map(M) ->
    M;
normalize_map(_) ->
    #{}.

error_response(Reason) ->
    #{errors => [#{message => iolist_to_binary(io_lib:format("~p", [Reason]))}]}. 

encode_json(Value) ->
    unicode:characters_to_binary(json:encode(normalize_json(Value))).

normalize_json(V) when is_map(V) ->
    maps:from_list([{json_key(K), normalize_json(Val)} || {K, Val} <- maps:to_list(V)]);
normalize_json(V) when is_list(V) ->
    [normalize_json(E) || E <- V];
normalize_json(V) ->
    V.

json_key(K) when is_atom(K) -> atom_to_binary(K, utf8);
json_key(K) when is_list(K) -> unicode:characters_to_binary(K);
json_key(K) -> K.
