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
-module(ipto_http_graphql_handler).

-include("ipto.hrl").

-export([init/2, parse_payload/1]).

-spec init(map(), map()) -> {ok, map(), map()}.
init(Req0, State) ->
    Method = cowboy_req:method(Req0),
    case Method of
        <<"POST">> ->
            {ok, Body, Req1} = cowboy_req:read_body(Req0),
            case parse_payload(Body) of
                {ok, Query, Variables, Context} ->
                    case ipto_graphql:execute(Query, Variables, Context) of
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

-spec parse_payload(binary()) -> {ok, binary() | string(), map(), map()} | {error, ipto_reason()}.
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

-spec normalize_map(term()) -> map().
normalize_map(M) when is_map(M) ->
    M;
normalize_map(_) ->
    #{}.

-spec error_response(term()) -> map().
error_response(Reason) ->
    #{errors => [#{message => iolist_to_binary(io_lib:format("~p", [Reason]))}]}. 

-spec encode_json(term()) -> binary().
encode_json(Value) ->
    unicode:characters_to_binary(json:encode(normalize_json(Value))).

-spec normalize_json(term()) -> term().
normalize_json(V) when is_map(V) ->
    maps:from_list([{json_key(K), normalize_json(Val)} || {K, Val} <- maps:to_list(V)]);
normalize_json(V) when is_list(V) ->
    [normalize_json(E) || E <- V];
normalize_json(V) ->
    V.

-spec json_key(term()) -> term().
json_key(K) when is_atom(K) -> atom_to_binary(K, utf8);
json_key(K) when is_list(K) -> unicode:characters_to_binary(K);
json_key(K) -> K.
