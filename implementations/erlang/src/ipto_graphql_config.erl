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
-module(ipto_graphql_config).

-export([
    init/0,
    reload/0,
    get_schema/0,
    get_mapping/0,
    set_schema_file/1,
    set_schema_sdl/1,
    clear_schema_overrides/0
]).

-define(SCHEMA_KEY, {?MODULE, schema}).
-define(MAPPING_KEY, {?MODULE, mapping}).

-spec init() -> ok.
init() ->
    reload().

-spec reload() -> ok.
reload() ->
    Schema = resolve_schema(),
    Mapping = resolve_mapping(),
    persistent_term:put(?SCHEMA_KEY, Schema),
    persistent_term:put(?MAPPING_KEY, Mapping),
    ok.

-spec get_schema() -> binary().
get_schema() ->
    persistent_term:get(?SCHEMA_KEY, resolve_schema()).

-spec get_mapping() -> map().
get_mapping() ->
    persistent_term:get(?MAPPING_KEY, resolve_mapping()).

-spec set_schema_file(binary() | string()) -> ok.
set_schema_file(Path) when is_binary(Path); is_list(Path) ->
    application:set_env(ipto, graphql_schema_file, Path),
    reload().

-spec set_schema_sdl(binary() | string()) -> ok.
set_schema_sdl(Sdl) when is_binary(Sdl); is_list(Sdl) ->
    application:set_env(ipto, graphql_schema_sdl, to_binary(Sdl)),
    reload().

-spec clear_schema_overrides() -> ok.
clear_schema_overrides() ->
    application:unset_env(ipto, graphql_schema_file),
    application:unset_env(ipto, graphql_schema_sdl),
    reload().

-spec resolve_schema() -> binary().
resolve_schema() ->
    case application:get_env(ipto, graphql_schema_sdl) of
        {ok, Sdl} when is_binary(Sdl); is_list(Sdl) ->
            to_binary(Sdl);
        _ ->
            case schema_file_path() of
                undefined -> ipto_graphql_schema:schema();
                Path ->
                    case file:read_file(Path) of
                        {ok, Bin} -> Bin;
                        _ -> ipto_graphql_schema:schema()
                    end
            end
    end.

-spec schema_file_path() -> binary() | string() | undefined.
schema_file_path() ->
    case application:get_env(ipto, graphql_schema_file) of
        {ok, Path} -> Path;
        _ ->
            case os:getenv("IPTO_GRAPHQL_SCHEMA_FILE") of
                false -> undefined;
                Path -> Path
            end
    end.

-spec resolve_mapping() -> map().
resolve_mapping() ->
    Base = # {
        objects => # {
            default => ipto_graphql_object_resource,
            'Query' => ipto_graphql_query_resource,
            'Mutation' => ipto_graphql_mutation_resource
        }
    },
    case application:get_env(ipto, graphql_mapping) of
        {ok, Custom} when is_map(Custom) -> deep_merge(Base, Custom);
        _ -> Base
    end.

-spec deep_merge(map(), map()) -> map().
deep_merge(A, B) when is_map(A), is_map(B) ->
    maps:fold(fun(K, V, Acc) ->
        case maps:get(K, Acc, undefined) of
            Existing when is_map(Existing), is_map(V) ->
                Acc#{K => deep_merge(Existing, V)};
            _ ->
                Acc#{K => V}
        end
    end, A, B);
deep_merge(_A, B) -> B.

-spec to_binary(term()) -> binary().
to_binary(Value) when is_binary(Value) -> Value;
to_binary(Value) when is_list(Value) -> unicode:characters_to_binary(Value);
to_binary(Value) -> unicode:characters_to_binary(io_lib:format("~p", [Value])).
