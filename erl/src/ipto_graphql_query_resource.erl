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
-module(ipto_graphql_query_resource).

-include("ipto.hrl").

-export([execute/4]).

-spec execute(map(), map() | undefined, binary(), map()) ->
    {ok, unit_map() | search_result() | null} | {error, unsupported_query_field | ipto_reason()}.
execute(Context, _Source, <<"unit">>, Args) ->
    ipto_graphql_resolvers:resolve(query, unit, normalize_args(Args), Context);
execute(Context, _Source, <<"units">>, Args) ->
    ipto_graphql_resolvers:resolve(query, units, normalize_args(Args), Context);
execute(_Context, _Source, _Field, _Args) ->
    {error, unsupported_query_field}.

normalize_args(Args) when is_map(Args) ->
    maps:from_list([{normalize_key(K), V} || {K, V} <- maps:to_list(Args)]);
normalize_args(_Args) ->
    #{}.

normalize_key(K) when is_binary(K) -> binary_to_atom(K, utf8);
normalize_key(K) when is_atom(K) -> K;
normalize_key(K) -> K.
