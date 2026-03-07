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
    {ok, unit_map() | search_result() | boolean() | integer() | map() | [map()] | null} |
    {error, unsupported_query_field | ipto_reason()}.
execute(Context, _Source, <<"unit">>, Args) ->
    ipto_graphql_resolvers:resolve(query, unit, normalize_args(Args), Context);
execute(Context, _Source, <<"unitByCorrid">>, Args) ->
    ipto_graphql_resolvers:resolve(query, unitByCorrid, normalize_args(Args), Context);
execute(Context, _Source, <<"unitExists">>, Args) ->
    ipto_graphql_resolvers:resolve(query, unitExists, normalize_args(Args), Context);
execute(Context, _Source, <<"unitLocked">>, Args) ->
    ipto_graphql_resolvers:resolve(query, unitLocked, normalize_args(Args), Context);
execute(Context, _Source, <<"units">>, Args) ->
    ipto_graphql_resolvers:resolve(query, units, normalize_args(Args), Context);
execute(Context, _Source, <<"unitsByExpression">>, Args) ->
    ipto_graphql_resolvers:resolve(query, unitsByExpression, normalize_args(Args), Context);
execute(Context, _Source, <<"rightRelation">>, Args) ->
    ipto_graphql_resolvers:resolve(query, rightRelation, normalize_args(Args), Context);
execute(Context, _Source, <<"rightRelations">>, Args) ->
    ipto_graphql_resolvers:resolve(query, rightRelations, normalize_args(Args), Context);
execute(Context, _Source, <<"leftRelations">>, Args) ->
    ipto_graphql_resolvers:resolve(query, leftRelations, normalize_args(Args), Context);
execute(Context, _Source, <<"countRightRelations">>, Args) ->
    ipto_graphql_resolvers:resolve(query, countRightRelations, normalize_args(Args), Context);
execute(Context, _Source, <<"countLeftRelations">>, Args) ->
    ipto_graphql_resolvers:resolve(query, countLeftRelations, normalize_args(Args), Context);
execute(Context, _Source, <<"rightAssociation">>, Args) ->
    ipto_graphql_resolvers:resolve(query, rightAssociation, normalize_args(Args), Context);
execute(Context, _Source, <<"rightAssociations">>, Args) ->
    ipto_graphql_resolvers:resolve(query, rightAssociations, normalize_args(Args), Context);
execute(Context, _Source, <<"leftAssociations">>, Args) ->
    ipto_graphql_resolvers:resolve(query, leftAssociations, normalize_args(Args), Context);
execute(Context, _Source, <<"countRightAssociations">>, Args) ->
    ipto_graphql_resolvers:resolve(query, countRightAssociations, normalize_args(Args), Context);
execute(Context, _Source, <<"countLeftAssociations">>, Args) ->
    ipto_graphql_resolvers:resolve(query, countLeftAssociations, normalize_args(Args), Context);
execute(Context, _Source, <<"tenantInfo">>, Args) ->
    ipto_graphql_resolvers:resolve(query, tenantInfo, normalize_args(Args), Context);
execute(Context, _Source, <<"attributeInfo">>, Args) ->
    ipto_graphql_resolvers:resolve(query, attributeInfo, normalize_args(Args), Context);
execute(Context, _Source, <<"inspectGraphqlSdl">>, Args) ->
    ipto_graphql_resolvers:resolve(query, inspectGraphqlSdl, normalize_args(Args), Context);
execute(Context, _Source, <<"registeredOperations">>, Args) ->
    ipto_graphql_resolvers:resolve(query, registeredOperations, normalize_args(Args), Context);
execute(_Context, _Source, _Field, _Args) ->
    {error, unsupported_query_field}.

-spec normalize_args(map() | term()) -> map().
normalize_args(Args) when is_map(Args) ->
    maps:from_list([{normalize_key(K), V} || {K, V} <- maps:to_list(Args)]);
normalize_args(_Args) ->
    #{}.

-spec normalize_key(term()) -> term().
normalize_key(K) when is_binary(K) -> binary_to_atom(K, utf8);
normalize_key(K) when is_atom(K) -> K;
normalize_key(K) -> K.
