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
-module(ipto_graphql_tests).

-include_lib("eunit/include/eunit.hrl").

schema_contains_query_root_test() ->
    Schema = ipto_graphql:schema(),
    true = binary:match(Schema, <<"type Query">>) =/= nomatch.

graphql_adapter_smoke_test() ->
    Result = ipto_graphql:execute("{ __typename }", #{}),
    case Result of
        {error, {missing_dependency, graphql_erl}} -> ok;
        {error, {unknown_graphql_erl_api, graphql_erl}} -> ok;
        {ok, _} -> ok;
        _ -> ?assert(false)
    end.

graphql_real_execution_test_() ->
    case code:ensure_loaded(graphql) of
        {module, graphql} ->
            fun graphql_real_execution/0;
        _ ->
            {"graphql_erl dependency not loaded in this profile", fun() -> ok end}
    end.

graphql_real_execution() ->
    application:set_env(ipto, backend, memory),
    {ok, _} = ipto:start_link(),
    Mutation = "mutation { createUnit(tenantid: 1, name: \"gql\") { tenantid unitid unitver } }",
    {ok, Response} = ipto_graphql:execute(Mutation, #{}),
    true = is_map(Response),
    ok.
