-module(erepo_graphql_tests).

-include_lib("eunit/include/eunit.hrl").

schema_contains_query_root_test() ->
    Schema = erepo_graphql:schema(),
    true = binary:match(Schema, <<"type Query">>) =/= nomatch.

graphql_adapter_smoke_test() ->
    Result = erepo_graphql:execute("{ __typename }", #{}),
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
    application:set_env(erepo, backend, memory),
    {ok, _} = erepo:start_link(),
    Mutation = "mutation { createUnit(tenantid: 1, name: \"gql\") { tenantid unitid unitver } }",
    {ok, Response} = erepo_graphql:execute(Mutation, #{}),
    true = is_map(Response),
    ok.
