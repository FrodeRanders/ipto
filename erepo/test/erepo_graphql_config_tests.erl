-module(erepo_graphql_config_tests).

-include_lib("eunit/include/eunit.hrl").

schema_override_from_sdl_test() ->
    ok = erepo_graphql_config:set_schema_sdl("type Query { ping: String }"),
    Bin = erepo_graphql_config:get_schema(),
    true = binary:match(Bin, <<"ping">>) =/= nomatch,
    ok = erepo_graphql_config:clear_schema_overrides().

schema_override_from_file_test() ->
    Tmp = filename:join([os:getenv("TMPDIR", "/tmp"), "erepo-graphql-schema.graphql"]),
    ok = file:write_file(Tmp, <<"type Query { pong: String }">>),
    ok = erepo_graphql_config:set_schema_file(Tmp),
    Bin = erepo_graphql_config:get_schema(),
    true = binary:match(Bin, <<"pong">>) =/= nomatch,
    ok = erepo_graphql_config:clear_schema_overrides(),
    _ = file:delete(Tmp),
    ok.
