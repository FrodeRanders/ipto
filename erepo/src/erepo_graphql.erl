-module(erepo_graphql).

-export([
    schema/0,
    build_context/1,
    execute/2,
    execute/3,
    reload_schema/0
]).

schema() ->
    erepo_graphql_schema:schema().

build_context(Options) when is_map(Options) ->
    #{
        tenantid => maps:get(tenantid, Options, undefined),
        authz => maps:get(authz, Options, undefined),
        meta => maps:get(meta, Options, #{})
    };
build_context(_Options) ->
    #{tenantid => undefined, authz => undefined, meta => #{}}.

execute(Query, Variables) ->
    Context = build_context(#{}),
    execute(Query, Variables, Context).

execute(Query, Variables, Context) ->
    erepo_graphql_adapter:execute(Query, Variables, Context).

reload_schema() ->
    erepo_graphql_adapter:reload_schema().
