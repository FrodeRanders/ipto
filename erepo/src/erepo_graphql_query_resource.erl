-module(erepo_graphql_query_resource).

-export([execute/4]).

execute(Context, _Source, <<"unit">>, Args) ->
    erepo_graphql_resolvers:resolve(query, unit, normalize_args(Args), Context);
execute(Context, _Source, <<"units">>, Args) ->
    erepo_graphql_resolvers:resolve(query, units, normalize_args(Args), Context);
execute(_Context, _Source, _Field, _Args) ->
    {error, unsupported_query_field}.

normalize_args(Args) when is_map(Args) ->
    maps:from_list([{normalize_key(K), V} || {K, V} <- maps:to_list(Args)]);
normalize_args(_Args) ->
    #{}.

normalize_key(K) when is_binary(K) -> binary_to_atom(K, utf8);
normalize_key(K) when is_atom(K) -> K;
normalize_key(K) -> K.
