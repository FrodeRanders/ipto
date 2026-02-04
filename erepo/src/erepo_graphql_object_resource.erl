-module(erepo_graphql_object_resource).

-export([execute/4]).

execute(_Context, Source, Field, _Args) when is_map(Source), is_binary(Field) ->
    Value =
        case maps:find(Field, Source) of
            {ok, V1} -> V1;
            error ->
                case maps:find(binary_to_atom(Field, utf8), Source) of
                    {ok, V2} -> V2;
                    error -> null
                end
        end,
    {ok, Value};
execute(_Context, _Source, _Field, _Args) ->
    {ok, null}.
