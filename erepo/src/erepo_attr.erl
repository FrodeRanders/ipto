-module(erepo_attr).

-export([create_attribute/5, get_attribute_info/1, sync/0]).

create_attribute(Alias, Name, QualName, Type, IsArray) ->
    erepo_db:create_attribute(Alias, Name, QualName, Type, IsArray).

get_attribute_info(NameOrId) ->
    erepo_db:get_attribute_info(NameOrId).

sync() ->
    %% Java repo syncs adapter maps with known attributes.
    %% Here we expose the hook; implementation is DB/adapter-specific.
    ok.
