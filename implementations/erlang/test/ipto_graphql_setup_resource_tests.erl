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
-module(ipto_graphql_setup_resource_tests).

-include_lib("eunit/include/eunit.hrl").

%% Confirms the query resource exposes SDL inspection without going through the
%% full GraphQL text parser.
inspect_graphql_sdl_query_resource_test() ->
    Sdl = <<
        "enum Attributes @attributeRegistry {\n"
        "  a @attribute(datatype: STRING, name: \"demo:a\")\n"
        "}\n"
    >>,
    {ok, JsonBin} = ipto_graphql_query_resource:execute(
        #{},
        undefined,
        <<"inspectGraphqlSdl">>,
        #{<<"sdl">> => Sdl}
    ),
    true = is_map(JsonBin),
    true = maps:is_key(attribute_registry, JsonBin).

%% Confirms the mutation resource can apply SDL and return the setup summary.
configure_graphql_sdl_mutation_resource_test() ->
    application:set_env(ipto, backend, memory),
    {ok, _} = ipto:start_link(),
    Suffix = integer_to_list(erlang:system_time(microsecond) rem 1000000000),
    AttrName = list_to_binary("demo:gql:attr:" ++ Suffix),
    Sdl = iolist_to_binary(io_lib:format(
        "enum Attributes @attributeRegistry {\n"
        "  a_~s @attribute(datatype: STRING, name: \"~s\")\n"
        "}\n",
        [Suffix, AttrName]
    )),
    {ok, JsonBin} = ipto_graphql_mutation_resource:execute(
        #{},
        undefined,
        <<"configureGraphqlSdl">>,
        #{<<"sdl">> => Sdl}
    ),
    true = is_map(JsonBin),
    true = maps:is_key(attributes, JsonBin).

%% Missing SDL files should be surfaced as resource-level configuration errors.
configure_graphql_sdl_file_mutation_resource_error_test() ->
    {error, {configure_graphql_sdl_file_failed, _}} = ipto_graphql_mutation_resource:execute(
        #{},
        undefined,
        <<"configureGraphqlSdlFile">>,
        #{<<"path">> => <<"/no/such/file.graphqls">>}
    ).

%% Exercises the structured search query resource with expression-style
%% arguments and paging controls.
units_by_expression_query_resource_test() ->
    application:set_env(ipto, backend, memory),
    {ok, _} = ipto:start_link(),
    {ok, U0} = ipto:create_unit(88, <<"gql-res-a">>),
    {ok, _} = ipto:store_unit(U0),
    {ok, U1} = ipto:create_unit(88, <<"gql-res-b">>),
    {ok, _} = ipto:store_unit(U1),

    {ok, Result} = ipto_graphql_query_resource:execute(
        #{},
        undefined,
        <<"unitsByExpression">>,
        #{
            <<"tenantid">> => 88,
            <<"nameLike">> => <<"%gql-res-%">>,
            <<"orderField">> => <<"created">>,
            <<"orderDir">> => <<"desc">>,
            <<"limit">> => 10,
            <<"offset">> => 0
        }
    ),
    true = is_map(Result),
    true = maps:get(total, Result) >= 2.

%% Covers the resource-layer equivalents of the broader GraphQL read and
%% mutation surface.
graphql_extended_read_mutation_resource_test() ->
    application:set_env(ipto, backend, memory),
    {ok, _} = ipto:start_link(),
    {ok, Attr} = ipto:create_attribute(<<"GQLR">>, <<"demo:gql:res">>, <<"demo:gql:res">>, 2, true),

    {ok, U0} = ipto:create_unit(89, <<"gql-ext-a">>),
    {ok, A} = ipto:store_unit(U0),
    {ok, U1} = ipto:create_unit(89, <<"gql-ext-b">>),
    {ok, B} = ipto:store_unit(U1),

    TenantId = maps:get(tenantid, A),
    UnitIdA = maps:get(unitid, A),
    UnitIdB = maps:get(unitid, B),
    CorrIdA = maps:get(corrid, A),

    {ok, true} = ipto_graphql_query_resource:execute(
        #{},
        undefined,
        <<"unitExists">>,
        #{<<"tenantid">> => TenantId, <<"unitid">> => UnitIdA}
    ),
    {ok, false} = ipto_graphql_query_resource:execute(
        #{},
        undefined,
        <<"unitLocked">>,
        #{<<"tenantid">> => TenantId, <<"unitid">> => UnitIdA}
    ),

    {ok, UnitByCorrid} = ipto_graphql_query_resource:execute(
        #{},
        undefined,
        <<"unitByCorrid">>,
        #{<<"tenantid">> => TenantId, <<"corrid">> => CorrIdA}
    ),
    UnitIdA = maps:get(unitid, UnitByCorrid),

    {ok, TenantInfo} = ipto_graphql_query_resource:execute(
        #{},
        undefined,
        <<"tenantInfo">>,
        #{<<"id">> => TenantId}
    ),
    TenantId = maps:get(id, TenantInfo),

    {ok, AttrInfo} = ipto_graphql_query_resource:execute(
        #{},
        undefined,
        <<"attributeInfo">>,
        #{<<"id">> => maps:get(id, Attr)}
    ),
    true = (maps:get(id, Attr) =:= maps:get(id, AttrInfo)),

    {ok, true} = ipto_graphql_mutation_resource:execute(
        #{},
        undefined,
        <<"lockUnit">>,
        #{
            <<"tenantid">> => TenantId,
            <<"unitid">> => UnitIdA,
            <<"locktype">> => 30,
            <<"purpose">> => <<"resource-test">>
        }
    ),
    {ok, true} = ipto_graphql_query_resource:execute(
        #{},
        undefined,
        <<"unitLocked">>,
        #{<<"tenantid">> => TenantId, <<"unitid">> => UnitIdA}
    ),
    {ok, false} = ipto_graphql_mutation_resource:execute(
        #{},
        undefined,
        <<"lockUnit">>,
        #{
            <<"tenantid">> => TenantId,
            <<"unitid">> => UnitIdA,
            <<"locktype">> => 30,
            <<"purpose">> => <<"resource-test">>
        }
    ),
    {ok, true} = ipto_graphql_mutation_resource:execute(
        #{},
        undefined,
        <<"unlockUnit">>,
        #{<<"tenantid">> => TenantId, <<"unitid">> => UnitIdA}
    ),
    {ok, false} = ipto_graphql_query_resource:execute(
        #{},
        undefined,
        <<"unitLocked">>,
        #{<<"tenantid">> => TenantId, <<"unitid">> => UnitIdA}
    ),
    {ok, #{status := 10}} = ipto_graphql_mutation_resource:execute(
        #{},
        undefined,
        <<"requestStatusTransition">>,
        #{<<"tenantid">> => TenantId, <<"unitid">> => UnitIdA, <<"status">> => 10}
    ),
    {ok, #{status := 30}} = ipto_graphql_mutation_resource:execute(
        #{},
        undefined,
        <<"transitionUnitStatus">>,
        #{<<"tenantid">> => TenantId, <<"unitid">> => UnitIdA, <<"status">> => 30}
    ),

    {ok, true} = ipto_graphql_mutation_resource:execute(
        #{},
        undefined,
        <<"addRelation">>,
        #{
            <<"tenantid">> => TenantId,
            <<"unitid">> => UnitIdA,
            <<"reltype">> => 1,
            <<"reltenantid">> => TenantId,
            <<"relunitid">> => UnitIdB
        }
    ),
    {ok, RightRelation} = ipto_graphql_query_resource:execute(
        #{},
        undefined,
        <<"rightRelation">>,
        #{<<"tenantid">> => TenantId, <<"unitid">> => UnitIdA, <<"reltype">> => 1}
    ),
    true = is_map(RightRelation),
    {ok, RightRelations} = ipto_graphql_query_resource:execute(
        #{},
        undefined,
        <<"rightRelations">>,
        #{<<"tenantid">> => TenantId, <<"unitid">> => UnitIdA, <<"reltype">> => 1}
    ),
    true = (length(RightRelations) =:= 1),
    {ok, LeftRelations} = ipto_graphql_query_resource:execute(
        #{},
        undefined,
        <<"leftRelations">>,
        #{<<"tenantid">> => TenantId, <<"unitid">> => UnitIdB, <<"reltype">> => 1}
    ),
    true = (length(LeftRelations) =:= 1),
    {ok, 1} = ipto_graphql_query_resource:execute(
        #{},
        undefined,
        <<"countRightRelations">>,
        #{<<"tenantid">> => TenantId, <<"unitid">> => UnitIdA, <<"reltype">> => 1}
    ),
    {ok, 1} = ipto_graphql_query_resource:execute(
        #{},
        undefined,
        <<"countLeftRelations">>,
        #{<<"tenantid">> => TenantId, <<"unitid">> => UnitIdB, <<"reltype">> => 1}
    ),
    {ok, true} = ipto_graphql_mutation_resource:execute(
        #{},
        undefined,
        <<"removeRelation">>,
        #{
            <<"tenantid">> => TenantId,
            <<"unitid">> => UnitIdA,
            <<"reltype">> => 1,
            <<"reltenantid">> => TenantId,
            <<"relunitid">> => UnitIdB
        }
    ),

    {ok, true} = ipto_graphql_mutation_resource:execute(
        #{},
        undefined,
        <<"addAssociation">>,
        #{
            <<"tenantid">> => TenantId,
            <<"unitid">> => UnitIdA,
            <<"assoctype">> => 2,
            <<"assocstring">> => <<"case:graphql-resource">>
        }
    ),
    {ok, RightAssociation} = ipto_graphql_query_resource:execute(
        #{},
        undefined,
        <<"rightAssociation">>,
        #{<<"tenantid">> => TenantId, <<"unitid">> => UnitIdA, <<"assoctype">> => 2}
    ),
    true = is_map(RightAssociation),
    {ok, RightAssociations} = ipto_graphql_query_resource:execute(
        #{},
        undefined,
        <<"rightAssociations">>,
        #{<<"tenantid">> => TenantId, <<"unitid">> => UnitIdA, <<"assoctype">> => 2}
    ),
    true = (length(RightAssociations) =:= 1),
    {ok, LeftAssociations} = ipto_graphql_query_resource:execute(
        #{},
        undefined,
        <<"leftAssociations">>,
        #{<<"assoctype">> => 2, <<"assocstring">> => <<"case:graphql-resource">>}
    ),
    true = (length(LeftAssociations) =:= 1),
    {ok, 1} = ipto_graphql_query_resource:execute(
        #{},
        undefined,
        <<"countRightAssociations">>,
        #{<<"tenantid">> => TenantId, <<"unitid">> => UnitIdA, <<"assoctype">> => 2}
    ),
    {ok, 1} = ipto_graphql_query_resource:execute(
        #{},
        undefined,
        <<"countLeftAssociations">>,
        #{<<"assoctype">> => 2, <<"assocstring">> => <<"case:graphql-resource">>}
    ),
    {ok, true} = ipto_graphql_mutation_resource:execute(
        #{},
        undefined,
        <<"removeAssociation">>,
        #{
            <<"tenantid">> => TenantId,
            <<"unitid">> => UnitIdA,
            <<"assoctype">> => 2,
            <<"assocstring">> => <<"case:graphql-resource">>
        }
    ).

%% The registered-operations resource should respect the configured allowlist.
registered_operations_query_resource_test() ->
    application:set_env(ipto, graphql_operation_allowlist, [<<"registeredOperations">>, <<"unit">>, <<"createUnit">>]),
    try
        {ok, Result} = ipto_graphql_query_resource:execute(
            #{},
            undefined,
            <<"registeredOperations">>,
            #{}
        ),
        Queries = maps:get(queries, Result),
        Mutations = maps:get(mutations, Result),
        QueryNames = [entry_name(Entry) || Entry <- Queries],
        MutationNames = [entry_name(Entry) || Entry <- Mutations],
        true = lists:member(<<"registeredOperations">>, QueryNames),
        true = lists:member(<<"unit">>, QueryNames),
        false = lists:member(<<"units">>, QueryNames),
        true = lists:member(<<"createUnit">>, MutationNames),
        false = lists:member(<<"lockUnit">>, MutationNames)
    after
        application:unset_env(ipto, graphql_operation_allowlist)
    end.

-spec entry_name({ok, map()} | map()) -> binary().
entry_name({ok, Entry}) ->
    maps:get(name, Entry);
entry_name(Entry) ->
    maps:get(name, Entry).
