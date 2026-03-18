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

%% Checks that the built-in schema still exposes a query root.
schema_contains_query_root_test() ->
    Schema = ipto_graphql:schema(),
    true = binary:match(Schema, <<"type Query">>) =/= nomatch.

%% Validates the facade behavior when `graphql_erl` is either available or
%% intentionally absent in the current profile.
graphql_adapter_smoke_test() ->
    Result = ipto_graphql:execute("{ __typename }", #{}),
    case Result of
        {error, {missing_dependency, graphql_erl}} -> ok;
        {error, {unknown_graphql_erl_api, graphql_erl}} -> ok;
        {ok, _} -> ok;
        _ -> ?assert(false)
    end.

%% Expands into runtime GraphQL tests only when the external dependency is
%% loaded; otherwise it reports a skipped EUnit case.
graphql_real_execution_test_() ->
    case code:ensure_loaded(graphql) of
        {module, graphql} ->
            [
                fun graphql_real_execution/0,
                fun graphql_setup_execution/0,
                fun graphql_search_execution/0,
                fun graphql_extended_execution/0
            ];
        _ ->
            {"graphql_erl dependency not loaded in this profile", fun() -> ok end}
    end.

%% Covers the simplest mutation flow through the real GraphQL executor.
graphql_real_execution() ->
    application:set_env(ipto, backend, memory),
    {ok, _} = ipto:start_link(),
    Mutation = "mutation { createUnit(tenantid: 1, name: \"gql\") { tenantid unitid unitver } }",
    {ok, Response} = ipto_graphql:execute(Mutation, #{}),
    true = is_map(Response),
    ok.

%% Verifies that SDL inspection and configuration operations are wired into the
%% executable GraphQL schema.
graphql_setup_execution() ->
    application:set_env(ipto, backend, memory),
    {ok, _} = ipto:start_link(),

    Sdl = "enum Attributes @attributeRegistry { a @attribute(datatype: STRING) }",
    InspectQuery =
        "{ inspectGraphqlSdl(sdl: \"" ++ Sdl ++ "\") { counts { attributes } errors { code } } }",
    {ok, InspectResponse} = ipto_graphql:execute(InspectQuery, #{}),
    true = is_map(InspectResponse),
    false = has_graphql_errors(InspectResponse),

    ConfigureMutation =
        "mutation { configureGraphqlSdl(sdl: \"" ++ Sdl ++ "\") { attributes { created existing } records { supported } templates { supported } } }",
    {ok, ConfigureResponse} = ipto_graphql:execute(ConfigureMutation, #{}),
    true = is_map(ConfigureResponse),
    false = has_graphql_errors(ConfigureResponse),
    ok.

%% Exercises both text-query and structured-expression search operations through
%% GraphQL.
graphql_search_execution() ->
    application:set_env(ipto, backend, memory),
    {ok, _} = ipto:start_link(),

    {ok, U0} = ipto:create_unit(7, <<"gql-search-a">>),
    {ok, _} = ipto:store_unit(U0),
    {ok, U1} = ipto:create_unit(7, <<"gql-search-b">>),
    {ok, _} = ipto:store_unit(U1),

    TextQuery =
        "{ units(query: \"tenantid=7 and name like '%gql-search%'\", orderField: \"created\", orderDir: \"desc\", limit: 10, offset: 0) { total } }",
    {ok, TextResponse} = ipto_graphql:execute(TextQuery, #{}),
    true = is_map(TextResponse),
    false = has_graphql_errors(TextResponse),

    ExprQuery =
        "{ unitsByExpression(tenantid: 7, nameLike: \"%gql-search%\", orderField: \"created\", orderDir: \"desc\", limit: 10, offset: 0) { total } }",
    {ok, ExprResponse} = ipto_graphql:execute(ExprQuery, #{}),
    true = is_map(ExprResponse),
    false = has_graphql_errors(ExprResponse),
    ok.

%% Runs a broad GraphQL end-to-end scenario across lookup, lifecycle, relation,
%% and association operations.
graphql_extended_execution() ->
    application:set_env(ipto, backend, memory),
    {ok, _} = ipto:start_link(),

    {ok, _Attr} = ipto:create_attribute(<<"GQLE">>, <<"demo:gql:ext">>, <<"demo:gql:ext">>, 2, true),
    {ok, U0} = ipto:create_unit(77, <<"gql-ext-a">>),
    {ok, A} = ipto:store_unit(U0),
    {ok, U1} = ipto:create_unit(77, <<"gql-ext-b">>),
    {ok, B} = ipto:store_unit(U1),

    TenantId = integer_to_list(maps:get(tenantid, A)),
    UnitIdA = integer_to_list(maps:get(unitid, A)),
    UnitIdB = integer_to_list(maps:get(unitid, B)),
    CorrIdA = binary_to_list(maps:get(corrid, A)),

    QueryByCorrid =
        "{ unitByCorrid(tenantid: " ++ TenantId ++ ", corrid: \"" ++ CorrIdA ++ "\") { unitid } }",
    {ok, ByCorridResp} = ipto_graphql:execute(QueryByCorrid, #{}),
    true = is_map(ByCorridResp),
    false = has_graphql_errors(ByCorridResp),

    ExistsQuery = "{ unitExists(tenantid: " ++ TenantId ++ ", unitid: " ++ UnitIdA ++ ") }",
    {ok, ExistsResp} = ipto_graphql:execute(ExistsQuery, #{}),
    true = is_map(ExistsResp),
    false = has_graphql_errors(ExistsResp),

    TenantInfoQuery = "{ tenantInfo(id: " ++ TenantId ++ ") { id name } }",
    {ok, TenantResp} = ipto_graphql:execute(TenantInfoQuery, #{}),
    true = is_map(TenantResp),
    false = has_graphql_errors(TenantResp),

    AttrInfoQuery = "{ attributeInfo(name: \"demo:gql:ext\") { id name } }",
    {ok, AttrResp} = ipto_graphql:execute(AttrInfoQuery, #{}),
    true = is_map(AttrResp),
    false = has_graphql_errors(AttrResp),

    RegisteredOperationsQuery = "{ registeredOperations { queries { name } mutations { name } } }",
    {ok, RegisteredResp} = ipto_graphql:execute(RegisteredOperationsQuery, #{}),
    true = is_map(RegisteredResp),
    false = has_graphql_errors(RegisteredResp),

    LockedBeforeQuery = "{ unitLocked(tenantid: " ++ TenantId ++ ", unitid: " ++ UnitIdA ++ ") }",
    {ok, LockedBeforeResp} = ipto_graphql:execute(LockedBeforeQuery, #{}),
    true = is_map(LockedBeforeResp),
    false = has_graphql_errors(LockedBeforeResp),

    LockMutation =
        "mutation { lockUnit(tenantid: " ++ TenantId ++ ", unitid: " ++ UnitIdA ++ ", locktype: 30, purpose: \"gql-ext\") }",
    {ok, LockResp} = ipto_graphql:execute(LockMutation, #{}),
    true = is_map(LockResp),
    false = has_graphql_errors(LockResp),

    LockedAfterQuery = "{ unitLocked(tenantid: " ++ TenantId ++ ", unitid: " ++ UnitIdA ++ ") }",
    {ok, LockedAfterResp} = ipto_graphql:execute(LockedAfterQuery, #{}),
    true = is_map(LockedAfterResp),
    false = has_graphql_errors(LockedAfterResp),

    UnlockMutation =
        "mutation { unlockUnit(tenantid: " ++ TenantId ++ ", unitid: " ++ UnitIdA ++ ") }",
    {ok, UnlockResp} = ipto_graphql:execute(UnlockMutation, #{}),
    true = is_map(UnlockResp),
    false = has_graphql_errors(UnlockResp),

    LockedAfterUnlockQuery = "{ unitLocked(tenantid: " ++ TenantId ++ ", unitid: " ++ UnitIdA ++ ") }",
    {ok, LockedAfterUnlockResp} = ipto_graphql:execute(LockedAfterUnlockQuery, #{}),
    true = is_map(LockedAfterUnlockResp),
    false = has_graphql_errors(LockedAfterUnlockResp),

    RequestTransitionMutation =
        "mutation { requestStatusTransition(tenantid: " ++ TenantId ++ ", unitid: " ++ UnitIdA ++ ", status: 10) { status } }",
    {ok, RequestTransitionResp} = ipto_graphql:execute(RequestTransitionMutation, #{}),
    true = is_map(RequestTransitionResp),
    false = has_graphql_errors(RequestTransitionResp),

    TransitionAliasMutation =
        "mutation { transitionUnitStatus(tenantid: " ++ TenantId ++ ", unitid: " ++ UnitIdA ++ ", status: 30) { status } }",
    {ok, TransitionAliasResp} = ipto_graphql:execute(TransitionAliasMutation, #{}),
    true = is_map(TransitionAliasResp),
    false = has_graphql_errors(TransitionAliasResp),

    AddRelMutation =
        "mutation { addRelation(tenantid: " ++ TenantId ++ ", unitid: " ++ UnitIdA
        ++ ", reltype: 1, reltenantid: " ++ TenantId ++ ", relunitid: " ++ UnitIdB ++ ") }",
    {ok, AddRelResp} = ipto_graphql:execute(AddRelMutation, #{}),
    true = is_map(AddRelResp),
    false = has_graphql_errors(AddRelResp),

    RightRelationQuery =
        "{ rightRelation(tenantid: " ++ TenantId ++ ", unitid: " ++ UnitIdA ++ ", reltype: 1) { reltenantid relunitid } }",
    {ok, RightRelationResp} = ipto_graphql:execute(RightRelationQuery, #{}),
    true = is_map(RightRelationResp),
    false = has_graphql_errors(RightRelationResp),

    CountRightRelationsQuery =
        "{ countRightRelations(tenantid: " ++ TenantId ++ ", unitid: " ++ UnitIdA ++ ", reltype: 1) }",
    {ok, CountRightRelResp} = ipto_graphql:execute(CountRightRelationsQuery, #{}),
    true = is_map(CountRightRelResp),
    false = has_graphql_errors(CountRightRelResp),

    RemoveRelMutation =
        "mutation { removeRelation(tenantid: " ++ TenantId ++ ", unitid: " ++ UnitIdA
        ++ ", reltype: 1, reltenantid: " ++ TenantId ++ ", relunitid: " ++ UnitIdB ++ ") }",
    {ok, RemoveRelResp} = ipto_graphql:execute(RemoveRelMutation, #{}),
    true = is_map(RemoveRelResp),
    false = has_graphql_errors(RemoveRelResp),

    AddAssocMutation =
        "mutation { addAssociation(tenantid: " ++ TenantId ++ ", unitid: " ++ UnitIdA
        ++ ", assoctype: 2, assocstring: \"case:gql-ext\") }",
    {ok, AddAssocResp} = ipto_graphql:execute(AddAssocMutation, #{}),
    true = is_map(AddAssocResp),
    false = has_graphql_errors(AddAssocResp),

    RightAssociationQuery =
        "{ rightAssociation(tenantid: " ++ TenantId ++ ", unitid: " ++ UnitIdA ++ ", assoctype: 2) { assocstring } }",
    {ok, RightAssocResp} = ipto_graphql:execute(RightAssociationQuery, #{}),
    true = is_map(RightAssocResp),
    false = has_graphql_errors(RightAssocResp),

    CountLeftAssociationsQuery =
        "{ countLeftAssociations(assoctype: 2, assocstring: \"case:gql-ext\") }",
    {ok, CountLeftAssocResp} = ipto_graphql:execute(CountLeftAssociationsQuery, #{}),
    true = is_map(CountLeftAssocResp),
    false = has_graphql_errors(CountLeftAssocResp),

    RemoveAssocMutation =
        "mutation { removeAssociation(tenantid: " ++ TenantId ++ ", unitid: " ++ UnitIdA
        ++ ", assoctype: 2, assocstring: \"case:gql-ext\") }",
    {ok, RemoveAssocResp} = ipto_graphql:execute(RemoveAssocMutation, #{}),
    true = is_map(RemoveAssocResp),
    false = has_graphql_errors(RemoveAssocResp),
    ok.

-spec has_graphql_errors(map()) -> boolean().
has_graphql_errors(Response) ->
    case maps:get(<<"errors">>, Response, maps:get(errors, Response, undefined)) of
        undefined -> false;
        [] -> false;
        _ -> true
    end.
