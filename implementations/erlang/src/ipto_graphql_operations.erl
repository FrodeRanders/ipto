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
-module(ipto_graphql_operations).

-export([registered/0]).

-spec registered() -> #{queries := [binary()], mutations := [binary()]}.
registered() ->
    Specs = operation_specs(),
    Allowed = allowed_canonicals(),
    #{
        queries => [Name || {query, Name, Canonical} <- Specs, allowed(Canonical, Allowed)],
        mutations => [Name || {mutation, Name, Canonical} <- Specs, allowed(Canonical, Allowed)]
    }.

-spec allowed(binary(), all | [binary()]) -> boolean().
allowed(_Canonical, all) ->
    true;
allowed(Canonical, Allowed) ->
    lists:member(Canonical, Allowed).

-spec allowed_canonicals() -> all | [binary()].
allowed_canonicals() ->
    case application:get_env(ipto, graphql_operation_allowlist) of
        {ok, Allowlist} when is_list(Allowlist), Allowlist =/= [] ->
            lists:usort([canonicalize(V) || V <- Allowlist]);
        _ ->
            all
    end.

-spec operation_specs() -> [{query | mutation, binary(), binary()}].
operation_specs() ->
    [
        {query, <<"unit">>, <<"unit">>},
        {query, <<"unitByCorrid">>, <<"unitbycorrid">>},
        {query, <<"unitExists">>, <<"unitexists">>},
        {query, <<"unitLocked">>, <<"unitlocked">>},
        {query, <<"units">>, <<"units">>},
        {query, <<"unitsByExpression">>, <<"unitsbyexpression">>},
        {query, <<"rightRelation">>, <<"rightrelation">>},
        {query, <<"rightRelations">>, <<"rightrelations">>},
        {query, <<"leftRelations">>, <<"leftrelations">>},
        {query, <<"countRightRelations">>, <<"countrightrelations">>},
        {query, <<"countLeftRelations">>, <<"countleftrelations">>},
        {query, <<"rightAssociation">>, <<"rightassociation">>},
        {query, <<"rightAssociations">>, <<"rightassociations">>},
        {query, <<"leftAssociations">>, <<"leftassociations">>},
        {query, <<"countRightAssociations">>, <<"countrightassociations">>},
        {query, <<"countLeftAssociations">>, <<"countleftassociations">>},
        {query, <<"tenantInfo">>, <<"tenantinfo">>},
        {query, <<"attributeInfo">>, <<"attributeinfo">>},
        {query, <<"inspectGraphqlSdl">>, <<"inspectgraphqlsdl">>},
        {query, <<"registeredOperations">>, <<"registeredoperations">>},
        {mutation, <<"createUnit">>, <<"createunit">>},
        {mutation, <<"inactivateUnit">>, <<"inactivateunit">>},
        {mutation, <<"activateUnit">>, <<"activateunit">>},
        {mutation, <<"requestStatusTransition">>, <<"requeststatustransition">>},
        {mutation, <<"transitionUnitStatus">>, <<"transitionunitstatus">>},
        {mutation, <<"lockUnit">>, <<"lockunit">>},
        {mutation, <<"unlockUnit">>, <<"unlockunit">>},
        {mutation, <<"addRelation">>, <<"addrelation">>},
        {mutation, <<"removeRelation">>, <<"removerelation">>},
        {mutation, <<"addAssociation">>, <<"addassociation">>},
        {mutation, <<"removeAssociation">>, <<"removeassociation">>},
        {mutation, <<"configureGraphqlSdl">>, <<"configuregraphqlsdl">>},
        {mutation, <<"configureGraphqlSdlFile">>, <<"configuregraphqlsdlfile">>}
    ].

-spec canonicalize(term()) -> binary().
canonicalize(Value) when is_atom(Value) ->
    canonicalize(atom_to_binary(Value, utf8));
canonicalize(Value) when is_list(Value) ->
    canonicalize(unicode:characters_to_binary(Value));
canonicalize(Value) when is_binary(Value) ->
    Lower = string:lowercase(binary_to_list(Value)),
    OnlyAlnum = [C || C <- Lower, (C >= $a andalso C =< $z) orelse (C >= $0 andalso C =< $9)],
    list_to_binary(OnlyAlnum);
canonicalize(_) ->
    <<>>.
