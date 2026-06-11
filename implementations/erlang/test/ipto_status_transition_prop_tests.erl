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
-module(ipto_status_transition_prop_tests).

-include_lib("eunit/include/eunit.hrl").

-include("ipto.hrl").

-define(N, 200).

all_allowed_transitions_test_() ->
    Allowed = [
        {?STATUS_EFFECTIVE, ?STATUS_PENDING_DELETION},
        {?STATUS_EFFECTIVE, ?STATUS_PENDING_DISPOSITION},
        {?STATUS_PENDING_DELETION, ?STATUS_PENDING_DISPOSITION},
        {?STATUS_PENDING_DELETION, ?STATUS_EFFECTIVE},
        {?STATUS_OBLITERATED, ?STATUS_PENDING_DISPOSITION},
        {?STATUS_OBLITERATED, ?STATUS_EFFECTIVE}
    ],
    [
        ?_assert(ipto_repo:allowed_transition(From, To))
        || {From, To} <- Allowed
    ].

disallowed_transitions_test_() ->
    Disallowed = [
        {?STATUS_ARCHIVED, ?STATUS_EFFECTIVE},
        {?STATUS_ARCHIVED, ?STATUS_PENDING_DELETION},
        {?STATUS_ARCHIVED, ?STATUS_PENDING_DISPOSITION},
        {?STATUS_ARCHIVED, ?STATUS_OBLITERATED},
        {?STATUS_EFFECTIVE, ?STATUS_ARCHIVED},
        {?STATUS_EFFECTIVE, ?STATUS_OBLITERATED},
        {?STATUS_PENDING_DELETION, ?STATUS_ARCHIVED},
        {?STATUS_PENDING_DISPOSITION, ?STATUS_ARCHIVED},
        {?STATUS_OBLITERATED, ?STATUS_ARCHIVED},
        {?STATUS_OBLITERATED, ?STATUS_PENDING_DELETION}
    ],
    [
        ?_assertNot(ipto_repo:allowed_transition(From, To))
        || {From, To} <- Disallowed
    ].

archived_is_terminal_test_() ->
    States = [?STATUS_PENDING_DISPOSITION, ?STATUS_PENDING_DELETION,
              ?STATUS_OBLITERATED, ?STATUS_EFFECTIVE, ?STATUS_ARCHIVED],
    [
        ?_assertNot(ipto_repo:allowed_transition(?STATUS_ARCHIVED, S))
        || S <- States
    ].

self_transitions_are_disallowed_test_() ->
    States = [?STATUS_PENDING_DISPOSITION, ?STATUS_PENDING_DELETION,
              ?STATUS_OBLITERATED, ?STATUS_EFFECTIVE, ?STATUS_ARCHIVED],
    [
        ?_assertNot(ipto_repo:allowed_transition(S, S))
        || S <- States
    ].

random_known_state_always_well_defined_test_() ->
    States = [?STATUS_PENDING_DISPOSITION, ?STATUS_PENDING_DELETION,
              ?STATUS_OBLITERATED, ?STATUS_EFFECTIVE, ?STATUS_ARCHIVED],
    Pairs = random_state_pairs(States, ?N),
    [
        begin
            IsAllowed = ipto_repo:allowed_transition(From, To),
            ?_assert(is_boolean(IsAllowed))
        end
        || {From, To} <- Pairs
    ].

effective_from_specific_states_only_test_() ->
    OnlyFrom = [?STATUS_PENDING_DELETION, ?STATUS_OBLITERATED],
    AllStates = [?STATUS_PENDING_DISPOSITION, ?STATUS_PENDING_DELETION,
                 ?STATUS_OBLITERATED, ?STATUS_EFFECTIVE, ?STATUS_ARCHIVED],
    [
        begin
            case lists:member(From, OnlyFrom) of
                true -> ?_assert(ipto_repo:allowed_transition(From, ?STATUS_EFFECTIVE));
                false -> ?_assertNot(ipto_repo:allowed_transition(From, ?STATUS_EFFECTIVE))
            end
        end
        || From <- AllStates
    ].

pending_disposition_accessible_from_correct_states_test_() ->
    DisallowedFrom = [?STATUS_PENDING_DISPOSITION, ?STATUS_ARCHIVED],
    AllowedFrom = [?STATUS_OBLITERATED, ?STATUS_PENDING_DELETION, ?STATUS_EFFECTIVE],
    [
        ?_assertNot(ipto_repo:allowed_transition(F, ?STATUS_PENDING_DISPOSITION))
        || F <- DisallowedFrom
    ] ++ [
        ?_assert(ipto_repo:allowed_transition(F, ?STATUS_PENDING_DISPOSITION))
        || F <- AllowedFrom
    ].

memory_transition_roundtrip_test() ->
    application:set_env(ipto, backend, memory),
    {ok, _} = ipto:start_link(),
    Unit = #{
        tenantid => 1,
        unitname => <<"transition-test">>,
        status => ?STATUS_EFFECTIVE
    },
    {ok, Stored} = ipto_repo:store_unit_json(Unit),
    TenantId = maps:get(tenantid, Stored),
    UnitId = maps:get(unitid, Stored),
    Ref = {TenantId, UnitId},

    {ok, ?STATUS_PENDING_DELETION} = ipto:request_status_transition(Ref, ?STATUS_PENDING_DELETION),
    {ok, ?STATUS_EFFECTIVE} = ipto:request_status_transition(Ref, ?STATUS_EFFECTIVE),
    {ok, ?STATUS_PENDING_DISPOSITION} = ipto:request_status_transition(Ref, ?STATUS_PENDING_DISPOSITION),
    {ok, ?STATUS_PENDING_DISPOSITION} = ipto:request_status_transition(Ref, ?STATUS_ARCHIVED),
    {ok, ?STATUS_PENDING_DISPOSITION} = ipto:request_status_transition(Ref, ?STATUS_OBLITERATED),
    ok.

disallowed_transition_returns_current_status_test() ->
    application:set_env(ipto, backend, memory),
    {ok, _} = ipto:start_link(),
    Unit = #{tenantid => 2, unitname => <<"noop-test">>, status => ?STATUS_ARCHIVED},
    {ok, Stored} = ipto_repo:store_unit_json(Unit),
    Ref = {maps:get(tenantid, Stored), maps:get(unitid, Stored)},
    {ok, ?STATUS_ARCHIVED} = ipto:request_status_transition(Ref, ?STATUS_EFFECTIVE),
    {ok, ?STATUS_ARCHIVED} = ipto:request_status_transition(Ref, ?STATUS_PENDING_DELETION),
    ok.

random_transition_sequence_never_crashes_test() ->
    application:set_env(ipto, backend, memory),
    {ok, _} = ipto:start_link(),
    Unit = #{tenantid => 3, unitname => <<"fuzz-test">>, status => ?STATUS_EFFECTIVE},
    {ok, Stored} = ipto_repo:store_unit_json(Unit),
    Ref = {maps:get(tenantid, Stored), maps:get(unitid, Stored)},
    Results = random_transitions(Ref, ?N, []),
    ?assert(lists:all(fun({ok, _}) -> true; (_) -> false end, Results)).

%% Helpers

random_state_pairs(States, N) ->
    NStates = length(States),
    [{lists:nth(rand:uniform(NStates), States), lists:nth(rand:uniform(NStates), States)}
     || _ <- lists:seq(1, N)].

random_transitions(_Ref, 0, Acc) ->
    Acc;
random_transitions(Ref, N, Acc) ->
    NextStatus = element(rand:uniform(5), {
        ?STATUS_EFFECTIVE,
        ?STATUS_PENDING_DELETION,
        ?STATUS_PENDING_DISPOSITION,
        ?STATUS_OBLITERATED,
        ?STATUS_ARCHIVED
    }),
    Result = ipto:request_status_transition(Ref, NextStatus),
    random_transitions(Ref, N - 1, [Result | Acc]).
