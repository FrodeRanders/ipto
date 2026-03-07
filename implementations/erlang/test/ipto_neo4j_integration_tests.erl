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
-module(ipto_neo4j_integration_tests).

-include_lib("eunit/include/eunit.hrl").

neo4j_backend_roundtrip_test_() ->
    case os:getenv("IPTO_NEO4J_INTEGRATION") of
        "1" ->
            fun neo4j_backend_roundtrip/0;
        _ ->
            {"neo4j integration disabled (set IPTO_NEO4J_INTEGRATION=1)", fun() -> ok end}
    end.

neo4j_backend_roundtrip() ->
    {ok, _} = ipto:start_link(),
    %% Ensure backend override survives application load defaults.
    ok = application:set_env(ipto, backend, neo4j),

    Tag = integer_to_binary(erlang:system_time(microsecond) rem 1000000000),
    TenantName = <<"neo4j-it-", Tag/binary>>,
    {ok, TenantInfo0} = ipto:get_tenant_info(TenantName),
    TenantId = maps:get(id, TenantInfo0),
    AssocRef = <<"neo4j:assoc:", Tag/binary>>,
    {ok, U0} = ipto:create_unit(TenantId, <<"neo4j-smoke">>),
    {ok, Stored1} = ipto:store_unit(U0),
    true = maps:is_key(unitid, Stored1),
    true = ipto:unit_exists(TenantId, maps:get(unitid, Stored1)),

    {ok, _Stored2} = ipto:store_unit_json(Stored1),
    {ok, Latest} = ipto:get_unit(TenantId, maps:get(unitid, Stored1)),
    2 = maps:get(unitver, Latest),

    AttrValues = [
        #{name => <<"neo.attr.text">>, value => <<"neo value">>},
        #{name => <<"neo.attr.int">>, value => 42},
        #{name => <<"neo.attr.record">>, value => #{kind => <<"R">>, level => 7, ok => true}}
    ],
    {ok, _} = ipto:create_attribute(<<"NSTR">>, <<"neo.attr.text">>, <<"neo.attr.text">>, 2, false),
    {ok, _} = ipto:create_attribute(<<"NINT">>, <<"neo.attr.int">>, <<"neo.attr.int">>, 4, false),
    {ok, _} = ipto:create_attribute(<<"NREC">>, <<"neo.attr.record">>, <<"neo.attr.record">>, 9, false),
    {ok, _Stored3} = ipto:store_unit_json(Latest#{attributes => AttrValues}),
    {ok, LatestWithAttrs} = ipto:get_unit(TenantId, maps:get(unitid, Stored1)),
    3 = maps:get(unitver, LatestWithAttrs),
    ?assertEqual(AttrValues, maps:get(attributes, LatestWithAttrs)),

    {ok, Search} = ipto:search_units(
        #{tenantid => TenantId, name_ilike => <<"%neo4j-smoke%">>},
        {created, desc},
        #{limit => 10, offset => 0}
    ),
    true = maps:get(total, Search) >= 1,

    {ok, B0} = ipto:create_unit(TenantId, <<"neo4j-related">>),
    {ok, B1} = ipto:store_unit(B0),
    ARef = #{tenantid => TenantId, unitid => maps:get(unitid, Stored1)},
    BRef = #{tenantid => TenantId, unitid => maps:get(unitid, B1)},

    UnitAId = maps:get(unitid, Stored1),
    UnitBId = maps:get(unitid, B1),
    Lower = erlang:min(UnitAId, UnitBId),
    Upper = erlang:max(UnitAId, UnitBId),
    SearchCorpus = [
        {"tenantid=~p and unitid in (~p, ~p)", [TenantId, UnitAId, UnitBId], 2},
        {"tenantid=~p and unitid in (~p, ~p) and not unitid=~p", [TenantId, UnitAId, UnitBId, UnitBId], 1},
        {"tenantid in (~p, ~p) and unitid in (~p, ~p)", [TenantId, TenantId + 1000, UnitAId, UnitBId], 2},
        {"tenantid=~p and unitid in (~p, ~p) and unitid not in (~p)", [TenantId, UnitAId, UnitBId, UnitBId], 1},
        {"tenantid=~p and unitid between ~p and ~p and unitid in (~p, ~p)", [TenantId, Lower, Upper, UnitAId, UnitBId], 2}
    ],
    ok = verify_search_corpus(SearchCorpus),

    ok = ipto:add_relation(ARef, 1, BRef),
    {ok, RightRel} = ipto:get_right_relation(ARef, 1),
    {ok, RightRels} = ipto:get_right_relations(ARef, 1),
    {ok, LeftRels} = ipto:get_left_relations(BRef, 1),
    {ok, 1} = ipto:count_right_relations(ARef, 1),
    {ok, 1} = ipto:count_left_relations(BRef, 1),
    ?assertEqual(maps:get(relunitid, RightRel), maps:get(unitid, B1)),
    1 = length(RightRels),
    1 = length(LeftRels),
    ok = ipto:remove_relation(ARef, 1, BRef),

    ok = ipto:add_association(ARef, 2, AssocRef),
    ok = ipto:add_association(BRef, 2, AssocRef),
    {ok, RightAssoc} = ipto:get_right_association(ARef, 2),
    {ok, RightAssocs} = ipto:get_right_associations(ARef, 2),
    {ok, LeftAssocs} = ipto:get_left_associations(2, AssocRef),
    {ok, 1} = ipto:count_right_associations(ARef, 2),
    {ok, 2} = ipto:count_left_associations(2, AssocRef),
    ?assertEqual(AssocRef, maps:get(assocstring, RightAssoc)),
    1 = length(RightAssocs),
    2 = length(LeftAssocs),
    ok = ipto:remove_association(ARef, 2, AssocRef),
    ok = ipto:remove_association(BRef, 2, AssocRef),

    ok = ipto:lock_unit(ARef, 30, <<"integration-lock">>),
    already_locked = ipto:lock_unit(ARef, 30, <<"integration-lock">>),
    ok = ipto:unlock_unit(ARef),

    ok = ipto:inactivate_unit(ARef),
    {ok, AfterInactivate} = ipto:get_unit(TenantId, maps:get(unitid, Stored1)),
    10 = maps:get(status, AfterInactivate),

    ok = ipto:activate_unit(ARef),
    {ok, AfterActivate} = ipto:get_unit(TenantId, maps:get(unitid, Stored1)),
    30 = maps:get(status, AfterActivate),

    {ok, Attr} = ipto:create_attribute(<<"NEO">>, <<"neo.attr">>, <<"neo.attr">>, 2, false),
    AttrId = maps:get(id, Attr),
    {ok, AttrByName} = ipto:get_attribute_info(<<"neo.attr">>),
    AttrId = maps:get(id, AttrByName),
    {ok, AttrById} = ipto:get_attribute_info(AttrId),
    AttrId = maps:get(id, AttrById),

    {ok, TenantById} = ipto:get_tenant_info(TenantId),
    TenantId = maps:get(id, TenantById),
    {ok, TenantByName} = ipto:get_tenant_info(TenantName),
    ?assertEqual(maps:get(name, TenantById), maps:get(name, TenantByName)).

-spec verify_search_corpus([{string(), [term()], non_neg_integer()}]) -> ok.
verify_search_corpus(CorpusQueries) ->
    lists:foreach(
        fun({Fmt, Args, ExpectedTotal}) ->
            Query = iolist_to_binary(io_lib:format(Fmt, Args)),
            {ok, Result} = ipto:search_units(Query, {created, desc}, #{limit => 100}),
            ExpectedTotal = maps:get(total, Result)
        end,
        CorpusQueries
    ),
    ok.
