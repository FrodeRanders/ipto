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
-module(erepo_neo4j_integration_tests).

-include_lib("eunit/include/eunit.hrl").

neo4j_backend_roundtrip_test_() ->
    case os:getenv("EREPO_NEO4J_INTEGRATION") of
        "1" ->
            fun neo4j_backend_roundtrip/0;
        _ ->
            {"neo4j integration disabled (set EREPO_NEO4J_INTEGRATION=1)", fun() -> ok end}
    end.

neo4j_backend_roundtrip() ->
    {ok, _} = erepo:start_link(),
    %% Ensure backend override survives application load defaults.
    ok = application:set_env(erepo, backend, neo4j),

    TenantId = 9001,
    {ok, U0} = erepo:create_unit(TenantId, <<"neo4j-smoke">>),
    {ok, Stored1} = erepo:store_unit(U0),
    true = maps:is_key(unitid, Stored1),
    true = erepo:unit_exists(TenantId, maps:get(unitid, Stored1)),

    {ok, _Stored2} = erepo:store_unit_json(Stored1),
    {ok, Latest} = erepo:get_unit(TenantId, maps:get(unitid, Stored1)),
    2 = maps:get(unitver, Latest),

    AttrValues = [
        #{name => <<"neo.attr.text">>, value => <<"neo value">>},
        #{name => <<"neo.attr.int">>, value => 42},
        #{name => <<"neo.attr.record">>, value => #{kind => <<"R">>, level => 7, ok => true}}
    ],
    {ok, _} = erepo:create_attribute(<<"NSTR">>, <<"neo.attr.text">>, <<"neo.attr.text">>, 2, false),
    {ok, _} = erepo:create_attribute(<<"NINT">>, <<"neo.attr.int">>, <<"neo.attr.int">>, 4, false),
    {ok, _} = erepo:create_attribute(<<"NREC">>, <<"neo.attr.record">>, <<"neo.attr.record">>, 9, false),
    {ok, _Stored3} = erepo:store_unit_json(Latest#{attributes => AttrValues}),
    {ok, LatestWithAttrs} = erepo:get_unit(TenantId, maps:get(unitid, Stored1)),
    3 = maps:get(unitver, LatestWithAttrs),
    ?assertEqual(AttrValues, maps:get(attributes, LatestWithAttrs)),

    {ok, Search} = erepo:search_units(
        #{tenantid => TenantId, name_ilike => <<"%neo4j-smoke%">>},
        {created, desc},
        #{limit => 10, offset => 0}
    ),
    true = maps:get(total, Search) >= 1,

    {ok, B0} = erepo:create_unit(TenantId, <<"neo4j-related">>),
    {ok, B1} = erepo:store_unit(B0),
    ARef = #{tenantid => TenantId, unitid => maps:get(unitid, Stored1)},
    BRef = #{tenantid => TenantId, unitid => maps:get(unitid, B1)},

    ok = erepo:add_relation(ARef, 1, BRef),
    ok = erepo:remove_relation(ARef, 1, BRef),

    ok = erepo:add_association(ARef, 2, <<"neo4j:assoc">>),
    ok = erepo:remove_association(ARef, 2, <<"neo4j:assoc">>),

    ok = erepo:lock_unit(ARef, 30, <<"integration-lock">>),
    already_locked = erepo:lock_unit(ARef, 30, <<"integration-lock">>),
    ok = erepo:unlock_unit(ARef),

    ok = erepo:inactivate_unit(ARef),
    {ok, AfterInactivate} = erepo:get_unit(TenantId, maps:get(unitid, Stored1)),
    10 = maps:get(status, AfterInactivate),

    ok = erepo:activate_unit(ARef),
    {ok, AfterActivate} = erepo:get_unit(TenantId, maps:get(unitid, Stored1)),
    30 = maps:get(status, AfterActivate),

    {ok, Attr} = erepo:create_attribute(<<"NEO">>, <<"neo.attr">>, <<"neo.attr">>, 2, false),
    AttrId = maps:get(id, Attr),
    {ok, AttrByName} = erepo:get_attribute_info(<<"neo.attr">>),
    AttrId = maps:get(id, AttrByName),
    {ok, AttrById} = erepo:get_attribute_info(AttrId),
    AttrId = maps:get(id, AttrById),

    {ok, TenantById} = erepo:get_tenant_info(TenantId),
    TenantId = maps:get(id, TenantById),
    {ok, TenantByName} = erepo:get_tenant_info(maps:get(name, TenantById)),
    ?assertEqual(maps:get(name, TenantById), maps:get(name, TenantByName)).
