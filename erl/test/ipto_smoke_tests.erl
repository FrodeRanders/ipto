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
-module(ipto_smoke_tests).

-include_lib("eunit/include/eunit.hrl").

create_and_store_unit_test() ->
    application:set_env(ipto, backend, memory),
    {ok, _} = ipto:start_link(),
    {ok, Unit0} = ipto:create_unit(1),
    {ok, Stored} = ipto:store_unit(Unit0),
    true = maps:is_key(unitid, Stored),
    true = maps:is_key(unitver, Stored).

status_transition_rules_test() ->
    application:set_env(ipto, backend, memory),
    {ok, _} = ipto:start_link(),
    {ok, Unit0} = ipto:create_unit(2),
    {ok, Stored0} = ipto:store_unit(Unit0),
    UnitRef = #{tenantid => maps:get(tenantid, Stored0), unitid => maps:get(unitid, Stored0)},

    ok = ipto:inactivate_unit(UnitRef),
    {ok, AfterInactivate} = ipto:get_unit(maps:get(tenantid, Stored0), maps:get(unitid, Stored0)),
    10 = maps:get(status, AfterInactivate),

    ok = ipto:activate_unit(UnitRef),
    {ok, AfterActivate} = ipto:get_unit(maps:get(tenantid, Stored0), maps:get(unitid, Stored0)),
    30 = maps:get(status, AfterActivate).

relation_assoc_lock_test() ->
    application:set_env(ipto, backend, memory),
    {ok, _} = ipto:start_link(),
    {ok, A0} = ipto:create_unit(3),
    {ok, B0} = ipto:create_unit(3),
    {ok, A} = ipto:store_unit(A0),
    {ok, B} = ipto:store_unit(B0),

    ARef = #{tenantid => maps:get(tenantid, A), unitid => maps:get(unitid, A)},
    BRef = #{tenantid => maps:get(tenantid, B), unitid => maps:get(unitid, B)},

    ok = ipto:add_relation(ARef, 1, BRef),
    ok = ipto:remove_relation(ARef, 1, BRef),

    ok = ipto:add_association(ARef, 2, <<"case:123">>),
    ok = ipto:remove_association(ARef, 2, <<"case:123">>),

    ok = ipto:lock_unit(ARef, 30, <<"test">>),
    already_locked = ipto:lock_unit(ARef, 30, <<"test">>),
    ok = ipto:unlock_unit(ARef).

relation_assoc_query_test() ->
    application:set_env(ipto, backend, memory),
    {ok, _} = ipto:start_link(),
    ok = ipto_cache:flush(),

    {ok, A0} = ipto:create_unit(4),
    {ok, B0} = ipto:create_unit(4),
    {ok, A} = ipto:store_unit(A0),
    {ok, B} = ipto:store_unit(B0),

    ARef = #{tenantid => maps:get(tenantid, A), unitid => maps:get(unitid, A)},
    BRef = #{tenantid => maps:get(tenantid, B), unitid => maps:get(unitid, B)},

    ok = ipto:add_relation(ARef, 1, BRef),
    ok = ipto:add_association(ARef, 2, <<"case:123">>),
    ok = ipto:add_association(BRef, 2, <<"case:123">>),

    {ok, [RightRel]} = ipto:get_right_relations(ARef, 1),
    {ok, RightRelOne} = ipto:get_right_relation(ARef, 1),
    {ok, LeftRels} = ipto:get_left_relations(BRef, 1),
    {ok, 1} = ipto:count_right_relations(ARef, 1),
    {ok, 1} = ipto:count_left_relations(BRef, 1),

    ?assertEqual(maps:get(reltenantid, RightRel), maps:get(tenantid, B)),
    ?assertEqual(maps:get(relunitid, RightRel), maps:get(unitid, B)),
    ?assertEqual(RightRel, RightRelOne),
    1 = length(LeftRels),

    {ok, [RightAssoc]} = ipto:get_right_associations(ARef, 2),
    {ok, RightAssocOne} = ipto:get_right_association(ARef, 2),
    {ok, LeftAssocs} = ipto:get_left_associations(2, <<"case:123">>),
    {ok, 1} = ipto:count_right_associations(ARef, 2),
    {ok, 2} = ipto:count_left_associations(2, <<"case:123">>),

    ?assertEqual(<<"case:123">>, maps:get(assocstring, RightAssoc)),
    ?assertEqual(RightAssoc, RightAssocOne),
    2 = length(LeftAssocs).

memory_search_units_test() ->
    application:set_env(ipto, backend, memory),
    {ok, _} = ipto:start_link(),
    ok = ipto_cache:flush(),

    {ok, U0} = ipto:create_unit(42, <<"alpha-one">>),
    {ok, U1} = ipto:store_unit(U0),
    timer:sleep(1000),
    {ok, V2} = ipto:store_unit_json(U1),

    {ok, U2} = ipto:create_unit(42, <<"beta-two">>),
    {ok, _} = ipto:store_unit(U2),

    {ok, ByTenant} = ipto:search_units(#{tenantid => 42}, {created, desc}, #{limit => 10, offset => 0}),
    2 = maps:get(total, ByTenant),
    2 = length(maps:get(results, ByTenant)),

    {ok, ByUnit} = ipto:search_units(#{tenantid => 42, unitid => maps:get(unitid, V2)}, {created, desc}, 10),
    1 = maps:get(total, ByUnit),
    [AlphaLatest] = maps:get(results, ByUnit),
    2 = maps:get(unitver, AlphaLatest),

    {ok, ByNameLike} = ipto:search_units("tenantid=42 and name~\"%beta%\"", {created, desc}, 10),
    1 = maps:get(total, ByNameLike),
    [Only] = maps:get(results, ByNameLike),
    <<"beta-two">> = maps:get(unitname, Only).

attribute_roundtrip_mixed_types_test() ->
    application:set_env(ipto, backend, memory),
    {ok, _} = ipto:start_link(),
    ok = ipto_cache:flush(),

    {ok, _} = ipto:create_attribute(<<"STR">>, <<"attr.string">>, <<"attr.string">>, 2, false),
    {ok, _} = ipto:create_attribute(<<"INT">>, <<"attr.integer">>, <<"attr.integer">>, 4, false),
    {ok, _} = ipto:create_attribute(<<"BOOL">>, <<"attr.bool">>, <<"attr.bool">>, 1, false),
    {ok, _} = ipto:create_attribute(<<"REC">>, <<"attr.record">>, <<"attr.record">>, 9, false),

    Attributes = [
        #{name => <<"attr.string">>, value => <<"hello">>},
        #{name => <<"attr.integer">>, value => 12345},
        #{name => <<"attr.bool">>, value => true},
        #{
            name => <<"attr.record">>,
            value => #{
                <<"_type">> => <<"Address">>,
                <<"street">> => <<"Main Street 1">>,
                <<"zip">> => 12345,
                <<"active">> => true
            }
        }
    ],

    UnitMap = #{
        tenantid => 77,
        corrid => ipto_unit:make_corrid(),
        status => 30,
        unitname => <<"typed-attrs">>,
        attributes => Attributes
    },

    {ok, Stored} = ipto:store_unit_json(UnitMap),
    {ok, Loaded} = ipto:get_unit(maps:get(tenantid, Stored), maps:get(unitid, Stored)),

    ?assertEqual(Attributes, maps:get(attributes, Loaded)).
