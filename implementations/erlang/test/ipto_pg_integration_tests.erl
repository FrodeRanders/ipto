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
-module(ipto_pg_integration_tests).

-include_lib("eunit/include/eunit.hrl").

pg_backend_roundtrip_test_() ->
    case os:getenv("IPTO_PG_INTEGRATION") of
        "1" ->
            fun pg_backend_roundtrip/0;
        _ ->
            {"pg integration disabled (set IPTO_PG_INTEGRATION=1)", fun() -> ok end}
    end.

pg_backend_roundtrip() ->
    application:set_env(ipto, backend, pg),
    {ok, _} = ipto:start_link(),

    case ipto:get_tenant_info(1) of
        not_found ->
            ok;
        {ok, _Tenant} ->
            {ok, Unit0} = ipto:create_unit(1),
            {ok, Stored} = ipto:store_unit(Unit0),
            TenantId = maps:get(tenantid, Stored),
            UnitId = maps:get(unitid, Stored),

            {ok, Reloaded} = ipto:get_unit(TenantId, UnitId),
            UnitId = maps:get(unitid, Reloaded),

            UnitRef = #{tenantid => TenantId, unitid => UnitId},
            ok = ipto:lock_unit(UnitRef, 30, <<"pg-test">>),
            already_locked = ipto:lock_unit(UnitRef, 30, <<"pg-test">>),
            ok = ipto:unlock_unit(UnitRef),

            {ok, Other0} = ipto:create_unit(1),
            {ok, OtherStored} = ipto:store_unit(Other0),
            OtherRef = #{tenantid => maps:get(tenantid, OtherStored), unitid => maps:get(unitid, OtherStored)},
            ok = ipto:add_relation(UnitRef, 1, OtherRef),
            {ok, RightRel} = ipto:get_right_relation(UnitRef, 1),
            {ok, RightRels} = ipto:get_right_relations(UnitRef, 1),
            {ok, LeftRels} = ipto:get_left_relations(OtherRef, 1),
            {ok, 1} = ipto:count_right_relations(UnitRef, 1),
            {ok, 1} = ipto:count_left_relations(OtherRef, 1),
            ?assertEqual(maps:get(relunitid, RightRel), maps:get(unitid, OtherStored)),
            1 = length(RightRels),
            1 = length(LeftRels),
            ok = ipto:remove_relation(UnitRef, 1, OtherRef),

            Query = iolist_to_binary(io_lib:format("tenantid=~p and unitid=~p", [TenantId, UnitId])),
            {ok, SearchResult} = ipto:search_units(Query, {created, desc}, #{limit => 10}),
            true = maps:is_key(total, SearchResult),
            true = maps:is_key(results, SearchResult),
            ok = ipto:add_association(UnitRef, 2, <<"case:pg-test">>),
            ok = ipto:add_association(OtherRef, 2, <<"case:pg-test">>),
            {ok, RightAssoc} = ipto:get_right_association(UnitRef, 2),
            {ok, RightAssocs} = ipto:get_right_associations(UnitRef, 2),
            {ok, LeftAssocs} = ipto:get_left_associations(2, <<"case:pg-test">>),
            {ok, 1} = ipto:count_right_associations(UnitRef, 2),
            {ok, 2} = ipto:count_left_associations(2, <<"case:pg-test">>),
            ?assertEqual(<<"case:pg-test">>, maps:get(assocstring, RightAssoc)),
            1 = length(RightAssocs),
            2 = length(LeftAssocs),
            ok = ipto:remove_association(UnitRef, 2, <<"case:pg-test">>),
            ok = ipto:remove_association(OtherRef, 2, <<"case:pg-test">>),
            ok
    end.
