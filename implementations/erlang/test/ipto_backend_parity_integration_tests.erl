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
-module(ipto_backend_parity_integration_tests).

-include_lib("eunit/include/eunit.hrl").

backend_search_parity_test_() ->
    case {os:getenv("IPTO_BACKEND_PARITY"), os:getenv("IPTO_NEO4J_INTEGRATION")} of
        {"1", "1"} ->
            fun backend_search_parity/0;
        {"1", _} ->
            {"backend parity requires neo4j integration (set IPTO_NEO4J_INTEGRATION=1)", fun() -> ok end};
        _ ->
            {"backend parity integration disabled (set IPTO_BACKEND_PARITY=1)", fun() -> ok end}
    end.

backend_search_parity() ->
    {ok, _} = ipto:start_link(),

    Tag = integer_to_list(erlang:system_time(microsecond) rem 1000000000),
    PgTenantId = resolve_pg_tenant(),
    NeoTenantId = 9100 + (erlang:system_time(second) rem 700),

    PgFixture = seed_backend(pg, PgTenantId, Tag),
    NeoFixture = seed_backend(neo4j, NeoTenantId, Tag),

    PgCounts = run_corpus(pg, PgFixture),
    NeoCounts = run_corpus(neo4j, NeoFixture),
    ?assertEqual(PgCounts, NeoCounts).

-spec resolve_pg_tenant() -> pos_integer().
resolve_pg_tenant() ->
    ok = application:set_env(ipto, backend, pg),
    case ipto:get_tenant_info(1) of
        {ok, _} ->
            1;
        _ ->
            erlang:error(pg_tenant_missing)
    end.

-spec seed_backend(pg | neo4j, pos_integer(), string()) -> map().
seed_backend(Backend, TenantId, Tag) ->
    ok = application:set_env(ipto, backend, Backend),
    NameA = list_to_binary("parity-a-" ++ Tag ++ "-" ++ atom_to_list(Backend)),
    NameB = list_to_binary("parity-b-" ++ Tag ++ "-" ++ atom_to_list(Backend)),
    {ok, A0} = ipto:create_unit(TenantId, NameA),
    {ok, A1} = ipto:store_unit(A0),
    {ok, B0} = ipto:create_unit(TenantId, NameB),
    {ok, B1} = ipto:store_unit(B0),
    #{
        backend => Backend,
        tenantid => TenantId,
        unitid_a => maps:get(unitid, A1),
        unitid_b => maps:get(unitid, B1),
        name_a => NameA,
        name_b => NameB
    }.

-spec run_corpus(pg | neo4j, map()) -> [non_neg_integer()].
run_corpus(Backend, Fixture) ->
    ok = application:set_env(ipto, backend, Backend),
    TenantId = maps:get(tenantid, Fixture),
    UnitA = maps:get(unitid_a, Fixture),
    UnitB = maps:get(unitid_b, Fixture),
    NameA = maps:get(name_a, Fixture),
    NameB = maps:get(name_b, Fixture),
    Lower = erlang:min(UnitA, UnitB),
    Upper = erlang:max(UnitA, UnitB),
    Corpus = [
        {"tenantid=~p and (name=\"~ts\" or name=\"~ts\")", [TenantId, NameA, NameB], 2},
        {"tenantid=~p and not name=\"~ts\" and name=\"~ts\"", [TenantId, NameB, NameA], 1},
        {"tenantid in (~p, ~p) and status in (30, 40)", [TenantId, TenantId + 1000], 2},
        {"tenantid=~p and unitid not in (~p)", [TenantId, UnitB], 1},
        {"tenantid=~p and unitid between ~p and ~p", [TenantId, Lower, Upper], 2}
    ],
    [
        begin
            Query = iolist_to_binary(io_lib:format(Fmt, Args)),
            {ok, Result} = ipto:search_units(Query, {created, desc}, #{limit => 100}),
            Total = maps:get(total, Result),
            ?assertEqual(Expected, Total),
            Expected
        end
        || {Fmt, Args, Expected} <- Corpus
    ].
