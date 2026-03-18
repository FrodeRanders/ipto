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

%% Guards the PostgreSQL roundtrip test behind an explicit integration flag.
pg_backend_roundtrip_test_() ->
    case os:getenv("IPTO_PG_INTEGRATION") of
        "1" ->
            fun pg_backend_roundtrip/0;
        _ ->
            {"pg integration disabled (set IPTO_PG_INTEGRATION=1)", fun() -> ok end}
    end.

%% Guards the PostgreSQL SDL persistence test behind the same integration flag.
pg_sdl_configure_persists_templates_test_() ->
    case os:getenv("IPTO_PG_INTEGRATION") of
        "1" ->
            fun pg_sdl_configure_persists_templates/0;
        _ ->
            {"pg integration disabled (set IPTO_PG_INTEGRATION=1)", fun() -> ok end}
    end.

%% Covers the PostgreSQL backend's end-to-end unit, relation, association,
%% search, and lock behavior against a real database.
pg_backend_roundtrip() ->
    application:set_env(ipto, backend, pg),
    {ok, _} = ipto:start_link(),
    TenantId = env_int("IPTO_PG_TEST_TENANT", 1),

    case ipto:get_tenant_info(TenantId) of
        not_found ->
            ok;
        {ok, _Tenant} ->
            ok = pg_reset_test_tenant(TenantId),
            {ok, Unit0} = ipto:create_unit(TenantId),
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

            OtherUnitId = maps:get(unitid, OtherStored),
            Lower = erlang:min(UnitId, OtherUnitId),
            Upper = erlang:max(UnitId, OtherUnitId),
            AssocRef = list_to_binary("case:pg-test:" ++ integer_to_list(erlang:system_time(microsecond) rem 1000000000)),
            CorpusQueries = [
                {"tenantid=~p and unitid in (~p, ~p)", [TenantId, UnitId, OtherUnitId], 2},
                {"tenantid=~p and unitid in (~p, ~p) and not unitid=~p", [TenantId, UnitId, OtherUnitId, OtherUnitId], 1},
                {"tenantid in (~p, ~p) and unitid in (~p, ~p)", [TenantId, TenantId + 1000, UnitId, OtherUnitId], 2},
                {"tenantid=~p and unitid in (~p, ~p) and unitid not in (~p)", [TenantId, UnitId, OtherUnitId, OtherUnitId], 1},
                {"tenantid=~p and unitid between ~p and ~p and unitid in (~p, ~p)", [TenantId, Lower, Upper, UnitId, OtherUnitId], 2}
            ],
            ok = verify_search_corpus(CorpusQueries),

            ok = ipto:add_association(UnitRef, 2, AssocRef),
            ok = ipto:add_association(OtherRef, 2, AssocRef),
            {ok, RightAssoc} = ipto:get_right_association(UnitRef, 2),
            {ok, RightAssocs} = ipto:get_right_associations(UnitRef, 2),
            {ok, LeftAssocs} = ipto:get_left_associations(2, AssocRef),
            {ok, 1} = ipto:count_right_associations(UnitRef, 2),
            {ok, 2} = ipto:count_left_associations(2, AssocRef),
            ?assertEqual(AssocRef, maps:get(assocstring, RightAssoc)),
            1 = length(RightAssocs),
            2 = length(LeftAssocs),
            ok = ipto:remove_association(UnitRef, 2, AssocRef),
            ok = ipto:remove_association(OtherRef, 2, AssocRef),
            ok
    end.

%% Replays a small search corpus and checks that every query returns the
%% expected total.
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

%% Verifies that SDL setup persists record and unit templates in PostgreSQL when
%% that backend is active.
pg_sdl_configure_persists_templates() ->
    application:set_env(ipto, backend, pg),
    {ok, _} = ipto:start_link(),

    Suffix = integer_to_list(erlang:system_time(microsecond) rem 1000000000),
    RecSymbol = list_to_binary("rec_" ++ Suffix),
    FieldSymbol = list_to_binary("fld_" ++ Suffix),
    RecAttrName = list_to_binary("ipto:pg:sdl:" ++ Suffix ++ ":record"),
    FieldAttrName = list_to_binary("ipto:pg:sdl:" ++ Suffix ++ ":field"),
    RecordTypeName = list_to_binary("Rec" ++ Suffix),
    TemplateTypeName = list_to_binary("Tpl" ++ Suffix),
    TemplateName = list_to_binary("tpl_" ++ Suffix),

    Sdl = iolist_to_binary(io_lib:format(
        "enum Attributes @attributeRegistry {\n"
        "  ~s @attribute(datatype: RECORD, array: false, name: \"~s\")\n"
        "  ~s @attribute(datatype: STRING, array: false, name: \"~s\")\n"
        "}\n"
        "type ~s @record(attribute: ~s) {\n"
        "  recordField: String @use(attribute: ~s)\n"
        "}\n"
        "type ~s @template(name: \"~s\") {\n"
        "  templateField: String @use(attribute: ~s)\n"
        "}\n",
        [
            RecSymbol, RecAttrName,
            FieldSymbol, FieldAttrName,
            RecordTypeName, RecSymbol, FieldSymbol,
            TemplateTypeName, TemplateName, FieldSymbol
        ]
    )),

    {ok, Summary} = ipto:configure_graphql_sdl(Sdl),
    RecordsSummary = maps:get(records, Summary),
    TemplatesSummary = maps:get(templates, Summary),
    true = maps:get(supported, RecordsSummary),
    true = maps:get(supported, TemplatesSummary),
    1 = maps:get(persisted, RecordsSummary),
    1 = maps:get(persisted, TemplatesSummary),

    {ok, Conn} = pg_connect(),
    try
        1 = pg_scalar_count(epgsql:equery(
            Conn,
            "SELECT count(*) "
            "FROM repo.repo_record_template rt "
            "JOIN repo.repo_attribute ra ON ra.attrid = rt.recordid "
            "WHERE ra.attrname = $1",
            [RecAttrName]
        )),
        1 = pg_scalar_count(epgsql:equery(
            Conn,
            "SELECT count(*) "
            "FROM repo.repo_record_template_elements rte "
            "JOIN repo.repo_record_template rt ON rt.recordid = rte.recordid "
            "JOIN repo.repo_attribute ra ON ra.attrid = rt.recordid "
            "WHERE ra.attrname = $1",
            [RecAttrName]
        )),
        1 = pg_scalar_count(epgsql:equery(
            Conn,
            "SELECT count(*) "
            "FROM repo.repo_unit_template "
            "WHERE name = $1",
            [TemplateName]
        )),
        1 = pg_scalar_count(epgsql:equery(
            Conn,
            "SELECT count(*) "
            "FROM repo.repo_unit_template_elements ute "
            "JOIN repo.repo_unit_template ut ON ut.templateid = ute.templateid "
            "WHERE ut.name = $1",
            [TemplateName]
        ))
    after
        epgsql:close(Conn)
    end.

%% Opens a direct PostgreSQL connection for verification queries used by the
%% integration tests.
-spec pg_connect() -> {ok, term()} | {error, term()}.
pg_connect() ->
    case code:ensure_loaded(epgsql) of
        {module, epgsql} ->
            Host = env_str("IPTO_PG_HOST", "localhost"),
            User = env_str("IPTO_PG_USER", "repo"),
            Pass = env_str("IPTO_PG_PASSWORD", "repo"),
            Db = env_str("IPTO_PG_DATABASE", "repo"),
            Port = env_int("IPTO_PG_PORT", 5432),
            epgsql:connect(Host, User, Pass, [{database, Db}, {port, Port}]);
        Error ->
            {error, Error}
    end.

%% Clears persisted units for the chosen tenant so the roundtrip test starts
%% from a predictable database state.
-spec pg_reset_test_tenant(pos_integer()) -> ok.
pg_reset_test_tenant(TenantId) ->
    {ok, Conn} = pg_connect(),
    try
        case epgsql:equery(Conn, "DELETE FROM repo.repo_unit_kernel WHERE tenantid = $1", [TenantId]) of
            {ok, _Count} ->
                ok;
            {ok, _Cols, _Rows} ->
                ok;
            Other ->
                erlang:error({pg_tenant_reset_failed, Other})
        end
    after
        epgsql:close(Conn)
    end.

%% Normalizes the different `epgsql` row shapes used by count queries into a
%% single integer result.
-spec pg_scalar_count(term()) -> non_neg_integer().
pg_scalar_count({ok, _Cols, [[Count] | _]}) when is_integer(Count), Count >= 0 ->
    Count;
pg_scalar_count({ok, _Cols, [Row | _]}) when is_tuple(Row) ->
    case tuple_to_list(Row) of
        [Count] when is_integer(Count), Count >= 0 -> Count;
        _ -> erlang:error({unexpected_pg_row, Row})
    end;
pg_scalar_count(Other) ->
    erlang:error({unexpected_pg_result, Other}).

-spec env_str(string(), string()) -> string().
env_str(Name, Default) ->
    case os:getenv(Name) of
        false -> Default;
        Value -> Value
    end.

-spec env_int(string(), integer()) -> integer().
env_int(Name, Default) ->
    case os:getenv(Name) of
        false -> Default;
        Value ->
            case string:to_integer(Value) of
                {I, _} -> I;
                _ -> Default
            end
    end.
