-module(erepo_pg_integration_tests).

-include_lib("eunit/include/eunit.hrl").

pg_backend_roundtrip_test_() ->
    case os:getenv("EREPO_PG_INTEGRATION") of
        "1" ->
            fun pg_backend_roundtrip/0;
        _ ->
            {"pg integration disabled (set EREPO_PG_INTEGRATION=1)", fun() -> ok end}
    end.

pg_backend_roundtrip() ->
    application:set_env(erepo, backend, pg),
    {ok, _} = erepo:start_link(),

    case erepo:get_tenant_info(1) of
        not_found ->
            ok;
        {ok, _Tenant} ->
            {ok, Unit0} = erepo:create_unit(1),
            {ok, Stored} = erepo:store_unit(Unit0),
            TenantId = maps:get(tenantid, Stored),
            UnitId = maps:get(unitid, Stored),

            {ok, Reloaded} = erepo:get_unit(TenantId, UnitId),
            UnitId = maps:get(unitid, Reloaded),

            UnitRef = #{tenantid => TenantId, unitid => UnitId},
            ok = erepo:lock_unit(UnitRef, 30, <<"pg-test">>),
            already_locked = erepo:lock_unit(UnitRef, 30, <<"pg-test">>),
            ok = erepo:unlock_unit(UnitRef),

            ok = erepo:add_association(UnitRef, 2, <<"case:pg-test">>),
            ok = erepo:remove_association(UnitRef, 2, <<"case:pg-test">>),

            {ok, Other0} = erepo:create_unit(1),
            {ok, OtherStored} = erepo:store_unit(Other0),
            OtherRef = #{tenantid => maps:get(tenantid, OtherStored), unitid => maps:get(unitid, OtherStored)},
            ok = erepo:add_relation(UnitRef, 1, OtherRef),
            ok = erepo:remove_relation(UnitRef, 1, OtherRef),

            Query = iolist_to_binary(io_lib:format("tenantid=~p and unitid=~p", [TenantId, UnitId])),
            {ok, SearchResult} = erepo:search_units(Query, {created, desc}, #{limit => 10}),
            true = maps:is_key(total, SearchResult),
            true = maps:is_key(results, SearchResult),
            ok
    end.
