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
-module(ipto_performance_tests).

-include_lib("eunit/include/eunit.hrl").

performance_large_test_() ->
    case os:getenv("IPTO_PERFORMANCE") of
        "1" ->
            {timeout, env_int("IPTO_PERF_TIMEOUT_SECS", 7200), fun performance_large/0};
        _ ->
            {"performance disabled (set IPTO_PERFORMANCE=1)", fun() -> ok end}
    end.

performance_large() ->
    Backend = env_backend("IPTO_PERF_BACKEND", memory),
    Units = env_int("IPTO_PERF_UNITS", 100000),
    TenantId = env_int("IPTO_PERF_TENANT", 1),
    Workers = env_int("IPTO_PERF_WORKERS", erlang:max(1, erlang:system_info(schedulers_online) div 2)),
    WorkerMode = env_worker_mode("IPTO_PERF_WORKER_MODE", range),
    SearchEvery = env_int("IPTO_PERF_SEARCH_EVERY", 10000),
    ProgressEvery = env_int("IPTO_PERF_PROGRESS_EVERY", 10000),

    ok = set_backend(Backend),
    {ok, _} = ipto:start_link(),
    ok = set_backend(Backend),
    ok = assert_backend(Backend),
    ok = assert_tenant_exists(TenantId),
    ok = ipto:reset_timing_data(),
    ok = maybe_reset_pg_pool_stats(Backend),

    {ok, EffectiveBackend} = application:get_env(ipto, backend),
    io:format("~n--- IPTO Erlang performance test ---~n", []),
    io:format(
        "backend(requested)=~p backend(effective)=~p units=~p tenant=~p workers=~p worker_mode=~p search_every=~p progress_every=~p~n",
        [Backend, EffectiveBackend, Units, TenantId, Workers, WorkerMode, SearchEvery, ProgressEvery]
    ),

    Prefix = integer_to_binary(erlang:system_time(microsecond)),
    ProgressCounter = init_progress_counter(),
    WallStartNs = erlang:monotonic_time(nanosecond),
    ok = run_parallel_store_and_search(
        Units, TenantId, Prefix, SearchEvery, ProgressEvery, Workers, WorkerMode, ProgressCounter
    ),
    SeedName = <<Prefix/binary, "-u-", (integer_to_binary(Units))/binary>>,
    ok = run_exact_search(TenantId, SeedName),
    WallElapsedMs = (erlang:monotonic_time(nanosecond) - WallStartNs) / 1000000.0,

    Stats = ipto:get_timing_data(),
    ?assert(maps:is_key(<<"create_unit_with_name_or_corrid">>, Stats)),
    ?assert(maps:is_key(<<"store_unit">>, Stats)),
    ?assert(maps:is_key(<<"search_units">>, Stats)),

    CreateStats = maps:get(<<"create_unit_with_name_or_corrid">>, Stats),
    StoreStats = maps:get(<<"store_unit">>, Stats),
    SearchStats = maps:get(<<"search_units">>, Stats),

    ?assertEqual(Units, maps:get(count, CreateStats)),
    ?assertEqual(Units, maps:get(count, StoreStats)),
    ?assertEqual(expected_search_count(Units, SearchEvery), maps:get(count, SearchStats)),

    WallSummary = wall_summary(Units, WallElapsedMs, CreateStats, StoreStats, SearchStats),
    mirror_to_log_file([WallSummary, "\n"]),
    logger:notice("~ts", [WallSummary]),

    Report = ipto:timing_report(),
    maybe_report_pg_pool_stats(Backend),
    mirror_to_log_file(["\n", Report, "\n"]),
    logger:notice("~n~ts~n", [Report]).

run_parallel_store_and_search(Units, TenantId, Prefix, SearchEvery, ProgressEvery, Workers0, Mode, ProgressCounter) ->
    Workers = normalize_workers(Units, Workers0),
    case Workers of
        1 ->
            _ = run_range(1, Units, Units, TenantId, Prefix, SearchEvery, ProgressEvery, ProgressCounter),
            ok;
        _ ->
            Parent = self(),
            Pids = spawn_workers(
                Mode, Parent, Units, TenantId, Prefix, SearchEvery, ProgressEvery, ProgressCounter, Workers
            ),
            collect_workers(Pids, #{})
    end.

spawn_workers(range, Parent, Units, TenantId, Prefix, SearchEvery, ProgressEvery, ProgressCounter, Workers) ->
    Ranges = build_ranges(Units, Workers),
    [
        spawn_monitor(fun() ->
            Result = run_range(
                StartIdx, EndIdx, Units, TenantId, Prefix, SearchEvery, ProgressEvery, ProgressCounter
            ),
            Parent ! {perf_worker_done, self(), Result}
        end)
     || {StartIdx, EndIdx} <- Ranges
    ];
spawn_workers(round_robin, Parent, Units, TenantId, Prefix, SearchEvery, ProgressEvery, ProgressCounter, Workers) ->
    Offsets = lists:seq(1, Workers),
    [
        spawn_monitor(fun() ->
            Result = run_round_robin(
                Offset, Workers, Units, TenantId, Prefix, SearchEvery, ProgressEvery, ProgressCounter
            ),
            Parent ! {perf_worker_done, self(), Result}
        end)
     || Offset <- Offsets
    ].

run_range(StartIdx, EndIdx, Units, TenantId, Prefix, SearchEvery, ProgressEvery, ProgressCounter) ->
    run_range_loop(StartIdx, EndIdx, Units, TenantId, Prefix, SearchEvery, ProgressEvery, ProgressCounter).

run_range_loop(Index, EndIdx, _Units, _TenantId, _Prefix, _SearchEvery, _ProgressEvery, _ProgressCounter)
  when Index > EndIdx ->
    ok;
run_range_loop(Index, EndIdx, Units, TenantId, Prefix, SearchEvery, ProgressEvery, ProgressCounter) ->
    Name = <<Prefix/binary, "-u-", (integer_to_binary(Index))/binary>>,
    {ok, Unit0} = ipto:create_unit(TenantId, Name),
    {ok, _Stored} = ipto:store_unit(Unit0),

    maybe_emit_progress(ProgressCounter, Units, ProgressEvery),
    maybe_search(TenantId, Name, Index, SearchEvery),

    run_range_loop(Index + 1, EndIdx, Units, TenantId, Prefix, SearchEvery, ProgressEvery, ProgressCounter).

run_round_robin(StartIdx, Step, Units, TenantId, Prefix, SearchEvery, ProgressEvery, ProgressCounter) ->
    run_round_robin_loop(StartIdx, Step, Units, TenantId, Prefix, SearchEvery, ProgressEvery, ProgressCounter).

run_round_robin_loop(Index, _Step, Units, _TenantId, _Prefix, _SearchEvery, _ProgressEvery, _ProgressCounter)
  when Index > Units ->
    ok;
run_round_robin_loop(Index, Step, Units, TenantId, Prefix, SearchEvery, ProgressEvery, ProgressCounter) ->
    Name = <<Prefix/binary, "-u-", (integer_to_binary(Index))/binary>>,
    {ok, Unit0} = ipto:create_unit(TenantId, Name),
    {ok, _Stored} = ipto:store_unit(Unit0),

    maybe_emit_progress(ProgressCounter, Units, ProgressEvery),
    maybe_search(TenantId, Name, Index, SearchEvery),

    run_round_robin_loop(Index + Step, Step, Units, TenantId, Prefix, SearchEvery, ProgressEvery, ProgressCounter).

collect_workers([], _Seen) ->
    ok;
collect_workers(Monitors, Seen) ->
    receive
        {perf_worker_done, Pid, ok} ->
            collect_workers(Monitors, Seen#{Pid => done});
        {perf_worker_done, _Pid, {error, Reason}} ->
            erlang:error({perf_worker_error, Reason});
        {'DOWN', Ref, process, _Pid, normal} ->
            collect_workers(lists:keydelete(Ref, 2, Monitors), Seen);
        {'DOWN', Ref, process, Pid, Reason} ->
            case maps:get(Pid, Seen, undefined) of
                done ->
                    collect_workers(lists:keydelete(Ref, 2, Monitors), Seen);
                _ ->
                    erlang:error({perf_worker_crash, #{pid => Pid, reason => Reason}})
            end
    end.

normalize_workers(_Units, Workers0) when Workers0 =< 1 ->
    1;
normalize_workers(Units, Workers0) ->
    erlang:min(Units, Workers0).

build_ranges(Units, Workers) ->
    Base = Units div Workers,
    Extra = Units rem Workers,
    build_ranges(1, Workers, Base, Extra, []).

build_ranges(_Start, 0, _Base, _Extra, Acc) ->
    lists:reverse(Acc);
build_ranges(Start, RemainingWorkers, Base, Extra, Acc) ->
    Size = Base + case Extra > 0 of true -> 1; false -> 0 end,
    EndIdx = Start + Size - 1,
    NextExtra = case Extra > 0 of true -> Extra - 1; false -> 0 end,
    build_ranges(EndIdx + 1, RemainingWorkers - 1, Base, NextExtra, [{Start, EndIdx} | Acc]).

init_progress_counter() ->
    atomics:new(1, []).

maybe_emit_progress(_ProgressCounter, _Units, ProgressEvery) when ProgressEvery =< 0 ->
    ok;
maybe_emit_progress(ProgressCounter, Units, ProgressEvery) ->
    Count = atomics:add_get(ProgressCounter, 1, 1),
    case Count rem ProgressEvery of
        0 ->
            Percent = (100.0 * Count) / Units,
            Line = lists:flatten(io_lib:format("progress: ~p / ~p (~.1f%)~n", [Count, Units, Percent])),
            mirror_to_log_file(Line),
            logger:notice("progress: ~p / ~p (~.1f%)", [Count, Units, Percent]);
        _ ->
            ok
    end.

maybe_search(TenantId, Name, Index, SearchEvery)
  when SearchEvery > 0, Index rem SearchEvery =:= 0 ->
    ok = run_exact_search(TenantId, Name);
maybe_search(_TenantId, _Name, _Index, _SearchEvery) ->
    ok.

run_exact_search(TenantId, Name) ->
    {ok, Search} = ipto:search_units(#{tenantid => TenantId, name => Name}, {created, desc}, #{limit => 10}),
    1 = maps:get(total, Search),
    [Only] = maps:get(results, Search),
    Name = maps:get(unitname, Only),
    ok.

expected_search_count(_Units, SearchEvery) when SearchEvery =< 0 ->
    1;
expected_search_count(Units, SearchEvery) ->
    (Units div SearchEvery) + 1.

env_int(Name, Default) ->
    case os:getenv(Name) of
        false ->
            Default;
        Value ->
            case string:to_integer(Value) of
                {Int, _} -> Int;
                _ -> Default
            end
    end.

env_backend(Name, Default) ->
    case os:getenv(Name) of
        "memory" -> memory;
        "pg" -> pg;
        "postgres" -> pg;
        "neo4j" -> neo4j;
        _ -> Default
    end.

env_worker_mode(Name, Default) ->
    case os:getenv(Name) of
        "range" -> range;
        "round_robin" -> round_robin;
        _ -> Default
    end.

set_backend(Backend) ->
    application:set_env(ipto, backend, Backend).

assert_backend(ExpectedBackend) ->
    case application:get_env(ipto, backend) of
        {ok, ExpectedBackend} ->
            ok;
        {ok, Other} ->
            erlang:error({unexpected_backend, #{expected => ExpectedBackend, actual => Other}});
        undefined ->
            erlang:error({unexpected_backend, #{expected => ExpectedBackend, actual => undefined}})
    end.

assert_tenant_exists(TenantId) ->
    case ipto:get_tenant_info(TenantId) of
        {ok, _Tenant} ->
            ok;
        not_found ->
            erlang:error({unknown_tenant, TenantId});
        {error, Reason} ->
            erlang:error({tenant_lookup_failed, #{tenantid => TenantId, reason => Reason}})
    end.

maybe_reset_pg_pool_stats(pg) ->
    ipto_pg_pool:reset_stats();
maybe_reset_pg_pool_stats(_Other) ->
    ok.

maybe_report_pg_pool_stats(pg) ->
    PoolStats = ipto_pg_pool:get_stats(),
    mirror_to_log_file(lists:flatten(io_lib:format("postgres pool stats: ~p~n", [PoolStats]))),
    logger:notice("postgres pool stats: ~p", [PoolStats]);
maybe_report_pg_pool_stats(_Other) ->
    ok.

mirror_to_log_file(Text) ->
    case os:getenv("IPTO_LOG_FILE") of
        false ->
            ok;
        Path ->
            _ = file:write_file(Path, Text, [append]),
            ok
    end.

wall_summary(Units, WallElapsedMs, CreateStats, StoreStats, SearchStats) ->
    StoreTotalMs = maps:get(mean, StoreStats) * maps:get(count, StoreStats),
    CreateTotalMs = maps:get(mean, CreateStats) * maps:get(count, CreateStats),
    SearchTotalMs = maps:get(mean, SearchStats) * maps:get(count, SearchStats),
    AggregateTotalMs = StoreTotalMs + CreateTotalMs + SearchTotalMs,
    UnitsPerSec = case WallElapsedMs > 0.0 of
        true -> (1000.0 * Units) / WallElapsedMs;
        false -> 0.0
    end,
    StoreConcurrency = case WallElapsedMs > 0.0 of
        true -> StoreTotalMs / WallElapsedMs;
        false -> 0.0
    end,
    AggregateConcurrency = case WallElapsedMs > 0.0 of
        true -> AggregateTotalMs / WallElapsedMs;
        false -> 0.0
    end,
    lists:flatten(io_lib:format(
        "wall_clock: elapsed=~.2f s throughput=~.1f units/s eff_concurrency(store)=~.2f eff_concurrency(total)=~.2f",
        [WallElapsedMs / 1000.0, UnitsPerSec, StoreConcurrency, AggregateConcurrency]
    )).
