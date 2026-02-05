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
-module(erepo_db).

-include("erepo.hrl").

-export([
    get_unit_json/3,
    unit_exists/2,
    store_unit_json/1,
    search_units/3,
    add_relation/3,
    remove_relation/3,
    get_right_relation/2,
    get_right_relations/2,
    get_left_relations/2,
    count_right_relations/2,
    count_left_relations/2,
    add_association/3,
    remove_association/3,
    get_right_association/2,
    get_right_associations/2,
    get_left_associations/2,
    count_right_associations/2,
    count_left_associations/2,
    lock_unit/3,
    unlock_unit/1,
    set_status/2,
    create_attribute/5,
    get_attribute_info/1,
    get_tenant_info/1
]).

%% Internal adapters used by backend modules while refactoring.
-export([
    memory_get_unit_json_backend/3,
    memory_unit_exists_backend/2,
    memory_store_unit_json_backend/1,
    memory_search_units_backend/3,
    memory_add_relation_backend/3,
    memory_remove_relation_backend/3,
    memory_get_right_relation_backend/2,
    memory_get_right_relations_backend/2,
    memory_get_left_relations_backend/2,
    memory_count_right_relations_backend/2,
    memory_count_left_relations_backend/2,
    memory_add_association_backend/3,
    memory_remove_association_backend/3,
    memory_get_right_association_backend/2,
    memory_get_right_associations_backend/2,
    memory_get_left_associations_backend/2,
    memory_count_right_associations_backend/2,
    memory_count_left_associations_backend/2,
    memory_lock_unit_backend/3,
    memory_unlock_unit_backend/1,
    memory_set_status_backend/2,
    memory_create_attribute_backend/5,
    memory_get_attribute_info_backend/1,
    memory_get_tenant_info_backend/1,
    pg_get_unit_json_backend/3,
    pg_unit_exists_backend/2,
    pg_store_unit_json_backend/1,
    pg_search_units_backend/3,
    pg_add_relation_backend/3,
    pg_remove_relation_backend/3,
    pg_get_right_relation_backend/2,
    pg_get_right_relations_backend/2,
    pg_get_left_relations_backend/2,
    pg_count_right_relations_backend/2,
    pg_count_left_relations_backend/2,
    pg_add_association_backend/3,
    pg_remove_association_backend/3,
    pg_get_right_association_backend/2,
    pg_get_right_associations_backend/2,
    pg_get_left_associations_backend/2,
    pg_count_right_associations_backend/2,
    pg_count_left_associations_backend/2,
    pg_lock_unit_backend/3,
    pg_unlock_unit_backend/1,
    pg_set_status_backend/2,
    pg_create_attribute_backend/5,
    pg_get_attribute_info_backend/1,
    pg_get_tenant_info_backend/1
]).

-define(ATTR_SEQ_KEY, {meta, attr_seq}).
-define(ATTR_BY_NAME_KEY, {meta, attrs_by_name}).
-define(ATTR_BY_ID_KEY, {meta, attrs_by_id}).
-define(REL_KEY, {meta, relations}).
-define(ASSOC_KEY, {meta, associations}).
-define(LOCK_KEY, {meta, locks}).

%% Public API

-spec get_unit_json(tenantid(), unitid(), version_selector()) -> unit_lookup_result().
get_unit_json(TenantId, UnitId, Version) ->
    call_backend(get_unit_json, [TenantId, UnitId, Version]).

-spec unit_exists(tenantid(), unitid()) -> boolean().
unit_exists(TenantId, UnitId) ->
    call_backend(unit_exists, [TenantId, UnitId]).

-spec store_unit_json(unit_map()) -> erepo_result(unit_map()).
store_unit_json(UnitMap) when is_map(UnitMap) ->
    case validate_store_input(UnitMap) of
        ok ->
            call_backend(store_unit_json, [UnitMap]);
        Error ->
            Error
    end;
store_unit_json(_UnitMap) ->
    {error, invalid_unit_map}.

-spec search_units(search_expression() | map(), search_order(), search_paging()) -> erepo_result(search_result()).
search_units(Expression, Order, PagingOrLimit) ->
    call_backend(search_units, [Expression, Order, PagingOrLimit]).

-spec add_relation(unit_ref_value(), relation_type(), unit_ref_value()) -> ok | {error, erepo_reason()}.
add_relation(UnitRef, RelType, OtherUnitRef) ->
    call_backend(add_relation, [UnitRef, RelType, OtherUnitRef]).

-spec remove_relation(unit_ref_value(), relation_type(), unit_ref_value()) -> ok | {error, erepo_reason()}.
remove_relation(UnitRef, RelType, OtherUnitRef) ->
    call_backend(remove_relation, [UnitRef, RelType, OtherUnitRef]).

-spec get_right_relation(unit_ref_value(), relation_type()) -> relation_lookup_result().
get_right_relation(UnitRef, RelType) ->
    call_backend(get_right_relation, [UnitRef, RelType]).

-spec get_right_relations(unit_ref_value(), relation_type()) -> erepo_result([relation()]).
get_right_relations(UnitRef, RelType) ->
    call_backend(get_right_relations, [UnitRef, RelType]).

-spec get_left_relations(unit_ref_value(), relation_type()) -> erepo_result([relation()]).
get_left_relations(UnitRef, RelType) ->
    call_backend(get_left_relations, [UnitRef, RelType]).

-spec count_right_relations(unit_ref_value(), relation_type()) -> erepo_result(non_neg_integer()).
count_right_relations(UnitRef, RelType) ->
    call_backend(count_right_relations, [UnitRef, RelType]).

-spec count_left_relations(unit_ref_value(), relation_type()) -> erepo_result(non_neg_integer()).
count_left_relations(UnitRef, RelType) ->
    call_backend(count_left_relations, [UnitRef, RelType]).

-spec add_association(unit_ref_value(), association_type(), ref_string()) -> ok | {error, erepo_reason()}.
add_association(UnitRef, AssocType, RefString) ->
    call_backend(add_association, [UnitRef, AssocType, RefString]).

-spec remove_association(unit_ref_value(), association_type(), ref_string()) -> ok | {error, erepo_reason()}.
remove_association(UnitRef, AssocType, RefString) ->
    call_backend(remove_association, [UnitRef, AssocType, RefString]).

-spec get_right_association(unit_ref_value(), association_type()) -> association_lookup_result().
get_right_association(UnitRef, AssocType) ->
    call_backend(get_right_association, [UnitRef, AssocType]).

-spec get_right_associations(unit_ref_value(), association_type()) -> erepo_result([association()]).
get_right_associations(UnitRef, AssocType) ->
    call_backend(get_right_associations, [UnitRef, AssocType]).

-spec get_left_associations(association_type(), ref_string()) -> erepo_result([association()]).
get_left_associations(AssocType, RefString) ->
    call_backend(get_left_associations, [AssocType, RefString]).

-spec count_right_associations(unit_ref_value(), association_type()) -> erepo_result(non_neg_integer()).
count_right_associations(UnitRef, AssocType) ->
    call_backend(count_right_associations, [UnitRef, AssocType]).

-spec count_left_associations(association_type(), ref_string()) -> erepo_result(non_neg_integer()).
count_left_associations(AssocType, RefString) ->
    call_backend(count_left_associations, [AssocType, RefString]).

-spec lock_unit(unit_ref_value(), lock_type(), ref_string()) -> ok | already_locked | {error, erepo_reason()}.
lock_unit(UnitRef, LockType, Purpose) ->
    call_backend(lock_unit, [UnitRef, LockType, Purpose]).

-spec unlock_unit(unit_ref_value()) -> ok | {error, erepo_reason()}.
unlock_unit(UnitRef) ->
    call_backend(unlock_unit, [UnitRef]).

-spec set_status(unit_ref_value(), unit_status()) -> ok | {error, erepo_reason()}.
set_status(UnitRef, Status) when is_integer(Status) ->
    call_backend(set_status, [UnitRef, Status]);
set_status(_UnitRef, _Status) ->
    {error, invalid_status}.

-spec create_attribute(attribute_alias(), attribute_name(), attribute_qualname(), attribute_type(), boolean()) ->
    erepo_result(attribute_info()).
create_attribute(Alias, Name, QualName, Type, IsArray) ->
    call_backend(create_attribute, [Alias, Name, QualName, Type, IsArray]).

-spec get_attribute_info(name_or_id()) -> {ok, attribute_info()} | not_found | {error, erepo_reason()}.
get_attribute_info(NameOrId) ->
    call_backend(get_attribute_info, [NameOrId]).

-spec get_tenant_info(name_or_id()) -> {ok, tenant_info()} | not_found | {error, erepo_reason()}.
get_tenant_info(NameOrId) ->
    call_backend(get_tenant_info, [NameOrId]).

%% Backend selection and config

backend() ->
    case application:get_env(erepo, backend) of
        {ok, pg} -> pg;
        {ok, postgres} -> pg;
        {ok, neo4j} -> neo4j;
        _ -> memory
    end.

backend_module() ->
    case backend() of
        pg -> erepo_db_pg;
        neo4j -> erepo_db_neo4j;
        memory -> erepo_db_memory
    end.

call_backend(Fun, Args) ->
    Module = backend_module(),
    apply(Module, Fun, Args).

pg_conn_opts() ->
    Host = env_str("EREPO_PG_HOST", "localhost"),
    User = env_str("EREPO_PG_USER", "repo"),
    Pass = env_str("EREPO_PG_PASSWORD", "repo"),
    Db = env_str("EREPO_PG_DATABASE", "repo"),
    Port = env_int("EREPO_PG_PORT", 5432),
    #{host => Host, user => User, pass => Pass, db => Db, port => Port}.

env_str(Name, Default) ->
    case os:getenv(Name) of
        false -> Default;
        Value -> Value
    end.

env_int(Name, Default) ->
    case os:getenv(Name) of
        false -> Default;
        Value ->
            case string:to_integer(Value) of
                {I, _} -> I;
                _ -> Default
            end
    end.

with_pg(Fun) ->
    case code:ensure_loaded(epgsql) of
        {module, epgsql} ->
            Opts = pg_conn_opts(),
            Host = maps:get(host, Opts),
            User = maps:get(user, Opts),
            Pass = maps:get(pass, Opts),
            ConnOpts = [{database, maps:get(db, Opts)}, {port, maps:get(port, Opts)}],
            case epgsql:connect(Host, User, Pass, ConnOpts) of
                {ok, Conn} ->
                    try
                        Fun(Conn)
                    after
                        epgsql:close(Conn)
                    end;
                Error ->
                    Error
            end;
        _ ->
            {error, {missing_dependency, epgsql}}
    end.

%% PG implementation (best-effort against current schema/procedures)

pg_get_unit_json(TenantId, UnitId, latest) ->
    pg_get_unit_json(TenantId, UnitId, -1);
pg_get_unit_json(TenantId, UnitId, Version) when is_integer(Version), Version /= 0 ->
    with_pg(fun(Conn) ->
        case pg_get_unit_json_proc(Conn, TenantId, UnitId, Version) of
            {ok, _} = Ok ->
                Ok;
            _ ->
                pg_get_unit_json_sql(Conn, TenantId, UnitId, Version)
        end
    end);
pg_get_unit_json(_TenantId, _UnitId, _Version) ->
    {error, invalid_version}.

pg_get_unit_json_proc(Conn, TenantId, UnitId, Version) ->
    Sql = "CALL repo.extract_unit_json($1::integer, $2::bigint, $3::integer, NULL::jsonb)",
    case query_rows(epgsql:equery(Conn, Sql, [TenantId, UnitId, Version])) of
        {ok, []} ->
            not_found;
        {ok, [Row | _]} ->
            [Json] = row_values(Row),
            if
                Json =:= null -> not_found;
                true -> {ok, json_to_map(Json)}
            end;
        Error ->
            {error, Error}
    end.

pg_get_unit_json_sql(Conn, TenantId, UnitId, Version) ->
    Sql =
        "SELECT uk.tenantid, uk.unitid, uv.unitver, uk.lastver, uk.corrid, uk.status, uk.created, uv.modified, uv.unitname"
        "  FROM repo.repo_unit_kernel uk"
        "  JOIN repo.repo_unit_version uv"
        "    ON (uk.tenantid = uv.tenantid AND uk.unitid = uv.unitid"
        "        AND uv.unitver = CASE WHEN $3 = -1 THEN uk.lastver ELSE $3 END)"
        " WHERE uk.tenantid = $1 AND uk.unitid = $2",
    case query_rows(epgsql:equery(Conn, Sql, [TenantId, UnitId, Version])) of
        {ok, []} -> not_found;
        {ok, [Row | _]} -> {ok, row_to_unit_map_with_lastver(Row)};
        Error -> {error, Error}
    end.

pg_unit_exists(TenantId, UnitId) ->
    with_pg(fun(Conn) ->
        Sql = "SELECT 1 FROM repo.repo_unit_kernel WHERE tenantid = $1 AND unitid = $2",
        case query_rows(epgsql:equery(Conn, Sql, [TenantId, UnitId])) of
            {ok, []} -> false;
            {ok, _Rows} -> true;
            _ -> false
        end
    end).

pg_search_units(Expression, Order, PagingOrLimit) ->
    with_pg(fun(Conn) ->
        {WhereSql, Params0, NextIdx} = build_where(Expression),
        OrderSql = build_order(Order),
        {PageSql, PageParams} = build_paging(PagingOrLimit, NextIdx),
        Params = Params0 ++ PageParams,

        BaseFrom = " FROM repo.repo_unit_kernel uk"
                " JOIN repo.repo_unit_version uv"
                "   ON (uk.tenantid = uv.tenantid AND uk.unitid = uv.unitid AND uk.lastver = uv.unitver)",

        CountSql = "SELECT count(*)" ++ BaseFrom ++ WhereSql,
        case query_rows(epgsql:equery(Conn, CountSql, Params0)) of
            {ok, [CountRow | _]} ->
                [Total] = row_values(CountRow),
                SelectSql =
                    "SELECT uk.tenantid, uk.unitid, uv.unitver, uk.corrid, uk.status, uk.created, uv.modified, uv.unitname" ++
                    BaseFrom ++
                    WhereSql ++
                    OrderSql ++
                    PageSql,
                case query_rows(epgsql:equery(Conn, SelectSql, Params)) of
                    {ok, Rows} ->
                        Results = [row_to_unit_map(Row) || Row <- Rows],
                        {ok, #{results => Results, total => Total}};
                    Error2 ->
                        {error, Error2}
                end;
            Error1 ->
                {error, Error1}
        end
    end).

pg_store_unit_json(UnitMap) ->
    with_pg(fun(Conn) ->
        case pg_store_unit_json_proc(Conn, UnitMap) of
            {ok, _} = Ok ->
                Ok;
            _ ->
                case maps:get(unitid, UnitMap, undefined) of
                    undefined ->
                        pg_store_new_unit(Conn, UnitMap);
                    _ ->
                        pg_store_new_version(Conn, UnitMap)
                end
        end
    end).

pg_store_unit_json_proc(Conn, UnitMap) ->
    JsonText = map_to_json(UnitMap),
    case maps:get(unitid, UnitMap, undefined) of
        undefined ->
            Sql = "CALL repo.ingest_new_unit_json($1::text, NULL, NULL, NULL, NULL)",
            case query_rows(epgsql:equery(Conn, Sql, [JsonText])) of
                {ok, [Row | _]} ->
                    [UnitId, UnitVer, Created, Modified] = row_values(Row),
                    {ok, UnitMap#{
                        unitid => UnitId,
                        unitver => UnitVer,
                        created => Created,
                        modified => Modified,
                        isreadonly => false
                    }};
                Error ->
                    {error, Error}
            end;
        _ ->
            Sql = "CALL repo.ingest_new_version_json($1::text, NULL, NULL)",
            case query_rows(epgsql:equery(Conn, Sql, [JsonText])) of
                {ok, [Row | _]} ->
                    [UnitVer, Modified] = row_values(Row),
                    {ok, UnitMap#{
                        unitver => UnitVer,
                        modified => Modified,
                        isreadonly => false
                    }};
                Error ->
                    {error, Error}
            end
    end.

pg_set_status(UnitRef, Status) ->
    case normalize_ref(UnitRef) of
        {TenantId, UnitId} ->
            with_pg(fun(Conn) ->
                Sql = "UPDATE repo.repo_unit_kernel SET status = $1 WHERE tenantid = $2 AND unitid = $3",
                case epgsql:equery(Conn, Sql, [Status, TenantId, UnitId]) of
                    {ok, Count} when is_integer(Count), Count > 0 -> ok;
                    {ok, _} -> {error, not_found};
                    Error -> {error, Error}
                end
            end);
        _ ->
            {error, invalid_unit_ref}
    end.

pg_add_relation(UnitRef, RelType, OtherUnitRef) when is_integer(RelType) ->
    case {normalize_ref(UnitRef), normalize_ref(OtherUnitRef)} of
        {{TenantId, UnitId}, {RelTenantId, RelUnitId}} ->
            with_pg(fun(Conn) ->
                Sql = "INSERT INTO repo.repo_internal_relation (tenantid, unitid, reltype, reltenantid, relunitid) VALUES ($1, $2, $3, $4, $5) ON CONFLICT DO NOTHING",
                case epgsql:equery(Conn, Sql, [TenantId, UnitId, RelType, RelTenantId, RelUnitId]) of
                    {ok, _Count} -> ok;
                    {ok, _Cols, _Rows} -> ok;
                    Error -> {error, Error}
                end
            end);
        _ ->
            {error, invalid_relation_ref}
    end;
pg_add_relation(_UnitRef, _RelType, _OtherUnitRef) ->
    {error, invalid_relation_type}.

pg_remove_relation(UnitRef, RelType, OtherUnitRef) when is_integer(RelType) ->
    case {normalize_ref(UnitRef), normalize_ref(OtherUnitRef)} of
        {{TenantId, UnitId}, {RelTenantId, RelUnitId}} ->
            with_pg(fun(Conn) ->
                Sql = "DELETE FROM repo.repo_internal_relation WHERE tenantid = $1 AND unitid = $2 AND reltype = $3 AND reltenantid = $4 AND relunitid = $5",
                case epgsql:equery(Conn, Sql, [TenantId, UnitId, RelType, RelTenantId, RelUnitId]) of
                    {ok, _Count} -> ok;
                    {ok, _Cols, _Rows} -> ok;
                    Error -> {error, Error}
                end
            end);
        _ ->
            {error, invalid_relation_ref}
    end;
pg_remove_relation(_UnitRef, _RelType, _OtherUnitRef) ->
    {error, invalid_relation_type}.

pg_get_right_relation(UnitRef, RelType) when is_integer(RelType) ->
    case normalize_ref(UnitRef) of
        {TenantId, UnitId} ->
            with_pg(fun(Conn) ->
                Sql = "SELECT tenantid, unitid, reltype, reltenantid, relunitid FROM repo.repo_internal_relation WHERE tenantid = $1 AND unitid = $2 AND reltype = $3 LIMIT 1",
                case query_rows(epgsql:equery(Conn, Sql, [TenantId, UnitId, RelType])) of
                    {ok, []} ->
                        not_found;
                    {ok, [Row | _]} ->
                        case relation_from_row(row_values(Row)) of
                            #{} -> {error, invalid_relation_row};
                            Rel -> {ok, Rel}
                        end;
                    Error ->
                        {error, Error}
                end
            end);
        _ ->
            {error, invalid_unit_ref}
    end;
pg_get_right_relation(_UnitRef, _RelType) ->
    {error, invalid_relation_type}.

pg_get_right_relations(UnitRef, RelType) when is_integer(RelType) ->
    case normalize_ref(UnitRef) of
        {TenantId, UnitId} ->
            with_pg(fun(Conn) ->
                Sql = "SELECT tenantid, unitid, reltype, reltenantid, relunitid FROM repo.repo_internal_relation WHERE tenantid = $1 AND unitid = $2 AND reltype = $3",
                case query_rows(epgsql:equery(Conn, Sql, [TenantId, UnitId, RelType])) of
                    {ok, Rows} ->
                        Relations = [relation_from_row(row_values(Row)) || Row <- Rows],
                        {ok, [R || R <- Relations, R =/= #{}]};
                    Error ->
                        {error, Error}
                end
            end);
        _ ->
            {error, invalid_unit_ref}
    end;
pg_get_right_relations(_UnitRef, _RelType) ->
    {error, invalid_relation_type}.

pg_get_left_relations(UnitRef, RelType) when is_integer(RelType) ->
    case normalize_ref(UnitRef) of
        {TenantId, UnitId} ->
            with_pg(fun(Conn) ->
                Sql = "SELECT tenantid, unitid, reltype, reltenantid, relunitid FROM repo.repo_internal_relation WHERE reltenantid = $1 AND relunitid = $2 AND reltype = $3",
                case query_rows(epgsql:equery(Conn, Sql, [TenantId, UnitId, RelType])) of
                    {ok, Rows} ->
                        Relations = [relation_from_row(row_values(Row)) || Row <- Rows],
                        {ok, [R || R <- Relations, R =/= #{}]};
                    Error ->
                        {error, Error}
                end
            end);
        _ ->
            {error, invalid_unit_ref}
    end;
pg_get_left_relations(_UnitRef, _RelType) ->
    {error, invalid_relation_type}.

pg_count_right_relations(UnitRef, RelType) when is_integer(RelType) ->
    case normalize_ref(UnitRef) of
        {TenantId, UnitId} ->
            with_pg(fun(Conn) ->
                Sql = "SELECT COUNT(*) FROM repo.repo_internal_relation WHERE tenantid = $1 AND unitid = $2 AND reltype = $3",
                case query_rows(epgsql:equery(Conn, Sql, [TenantId, UnitId, RelType])) of
                    {ok, []} ->
                        {ok, 0};
                    {ok, [Row | _]} ->
                        [Count] = row_values(Row),
                        {ok, Count};
                    Error ->
                        {error, Error}
                end
            end);
        _ ->
            {error, invalid_unit_ref}
    end;
pg_count_right_relations(_UnitRef, _RelType) ->
    {error, invalid_relation_type}.

pg_count_left_relations(UnitRef, RelType) when is_integer(RelType) ->
    case normalize_ref(UnitRef) of
        {TenantId, UnitId} ->
            with_pg(fun(Conn) ->
                Sql = "SELECT COUNT(*) FROM repo.repo_internal_relation WHERE reltenantid = $1 AND relunitid = $2 AND reltype = $3",
                case query_rows(epgsql:equery(Conn, Sql, [TenantId, UnitId, RelType])) of
                    {ok, []} ->
                        {ok, 0};
                    {ok, [Row | _]} ->
                        [Count] = row_values(Row),
                        {ok, Count};
                    Error ->
                        {error, Error}
                end
            end);
        _ ->
            {error, invalid_unit_ref}
    end;
pg_count_left_relations(_UnitRef, _RelType) ->
    {error, invalid_relation_type}.

pg_add_association(UnitRef, AssocType, RefString) when is_integer(AssocType), (is_binary(RefString) orelse is_list(RefString)) ->
    case normalize_ref(UnitRef) of
        {TenantId, UnitId} ->
            AssocString = normalize_string(RefString),
            with_pg(fun(Conn) ->
                Sql = "INSERT INTO repo.repo_external_assoc (tenantid, unitid, assoctype, assocstring) VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING",
                case epgsql:equery(Conn, Sql, [TenantId, UnitId, AssocType, AssocString]) of
                    {ok, _Count} -> ok;
                    {ok, _Cols, _Rows} -> ok;
                    Error -> {error, Error}
                end
            end);
        _ ->
            {error, invalid_association_ref}
    end;
pg_add_association(_UnitRef, _AssocType, _RefString) ->
    {error, invalid_association}.

pg_remove_association(UnitRef, AssocType, RefString) when is_integer(AssocType), (is_binary(RefString) orelse is_list(RefString)) ->
    case normalize_ref(UnitRef) of
        {TenantId, UnitId} ->
            AssocString = normalize_string(RefString),
            with_pg(fun(Conn) ->
                Sql = "DELETE FROM repo.repo_external_assoc WHERE tenantid = $1 AND unitid = $2 AND assoctype = $3 AND assocstring = $4",
                case epgsql:equery(Conn, Sql, [TenantId, UnitId, AssocType, AssocString]) of
                    {ok, _Count} -> ok;
                    {ok, _Cols, _Rows} -> ok;
                    Error -> {error, Error}
                end
            end);
        _ ->
            {error, invalid_association_ref}
    end;
pg_remove_association(_UnitRef, _AssocType, _RefString) ->
    {error, invalid_association}.

pg_get_right_association(UnitRef, AssocType) when is_integer(AssocType) ->
    case normalize_ref(UnitRef) of
        {TenantId, UnitId} ->
            with_pg(fun(Conn) ->
                Sql = "SELECT tenantid, unitid, assoctype, assocstring FROM repo.repo_external_assoc WHERE tenantid = $1 AND unitid = $2 AND assoctype = $3 LIMIT 1",
                case query_rows(epgsql:equery(Conn, Sql, [TenantId, UnitId, AssocType])) of
                    {ok, []} ->
                        not_found;
                    {ok, [Row | _]} ->
                        case association_from_row(row_values(Row)) of
                            #{} -> {error, invalid_association_row};
                            Assoc -> {ok, Assoc}
                        end;
                    Error ->
                        {error, Error}
                end
            end);
        _ ->
            {error, invalid_unit_ref}
    end;
pg_get_right_association(_UnitRef, _AssocType) ->
    {error, invalid_association}.

pg_get_right_associations(UnitRef, AssocType) when is_integer(AssocType) ->
    case normalize_ref(UnitRef) of
        {TenantId, UnitId} ->
            with_pg(fun(Conn) ->
                Sql = "SELECT tenantid, unitid, assoctype, assocstring FROM repo.repo_external_assoc WHERE tenantid = $1 AND unitid = $2 AND assoctype = $3",
                case query_rows(epgsql:equery(Conn, Sql, [TenantId, UnitId, AssocType])) of
                    {ok, Rows} ->
                        Assocs = [association_from_row(row_values(Row)) || Row <- Rows],
                        {ok, [A || A <- Assocs, A =/= #{}]};
                    Error ->
                        {error, Error}
                end
            end);
        _ ->
            {error, invalid_unit_ref}
    end;
pg_get_right_associations(_UnitRef, _AssocType) ->
    {error, invalid_association}.

pg_get_left_associations(AssocType, RefString) when is_integer(AssocType), (is_binary(RefString) orelse is_list(RefString)) ->
    AssocString = normalize_string(RefString),
    with_pg(fun(Conn) ->
        Sql = "SELECT tenantid, unitid, assoctype, assocstring FROM repo.repo_external_assoc WHERE assoctype = $1 AND assocstring = $2",
        case query_rows(epgsql:equery(Conn, Sql, [AssocType, AssocString])) of
            {ok, Rows} ->
                Assocs = [association_from_row(row_values(Row)) || Row <- Rows],
                {ok, [A || A <- Assocs, A =/= #{}]};
            Error ->
                {error, Error}
        end
    end);
pg_get_left_associations(_AssocType, _RefString) ->
    {error, invalid_association}.

pg_count_right_associations(UnitRef, AssocType) when is_integer(AssocType) ->
    case normalize_ref(UnitRef) of
        {TenantId, UnitId} ->
            with_pg(fun(Conn) ->
                Sql = "SELECT COUNT(*) FROM repo.repo_external_assoc WHERE tenantid = $1 AND unitid = $2 AND assoctype = $3",
                case query_rows(epgsql:equery(Conn, Sql, [TenantId, UnitId, AssocType])) of
                    {ok, []} ->
                        {ok, 0};
                    {ok, [Row | _]} ->
                        [Count] = row_values(Row),
                        {ok, Count};
                    Error ->
                        {error, Error}
                end
            end);
        _ ->
            {error, invalid_unit_ref}
    end;
pg_count_right_associations(_UnitRef, _AssocType) ->
    {error, invalid_association}.

pg_count_left_associations(AssocType, RefString) when is_integer(AssocType), (is_binary(RefString) orelse is_list(RefString)) ->
    AssocString = normalize_string(RefString),
    with_pg(fun(Conn) ->
        Sql = "SELECT COUNT(*) FROM repo.repo_external_assoc WHERE assoctype = $1 AND assocstring = $2",
        case query_rows(epgsql:equery(Conn, Sql, [AssocType, AssocString])) of
            {ok, []} ->
                {ok, 0};
            {ok, [Row | _]} ->
                [Count] = row_values(Row),
                {ok, Count};
            Error ->
                {error, Error}
        end
    end);
pg_count_left_associations(_AssocType, _RefString) ->
    {error, invalid_association}.

pg_lock_unit(UnitRef, LockType, Purpose) when is_integer(LockType) ->
    case normalize_ref(UnitRef) of
        {TenantId, UnitId} ->
            with_pg(fun(Conn) ->
                ExistsSql = "SELECT 1 FROM repo.repo_lock WHERE tenantid = $1 AND unitid = $2 FETCH FIRST 1 ROWS ONLY",
                case query_rows(epgsql:equery(Conn, ExistsSql, [TenantId, UnitId])) of
                    {ok, [_ | _]} ->
                        already_locked;
                    {ok, []} ->
                        InsertSql = "INSERT INTO repo.repo_lock (tenantid, unitid, purpose, locktype, expire) VALUES ($1, $2, $3, $4, $5)",
                        case epgsql:equery(Conn, InsertSql, [TenantId, UnitId, normalize_purpose(Purpose), LockType, null]) of
                            {ok, _Count} -> ok;
                            {ok, _InsertCols, _InsertRows} -> ok;
                            Error -> {error, Error}
                        end;
                    Error ->
                        {error, Error}
                end
            end);
        _ ->
            {error, invalid_unit_ref}
    end;
pg_lock_unit(_UnitRef, _LockType, _Purpose) ->
    {error, invalid_lock_type}.

pg_unlock_unit(UnitRef) ->
    case normalize_ref(UnitRef) of
        {TenantId, UnitId} ->
            with_pg(fun(Conn) ->
                Sql = "DELETE FROM repo.repo_lock WHERE tenantid = $1 AND unitid = $2",
                case epgsql:equery(Conn, Sql, [TenantId, UnitId]) of
                    {ok, _Count} -> ok;
                    {ok, _Cols, _Rows} -> ok;
                    Error -> {error, Error}
                end
            end);
        _ ->
            {error, invalid_unit_ref}
    end.

pg_create_attribute(Alias, Name, QualName, Type, IsArray)
  when is_binary(Name), is_binary(QualName), is_integer(Type), is_boolean(IsArray) ->
    with_pg(fun(Conn) ->
        Sql = "INSERT INTO repo.repo_attribute (attrtype, scalar, attrname, qualname, alias) VALUES ($1, $2, $3, $4, $5) RETURNING attrid, alias, attrname, qualname, attrtype, scalar",
        Params = [Type, not IsArray, to_pg_text(Name), to_pg_text(QualName), maybe_null_text(Alias)],
        case query_rows(epgsql:equery(Conn, Sql, Params)) of
            {ok, [Row | _]} ->
                {ok, attr_row_to_info(Row)};
            {error, _} ->
                pg_get_attribute_info(Name);
            Error ->
                {error, Error}
        end
    end);
pg_create_attribute(_Alias, _Name, _QualName, _Type, _IsArray) ->
    {error, invalid_attribute_definition}.

pg_get_attribute_info(NameOrId) when is_binary(NameOrId) ->
    with_pg(fun(Conn) ->
        Sql = "SELECT attrid, alias, attrname, qualname, attrtype, scalar FROM repo.repo_attribute WHERE attrname = $1 OR alias = $1 OR qualname = $1",
        case query_rows(epgsql:equery(Conn, Sql, [to_pg_text(NameOrId)])) of
            {ok, [Row | _]} -> {ok, attr_row_to_info(Row)};
            {ok, []} -> not_found;
            Error -> {error, Error}
        end
    end);
pg_get_attribute_info(NameOrId) when is_integer(NameOrId), NameOrId > 0 ->
    with_pg(fun(Conn) ->
        Sql = "SELECT attrid, alias, attrname, qualname, attrtype, scalar FROM repo.repo_attribute WHERE attrid = $1",
        case query_rows(epgsql:equery(Conn, Sql, [NameOrId])) of
            {ok, [Row | _]} -> {ok, attr_row_to_info(Row)};
            {ok, []} -> not_found;
            Error -> {error, Error}
        end
    end);
pg_get_attribute_info(_NameOrId) ->
    {error, invalid_attribute_id_or_name}.

pg_get_tenant_info(NameOrId) when is_integer(NameOrId), NameOrId > 0 ->
    with_pg(fun(Conn) ->
        Sql = "SELECT tenantid, name, description, created FROM repo.repo_tenant WHERE tenantid = $1",
        case query_rows(epgsql:equery(Conn, Sql, [NameOrId])) of
            {ok, [Row | _]} ->
                [Id, Name, Description, Created] = row_values(Row),
                {ok, #{id => Id, name => Name, description => Description, created => Created}};
            {ok, []} ->
                not_found;
            Error ->
                {error, Error}
        end
    end);
pg_get_tenant_info(NameOrId) when is_binary(NameOrId) ->
    with_pg(fun(Conn) ->
        Sql = "SELECT tenantid, name, description, created FROM repo.repo_tenant WHERE lower(name) = lower($1)",
        case query_rows(epgsql:equery(Conn, Sql, [to_pg_text(NameOrId)])) of
            {ok, [Row | _]} ->
                [Id, Name, Description, Created] = row_values(Row),
                {ok, #{id => Id, name => Name, description => Description, created => Created}};
            {ok, []} ->
                not_found;
            Error ->
                {error, Error}
        end
    end);
pg_get_tenant_info(_NameOrId) ->
    {error, invalid_tenant_id_or_name}.

%% Backend adapter exports (temporary bridge during backend module split)
-spec memory_get_unit_json_backend(tenantid(), unitid(), version_selector()) -> unit_lookup_result().
-spec memory_unit_exists_backend(tenantid(), unitid()) -> boolean().
-spec memory_store_unit_json_backend(unit_map()) -> erepo_result(unit_map()).
-spec memory_search_units_backend(search_expression() | map(), search_order(), search_paging()) -> erepo_result(search_result()).
-spec memory_add_relation_backend(unit_ref_value(), relation_type(), unit_ref_value()) -> ok | {error, erepo_reason()}.
-spec memory_remove_relation_backend(unit_ref_value(), relation_type(), unit_ref_value()) -> ok | {error, erepo_reason()}.
-spec memory_get_right_relation_backend(unit_ref_value(), relation_type()) -> relation_lookup_result().
-spec memory_get_right_relations_backend(unit_ref_value(), relation_type()) -> erepo_result([relation()]).
-spec memory_get_left_relations_backend(unit_ref_value(), relation_type()) -> erepo_result([relation()]).
-spec memory_count_right_relations_backend(unit_ref_value(), relation_type()) -> erepo_result(non_neg_integer()).
-spec memory_count_left_relations_backend(unit_ref_value(), relation_type()) -> erepo_result(non_neg_integer()).
-spec memory_add_association_backend(unit_ref_value(), association_type(), ref_string()) -> ok | {error, erepo_reason()}.
-spec memory_remove_association_backend(unit_ref_value(), association_type(), ref_string()) -> ok | {error, erepo_reason()}.
-spec memory_get_right_association_backend(unit_ref_value(), association_type()) -> association_lookup_result().
-spec memory_get_right_associations_backend(unit_ref_value(), association_type()) -> erepo_result([association()]).
-spec memory_get_left_associations_backend(association_type(), ref_string()) -> erepo_result([association()]).
-spec memory_count_right_associations_backend(unit_ref_value(), association_type()) -> erepo_result(non_neg_integer()).
-spec memory_count_left_associations_backend(association_type(), ref_string()) -> erepo_result(non_neg_integer()).
-spec memory_lock_unit_backend(unit_ref_value(), lock_type(), ref_string()) -> ok | already_locked | {error, erepo_reason()}.
-spec memory_unlock_unit_backend(unit_ref_value()) -> ok | {error, erepo_reason()}.
-spec memory_set_status_backend(unit_ref_value(), unit_status()) -> ok | {error, erepo_reason()}.
-spec memory_create_attribute_backend(attribute_alias(), attribute_name(), attribute_qualname(), attribute_type(), boolean()) ->
    erepo_result(attribute_info()).
-spec memory_get_attribute_info_backend(name_or_id()) -> {ok, attribute_info()} | not_found | {error, erepo_reason()}.
-spec memory_get_tenant_info_backend(name_or_id()) -> {ok, tenant_info()} | not_found | {error, erepo_reason()}.
-spec pg_get_unit_json_backend(tenantid(), unitid(), version_selector()) -> unit_lookup_result().
-spec pg_unit_exists_backend(tenantid(), unitid()) -> boolean().
-spec pg_store_unit_json_backend(unit_map()) -> erepo_result(unit_map()).
-spec pg_search_units_backend(search_expression() | map(), search_order(), search_paging()) -> erepo_result(search_result()).
-spec pg_add_relation_backend(unit_ref_value(), relation_type(), unit_ref_value()) -> ok | {error, erepo_reason()}.
-spec pg_remove_relation_backend(unit_ref_value(), relation_type(), unit_ref_value()) -> ok | {error, erepo_reason()}.
-spec pg_get_right_relation_backend(unit_ref_value(), relation_type()) -> relation_lookup_result().
-spec pg_get_right_relations_backend(unit_ref_value(), relation_type()) -> erepo_result([relation()]).
-spec pg_get_left_relations_backend(unit_ref_value(), relation_type()) -> erepo_result([relation()]).
-spec pg_count_right_relations_backend(unit_ref_value(), relation_type()) -> erepo_result(non_neg_integer()).
-spec pg_count_left_relations_backend(unit_ref_value(), relation_type()) -> erepo_result(non_neg_integer()).
-spec pg_add_association_backend(unit_ref_value(), association_type(), ref_string()) -> ok | {error, erepo_reason()}.
-spec pg_remove_association_backend(unit_ref_value(), association_type(), ref_string()) -> ok | {error, erepo_reason()}.
-spec pg_get_right_association_backend(unit_ref_value(), association_type()) -> association_lookup_result().
-spec pg_get_right_associations_backend(unit_ref_value(), association_type()) -> erepo_result([association()]).
-spec pg_get_left_associations_backend(association_type(), ref_string()) -> erepo_result([association()]).
-spec pg_count_right_associations_backend(unit_ref_value(), association_type()) -> erepo_result(non_neg_integer()).
-spec pg_count_left_associations_backend(association_type(), ref_string()) -> erepo_result(non_neg_integer()).
-spec pg_lock_unit_backend(unit_ref_value(), lock_type(), ref_string()) -> ok | already_locked | {error, erepo_reason()}.
-spec pg_unlock_unit_backend(unit_ref_value()) -> ok | {error, erepo_reason()}.
-spec pg_set_status_backend(unit_ref_value(), unit_status()) -> ok | {error, erepo_reason()}.
-spec pg_create_attribute_backend(attribute_alias(), attribute_name(), attribute_qualname(), attribute_type(), boolean()) ->
    erepo_result(attribute_info()).
-spec pg_get_attribute_info_backend(name_or_id()) -> {ok, attribute_info()} | not_found | {error, erepo_reason()}.
-spec pg_get_tenant_info_backend(name_or_id()) -> {ok, tenant_info()} | not_found | {error, erepo_reason()}.

memory_get_unit_json_backend(TenantId, UnitId, Version) ->
    memory_get_unit_json(TenantId, UnitId, Version).
memory_unit_exists_backend(TenantId, UnitId) ->
    memory_unit_exists(TenantId, UnitId).
memory_store_unit_json_backend(UnitMap) ->
    memory_store_unit_json(UnitMap).
memory_search_units_backend(Expression, Order, PagingOrLimit) ->
    memory_search_units(Expression, Order, PagingOrLimit).
memory_add_relation_backend(UnitRef, RelType, OtherUnitRef) ->
    memory_add_relation(UnitRef, RelType, OtherUnitRef).
memory_remove_relation_backend(UnitRef, RelType, OtherUnitRef) ->
    memory_remove_relation(UnitRef, RelType, OtherUnitRef).
memory_get_right_relation_backend(UnitRef, RelType) ->
    memory_get_right_relation(UnitRef, RelType).
memory_get_right_relations_backend(UnitRef, RelType) ->
    memory_get_right_relations(UnitRef, RelType).
memory_get_left_relations_backend(UnitRef, RelType) ->
    memory_get_left_relations(UnitRef, RelType).
memory_count_right_relations_backend(UnitRef, RelType) ->
    memory_count_right_relations(UnitRef, RelType).
memory_count_left_relations_backend(UnitRef, RelType) ->
    memory_count_left_relations(UnitRef, RelType).
memory_add_association_backend(UnitRef, AssocType, RefString) ->
    memory_add_association(UnitRef, AssocType, RefString).
memory_remove_association_backend(UnitRef, AssocType, RefString) ->
    memory_remove_association(UnitRef, AssocType, RefString).
memory_get_right_association_backend(UnitRef, AssocType) ->
    memory_get_right_association(UnitRef, AssocType).
memory_get_right_associations_backend(UnitRef, AssocType) ->
    memory_get_right_associations(UnitRef, AssocType).
memory_get_left_associations_backend(AssocType, RefString) ->
    memory_get_left_associations(AssocType, RefString).
memory_count_right_associations_backend(UnitRef, AssocType) ->
    memory_count_right_associations(UnitRef, AssocType).
memory_count_left_associations_backend(AssocType, RefString) ->
    memory_count_left_associations(AssocType, RefString).
memory_lock_unit_backend(UnitRef, LockType, Purpose) ->
    memory_lock_unit(UnitRef, LockType, Purpose).
memory_unlock_unit_backend(UnitRef) ->
    memory_unlock_unit(UnitRef).
memory_set_status_backend(UnitRef, Status) ->
    memory_set_status(UnitRef, Status).
memory_create_attribute_backend(Alias, Name, QualName, Type, IsArray) ->
    memory_create_attribute(Alias, Name, QualName, Type, IsArray).
memory_get_attribute_info_backend(NameOrId) ->
    memory_get_attribute_info(NameOrId).
memory_get_tenant_info_backend(NameOrId) ->
    memory_get_tenant_info(NameOrId).

pg_get_unit_json_backend(TenantId, UnitId, Version) ->
    pg_get_unit_json(TenantId, UnitId, Version).
pg_unit_exists_backend(TenantId, UnitId) ->
    pg_unit_exists(TenantId, UnitId).
pg_store_unit_json_backend(UnitMap) ->
    pg_store_unit_json(UnitMap).
pg_search_units_backend(Expression, Order, PagingOrLimit) ->
    pg_search_units(Expression, Order, PagingOrLimit).
pg_add_relation_backend(UnitRef, RelType, OtherUnitRef) ->
    pg_add_relation(UnitRef, RelType, OtherUnitRef).
pg_remove_relation_backend(UnitRef, RelType, OtherUnitRef) ->
    pg_remove_relation(UnitRef, RelType, OtherUnitRef).
pg_get_right_relation_backend(UnitRef, RelType) ->
    pg_get_right_relation(UnitRef, RelType).
pg_get_right_relations_backend(UnitRef, RelType) ->
    pg_get_right_relations(UnitRef, RelType).
pg_get_left_relations_backend(UnitRef, RelType) ->
    pg_get_left_relations(UnitRef, RelType).
pg_count_right_relations_backend(UnitRef, RelType) ->
    pg_count_right_relations(UnitRef, RelType).
pg_count_left_relations_backend(UnitRef, RelType) ->
    pg_count_left_relations(UnitRef, RelType).
pg_add_association_backend(UnitRef, AssocType, RefString) ->
    pg_add_association(UnitRef, AssocType, RefString).
pg_remove_association_backend(UnitRef, AssocType, RefString) ->
    pg_remove_association(UnitRef, AssocType, RefString).
pg_get_right_association_backend(UnitRef, AssocType) ->
    pg_get_right_association(UnitRef, AssocType).
pg_get_right_associations_backend(UnitRef, AssocType) ->
    pg_get_right_associations(UnitRef, AssocType).
pg_get_left_associations_backend(AssocType, RefString) ->
    pg_get_left_associations(AssocType, RefString).
pg_count_right_associations_backend(UnitRef, AssocType) ->
    pg_count_right_associations(UnitRef, AssocType).
pg_count_left_associations_backend(AssocType, RefString) ->
    pg_count_left_associations(AssocType, RefString).
pg_lock_unit_backend(UnitRef, LockType, Purpose) ->
    pg_lock_unit(UnitRef, LockType, Purpose).
pg_unlock_unit_backend(UnitRef) ->
    pg_unlock_unit(UnitRef).
pg_set_status_backend(UnitRef, Status) ->
    pg_set_status(UnitRef, Status).
pg_create_attribute_backend(Alias, Name, QualName, Type, IsArray) ->
    pg_create_attribute(Alias, Name, QualName, Type, IsArray).
pg_get_attribute_info_backend(NameOrId) ->
    pg_get_attribute_info(NameOrId).
pg_get_tenant_info_backend(NameOrId) ->
    pg_get_tenant_info(NameOrId).

%% Memory implementation

memory_get_unit_json(TenantId, UnitId, latest) ->
    get_latest_version(TenantId, UnitId);
memory_get_unit_json(TenantId, UnitId, Version) when is_integer(Version), Version > 0 ->
    Key = {unit, TenantId, UnitId, Version},
    case erepo_cache:get(Key) of
        undefined -> not_found;
        Unit -> {ok, Unit}
    end;
memory_get_unit_json(_TenantId, _UnitId, _Version) ->
    {error, invalid_version}.

memory_unit_exists(TenantId, UnitId) ->
    case get_latest_version(TenantId, UnitId) of
        {ok, _} -> true;
        not_found -> false;
        _ -> false
    end.

memory_store_unit_json(UnitMap0) ->
    TenantId = maps:get(tenantid, UnitMap0),
    UnitId = maps:get(unitid, UnitMap0, undefined),
    AssignedUnitId = case UnitId of
        undefined -> erlang:unique_integer([monotonic, positive]);
        _ -> UnitId
    end,
    CurrentVersion =
        case get_latest_version(TenantId, AssignedUnitId) of
            {ok, Existing} -> maps:get(unitver, Existing, 1);
            not_found -> 0
        end,
    NextVersion = CurrentVersion + 1,
    Now = erlang:system_time(second),
    Created = case CurrentVersion of
        0 -> Now;
        _ -> maps:get(created, UnitMap0, Now)
    end,
    UnitMap = UnitMap0#{
        unitid => AssignedUnitId,
        unitver => NextVersion,
        created => Created,
        modified => Now,
        isreadonly => false
    },
    Key = {unit, TenantId, AssignedUnitId, NextVersion},
    LatestKey = {latest, TenantId, AssignedUnitId},
    erepo_cache:put(Key, UnitMap),
    erepo_cache:put(LatestKey, NextVersion),
    {ok, UnitMap}.

memory_add_relation(UnitRef, RelType, OtherUnitRef) ->
    Relations0 = get_meta_map(?REL_KEY),
    RelKey = {normalize_ref(UnitRef), RelType, normalize_ref(OtherUnitRef)},
    erepo_cache:put(?REL_KEY, Relations0#{RelKey => true}),
    ok.

memory_remove_relation(UnitRef, RelType, OtherUnitRef) ->
    Relations0 = get_meta_map(?REL_KEY),
    RelKey = {normalize_ref(UnitRef), RelType, normalize_ref(OtherUnitRef)},
    erepo_cache:put(?REL_KEY, maps:remove(RelKey, Relations0)),
    ok.

memory_get_right_relation(UnitRef, RelType) when is_integer(RelType) ->
    case memory_get_right_relations(UnitRef, RelType) of
        {ok, [Rel | _]} -> {ok, Rel};
        {ok, []} -> not_found;
        Error -> Error
    end;
memory_get_right_relation(_UnitRef, _RelType) ->
    {error, invalid_relation_type}.

memory_get_right_relations(UnitRef, RelType) when is_integer(RelType) ->
    case normalize_ref(UnitRef) of
        {_, _} = Ref ->
            Relations0 = get_meta_map(?REL_KEY),
            Matches = [
                relation_from_key(RelKey)
                || {RelKey = {Ref0, RelType0, _OtherRef}, true} <- maps:to_list(Relations0),
                   Ref0 =:= Ref,
                   RelType0 =:= RelType
            ],
            {ok, [R || R <- Matches, R =/= #{}]};
        _ ->
            {error, invalid_unit_ref}
    end;
memory_get_right_relations(_UnitRef, _RelType) ->
    {error, invalid_relation_type}.

memory_get_left_relations(UnitRef, RelType) when is_integer(RelType) ->
    case normalize_ref(UnitRef) of
        {_, _} = Ref ->
            Relations0 = get_meta_map(?REL_KEY),
            Matches = [
                relation_from_key(RelKey)
                || {RelKey = {_Ref0, RelType0, OtherRef}, true} <- maps:to_list(Relations0),
                   OtherRef =:= Ref,
                   RelType0 =:= RelType
            ],
            {ok, [R || R <- Matches, R =/= #{}]};
        _ ->
            {error, invalid_unit_ref}
    end;
memory_get_left_relations(_UnitRef, _RelType) ->
    {error, invalid_relation_type}.

memory_count_right_relations(UnitRef, RelType) when is_integer(RelType) ->
    case memory_get_right_relations(UnitRef, RelType) of
        {ok, Relations} -> {ok, length(Relations)};
        Error -> Error
    end;
memory_count_right_relations(_UnitRef, _RelType) ->
    {error, invalid_relation_type}.

memory_count_left_relations(UnitRef, RelType) when is_integer(RelType) ->
    case memory_get_left_relations(UnitRef, RelType) of
        {ok, Relations} -> {ok, length(Relations)};
        Error -> Error
    end;
memory_count_left_relations(_UnitRef, _RelType) ->
    {error, invalid_relation_type}.

memory_add_association(UnitRef, AssocType, RefString) ->
    Assocs0 = get_meta_map(?ASSOC_KEY),
    AssocKey = {normalize_ref(UnitRef), AssocType, RefString},
    erepo_cache:put(?ASSOC_KEY, Assocs0#{AssocKey => true}),
    ok.

memory_remove_association(UnitRef, AssocType, RefString) ->
    Assocs0 = get_meta_map(?ASSOC_KEY),
    AssocKey = {normalize_ref(UnitRef), AssocType, RefString},
    erepo_cache:put(?ASSOC_KEY, maps:remove(AssocKey, Assocs0)),
    ok.

memory_get_right_association(UnitRef, AssocType) when is_integer(AssocType) ->
    case memory_get_right_associations(UnitRef, AssocType) of
        {ok, [Assoc | _]} -> {ok, Assoc};
        {ok, []} -> not_found;
        Error -> Error
    end;
memory_get_right_association(_UnitRef, _AssocType) ->
    {error, invalid_association}.

memory_get_right_associations(UnitRef, AssocType) when is_integer(AssocType) ->
    case normalize_ref(UnitRef) of
        {_, _} = Ref ->
            Assocs0 = get_meta_map(?ASSOC_KEY),
            Matches = [
                association_from_key(AssocKey)
                || {AssocKey = {Ref0, AssocType0, _RefString}, true} <- maps:to_list(Assocs0),
                   Ref0 =:= Ref,
                   AssocType0 =:= AssocType
            ],
            {ok, [A || A <- Matches, A =/= #{}]};
        _ ->
            {error, invalid_unit_ref}
    end;
memory_get_right_associations(_UnitRef, _AssocType) ->
    {error, invalid_association}.

memory_get_left_associations(AssocType, RefString) when is_integer(AssocType), (is_binary(RefString) orelse is_list(RefString)) ->
    RefStringNorm = normalize_string(RefString),
    Assocs0 = get_meta_map(?ASSOC_KEY),
    Matches = [
        association_from_key(AssocKey)
        || {AssocKey = {_Ref0, AssocType0, AssocString0}, true} <- maps:to_list(Assocs0),
           AssocType0 =:= AssocType,
           normalize_string(AssocString0) =:= RefStringNorm
    ],
    {ok, [A || A <- Matches, A =/= #{}]};
memory_get_left_associations(_AssocType, _RefString) ->
    {error, invalid_association}.

memory_count_right_associations(UnitRef, AssocType) when is_integer(AssocType) ->
    case memory_get_right_associations(UnitRef, AssocType) of
        {ok, Assocs} -> {ok, length(Assocs)};
        Error -> Error
    end;
memory_count_right_associations(_UnitRef, _AssocType) ->
    {error, invalid_association}.

memory_count_left_associations(AssocType, RefString) when is_integer(AssocType), (is_binary(RefString) orelse is_list(RefString)) ->
    case memory_get_left_associations(AssocType, RefString) of
        {ok, Assocs} -> {ok, length(Assocs)};
        Error -> Error
    end;
memory_count_left_associations(_AssocType, _RefString) ->
    {error, invalid_association}.

memory_lock_unit(UnitRef, LockType, Purpose) ->
    Locks0 = get_meta_map(?LOCK_KEY),
    Ref = normalize_ref(UnitRef),
    case maps:is_key(Ref, Locks0) of
        true -> already_locked;
        false ->
            Lock = #{locktype => LockType, purpose => Purpose, locked_at => erlang:system_time(second)},
            erepo_cache:put(?LOCK_KEY, Locks0#{Ref => Lock}),
            ok
    end.

memory_unlock_unit(UnitRef) ->
    Locks0 = get_meta_map(?LOCK_KEY),
    Ref = normalize_ref(UnitRef),
    erepo_cache:put(?LOCK_KEY, maps:remove(Ref, Locks0)),
    ok.

memory_set_status(UnitRef, Status) ->
    case normalize_ref(UnitRef) of
        {TenantId, UnitId} ->
            case get_latest_version(TenantId, UnitId) of
                {ok, Unit} ->
                    Version = maps:get(unitver, Unit, 1),
                    Key = {unit, TenantId, UnitId, Version},
                    UpdatedUnit = Unit#{status => Status},
                    erepo_cache:put(Key, UpdatedUnit),
                    ok;
                not_found ->
                    {error, not_found}
            end;
        _ ->
            {error, invalid_unit_ref}
    end.

memory_create_attribute(Alias, Name, QualName, Type, IsArray)
  when is_binary(Name), is_binary(QualName), is_integer(Type), is_boolean(IsArray) ->
    AttrByName0 = get_meta_map(?ATTR_BY_NAME_KEY),
    case maps:get(Name, AttrByName0, undefined) of
        undefined ->
            NextId = next_attr_id(),
            Info = #{
                id => NextId,
                alias => Alias,
                name => Name,
                qualname => QualName,
                type => Type,
                forced_scalar => not IsArray
            },
            AttrById0 = get_meta_map(?ATTR_BY_ID_KEY),
            erepo_cache:put(?ATTR_BY_NAME_KEY, AttrByName0#{Name => Info}),
            erepo_cache:put(?ATTR_BY_ID_KEY, AttrById0#{NextId => Info}),
            {ok, Info};
        Existing ->
            {ok, Existing}
    end;
memory_create_attribute(_Alias, _Name, _QualName, _Type, _IsArray) ->
    {error, invalid_attribute_definition}.

memory_get_attribute_info(NameOrId) when is_binary(NameOrId) ->
    AttrByName = get_meta_map(?ATTR_BY_NAME_KEY),
    case maps:get(NameOrId, AttrByName, undefined) of
        undefined -> not_found;
        Info -> {ok, Info}
    end;
memory_get_attribute_info(NameOrId) when is_integer(NameOrId), NameOrId > 0 ->
    AttrById = get_meta_map(?ATTR_BY_ID_KEY),
    case maps:get(NameOrId, AttrById, undefined) of
        undefined -> not_found;
        Info -> {ok, Info}
    end;
memory_get_attribute_info(_NameOrId) ->
    {error, invalid_attribute_id_or_name}.

memory_get_tenant_info(NameOrId) when is_integer(NameOrId), NameOrId > 0 ->
    {ok, #{id => NameOrId, name => iolist_to_binary(io_lib:format("tenant-~p", [NameOrId]))}};
memory_get_tenant_info(NameOrId) when is_binary(NameOrId) ->
    {ok, #{id => 1, name => NameOrId}};
memory_get_tenant_info(_NameOrId) ->
    {error, invalid_tenant_id_or_name}.

memory_search_units(Expression0, Order, PagingOrLimit) ->
    case normalize_search_expression(Expression0) of
        {ok, Expression} ->
            Units0 = memory_latest_units(),
            Filtered = [Unit || Unit <- Units0, memory_match_unit(Unit, Expression)],
            Sorted = memory_sort_units(Filtered, Order),
            Paged = memory_apply_paging(Sorted, PagingOrLimit),
            {ok, #{results => Paged, total => length(Filtered)}};
        Error ->
            Error
    end.

normalize_search_expression(undefined) ->
    {ok, #{}};
normalize_search_expression(Expression) when is_map(Expression) ->
    {ok, Expression};
normalize_search_expression(Expression) when is_list(Expression) ->
    case is_proplist(Expression) of
        true -> {ok, maps:from_list(Expression)};
        false ->
            case erepo_search_parser:parse(Expression) of
                {ok, Parsed} -> {ok, Parsed};
                Error -> Error
            end
    end;
normalize_search_expression(Expression) when is_binary(Expression) ->
    erepo_search_parser:parse(Expression);
normalize_search_expression(_) ->
    {error, invalid_query}.

is_proplist([]) ->
    true;
is_proplist([{Key, _Value} | Rest]) when is_atom(Key); is_binary(Key) ->
    is_proplist(Rest);
is_proplist(_) ->
    false.

memory_latest_units() ->
    Cache = erepo_cache:all(),
    maps:fold(
      fun(Key, Value, Acc) ->
          case Key of
              {latest, TenantId, UnitId} when is_integer(Value) ->
                  UnitKey = {unit, TenantId, UnitId, Value},
                  case maps:get(UnitKey, Cache, undefined) of
                      Unit when is_map(Unit) -> [Unit | Acc];
                      _ -> Acc
                  end;
              _ ->
                  Acc
          end
      end,
      [],
      Cache).

memory_match_unit(_Unit, Expr) when map_size(Expr) =:= 0 ->
    true;
memory_match_unit(Unit, Expr) ->
    maps:fold(
      fun(Key, Value, Acc) ->
          Acc andalso memory_match_field(Unit, Key, Value)
      end,
      true,
      Expr).

memory_match_field(Unit, tenantid, Value) when is_integer(Value) ->
    maps:get(tenantid, Unit, undefined) =:= Value;
memory_match_field(Unit, unitid, Value) when is_integer(Value) ->
    maps:get(unitid, Unit, undefined) =:= Value;
memory_match_field(Unit, status, Value) when is_integer(Value) ->
    maps:get(status, Unit, undefined) =:= Value;
memory_match_field(Unit, name, Value) ->
    normalize_string(maps:get(unitname, Unit, <<"">>)) =:= normalize_string(Value);
memory_match_field(Unit, name_ilike, Pattern) ->
    like_match(normalize_string(maps:get(unitname, Unit, <<"">>)), normalize_string(Pattern));
memory_match_field(Unit, created_after, Value) ->
    compare_created(maps:get(created, Unit, 0), Value, ge);
memory_match_field(Unit, created_before, Value) ->
    compare_created(maps:get(created, Unit, 0), Value, lt);
memory_match_field(_Unit, _Key, _Value) ->
    true.

like_match(Text0, Pattern0) ->
    Text = unicode:characters_to_list(normalize_string(Text0)),
    Pattern = unicode:characters_to_list(normalize_string(Pattern0)),
    case re:run(Text, like_pattern_to_regex(Pattern), [{capture, none}, unicode]) of
        match -> true;
        nomatch -> false
    end.

like_pattern_to_regex(Pattern) ->
    "^" ++ lists:flatten([like_pattern_char(C) || C <- Pattern]) ++ "$".

like_pattern_char($%) ->
    ".*";
like_pattern_char($_) ->
    ".";
like_pattern_char(C) when C =:= $.; C =:= $^; C =:= $$; C =:= $*; C =:= $+; C =:= $?; C =:= $(; C =:= $); C =:= $[; C =:= $]; C =:= ${; C =:= $}; C =:= $|; C =:= $\\ ->
    [$\\, C];
like_pattern_char(C) ->
    [C].

compare_created(Created, FilterValue, Op) ->
    case to_int_maybe(FilterValue) of
        {ok, N} ->
            compare_int(Created, N, Op);
        error ->
            compare_text(normalize_string(Created), normalize_string(FilterValue), Op)
    end.

to_int_maybe(Value) when is_integer(Value) ->
    {ok, Value};
to_int_maybe(Value) when is_binary(Value) ->
    to_int_maybe(binary_to_list(Value));
to_int_maybe(Value) when is_list(Value) ->
    case string:to_integer(string:trim(Value)) of
        {I, _} -> {ok, I};
        _ -> error
    end;
to_int_maybe(_) ->
    error.

compare_int(A, B, ge) -> A >= B;
compare_int(A, B, lt) -> A < B.

compare_text(A, B, ge) -> A >= B;
compare_text(A, B, lt) -> A < B.

memory_sort_units(Units, Order) ->
    {Field, Dir} = normalize_order(Order),
    lists:sort(fun(A, B) -> memory_unit_before(A, B, Field, Dir) end, Units).

normalize_order(#{field := Field, dir := Dir}) ->
    normalize_order({Field, Dir});
normalize_order({Field, Dir}) ->
    NormField =
        case Field of
            created -> created;
            modified -> modified;
            unitid -> unitid;
            status -> status;
            _ -> created
        end,
    NormDir =
        case Dir of
            asc -> asc;
            'ASC' -> asc;
            _ -> desc
        end,
    {NormField, NormDir};
normalize_order(_) ->
    {created, desc}.

memory_unit_before(A, B, Field, Dir) ->
    KA = maps:get(Field, A, undefined),
    KB = maps:get(Field, B, undefined),
    case KA =:= KB of
        true ->
            tie_break_before(A, B);
        false ->
            case Dir of
                asc -> KA < KB;
                desc -> KA > KB
            end
    end.

tie_break_before(A, B) ->
    AUnitId = maps:get(unitid, A, 0),
    BUnitId = maps:get(unitid, B, 0),
    case AUnitId =:= BUnitId of
        true ->
            maps:get(unitver, A, 0) < maps:get(unitver, B, 0);
        false ->
            AUnitId < BUnitId
    end.

memory_apply_paging(Units, #{limit := Limit, offset := Offset})
  when is_integer(Limit), Limit > 0, is_integer(Offset), Offset >= 0 ->
    take_n(drop_n(Units, Offset), Limit);
memory_apply_paging(Units, #{limit := Limit}) when is_integer(Limit), Limit > 0 ->
    take_n(Units, Limit);
memory_apply_paging(Units, Limit) when is_integer(Limit), Limit > 0 ->
    take_n(Units, Limit);
memory_apply_paging(Units, _) ->
    Units.

drop_n(List, N) when N =< 0 ->
    List;
drop_n([], _N) ->
    [];
drop_n([_ | Rest], N) ->
    drop_n(Rest, N - 1).

take_n(_List, N) when N =< 0 ->
    [];
take_n([], _N) ->
    [];
take_n([H | T], N) ->
    [H | take_n(T, N - 1)].

%% Shared helpers

get_latest_version(TenantId, UnitId) ->
    LatestKey = {latest, TenantId, UnitId},
    case erepo_cache:get(LatestKey) of
        undefined ->
            not_found;
        Version ->
            Key = {unit, TenantId, UnitId, Version},
            case erepo_cache:get(Key) of
                undefined -> not_found;
                Unit -> {ok, Unit}
            end
    end.

validate_store_input(UnitMap) ->
    case maps:is_key(tenantid, UnitMap) of
        true -> ok;
        false -> {error, missing_tenantid}
    end.

get_meta_map(Key) ->
    case erepo_cache:get(Key) of
        undefined -> #{};
        Map when is_map(Map) -> Map
    end.

next_attr_id() ->
    Seq0 =
        case erepo_cache:get(?ATTR_SEQ_KEY) of
            undefined -> 0;
            ExistingSeq -> ExistingSeq
        end,
    Seq = Seq0 + 1,
    erepo_cache:put(?ATTR_SEQ_KEY, Seq),
    Seq.

normalize_ref(#unit_ref{tenantid = TenantId, unitid = UnitId}) ->
    {TenantId, UnitId};
normalize_ref(#{tenantid := TenantId, unitid := UnitId}) ->
    {TenantId, UnitId};
normalize_ref({TenantId, UnitId}) ->
    {TenantId, UnitId};
normalize_ref(_) ->
    invalid_ref.

maybe_null_text(undefined) -> null;
maybe_null_text(null) -> null;
maybe_null_text(Value) -> to_pg_text(Value).

normalize_purpose(undefined) -> <<"">>;
normalize_purpose(null) -> <<"">>;
normalize_purpose(Value) when is_binary(Value) -> Value;
normalize_purpose(Value) when is_list(Value) -> unicode:characters_to_binary(Value);
normalize_purpose(Value) -> unicode:characters_to_binary(io_lib:format("~p", [Value])).

normalize_string(Value) when is_binary(Value) -> Value;
normalize_string(Value) when is_list(Value) -> unicode:characters_to_binary(Value);
normalize_string(Value) -> unicode:characters_to_binary(io_lib:format("~p", [Value])).

to_pg_text(Value) when is_binary(Value) -> unicode:characters_to_list(Value);
to_pg_text(Value) when is_list(Value) -> Value;
to_pg_text(Value) when is_atom(Value) -> atom_to_list(Value);
to_pg_text(Value) -> unicode:characters_to_list(io_lib:format("~p", [Value])).

row_to_unit_map(Row) ->
    [TenantId, UnitId, UnitVer, CorrId, Status, Created, Modified, UnitName] = row_values(Row),
    #{
        tenantid => TenantId,
        unitid => UnitId,
        unitver => UnitVer,
        corrid => CorrId,
        status => Status,
        created => Created,
        modified => Modified,
        unitname => UnitName,
        isreadonly => false
    }.

row_to_unit_map_with_lastver(Row) ->
    [TenantId, UnitId, UnitVer, LastVer, CorrId, Status, Created, Modified, UnitName] = row_values(Row),
    #{
        tenantid => TenantId,
        unitid => UnitId,
        unitver => UnitVer,
        corrid => CorrId,
        status => Status,
        created => Created,
        modified => Modified,
        unitname => UnitName,
        isreadonly => LastVer > UnitVer
    }.

row_values(Row) when is_tuple(Row) -> tuple_to_list(Row);
row_values(Row) when is_list(Row) -> Row.

relation_from_row([TenantId, UnitId, RelType, RelTenantId, RelUnitId]) ->
    #{
        tenantid => TenantId,
        unitid => UnitId,
        reltype => RelType,
        reltenantid => RelTenantId,
        relunitid => RelUnitId
    };
relation_from_row(_) ->
    #{}.

association_from_row([TenantId, UnitId, AssocType, AssocString]) ->
    #{
        tenantid => TenantId,
        unitid => UnitId,
        assoctype => AssocType,
        assocstring => normalize_string(AssocString)
    };
association_from_row(_) ->
    #{}.

relation_from_key({{TenantId, UnitId}, RelType, {RelTenantId, RelUnitId}}) ->
    #{
        tenantid => TenantId,
        unitid => UnitId,
        reltype => RelType,
        reltenantid => RelTenantId,
        relunitid => RelUnitId
    };
relation_from_key(_) ->
    #{}.

association_from_key({{TenantId, UnitId}, AssocType, AssocString}) ->
    #{
        tenantid => TenantId,
        unitid => UnitId,
        assoctype => AssocType,
        assocstring => normalize_string(AssocString)
    };
association_from_key(_) ->
    #{}.

query_rows({ok, Cols, Rows}) when is_list(Cols), is_list(Rows) ->
    {ok, Rows};
query_rows({ok, _Count, Cols, Rows}) when is_list(Cols), is_list(Rows) ->
    {ok, Rows};
query_rows({error, _} = Error) ->
    Error;
query_rows(Other) ->
    {error, Other}.

pg_store_new_unit(Conn, UnitMap) ->
    pg_tx(Conn, fun() ->
        TenantId = maps:get(tenantid, UnitMap),
        CorrId = to_pg_text(maps:get(corrid, UnitMap, erepo_unit:make_corrid())),
        Status = maps:get(status, UnitMap, 30),
        UnitName = maybe_null_text(maps:get(unitname, UnitMap, undefined)),

        KernelSql = "INSERT INTO repo.repo_unit_kernel (tenantid, corrid, status) VALUES ($1, $2, $3) RETURNING unitid, lastver, created",
        case query_rows(epgsql:equery(Conn, KernelSql, [TenantId, CorrId, Status])) of
            {ok, [Row1 | _]} ->
                [UnitId, UnitVer, Created] = row_values(Row1),
                VersionSql = "INSERT INTO repo.repo_unit_version (tenantid, unitid, unitver, unitname) VALUES ($1, $2, $3, $4) RETURNING modified",
                case query_rows(epgsql:equery(Conn, VersionSql, [TenantId, UnitId, UnitVer, UnitName])) of
                    {ok, [Row2 | _]} ->
                        [Modified] = row_values(Row2),
                        {ok, UnitMap#{
                            unitid => UnitId,
                            unitver => UnitVer,
                            created => Created,
                            modified => Modified,
                            isreadonly => false
                        }};
                    Error2 ->
                        {error, Error2}
                end;
            {ok, []} ->
                {error, no_inserted_unit};
            Error1 ->
                {error, Error1}
        end
    end).

pg_store_new_version(Conn, UnitMap) ->
    pg_tx(Conn, fun() ->
        TenantId = maps:get(tenantid, UnitMap),
        UnitId = maps:get(unitid, UnitMap),
        Status = maps:get(status, UnitMap, 30),
        UnitName = maybe_null_text(maps:get(unitname, UnitMap, undefined)),

        UpdateKernelSql = "UPDATE repo.repo_unit_kernel SET status = $1, lastver = lastver + 1 WHERE tenantid = $2 AND unitid = $3 RETURNING lastver",
        case query_rows(epgsql:equery(Conn, UpdateKernelSql, [Status, TenantId, UnitId])) of
            {ok, [Row1 | _]} ->
                [UnitVer] = row_values(Row1),
                InsertVersionSql = "INSERT INTO repo.repo_unit_version (tenantid, unitid, unitver, unitname) VALUES ($1, $2, $3, $4) RETURNING modified",
                case query_rows(epgsql:equery(Conn, InsertVersionSql, [TenantId, UnitId, UnitVer, UnitName])) of
                    {ok, [Row2 | _]} ->
                        [Modified] = row_values(Row2),
                        {ok, UnitMap#{
                            unitver => UnitVer,
                            modified => Modified,
                            isreadonly => false
                        }};
                    Error2 ->
                        {error, Error2}
                end;
            {ok, []} ->
                {error, not_found};
            Error1 ->
                {error, Error1}
        end
    end).

pg_tx(Conn, Fun) ->
    case epgsql:squery(Conn, "BEGIN") of
        {ok, _, _} ->
            case Fun() of
                {ok, _} = Ok ->
                    _ = epgsql:squery(Conn, "COMMIT"),
                    Ok;
                Error ->
                    _ = epgsql:squery(Conn, "ROLLBACK"),
                    Error
            end;
        Error ->
            {error, Error}
    end.

build_where(undefined) ->
    {"", [], 1};
build_where(Expr) when is_map(Expr) ->
    Keys = maps:keys(Expr),
    build_where_from_keys(Keys, Expr, [], [], 1);
build_where(Expr) when is_list(Expr) ->
    MapExpr = maps:from_list(Expr),
    build_where(MapExpr);
build_where(_) ->
    {"", [], 1}.

build_where_from_keys([], _Expr, ClausesAcc, ParamsAcc, NextIdx) ->
    Clauses = lists:reverse(ClausesAcc),
    Params = lists:reverse(ParamsAcc),
    WhereSql = case Clauses of
        [] -> "";
        _ -> " WHERE " ++ string:join(Clauses, " AND ")
    end,
    {WhereSql, Params, NextIdx};
build_where_from_keys([Key | Rest], Expr, ClausesAcc, ParamsAcc, NextIdx) ->
    {MaybeClause, MaybeParam} =
        case {Key, maps:get(Key, Expr)} of
            {tenantid, TenantId} when is_integer(TenantId) ->
                {"uk.tenantid = $" ++ integer_to_list(NextIdx), TenantId};
            {unitid, UnitId} when is_integer(UnitId) ->
                {"uk.unitid = $" ++ integer_to_list(NextIdx), UnitId};
            {status, Status} when is_integer(Status) ->
                {"uk.status = $" ++ integer_to_list(NextIdx), Status};
            {name, Name} ->
                {"uv.unitname = $" ++ integer_to_list(NextIdx), normalize_string(Name)};
            {name_ilike, NameLike} ->
                {"uv.unitname ILIKE $" ++ integer_to_list(NextIdx), normalize_string(NameLike)};
            {created_after, CreatedAfter} ->
                {"uk.created >= $" ++ integer_to_list(NextIdx), to_pg_text(CreatedAfter)};
            {created_before, CreatedBefore} ->
                {"uk.created < $" ++ integer_to_list(NextIdx), to_pg_text(CreatedBefore)};
            _ ->
                {undefined, undefined}
        end,
    case MaybeClause of
        undefined ->
            build_where_from_keys(Rest, Expr, ClausesAcc, ParamsAcc, NextIdx);
        _ ->
            build_where_from_keys(Rest, Expr, [MaybeClause | ClausesAcc], [MaybeParam | ParamsAcc], NextIdx + 1)
    end.

build_order(#{field := Field, dir := Dir}) ->
    build_order({Field, Dir});
build_order({Field, Dir}) ->
    FieldSql = case Field of
        created -> "uk.created";
        modified -> "uv.modified";
        unitid -> "uk.unitid";
        status -> "uk.status";
        _ -> "uk.created"
    end,
    DirSql = case Dir of
        asc -> "ASC";
        'ASC' -> "ASC";
        _ -> "DESC"
    end,
    " ORDER BY " ++ FieldSql ++ " " ++ DirSql;
build_order(_) ->
    " ORDER BY uk.created DESC".

build_paging(#{limit := Limit, offset := Offset}, NextIdx)
  when is_integer(Limit), Limit > 0, is_integer(Offset), Offset >= 0 ->
    Sql = " LIMIT $" ++ integer_to_list(NextIdx) ++
          " OFFSET $" ++ integer_to_list(NextIdx + 1),
    {Sql, [Limit, Offset]};
build_paging(#{limit := Limit}, NextIdx) when is_integer(Limit), Limit > 0 ->
    Sql = " LIMIT $" ++ integer_to_list(NextIdx),
    {Sql, [Limit]};
build_paging(Limit, NextIdx) when is_integer(Limit), Limit > 0 ->
    Sql = " LIMIT $" ++ integer_to_list(NextIdx),
    {Sql, [Limit]};
build_paging(_, _NextIdx) ->
    {"", []}.

attr_row_to_info(Row) ->
    [Id, Alias, Name, QualName, Type, Scalar] = row_values(Row),
    #{
        id => Id,
        alias => Alias,
        name => Name,
        qualname => QualName,
        type => Type,
        forced_scalar => Scalar
    }.

map_to_json(Map) ->
    unicode:characters_to_binary(json:encode(normalize_json(Map))).

normalize_json(Value) when is_map(Value) ->
    maps:from_list([{json_key(K), normalize_json(V)} || {K, V} <- maps:to_list(Value)]);
normalize_json(Value) when is_list(Value) ->
    [normalize_json(V) || V <- Value];
normalize_json(Value) ->
    Value.

json_key(Key) when is_atom(Key) ->
    atom_to_binary(Key, utf8);
json_key(Key) when is_list(Key) ->
    unicode:characters_to_binary(Key);
json_key(Key) ->
    Key.

json_to_map(JsonBin) when is_binary(JsonBin) ->
    to_atom_keys(json:decode(JsonBin));
json_to_map(JsonText) when is_list(JsonText) ->
    json_to_map(unicode:characters_to_binary(JsonText));
json_to_map(Other) ->
    Other.

to_atom_keys(Value) when is_map(Value) ->
    maps:from_list([{to_atom_key(K), to_atom_keys(V)} || {K, V} <- maps:to_list(Value)]);
to_atom_keys(Value) when is_list(Value) ->
    [to_atom_keys(V) || V <- Value];
to_atom_keys(Value) ->
    Value.

to_atom_key(Key) when is_binary(Key) ->
    binary_to_atom(Key, utf8);
to_atom_key(Key) ->
    Key.
