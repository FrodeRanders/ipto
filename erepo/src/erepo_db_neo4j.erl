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
-module(erepo_db_neo4j).
-behaviour(erepo_backend).

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

-define(DEFAULT_DB, "neo4j").
-define(BOOTSTRAP_KEY, {?MODULE, bootstrap_done}).
-define(DEFAULT_TIMEOUT_MS, 5000).
-define(DEFAULT_CONNECT_TIMEOUT_MS, 2000).
-define(DEFAULT_RETRIES, 2).
-define(DEFAULT_RETRY_BASE_MS, 100).

%% --------------------------------------------------------------------
%% Backend API
%% --------------------------------------------------------------------
-spec get_unit_json(tenantid(), unitid(), version_selector()) -> unit_lookup_result().
get_unit_json(TenantId, UnitId, latest) ->
    get_unit_json(TenantId, UnitId, -1);
get_unit_json(TenantId, UnitId, Version) when is_integer(Version), Version =/= 0 ->
    Cypher =
        "MATCH (k:UnitKernel {tenantid: $tenantid, unitid: $unitid}) "
        "WITH k, CASE WHEN $version = -1 THEN k.lastver ELSE $version END AS wanted "
        "MATCH (v:UnitVersion {tenantid: k.tenantid, unitid: k.unitid, unitver: wanted}) "
        "RETURN k.tenantid, k.unitid, v.unitver, k.lastver, k.corrid, k.status, k.created, v.modified, v.unitname, v.payload",
    Params = #{tenantid => TenantId, unitid => UnitId, version => Version},
    case neo4j_query(Cypher, Params) of
        {ok, []} ->
            not_found;
        {ok, [Row | _]} ->
            row_to_unit_map(Row);
        Error ->
            Error
    end;
get_unit_json(_TenantId, _UnitId, _Version) ->
    {error, invalid_version}.

-spec unit_exists(tenantid(), unitid()) -> boolean().
unit_exists(TenantId, UnitId) ->
    Cypher =
        "MATCH (k:UnitKernel {tenantid: $tenantid, unitid: $unitid}) "
        "RETURN count(k) > 0 AS exists",
    case neo4j_query(Cypher, #{tenantid => TenantId, unitid => UnitId}) of
        {ok, [[Exists] | _]} when is_boolean(Exists) ->
            Exists;
        {ok, _} ->
            false;
        _ ->
            false
    end.

-spec store_unit_json(unit_map()) -> erepo_result(unit_map()).
store_unit_json(UnitMap) when is_map(UnitMap) ->
    case maps:get(unitid, UnitMap, undefined) of
        undefined ->
            store_new_unit(UnitMap);
        _ ->
            store_new_version(UnitMap)
    end.

-spec search_units(search_expression() | map(), search_order(), search_paging()) -> erepo_result(search_result()).
search_units(Expression0, Order, PagingOrLimit) ->
    case normalize_search_expression(Expression0) of
        {ok, Expression} ->
            case build_where(Expression) of
                {ok, WhereSql, Params0} ->
                    {OrderSql, OrderParams} = build_order(Order),
                    {PageSql, PageParams} = build_paging(PagingOrLimit),
                    Params = maps:merge(maps:merge(Params0, OrderParams), PageParams),

                    Match =
                        "MATCH (k:UnitKernel) "
                        "MATCH (k)-[:HAS_VERSION]->(v:UnitVersion) "
                        "WHERE v.unitver = k.lastver",
                    CountCypher = Match ++ WhereSql ++ " RETURN count(*)",
                    DataCypher =
                        Match ++
                        WhereSql ++
                        " RETURN k.tenantid, k.unitid, v.unitver, k.lastver, k.corrid, k.status, k.created, v.modified, v.unitname, v.payload" ++
                        OrderSql ++
                        PageSql,
                    case neo4j_query(CountCypher, Params0) of
                        {ok, [[Total] | _]} ->
                            case neo4j_query(DataCypher, Params) of
                                {ok, Rows} ->
                                    {ok, #{
                                        results => [unit_map_from_row(Row) || Row <- Rows],
                                        total => Total
                                    }};
                                Error2 ->
                                    Error2
                            end;
                        {ok, _} ->
                            {ok, #{results => [], total => 0}};
                        Error1 ->
                            Error1
                    end;
                Error ->
                    Error
            end;
        Error ->
            Error
    end.

-spec add_relation(unit_ref_value(), relation_type(), unit_ref_value()) -> ok | {error, erepo_reason()}.
add_relation(UnitRef, RelType, OtherUnitRef) when is_integer(RelType) ->
    case {normalize_ref(UnitRef), normalize_ref(OtherUnitRef)} of
        {{TenantId, UnitId}, {RelTenantId, RelUnitId}} ->
            Cypher =
                "MATCH (a:UnitKernel {tenantid: $tenantid, unitid: $unitid}) "
                "MATCH (b:UnitKernel {tenantid: $reltenantid, unitid: $relunitid}) "
                "MERGE (a)-[:RELATED_TO {reltype: $reltype, reltenantid: $reltenantid, relunitid: $relunitid}]->(b) "
                "RETURN 1",
            Params = #{
                tenantid => TenantId,
                unitid => UnitId,
                reltype => RelType,
                reltenantid => RelTenantId,
                relunitid => RelUnitId
            },
            case neo4j_query(Cypher, Params) of
                {ok, []} -> {error, not_found};
                {ok, _} -> ok;
                Error -> Error
            end;
        _ ->
            {error, invalid_unit_ref}
    end;
add_relation(_UnitRef, _RelType, _OtherUnitRef) ->
    {error, invalid_relation_type}.

-spec remove_relation(unit_ref_value(), relation_type(), unit_ref_value()) -> ok | {error, erepo_reason()}.
remove_relation(UnitRef, RelType, OtherUnitRef) when is_integer(RelType) ->
    case {normalize_ref(UnitRef), normalize_ref(OtherUnitRef)} of
        {{TenantId, UnitId}, {RelTenantId, RelUnitId}} ->
            Cypher =
                "MATCH (:UnitKernel {tenantid: $tenantid, unitid: $unitid})-[r:RELATED_TO {reltype: $reltype, reltenantid: $reltenantid, relunitid: $relunitid}]->(:UnitKernel {tenantid: $reltenantid, unitid: $relunitid}) "
                "DELETE r "
                "RETURN count(r)",
            Params = #{
                tenantid => TenantId,
                unitid => UnitId,
                reltype => RelType,
                reltenantid => RelTenantId,
                relunitid => RelUnitId
            },
            case neo4j_query(Cypher, Params) of
                {ok, _} -> ok;
                Error -> Error
            end;
        _ ->
            {error, invalid_unit_ref}
    end;
remove_relation(_UnitRef, _RelType, _OtherUnitRef) ->
    {error, invalid_relation_type}.

-spec get_right_relation(unit_ref_value(), relation_type()) -> relation_lookup_result().
get_right_relation(UnitRef, RelType) when is_integer(RelType) ->
    case normalize_ref(UnitRef) of
        {TenantId, UnitId} ->
            Cypher =
                "MATCH (a:UnitKernel {tenantid: $tenantid, unitid: $unitid}) "
                "-[r:RELATED_TO {reltype: $reltype}]->(b:UnitKernel) "
                "RETURN {tenantid: a.tenantid, unitid: a.unitid, reltype: r.reltype, "
                "reltenantid: r.reltenantid, relunitid: r.relunitid} AS rel "
                "LIMIT 1",
            Params = #{tenantid => TenantId, unitid => UnitId, reltype => RelType},
            case neo4j_query(Cypher, Params) of
                {ok, []} ->
                    not_found;
                {ok, [Row | _]} ->
                    case relation_row_binary(Row) of
                        Rel0 when is_map(Rel0), map_size(Rel0) =:= 0 ->
                            case relation_row_to_map(Row) of
                                Rel1 when is_map(Rel1), map_size(Rel1) =:= 0 ->
                                    case relation_row_fallback(Row) of
                                        Rel2 when is_map(Rel2), map_size(Rel2) =:= 0 ->
                                            {error, {invalid_relation_row, Row}};
                                        Rel2 ->
                                            {ok, Rel2}
                                    end;
                                Rel1 ->
                                    {ok, Rel1}
                            end;
                        Rel0 ->
                            {ok, Rel0}
                    end;
                Error ->
                    Error
            end;
        _ ->
            {error, invalid_unit_ref}
    end;
get_right_relation(_UnitRef, _RelType) ->
    {error, invalid_relation_type}.

-spec get_right_relations(unit_ref_value(), relation_type()) -> erepo_result([relation()]).
get_right_relations(UnitRef, RelType) when is_integer(RelType) ->
    case normalize_ref(UnitRef) of
        {TenantId, UnitId} ->
            Cypher =
                "MATCH (a:UnitKernel {tenantid: $tenantid, unitid: $unitid}) "
                "-[r:RELATED_TO {reltype: $reltype}]->(b:UnitKernel) "
                "RETURN {tenantid: a.tenantid, unitid: a.unitid, reltype: r.reltype, "
                "reltenantid: r.reltenantid, relunitid: r.relunitid} AS rel",
            Params = #{tenantid => TenantId, unitid => UnitId, reltype => RelType},
            case neo4j_query(Cypher, Params) of
                {ok, Rows} ->
                    Relations0 = [relation_row_binary(Row) || Row <- Rows],
                    Relations = [
                        case Rel of
                            Rel1 when is_map(Rel1), map_size(Rel1) =:= 0 ->
                                case relation_row_to_map(Row) of
                                    Rel2 when is_map(Rel2), map_size(Rel2) =:= 0 -> relation_row_fallback(Row);
                                    OtherRel -> OtherRel
                                end;
                            _ -> Rel
                        end
                        || {Rel, Row} <- lists:zip(Relations0, Rows)
                    ],
                    case [R || R <- Relations, not (is_map(R) andalso map_size(R) =:= 0)] of
                        [] when Rows =/= [] ->
                            {error, {invalid_relation_rows, Rows}};
                        Clean ->
                            {ok, Clean}
                    end;
                Error ->
                    Error
            end;
        _ ->
            {error, invalid_unit_ref}
    end;
get_right_relations(_UnitRef, _RelType) ->
    {error, invalid_relation_type}.

-spec get_left_relations(unit_ref_value(), relation_type()) -> erepo_result([relation()]).
get_left_relations(UnitRef, RelType) when is_integer(RelType) ->
    case normalize_ref(UnitRef) of
        {TenantId, UnitId} ->
            Cypher =
                "MATCH (a:UnitKernel)-[r:RELATED_TO {reltype: $reltype}]->"
                "(b:UnitKernel {tenantid: $reltenantid, unitid: $relunitid}) "
                "RETURN {tenantid: a.tenantid, unitid: a.unitid, reltype: r.reltype, "
                "reltenantid: r.reltenantid, relunitid: r.relunitid} AS rel",
            Params = #{reltenantid => TenantId, relunitid => UnitId, reltype => RelType},
            case neo4j_query(Cypher, Params) of
                {ok, Rows} ->
                    Relations0 = [relation_row_binary(Row) || Row <- Rows],
                    Relations = [
                        case Rel of
                            Rel1 when is_map(Rel1), map_size(Rel1) =:= 0 ->
                                case relation_row_to_map(Row) of
                                    Rel2 when is_map(Rel2), map_size(Rel2) =:= 0 -> relation_row_fallback(Row);
                                    OtherRel -> OtherRel
                                end;
                            _ -> Rel
                        end
                        || {Rel, Row} <- lists:zip(Relations0, Rows)
                    ],
                    case [R || R <- Relations, not (is_map(R) andalso map_size(R) =:= 0)] of
                        [] when Rows =/= [] ->
                            {error, {invalid_relation_rows, Rows}};
                        Clean ->
                            {ok, Clean}
                    end;
                Error ->
                    Error
            end;
        _ ->
            {error, invalid_unit_ref}
    end;
get_left_relations(_UnitRef, _RelType) ->
    {error, invalid_relation_type}.

-spec count_right_relations(unit_ref_value(), relation_type()) -> erepo_result(non_neg_integer()).
count_right_relations(UnitRef, RelType) when is_integer(RelType) ->
    case normalize_ref(UnitRef) of
        {TenantId, UnitId} ->
            Cypher =
                "MATCH (:UnitKernel {tenantid: $tenantid, unitid: $unitid}) "
                "-[r:RELATED_TO {reltype: $reltype}]->(:UnitKernel) "
                "RETURN count(r)",
            Params = #{tenantid => TenantId, unitid => UnitId, reltype => RelType},
            case neo4j_query(Cypher, Params) of
                {ok, []} ->
                    {ok, 0};
                {ok, [[Count] | _]} ->
                    {ok, trunc(Count)};
                Error ->
                    Error
            end;
        _ ->
            {error, invalid_unit_ref}
    end;
count_right_relations(_UnitRef, _RelType) ->
    {error, invalid_relation_type}.

-spec count_left_relations(unit_ref_value(), relation_type()) -> erepo_result(non_neg_integer()).
count_left_relations(UnitRef, RelType) when is_integer(RelType) ->
    case normalize_ref(UnitRef) of
        {TenantId, UnitId} ->
            Cypher =
                "MATCH (:UnitKernel)-[r:RELATED_TO {reltype: $reltype}]->"
                "(:UnitKernel {tenantid: $reltenantid, unitid: $relunitid}) "
                "RETURN count(r)",
            Params = #{reltenantid => TenantId, relunitid => UnitId, reltype => RelType},
            case neo4j_query(Cypher, Params) of
                {ok, []} ->
                    {ok, 0};
                {ok, [[Count] | _]} ->
                    {ok, trunc(Count)};
                Error ->
                    Error
            end;
        _ ->
            {error, invalid_unit_ref}
    end;
count_left_relations(_UnitRef, _RelType) ->
    {error, invalid_relation_type}.

-spec add_association(unit_ref_value(), association_type(), ref_string()) -> ok | {error, erepo_reason()}.
add_association(UnitRef, AssocType, RefString) when is_integer(AssocType) ->
    case normalize_ref(UnitRef) of
        {TenantId, UnitId} ->
            Cypher =
                "MATCH (k:UnitKernel {tenantid: $tenantid, unitid: $unitid}) "
                "MERGE (a:AssociationRef {assoctype: $assoctype, refstring: $refstring}) "
                "MERGE (k)-[:ASSOCIATED_WITH {assoctype: $assoctype, refstring: $refstring}]->(a) "
                "RETURN 1",
            Params = #{
                tenantid => TenantId,
                unitid => UnitId,
                assoctype => AssocType,
                refstring => normalize_string(RefString)
            },
            case neo4j_query(Cypher, Params) of
                {ok, []} -> {error, not_found};
                {ok, _} -> ok;
                Error -> Error
            end;
        _ ->
            {error, invalid_unit_ref}
    end;
add_association(_UnitRef, _AssocType, _RefString) ->
    {error, invalid_association_type}.

-spec remove_association(unit_ref_value(), association_type(), ref_string()) -> ok | {error, erepo_reason()}.
remove_association(UnitRef, AssocType, RefString) when is_integer(AssocType) ->
    case normalize_ref(UnitRef) of
        {TenantId, UnitId} ->
            Cypher =
                "MATCH (:UnitKernel {tenantid: $tenantid, unitid: $unitid})-[r:ASSOCIATED_WITH {assoctype: $assoctype, refstring: $refstring}]->(:AssociationRef {assoctype: $assoctype, refstring: $refstring}) "
                "DELETE r "
                "RETURN count(r)",
            Params = #{
                tenantid => TenantId,
                unitid => UnitId,
                assoctype => AssocType,
                refstring => normalize_string(RefString)
            },
            case neo4j_query(Cypher, Params) of
                {ok, _} -> ok;
                Error -> Error
            end;
        _ ->
            {error, invalid_unit_ref}
    end;
remove_association(_UnitRef, _AssocType, _RefString) ->
    {error, invalid_association_type}.

-spec get_right_association(unit_ref_value(), association_type()) -> association_lookup_result().
get_right_association(UnitRef, AssocType) when is_integer(AssocType) ->
    case normalize_ref(UnitRef) of
        {TenantId, UnitId} ->
            Cypher =
                "MATCH (k:UnitKernel {tenantid: $tenantid, unitid: $unitid}) "
                "-[r:ASSOCIATED_WITH {assoctype: $assoctype}]->(a:AssociationRef) "
                "RETURN {tenantid: k.tenantid, unitid: k.unitid, assoctype: r.assoctype, "
                "assocstring: r.refstring} AS assoc "
                "LIMIT 1",
            Params = #{tenantid => TenantId, unitid => UnitId, assoctype => AssocType},
            case neo4j_query(Cypher, Params) of
                {ok, []} ->
                    not_found;
                {ok, [Row | _]} ->
                    case association_row_binary(Row) of
                        Assoc0 when is_map(Assoc0), map_size(Assoc0) =:= 0 ->
                            case association_row_to_map(Row) of
                                Assoc1 when is_map(Assoc1), map_size(Assoc1) =:= 0 ->
                                    case association_row_fallback(Row) of
                                        Assoc2 when is_map(Assoc2), map_size(Assoc2) =:= 0 ->
                                            {error, {invalid_association_row, Row}};
                                        Assoc2 ->
                                            {ok, Assoc2}
                                    end;
                                Assoc1 ->
                                    {ok, Assoc1}
                            end;
                        Assoc0 ->
                            {ok, Assoc0}
                    end;
                Error ->
                    Error
            end;
        _ ->
            {error, invalid_unit_ref}
    end;
get_right_association(_UnitRef, _AssocType) ->
    {error, invalid_association_type}.

-spec get_right_associations(unit_ref_value(), association_type()) -> erepo_result([association()]).
get_right_associations(UnitRef, AssocType) when is_integer(AssocType) ->
    case normalize_ref(UnitRef) of
        {TenantId, UnitId} ->
            Cypher =
                "MATCH (k:UnitKernel {tenantid: $tenantid, unitid: $unitid}) "
                "-[r:ASSOCIATED_WITH {assoctype: $assoctype}]->(a:AssociationRef) "
                "RETURN {tenantid: k.tenantid, unitid: k.unitid, assoctype: r.assoctype, "
                "assocstring: r.refstring} AS assoc",
            Params = #{tenantid => TenantId, unitid => UnitId, assoctype => AssocType},
            case neo4j_query(Cypher, Params) of
                {ok, Rows} ->
                    Assocs0 = [association_row_binary(Row) || Row <- Rows],
                    Assocs = [
                        case Assoc of
                            Assoc1 when is_map(Assoc1), map_size(Assoc1) =:= 0 ->
                                case association_row_to_map(Row) of
                                    Assoc2 when is_map(Assoc2), map_size(Assoc2) =:= 0 -> association_row_fallback(Row);
                                    OtherAssoc -> OtherAssoc
                                end;
                            _ -> Assoc
                        end
                        || {Assoc, Row} <- lists:zip(Assocs0, Rows)
                    ],
                    case [A || A <- Assocs, not (is_map(A) andalso map_size(A) =:= 0)] of
                        [] when Rows =/= [] ->
                            {error, {invalid_association_rows, Rows}};
                        Clean ->
                            {ok, Clean}
                    end;
                Error ->
                    Error
            end;
        _ ->
            {error, invalid_unit_ref}
    end;
get_right_associations(_UnitRef, _AssocType) ->
    {error, invalid_association_type}.

-spec get_left_associations(association_type(), ref_string()) -> erepo_result([association()]).
get_left_associations(AssocType, RefString) when is_integer(AssocType) ->
    RefStringNorm = normalize_string(RefString),
    Cypher =
        "MATCH (k:UnitKernel)-[r:ASSOCIATED_WITH {assoctype: $assoctype, refstring: $refstring}]->"
        "(:AssociationRef {assoctype: $assoctype, refstring: $refstring}) "
        "RETURN {tenantid: k.tenantid, unitid: k.unitid, assoctype: r.assoctype, "
        "assocstring: r.refstring} AS assoc",
    Params = #{assoctype => AssocType, refstring => RefStringNorm},
    case neo4j_query(Cypher, Params) of
        {ok, Rows} ->
            Assocs0 = [association_row_binary(Row) || Row <- Rows],
            Assocs = [
                case Assoc of
                    Assoc1 when is_map(Assoc1), map_size(Assoc1) =:= 0 ->
                        case association_row_to_map(Row) of
                            Assoc2 when is_map(Assoc2), map_size(Assoc2) =:= 0 -> association_row_fallback(Row);
                            OtherAssoc -> OtherAssoc
                        end;
                    _ -> Assoc
                end
                || {Assoc, Row} <- lists:zip(Assocs0, Rows)
            ],
            case [A || A <- Assocs, not (is_map(A) andalso map_size(A) =:= 0)] of
                [] when Rows =/= [] ->
                    {error, {invalid_association_rows, Rows}};
                Clean ->
                    {ok, Clean}
            end;
        Error ->
            Error
    end;
get_left_associations(_AssocType, _RefString) ->
    {error, invalid_association_type}.

-spec count_right_associations(unit_ref_value(), association_type()) -> erepo_result(non_neg_integer()).
count_right_associations(UnitRef, AssocType) when is_integer(AssocType) ->
    case normalize_ref(UnitRef) of
        {TenantId, UnitId} ->
            Cypher =
                "MATCH (:UnitKernel {tenantid: $tenantid, unitid: $unitid}) "
                "-[r:ASSOCIATED_WITH {assoctype: $assoctype}]->(:AssociationRef) "
                "RETURN count(r)",
            Params = #{tenantid => TenantId, unitid => UnitId, assoctype => AssocType},
            case neo4j_query(Cypher, Params) of
                {ok, []} ->
                    {ok, 0};
                {ok, [[Count] | _]} ->
                    {ok, trunc(Count)};
                Error ->
                    Error
            end;
        _ ->
            {error, invalid_unit_ref}
    end;
count_right_associations(_UnitRef, _AssocType) ->
    {error, invalid_association_type}.

-spec count_left_associations(association_type(), ref_string()) -> erepo_result(non_neg_integer()).
count_left_associations(AssocType, RefString) when is_integer(AssocType) ->
    RefStringNorm = normalize_string(RefString),
    Cypher =
        "MATCH (:UnitKernel)-[r:ASSOCIATED_WITH {assoctype: $assoctype, refstring: $refstring}]->"
        "(:AssociationRef {assoctype: $assoctype, refstring: $refstring}) "
        "RETURN count(r)",
    Params = #{assoctype => AssocType, refstring => RefStringNorm},
    case neo4j_query(Cypher, Params) of
        {ok, []} ->
            {ok, 0};
        {ok, [[Count] | _]} ->
            {ok, trunc(Count)};
        Error ->
            Error
    end;
count_left_associations(_AssocType, _RefString) ->
    {error, invalid_association_type}.
-spec lock_unit(unit_ref_value(), lock_type(), ref_string()) -> ok | already_locked | {error, erepo_reason()}.
lock_unit(UnitRef, LockType, Purpose) when is_integer(LockType) ->
    case normalize_ref(UnitRef) of
        {TenantId, UnitId} ->
            CheckCypher =
                "MATCH (k:UnitKernel {tenantid: $tenantid, unitid: $unitid}) "
                "OPTIONAL MATCH (k)-[:HAS_LOCK]->(l:UnitLock) "
                "RETURN count(l) > 0",
            Params = #{tenantid => TenantId, unitid => UnitId},
            case neo4j_query(CheckCypher, Params) of
                {ok, []} ->
                    {error, not_found};
                {ok, [[true] | _]} ->
                    already_locked;
                {ok, [[false] | _]} ->
                    CreateCypher =
                        "MATCH (k:UnitKernel {tenantid: $tenantid, unitid: $unitid}) "
                        "CREATE (l:UnitLock {locktype: $locktype, purpose: $purpose, locked_at: $locked_at}) "
                        "CREATE (k)-[:HAS_LOCK]->(l) "
                        "RETURN 1",
                    CreateParams = Params#{
                        locktype => LockType,
                        purpose => normalize_string(Purpose),
                        locked_at => erlang:system_time(second)
                    },
                    case neo4j_query(CreateCypher, CreateParams) of
                        {ok, []} -> {error, not_found};
                        {ok, _} -> ok;
                        Error -> Error
                    end;
                {ok, _} ->
                    {error, invalid_lock_response};
                Error ->
                    Error
            end;
        _ ->
            {error, invalid_unit_ref}
    end;
lock_unit(_UnitRef, _LockType, _Purpose) ->
    {error, invalid_lock_type}.

-spec unlock_unit(unit_ref_value()) -> ok | {error, erepo_reason()}.
unlock_unit(UnitRef) ->
    case normalize_ref(UnitRef) of
        {TenantId, UnitId} ->
            Cypher =
                "MATCH (k:UnitKernel {tenantid: $tenantid, unitid: $unitid}) "
                "OPTIONAL MATCH (k)-[r:HAS_LOCK]->(l:UnitLock) "
                "DELETE r, l "
                "RETURN 1",
            case neo4j_query(Cypher, #{tenantid => TenantId, unitid => UnitId}) of
                {ok, []} -> {error, not_found};
                {ok, _} -> ok;
                Error -> Error
            end;
        _ ->
            {error, invalid_unit_ref}
    end.

-spec set_status(unit_ref_value(), unit_status()) -> ok | {error, erepo_reason()}.
set_status(UnitRef, Status) when is_integer(Status) ->
    case normalize_ref(UnitRef) of
        {TenantId, UnitId} ->
            Cypher =
                "MATCH (k:UnitKernel {tenantid: $tenantid, unitid: $unitid}) "
                "SET k.status = $status "
                "RETURN k.status",
            Params = #{tenantid => TenantId, unitid => UnitId, status => Status},
            case neo4j_query(Cypher, Params) of
                {ok, []} ->
                    {error, not_found};
                {ok, _} ->
                    ok;
                Error ->
                    Error
            end;
        _ ->
            {error, invalid_unit_ref}
    end;
set_status(_UnitRef, _Status) ->
    {error, invalid_status}.

-spec create_attribute(attribute_alias(), attribute_name(), attribute_qualname(), attribute_type(), boolean()) ->
    erepo_result(attribute_info()).
create_attribute(Alias, Name, QualName, Type, IsArray)
  when is_binary(Name), is_binary(QualName), is_integer(Type), is_boolean(IsArray) ->
    case get_attribute_info(Name) of
        {ok, Existing} ->
            {ok, Existing};
        not_found ->
            case next_attrid() of
                {ok, AttrId} ->
                    ForcedScalar = not IsArray,
                    Cypher =
                        "CREATE (a:Attribute {id: $id, alias: $alias, name: $name, qualname: $qualname, type: $type, forced_scalar: $forced_scalar}) "
                        "RETURN a.id, a.alias, a.name, a.qualname, a.type, a.forced_scalar",
                    Params = #{
                        id => AttrId,
                        alias => normalize_nullable_string(Alias),
                        name => Name,
                        qualname => QualName,
                        type => Type,
                        forced_scalar => ForcedScalar
                    },
                    case neo4j_query(Cypher, Params) of
                        {ok, [[Id, AliasR, NameR, QualNameR, TypeR, Scalar] | _]} ->
                            {ok, #{
                                id => Id,
                                alias => AliasR,
                                name => NameR,
                                qualname => QualNameR,
                                type => TypeR,
                                forced_scalar => Scalar
                            }};
                        Error ->
                            Error
                    end;
                Error ->
                    Error
            end;
        Error ->
            Error
    end;
create_attribute(_Alias, _Name, _QualName, _Type, _IsArray) ->
    {error, invalid_attribute_definition}.

-spec get_attribute_info(name_or_id()) -> {ok, attribute_info()} | not_found | {error, erepo_reason()}.
get_attribute_info(NameOrId) when is_binary(NameOrId) ->
    Cypher =
        "MATCH (a:Attribute {name: $name}) "
        "RETURN a.id, a.alias, a.name, a.qualname, a.type, a.forced_scalar "
        "LIMIT 1",
    case neo4j_query(Cypher, #{name => NameOrId}) of
        {ok, [[Id, Alias, Name, QualName, Type, Scalar] | _]} ->
            {ok, #{
                id => Id,
                alias => Alias,
                name => Name,
                qualname => QualName,
                type => Type,
                forced_scalar => Scalar
            }};
        {ok, []} ->
            not_found;
        Error ->
            Error
    end;
get_attribute_info(NameOrId) when is_integer(NameOrId), NameOrId > 0 ->
    Cypher =
        "MATCH (a:Attribute {id: $id}) "
        "RETURN a.id, a.alias, a.name, a.qualname, a.type, a.forced_scalar "
        "LIMIT 1",
    case neo4j_query(Cypher, #{id => NameOrId}) of
        {ok, [[Id, Alias, Name, QualName, Type, Scalar] | _]} ->
            {ok, #{
                id => Id,
                alias => Alias,
                name => Name,
                qualname => QualName,
                type => Type,
                forced_scalar => Scalar
            }};
        {ok, []} ->
            not_found;
        Error ->
            Error
    end;
get_attribute_info(_NameOrId) ->
    {error, invalid_attribute_id_or_name}.

-spec get_tenant_info(name_or_id()) -> {ok, tenant_info()} | not_found | {error, erepo_reason()}.
get_tenant_info(NameOrId) when is_integer(NameOrId), NameOrId > 0 ->
    Now = erlang:system_time(second),
    Cypher =
        "MERGE (t:Tenant {id: $id}) "
        "ON CREATE SET t.name = $name, t.description = $description, t.created = $created "
        "RETURN t.id, t.name, t.description, t.created",
    Params = #{
        id => NameOrId,
        name => tenant_name(NameOrId),
        description => null,
        created => Now
    },
    case neo4j_query(Cypher, Params) of
        {ok, [[Id, Name, Description, Created] | _]} ->
            {ok, #{id => Id, name => Name, description => Description, created => Created}};
        Error ->
            Error
    end;
get_tenant_info(NameOrId) when is_binary(NameOrId) ->
    Cypher = "MATCH (t:Tenant {name: $name}) RETURN t.id, t.name, t.description, t.created LIMIT 1",
    case neo4j_query(Cypher, #{name => NameOrId}) of
        {ok, [[Id, Name, Description, Created] | _]} ->
            {ok, #{id => Id, name => Name, description => Description, created => Created}};
        {ok, []} ->
            case next_tenantid() of
                {ok, TenantId} ->
                    Now = erlang:system_time(second),
                    CreateCypher =
                        "CREATE (t:Tenant {id: $id, name: $name, description: $description, created: $created}) "
                        "RETURN t.id, t.name, t.description, t.created",
                    Params = #{id => TenantId, name => NameOrId, description => null, created => Now},
                    case neo4j_query(CreateCypher, Params) of
                        {ok, [[Id, Name, Description, Created] | _]} ->
                            {ok, #{id => Id, name => Name, description => Description, created => Created}};
                        Error ->
                            Error
                    end;
                Error ->
                    Error
            end;
        Error ->
            Error
    end;
get_tenant_info(_NameOrId) ->
    {error, invalid_tenant_id_or_name}.

store_new_unit(UnitMap) ->
    case next_unitid() of
        {ok, UnitId} ->
            TenantId = maps:get(tenantid, UnitMap),
            CorrId = normalize_string(maps:get(corrid, UnitMap, erepo_unit:make_corrid())),
            Status = maps:get(status, UnitMap, 30),
            UnitName = normalize_nullable_string(maps:get(unitname, UnitMap, undefined)),
            Now = erlang:system_time(second),
            Stored = UnitMap#{
                unitid => UnitId,
                unitver => 1,
                corrid => CorrId,
                status => Status,
                created => Now,
                modified => Now,
                isreadonly => false
            },
            Payload = map_to_json(Stored),
            Cypher =
                "CREATE (k:UnitKernel {tenantid: $tenantid, unitid: $unitid, corrid: $corrid, status: $status, created: $created, lastver: 1}) "
                "CREATE (v:UnitVersion {tenantid: $tenantid, unitid: $unitid, unitver: 1, unitname: $unitname, modified: $modified, payload: $payload}) "
                "CREATE (k)-[:HAS_VERSION {unitver: 1}]->(v) "
                "RETURN k.unitid, 1, k.created, v.modified",
            Params = #{
                tenantid => TenantId,
                unitid => UnitId,
                corrid => CorrId,
                status => Status,
                created => Now,
                unitname => UnitName,
                modified => Now,
                payload => Payload
            },
            case neo4j_query(Cypher, Params) of
                {ok, [[NewUnitId, UnitVer, Created, Modified] | _]} ->
                    {ok, Stored#{
                        unitid => NewUnitId,
                        unitver => UnitVer,
                        created => Created,
                        modified => Modified
                    }};
                Error ->
                    Error
            end;
        Error ->
            Error
    end.

store_new_version(UnitMap) ->
    TenantId = maps:get(tenantid, UnitMap),
    UnitId = maps:get(unitid, UnitMap),
    Status = maps:get(status, UnitMap, 30),
    UnitName = normalize_nullable_string(maps:get(unitname, UnitMap, undefined)),
    Now = erlang:system_time(second),
    Cypher =
        "MATCH (k:UnitKernel {tenantid: $tenantid, unitid: $unitid}) "
        "SET k.lastver = coalesce(k.lastver, 0) + 1, k.status = $status "
        "WITH k, k.lastver AS nextver "
        "CREATE (v:UnitVersion {tenantid: k.tenantid, unitid: k.unitid, unitver: nextver, unitname: $unitname, modified: $modified, payload: $payload}) "
        "CREATE (k)-[:HAS_VERSION {unitver: nextver}]->(v) "
        "RETURN nextver, v.modified",
    Payload = map_to_json(UnitMap#{modified => Now}),
    Params = #{
        tenantid => TenantId,
        unitid => UnitId,
        status => Status,
        unitname => UnitName,
        modified => Now,
        payload => Payload
    },
    case neo4j_query(Cypher, Params) of
        {ok, []} ->
            {error, not_found};
        {ok, [[UnitVer, Modified] | _]} ->
            {ok, UnitMap#{
                unitver => UnitVer,
                modified => Modified,
                status => Status,
                isreadonly => false
            }};
        Error ->
            Error
    end.

next_unitid() ->
    Cypher =
        "MERGE (c:Counter {name: 'unitid'}) "
        "ON CREATE SET c.value = 0 "
        "SET c.value = c.value + 1 "
        "RETURN c.value",
    case neo4j_query(Cypher, #{}) of
        {ok, [[UnitId] | _]} when is_integer(UnitId) ->
            {ok, UnitId};
        {ok, [[UnitId0] | _]} ->
            {ok, trunc(UnitId0)};
        Error ->
            Error
    end.

next_attrid() ->
    next_counter("attrid").

next_tenantid() ->
    next_counter("tenantid").

next_counter(CounterName) ->
    Cypher =
        "MERGE (c:Counter {name: $name}) "
        "ON CREATE SET c.value = 0 "
        "SET c.value = c.value + 1 "
        "RETURN c.value",
    case neo4j_query(Cypher, #{name => CounterName}) of
        {ok, [[Id] | _]} when is_integer(Id) ->
            {ok, Id};
        {ok, [[Id0] | _]} ->
            {ok, trunc(Id0)};
        Error ->
            Error
    end.

row_to_unit_map([TenantId, UnitId, UnitVer, LastVer, CorrId, Status, Created, Modified, UnitName, Payload]) ->
    case json_to_map(Payload) of
        Map when is_map(Map) ->
            {ok, Map#{
                tenantid => TenantId,
                unitid => UnitId,
                unitver => UnitVer,
                corrid => CorrId,
                status => Status,
                created => Created,
                modified => Modified,
                unitname => UnitName,
                isreadonly => LastVer > UnitVer
            }};
        _ ->
            {ok, #{
                tenantid => TenantId,
                unitid => UnitId,
                unitver => UnitVer,
                corrid => CorrId,
                status => Status,
                created => Created,
                modified => Modified,
                unitname => UnitName,
                isreadonly => LastVer > UnitVer
            }}
    end;
row_to_unit_map(_) ->
    {error, invalid_row}.

relation_row_to_map(Row) when is_tuple(Row) ->
    relation_row_to_map(tuple_to_list(Row));
relation_row_to_map([Map]) when is_map(Map) ->
    relation_row_to_map(Map);
relation_row_to_map([Inner]) when is_tuple(Inner); is_list(Inner) ->
    relation_row_to_map(Inner);
relation_row_to_map([Inner]) when is_map(Inner) ->
    case normalize_relation_map(Inner) of
        Rel when is_map(Rel), map_size(Rel) =:= 0 -> direct_relation_map(Inner);
        Rel -> Rel
    end;
relation_row_to_map(Row) when is_map(Row) ->
    case maps:is_key(<<"tenantid">>, Row) orelse maps:is_key(tenantid, Row) of
        true ->
            direct_relation_map(Row);
        false ->
            case map_lookup(Row, [rel, <<"rel">>, <<"REL">>]) of
                RelMap when is_map(RelMap) ->
                    relation_row_to_map(RelMap);
                _ ->
                    case normalize_relation_map(Row) of
                        Rel when is_map(Rel), map_size(Rel) =:= 0 -> direct_relation_map(Row);
                        Rel -> Rel
                    end
            end
    end;
relation_row_to_map(Row) when is_list(Row), length(Row) >= 5 ->
    [TenantId, UnitId, RelType, RelTenantId, RelUnitId | _] = Row,
    #{
        tenantid => TenantId,
        unitid => UnitId,
        reltype => RelType,
        reltenantid => RelTenantId,
        relunitid => RelUnitId
    };
relation_row_to_map(_) ->
    #{}.

association_row_to_map(Row) when is_tuple(Row) ->
    association_row_to_map(tuple_to_list(Row));
association_row_to_map([Map]) when is_map(Map) ->
    association_row_to_map(Map);
association_row_to_map([Inner]) when is_tuple(Inner); is_list(Inner) ->
    association_row_to_map(Inner);
association_row_to_map([Inner]) when is_map(Inner) ->
    case normalize_association_map(Inner) of
        Assoc when is_map(Assoc), map_size(Assoc) =:= 0 -> direct_association_map(Inner);
        Assoc -> Assoc
    end;
association_row_to_map(Row) when is_map(Row) ->
    case maps:is_key(<<"tenantid">>, Row) orelse maps:is_key(tenantid, Row) of
        true ->
            direct_association_map(Row);
        false ->
            case map_lookup(Row, [assoc, <<"assoc">>, <<"ASSOC">>]) of
                AssocMap when is_map(AssocMap) ->
                    association_row_to_map(AssocMap);
                _ ->
                    case normalize_association_map(Row) of
                        Assoc when is_map(Assoc), map_size(Assoc) =:= 0 -> direct_association_map(Row);
                        Assoc -> Assoc
                    end
            end
    end;
association_row_to_map(Row) when is_list(Row), length(Row) >= 4 ->
    [TenantId, UnitId, AssocType, AssocString | _] = Row,
    #{
        tenantid => TenantId,
        unitid => UnitId,
        assoctype => AssocType,
        assocstring => normalize_string(AssocString)
    };
association_row_to_map(_) ->
    #{}.

get_map_value(_Map, []) ->
    undefined;
get_map_value(Map, [Key | Rest]) when is_map(Map) ->
    case maps:get(Key, Map, undefined) of
        undefined -> get_map_value(Map, Rest);
        Value -> Value
    end.

map_lookup(Map, Keys) when is_map(Map), is_list(Keys) ->
    get_map_value(Map, Keys).

normalize_relation_map(Map) when is_map(Map) ->
    TenantId = map_lookup(Map, [tenantid, <<"tenantid">>, <<"tenantId">>]),
    case TenantId of
        undefined ->
            #{};
        _ ->
            #{
                tenantid => TenantId,
                unitid => map_lookup(Map, [unitid, <<"unitid">>, <<"unitId">>]),
                reltype => map_lookup(Map, [reltype, <<"reltype">>, <<"relType">>]),
                reltenantid => map_lookup(Map, [reltenantid, <<"reltenantid">>, <<"relTenantId">>]),
                relunitid => map_lookup(Map, [relunitid, <<"relunitid">>, <<"relUnitId">>])
            }
    end.

normalize_association_map(Map) when is_map(Map) ->
    TenantId = map_lookup(Map, [tenantid, <<"tenantid">>, <<"tenantId">>]),
    case TenantId of
        undefined ->
            #{};
        _ ->
            #{
                tenantid => TenantId,
                unitid => map_lookup(Map, [unitid, <<"unitid">>, <<"unitId">>]),
                assoctype => map_lookup(Map, [assoctype, <<"assoctype">>, <<"assocType">>]),
                assocstring => normalize_string(map_lookup(Map, [assocstring, <<"assocstring">>, <<"assocString">>]))
            }
    end.

direct_relation_map(Map) when is_map(Map) ->
    TenantId = maps:get(<<"tenantid">>, Map, maps:get(tenantid, Map, undefined)),
    case TenantId of
        undefined ->
            #{};
        _ ->
            #{
                tenantid => TenantId,
                unitid => maps:get(<<"unitid">>, Map, maps:get(unitid, Map, undefined)),
                reltype => maps:get(<<"reltype">>, Map, maps:get(reltype, Map, undefined)),
                reltenantid => maps:get(<<"reltenantid">>, Map, maps:get(reltenantid, Map, undefined)),
                relunitid => maps:get(<<"relunitid">>, Map, maps:get(relunitid, Map, undefined))
            }
    end.

direct_association_map(Map) when is_map(Map) ->
    TenantId = maps:get(<<"tenantid">>, Map, maps:get(tenantid, Map, undefined)),
    case TenantId of
        undefined ->
            #{};
        _ ->
            #{
                tenantid => TenantId,
                unitid => maps:get(<<"unitid">>, Map, maps:get(unitid, Map, undefined)),
                assoctype => maps:get(<<"assoctype">>, Map, maps:get(assoctype, Map, undefined)),
                assocstring => normalize_string(maps:get(<<"assocstring">>, Map, maps:get(assocstring, Map, undefined)))
            }
    end.

relation_row_fallback([Map]) when is_map(Map) ->
    relation_row_fallback(Map);
relation_row_fallback([Inner]) when is_tuple(Inner); is_list(Inner) ->
    relation_row_fallback(Inner);
relation_row_fallback(Row) when is_map(Row) ->
    case map_lookup(Row, [rel, <<"rel">>, <<"REL">>]) of
        RelMap when is_map(RelMap) ->
            relation_row_fallback(RelMap);
        _ ->
            direct_relation_map(Row)
    end;
relation_row_fallback(Row) when is_tuple(Row) ->
    relation_row_fallback(tuple_to_list(Row));
relation_row_fallback(_) ->
    #{}.

association_row_fallback([Map]) when is_map(Map) ->
    association_row_fallback(Map);
association_row_fallback([Inner]) when is_tuple(Inner); is_list(Inner) ->
    association_row_fallback(Inner);
association_row_fallback(Row) when is_map(Row) ->
    case map_lookup(Row, [assoc, <<"assoc">>, <<"ASSOC">>]) of
        AssocMap when is_map(AssocMap) ->
            association_row_fallback(AssocMap);
        _ ->
            direct_association_map(Row)
    end;
association_row_fallback(Row) when is_tuple(Row) ->
    association_row_fallback(tuple_to_list(Row));
association_row_fallback(_) ->
    #{}.

relation_row_binary([Map]) when is_map(Map) ->
    relation_row_binary(Map);
relation_row_binary([Inner]) when is_tuple(Inner); is_list(Inner) ->
    relation_row_binary(Inner);
relation_row_binary(Row) when is_tuple(Row) ->
    relation_row_binary(tuple_to_list(Row));
relation_row_binary(Row) when is_map(Row) ->
    case maps:is_key(<<"tenantid">>, Row) andalso
         maps:is_key(<<"unitid">>, Row) andalso
         maps:is_key(<<"reltype">>, Row) andalso
         maps:is_key(<<"reltenantid">>, Row) andalso
         maps:is_key(<<"relunitid">>, Row) of
        true ->
            #{
                tenantid => maps:get(<<"tenantid">>, Row),
                unitid => maps:get(<<"unitid">>, Row),
                reltype => maps:get(<<"reltype">>, Row),
                reltenantid => maps:get(<<"reltenantid">>, Row),
                relunitid => maps:get(<<"relunitid">>, Row)
            };
        false ->
            case map_lookup(Row, [rel, <<"rel">>, <<"REL">>]) of
                RelMap when is_map(RelMap) ->
                    relation_row_binary(RelMap);
                _ ->
                    case {map_lookup(Row, [tenantid, <<"tenantid">>, <<"tenantId">>]),
                          map_lookup(Row, [unitid, <<"unitid">>, <<"unitId">>]),
                          map_lookup(Row, [reltype, <<"reltype">>, <<"relType">>]),
                          map_lookup(Row, [reltenantid, <<"reltenantid">>, <<"relTenantId">>]),
                          map_lookup(Row, [relunitid, <<"relunitid">>, <<"relUnitId">>])} of
                        {undefined, _, _, _, _} -> #{};
                        {_, undefined, _, _, _} -> #{};
                        {_, _, undefined, _, _} -> #{};
                        {_, _, _, undefined, _} -> #{};
                        {_, _, _, _, undefined} -> #{};
                        {TenantId, UnitId, RelType, RelTenantId, RelUnitId} ->
                            #{
                                tenantid => TenantId,
                                unitid => UnitId,
                                reltype => RelType,
                                reltenantid => RelTenantId,
                                relunitid => RelUnitId
                            }
                    end
            end
    end;
relation_row_binary(_) ->
    #{}.

association_row_binary([Map]) when is_map(Map) ->
    association_row_binary(Map);
association_row_binary([Inner]) when is_tuple(Inner); is_list(Inner) ->
    association_row_binary(Inner);
association_row_binary(Row) when is_tuple(Row) ->
    association_row_binary(tuple_to_list(Row));
association_row_binary(Row) when is_map(Row) ->
    case maps:is_key(<<"tenantid">>, Row) andalso
         maps:is_key(<<"unitid">>, Row) andalso
         maps:is_key(<<"assoctype">>, Row) andalso
         maps:is_key(<<"assocstring">>, Row) of
        true ->
            #{
                tenantid => maps:get(<<"tenantid">>, Row),
                unitid => maps:get(<<"unitid">>, Row),
                assoctype => maps:get(<<"assoctype">>, Row),
                assocstring => normalize_string(maps:get(<<"assocstring">>, Row))
            };
        false ->
            case map_lookup(Row, [assoc, <<"assoc">>, <<"ASSOC">>]) of
                AssocMap when is_map(AssocMap) ->
                    association_row_binary(AssocMap);
                _ ->
                    case {map_lookup(Row, [tenantid, <<"tenantid">>, <<"tenantId">>]),
                          map_lookup(Row, [unitid, <<"unitid">>, <<"unitId">>]),
                          map_lookup(Row, [assoctype, <<"assoctype">>, <<"assocType">>]),
                          map_lookup(Row, [assocstring, <<"assocstring">>, <<"assocString">>])} of
                        {undefined, _, _, _} -> #{};
                        {_, undefined, _, _} -> #{};
                        {_, _, undefined, _} -> #{};
                        {_, _, _, undefined} -> #{};
                        {TenantId, UnitId, AssocType, AssocString} ->
                            #{
                                tenantid => TenantId,
                                unitid => UnitId,
                                assoctype => AssocType,
                                assocstring => normalize_string(AssocString)
                            }
                    end
            end
    end;
association_row_binary(_) ->
    #{}.

neo4j_query(Cypher, Params) ->
    with_http(fun() ->
        case ensure_bootstrap() of
            ok ->
                neo4j_query_with_retry(Cypher, Params);
            Error ->
                Error
        end
    end).

neo4j_query_with_retry(Cypher, Params) ->
    MaxAttempts = retry_attempts(),
    BaseMs = retry_base_ms(),
    neo4j_query_with_retry(Cypher, Params, 1, MaxAttempts, BaseMs).

neo4j_query_with_retry(Cypher, Params, Attempt, MaxAttempts, BaseMs) ->
    case neo4j_query_once(Cypher, Params) of
        {ok, _} = Ok ->
            Ok;
        Error ->
            case Attempt < MaxAttempts andalso is_retriable(Error) of
                true ->
                    BackoffMs = BaseMs * (1 bsl (Attempt - 1)),
                    timer:sleep(BackoffMs),
                    neo4j_query_with_retry(Cypher, Params, Attempt + 1, MaxAttempts, BaseMs);
                false ->
                    Error
            end
    end.

neo4j_query_once(Cypher, Params) ->
    Url = neo4j_url(),
    Headers = [
        {"Content-Type", "application/json"},
        {"Authorization", authorization_header()}
    ],
    Payload = map_to_json(#{statements => [#{statement => Cypher, parameters => Params}]}),
    Request = {Url, Headers, "application/json", binary_to_list(Payload)},
    HTTPOpts = [{connect_timeout, connect_timeout_ms()}, {timeout, timeout_ms()}, {ssl, ssl_options()}],
    case httpc:request(post, Request, HTTPOpts, []) of
        {ok, {{_Vsn, 200, _Reason}, _RespHeaders, Body0}} ->
            parse_neo4j_response(to_binary(Body0));
        {ok, {{_Vsn, Code, Reason}, _RespHeaders, Body0}} ->
            {error, {neo4j_http_error, Code, Reason, to_binary(Body0)}};
        Error ->
            {error, {neo4j_request_failed, Error}}
    end.

ensure_bootstrap() ->
    case bootstrap_enabled() of
        false ->
            ok;
        true ->
            case persistent_term:get(?BOOTSTRAP_KEY, false) of
                true ->
                    ok;
                _ ->
                    bootstrap_schema()
            end
    end.

bootstrap_schema() ->
    Statements = bootstrap_statements(),
    case run_bootstrap_statements(Statements) of
        ok ->
            persistent_term:put(?BOOTSTRAP_KEY, true),
            ok;
        Error ->
            Error
    end.

run_bootstrap_statements([]) ->
    ok;
run_bootstrap_statements([Stmt | Rest]) ->
    case neo4j_query_with_retry(Stmt, #{}) of
        {ok, _} ->
            run_bootstrap_statements(Rest);
        Error ->
            {error, {neo4j_bootstrap_failed, Stmt, Error}}
    end.

bootstrap_statements() ->
    [
        "CREATE CONSTRAINT unit_kernel_key IF NOT EXISTS FOR (k:UnitKernel) REQUIRE (k.tenantid, k.unitid) IS UNIQUE",
        "CREATE CONSTRAINT unit_version_key IF NOT EXISTS FOR (v:UnitVersion) REQUIRE (v.tenantid, v.unitid, v.unitver) IS UNIQUE",
        "CREATE CONSTRAINT counter_name_key IF NOT EXISTS FOR (c:Counter) REQUIRE c.name IS UNIQUE",
        "CREATE CONSTRAINT attribute_id_key IF NOT EXISTS FOR (a:Attribute) REQUIRE a.id IS UNIQUE",
        "CREATE CONSTRAINT attribute_name_key IF NOT EXISTS FOR (a:Attribute) REQUIRE a.name IS UNIQUE",
        "CREATE CONSTRAINT tenant_id_key IF NOT EXISTS FOR (t:Tenant) REQUIRE t.id IS UNIQUE",
        "CREATE CONSTRAINT tenant_name_key IF NOT EXISTS FOR (t:Tenant) REQUIRE t.name IS UNIQUE",
        "CREATE CONSTRAINT association_ref_key IF NOT EXISTS FOR (a:AssociationRef) REQUIRE (a.assoctype, a.refstring) IS UNIQUE",
        "CREATE INDEX unit_kernel_status_idx IF NOT EXISTS FOR (k:UnitKernel) ON (k.status)",
        "CREATE INDEX unit_kernel_created_idx IF NOT EXISTS FOR (k:UnitKernel) ON (k.created)",
        "CREATE INDEX unit_version_name_idx IF NOT EXISTS FOR (v:UnitVersion) ON (v.unitname)"
    ].

is_retriable({error, {neo4j_request_failed, _}}) ->
    true;
is_retriable({error, {neo4j_http_error, Code, _Reason, _Body}}) when Code =:= 429; Code >= 500 ->
    true;
is_retriable({error, {neo4j_query_error, Errors}}) ->
    has_transient_neo4j_error(Errors);
is_retriable(_) ->
    false.

has_transient_neo4j_error([]) ->
    false;
has_transient_neo4j_error([Error | Rest]) when is_map(Error) ->
    case maps:get(<<"code">>, Error, <<"">>) of
        Code when is_binary(Code) ->
            case binary:match(Code, <<"Neo.TransientError">>) of
                nomatch -> has_transient_neo4j_error(Rest);
                _ -> true
            end;
        _ ->
            has_transient_neo4j_error(Rest)
    end;
has_transient_neo4j_error([_ | Rest]) ->
    has_transient_neo4j_error(Rest).

parse_neo4j_response(Body) ->
    try
        Decoded = json:decode(Body),
        Errors = maps:get(<<"errors">>, Decoded, []),
        case Errors of
            [] ->
                Results = maps:get(<<"results">>, Decoded, []),
                case Results of
                    [First | _] ->
                        Data = maps:get(<<"data">>, First, []),
                        Rows = [maps:get(<<"row">>, Entry, []) || Entry <- Data],
                        {ok, Rows};
                    _ ->
                        {ok, []}
                end;
            _ ->
                {error, {neo4j_query_error, Errors}}
        end
    catch
        _:Reason -> {error, {neo4j_invalid_response, Reason}}
    end.

with_http(Fun) ->
    case application:ensure_all_started(inets) of
        {ok, _} -> Fun();
        {error, {already_started, inets}} -> Fun();
        {error, {already_started, _Other}} -> Fun();
        Error -> {error, {inets_start_failed, Error}}
    end.

neo4j_url() ->
    Base = env_str("EREPO_NEO4J_URL", "http://localhost:7474"),
    Db = env_str("EREPO_NEO4J_DATABASE", ?DEFAULT_DB),
    Base ++ "/db/" ++ Db ++ "/tx/commit".

authorization_header() ->
    User = env_str("EREPO_NEO4J_USER", "neo4j"),
    Pass = env_str("EREPO_NEO4J_PASSWORD", "neo4j"),
    Enc = base64:encode_to_string(User ++ ":" ++ Pass),
    "Basic " ++ Enc.

env_str(Name, Default) ->
    case os:getenv(Name) of
        false -> Default;
        Value -> Value
    end.

env_int(Name, Default) ->
    case os:getenv(Name) of
        false ->
            Default;
        Value ->
            case string:to_integer(string:trim(Value)) of
                {I, _} -> I;
                _ -> Default
            end
    end.

env_bool(Name, Default) ->
    case os:getenv(Name) of
        false ->
            Default;
        Value ->
            Lower = string:lowercase(string:trim(Value)),
            case Lower of
                "1" -> true;
                "true" -> true;
                "yes" -> true;
                "on" -> true;
                "0" -> false;
                "false" -> false;
                "no" -> false;
                "off" -> false;
                _ -> Default
            end
    end.

timeout_ms() ->
    env_int("EREPO_NEO4J_TIMEOUT_MS", ?DEFAULT_TIMEOUT_MS).

connect_timeout_ms() ->
    env_int("EREPO_NEO4J_CONNECT_TIMEOUT_MS", ?DEFAULT_CONNECT_TIMEOUT_MS).

retry_attempts() ->
    Retries = env_int("EREPO_NEO4J_RETRIES", ?DEFAULT_RETRIES),
    case Retries < 0 of
        true -> 1;
        false -> Retries + 1
    end.

retry_base_ms() ->
    Base = env_int("EREPO_NEO4J_RETRY_BASE_MS", ?DEFAULT_RETRY_BASE_MS),
    case Base < 1 of
        true -> 1;
        false -> Base
    end.

bootstrap_enabled() ->
    env_bool("EREPO_NEO4J_BOOTSTRAP", true).

ssl_options() ->
    case env_bool("EREPO_NEO4J_SSL_VERIFY", false) of
        true ->
            [{verify, verify_peer}];
        false ->
            [{verify, verify_none}]
    end.

map_to_json(Map) ->
    unicode:characters_to_binary(json:encode(normalize_json(Map))).

normalize_json(Value) when is_map(Value) ->
    maps:from_list([{json_key(K), normalize_json(V)} || {K, V} <- maps:to_list(Value)]);
normalize_json(Value) when is_list(Value) ->
    case is_string_list(Value) of
        true -> unicode:characters_to_binary(Value);
        false -> [normalize_json(V) || V <- Value]
    end;
normalize_json(Value) ->
    Value.

is_string_list(Value) when is_list(Value) ->
    try
        _ = unicode:characters_to_binary(Value),
        true
    catch
        _:_ -> false
    end.

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
json_to_map(_) ->
    undefined.

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

normalize_string(Value) when is_binary(Value) ->
    Value;
normalize_string(Value) when is_list(Value) ->
    unicode:characters_to_binary(Value);
normalize_string(Value) ->
    unicode:characters_to_binary(io_lib:format("~p", [Value])).

normalize_nullable_string(undefined) ->
    null;
normalize_nullable_string(null) ->
    null;
normalize_nullable_string(Value) ->
    normalize_string(Value).

to_binary(V) when is_binary(V) ->
    V;
to_binary(V) when is_list(V) ->
    unicode:characters_to_binary(V);
to_binary(V) ->
    unicode:characters_to_binary(io_lib:format("~p", [V])).

normalize_search_expression(undefined) ->
    {ok, #{}};
normalize_search_expression(Expression) when is_map(Expression) ->
    {ok, normalize_expr_keys(Expression)};
normalize_search_expression(Expression) when is_binary(Expression) ->
    erepo_search_parser:parse(Expression);
normalize_search_expression(Expression) when is_list(Expression) ->
    case is_proplist(Expression) of
        true -> {ok, normalize_expr_keys(maps:from_list(Expression))};
        false -> erepo_search_parser:parse(Expression)
    end;
normalize_search_expression(_) ->
    {error, invalid_query}.

is_proplist([]) ->
    true;
is_proplist([{_Key, _Value} | Rest]) ->
    is_proplist(Rest);
is_proplist(_) ->
    false.

normalize_expr_keys(Expr) ->
    maps:from_list([{normalize_expr_key(K), V} || {K, V} <- maps:to_list(Expr)]).

normalize_expr_key(<<"tenantid">>) -> tenantid;
normalize_expr_key(<<"unitid">>) -> unitid;
normalize_expr_key(<<"status">>) -> status;
normalize_expr_key(<<"name">>) -> name;
normalize_expr_key(<<"name_ilike">>) -> name_ilike;
normalize_expr_key(<<"created_after">>) -> created_after;
normalize_expr_key(<<"created_before">>) -> created_before;
normalize_expr_key(K) -> K.

build_where(Expression) ->
    Entries = maps:to_list(Expression),
    build_where_entries(Entries, [], #{}).

build_where_entries([], ClausesAcc, ParamsAcc) ->
    Sql =
        case lists:reverse(ClausesAcc) of
            [] -> "";
            Clauses -> " AND " ++ string:join(Clauses, " AND ")
        end,
    {ok, Sql, ParamsAcc};
build_where_entries([{Key, Value} | Rest], ClausesAcc, ParamsAcc) ->
    case where_clause(Key, Value) of
        skip ->
            build_where_entries(Rest, ClausesAcc, ParamsAcc);
        {Clause, K, V} ->
            build_where_entries(Rest, [Clause | ClausesAcc], ParamsAcc#{K => V});
        {error, _} = Error ->
            Error
    end.

where_clause(tenantid, Value) when is_integer(Value) ->
    {"k.tenantid = $tenantid", tenantid, Value};
where_clause(unitid, Value) when is_integer(Value) ->
    {"k.unitid = $unitid", unitid, Value};
where_clause(status, Value) when is_integer(Value) ->
    {"k.status = $status", status, Value};
where_clause(name, Value) ->
    {"coalesce(v.unitname, '') = $name", name, normalize_string(Value)};
where_clause(name_ilike, Value) ->
    {"coalesce(v.unitname, '') =~ $name_regex", name_regex, like_to_regex(Value)};
where_clause(created_after, Value) ->
    case to_int_maybe(Value) of
        {ok, N} -> {"toInteger(k.created) >= $created_after", created_after, N};
        error -> {error, {invalid_created_after, Value}}
    end;
where_clause(created_before, Value) ->
    case to_int_maybe(Value) of
        {ok, N} -> {"toInteger(k.created) < $created_before", created_before, N};
        error -> {error, {invalid_created_before, Value}}
    end;
where_clause(_, _) ->
    skip.

build_order({Field, Dir}) ->
    FieldSql =
        case Field of
            created -> "k.created";
            modified -> "v.modified";
            unitid -> "k.unitid";
            status -> "k.status";
            _ -> "k.created"
        end,
    DirSql =
        case Dir of
            asc -> "ASC";
            'ASC' -> "ASC";
            _ -> "DESC"
        end,
    {" ORDER BY " ++ FieldSql ++ " " ++ DirSql, #{}};
build_order(#{field := Field, dir := Dir}) ->
    build_order({Field, Dir});
build_order(_) ->
    {" ORDER BY k.created DESC", #{}}.

build_paging(#{limit := Limit, offset := Offset})
  when is_integer(Limit), Limit > 0, is_integer(Offset), Offset >= 0 ->
    {" SKIP $offset LIMIT $limit", #{offset => Offset, limit => Limit}};
build_paging(#{limit := Limit}) when is_integer(Limit), Limit > 0 ->
    {" LIMIT $limit", #{limit => Limit}};
build_paging(Limit) when is_integer(Limit), Limit > 0 ->
    {" LIMIT $limit", #{limit => Limit}};
build_paging(_) ->
    {"", #{}}.

unit_map_from_row(Row) ->
    case row_to_unit_map(Row) of
        {ok, Unit} -> Unit;
        _ -> #{}
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

like_to_regex(Value) ->
    Raw = unicode:characters_to_list(normalize_string(Value)),
    "(?i)^" ++ lists:flatten([regex_char(C) || C <- Raw]) ++ "$".

regex_char($%) -> ".*";
regex_char($_) -> ".";
regex_char(C) when C =:= $.; C =:= $^; C =:= $$; C =:= $*; C =:= $+; C =:= $?; C =:= $(; C =:= $); C =:= $[; C =:= $]; C =:= ${; C =:= $}; C =:= $|; C =:= $\\ ->
    [$\\, C];
regex_char(C) ->
    [C].

tenant_name(TenantId) ->
    iolist_to_binary(io_lib:format("tenant-~p", [TenantId])).

normalize_ref(#unit_ref{tenantid = TenantId, unitid = UnitId}) ->
    {TenantId, UnitId};
normalize_ref(#{tenantid := TenantId, unitid := UnitId}) ->
    {TenantId, UnitId};
normalize_ref({TenantId, UnitId}) ->
    {TenantId, UnitId};
normalize_ref(_) ->
    invalid_ref.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

relation_row_parsing_test() ->
    Row = [#{
        <<"tenantid">> => 9001,
        <<"unitid">> => 61,
        <<"reltype">> => 1,
        <<"reltenantid">> => 9001,
        <<"relunitid">> => 62
    }],
    WrappedRow = [#{<<"rel">> => hd(Row)}],
    WrappedRowAtom = [#{rel => hd(Row)}],
    Expected = #{
        tenantid => 9001,
        unitid => 61,
        reltype => 1,
        reltenantid => 9001,
        relunitid => 62
    },
    ?assertEqual(Expected, relation_row_binary(Row)),
    ?assertEqual(Expected, relation_row_to_map(Row)),
    ?assertEqual(Expected, relation_row_fallback(Row)),
    ?assertEqual(Expected, relation_row_binary(WrappedRow)),
    ?assertEqual(Expected, relation_row_to_map(WrappedRow)),
    ?assertEqual(Expected, relation_row_fallback(WrappedRow)),
    ?assertEqual(Expected, relation_row_binary(WrappedRowAtom)),
    ?assertEqual(Expected, relation_row_to_map(WrappedRowAtom)),
    ?assertEqual(Expected, relation_row_fallback(WrappedRowAtom)).

association_row_parsing_test() ->
    Row = [#{
        <<"tenantid">> => 9001,
        <<"unitid">> => 61,
        <<"assoctype">> => 2,
        <<"assocstring">> => <<"neo4j:assoc">>
    }],
    WrappedRow = [#{<<"assoc">> => hd(Row)}],
    WrappedRowAtom = [#{assoc => hd(Row)}],
    Expected = #{
        tenantid => 9001,
        unitid => 61,
        assoctype => 2,
        assocstring => <<"neo4j:assoc">>
    },
    ?assertEqual(Expected, association_row_binary(Row)),
    ?assertEqual(Expected, association_row_to_map(Row)),
    ?assertEqual(Expected, association_row_fallback(Row)),
    ?assertEqual(Expected, association_row_binary(WrappedRow)),
    ?assertEqual(Expected, association_row_to_map(WrappedRow)),
    ?assertEqual(Expected, association_row_fallback(WrappedRow)),
    ?assertEqual(Expected, association_row_binary(WrappedRowAtom)),
    ?assertEqual(Expected, association_row_to_map(WrappedRowAtom)),
    ?assertEqual(Expected, association_row_fallback(WrappedRowAtom)).

-endif.
