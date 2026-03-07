/*
 * Copyright (C) 2025-2026 Frode Randers
 * All rights reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gautelis.ipto.graphql.runtime.service;

import org.gautelis.ipto.graphql.model.CatalogAttribute;
import org.gautelis.ipto.graphql.model.FieldAliasResolver;
import org.gautelis.ipto.graphql.model.Query;
import org.gautelis.ipto.graphql.runtime.box.Box;
import org.gautelis.ipto.graphql.runtime.box.UnitBoxFactory;
import org.gautelis.ipto.repo.model.Repository;
import org.gautelis.ipto.repo.model.Unit;
import org.gautelis.ipto.repo.search.UnitSearch;
import org.gautelis.ipto.repo.search.query.AndExpression;
import org.gautelis.ipto.repo.search.query.DatabaseAdapter;
import org.gautelis.ipto.repo.search.query.QueryBuilder;
import org.gautelis.ipto.repo.search.query.SearchExpression;
import org.gautelis.ipto.repo.search.query.SearchExpressionQueryParser;
import org.gautelis.ipto.repo.search.query.SearchOrder;
import org.gautelis.ipto.repo.search.query.SearchStrategy;
import org.slf4j.Logger;

import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

final class RuntimeSearchService {
    private final Repository repo;
    private final Map<String, CatalogAttribute> allAttributesByAlias;
    private final Logger log;

    RuntimeSearchService(
            Repository repo,
            Map<String, CatalogAttribute> allAttributesByAlias,
            Logger log
    ) {
        this.repo = repo;
        this.allAttributesByAlias = allAttributesByAlias;
        this.log = log;
    }

    List<Box> search(Query.Filter filter) {
        log.trace("↪ RuntimeSearchService::search");

        Collection<Unit.Id> ids = searchIds(filter);
        if (ids.isEmpty()) {
            return List.of();
        }

        List<Box> units = new ArrayList<>();
        for (Unit.Id id : ids) {
            try {
                Optional<Unit> unit = repo.getUnit(id.tenantId(), id.unitId());
                if (unit.isPresent()) {
                    units.add(UnitBoxFactory.fromUnit(unit.get()));
                } else {
                    log.error("↪ Unknown unit: {}", id);
                }
            } catch (Throwable t) {
                log.error(t.getMessage(), t);
            }
        }
        return units;
    }

    byte[] searchRaw(Query.Filter filter) {
        log.trace("↪ RuntimeSearchService::searchRaw");

        Collection<Unit.Id> ids = searchIds(filter);
        List<Unit> units = new ArrayList<>();

        for (Unit.Id id : ids) {
            try {
                Optional<Unit> unit = repo.getUnit(id.tenantId(), id.unitId());
                if (unit.isPresent()) {
                    units.add(unit.get());
                } else {
                    log.error("↪ Unknown unit: {}", id);
                }
            } catch (Throwable t) {
                log.error(t.getMessage(), t);
            }
        }

        String json = "[";
        json += units.stream()
                .map(unit -> unit.asJson(false))
                .collect(Collectors.joining(", "));
        json += "]";
        return json.getBytes(StandardCharsets.UTF_8);
    }

    byte[] searchRawPayload(
            Query.Filter filter,
            Function<Unit, byte[]> rawPayloadExtractor
    ) {
        log.trace("↪ RuntimeSearchService::searchRawPayload");

        Collection<Unit.Id> ids = searchIds(filter);
        List<String> payloads = new ArrayList<>();

        for (Unit.Id id : ids) {
            try {
                Optional<Unit> unit = repo.getUnit(id.tenantId(), id.unitId());
                if (unit.isPresent()) {
                    byte[] payload = rawPayloadExtractor.apply(unit.get());
                    if (payload != null) {
                        payloads.add(new String(payload, StandardCharsets.UTF_8));
                    } else {
                        log.debug("↪ No raw_payload for unit {}", id);
                    }
                } else {
                    log.error("↪ Unknown unit: {}", id);
                }
            } catch (Throwable t) {
                log.error(t.getMessage(), t);
            }
        }

        String json = "[";
        json += String.join(", ", payloads);
        json += "]";
        return json.getBytes(StandardCharsets.UTF_8);
    }

    private Collection<Unit.Id> searchIds(Query.Filter filter) {
        SearchExpression expr = assembleConstraints(filter);

        SearchOrder order = resolveOrder(filter);
        // SearchStrategy.SET_OPS keeps execution in SQL set operations and scales better
        // than iterative lookups for broad filters.
        UnitSearch usd = new UnitSearch(expr, SearchStrategy.SET_OPS, order, filter.offset(), filter.size());
        DatabaseAdapter searchAdapter = repo.getDatabaseAdapter();

        Collection<Unit.Id> ids = new ArrayList<>();
        try {
            repo.withConnection(conn -> searchAdapter.search(conn, usd, repo.getTimingData(), rs -> {
                while (rs.next()) {
                    int j = 0;
                    int tenantId = rs.getInt(++j);
                    long unitId = rs.getLong(++j);
                    int unitVer = rs.getInt(++j);
                    Timestamp created = rs.getTimestamp(++j);
                    Timestamp modified = rs.getTimestamp(++j);

                    log.debug(
                            "↪ Found: unit={}.{}:{} created={} modified={}",
                            tenantId, unitId, unitVer, created, modified
                    );
                    ids.add(new Unit.Id(tenantId, unitId));
                }
            }));
        } catch (SQLException sqle) {
            log.error(sqle.getMessage(), sqle);
            return List.of();
        }

        return ids;
    }

    private SearchExpression assembleConstraints(Query.Filter filter) {
        int tenantId = filter.tenantId();
        // Runtime queries are always tenant-scoped and only expose EFFECTIVE units.
        SearchExpression expr = QueryBuilder.constrainToSpecificTenant(tenantId);
        expr = QueryBuilder.assembleAnd(expr, QueryBuilder.constrainToSpecificStatus(Unit.Status.EFFECTIVE));

        if (filter.where() != null && !filter.where().isBlank()) {
            SearchExpression textExpr = SearchExpressionQueryParser.parse(
                    filter.where(),
                    this::resolveAttribute,
                    SearchExpressionQueryParser.AttributeNameMode.NAMES_OR_ALIASES
            );
            return new AndExpression(expr, textExpr);
        }
        return expr;
    }

    private Optional<SearchExpressionQueryParser.ResolvedAttribute> resolveAttribute(String name) {
        return FieldAliasResolver.resolveCatalogAttribute(allAttributesByAlias, name)
                .map(attribute -> new SearchExpressionQueryParser.ResolvedAttribute(attribute.attrName(), attribute.attrType()));
    }

    private SearchOrder resolveOrder(Query.Filter filter) {
        if (filter == null || filter.orderBy() == null || filter.orderBy().isBlank()) {
            // Deterministic default ordering keeps paging stable.
            return SearchOrder.orderByUnitId(true);
        }

        String normalized = filter.orderBy().trim().toLowerCase(Locale.ROOT);
        boolean ascending = resolveDirection(filter.orderDirection(), normalized);

        return switch (normalized) {
            case "unitid", "unit_id", "unit" -> SearchOrder.orderByUnitId(ascending);
            case "created" -> SearchOrder.orderByCreation(ascending);
            case "modified", "updated" -> SearchOrder.orderByModified(ascending);
            default -> throw new IllegalArgumentException("orderBy must be one of: unitid, created, modified");
        };
    }

    private boolean resolveDirection(String direction, String orderBy) {
        if (direction == null || direction.isBlank()) {
            return switch (orderBy) {
                case "created", "modified", "updated" -> false;
                default -> true;
            };
        }
        String normalized = direction.trim().toLowerCase(Locale.ROOT);
        return switch (normalized) {
            case "asc", "ascending" -> true;
            case "desc", "descending" -> false;
            default -> throw new IllegalArgumentException("orderDirection must be asc or desc");
        };
    }
}
