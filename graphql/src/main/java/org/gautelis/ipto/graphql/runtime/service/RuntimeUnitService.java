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

import org.gautelis.ipto.graphql.runtime.box.Box;
import org.gautelis.ipto.graphql.runtime.box.UnitBoxFactory;
import org.gautelis.ipto.repo.model.AttributeType;
import org.gautelis.ipto.repo.model.Repository;
import org.gautelis.ipto.repo.model.Unit;
import org.gautelis.ipto.repo.model.attributes.Attribute;
import org.gautelis.ipto.repo.search.SearchResult;
import org.gautelis.ipto.repo.search.query.SearchExpression;
import org.gautelis.ipto.repo.search.query.SearchExpressionQueryParser;
import org.gautelis.ipto.repo.search.query.SearchOrder;
import org.slf4j.Logger;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Optional;
import java.util.UUID;

final class RuntimeUnitService {
    private final Repository repo;
    private final Logger log;

    RuntimeUnitService(Repository repo, Logger log) {
        this.repo = repo;
        this.log = log;
    }

    Box loadUnit(int tenantId, long unitId) {
        log.trace("↪ RuntimeService::loadUnit({}, {})", tenantId, unitId);

        Optional<Unit> unit = repo.getUnit(tenantId, unitId);
        if (unit.isEmpty()) {
            log.trace("↪ No unit with id {}.{}", tenantId, unitId);
            return null;
        }
        if (unit.get().getAttributes().isEmpty()) {
            log.debug("↪ No attributes for unit with id {}.{}", tenantId, unitId);
        }
        return UnitBoxFactory.fromUnit(unit.get());
    }

    Box loadUnitByCorrId(int tenantId, UUID corrId) {
        log.trace("↪ RuntimeService::loadUnitByCorrId({}, {})", tenantId, corrId);

        Unit unit = findUnitByCorrId(tenantId, corrId);
        if (unit == null) {
            log.trace("↪ No unit with corrid {} for tenant {}", corrId, tenantId);
            return null;
        }
        if (unit.getAttributes().isEmpty()) {
            log.debug("↪ No attributes for unit identified by correlation ID {} in tenant {}", corrId, tenantId);
        }
        return UnitBoxFactory.fromUnit(unit);
    }

    byte[] loadRawUnit(int tenantId, long unitId) {
        log.trace("↪ RuntimeService::loadRawUnit({}, {})", tenantId, unitId);

        Optional<Unit> unit = repo.getUnit(tenantId, unitId);
        if (unit.isEmpty()) {
            log.trace("↪ No unit with id {}.{}", tenantId, unitId);
            return null;
        }
        return unit.get().asJson(false).getBytes(StandardCharsets.UTF_8);
    }

    byte[] loadRawPayload(int tenantId, long unitId) {
        log.trace("↪ RuntimeService::loadRawPayload({}, {})", tenantId, unitId);

        Optional<Unit> unit = repo.getUnit(tenantId, unitId);
        if (unit.isEmpty()) {
            log.trace("↪ No unit with id {}.{}", tenantId, unitId);
            return null;
        }
        byte[] payload = rawPayload(unit.get());
        if (payload == null) {
            log.trace("↪ No raw_payload for unit {}.{}", tenantId, unitId);
        }
        return payload;
    }

    byte[] loadRawPayload(Unit unit) {
        return rawPayload(unit);
    }

    byte[] loadRawPayloadByCorrId(int tenantId, UUID corrId) {
        log.trace("↪ RuntimeService::loadRawPayloadByCorrId({}, {})", tenantId, corrId);

        Unit unit = findUnitByCorrId(tenantId, corrId);
        if (unit == null) {
            log.trace("↪ No unit with corrid {} for tenant {}", corrId, tenantId);
            return null;
        }
        byte[] payload = rawPayload(unit);
        if (payload == null) {
            log.trace("↪ No raw_payload for unit {}", unit.getReference());
        }
        return payload;
    }

    private Unit findUnitByCorrId(int tenantId, UUID corrId) {
        String query = "tenantid = " + tenantId + " AND corrid = \"" + corrId + "\"";
        SearchExpression expr = SearchExpressionQueryParser.parse(query, repo);
        SearchResult result = repo.searchUnit(1, 1, 1, expr, SearchOrder.getDefaultOrder());
        if (result.results().size() > 1) {
            throw new IllegalArgumentException("Multiple units found for corrid " + corrId);
        }
        return result.results().stream().findFirst().orElse(null);
    }

    private byte[] rawPayload(Unit unit) {
        for (Attribute<?> attr : unit.getAttributes()) {
            if ("raw_payload".equals(attr.getAlias())) {
                if (!AttributeType.DATA.equals(attr.getType())) {
                    log.warn("↪ raw_payload attribute is not DATA on unit {}", unit.getReference());
                    return null;
                }
                ArrayList<?> values = attr.getValueVector();
                if (values.isEmpty()) {
                    return null;
                }
                Object value = values.getFirst();
                if (value instanceof byte[] bytes) {
                    return bytes;
                }
                log.warn("↪ raw_payload value is not byte[] on unit {}", unit.getReference());
                return null;
            }
        }
        return null;
    }
}
