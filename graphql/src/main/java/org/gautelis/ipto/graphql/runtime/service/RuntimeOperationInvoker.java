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

import org.gautelis.ipto.graphql.model.GqlOperationShape;
import org.gautelis.ipto.graphql.model.ParameterDefinition;
import org.gautelis.ipto.graphql.model.Query;
import org.gautelis.ipto.graphql.model.RuntimeOperation;
import org.gautelis.ipto.graphql.model.SchemaOperation;
import org.gautelis.ipto.graphql.model.TypeDefinition;
import tools.jackson.databind.ObjectMapper;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

final class RuntimeOperationInvoker {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final RuntimeService service;

    RuntimeOperationInvoker(RuntimeService service) {
        this.service = service;
    }

    Object invokeOperation(
            GqlOperationShape operation,
            Map<String, Object> arguments
    ) {
        Map<String, Object> typedArguments = coerceArguments(operation, arguments);
        RuntimeOperation runtimeOperation = operation.runtimeOperation();
        if (runtimeOperation != null) {
            // Explicit @ipto(operation: ...) always wins over shape-based inference.
            return invokeExplicitRuntimeOperation(operation, typedArguments, runtimeOperation);
        }
        String outputType = operation.outputTypeName();

        if (operation.category() == SchemaOperation.QUERY) {
            if (hasSingleParameterNamed(operation, "id")) {
                Object idValue = typedArguments.get("id");
                Optional<Query.UnitIdentification> unitIdentification = tryUnitIdentification(idValue);
                if (unitIdentification.isPresent()) {
                    Query.UnitIdentification id = unitIdentification.get();
                    if ("Bytes".equals(outputType)) {
                        return service.loadRawUnit(id.tenantId(), id.unitId());
                    }
                    return service.loadUnit(id.tenantId(), id.unitId());
                }

                Optional<TenantCorrId> corrIdentification = tryTenantCorrId(idValue);
                if (corrIdentification.isPresent()) {
                    TenantCorrId id = corrIdentification.get();
                    if ("Bytes".equals(outputType)) {
                        return service.loadRawPayloadByCorrId(id.tenantId(), id.corrId());
                    }
                    return service.loadUnitByCorrId(id.tenantId(), id.corrId());
                }
            }

            if (hasSingleParameterNamed(operation, "filter")) {
                Optional<Query.Filter> filterValue = tryFilter(typedArguments.get("filter"));
                if (filterValue.isEmpty()) {
                    throw new IllegalArgumentException("Operation '" + operation.operationName() + "' requires parameter 'filter' with at least tenantId");
                }
                Query.Filter filter = filterValue.get();
                if ("Bytes".equals(outputType)) {
                    return service.searchRaw(filter);
                }
                return service.search(filter);
            }
        }

        if (operation.category() == SchemaOperation.MUTATION) {
            if (hasSingleParameterOfType(operation, "Bytes", "data")) {
                byte[] data = (byte[]) typedArguments.get("data");
                return service.storeRawUnit(data);
            }
        }

        throw new IllegalArgumentException("No automatic runtime mapping for operation '" + operation.operationName() + "'");
    }

    private Object invokeExplicitRuntimeOperation(
            GqlOperationShape operation,
            Map<String, Object> typedArguments,
            RuntimeOperation runtimeOperation
    ) {
        return switch (runtimeOperation) {
            case LOAD_UNIT -> {
                Query.UnitIdentification id = requiredUnitIdentification(typedArguments, "id", operation.operationName(), runtimeOperation);
                yield service.loadUnit(id.tenantId(), id.unitId());
            }
            case LOAD_UNIT_RAW -> {
                Query.UnitIdentification id = requiredUnitIdentification(typedArguments, "id", operation.operationName(), runtimeOperation);
                yield service.loadRawUnit(id.tenantId(), id.unitId());
            }
            case LOAD_BY_CORRID -> {
                TenantCorrId id = requiredTenantCorrId(typedArguments, "id", operation.operationName(), runtimeOperation);
                yield service.loadUnitByCorrId(id.tenantId(), id.corrId());
            }
            case LOAD_RAW_PAYLOAD_BY_CORRID -> {
                TenantCorrId id = requiredTenantCorrId(typedArguments, "id", operation.operationName(), runtimeOperation);
                yield service.loadRawPayloadByCorrId(id.tenantId(), id.corrId());
            }
            case SEARCH -> {
                Query.Filter filter = requiredFilter(typedArguments, "filter", operation.operationName(), runtimeOperation);
                yield service.search(filter);
            }
            case SEARCH_RAW -> {
                Query.Filter filter = requiredFilter(typedArguments, "filter", operation.operationName(), runtimeOperation);
                yield service.searchRaw(filter);
            }
            case SEARCH_RAW_PAYLOAD -> {
                Query.Filter filter = requiredFilter(typedArguments, "filter", operation.operationName(), runtimeOperation);
                yield service.searchRawPayload(filter);
            }
            case STORE_RAW_UNIT -> {
                byte[] data = requiredBytes(typedArguments, "data", operation.operationName(), runtimeOperation);
                yield service.storeRawUnit(data);
            }
            case CUSTOM, MANUAL ->
                    throw new IllegalArgumentException("Operation '" + operation.operationName() + "' is marked as " + runtimeOperation + " and should be wired manually");
        };
    }

    private Map<String, Object> coerceArguments(
            GqlOperationShape operation,
            Map<String, Object> arguments
    ) {
        // GraphQL Java provides generic argument maps for input objects.
        // Convert to repo/runtime domain records before dispatch.
        Map<String, Object> coerced = new HashMap<>();
        for (ParameterDefinition parameter : operation.parameters()) {
            String name = parameter.parameterName();
            Object raw = arguments.get(name);
            if (raw == null) {
                continue;
            }
            TypeDefinition type = parameter.parameterType();
            coerced.put(name, coerceArgumentValue(type.typeName(), raw));
        }
        return coerced;
    }

    private Object coerceArgumentValue(String typeName, Object raw) {
        return switch (typeName) {
            case "UnitIdentification" -> MAPPER.convertValue(raw, Query.UnitIdentification.class);
            case "Filter" -> MAPPER.convertValue(raw, Query.Filter.class);
            case "Bytes" -> (raw instanceof byte[] bytes) ? bytes : MAPPER.convertValue(raw, byte[].class);
            case "Int" -> MAPPER.convertValue(raw, Integer.class);
            case "Long" -> MAPPER.convertValue(raw, Long.class);
            case "String" -> MAPPER.convertValue(raw, String.class);
            case "Boolean" -> MAPPER.convertValue(raw, Boolean.class);
            case "Float", "Double" -> MAPPER.convertValue(raw, Double.class);
            default -> raw;
        };
    }

    private Query.Filter requiredFilter(
            Map<String, Object> typedArguments,
            String parameterName,
            String operationName,
            RuntimeOperation runtimeOperation
    ) {
        Object value = typedArguments.get(parameterName);
        Optional<Query.Filter> filter = tryFilter(value);
        if (filter.isPresent()) {
            return filter.get();
        }
        throw new IllegalArgumentException("Operation '" + operationName + "' with runtime operation '" + runtimeOperation + "' requires parameter '" + parameterName + "' of type Filter");
    }

    private byte[] requiredBytes(
            Map<String, Object> typedArguments,
            String parameterName,
            String operationName,
            RuntimeOperation runtimeOperation
    ) {
        Object value = typedArguments.get(parameterName);
        if (value instanceof byte[] bytes) {
            return bytes;
        }
        throw new IllegalArgumentException("Operation '" + operationName + "' with runtime operation '" + runtimeOperation + "' requires parameter '" + parameterName + "' of type Bytes");
    }

    private Query.UnitIdentification requiredUnitIdentification(
            Map<String, Object> typedArguments,
            String parameterName,
            String operationName,
            RuntimeOperation runtimeOperation
    ) {
        Object value = typedArguments.get(parameterName);
        Optional<Query.UnitIdentification> id = tryUnitIdentification(value);
        if (id.isPresent()) {
            return id.get();
        }
        throw new IllegalArgumentException("Operation '" + operationName + "' with runtime operation '" + runtimeOperation + "' requires parameter '" + parameterName + "' with tenantId and unitId");
    }

    private TenantCorrId requiredTenantCorrId(
            Map<String, Object> typedArguments,
            String parameterName,
            String operationName,
            RuntimeOperation runtimeOperation
    ) {
        Object value = typedArguments.get(parameterName);
        Optional<TenantCorrId> id = tryTenantCorrId(value);
        if (id.isPresent()) {
            return id.get();
        }
        throw new IllegalArgumentException("Operation '" + operationName + "' with runtime operation '" + runtimeOperation + "' requires parameter '" + parameterName + "' with tenantId and corrId");
    }

    private Optional<Query.UnitIdentification> tryUnitIdentification(Object value) {
        if (value instanceof Query.UnitIdentification identification) {
            return Optional.of(identification);
        }
        if (value instanceof Map<?, ?> map) {
            Integer tenantId = parseInteger(map.get("tenantId"));
            Long unitId = parseLong(map.get("unitId"));
            if (tenantId != null && unitId != null) {
                return Optional.of(new Query.UnitIdentification(tenantId, unitId));
            }
        }
        return Optional.empty();
    }

    private Optional<Query.Filter> tryFilter(Object value) {
        if (value instanceof Query.Filter filter) {
            return Optional.of(filter);
        }
        if (value instanceof Map<?, ?> map) {
            Integer tenantId = parseInteger(map.get("tenantId"));
            if (tenantId == null) {
                return Optional.empty();
            }
            // Keep defaults aligned with Query.Filter semantics.
            String where = parseString(map.get("where"));
            Integer offset = parseInteger(map.get("offset"));
            Integer size = parseInteger(map.get("size"));
            String orderBy = parseString(map.get("orderBy"));
            String orderDirection = parseString(map.get("orderDirection"));
            return Optional.of(
                    new Query.Filter(
                            tenantId,
                            where,
                            offset != null ? offset : 0,
                            size != null ? size : 20,
                            orderBy,
                            orderDirection
                    )
            );
        }
        return Optional.empty();
    }

    private Optional<TenantCorrId> tryTenantCorrId(Object value) {
        if (value instanceof Query.YrkanIdentification identification) {
            return tryParseUuid(identification.corrId()).map(corrId -> new TenantCorrId(identification.tenantId(), corrId));
        }
        if (value instanceof Map<?, ?> map) {
            Integer tenantId = parseInteger(map.get("tenantId"));
            String corrId = parseString(map.get("corrId"));
            if (tenantId != null && corrId != null) {
                return tryParseUuid(corrId).map(uuid -> new TenantCorrId(tenantId, uuid));
            }
        }
        return Optional.empty();
    }

    private Optional<UUID> tryParseUuid(String value) {
        if (value == null || value.isBlank()) {
            return Optional.empty();
        }
        try {
            return Optional.of(UUID.fromString(value.trim()));
        } catch (IllegalArgumentException iae) {
            return Optional.empty();
        }
    }

    private Integer parseInteger(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Integer i) {
            return i;
        }
        if (value instanceof Number number) {
            return number.intValue();
        }
        if (value instanceof String s && !s.isBlank()) {
            try {
                return Integer.parseInt(s.trim());
            } catch (NumberFormatException nfe) {
                return null;
            }
        }
        return null;
    }

    private Long parseLong(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Long l) {
            return l;
        }
        if (value instanceof Number number) {
            return number.longValue();
        }
        if (value instanceof String s && !s.isBlank()) {
            try {
                return Long.parseLong(s.trim());
            } catch (NumberFormatException nfe) {
                return null;
            }
        }
        return null;
    }

    private String parseString(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof String s) {
            return s;
        }
        return value.toString();
    }

    private boolean hasSingleParameterNamed(
            GqlOperationShape operation,
            String parameterName
    ) {
        if (operation.parameters().size() != 1) {
            return false;
        }
        ParameterDefinition parameter = operation.parameters().getFirst();
        return parameterName.equals(parameter.parameterName());
    }

    private boolean hasSingleParameterOfType(
            GqlOperationShape operation,
            String typeName,
            String parameterName
    ) {
        if (operation.parameters().size() != 1) {
            return false;
        }
        ParameterDefinition parameter = operation.parameters().getFirst();
        return parameterName.equals(parameter.parameterName())
                && typeName.equals(parameter.parameterType().typeName());
    }

    private record TenantCorrId(int tenantId, UUID corrId) {}
}
