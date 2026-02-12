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
package org.gautelis.ipto.graphql.runtime;

import com.networknt.schema.*;
import graphql.schema.idl.RuntimeWiring;
import org.gautelis.ipto.graphql.configuration.Configurator;
import org.gautelis.ipto.graphql.model.CatalogAttribute;
import org.gautelis.ipto.graphql.model.GqlOperationShape;
import org.gautelis.ipto.graphql.model.ParameterDefinition;
import org.gautelis.ipto.graphql.model.Query;
import org.gautelis.ipto.graphql.model.SchemaOperation;
import org.gautelis.ipto.graphql.model.TypeDefinition;
import org.gautelis.ipto.repo.model.AttributeType;
import org.gautelis.ipto.repo.model.Repository;
import org.gautelis.ipto.repo.model.Unit;
import org.gautelis.ipto.repo.model.attributes.Attribute;
import org.gautelis.ipto.repo.search.SearchResult;
import org.gautelis.ipto.repo.search.UnitSearch;
import org.gautelis.ipto.repo.search.model.*;
import org.gautelis.ipto.repo.search.query.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;
import java.util.stream.Collectors;


public class RuntimeService {
    private static final Logger log = LoggerFactory.getLogger(RuntimeService.class);

    private final Repository repo;
    private final Map</* attribute alias */ String, CatalogAttribute> allAttributesByAlias = new HashMap<>();

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Schema unitSchema;

    public RuntimeService(
            Repository repo,
            Configurator.CatalogViewpoint catalogView
    ) {
        this.repo = repo;

        // Rearrange attributes found in catalog view, since
        // we will refer to them through their aliases
        for (CatalogAttribute attribute : catalogView.attributes().values()) {
            String alias = attribute.alias();
            if (alias != null && !alias.isEmpty()) {
                allAttributesByAlias.put(alias, attribute);
            }
        }

        SchemaRegistry schemaRegistry = SchemaRegistry.withDefaultDialect(SpecificationVersion.DRAFT_2020_12);
        unitSchema = schemaRegistry.getSchema(SchemaLocation.of("classpath:schema/unit.json"));
        if (null != unitSchema) {
            unitSchema.initializeValidators();
        } else {
            log.warn("No schema for validation");
        }
    }

    public static String headHex(byte[] bytes, int n) {
        int len = Math.min(bytes.length, n);
        String hex = HexFormat.of().formatHex(bytes, 0, len);
        return hex.replaceAll("..(?!$)", "$0 ");
    }

    public void wire(
            RuntimeWiring.Builder runtimeWiring,
            Configurator.GqlViewpoint gqlViewpoint,
            Configurator.CatalogViewpoint catalogViewpoint
    ) {
        RuntimeOperators.wireRecords(runtimeWiring, this, gqlViewpoint);
        RuntimeOperators.wireUnions(runtimeWiring, gqlViewpoint);
        // Operations are wired separately
    }

    public void wireOperations(
            RuntimeWiring.Builder runtimeWiring,
            Configurator.GqlViewpoint gqlViewpoint
    ) {
        RuntimeOperators.wireOperations(runtimeWiring, this, gqlViewpoint);
    }

    public Object invokeOperation(
            GqlOperationShape operation,
            Map<String, Object> arguments
    ) {
        Map<String, Object> typedArguments = coerceArguments(operation, arguments);
        String runtimeOperation = normalizeRuntimeOperation(operation.runtimeOperation());
        if (runtimeOperation != null) {
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
                        return loadRawUnit(id.tenantId(), id.unitId());
                    }
                    return loadUnit(id.tenantId(), id.unitId());
                }

                Optional<TenantCorrId> corrIdentification = tryTenantCorrId(idValue);
                if (corrIdentification.isPresent()) {
                    TenantCorrId id = corrIdentification.get();
                    if ("Bytes".equals(outputType)) {
                        return loadRawPayloadByCorrId(id.tenantId(), id.corrId());
                    }
                    return loadUnitByCorrId(id.tenantId(), id.corrId());
                }
            }

            if (hasSingleParameterNamed(operation, "filter")) {
                Optional<Query.Filter> filterValue = tryFilter(typedArguments.get("filter"));
                if (filterValue.isEmpty()) {
                    throw new IllegalArgumentException("Operation '" + operation.operationName() + "' requires parameter 'filter' with at least tenantId");
                }
                Query.Filter filter = filterValue.get();
                if ("Bytes".equals(outputType)) {
                    return searchRaw(filter);
                }
                return search(filter);
            }
        }

        if (operation.category() == SchemaOperation.MUTATION) {
            if (hasSingleParameterOfType(operation, "Bytes", "data")) {
                byte[] data = (byte[]) typedArguments.get("data");
                return storeRawUnit(data);
            }
        }

        throw new IllegalArgumentException("No automatic runtime mapping for operation '" + operation.operationName() + "'");
    }

    private Object invokeExplicitRuntimeOperation(
            GqlOperationShape operation,
            Map<String, Object> typedArguments,
            String runtimeOperation
    ) {
        return switch (runtimeOperation) {
            case "LOAD_UNIT" -> {
                Query.UnitIdentification id = requiredUnitIdentification(typedArguments, "id", operation.operationName(), runtimeOperation);
                yield loadUnit(id.tenantId(), id.unitId());
            }
            case "LOAD_UNIT_RAW" -> {
                Query.UnitIdentification id = requiredUnitIdentification(typedArguments, "id", operation.operationName(), runtimeOperation);
                yield loadRawUnit(id.tenantId(), id.unitId());
            }
            case "LOAD_BY_CORRID" -> {
                TenantCorrId id = requiredTenantCorrId(typedArguments, "id", operation.operationName(), runtimeOperation);
                yield loadUnitByCorrId(id.tenantId(), id.corrId());
            }
            case "LOAD_RAW_PAYLOAD_BY_CORRID" -> {
                TenantCorrId id = requiredTenantCorrId(typedArguments, "id", operation.operationName(), runtimeOperation);
                yield loadRawPayloadByCorrId(id.tenantId(), id.corrId());
            }
            case "SEARCH" -> {
                Query.Filter filter = requiredFilter(typedArguments, "filter", operation.operationName(), runtimeOperation);
                yield search(filter);
            }
            case "SEARCH_RAW" -> {
                Query.Filter filter = requiredFilter(typedArguments, "filter", operation.operationName(), runtimeOperation);
                yield searchRaw(filter);
            }
            case "SEARCH_RAW_PAYLOAD" -> {
                Query.Filter filter = requiredFilter(typedArguments, "filter", operation.operationName(), runtimeOperation);
                yield searchRawPayload(filter);
            }
            case "STORE_RAW_UNIT" -> {
                byte[] data = requiredBytes(typedArguments, "data", operation.operationName(), runtimeOperation);
                yield storeRawUnit(data);
            }
            default -> throw new IllegalArgumentException("Unknown runtime operation '" + runtimeOperation + "' for operation '" + operation.operationName() + "'");
        };
    }

    private String normalizeRuntimeOperation(String runtimeOperation) {
        if (runtimeOperation == null) {
            return null;
        }
        String normalized = runtimeOperation.trim();
        if (normalized.isEmpty()) {
            return null;
        }
        return normalized.toUpperCase(Locale.ROOT);
    }

    private Map<String, Object> coerceArguments(
            GqlOperationShape operation,
            Map<String, Object> arguments
    ) {
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
            String runtimeOperation
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
            String runtimeOperation
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
            String runtimeOperation
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
            String runtimeOperation
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

    private record TenantCorrId(int tenantId, UUID corrId) {}

    private boolean hasSingleParameterNamed(
            GqlOperationShape operation,
            String parameterName
    ) {
        if (operation.parameters().length != 1) {
            return false;
        }
        ParameterDefinition parameter = operation.parameters()[0];
        return parameterName.equals(parameter.parameterName());
    }

    private boolean hasSingleParameterOfType(
            GqlOperationShape operation,
            String typeName,
            String parameterName
    ) {
        if (operation.parameters().length != 1) {
            return false;
        }
        ParameterDefinition parameter = operation.parameters()[0];
        return parameterName.equals(parameter.parameterName())
                && typeName.equals(parameter.parameterType().typeName());
    }

    public Object storeRawUnit(byte[] bytes) {
        log.trace("↪ RuntimeService::storeRawUnit({}...)", headHex(bytes, 16));

        String json = new String(bytes);

        List<com.networknt.schema.Error> errors = unitSchema.validate(json, InputFormat.JSON,
                executionContext -> executionContext
                        .executionConfig(executionConfig -> executionConfig.formatAssertionsEnabled(true)));

        if (errors.isEmpty()) {
            JsonNode root = MAPPER.readTree(json);
            Unit unit = repo.storeUnit(root);
            return unit.asJson(/* pretty? */ false).getBytes(StandardCharsets.UTF_8);

        } else {
            StringBuilder buf = new StringBuilder();
            errors.forEach(e -> buf.append('\n').append(e.getMessage()));
            log.info("↪ JSON validation errors: {}", buf);
            return Map.of("validation-errors", buf.toString());
        }
    }

    public Box loadUnit(int tenantId, long unitId) {
        log.trace("↪ RuntimeService::loadUnit({}, {})", tenantId, unitId);

        Optional<Unit> _unit = repo.getUnit(tenantId, unitId);
        if (_unit.isEmpty()) {
            log.trace("↪ No unit with id {}.{}", tenantId, unitId);
            return null;
        }
        Unit unit = _unit.get();

        //---------------------------------------------------------------------
        // OBSERVE
        //    We are assuming that the field names used in the SDL equals the
        //    attribute aliases used.
        //---------------------------------------------------------------------
        Map</* field name */ String, Attribute<?>> attributes = new HashMap<>();

        unit.getAttributes().forEach(attr -> {
            attributes.put(attr.getAlias(), attr); // here we assume alias == field name
        });

        if (attributes.isEmpty()) {
            log.debug("↪ No attributes for unit with id {}.{}", tenantId, unitId);
        }

        return new /* outermost */ AttributeBox(unit, attributes);
    }

    public Box loadUnitByCorrId(int tenantId, UUID corrId) {
        log.trace("↪ RuntimeService::loadUnitByCorrId({}, {})", tenantId, corrId);

        Unit unit = findUnitByCorrId(tenantId, corrId);
        if (unit == null) {
            log.trace("↪ No unit with corrid {} for tenant {}", corrId, tenantId);
            return null;
        }

        //---------------------------------------------------------------------
        // OBSERVE
        //    We are assuming that the field names used in the SDL equals the
        //    attribute aliases used.
        //---------------------------------------------------------------------
        Map</* field name */ String, Attribute<?>> attributes = new HashMap<>();

        unit.getAttributes().forEach(attr -> {
            attributes.put(attr.getAlias(), attr); // here we assume alias == field name
        });

        if (attributes.isEmpty()) {
            log.debug("↪ No attributes for unit identified by correlation ID {} in tenant {}", corrId, tenantId);
        }

        return new /* outermost */ AttributeBox(unit, attributes);
    }



    public byte[] loadRawUnit(int tenantId, long unitId) {
        log.trace("↪ RuntimeService::loadRawUnit({}, {})", tenantId, unitId);

        Optional<Unit> unit = repo.getUnit(tenantId, unitId);
        if (unit.isEmpty()) {
            log.trace("↪ No unit with id {}.{}", tenantId, unitId);
            return null;
        }

        String json = unit.get().asJson(/* pretty? */ false);
        return json.getBytes(StandardCharsets.UTF_8);
    }

    public byte[] loadRawPayload(int tenantId, long unitId) {
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

    public byte[] loadRawPayloadByCorrId(int tenantId, UUID corrId) {
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

    public Object getValueArray(
            List<String> fieldNames,
            RecordBox box,
            boolean isMandatory
    ) {
        log.trace("↪ RuntimeService::getValueArray({}, {}, {})", fieldNames, box, isMandatory);

        Attribute<?> attribute = null;
        String fieldName = null;

        for (String name : fieldNames) {
            fieldName = name;

            Attribute<Attribute<?>> recordAttribute = box.getRecordAttribute();
            ArrayList<Attribute<?>> values = recordAttribute.getValueVector();

            for (Attribute<?> attr : values) {
                if (attr.getAlias().equals(fieldName)) {
                    attribute = attr;
                    break;
                }
            }
        }

        if (null == attribute) {
            log.trace("↪ Attribute(s) not present: {}", fieldNames);
            if (isMandatory) {
                log.info("↪ Mandatory field(s) not present: {}", fieldNames);
            }
            return null;
        }

        ArrayList<?> values = attribute.getValueVector();
        if (values.isEmpty()) {
            log.trace("↪ No values for attribute '{}'.", fieldName);
            if (isMandatory) {
                log.info("↪ Mandatory value(s) for field '{}' not present", fieldName);
            }
            return null;
        }

        // 1) non-record attribute case
        if (!AttributeType.RECORD.equals(attribute.getType())) {
            return values;
        }

        // 2) record attribute case
        //---------------------------------------------------------------------
        // OBSERVE
        //    We are assuming that the field names used in the SDL equals the
        //    attribute aliases used.
        //---------------------------------------------------------------------
        @SuppressWarnings("unchecked") // since attribute _is_ RECORD, i.e. Attribute<Attribute<?>>
        ArrayList<Attribute<?>> children = (ArrayList<Attribute<?>>) attribute.getValueVector();
        ArrayList<Box> boxes = new ArrayList<>();

        children.forEach(child -> {
            if (!AttributeType.RECORD.equals(child.getType())) {
                // Non-record entries are still represented as attribute maps so field resolution stays uniform.
                boxes.add(new /* inner */ AttributeBox(/* outer */ box, Map.of(child.getAlias(), child)));
            } else {
                @SuppressWarnings("unchecked") // since child _is_ RECORD, i.e. Attribute<Attribute<?>>
                Attribute<Attribute<?>> childAsRecord =  (Attribute<Attribute<?>>) child;
                boxes.add(new /* inner */ RecordBox(/* outer */ box,  childAsRecord, Map.of(child.getAlias(), child)));
            }
        });

        return boxes;
    }

    public Object getAttributeArray(
            List<String> fieldNames,
            AttributeBox box,
            boolean isMandatory
    ) {
        log.trace("↪ RuntimeService::getAttributeArray({}, {}, {})", fieldNames, box, isMandatory);

        String fieldName = null;
        Attribute<?> attribute = null;

        Iterator<String> fnit = fieldNames.iterator();
        if (fnit.hasNext()) {
            fieldName = fnit.next();

            attribute = box.getAttribute(fieldName);
            if (null == attribute) {
                log.trace("↪ No attribute '{}'.", fieldName);

                while (fnit.hasNext()) {
                    fieldName = fnit.next();
                    log.debug("↪  ... trying '{}'.", fieldName);

                    attribute = box.getAttribute(fieldName);
                    if (attribute != null) {
                        // TODO I think we should assemble attributes for all field names
                        //      and not break after first, since we are operating on an array
                        break;
                    }
                    log.trace("↪ No attribute '{}'.", fieldName);
                }
            }
        }

        if (null == attribute) {
            if (isMandatory) {
                log.info("↪ Mandatory field(s) not present: {}", fieldNames);
            }
            return null;
        }

        ArrayList<?> values = attribute.getValueVector();
        if (values.isEmpty()) {
            log.trace("↪ No values for attribute '{}'.", fieldName);
            if (isMandatory) {
                log.info("↪ Mandatory value(s) for field '{}' not present", fieldName);
            }
            return null;
        }

        // 1) non-record attribute case
        if (!AttributeType.RECORD.equals(attribute.getType())) {
            return values;
        }

        // 2) record attribute case
        //---------------------------------------------------------------------
        // OBSERVE
        //    We are assuming that the field names used in the SDL equals the
        //    attribute aliases used.
        //---------------------------------------------------------------------
        @SuppressWarnings("unchecked") // since attribute _is_ RECORD, i.e. Attribute<Attribute<?>>
        ArrayList<Attribute<?>> children = (ArrayList<Attribute<?>>) attribute.getValueVector();
        ArrayList<Box> boxes = new ArrayList<>();

        children.forEach(child -> {
            if (!AttributeType.RECORD.equals(child.getType())) {
                // Non-record entries are still represented as attribute maps so field resolution stays uniform.
                boxes.add(new /* inner */ AttributeBox(/* outer */ box, Map.of(child.getAlias(), child)));
            } else {
                @SuppressWarnings("unchecked") // since child _is_ RECORD, i.e. Attribute<Attribute<?>>
                Attribute<Attribute<?>> childAsRecord =  (Attribute<Attribute<?>>) child;
                boxes.add(new /* inner */ RecordBox(/* outer */ box,  childAsRecord, Map.of(child.getAlias(), child)));
            }
        });

        return boxes;
    }

    public Object getAttributeArray(
            List<String> fieldNames,
            AttributeBox box
    ) {
        return getAttributeArray(fieldNames, box, false);
    }

    public Object getValueScalar(
            List<String> fieldNames,
            RecordBox box,
            boolean isMandatory
    ) {
        log.trace("↪ RuntimeService::getValueScalar({}, {}, {})", fieldNames, box, isMandatory);

        Attribute<?> attribute = null;
        String fieldName = null;

        for (String name : fieldNames) {
            fieldName = name;

            Attribute<Attribute<?>> recordAttribute = box.getRecordAttribute();
            ArrayList<Attribute<?>> values = recordAttribute.getValueVector();

            for (Attribute<?> attr : values) {
                if (attr.getAlias().equals(fieldName)) {
                    attribute = attr;
                    break;
                }
            }
        }

        if (null == attribute) {
            log.trace("↪ Attribute(s) not present: {}", fieldNames);
            if (isMandatory) {
                log.info("↪ Mandatory field(s) not present: {}", fieldNames);
            }
            return null;
        }

        ArrayList<?> values = attribute.getValueVector();
        if (values.isEmpty()) {
            log.trace("↪ No values for attribute '{}'.", fieldName);
            if (isMandatory) {
                log.info("↪ Mandatory value(s) for field '{}' not present", fieldName);
            }
            return null;
        }

        // 1) non-record attribute case
        if (!AttributeType.RECORD.equals(attribute.getType())) {
            return values.getFirst(); // since scalar
        }

        // 2) record attribute case
        //---------------------------------------------------------------------
        // OBSERVE
        //    We are assuming that the field names used in the SDL equals the
        //    attribute aliases used.
        //---------------------------------------------------------------------
        Map</* field name */ String, Attribute<?>> attributes = new HashMap<>();

        @SuppressWarnings("unchecked") // since attribute _is_ RECORD, i.e. Attribute<Attribute<?>>
        ArrayList<Attribute<?>> children = (ArrayList<Attribute<?>>) values;
        children.forEach(attr -> {
            attributes.put(attr.getAlias(), attr);  // here we assume alias == field name
        });

        @SuppressWarnings("unchecked") // since attribute _is_ RECORD, i.e. Attribute<Attribute<?>>
        Attribute<Attribute<?>> attributeAsRecord = (Attribute<Attribute<?>>) attribute;
        return new /* inner */ RecordBox(/* outer */ box, attributeAsRecord, attributes);
    }

    public Object getAttributeScalar(
            List<String> fieldNames,
            AttributeBox box,
            boolean isMandatory
    ) {
        log.trace("↪ RuntimeService::getAttributeScalar({}, {}, {})", fieldNames, box, isMandatory);

        String fieldName = null;
        Attribute<?> attribute = null;

        Iterator<String> fnit = fieldNames.iterator();
        if (fnit.hasNext()) {
            fieldName = fnit.next();

            attribute = box.getAttribute(fieldName);
            if (null == attribute) {
                log.trace("↪ No attribute '{}'.", fieldName);

                while (fnit.hasNext()) {
                    fieldName = fnit.next();
                    log.debug("↪  ... trying '{}'.", fieldName);

                    attribute = box.getAttribute(fieldName);
                    if (attribute != null) {
                        break;
                    }
                    log.trace("↪ No attribute '{}'.", fieldName);
                }
            }
        }

        if (null == attribute) {
            if (isMandatory) {
                log.info("↪ Mandatory field(s) not present: {}", fieldNames);
            }
            return null;
        }

        ArrayList<?> values = attribute.getValueVector();
        if (values.isEmpty()) {
            log.trace("↪ No values for attribute '{}'.", fieldName);
            if (isMandatory) {
                log.info("↪ Mandatory value(s) for field '{}' not present", fieldName);
            }
            return null;
        }

        // 1) non-record attribute case
        if (!AttributeType.RECORD.equals(attribute.getType())) {
            return values.getFirst(); // since scalar
        }

        // 2) record attribute case
        //---------------------------------------------------------------------
        // OBSERVE
        //    We are assuming that the field names used in the SDL equals the
        //    attribute aliases used.
        //---------------------------------------------------------------------
        Map</* field name */ String, Attribute<?>> attributes = new HashMap<>();

        @SuppressWarnings("unchecked") // since attribute _is_ RECORD, i.e. Attribute<Attribute<?>>
        ArrayList<Attribute<?>> children = (ArrayList<Attribute<?>>) values;
        children.forEach(attr -> {
            attributes.put(attr.getAlias(), attr);  // here we assume alias == field name
        });

        @SuppressWarnings("unchecked") // since attribute _is_ RECORD, i.e. Attribute<Attribute<?>>
        Attribute<Attribute<?>> attributeAsRecord = (Attribute<Attribute<?>>) attribute;
        return new /* inner */ RecordBox(/* outer */ box, attributeAsRecord, attributes);
    }

    public Object getAttributeScalar(
            List<String> fieldNames,
            AttributeBox box
    ) {
        return getAttributeScalar(fieldNames, box,false);
    }

    private Collection<Unit.Id> search0(
            Query.Filter filter
    ) {
        SearchExpression expr = assembleConstraints(filter);

        // Result set constraints (paging)
        SearchOrder order = resolveOrder(filter);
        UnitSearch usd = new UnitSearch(expr, SearchStrategy.SET_OPS, order, filter.offset(), filter.size());

        // Build SQL statement for search
        DatabaseAdapter searchAdapter = repo.getDatabaseAdapter();

        Collection<Unit.Id> ids = new ArrayList<>();
        try {
            repo.withConnection(conn -> searchAdapter.search(conn, usd, repo.getTimingData(), rs -> {
                while (rs.next()) {
                    int j = 0;
                    int _tenantId = rs.getInt(++j);
                    long _unitId = rs.getLong(++j);
                    int _unitVer = rs.getInt(++j);
                    Timestamp _created = rs.getTimestamp(++j);
                    Timestamp _modified = rs.getTimestamp(++j);

                    log.debug(
                            "↪ Found: unit={}.{}:{} created={} modified={}",
                            _tenantId, _unitId, _unitVer, _created, _modified
                    );
                    ids.add(new Unit.Id(_tenantId, _unitId));
                }
            }));
        } catch (SQLException sqle) {
            log.error(sqle.getMessage(), sqle);
            return List.of();
        }

        return ids;
    }

    private SearchOrder resolveOrder(Query.Filter filter) {
        if (filter == null || filter.orderBy() == null || filter.orderBy().isBlank()) {
            return SearchOrder.orderByUnitId(true);
        }

        String normalized = filter.orderBy().trim().toLowerCase(Locale.ROOT);
        boolean ascending = resolveDirection(filter.orderDirection(), normalized);

        return switch (normalized) {
            case "unitid", "unit_id", "unit" -> SearchOrder.orderByUnitId(ascending);
            case "created" -> SearchOrder.orderByCreation(ascending);
            case "modified", "updated" -> SearchOrder.orderByModified(ascending);
            default -> throw new IllegalArgumentException(
                    "orderBy must be one of: unitId, created, modified"
            );
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

    public List<Box> search(
            Query.Filter filter
    ) {
        log.trace("↪ RuntimeService::search");

        Collection<Unit.Id> ids = search0(filter);

        if (ids.isEmpty()) {
            return List.of();
        } else {
            List<Box> units = new ArrayList<>();
            for (Unit.Id id : ids) {
                try {
                    Optional<Unit> _unit = repo.getUnit(id.tenantId(), id.unitId());
                    if (_unit.isPresent()) {
                        Unit unit = _unit.get();

                        //---------------------------------------------------------------------
                        // OBSERVE
                        //    We are assuming that the field names used in the SDL equals the
                        //    attribute aliases used.
                        //---------------------------------------------------------------------
                        Map</* field name */ String, Attribute<?>> attributes = new HashMap<>();

                        for (Attribute<?> attr : unit.getAttributes()) {
                            attributes.put(attr.getAlias(), attr); // here we assume alias == field name
                        }
                        units.add(new /* outermost */ AttributeBox(unit, attributes));

                    } else {
                        log.error("↪ Unknown unit: {}", id);
                    }
                } catch (Throwable t) {
                    log.error(t.getMessage(), t);
                }
            }
            return units;
        }
    }

    public byte[] searchRaw(
            Query.Filter filter
    ) {
        log.trace("↪ RuntimeService::searchRaw");

        Collection<Unit.Id> ids = search0(filter);

        List<Unit> units = new ArrayList<>();
        for (Unit.Id id : ids) {
            try {
                Optional<Unit> _unit = repo.getUnit(id.tenantId(), id.unitId());
                if (_unit.isPresent()) {
                    units.add(_unit.get());

                } else {
                    log.error("↪ Unknown unit: {}", id);
                }
            } catch (Throwable t) {
                log.error(t.getMessage(), t);
            }
        }

        String json = "[";
        json += units.stream()
                .map(unit -> unit.asJson(/* pretty? */ false))
                .collect(Collectors.joining(", "));
        json += "]";
        return json.getBytes(StandardCharsets.UTF_8);
    }

    public byte[] searchRawPayload(
            Query.Filter filter
    ) {
        log.trace("↪ RuntimeService::searchRawPayload");

        Collection<Unit.Id> ids = search0(filter);

        List<String> payloads = new ArrayList<>();
        for (Unit.Id id : ids) {
            try {
                Optional<Unit> _unit = repo.getUnit(id.tenantId(), id.unitId());
                if (_unit.isPresent()) {
                    byte[] payload = rawPayload(_unit.get());
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

    private Unit findUnitByCorrId(int tenantId, UUID corrId) {
        String query = "tenantid = " + tenantId + " AND corrid = \"" + corrId + "\"";
        SearchExpression expr = SearchExpressionQueryParser.parse(query, repo);
        SearchResult result = repo.searchUnit(1, 1, 1, expr, SearchOrder.getDefaultOrder());
        if (result.results().size() > 1) {
            throw new IllegalArgumentException("Multiple units found for corrid " + corrId);
        }
        return result.results().stream().findFirst().orElse(null);
    }

    /****************** Search related ******************/

    private SearchExpression assembleConstraints(
            Query.Filter filter
    ) {
        // Implicit unit constraints
        int tenantId = filter.tenantId();
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
        CatalogAttribute byAlias = allAttributesByAlias.get(name);
        if (byAlias != null) {
            return Optional.of(new SearchExpressionQueryParser.ResolvedAttribute(byAlias.attrName(), byAlias.attrType()));
        }
        for (CatalogAttribute attribute : allAttributesByAlias.values()) {
            if (name.equals(attribute.attrName()) || name.equals(attribute.qualifiedName())) {
                return Optional.of(new SearchExpressionQueryParser.ResolvedAttribute(attribute.attrName(), attribute.attrType()));
            }
        }
        return Optional.empty();
    }
}
