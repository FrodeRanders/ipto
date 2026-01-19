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
import org.gautelis.ipto.graphql.model.Query;
import org.gautelis.ipto.repo.model.AttributeType;
import org.gautelis.ipto.repo.model.Repository;
import org.gautelis.ipto.repo.model.Unit;
import org.gautelis.ipto.repo.model.attributes.Attribute;
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

        return new /* outermost */ UnitBox(unit, attributes);
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
                // Primitive attribute
                boxes.add(new /* inner */ PrimitiveBox(/* outer */ box, child, child.getValueVector()));
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
                // Primitive attribute
                boxes.add(new /* inner */ PrimitiveBox(/* outer */ box, child, child.getValueVector()));
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
