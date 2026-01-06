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

import com.fasterxml.uuid.Generators;
import com.networknt.schema.*;
import graphql.schema.idl.RuntimeWiring;
import org.gautelis.ipto.repo.exceptions.InvalidParameterException;
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

import java.lang.Error;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;


public class RuntimeService {
    private static final Logger log = LoggerFactory.getLogger(RuntimeService.class);

    private final Repository repo;
    private final Map</* attribute alias */ String, CatalogAttribute> allAttributesByAlias = new HashMap<>();

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
        RuntimeOperators.wireUnits(runtimeWiring, this, gqlViewpoint, catalogViewpoint);
        RuntimeOperators.wireUnions(runtimeWiring, gqlViewpoint);
        // Operations are wired separately
    }

    public Object storeRawUnit(byte[] bytes) {
        log.trace("\u21aa RuntimeService::storeRawUnit({}...)", headHex(bytes, 16));

        String json = new String(bytes);

        List<com.networknt.schema.Error> errors = unitSchema.validate(json, InputFormat.JSON,
                executionContext -> executionContext
                        .executionConfig(executionConfig -> executionConfig.formatAssertionsEnabled(true)));

        if (errors.isEmpty()) {

            //Repository repo = RepositoryFactory.getRepository();
            //Unit unit = repo.createUnit(tenantId);

            UUID dataleveransId = Generators.timeBasedEpochGenerator().generate();
            return Map.of(
                    "dataleveransid", dataleveransId.toString(),
                    "data", bytes // Currently just an echo
            );
        } else {
            StringBuilder buf = new StringBuilder();
            errors.forEach(e -> buf.append('\n').append(e.getMessage()));
            log.info("\u21aa JSON validation errors: {}", buf);
            return Map.of("validation-errors", buf.toString());
        }
    }

    public Box loadUnit(int tenantId, long unitId) {
        log.trace("\u21aa RuntimeService::loadUnit({}, {})", tenantId, unitId);

        Optional<Unit> _unit = repo.getUnit(tenantId, unitId);
        if (_unit.isEmpty()) {
            log.trace("\u21aa No unit with id {}.{}", tenantId, unitId);
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
            log.debug("\u21aa No attributes for unit with id {}.{}", tenantId, unitId);
        }

        return new /* outermost */ UnitBox(unit, attributes);
    }

    public byte[] loadRawUnit(int tenantId, long unitId) {
        log.trace("\u21aa RuntimeService::loadRawUnit({}, {})", tenantId, unitId);

        Optional<Unit> unit = repo.getUnit(tenantId, unitId);
        if (unit.isEmpty()) {
            log.trace("\u21aa No unit with id {}.{}", tenantId, unitId);
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
        log.trace("\u21aa RuntimeService::getValueArray({}, {}, {})", fieldNames, box, isMandatory);

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
            log.trace("\u21aa Attribute(s) not present: {}", fieldNames);
            if (isMandatory) {
                log.info("\u21aa Mandatory field(s) not present: {}", fieldNames);
            }
            return null;
        }

        ArrayList<?> values = attribute.getValueVector();
        if (values.isEmpty()) {
            log.trace("\u21aa No values for attribute '{}'.", fieldName);
            if (isMandatory) {
                log.info("\u21aa Mandatory value(s) for field '{}' not present", fieldName);
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
        log.trace("\u21aa RuntimeService::getAttributeArray({}, {}, {})", fieldNames, box, isMandatory);

        String fieldName = null;
        Attribute<?> attribute = null;

        Iterator<String> fnit = fieldNames.iterator();
        if (fnit.hasNext()) {
            fieldName = fnit.next();

            attribute = box.getAttribute(fieldName);
            if (null == attribute) {
                log.trace("\u21aa No attribute '{}'.", fieldName);

                while (fnit.hasNext()) {
                    fieldName = fnit.next();
                    log.debug("\u21aa  ... trying '{}'.", fieldName);

                    attribute = box.getAttribute(fieldName);
                    if (attribute != null) {
                        // TODO I think we should assemble attributes for all field names
                        //      and not break after first, since we are operating on an array
                        break;
                    }
                    log.trace("\u21aa No attribute '{}'.", fieldName);
                }
            }
        }

        if (null == attribute) {
            if (isMandatory) {
                log.info("\u21aa Mandatory field(s) not present: {}", fieldNames);
            }
            return null;
        }

        ArrayList<?> values = attribute.getValueVector();
        if (values.isEmpty()) {
            log.trace("\u21aa No values for attribute '{}'.", fieldName);
            if (isMandatory) {
                log.info("\u21aa Mandatory value(s) for field '{}' not present", fieldName);
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
        log.trace("\u21aa RuntimeService::getValueScalar({}, {}, {})", fieldNames, box, isMandatory);

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
            log.trace("\u21aa Attribute(s) not present: {}", fieldNames);
            if (isMandatory) {
                log.info("\u21aa Mandatory field(s) not present: {}", fieldNames);
            }
            return null;
        }

        ArrayList<?> values = attribute.getValueVector();
        if (values.isEmpty()) {
            log.trace("\u21aa No values for attribute '{}'.", fieldName);
            if (isMandatory) {
                log.info("\u21aa Mandatory value(s) for field '{}' not present", fieldName);
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
        log.trace("\u21aa RuntimeService::getAttributeScalar({}, {}, {})", fieldNames, box, isMandatory);

        String fieldName = null;
        Attribute<?> attribute = null;

        Iterator<String> fnit = fieldNames.iterator();
        if (fnit.hasNext()) {
            fieldName = fnit.next();

            attribute = box.getAttribute(fieldName);
            if (null == attribute) {
                log.trace("\u21aa No attribute '{}'.", fieldName);

                while (fnit.hasNext()) {
                    fieldName = fnit.next();
                    log.debug("\u21aa  ... trying '{}'.", fieldName);

                    attribute = box.getAttribute(fieldName);
                    if (attribute != null) {
                        break;
                    }
                    log.trace("\u21aa No attribute '{}'.", fieldName);
                }
            }
        }

        if (null == attribute) {
            if (isMandatory) {
                log.info("\u21aa Mandatory field(s) not present: {}", fieldNames);
            }
            return null;
        }

        ArrayList<?> values = attribute.getValueVector();
        if (values.isEmpty()) {
            log.trace("\u21aa No values for attribute '{}'.", fieldName);
            if (isMandatory) {
                log.info("\u21aa Mandatory value(s) for field '{}' not present", fieldName);
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
        SearchOrder order = SearchOrder.orderByUnitId(true); // ascending on unit id
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

                    log.debug("\u21aa Found: unit=" + _tenantId + "." + _unitId + ":" + _unitVer + " created=" + _created + " modified=" + _modified);
                    ids.add(new Unit.Id(_tenantId, _unitId));
                }
            }));
        } catch (SQLException sqle) {
            log.error(sqle.getMessage(), sqle);
            return List.of();
        }

        return ids;
    }

    public List<Box> search(
            Query.Filter filter
    ) {
        log.trace("\u21aa RuntimeService::search");

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
                        log.error("\u21aa Unknown unit: {}", id);
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
        log.trace("\u21aa RuntimeService::searchRaw");

        Collection<Unit.Id> ids = search0(filter);

        List<Unit> units = new ArrayList<>();
        for (Unit.Id id : ids) {
            try {
                Optional<Unit> _unit = repo.getUnit(id.tenantId(), id.unitId());
                if (_unit.isPresent()) {
                    units.add(_unit.get());

                } else {
                    log.error("\u21aa Unknown unit: {}", id);
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

        // attribute constraints
        return new AndExpression(expr, assembleConstraints(filter.where()));
    }

    private SearchExpression assembleConstraints(
            Query.Node node
    ) {
        Query.AttributeExpression attrExpr = node.attrExpr();
        Query.TreeExpression treeExpr = node.treeExpr();

        if (null != treeExpr && null == attrExpr) {
            return assembleTreeConstraints(treeExpr);
        }
        else if (null != attrExpr && null == treeExpr) {
            return assembleAttributeConstraints(attrExpr);
        }
        else {
            throw new InvalidParameterException("Either Node.attrExpr or Node.treeExpr must be null, but not both.");
        }
    }

    private SearchExpression assembleTreeConstraints(
            Query.TreeExpression treeExpr
    ) {
        Query.Logical op = treeExpr.op();
        Query.Node left = treeExpr.left();
        Query.Node right = treeExpr.right();

        if (Objects.requireNonNull(op) == Query.Logical.AND)
            return QueryBuilder.assembleAnd(assembleConstraints(left), assembleConstraints(right));
        else // Logical.OR
            return QueryBuilder.assembleOr(assembleConstraints(left), assembleConstraints(right));

    }

    private LeafExpression<?> assembleAttributeConstraints(
            Query.AttributeExpression attrExpr
    ) {
        String attrName = attrExpr.attr();
        Query.FilterOperator op = attrExpr.op();
        String value = attrExpr.value();

        //---------------------------------------------------------------------
        // OBSERVE
        //    We are assuming that the field names used in the SDL equals the
        //    attribute aliases used.
        //---------------------------------------------------------------------
        CatalogAttribute catalogAttribute = allAttributesByAlias.get(attrName);
        if (null == catalogAttribute) {
            throw new InvalidParameterException("Unknown attribute " + attrName);
        }

        AttributeType attrType = catalogAttribute.attrType();
        switch (attrType) {
            case STRING -> {
                if (Objects.requireNonNull(op) == Query.FilterOperator.EQ) {
                    value = value.replace('*', '%');
                    boolean useLIKE = value.indexOf('%') >= 0 || value.indexOf('_') >= 0;  // Uses wildcard
                    if (useLIKE) {
                        return new LeafExpression<>(new StringAttributeSearchItem(attrName, Operator.LIKE, value));
                    } else {
                        return new LeafExpression<>(new StringAttributeSearchItem(attrName, Operator.EQ, value));
                    }
                }
                return new LeafExpression<>(new StringAttributeSearchItem(attrName, op.iptoOp(), value));
            }
            case TIME -> {
                return new LeafExpression<>(new TimeAttributeSearchItem(attrName, op.iptoOp(), Instant.parse(value)));
            }
            case INTEGER -> {
                return new LeafExpression<>(new IntegerAttributeSearchItem(attrName, op.iptoOp(), Integer.parseInt(value)));
            }
            case LONG -> {
                return new LeafExpression<>(new LongAttributeSearchItem(attrName, op.iptoOp(), Long.parseLong(value)));
            }
            case DOUBLE -> {
                return new LeafExpression<>(new DoubleAttributeSearchItem(attrName, op.iptoOp(), Double.parseDouble(value)));
            }
            case BOOLEAN -> {
                return new LeafExpression<>(new BooleanAttributeSearchItem(attrName, op.iptoOp(), Boolean.parseBoolean(value)));
            }
            default -> throw new InvalidParameterException("Attribute type " + attrType.name() + " is not searchable: " + attrName);
        }
    }
}
