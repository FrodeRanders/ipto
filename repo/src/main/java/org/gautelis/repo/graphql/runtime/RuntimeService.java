package org.gautelis.repo.graphql.runtime;

import org.gautelis.repo.RepositoryFactory;
import org.gautelis.repo.exceptions.InvalidParameterException;
import org.gautelis.repo.graphql.configuration.Configurator;
import org.gautelis.repo.graphql.model.CatalogAttribute;
import org.gautelis.repo.graphql.model.Query;
import org.gautelis.repo.model.AttributeType;
import org.gautelis.repo.model.Repository;
import org.gautelis.repo.model.Unit;
import org.gautelis.repo.model.attributes.Attribute;
import org.gautelis.repo.search.UnitSearch;
import org.gautelis.repo.search.model.*;
import org.gautelis.repo.search.query.*;
import org.gautelis.repo.utils.TimeHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    }

    public byte[] storeRawUnit(byte[] bytes) {
        // TODO!!!  Not implemented
        log.error("NOT IMPLEMENTED: storeRawUnit");

        log.trace("Store raw unit bytes {}", bytes);

        Repository repo = RepositoryFactory.getRepository();
        int tenantId = 1;
        Unit unit = repo.createUnit(tenantId);

        String json = unit.asJson(/* pretty? */ false);
        return json.getBytes(StandardCharsets.UTF_8);
    }

    public Box loadUnit(int tenantId, long unitId) {
        log.trace("RuntimeService::loadUnit({}, {})", tenantId, unitId);

        Optional<Unit> _unit = repo.getUnit(tenantId, unitId);
        if (_unit.isEmpty()) {
            log.trace("No unit with id {}.{}", tenantId, unitId);
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
            log.debug("No attributes for unit with id {}.{}", tenantId, unitId);
        }

        return new /* outermost */ UnitBox(unit, attributes);
    }

    public byte[] loadRawUnit(int tenantId, long unitId) {
        log.trace("RuntimeService::loadRawUnit({}, {})", tenantId, unitId);

        Optional<Unit> unit = repo.getUnit(tenantId, unitId);
        if (unit.isEmpty()) {
            log.trace("No unit with id {}.{}", tenantId, unitId);
            return null;
        }

        String json = unit.get().asJson(/* pretty? */ false);
        return json.getBytes(StandardCharsets.UTF_8);
    }

    public Object getArray(List<String> fieldNames,  Box box, boolean isMandatory) {
        log.trace("RuntimeService::getArray({}, {}, {})", fieldNames, box, isMandatory);

        String fieldName = null;
        Attribute<?> attribute = null;

        Iterator<String> fnit = fieldNames.iterator();
        if (fnit.hasNext()) {
            fieldName = fnit.next();

            attribute = box.getAttribute(fieldName);
            if (null == attribute) {
                log.trace("No attribute '{}'.", fieldName);

                while (fnit.hasNext()) {
                    fieldName = fnit.next();
                    log.debug(" ... trying '{}'.", fieldName);

                    attribute = box.getAttribute(fieldName);
                    if (attribute != null) {
                        break;
                    }
                    log.trace("No attribute '{}'.", fieldName);
                }
            }
        }

        if (null == attribute) {
            if (isMandatory) {
                log.info("Mandatory field(s) not present: {}", fieldNames);
            }
            return null;
        }

        ArrayList<?> values = attribute.getValueVector();
        if (values.isEmpty()) {
            log.trace("No values for attribute '{}'.", fieldName);
            if (isMandatory) {
                log.info("Mandatory value(s) for field '{}' not present", fieldName);
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
        Map</* field name */ String, Attribute<?>> attributes = new HashMap<>();

        ArrayList<Attribute<?>> children = (ArrayList<Attribute<?>>) attribute.getValueVector();
        children.forEach(attr -> {
            attributes.put(attr.getAlias(), attr); // here we assume alias == field name
        });

        return new /* inner */ Box(/* outer */ box, attributes);
    }

    public Object getArray(List<String> fieldNames, Box box) {
        return getArray(fieldNames, box, false);
    }

    public Object getScalar(List<String> fieldNames, Box box, boolean isMandatory) {
        log.trace("RuntimeService::getScalar({}, {}, {})", fieldNames, box, isMandatory);

        String fieldName = null;
        Attribute<?> attribute = null;

        Iterator<String> fnit = fieldNames.iterator();
        if (fnit.hasNext()) {
            fieldName = fnit.next();

            attribute = box.getAttribute(fieldName);
            if (null == attribute) {
                log.trace("No attribute '{}'.", fieldName);

                while (fnit.hasNext()) {
                    fieldName = fnit.next();
                    log.debug(" ... trying '{}'.", fieldName);

                    attribute = box.getAttribute(fieldName);
                    if (attribute != null) {
                        break;
                    }
                    log.trace("No attribute '{}'.", fieldName);
                }
            }
        }

        if (null == attribute) {
            if (isMandatory) {
                log.info("Mandatory field(s) not present: {}", fieldNames);
            }
            return null;
        }

        ArrayList<?> values = attribute.getValueVector();
        if (values.isEmpty()) {
            log.trace("No values for attribute '{}'.", fieldName);
            if (isMandatory) {
                log.info("Mandatory value(s) for field '{}' not present", fieldName);
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

        ArrayList<Attribute<?>> children = (ArrayList<Attribute<?>>) attribute.getValueVector();
        children.forEach(attr -> {
            attributes.put(attr.getAlias(), attr);  // here we assume alias == field name
        });

        return new /* inner */ RecordBox(/* outer */ box, attribute, attributes);
    }

    public Object getScalar(List<String> fieldNames, Box box) {
        return getScalar(fieldNames, box,false);
    }

    private Collection<Unit.Id> search0(Query.Filter filter) {
        SearchExpression expr = assembleConstraints(filter);

        // Result set constraints (paging)
        SearchOrder order = SearchOrder.orderByUnitId(true); // ascending on unit id
        UnitSearch usd = new UnitSearch(expr, order, filter.offset(), filter.size());

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

                    log.debug("Found: unit=" + _tenantId + "." + _unitId + ":" + _unitVer + " created=" + _created + " modified=" + _modified);
                    ids.add(new Unit.Id(_tenantId, _unitId));
                }
            }));
        } catch (SQLException sqle) {
            log.error(sqle.getMessage(), sqle);
            return List.of();
        }

        return ids;
    }

    public List<Box> search(Query.Filter filter) {
        log.trace("RuntimeService::search");

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
                        units.add(new /* outermost */ Box(unit, attributes));

                    } else {
                        log.error("Unknown unit: {}", id);
                    }
                } catch (Throwable t) {
                    log.error(t.getMessage(), t);
                }
            }
            return units;
        }
    }

    public byte[] searchRaw(Query.Filter filter) {
        log.trace("RuntimeService::searchRaw");

        Collection<Unit.Id> ids = search0(filter);

        List<Unit> units = new ArrayList<>();
        for (Unit.Id id : ids) {
            try {
                Optional<Unit> _unit = repo.getUnit(id.tenantId(), id.unitId());
                if (_unit.isPresent()) {
                    units.add(_unit.get());

                } else {
                    log.error("Unknown unit: {}", id);
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

    private SearchExpression assembleConstraints(Query.Filter filter) {
        // Implicit unit constraints
        int tenantId = filter.tenantId();
        SearchExpression expr = QueryBuilder.constrainToSpecificTenant(tenantId);
        expr = QueryBuilder.assembleAnd(expr, QueryBuilder.constrainToSpecificStatus(Unit.Status.EFFECTIVE));

        // attribute constraints
        return new AndExpression(expr, assembleConstraints(filter.where()));
    }

    private SearchExpression assembleConstraints(Query.Node node) {
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

    private SearchExpression assembleTreeConstraints(Query.TreeExpression treeExpr) {
        Query.Logical op = treeExpr.op();
        Query.Node left = treeExpr.left();
        Query.Node right = treeExpr.right();

        if (Objects.requireNonNull(op) == Query.Logical.AND)
            return QueryBuilder.assembleAnd(assembleConstraints(left), assembleConstraints(right));
        else // Logical.OR
            return QueryBuilder.assembleOr(assembleConstraints(left), assembleConstraints(right));

    }

    private LeafExpression<?> assembleAttributeConstraints(Query.AttributeExpression attrExpr) {
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

        int attrId = catalogAttribute.attrId();
        AttributeType attrType = catalogAttribute.attrType();
        switch (attrType) {
            case STRING -> {
                if (Objects.requireNonNull(op) == Query.FilterOperator.EQ) {
                    value = value.replace('*', '%');
                    boolean useLIKE = value.indexOf('%') >= 0 || value.indexOf('_') >= 0;  // Uses wildcard
                    if (useLIKE) {
                        return new LeafExpression<>(new StringAttributeSearchItem(attrId, Operator.LIKE, value));
                    } else {
                        return new LeafExpression<>(new StringAttributeSearchItem(attrId, Operator.EQ, value));
                    }
                }
                return new LeafExpression<>(new StringAttributeSearchItem(attrId, op.iptoOp(), value));
            }
            case TIME -> {
                return new LeafExpression<>(new TimeAttributeSearchItem(attrId, op.iptoOp(), Instant.parse(value)));
            }
            case INTEGER -> {
                return new LeafExpression<>(new IntegerAttributeSearchItem(attrId, op.iptoOp(), Integer.parseInt(value)));
            }
            case LONG -> {
                return new LeafExpression<>(new LongAttributeSearchItem(attrId, op.iptoOp(), Long.parseLong(value)));
            }
            case DOUBLE -> {
                return new LeafExpression<>(new DoubleAttributeSearchItem(attrId, op.iptoOp(), Double.parseDouble(value)));
            }
            case BOOLEAN -> {
                return new LeafExpression<>(new BooleanAttributeSearchItem(attrId, op.iptoOp(), Boolean.parseBoolean(value)));
            }
            default -> throw new InvalidParameterException("Attribute type " + attrType.name() + " is not searchable: " + attrName);
        }
    }
}
