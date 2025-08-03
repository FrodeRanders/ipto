package org.gautelis.repo.graphql.runtime;

import org.gautelis.repo.exceptions.InvalidParameterException;
import org.gautelis.repo.graphql.configuration.Configurator;
import org.gautelis.repo.graphql.configuration.OperationsConfigurator;
import org.gautelis.repo.model.KnownAttributes;
import org.gautelis.repo.model.Repository;
import org.gautelis.repo.model.Unit;
import org.gautelis.repo.model.attributes.Attribute;
import org.gautelis.repo.model.attributes.Type;
import org.gautelis.repo.search.UnitSearch;
import org.gautelis.repo.search.model.*;
import org.gautelis.repo.search.query.*;
import org.gautelis.repo.utils.TimeHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;


public class RuntimeService {
    private static final Logger log = LoggerFactory.getLogger(RuntimeService.class);

    private final Repository repo;

    private final Map<String, Configurator.ExistingDatatypeMeta> datatypes;
    private final Map<Integer, Configurator.ProposedAttributeMeta> attributesIptoView;

    public RuntimeService(
            Repository repo,
            Map<String, Configurator.ExistingDatatypeMeta> datatypes,
            Map<Integer, Configurator.ProposedAttributeMeta> attributesIptoView
    ) {
        this.repo = repo;
        this.datatypes = datatypes; // empty at the moment
        this.attributesIptoView = attributesIptoView; // empty at the moment
    }

    public Box loadUnit(int tenantId, long unitId) {
        log.trace("RuntimeService::loadUnit({}, {})", tenantId, unitId);

        Optional<Unit> unit = repo.getUnit(tenantId, unitId);
        if (unit.isEmpty()) {
            return null;
        }

        Map<Integer, Attribute<?>> attributes = new HashMap<>();

        unit.get().getAttributes().forEach(attr -> {
            int attrId = attr.getAttrId();
            Configurator.ProposedAttributeMeta attributeMeta = attributesIptoView.get(attrId);

            log.trace("Adding attribute {} ({}) of type {}", attributeMeta.nameInSchema(), attr.getName(), attr.getType());
            attributes.put(attributeMeta.attrId(), attr);
        });

        return new /* outermost */ Box(tenantId, unitId, attributes);
    }

    public byte[] loadRawUnit(int tenantId, long unitId) {
        log.trace("RuntimeService::loadRawUnit({}, {})", tenantId, unitId);

        Optional<Unit> unit = repo.getUnit(tenantId, unitId);
        if (unit.isEmpty()) {
            return null;
        }

        String json = unit.get().asJson(/* complete? */ true, /* pretty? */ false, /* flat? */ false);
        return json.getBytes(StandardCharsets.UTF_8);
    }

    public Object getArray(Box box, int attrId, boolean isMandatory) {
        log.trace("RuntimeService::getArray({}, {}, {})", box, attrId, isMandatory);

        Attribute<?> attribute = box.getAttribute(attrId);
        if (null == attribute) {
            if (isMandatory) {
                log.info("Mandatory attribute {} not present", attrId);
            }
            return null;
        }

        ArrayList<?> values = attribute.getValue();
        if (values.isEmpty()) {
            if (isMandatory) {
                log.info("Mandatory value(s) for attribute {} not present", attrId);
            }
            return null;
        }

        if (!Type.RECORD.equals(attribute.getType())) {
            return values;
        }

        Map<Integer, Attribute<?>> attributeMap = new HashMap<>();

        ArrayList<Attribute<?>> children = (ArrayList<Attribute<?>>) attribute.getValue();
        children.forEach(attr -> {
            int childAttrId = attr.getAttrId();
            Configurator.ProposedAttributeMeta attributeMeta = attributesIptoView.get(childAttrId);

            log.trace("Adding attribute {} ({}) of type {}", attributeMeta.nameInSchema(), attr.getName(), attr.getType());
            attributeMap.put(attributeMeta.attrId(), attr);
        });

        return new /* inner */ Box(/* outer */ box, attributeMap);
    }

    public Object getArray(Box box, int attrId) {
        return getArray(box, attrId, false);
    }

    public Object getScalar(Box box, int attrId, boolean isMandatory) {
        log.trace("RuntimeService::getScalar({}, {}, {})", box, attrId, isMandatory);

        Attribute<?> attribute = box.getAttribute(attrId);
        if (null == attribute) {
            if (isMandatory) {
                log.info("Mandatory attribute {} not present", attrId);
            }
            return null;
        }

        ArrayList<?> values = attribute.getValue();
        if (values.isEmpty()) {
            if (isMandatory) {
                log.info("Mandatory value(s) for attribute {} not present", attrId);
            }
            return null;
        }

        if (!Type.RECORD.equals(attribute.getType())) {
            return values.getFirst();
        }

        Map<Integer, Attribute<?>> attributeMap = new HashMap<>();

        ArrayList<Attribute<?>> children = (ArrayList<Attribute<?>>) attribute.getValue();
        children.forEach(attr -> {
            int childAttrId = attr.getAttrId();
            Configurator.ProposedAttributeMeta attributeMeta = attributesIptoView.get(childAttrId);

            log.trace("Adding attribute {} ({}) of type {}", attributeMeta.nameInSchema(), attr.getName(), attr.getType());
            attributeMap.put(attributeMeta.attrId(), attr);
        });

        return new /* inner */ Box(/* outer */ box, attributeMap);
    }

    public Object getScalar(Box box, int attrId) {
        return getScalar(box, attrId,false);
    }

    public List<Box> search(OperationsConfigurator.Filter filter) {
        log.trace("RuntimeService::search");

        int tenantId = filter.tenantId();

        SearchExpression expr = assembleConstraints(filter);

        Map<Integer, Attribute<?>> attributes = new HashMap<>();

        // Result set constraints (paging)
        SearchOrder order = SearchOrder.orderByUnitId(true); // ascending on unit id
        UnitSearch usd = new UnitSearch(expr, order, filter.offset(), filter.size());

        // Build SQL statement for search
        DatabaseAdapter searchAdapter = repo.getDatabaseAdapter();

        Collection<Unit.Id> unitId = new ArrayList<>();
        try {
            repo.withConnection(conn -> searchAdapter.search(conn, usd, repo.getTimingData(), rs -> {
                while (rs.next()) {
                    int j = 0;
                    int _tenantId = rs.getInt(++j);
                    long _unitId = rs.getLong(++j);
                    Timestamp _created = rs.getTimestamp(++j);

                    log.debug("Found: tenantId=" + _tenantId + " unitId=" + _unitId + " created=" + _created);
                    unitId.add(new Unit.Id(_tenantId, _unitId));
                }
            }));
        } catch (SQLException sqle) {
            log.error(sqle.getMessage(), sqle);
            return List.of();
        }

        if (unitId.isEmpty()) {
            return List.of();
        } else {
            List<Box> units = new ArrayList<>();
            for (Unit.Id id : unitId) {
                try {
                    Optional<Unit> _unit = repo.getUnit(id.tenantId(), id.unitId());
                    if (_unit.isPresent()) {
                        Unit unit = _unit.get();
                        for (Attribute<?> attr : unit.getAttributes()) {
                            int attrId = attr.getAttrId();
                            Configurator.ProposedAttributeMeta attributeMeta = attributesIptoView.get(attrId);
                            attributes.put(attributeMeta.attrId(), attr);
                        }

                        units.add(new /* outermost */ Box(unit.getTenantId(), unit.getUnitId(), attributes));
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

    /****************** Search related ******************/

    private SearchExpression assembleConstraints(OperationsConfigurator.Filter filter) {
        // Implicit unit constraints
        int tenantId = filter.tenantId();
        SearchExpression expr = QueryBuilder.constrainToSpecificTenant(tenantId);
        expr = QueryBuilder.assembleAnd(expr, QueryBuilder.constrainToSpecificStatus(Unit.Status.EFFECTIVE));

        // attribute constraints
        return new AndExpression(expr, assembleConstraints(filter.where()));
    }

    private SearchExpression assembleConstraints(OperationsConfigurator.Node node) {
        OperationsConfigurator.AttributeExpression attrExpr = node.attrExpr();
        OperationsConfigurator.TreeExpression treeExpr = node.treeExpr();

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

    private SearchExpression assembleTreeConstraints(OperationsConfigurator.TreeExpression treeExpr) {
        OperationsConfigurator.Logical op = treeExpr.op();
        OperationsConfigurator.Node left = treeExpr.left();
        OperationsConfigurator.Node right = treeExpr.right();

        if (Objects.requireNonNull(op) == OperationsConfigurator.Logical.AND)
            return QueryBuilder.assembleAnd(assembleConstraints(left), assembleConstraints(right));
        else // Logical.OR
            return QueryBuilder.assembleOr(assembleConstraints(left), assembleConstraints(right));

    }

    private LeafExpression<?> assembleAttributeConstraints(OperationsConfigurator.AttributeExpression attrExpr) {
        String attrName = attrExpr.attr();
        OperationsConfigurator.Operator op = attrExpr.op();
        String value = attrExpr.value();

        Optional<KnownAttributes.AttributeInfo> _info = repo.getAttributeInfo(attrName);
        if (_info.isEmpty()) {
            throw new InvalidParameterException("Unknown attribute " + attrName);
        }
        KnownAttributes.AttributeInfo info = _info.get();

        int attrId = info.id;
        Type attrType = Type.of(info.type);
        switch (attrType) {
            case STRING -> {
                if (Objects.requireNonNull(op) == OperationsConfigurator.Operator.EQ) {
                    value = value.replace('*', '%');
                    boolean useLIKE = value.indexOf('%') >= 0 || value.indexOf('_') >= 0;  // Uses wildcard
                    if (useLIKE) {
                        return new LeafExpression<>(new StringAttributeSearchItem(attrId, org.gautelis.repo.search.model.Operator.LIKE, value));
                    } else {
                        return new LeafExpression<>(new StringAttributeSearchItem(attrId, org.gautelis.repo.search.model.Operator.EQ, value));
                    }
                }
                return new LeafExpression<>(new StringAttributeSearchItem(attrId, op.iptoOp(), value));
            }
            case TIME -> {
                return new LeafExpression<>(new TimeAttributeSearchItem(attrId, op.iptoOp(), TimeHelper.parseInstant(value)));
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
