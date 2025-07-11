package org.gautelis.repo.graphql.runtime;

import org.gautelis.repo.graphql.configuration.Configurator;
import org.gautelis.repo.model.Repository;
import org.gautelis.repo.model.Unit;
import org.gautelis.repo.model.attributes.Attribute;
import org.gautelis.repo.model.attributes.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;


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
}
