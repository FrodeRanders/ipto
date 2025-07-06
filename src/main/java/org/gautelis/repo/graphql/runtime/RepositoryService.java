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


public class RepositoryService {
    private static final Logger log = LoggerFactory.getLogger(RepositoryService.class);

    private final Repository repo;

    private final Map<String, Configurator.ExistingDatatypeMeta> datatypes;
    private final Map<Integer, Configurator.ProposedAttributeMeta> attributesIptoView;

    public RepositoryService(
            Repository repo,
            Map<String, Configurator.ExistingDatatypeMeta> datatypes,
            Map<Integer, Configurator.ProposedAttributeMeta> attributesIptoView
    ) {
        this.repo = repo;
        this.datatypes = datatypes; // empty at the moment
        this.attributesIptoView = attributesIptoView; // empty at the moment
    }

    public Snapshot loadUnit(int tenantId, long unitId) {
        log.trace("RepositoryService::loadUnit({}, {})", tenantId, unitId);

        Optional<Unit> unit = repo.getUnit(tenantId, unitId);
        if (unit.isEmpty()) {
            return null;
        }

        Map<Integer, Attribute<?>> attributes = new HashMap<>();

        unit.get().getAttributes().forEach(attr -> {
            int attrId = attr.getAttrId();
            Configurator.ProposedAttributeMeta attributeMeta = attributesIptoView.get(attrId);

            log.debug("Adding attribute {} ({}) of type {}", attributeMeta.nameInSchema(), attr.getName(), attr.getType());
            attributes.put(attributeMeta.attrId(), attr);
        });

        return new Snapshot(tenantId, unitId, attributes);
    }

    public Object getArray(Snapshot snap, int attrId, boolean isMandatory) {
        log.trace("RepositoryService::getArray({}, {}, {})", snap, attrId, isMandatory);

        Attribute<?> attribute = snap.attributes.getOrDefault(attrId, null);
        if (null == attribute) {
            if (isMandatory) {
                log.info("Mandatory attribute {} not present", attrId);
            }
            return null;
        }

        return attribute.getValue();
    }

    public Object getArray(Snapshot snap, int attrId) {
        return getArray(snap, attrId, false);
    }

    public Object getScalar(Snapshot snap, int attrId, boolean isMandatory) {
        log.trace("RepositoryService::getScalar({}, {}, {})", snap, attrId, isMandatory);

        Attribute<?> attribute = snap.attributes.getOrDefault(attrId, null);
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

        if (!Type.COMPOUND.equals(attribute.getType())) {
            return values.getFirst();
        }

        Map<Integer, Attribute<?>> attributeMap = new HashMap<>();

        ArrayList<Attribute<?>> children = (ArrayList<Attribute<?>>) attribute.getValue();
        children.forEach(attr -> {
            int childAttrId = attr.getAttrId();
            Configurator.ProposedAttributeMeta attributeMeta = attributesIptoView.get(childAttrId);

            log.debug("Adding attribute {} ({}) of type {}", attributeMeta.nameInSchema(), attr.getName(), attr.getType());
            attributeMap.put(attributeMeta.attrId(), attr);
        });

        return new Snapshot(snap.getTenantId(),  snap.getUnitId(), attributeMap);
    }

    public Object getScalar(Snapshot snap, int attrId) {
        return getScalar(snap, attrId, false);
    }

    public Object getRecord(Snapshot parent, int compoundAttrIde, int idx) {
        log.warn("RepositoryService::getRecord({}, {}, {})", parent, compoundAttrIde, idx);
        boolean isMandatory = false; // TODO and for now

        Attribute<?> attribute = parent.attributes.getOrDefault(compoundAttrIde, null);
        if (null == attribute) {
            if (isMandatory) {
                log.info("Mandatory attribute {} not present", compoundAttrIde);
            }
            return null;
        }

        ArrayList<?> values = attribute.getValue();
        if (values.isEmpty()) {
            if (isMandatory) {
                log.info("Mandatory value(s) for attribute {} not present", compoundAttrIde);
            }
            return null;
        }

        if (!Type.COMPOUND.equals(attribute.getType())) {
            return values.getFirst();
        }

        Map<Integer, Attribute<?>> attributeMap = new HashMap<>();

        ArrayList<Attribute<?>> children = (ArrayList<Attribute<?>>) attribute.getValue();
        children.forEach(attr -> {
            int childAttrId = attr.getAttrId();
            Configurator.ProposedAttributeMeta attributeMeta = attributesIptoView.get(childAttrId);

            log.debug("Adding attribute {} ({}) of type {}", attributeMeta.nameInSchema(), attr.getName(), attr.getType());
            attributeMap.put(attributeMeta.attrId(), attr);
        });

        return new Snapshot(parent.getTenantId(),  parent.getUnitId(), attributeMap);
    }
}
