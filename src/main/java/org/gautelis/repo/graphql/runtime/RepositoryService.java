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

        Map<Integer, ValueVector<?>> values = new HashMap<>();
        Map<Integer, AttributeVector> compoundAttributes = new HashMap<>();

        unit.get().getAttributes().forEach(attr -> {
            int attrId = attr.getAttrId();
            Configurator.ProposedAttributeMeta attributeMeta = attributesIptoView.get(attrId);

            log.debug("Lookup IPTO attribute {} ({}) -> {}", attrId, attr.getName(), attributeMeta.nameInSchema());

            if (attr.getType() == Type.COMPOUND) {
                compoundAttributes.put(attributeMeta.attrId(), new AttributeVector((ArrayList<Attribute<?>>) attr.getValue()));
            } else {
                log.debug("Adding attribute {} ({}) of type {}", attributeMeta.nameInSchema(), attr.getName(), attr.getType());
                values.put(attributeMeta.attrId(), new ValueVector<>(attr.getValue()));
            }
        });

        return new UnitSnapshot(tenantId, unitId, values, compoundAttributes);
    }

    public Object getVector(Snapshot snap, int attrId) {
        log.trace("RepositoryService::getVector({}, {})", snap, attrId);
        return snap.values.getOrDefault(attrId, null);
    }

    public Object getScalar(Snapshot snap, int attrId) {
        log.trace("RepositoryService::getScalar({}, {})", snap, attrId);
        ValueVector<?> vv = snap.values.getOrDefault(attrId, null);
        if (vv == null || vv.isEmpty()) {
            return null;
        } else {
            return vv.getFirst();
        }
    }

    public Snapshot getCompound(Snapshot parent, int compoundAttrIde, int idx) {
        log.warn("RepositoryService::getCompound({}, {}, {})", parent, compoundAttrIde, idx);
        return null; // TODO
    }
}
