package org.gautelis.repo.graphql.runtime;

import org.gautelis.repo.model.attributes.Attribute;
import org.gautelis.repo.model.attributes.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Snapshot {
    private static final Logger log = LoggerFactory.getLogger(Snapshot.class);

    final int tenantId;
    final long unitId;

    // attrId in IPTO -> attribute
    final Map<Integer, Attribute<?>> attributes;

    Snapshot(int tenantId, long unitId, Map<Integer, Attribute<?>> attributes) {
        log.trace("Creating Snapshot");
        this.tenantId = tenantId;
        this.unitId = unitId;
        this.attributes = attributes;
    }

    public int getTenantId() {
        return tenantId;
    }

    public long getUnitId() {
        return unitId;
    }

    /* Used by generic DataFetchers */

    /** Null when the attribute is missing or vector is empty. */
    private String scalarString(int attrId) {
        log.trace("Snapshot::scalarString({})", attrId);
        return firstOrNull(attributes.get(attrId), String.class);
    }

    private Instant scalarTime(int attrId) {
        log.trace("Snapshot::scalarTime({})", attrId);
        return firstOrNull(attributes.get(attrId), Instant.class);
    }

    private Integer scalarInt(int attrId) {
        log.trace("Snapshot::scalarInt({})", attrId);
        return firstOrNull(attributes.get(attrId), Integer.class);
    }

    private Long scalarLong(int attrId) {
        log.trace("Snapshot::scalarLong({})", attrId);
        return firstOrNull(attributes.get(attrId), Long.class);
    }

    private Double scalarDouble(int attrId) {
        log.trace("Snapshot::scalarDouble({})", attrId);
        return firstOrNull(attributes.get(attrId), Double.class);
    }

    private List<Double> doubleArray(int attrId) {
        log.trace("Snapshot::doubleVector({})", attrId);
        Attribute<?> attribute = attributes.get(attrId);
        if (null == attribute) {
            return List.of();
        }
        ValueVector<?> values = new ValueVector<>(attribute.getValue());
        return values.isEmpty() ? List.of() : values.stream(Double.class).toList();
    }

    /* Helpers */
    private static <T> T firstOrNull(Attribute<?> attribute, Class<T> type) {
        if (null == attribute) {
            return null;
        }
        ArrayList<?> valueVector = attribute.getValue();
        return valueVector.isEmpty() ? null : type.cast(valueVector.getFirst());
    }

    /** Child snapshot inside a compound (or null). */
    private /* for now */ Object child(int compoundAttrId, int idx) {
        log.trace("Snapshot::child({}, {})", compoundAttrId, idx);
        Attribute<?> attribute = attributes.get(compoundAttrId);
        if (null == attribute) {
            return null;
        }
        if (!Type.COMPOUND.equals(attribute.getType())) {
            return null;
        }

        ArrayList<Attribute<?>> childAttributes = (ArrayList<Attribute<?>>) attribute.getValue();
        if (childAttributes.isEmpty()) {
            return null;
        }
        Map<String, Attribute<?>> children = new HashMap<>();
        for (Attribute<?> child : childAttributes) {
            children.put(child.getName(), child);
        }
        return children;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Snapshot{");
        sb.append("tenantId=").append(tenantId);
        sb.append(", unitId=").append(unitId);
        sb.append(", attributes=").append(attributes);
        sb.append("}");
        return sb.toString();
    }
}
