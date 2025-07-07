package org.gautelis.repo.graphql.runtime;

import org.gautelis.repo.model.attributes.Attribute;
import org.gautelis.repo.model.attributes.AttributeValueRunnable;
import org.gautelis.repo.model.attributes.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;

public class Snapshot {
    private static final Logger log = LoggerFactory.getLogger(Snapshot.class);

    private final int tenantId;
    private final long unitId;

    // attrId in IPTO -> attribute
    private final Map<Integer, Attribute<?>> attributes;

    /* package accessible only */
    Snapshot(int tenantId, long unitId, Map<Integer, Attribute<?>> attributes) {
        log.trace("Creating Snapshot");
        this.tenantId = tenantId;
        this.unitId = unitId;
        this.attributes = attributes;
    }

    /* package accessible only */
    Snapshot(Snapshot parent, Map<Integer, Attribute<?>> attributes) {
        this(Objects.requireNonNull(parent).tenantId, parent.unitId, attributes);
    }

    public int getTenantId() {
        return tenantId;
    }

    public long getUnitId() {
        return unitId;
    }

    /* package accessible only */
    Attribute<?> getAttribute(int attrId) {
        return attributes.get(attrId);
    }

    public <A> A scalar(int attrId, Class<A> expectedClass) {
        log.trace("Snapshot::scalar({}, {})", attrId, expectedClass.getName());

        Attribute<?> attribute = attributes.get(attrId);
        if (null == attribute) {
            return null;
        }

        ArrayList<?> values = attribute.getValue();
        if (values.isEmpty()) {
            return null;
        }

        return expectedClass.cast(values.getFirst());
    }

    public <A> List<A> array(int attrId, Class<A> expectedClass) {
        log.trace("Snapshot::array({}, {})", attrId, expectedClass.getName());

        Attribute<?> attribute = attributes.get(attrId);
        if (null == attribute) {
            return List.of();
        }

        ArrayList<?> values = attribute.getValue();
        if (values.isEmpty()) {
            return List.of();
        }

        return values.stream().map(expectedClass::cast).toList();
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
