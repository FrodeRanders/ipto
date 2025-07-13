package org.gautelis.repo.graphql.runtime;

import org.gautelis.repo.model.attributes.Attribute;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class Box {
    private static final Logger log = LoggerFactory.getLogger(Box.class);

    private final int tenantId;
    private final long unitId;

    // attrId in IPTO -> attribute
    private final Map<Integer, Attribute<?>> attributes;

    /* package accessible only */
    Box(int tenantId, long unitId, Map<Integer, Attribute<?>> attributes) {
        this.tenantId = tenantId;
        this.unitId = unitId;
        this.attributes = attributes;
    }

    /* package accessible only */
    Box(Box parent, Map<Integer, Attribute<?>> attributes) {
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
        log.trace("Box::scalar({}, {})", attrId, expectedClass.getName());

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
        log.trace("Box::array({}, {})", attrId, expectedClass.getName());

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
        sb.append("Box{");
        sb.append("tenantId=").append(tenantId);
        sb.append(", unitId=").append(unitId);
        sb.append(", attributes=").append(attributes);
        sb.append("}");
        return sb.toString();
    }
}
