package org.gautelis.repo.graphql.runtime;

import org.gautelis.repo.model.Unit;
import org.gautelis.repo.model.attributes.Attribute;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class Box {
    private static final Logger log = LoggerFactory.getLogger(Box.class);

    protected final Unit unit;

    //---------------------------------------------------------------------
    // OBSERVE
    //    We are assuming that the field names used in the SDL equals the
    //    attribute aliases used.
    //    GraphQL operates on the information in the SDL, so when assembling
    //    results (of queries and so on), it maps Box' contents for units
    //    and records to fields as described in the SDL.
    //---------------------------------------------------------------------
    private final Map</* field name */ String, Attribute<?>> attributesByFieldName;

    protected Box(Unit unit, Map</* field name */ String, Attribute<?>> attributes) {
        Objects.requireNonNull(unit, "unit");
        this.unit = unit;
        this.attributesByFieldName = attributes;
    }

    /* package accessible only */
    Box(Box parent, Map</* field name */ String, Attribute<?>> attributes) {
        this(Objects.requireNonNull(parent).getUnit(), attributes);
    }

    public Unit getUnit() { return unit; }

    /* package accessible only */
    Attribute<?> getAttribute(String fieldName) {
        return attributesByFieldName.get(fieldName);
    }

    public <A> A scalar(String alias, Class<A> expectedClass) {
        log.trace("Box::scalar({}, {})", alias, expectedClass.getName());

        Attribute<?> attribute = attributesByFieldName.get(alias);
        if (null == attribute) {
            log.debug("No attribute found for alias '{}'", alias);
            return null;
        }

        ArrayList<?> values = attribute.getValueVector();
        if (values.isEmpty()) {
            log.debug("No values found for attribute with alias '{}'", alias);
            return null;
        }

        return expectedClass.cast(values.getFirst());
    }

    public <A> List<A> array(String alias, Class<A> expectedClass) {
        log.trace("Box::array({}, {})", alias, expectedClass.getName());

        Attribute<?> attribute = attributesByFieldName.get(alias);
        if (null == attribute) {
            log.debug("No attribute found for alias '{}'", alias);
            return List.of();
        }

        ArrayList<?> values = attribute.getValueVector();
        if (values.isEmpty()) {
            log.debug("No values found for attribute with alias '{}'", alias);
            return List.of();
        }

        return values.stream().map(expectedClass::cast).toList();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("unit=").append(unit.getReference());
        sb.append(", attributes=[");
        for (Map.Entry<String, Attribute<?>> entry : attributesByFieldName.entrySet()) {
            Attribute<?> attribute = entry.getValue();
            sb.append(attribute).append(" ");
        }
        sb.append("]}");
        return sb.toString();
    }
}
