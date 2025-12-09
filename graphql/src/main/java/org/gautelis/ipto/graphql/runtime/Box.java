package org.gautelis.ipto.graphql.runtime;

import org.gautelis.ipto.repo.model.Unit;
import org.gautelis.ipto.repo.model.attributes.Attribute;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public abstract class Box {
    private static final Logger log = LoggerFactory.getLogger(Box.class);

    protected final Unit unit;

    protected Box(Unit unit) {
        Objects.requireNonNull(unit, "unit");
        this.unit = unit;
    }

    /* package accessible only */
    Box(Box parent) {
        this(Objects.requireNonNull(parent).getUnit());
    }

    public Unit getUnit() { return unit; }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("unit=").append(unit.getReference());
        return sb.toString();
    }
}
