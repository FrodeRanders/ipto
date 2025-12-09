package org.gautelis.ipto.graphql.runtime;

import org.gautelis.ipto.repo.model.Unit;
import org.gautelis.ipto.repo.model.attributes.Attribute;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class UnitBox extends AttributeBox {
    private static final Logger log = LoggerFactory.getLogger(UnitBox.class);

    /* package accessible only */
    UnitBox(Unit unit, Map</* field name */ String, Attribute<?>> attributes) {
        super(unit, attributes);

        log.trace("\u2193 Created {}", this);
    }

    /* package accessible only */
    UnitBox(Box parent, Unit unit, Map</* field name */ String, Attribute<?>> attributes) {
        this(Objects.requireNonNull(parent).getUnit(), attributes);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("UnitBox{");
        sb.append(super.toString());
        sb.append("}");
        return sb.toString();
    }
}
