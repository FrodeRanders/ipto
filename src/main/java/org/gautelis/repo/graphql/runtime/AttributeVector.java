package org.gautelis.repo.graphql.runtime;

import org.gautelis.repo.model.attributes.Attribute;

import java.util.ArrayList;
import java.util.Collections;

public class AttributeVector extends ArrayList<Attribute<?>> {
    public AttributeVector(ArrayList<Attribute<?>> values) {
        super(Collections.unmodifiableList(values));
    }
}