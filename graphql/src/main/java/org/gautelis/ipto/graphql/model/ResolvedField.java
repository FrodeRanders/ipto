package org.gautelis.ipto.graphql.model;

import org.gautelis.ipto.repo.model.AttributeType;
import org.gautelis.ipto.repo.model.attributes.Attribute;
import org.gautelis.ipto.repo.model.attributes.Cardinality;

import java.util.EnumSet;

public record ResolvedField(
        String typeName,
        String fieldName,
        Attribute.Reference attribute,  // final chosen attribute binding
        AttributeType attrType,
        Cardinality cardinality,
        EnumSet<Source> provenance      // which sources contributed (for diff/diagnostics)
) {}
