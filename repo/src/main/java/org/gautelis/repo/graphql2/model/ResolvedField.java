package org.gautelis.repo.graphql2.model;

import org.gautelis.repo.model.AttributeType;
import org.gautelis.repo.model.attributes.Attribute;
import org.gautelis.repo.model.attributes.Cardinality;

import java.util.EnumSet;

public record ResolvedField(
        String typeName,
        String fieldName,
        Attribute.Reference attribute,  // final chosen attribute binding
        AttributeType attrType,
        Cardinality cardinality,
        boolean isRecord,               // true if it is a record field
        String recordTypeName,          // when isRecord=true
        EnumSet<Source> provenance      // which sources contributed (for diff/diagnostics)
) {}
