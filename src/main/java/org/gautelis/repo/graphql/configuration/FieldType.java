package org.gautelis.repo.graphql.configuration;

import graphql.language.FieldDefinition;
import graphql.language.ListType;
import graphql.language.NonNullType;
import graphql.language.TypeName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public record FieldType(String name, boolean isArray, boolean isMandatory) {
    private static final Logger log = LoggerFactory.getLogger(FieldType.class);

    public static FieldType get(FieldDefinition f) {
        final String fieldName = f.getName();

        //
        boolean isArray = false;
        boolean isMandatory = false;
        String fieldTypeName = null;

        //
        if (f.getType() instanceof ListType listType) {
            isArray = true;
            if (listType.getType() instanceof NonNullType nonNullType) {
                isMandatory = true;
                fieldTypeName = ((TypeName) nonNullType.getType()).getName();
                log.trace("Signature: {} [{}!]", fieldName, fieldTypeName);
            } else {
                fieldTypeName = ((TypeName) listType.getType()).getName();
                log.trace("Signature: {} [{}]", fieldName, fieldTypeName);
            }
        } else if (f.getType() instanceof NonNullType nonNullType) {
            isMandatory = true;
            if (nonNullType.getType() instanceof ListType listType) {
                isArray = true;
                fieldTypeName = ((TypeName) listType.getType()).getName();
                log.trace("Signature: {} [{}]!", fieldName, fieldTypeName);
            } else {
                fieldTypeName = ((TypeName) nonNullType.getType()).getName();
                log.trace("Signature: {} {}!", fieldName, fieldTypeName);
            }
        } else if (f.getType() instanceof TypeName typeName) {
            fieldTypeName = typeName.getName();
            log.trace("Signature: {} {}", fieldName, fieldTypeName);
        }

        return new FieldType(fieldTypeName, isArray, isMandatory);
    }
}
