package org.gautelis.repo.graphql.configuration;

import graphql.language.*;

public record TypeDefinition(String typeName, boolean isArray, boolean isMandatory) {

    public static TypeDefinition get(Type t) {
        boolean isArray = false;
        boolean isMandatory = false;
        String name = null;

        //
        if (t instanceof ListType listType) {
            isArray = true;
            if (listType.getType() instanceof NonNullType nonNullType) {
                isMandatory = true;
                name = ((TypeName) nonNullType.getType()).getName();
            } else {
                name = ((TypeName) listType.getType()).getName();
            }
        } else if (t instanceof NonNullType nonNullType) {
            isMandatory = true;
            if (nonNullType.getType() instanceof ListType listType) {
                isArray = true;
                name = ((TypeName) listType.getType()).getName();
            } else {
                name = ((TypeName) nonNullType.getType()).getName();
            }
        } else if (t instanceof TypeName typeName) {
            name = typeName.getName();
        }

        return new TypeDefinition(name, isArray, isMandatory);
    }
}
