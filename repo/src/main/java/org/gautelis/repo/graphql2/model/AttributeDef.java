package org.gautelis.repo.graphql2.model;

import org.jetbrains.annotations.NotNull;

/*
 * enum Attributes @attributeRegistry {
 *     "The name given to the resource. It''s a human-readable identifier that provides a concise representation of the resource''s content."
 *     TITLE @attribute(id: 1, datatype: STRING, array: false, alias: "dc:title", uri: "http://purl.org/dc/elements/1.1/title", description: "Namnet som ges till resursen...")
 *     ...
 *     SHIPMENT_ID @attribute(id: 1004, datatype: STRING)
 *     SHIPMENT    @attribute(id: 1099, datatype: RECORD, array: false)
 * }
 *
 * TITLE @attribute(id: 1, datatype: STRING, array: false, alias: "dc:title", qualname: "http:...", description: "...")
 *   ^                  ^              ^              ^               ^                    ^                       ^
 *   | (a)              | (b)          | (c)          | (d)           | (e)                | (f)                   | (g)
 */
public record AttributeDef(
        String attributeName,      /* (a) GraphQL specific */
        int attributeId,           /* (b) Ipto specific */
        String attributeTypeName,  /* (c) GraphQL and Ipto shared */
        int attributeTypeId,       /* Ipto specific */
        boolean isArray,           /* (d) GraphQL and Ipto shared */
        String alias,              /* (e) Ipto specific */
        String qualifiedName,      /* (f) Ipto specific */
        String description         /* (g) Ipto specific */
) implements NodeDef {
    @NotNull
    @Override
    public String toString() {
        String info = "AttributeDef{";
        if (null != attributeName) {
            info += "graphql-attribute-name='" + attributeName + "', ";
        }
        info += "ipto-attribute-id=" + attributeId;
        info += ", graphql-attribute-type-name='" + attributeTypeName + '\'';
        info += ", ipto-attribute-type-id=" + attributeTypeId;
        info += ", array=" + isArray;
        if (null != alias) {
            info += ", ipto-alias='" + alias + '\'';
        }
        if (null != qualifiedName) {
            info += ", ipto-qualified-name='" + qualifiedName + '\'';
        }
        if (null != description) {
            info += ", ipto-description='" + description + '\'';
        }
        info += '}';
        return info;
    }

    public boolean compare(@NotNull AttributeDef other) {

//        String attributeName,      /* (a) GraphQL specific */
//        int attributeId,           /* (b) Ipto specific */
//        String attributeTypeName,  /* (c) GraphQL and Ipto shared */
//        int attributeTypeId,       /* Ipto specific */
//        boolean isArray,           /* (d) GraphQL and Ipto shared */
//        String alias,              /* (e) Ipto specific */
//        String qualifiedName,      /* (f) Ipto specific */
//        String description         /* (g) Ipto specific */

        return attributeName.equals(other.attributeName)
                && attributeId == other.attributeId
                && attributeTypeName.equals(other.attributeTypeName)
                && attributeTypeId == other.attributeTypeId
                && isArray == other.isArray
                && alias.equals(other.alias)
                && qualifiedName.equals(other.qualifiedName);
    }
}
