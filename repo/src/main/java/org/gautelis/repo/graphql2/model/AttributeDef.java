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
        String attributeName,      /* (a) */
        int attributeId,           /* (b) */
        String attributeTypeName,  /* (c) */
        int attributeTypeId,
        boolean isArray,          /* (d) */
        String alias,             /* (e) */
        String qualifiedName,     /* (f) */
        String description        /* (g) */
) implements NodeDef {
    @NotNull
    @Override
    public String toString() {
        String info = "AttributeDef{";
        info += "attribute-name='" + attributeName + '\'';
        info += ", attribute-id=" + attributeId;
        info += ", attribute-type='" + attributeTypeName + '\'';
        info += ", attribute-type-id=" + attributeTypeId;
        info += ", array=" + isArray;
        if (null != alias) {
            info += ", alias='" + alias + '\'';
        }
        if (null != qualifiedName) {
            info += ", qualname='" + qualifiedName + '\'';
        }
        if (null != description) {
            info += ", description='" + description + '\'';
        }
        info += '}';
        return info;
    }
}
