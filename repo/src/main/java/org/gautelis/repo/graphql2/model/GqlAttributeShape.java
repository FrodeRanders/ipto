package org.gautelis.repo.graphql2.model;

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
public class GqlAttributeShape {
    public final String attributeName;      /* (a) GraphQL specific */
    public final int attributeId;           /* (b) Ipto specific, but shared */
    public final String attributeTypeName;  /* (c) GraphQL and Ipto shared */
    public final boolean isArray;           /* (d) GraphQL and Ipto shared */

    public GqlAttributeShape(String attributeName, int attributeId, String attributeTypeName, boolean isArray) {
        this.attributeName = attributeName;
        this.attributeId = attributeId;
        this.attributeTypeName = attributeTypeName;
        this.isArray = isArray;
    }

    @Override
    public String toString() {
        String info = "GqlAttributeShape{";
        if (null != attributeName) {
            info += "attribute-name='" + attributeName + "', ";
        }
        info += "attribute-id=" + attributeId;
        info += ", is-array=" + isArray;
        info += ", attribute-type-name='" + attributeTypeName + '\'';
        info += '}';
        return info;
    }

    public boolean equals(GqlAttributeShape other) {
        return attributeId == other.attributeId
                && attributeTypeName.equals(other.attributeTypeName)
                && isArray == other.isArray;
    }
}
