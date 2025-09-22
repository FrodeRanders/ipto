package org.gautelis.repo.graphql2.model.external;

import org.gautelis.repo.graphql2.model.AttributeDef;/*
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
public class ExternalAttributeDef extends AttributeDef {
    public final String attributeName;       /* (a) GraphQL specific */

    public ExternalAttributeDef(String attributeName, int attributeId, String attributeTypeName, boolean isArray) {
        super(attributeId, attributeTypeName, isArray);
        this.attributeName = attributeName;
    }

    @Override
    public String toString() {
        String info = "ExternalAttributeDef{";
        if (null != attributeName) {
            info += "attribute-name='" + attributeName + "', ";
        }
        info += super.toString();
        info += '}';
        return info;
    }

    /*
    public boolean compare(AttributeDef other) {
        return attributeName.equals(other.attributeName)
                && attributeId == other.attributeId
                && attributeTypeName.equals(other.attributeTypeName)
                && attributeTypeId == other.attributeTypeId
                && isArray == other.isArray
                && alias.equals(other.alias)
                && qualifiedName.equals(other.qualifiedName);
    }
    */
}
