package org.gautelis.repo.graphql2.model.internal;

import org.gautelis.repo.graphql2.model.AttributeDef;

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
public class InternalAttributeDef extends AttributeDef {
    public final int attributeTypeId;       /* Ipto specific */
    public final String alias;              /* (e) Ipto specific */
    public final String qualifiedName;      /* (f) Ipto specific */
    public final String description;        /* (g) Ipto specific */

    public InternalAttributeDef(
            int attributeId,
            String attributeTypeName,
            int attributeTypeId,
            boolean isArray,
            String alias,
            String qualifiedName,
            String description
    ) {
        super(attributeId, attributeTypeName, isArray);
        this.attributeTypeId = attributeTypeId;
        this.alias = alias;
        this.qualifiedName = qualifiedName;
        this.description = description;
    }

    @Override
    public String toString() {
        String info = "InternalAttributeDef{";
        info += super.toString();
        info += ", attribute-type-id=" + attributeTypeId;
        info += ", alias=";
        if (null != alias) {
            info += '\'' + alias + '\'';
        }
        info += ", qualified-name=";
        if (null != qualifiedName) {
            info += '\'' + qualifiedName + '\'';
        }
        info += ", description=";
        if (null != description) {
            info += '\'' + description + '\'';
        }
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
