package org.gautelis.repo.graphql.model;

/*
 * enum Attributes @attributeRegistry {
 *     "The name given to the resource. It''s a human-readable identifier that provides a concise representation of the resource''s content."
 *     title @attribute(id: 1, datatype: STRING, array: false, name: "dc:title", uri: "http://purl.org/dc/elements/1.1/title", description: "Namnet som ges till resursen...")
 *     ...
 *     shipmentId  @attribute(id: 1004, datatype: STRING)
 *     shipment    @attribute(id: 1099, datatype: RECORD, array: false)
 * }
 *
 * title @attribute(id: 1, datatype: STRING, array: false, name: "dc:title", qualname: "http:...", description: "...")
 *     ^                  ^              ^              ^               ^                    ^                       ^
 *     | (a)              | (b)          | (c)          | (d)           | (e)                | (f)                   | (g)
 */
public class GqlAttributeShape {
    public final String alias;       // (a)
    public final int attrId;         // (b)
    public final String typeName;    // (c)
    public final boolean isArray;    // (d)
    public final String name;        // (e)
    public final String qualName;    // (f)
    public final String description; // (g)

    public GqlAttributeShape(
            String alias,
            int attrId,
            String typeName,
            boolean isArray,
            String name,
            String qualName,
            String description
    ) {
        this.alias = alias;
        this.attrId = attrId;
        this.typeName = typeName;
        this.isArray = isArray;
        this.name = name;
        this.qualName = qualName;
        this.description = description;
    }

    public boolean equals(CatalogAttribute other) {
        return attrId == other.attrId()
                && alias.equals(other.alias())
                && name.equals(other.attrName())
                && qualName.equals(other.qualifiedName())
                && typeName.equals(other.attrType().name())
                && isArray == other.isArray();
    }

    @Override
    public String toString() {
        String info = "GqlAttributeShape{";
        info += "alias=";
        if (null != alias) {
            info += '\'' + alias + '\'';
        }
        info += ", attribute-id=" + attrId;
        info += ", attribute-type='" + typeName + '\'';
        info += ", attribute-name=";
        if (null != name) {
            info += '\'' + name + '\'';
        }
        info += ", attribute-qname=";
        if (null != qualName) {
            info += '\'' + qualName + '\'';
        }
        info += ", is-array=" + isArray;
        info += ", description=...";
        info += '}';
        return info;
    }
}
