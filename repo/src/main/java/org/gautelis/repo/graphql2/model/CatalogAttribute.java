package org.gautelis.repo.graphql2.model;

import org.gautelis.repo.model.AttributeType;

public record CatalogAttribute(
        int    attrId,
        String alias,
        String attrName,
        String qualifiedName,
        AttributeType attrType,
        boolean isArray
) {
    @Override
    public String toString() {
        String info = "CatalogAttribute{";
        info += "attribute-id=" + attrId;
        info += ", alias=";
        if (null != alias) {
            info += '\'' + alias + '\'';
        }
        info += ", attribute-name='" + attrName + '\'';
        info += ", attribute-qname='" + qualifiedName + '\'';
        info += ", attribute-type=" + attrType.name();
        info += ", is-array=" + isArray;
        info += "}";
        return info;
    }
}
