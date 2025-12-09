package org.gautelis.ipto.graphql.model;

import org.gautelis.ipto.repo.model.AttributeType;
import org.gautelis.ipto.repo.model.attributes.Attribute;

public class CatalogAttribute {
    private int attrId = Attribute.INVALID_ATTRID;
    private final String alias;
    private final String attrName;
    private final String qualifiedName;
    private final AttributeType attrType;
    private final boolean isArray;

    public CatalogAttribute(String alias, String attrName, String qualifiedName, AttributeType attrType, boolean isArray) {
        this.alias = alias;
        this.attrName = attrName;
        this.qualifiedName = qualifiedName;
        this.attrType = attrType;
        this.isArray = isArray;
    }

    public void setAttrId(int attrId) {
        this.attrId = attrId;
    }

    public int attrId() {
        return attrId;
    }

    public String alias() {
        return alias;
    }

    public String attrName() {
        return attrName;
    }

    public String qualifiedName() {
        return qualifiedName;
    }

    public AttributeType attrType() {
        return attrType;
    }

    public boolean isArray() {
        return isArray;
    }

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
