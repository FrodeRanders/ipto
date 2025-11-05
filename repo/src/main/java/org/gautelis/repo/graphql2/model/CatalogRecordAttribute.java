package org.gautelis.repo.graphql2.model;

import org.gautelis.repo.model.AttributeType;

public record CatalogRecordAttribute(
        int    attrId,
        String attrName,          // canonical registry name
        AttributeType attrType,   // your enum
        boolean isVector,
        boolean isRecord,         // true for compound/record attributes
        String  recordName    // if isRecord, which record it represents
) {
    @Override
    public String toString() {
        String info = "CatalogAttribute{";
        info += "attribute-id=" + attrId;
        info += ", attribute-name='" + attrName + '\'';
        info += ", attribute-type=" + attrType.name();
        info += ", is-vector=" + isVector;
        info += ", is-record=" + isRecord;
        if (isRecord) {
            info += ", record-name='" + recordName + '\'';
        }
        info += "}";
        return info;
    }
}
