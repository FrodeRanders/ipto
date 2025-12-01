package org.gautelis.repo.graphql.model;

import graphql.language.ObjectTypeDefinition;
import graphql.language.Type;

import java.util.List;

/*
 * type Shipment @record(attribute: shipment) {
 *    shipmentId : String    @use(attribute: shipmentId)  <-- a FIELD in this record
 *
 * type Shipment @record(attribute: shipment) {
 *        ^                           ^
 *        | (a)                       | (b)
 *
 * Details about individual fields are found in GqlFieldShape
 */
public record GqlRecordShape(
        String typeName,           // (a)
        List<Type> objectTypes,
        String attributeEnumName,  // (b)
        String attributeName,
        List<GqlFieldShape> fields
) {
    public boolean equals(CatalogRecord other) {
        return typeName.equals(other.recordName);
    }

    @Override
    public String toString() {
        String info = "GqlRecordShape{";
        info += "record-name='" + typeName + '\'';
        info += ", attribute-enum-name='" + attributeEnumName + '\'';
        info += ", attribute-name='" + attributeName + '\'';
        info += ", fields=[";
        for (GqlFieldShape field : fields) {
            info += field + ", ";
        }
        info += "]}";
        return info;
    }
}
