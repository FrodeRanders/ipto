package org.gautelis.repo.graphql2.model;

import java.util.List;

/*
 * type Shipment @record(attribute: SHIPMENT) {
 *    shipmentId : String    @use(attribute: SHIPMENT_ID)  <-- a FIELD in this record
 *
 * type Shipment @record(attribute: SHIPMENT) {
 *        ^                           ^
 *        | (b)                       | (b)
 *
 * Details about individual fields are found in GqlFieldShape
 */
public record GqlRecordShape(
    String typeName,      // (a)
    String attributeName,  // (b)
    List<GqlFieldShape>fields
) {
    @Override
    public String toString() {
        String info = "GqlRecordShape{";
        info += "type-name='" + typeName + '\'';
        info += ", attribute-name=" + attributeName;
        info += ", fields=[";
        for (GqlFieldShape field : fields) {
            info += field + ", ";
        }
        info += "]}";
        return info;
    }
}
