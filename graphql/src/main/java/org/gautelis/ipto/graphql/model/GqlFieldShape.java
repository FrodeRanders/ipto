package org.gautelis.ipto.graphql.model;

/*
 * type Shipment @record(attribute: shipment) {
 *    shipmentId : String    @use(attribute: shipmentId)  <-- a FIELD in this record
 *    ...
 * }
 *
 * type Shipment @record(attribute: shipment) {
 *        ^
 *        | (a)
 *
 *    shipmentId  : String  @use(attribute: shipmentId)
 *        ^           ^                         ^
 *        | (b)       | (c)                     | (d)
 */
public record GqlFieldShape(
        String typeName,          // (a)
        String fieldName,         // (b)
        String gqlTypeRef,        // from (c)
        boolean isArray,          // from (c)
        boolean isMandatory,      // from (c)
        String usedAttributeName  // (d)
) {
    @Override
    public String toString() {
        String info = "GqlFieldShape{";
        info += "type-name='" + typeName + '\'';
        info += ", field-name='" + fieldName + '\'';
        info += ", type-ref='" + gqlTypeRef + '\'';
        info += ", used-attribute='" + usedAttributeName + '\'';
        info += ", is-array=" + isArray;
        info += ", is-mandatory=" + isMandatory;
        info += "]}";
        return info;
    }
}
