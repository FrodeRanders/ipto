package org.gautelis.repo.graphql2.model;

/*
 * type Shipment @record(attribute: SHIPMENT) {
 *    shipmentId : String    @use(attribute: SHIPMENT_ID)  <-- a FIELD in this record
 *    ...
 * }
 *
 * type Shipment @record(attribute: SHIPMENT) {
 *        ^
 *        | (a)
 *
 *    shipmentId  : String  @use(attribute: SHIPMENT_ID)
 *        ^           ^                         ^
 *        | (b)       | (c)                     | (d)
 */
public record GqlFieldShape(
        String typeName,          // (a)
        String fieldName,         // (b)
        String gqlTypeRef,        // from (c)
        boolean isArray,          // from (c)
        boolean isNonNull,        // from (c)
        String usedAttributeName  // (d)
) {}
