package org.gautelis.repo.graphql2.model;

import org.jetbrains.annotations.NotNull;

/*
 * type Shipment @record(attribute: SHIPMENT) {
 *    shipmentId  : String  @use(attribute: SHIPMENT_ID)
 *    deadline : DateTime   @use(attribute: DEADLINE)
 *    reading  : [Float]    @use(attribute: READING)
 * }
 *
 * shipmentId  : String  @use(attribute: SHIPMENT_ID)
 *     ^           ^                         ^
 *     | (c)       | (d)                     | (e)
 *
 *  ---or---
 *
 * type PurchaseOrder @unit(id: 42) {
 *    orderId  : String    @use(attribute: ORDER_ID)
 *    shipment : Shipment! @use(attribute: SHIPMENT)
 * }
 *
 *    orderId  : String    @use(attribute: ORDER_ID)
 *     ^           ^                         ^
 *     | (c)       | (d)                     | (e)
 */
public record TypeFieldDef(
        String name,          /* (c) GraphQL specific */
        TypeDef typeDef,      /* (d) GraphQL specific */
        String attributeName, /* (e) referenced by name, Ipto specific */
        int attributeId       /* Ipto specific */
) {
    @NotNull
    @Override
    public String toString() {
        String info = "TypeFieldDef{";
        if (null != name) {
            info += "graphql-name='" + name + "', ";
        }
        if (null != typeDef) {
            info += "graphql-type-definition=" + typeDef + ", ";
        }
        info += "ipto-attribute='" + attributeName + "' (" + attributeId + ")";
        info += '}';
        return info;
    }
}
