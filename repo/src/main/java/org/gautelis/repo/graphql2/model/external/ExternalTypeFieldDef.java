package org.gautelis.repo.graphql2.model.external;

import org.gautelis.repo.graphql2.model.TypeFieldDef;

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
public class ExternalTypeFieldDef extends TypeFieldDef {
    public final String fieldName;      /* (c) GraphQL specific */
    public final TypeDef typeDef;       /* (d) GraphQL specific */

    public ExternalTypeFieldDef(String fieldName, TypeDef typeDef, String attributeName, int attributeId) {
        super(attributeName, attributeId);
        this.fieldName = fieldName;
        this.typeDef = typeDef;
    }

    @Override
    public String toString() {
        String info = "ExternalTypeFieldDef{";
        info += "field-name='" + fieldName + '\'';
        info += ", type-definition=" + typeDef + ", ";
        info += super.toString();
        info += '}';
        return info;
    }

    /*
    public boolean compare(TypeFieldDef other) {
        return fieldName.equals(other.fieldName)
                && attributeName.equals(other.attributeName)
                && attributeId == other.attributeId;
                // Ignore type defs!
    }
    */
}
