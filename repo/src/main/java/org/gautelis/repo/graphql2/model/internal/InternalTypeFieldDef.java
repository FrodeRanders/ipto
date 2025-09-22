package org.gautelis.repo.graphql2.model.internal;

import org.gautelis.repo.graphql2.model.TypeDef;
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
public class InternalTypeFieldDef extends TypeFieldDef {
    public InternalTypeFieldDef(String attributeName, int attributeId) {
        super(attributeName, attributeId);
    }
    
    @Override
    public String toString() {
        String info = "InternalTypeFieldDef{";
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
