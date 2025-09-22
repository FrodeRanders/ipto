package org.gautelis.repo.graphql2.model;

import java.util.List;

/*
 * type PurchaseOrder @unit(id: 42) {
 *    orderId  : String    @use(attribute: ORDER_ID)
 *    shipment : Shipment! @use(attribute: SHIPMENT)
 * }
 *
 * type PurchaseOrder @unit(id: 42) {
 *             ^                 ^
 *             | (a)             | (b)
 *
 * Details about individual fields are found in TypeFieldDef
 */
public record UnitDef(
        String fieldName,              /* (a) GraphQL specific */
        int templateId,                /* (b) Ipto specific */
        List<TypeFieldDef> fields
) {
    @Override
    public String toString() {
        String info = "UnitDef{";
        info += "field-name='" + fieldName + '\'';
        info += ", ipto-template-id=" + templateId;
        info += ", fields=[";
        for (TypeFieldDef field : fields) {
            info += field + ", ";
        }
        info += "]}";
        return info;
    }

}
