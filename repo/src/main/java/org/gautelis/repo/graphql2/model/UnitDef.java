package org.gautelis.repo.graphql2.model;

import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.Map;

/*
 * type PurchaseOrder @unit(id: 42) {
 *    orderId  : String    @use(attribute: ORDER_ID)
 *    shipment : Shipment! @use(attribute: SHIPMENT)
 * }
 *
 * type PurchaseOrder @unit(id: 42) {
 *             ^                 ^
 *             | (a)             | (b)
 */
public record UnitDef(
        String name,                   /* (a) */
        int templateId,                /* (b) */
        List<TypeFieldDef> fields
) implements NodeDef {
    @NotNull
    @Override
    public String toString() {
        String info = "UnitDef{";
        info += "name='" + name + '\'';
        info += ", templateId=" + templateId;
        info += ", fields=[";
        for (TypeFieldDef field : fields) {
            info += field + ", ";
        }
        info += "]}";
        return info;
    }

}
