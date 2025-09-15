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
 */
public record UnitEntryDef(
        String name,
        Map<String, String> metadata,          // arbitrary directive-driven flags
        List<String> attributes,               // references by name
        List<RecordDef> records
) {
    @NotNull
    @Override
    public String toString() {
        String info = "UnitEntryDef{";
        info += "name='" + name + '\'';
        info += ", metadata=<withheld>";
        info += ", attributes=[" + String.join(",", attributes) + "]";
        info += ", records=[";
        for (RecordDef rd : records) {
            info += rd;
            info += ", ";
        }
        info += "]";
        info += '}';
        return info;
    }

}
