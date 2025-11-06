package org.gautelis.repo.graphql2.model;

import java.util.List;

/*
 * type PurchaseOrder @unit(id: 42) {
 *    orderId  : String    @use(attribute: ORDER_ID)  <-- a FIELD in this unit
 *    ...
 * }
 *
 * type PurchaseOrder @unit(id: 42) {
 *             ^                 ^
 *             | (a)             | (b)
 *
 * -or-
 *
 * type PurchaseOrder @unit(name: Order) {
 *             ^                   ^
 *             | (a)               | (c)
 *
 * Details about individual fields are found in GqlFieldShape
 */
public record GqlUnitShape(
        String typeName,     // (a)
        int templateId,      // (b)
        String templateName, // (c)
        List<GqlFieldShape> fields
) {
    @Override
    public String toString() {
        String info = "GqlUnitShape{";
        info += "type-name='" + typeName + '\'';
        if (null != templateName) {
            info += ", template-name=" + templateName;
        } else {
            info += ", template-id=" + templateId;
        }
        info += ", fields=[";
        for (GqlFieldShape field : fields) {
            info += field + ", ";
        }
        info += "]}";
        return info;
    }
}
