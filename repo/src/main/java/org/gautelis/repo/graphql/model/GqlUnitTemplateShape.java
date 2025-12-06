package org.gautelis.repo.graphql.model;

import java.util.List;

/*
 * type PurchaseOrder @unit {
 *    orderId  : String    @use(attribute: ORDER_ID)  <-- a FIELD in this unit
 *    ...
 * }
 *
 * type PurchaseOrder @unit(name: Order) {
 *             ^                   ^
 *             | (a)               | (c)
 *
 * Details about individual fields are found in GqlFieldShape
 */
public record GqlUnitTemplateShape(
        String typeName,           // (a)
        String templateName,       // (c)
        List<GqlFieldShape> fields
) {
    public boolean equals(CatalogUnitTemplate other) {
        return typeName.equals(other.templateName);
    }

    @Override
    public String toString() {
        String info = "GqlUnitTemplateShape{";
        info += "type-name='" + typeName + '\'';
        if (null != templateName) {
            info += ", template-name=" + templateName;
        }
        info += ", fields=[";
        for (GqlFieldShape field : fields) {
            info += field + ", ";
        }
        info += "]}";
        return info;
    }
}
