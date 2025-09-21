package org.gautelis.repo.graphql2.model;

import java.util.List;

/*
 * type Shipment @record(attribute: SHIPMENT) {
 *    shipmentId : String    @use(attribute: SHIPMENT_ID)
 *    deadline :   DateTime  @use(attribute: DEADLINE)
 *    reading :    [Float]   @use(attribute: READING)
 * }
 *
 * type Shipment @record(attribute: SHIPMENT) {
 *        ^                            ^
 *        | (a)                        | (b)
 *
 * Details about individual fields are found in TypeFieldDef
 */
public record RecordDef(
        String name,            /* (a) GraphQL specific */
        String attributeName,   /* (b) referenced by name, Ipto specific */
        int attributeId,        /* Ipto specific */
        List<TypeFieldDef> fields
) {
    @Override
    public String toString() {
        String info = "RecordDef{";
        if (null != name) {
            info += "graphql-name='" + name + "', ";
        }
        info += "ipto-attribute='" + attributeName + "' (" +  attributeId + ")";
        info += ", fields=[";
        for (TypeFieldDef field : fields) {
            info += field + ", ";
        }
        info += "]}";
        return info;
    }

    public boolean compare(RecordDef other) {
        return name.equals(other.name)
                && attributeName.equals(other.attributeName)
                && attributeId == other.attributeId;
                // TODO compare fields
    }
}
