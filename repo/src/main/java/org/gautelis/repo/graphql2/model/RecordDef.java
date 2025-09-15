package org.gautelis.repo.graphql2.model;

import org.jetbrains.annotations.NotNull;

import java.util.List;

/*
 * type Shipment @record(attribute: SHIPMENT) {
 *    shipmentId  : String  @use(attribute: SHIPMENT_ID)
 *    deadline : DateTime   @use(attribute: DEADLINE)
 *    reading  : [Float]    @use(attribute: READING)
 * }
 *
 * type Shipment @record(attribute: SHIPMENT) {
 *        ^                            ^
 *        | (a)                        | (b)
 */
public record RecordDef(
        String name,            /* (a) */
        String attributeName,   /* (b) referenced by name */
        int attributeId,
        List<TypeFieldDef> fields
) implements NodeDef {
    @NotNull
    @Override
    public String toString() {
        String info = "RecordDef{";
        info += "name='" + name + '\'';
        info += ", attribute='" + attributeName + "' (" + attributeId + ")";
        info += ", fields=[";
        for (TypeFieldDef field : fields) {
            info += field + ", ";
        }
        info += "]}";
        return info;
    }
}
