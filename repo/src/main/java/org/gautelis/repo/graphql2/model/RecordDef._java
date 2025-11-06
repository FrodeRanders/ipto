package org.gautelis.repo.graphql2.model;

import java.util.ArrayList;
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
public abstract class RecordDef {
    public final String attributeName;   /* (b) referenced by name, Ipto specific */
    public final int attributeId;        /* Ipto specific */
    public final List<TypeFieldDef> fields;

    public RecordDef(String attributeName, int attributeId, List<TypeFieldDef> fields) {
        this.attributeName = attributeName;
        this.attributeId = attributeId;
        this.fields = fields;
    }

    @Override
    public String toString() {
        String info = "";
        info += "attribute-name='" + attributeName + "', ";
        info += "attribute-id=" +  attributeId;
        info += ", fields=[";
        for (TypeFieldDef field : fields) {
            info += field + ", ";
        }
        info += "]";
        return info;
    }

    /*
    public boolean compare(RecordDef other) {
        return fieldName.equals(other.fieldName)
                && attributeName.equals(other.attributeName)
                && attributeId == other.attributeId;
                // TODO compare fields
    }
    */
}
