package org.gautelis.repo.graphql2.model.external;

import org.gautelis.repo.graphql2.model.RecordDef;
import org.gautelis.repo.graphql2.model.TypeFieldDef;

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
public class ExternalRecordDef extends RecordDef {
    public final String fieldName;       /* (a) GraphQL specific */

    public ExternalRecordDef(String fieldName, String attributeName, int attributeId, List<TypeFieldDef> fields) {
        super(attributeName, attributeId, fields);
        this.fieldName = fieldName;
    }

    @Override
    public String toString() {
        String info = "ExternalRecordDef{";
        info += super.toString();
        info += "}";
        return info;
    }

    /*
    public boolean compare(ExternalRecordDef other) {
        return name.equals(other.name)
                && attributeName.equals(other.attributeName)
                && attributeId == other.attributeId;
                // TODO compare fields
    }
    */
}
