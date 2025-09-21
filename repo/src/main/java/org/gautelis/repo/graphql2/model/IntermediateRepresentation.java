package org.gautelis.repo.graphql2.model;

import java.util.Map;

public record IntermediateRepresentation(
        Map<String, DataType> datatypes,
        Map<String, AttributeDef> attributes,
        Map<String, RecordDef> records,
        Map<String, UnitDef> units,
        Map<String, OperationDef> operations
) {
    /*
    public static IntermediateRepresentation empty() {
        return new IntermediateRepresentation(Map.of(), Map.of(), Map.of(), Map.of(), Map.of());
    }
    */
}
