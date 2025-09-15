package org.gautelis.repo.graphql2.model;

import java.util.Map;

public record ConfigIR(
        Map<String, DataTypeDef> datatypes,
        Map<String, AttributeDef> attributes,
        Map<String, RecordDef> records,
        Map<String, UnitDef> units,
        Map<String, OperationDef> operations
) {
    public static ConfigIR empty() {
        return new ConfigIR(Map.of(), Map.of(), Map.of(), Map.of(), Map.of());
    }
}
