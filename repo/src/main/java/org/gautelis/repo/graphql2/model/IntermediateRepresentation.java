package org.gautelis.repo.graphql2.model;

import org.gautelis.repo.graphql2.configuration.*;
import org.gautelis.repo.graphql2.model.external.ExternalAttributeDef;
import org.gautelis.repo.graphql2.model.external.ExternalDataTypeDef;
import org.gautelis.repo.graphql2.model.external.ExternalRecordDef;
import org.gautelis.repo.graphql2.model.internal.InternalAttributeDef;
import org.gautelis.repo.graphql2.model.internal.InternalDataTypeDef;
import org.gautelis.repo.graphql2.model.internal.InternalRecordDef;

import java.util.HashMap;
import java.util.Map;

public class IntermediateRepresentation {
    public final Map<String, DataTypeDef> datatypes;
    public final Map<String, AttributeDef> attributes;
    public final Map<String, RecordDef> records;
    public final Map<String, UnitDef> units;
    public final Map<String, OperationDef> operations;

    public IntermediateRepresentation() {
        datatypes = new HashMap<>();
        attributes = new HashMap<>();
        records = new HashMap<>();
        units = new HashMap<>();
        operations = new HashMap<>();
    }

    public static IntermediateRepresentation fromExternal(
        Map<String, ExternalDataTypeDef> datatypes,
        Map<String, ExternalAttributeDef> attributes,
        Map<String, ExternalRecordDef> records,
        Map<String, UnitDef> units,
        Map<String, OperationDef> operations
    ) {
        IntermediateRepresentation ir = new IntermediateRepresentation();
        ir.datatypes.putAll(datatypes);
        ir.attributes.putAll(attributes);
        ir.records.putAll(records);
        ir.units.putAll(units);
        ir.operations.putAll(operations);
        return ir;
    }

    public static IntermediateRepresentation fromInternal(
        Map<String, InternalDataTypeDef> datatypes,
        Map<String, InternalAttributeDef> attributes,
        Map<String, InternalRecordDef> records,
        Map<String, UnitDef> units
    ) {
        IntermediateRepresentation ir = new IntermediateRepresentation();
        ir.datatypes.putAll(datatypes);
        ir.attributes.putAll(attributes);
        ir.records.putAll(records);
        ir.units.putAll(units);
        return ir;
    }

    /*
    public static IntermediateRepresentation empty() {
        return new IntermediateRepresentation(Map.of(), Map.of(), Map.of(), Map.of(), Map.of());
    }
    */
}
