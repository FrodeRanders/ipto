package org.gautelis.repo.graphql2.model;

import org.gautelis.repo.graphql2.model.external.ExternalAttributeDef;
import org.gautelis.repo.graphql2.model.external.ExternalDataTypeDef;
import org.gautelis.repo.graphql2.model.external.ExternalRecordDef;
import org.gautelis.repo.graphql2.model.internal.InternalAttributeDef;
import org.gautelis.repo.graphql2.model.internal.InternalDataTypeDef;
import org.gautelis.repo.graphql2.model.internal.InternalRecordDef;

import java.io.PrintStream;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;

/**
 * Intermediate representation.
 */
public class IntRep {
    public final Map<String, DataTypeDef> datatypes;
    public final Map<String, AttributeDef> attributes;
    public final Map<String, RecordDef> records;
    public final Map<String, UnitDef> units;
    public final Map<String, OperationDef> operations;

    public IntRep() {
        datatypes = new HashMap<>();
        attributes = new HashMap<>();
        records = new HashMap<>();
        units = new HashMap<>();
        operations = new HashMap<>();
    }

    public static IntRep fromExternal(
        Map<String, ExternalDataTypeDef> datatypes,
        Map<String, ExternalAttributeDef> attributes,
        Map<String, ExternalRecordDef> records,
        Map<String, UnitDef> units,
        Map<String, OperationDef> operations
    ) {
        IntRep ir = new IntRep();
        ir.datatypes.putAll(datatypes);
        ir.attributes.putAll(attributes);
        ir.records.putAll(records);
        ir.units.putAll(units);
        ir.operations.putAll(operations);
        return ir;
    }

    public static IntRep fromInternal(
        Map<String, InternalDataTypeDef> datatypes,
        Map<String, InternalAttributeDef> attributes,
        Map<String, InternalRecordDef> records,
        Map<String, UnitDef> units
    ) {
        IntRep ir = new IntRep();
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

    public void dumpIr(String source, PrintStream out) {
        out.println("===< " + source + " >===");
        out.println("--- Datatypes ---");
        for (Map.Entry<String, DataTypeDef> entry : datatypes.entrySet()) {
            out.println("  " + entry.getValue());
        }
        out.println();

        out.println("--- Attributes ---");
        for (Map.Entry<String, AttributeDef> entry : attributes.entrySet()) {
            out.println("  " + entry.getValue());
        }
        out.println();

        out.println("--- Records ---");
        for (Map.Entry<String, RecordDef> entry : records.entrySet()) {
            out.println("  " + entry.getValue());
        }
        out.println();

        out.println("--- Units ---");
        for (Map.Entry<String, UnitDef> entry : units.entrySet()) {
            out.println("  " + entry.getValue());
        }
        out.println();

        out.println("--- Operations ---");
        for (Map.Entry<String, OperationDef> entry : operations.entrySet()) {
            out.println("  " + entry.getValue());
        }
        out.println();
    }
}
