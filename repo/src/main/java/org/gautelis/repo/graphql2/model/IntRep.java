package org.gautelis.repo.graphql2.model;

import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;

/**
 * Intermediate representation.
 */
public class IntRep {
    public final Map<String, GqlDataTypeShape> datatypes;
    public final Map<String, GqlAttributeShape> attributes;
    public final Map<String, GqlRecordShape> records;
    public final Map<String, GqlUnitShape> units;
    public final Map<String, GqlOperationShape> operations;

    public IntRep() {
        datatypes = new HashMap<>();
        attributes = new HashMap<>();
        records = new HashMap<>();
        units = new HashMap<>();
        operations = new HashMap<>();
    }

    public static IntRep fromGql(
        Map<String, GqlDataTypeShape> datatypes,
        Map<String, GqlAttributeShape> attributes,
        Map<String, GqlRecordShape> records,
        Map<String, GqlUnitShape> units,
        Map<String, GqlOperationShape> operations
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
        //Map<String, InternalDataTypeDef> datatypes,
        //Map<String, InternalAttributeDef> attributes
        //Map<String, InternalRecordDef> records,
        //Map<String, UnitDef> units
    ) {
        IntRep ir = new IntRep();
        //ir.datatypes.putAll(datatypes);
        //ir.attributes.putAll(attributes);
        //ir.records.putAll(records);
        //ir.units.putAll(units);
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
        for (Map.Entry<String, GqlDataTypeShape> entry : datatypes.entrySet()) {
            out.println("  " + entry.getValue());
        }
        out.println();

        out.println("--- Attributes ---");
        for (Map.Entry<String, GqlAttributeShape> entry : attributes.entrySet()) {
            out.println("  " + entry.getValue());
        }
        out.println();

        out.println("--- Records ---");
        for (Map.Entry<String, GqlRecordShape> entry : records.entrySet()) {
            out.println("  " + entry.getValue());
        }
        out.println();

        out.println("--- Units ---");
        for (Map.Entry<String, GqlUnitShape> entry : units.entrySet()) {
            out.println("  " + entry.getValue());
        }
        out.println();

        out.println("--- Operations ---");
        for (Map.Entry<String, GqlOperationShape> entry : operations.entrySet()) {
            out.println("  " + entry.getValue());
        }
        out.println();
    }
}
