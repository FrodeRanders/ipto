package org.gautelis.repo.graphql2.configuration;

import graphql.schema.idl.SchemaParser;
import graphql.schema.idl.TypeDefinitionRegistry;
import org.gautelis.repo.graphql2.model.*;
import org.gautelis.repo.model.Repository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;
import java.io.Reader;
import java.util.Map;

public class Configurator {
    private static final Logger log = LoggerFactory.getLogger(Configurator.class);

    private Configurator() {
    }

    public static IntRep loadFromFile(Reader reader) {
        final TypeDefinitionRegistry registry = new SchemaParser().parse(reader);

        Map<String, GqlDataTypeShape> datatypes = Datatypes.derive(registry);
        Map<String, GqlAttributeShape> attributes = Attributes.derive(registry, datatypes);
        Map<String, GqlRecordShape> records = Records.derive(registry, attributes);
        Map<String, GqlUnitShape> units = Units.derive(registry, attributes);
        Map<String, GqlOperationShape> operations  = Operations.derive(registry);

        // Merge into a single immutable IR
        return IntRep.fromGql(datatypes, attributes, records, units, operations);
    }

    public static IntRep loadFromCatalog(Repository repository) {
        Map<String, CatalogDatatype> datatypes = Datatypes.read(repository);
        Map<String, CatalogAttribute> attributes = Attributes.read(repository);
        Map<String, CatalogRecord> records = Records.read(repository);
        Map<String, CatalogTemplate> units = Units.read(repository);

        PrintStream out = System.out;
        out.println("===< Catalog >===");
        out.println("--- Datatypes ---");
        for (Map.Entry<String, CatalogDatatype> entry : datatypes.entrySet()) {
            out.println("  " + entry.getValue());
        }
        out.println();

        out.println("--- Attributes ---");
        for (Map.Entry<String, CatalogAttribute> entry : attributes.entrySet()) {
            out.println("  " + entry.getValue());
        }
        out.println();

        out.println("--- Records ---");
        for (Map.Entry<String, CatalogRecord> entry : records.entrySet()) {
            out.println("  " + entry.getValue());
        }
        out.println();

        out.println("--- Templates ---");
        for (Map.Entry<String, CatalogTemplate> entry : units.entrySet()) {
            out.println("  " + entry.getValue());
        }
        out.println();



        return null; // IntRep.fromInternal(/*Datatypes.read(repository),*/ Attributes.read(repository) /* , Records.read(repository), Units.read(repository) */);
    }

    public static void reconcile(Reader reader, Repository repository) {
        IntRep external = loadFromFile(reader);
        external.dumpIr("GraphQL SDL", System.out);
        IntRep internal = loadFromCatalog(repository);
        internal.dumpIr("IPTO", System.out);

        // Datatypes
        for (String key : external.datatypes.keySet()) {
            if (!internal.datatypes.containsKey(key)) {
                log.warn("No matching catalog datatype: {}", key);
                System.out.println("No matching catalog datatype: " + key);
                continue;
            }
            GqlDataTypeShape externalDataType = external.datatypes.get(key);
            GqlDataTypeShape internalDataType = internal.datatypes.get(key);
            if (!externalDataType.equals(internalDataType)) {
                log.warn("External and catalog datatype do not match: {} != {}", externalDataType, internalDataType);
                System.out.println("External and catalog datatype do not match: " + externalDataType +  " != " + internalDataType);
            }
        }

        // Attributes
        for (String key : external.attributes.keySet()) {
            if (!internal.attributes.containsKey(key)) {
                log.warn("No matching catalog attribute: {}", key);
                System.out.println("No matching catalog attribute: " + key);
                continue;
            }
            GqlAttributeShape externalAttribute = external.attributes.get(key);
            GqlAttributeShape internalAttribute = internal.attributes.get(key);
            if (!externalAttribute.equals(internalAttribute)) {
                log.warn("External and catalog attribute do not match: {} != {}", externalAttribute, internalAttribute);
                System.out.println("External and catalog attribute do not match: " + externalAttribute +  " != " + internalAttribute);
            }
        }

    }

    /*
    public static void wire(
            IntRep intRep,
            RuntimeWiring.Builder runtimeWiring,
            Repository repository,
            RuntimeService runtimeService // TODO
    ) {
        //
        Map<String, UnitDef> units = intRep.units;
        for (Map.Entry<String, UnitDef> entry : units.entrySet()) {
            String unitName = entry.getKey();

            UnitDef unitDef = entry.getValue();

            runtimeWiring.type(unitName, builder -> {
                for (TypeFieldDef fieldDef : unitDef.fields()) {
                    // TODO
                    CombinedTypeFieldDef combinedTypeFieldDef = (CombinedTypeFieldDef) fieldDef;

                    String fieldName = combinedTypeFieldDef.fieldName;
                    TypeDef fieldType = combinedTypeFieldDef.typeDef;
                    int attrId = combinedTypeFieldDef.attributeId;

                    // ----------------------------------------------------------------------
                    // Tell GraphQL how to fetch this specific unit type
                    // by attaching generic field fetchers to every @unit type
                    // ----------------------------------------------------------------------
                    DataFetcher<?> fetcher = env -> {
                        //**** Executed at runtime **********************************
                        // My mission in life is to resolve a specific attribute
                        // (the current 'fieldName') in a specific type (the current
                        // 'unitName'). Everything needed at runtime is accessible
                        // right now so it is captured for later.
                        //***********************************************************
                        Box box = env.getSource();
                        if (null == box) {
                            log.warn("No box");
                            return null;
                        }

                        log.trace("Fetching attribute {} from unit {}: {}.{}", fieldType.isArray() ? fieldName + "[]" : fieldName, unitName, box.getTenantId(), box.getUnitId());

                        if (fieldType.isArray()) {
                            return runtimeService.getArray(box, attrId);
                        } else {
                            return runtimeService.getScalar(box, attrId);
                        }
                    };
                    builder.dataFetcher(fieldName, fetcher);
                    log.info("Wiring: {}>{}", unitName, fieldName);
                }

                return builder;
            });
        }
    }
    */
}
