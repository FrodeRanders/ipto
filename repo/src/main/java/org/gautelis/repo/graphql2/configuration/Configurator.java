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

    public record GqlViewpoint(
            Map<String, GqlDatatypeShape> datatypes,
            Map<String, GqlAttributeShape> attributes,
            Map<String, GqlRecordShape> records,
            Map<String, GqlUnitShape> templates,
            Map<String, GqlOperationShape> operations
    ) {}

    public record CatalogViewpoint(
            Map<String, CatalogDatatype> datatypes,
            Map<String, CatalogAttribute> attributes,
            Map<String, CatalogRecord> records,
            Map<String, CatalogTemplate> units
    ) {}

    private Configurator() {
    }

    public static GqlViewpoint loadFromFile(Reader reader) {
        final TypeDefinitionRegistry registry = new SchemaParser().parse(reader);

        Map<String, GqlDatatypeShape> datatypes = Datatypes.derive(registry);
        Map<String, GqlAttributeShape> attributes = Attributes.derive(registry, datatypes);
        Map<String, GqlRecordShape> records = Records.derive(registry, attributes);
        Map<String, GqlUnitShape> templates = Units.derive(registry, attributes);
        Map<String, GqlOperationShape> operations  = Operations.derive(registry);

        return new GqlViewpoint(datatypes, attributes, records, templates, operations);
    }

    public static CatalogViewpoint loadFromCatalog(Repository repository) {
        Map<String, CatalogDatatype> datatypes = Datatypes.read(repository);
        Map<String, CatalogAttribute> attributes = Attributes.read(repository);
        Map<String, CatalogRecord> records = Records.read(repository);
        Map<String, CatalogTemplate> templates = Units.read(repository);

        return new CatalogViewpoint(datatypes, attributes, records, templates);
    }

    public static void dump(GqlViewpoint gql, PrintStream out) {
        out.println("===< GraphQL >===");
        out.println("--- Datatypes ---");
        for (Map.Entry<String, GqlDatatypeShape> entry : gql.datatypes().entrySet()) {
            out.println("  " + entry.getValue());
        }
        out.println();

        out.println("--- Attributes ---");
        for (Map.Entry<String, GqlAttributeShape> entry : gql.attributes().entrySet()) {
            out.println("  " + entry.getValue());
        }
        out.println();

        out.println("--- Records ---");
        for (Map.Entry<String, GqlRecordShape> entry : gql.records().entrySet()) {
            out.println("  " + entry.getValue());
        }
        out.println();

        out.println("--- Templates ---");
        for (Map.Entry<String, GqlUnitShape> entry : gql.templates().entrySet()) {
            out.println("  " + entry.getValue());
        }
        out.println();

        out.println("--- Operations ---");
        for (Map.Entry<String, GqlOperationShape> entry : gql.operations().entrySet()) {
            out.println("  " + entry.getValue());
        }
        out.println();
    }

    public static void dump(CatalogViewpoint ipto, PrintStream out) {
        out.println("===< Catalog >===");
        out.println("--- Datatypes ---");
        for (Map.Entry<String, CatalogDatatype> entry : ipto.datatypes().entrySet()) {
            out.println("  " + entry.getValue());
        }
        out.println();

        out.println("--- Attributes ---");
        for (Map.Entry<String, CatalogAttribute> entry : ipto.attributes().entrySet()) {
            out.println("  " + entry.getValue());
        }
        out.println();

        out.println("--- Records ---");
        for (Map.Entry<String, CatalogRecord> entry : ipto.records().entrySet()) {
            out.println("  " + entry.getValue());
        }
        out.println();

        out.println("--- Templates ---");
        for (Map.Entry<String, CatalogTemplate> entry : ipto.units().entrySet()) {
            out.println("  " + entry.getValue());
        }
        out.println();
    }

    public static void reconcile(GqlViewpoint gql, CatalogViewpoint ipto, ResolutionPolicy policy) {

        // --- Datatypes ---
        for (String key : gql.datatypes().keySet()) {
            if (!ipto.datatypes().containsKey(key)) {
                log.warn("No matching catalog datatype: {}", key);
                System.out.println("No matching catalog datatype: " + key);
                continue;
            }
            GqlDatatypeShape gqlDatatype = gql.datatypes().get(key);
            CatalogDatatype iptoDatatype = ipto.datatypes().get(key);
            if (!gqlDatatype.equals(iptoDatatype)) {
                log.warn("GraphQL SDL and catalog datatype do not match: {} != {}", gqlDatatype, iptoDatatype);
                System.out.println("GraphQL SDL and catalog datatype do not match: " + gqlDatatype +  " != " + iptoDatatype);
            }
        }

        // Attributes
        for (String key : gql.attributes().keySet()) {
            if (!ipto.attributes().containsKey(key)) {
                log.warn("No matching catalog attribute: {}", key);
                System.out.println("No matching catalog attribute: " + key);
                continue;
            }
            GqlAttributeShape gqlAttribute = gql.attributes().get(key);
            CatalogAttribute iptoAttribute = ipto.attributes().get(key);
            if (!gqlAttribute.equals(iptoAttribute)) {
                log.warn("GraphQL SDL and catalog attribute do not match: {} != {}", gqlAttribute, iptoAttribute);
                System.out.println("GraphQL SDL and catalog attribute do not match: " + gqlAttribute +  " != " + iptoAttribute);
            }

            /*
            GraphQL SDL and catalog attribute do not match:
            GqlAttributeShape{name='dcLanguage'}
            !=
            CatalogAttribute{attribute-name='dc:language'}
             */
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
