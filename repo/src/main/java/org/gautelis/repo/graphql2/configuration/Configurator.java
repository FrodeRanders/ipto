package org.gautelis.repo.graphql2.configuration;

import graphql.schema.idl.SchemaParser;
import graphql.schema.idl.TypeDefinitionRegistry;
import org.gautelis.repo.graphql2.model.*;
import org.gautelis.repo.graphql2.model.external.ExternalAttributeDef;
import org.gautelis.repo.graphql2.model.external.ExternalDataTypeDef;
import org.gautelis.repo.graphql2.model.external.ExternalRecordDef;
import org.gautelis.repo.model.Repository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Reader;
import java.util.Map;

public class Configurator {
    private static final Logger log = LoggerFactory.getLogger(Configurator.class);

    private Configurator() {
    }

    public static IntRep loadFromFile(Reader reader) {
        final TypeDefinitionRegistry registry = new SchemaParser().parse(reader);

        Map<String, ExternalDataTypeDef> datatypes = Datatypes.derive(registry);
        Map<String, ExternalAttributeDef> attributes = Attributes.derive(registry, datatypes);
        Map<String, ExternalRecordDef> records = Records.derive(registry, attributes);
        Map<String, UnitDef> units = Units.derive(registry, attributes);
        Map<String, OperationDef> operations  = Operations.derive(registry);

        // Merge into a single immutable IR
        return IntRep.fromExternal(datatypes, attributes, records, units, operations);
    }

    public static IntRep loadFromDB(Repository repository) {
        return IntRep.fromInternal(Datatypes.read(repository), Attributes.read(repository), Records.read(repository), Units.read(repository));
    }

    public static void reconcile(Reader reader, Repository repository) {
        IntRep external = loadFromFile(reader);
        external.dumpIr("GraphQL SDL", System.out);
        IntRep internal = loadFromDB(repository);
        internal.dumpIr("IPTO", System.out);

        // Datatypes
        for (String key : external.datatypes.keySet()) {
            if (!internal.datatypes.containsKey(key)) {
                log.warn("No matching internal datatype: {}", key);
                System.out.println("No matching internal datatype: " + key);
                continue;
            }
            DataTypeDef externalDataType = external.datatypes.get(key);
            DataTypeDef internalDataType = internal.datatypes.get(key);
            if (!externalDataType.equals(internalDataType)) {
                log.warn("External and internal datatype do not match: {} != {}", externalDataType, internalDataType);
                System.out.println("External and internal datatype do not match: " + externalDataType +  " != " + internalDataType);
            }
        }

        // Attributes
        for (String key : external.attributes.keySet()) {
            if (!internal.attributes.containsKey(key)) {
                log.warn("No matching internal attribute: {}", key);
                System.out.println("No matching internal attribute: " + key);
                continue;
            }
            AttributeDef externalAttribute = external.attributes.get(key);
            AttributeDef internalAttribute = internal.attributes.get(key);
            if (!externalAttribute.equals(internalAttribute)) {
                log.warn("External and internal attribute do not match: {} != {}", externalAttribute, internalAttribute);
                System.out.println("External and internal attribute do not match: " + externalAttribute +  " != " + internalAttribute);
            }
        }

    }
}
