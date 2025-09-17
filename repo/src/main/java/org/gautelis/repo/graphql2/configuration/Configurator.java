package org.gautelis.repo.graphql2.configuration;

import graphql.schema.idl.SchemaParser;
import graphql.schema.idl.TypeDefinitionRegistry;
import org.gautelis.repo.graphql2.model.*;
import org.gautelis.repo.model.Repository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Reader;
import java.util.Map;

public class Configurator {
    private static final Logger log = LoggerFactory.getLogger(Configurator.class);

    private Configurator() {
    }

    public static ConfigIR loadFromFile(Reader reader) {
        final TypeDefinitionRegistry registry = new SchemaParser().parse(reader);

        Map<String, DataTypeDef> datatypes = Datatypes.derive(registry);
        Map<String, AttributeDef> attributes = Attributes.derive(registry, datatypes);
        Map<String, RecordDef> records = Records.derive(registry, attributes);
        Map<String, UnitDef> units = Units.derive(registry, attributes);
        Map<String, OperationDef> operations  = Operations.derive(registry);

        // Merge into a single immutable IR
        return new ConfigIR(datatypes, attributes, records, units, operations);
    }

    public static ConfigIR loadFromDB(Repository repository) {
        Map<String, DataTypeDef> datatypes = Datatypes.read(repository);
        Map<String, AttributeDef> attributes = Attributes.read(repository);
        Map<String, RecordDef> records = Records.read(repository);
        Map<String, UnitDef> units = Units.read(repository);
        Map<String, OperationDef> operations = Map.of(); // This is a GraphQL thing...

        // Merge into a single immutable IR
        return new ConfigIR(datatypes, attributes, records, units, operations);
    }
}
