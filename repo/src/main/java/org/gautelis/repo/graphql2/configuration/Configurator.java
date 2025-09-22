package org.gautelis.repo.graphql2.configuration;

import graphql.schema.idl.SchemaParser;
import graphql.schema.idl.TypeDefinitionRegistry;
import org.gautelis.repo.graphql2.model.*;
import org.gautelis.repo.graphql2.model.external.ExternalAttributeDef;
import org.gautelis.repo.graphql2.model.external.ExternalDataTypeDef;
import org.gautelis.repo.graphql2.model.internal.InternalAttributeDef;
import org.gautelis.repo.graphql2.model.internal.InternalDataTypeDef;
import org.gautelis.repo.model.Repository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Reader;
import java.util.Map;

public class Configurator {
    private static final Logger log = LoggerFactory.getLogger(Configurator.class);

    private Configurator() {
    }

    public static IntermediateRepresentation loadFromFile(Reader reader) {
        final TypeDefinitionRegistry registry = new SchemaParser().parse(reader);

        Map<String, ExternalDataTypeDef> datatypes = Datatypes.derive(registry);
        Map<String, ExternalAttributeDef> attributes = Attributes.derive(registry, datatypes);
        Map<String, RecordDef> records = Records.derive(registry, attributes);
        Map<String, UnitDef> units = Units.derive(registry, attributes);
        Map<String, OperationDef> operations  = Operations.derive(registry);

        // Merge into a single immutable IR
        return IntermediateRepresentation.fromExternal(datatypes, attributes, records, units, operations);
    }

    public static IntermediateRepresentation loadFromDB(Repository repository) {
        return IntermediateRepresentation.fromInternal(Datatypes.read(repository), Attributes.read(repository), Records.read(repository), Units.read(repository));
    }

    public static IntermediateRepresentation reconcile(/* TODO */) {
        return null;
    }
}
