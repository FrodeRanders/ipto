package org.gautelis.repo.graphql2.configuration;

import graphql.schema.idl.SchemaParser;
import graphql.schema.idl.TypeDefinitionRegistry;
import org.gautelis.repo.graphql2.model.ConfigIR;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Reader;

public class Configurator {
    private static final Logger log = LoggerFactory.getLogger(Configurator.class);

    private final TypeDefinitionRegistry registry;

    public Configurator(Reader reader) {
        this.registry = new SchemaParser().parse(reader);
    }

    public ConfigIR load() {
        var datatypes   = Datatypes.derive(registry);
        var attributes  = Attributes.derive(registry, datatypes);
        var records     = Records.derive(registry, attributes);
        var units       = Units.derive(registry, attributes);
        var operations  = Operations.derive(registry);

        // Merge into a single immutable IR
        return new ConfigIR(datatypes, attributes, records, units, operations);
    }
}
