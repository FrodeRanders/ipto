package org.gautelis.repo.graphql.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import graphql.language.*;
import graphql.schema.DataFetcher;
import graphql.schema.idl.RuntimeWiring;
import org.gautelis.repo.graphql.runtime.RuntimeService;
import org.gautelis.repo.model.Repository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class OperationsConfigurator {
    private static final Logger log = LoggerFactory.getLogger(OperationsConfigurator.class);

    /* package visible only */
    record UnitIdentification(int tenantId, long unitId) {
    }

    /* package visible only */
    enum SchemaOperation {QUERY, MUTATION}

    private final Repository repo;
    private final Map<String, SchemaOperation> operations;

    private ObjectMapper objectMapper = new ObjectMapper();

    /* package accessible only */
    OperationsConfigurator(Repository repo, Map<String, SchemaOperation> operations) {
        this.repo = repo;
        this.operations = operations;
    }

    /* package visible only */
    void load(
            ObjectTypeDefinition type,
            Map<String, Configurator.ExistingDatatypeMeta> datatypes,
            Map<String, Configurator.ProposedAttributeMeta> attributesSchemaView,
            Map<Integer, Configurator.ProposedAttributeMeta> attributesIptoView,
            RuntimeWiring.Builder runtimeWiring,
            RuntimeService repoService
    ) {
        SchemaOperation operation = operations.get(type.getName());
        if (operation != null) {
            switch (operation) {
                case QUERY -> loadQuery(type, datatypes, attributesSchemaView, attributesIptoView, runtimeWiring, repoService);
                case MUTATION -> loadMutation(type, datatypes, attributesSchemaView, attributesIptoView, runtimeWiring, repoService);
            }
        }
    }

    private void loadQuery(
            ObjectTypeDefinition type,
            Map<String, Configurator.ExistingDatatypeMeta> datatypes,
            Map<String, Configurator.ProposedAttributeMeta> attributesSchemaView,
            Map<Integer, Configurator.ProposedAttributeMeta> attributesIptoView,
            RuntimeWiring.Builder runtimeWiring,
            RuntimeService repoService
    ) {
        for (FieldDefinition f : type.getFieldDefinitions()) {
            // Operation name
            final String fieldName = f.getName();
            final FieldType fieldType = FieldType.get(f);

            switch (fieldName) {
                // "Hardcoded" point lookup for specific unit
                case "unit" -> {
                    log.trace("{}::{}(...) : {}", type.getName(), f.getName(), fieldType.name());

                    DataFetcher<?> unitById = env -> {
                        log.trace("{}::{}(id : {}) : {}", type.getName(), fieldName, env.getArgument("id"), fieldType.name());

                        UnitIdentification id = objectMapper.convertValue(env.getArgument("id"), UnitIdentification.class);
                        return repoService.loadUnit(id.tenantId(), id.unitId());
                    };

                    runtimeWiring.type(type.getName(), t -> t.dataFetcher(fieldName, unitById));
                    log.info("Wiring: {}::{}", type.getName(), fieldName);
                }

                //
                case "units" -> {
                    log.trace("{}::{}(...) : {}", type.getName(), fieldName, fieldType.name());
                    log.info("Wiring: {}::{}", type.getName(), fieldName);
                }
            }
        }
    }

    private void loadMutation(
            ObjectTypeDefinition type,
            Map<String, Configurator.ExistingDatatypeMeta> datatypes,
            Map<String, Configurator.ProposedAttributeMeta> attributesSchemaView,
            Map<Integer, Configurator.ProposedAttributeMeta> attributesIptoView,
            RuntimeWiring.Builder runtimeWiring,
            RuntimeService repoService
    ) {

    }
}