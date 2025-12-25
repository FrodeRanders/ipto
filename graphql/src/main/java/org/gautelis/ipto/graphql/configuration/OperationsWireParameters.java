package org.gautelis.ipto.graphql.configuration;

import graphql.schema.idl.RuntimeWiring;
import org.gautelis.ipto.graphql.runtime.RuntimeService;
import org.gautelis.ipto.repo.model.Repository;
import tools.jackson.databind.ObjectMapper;

public record OperationsWireParameters(
        RuntimeWiring.Builder runtimeWiring,
        RuntimeService runtimeService,
        Repository repository
) {}
