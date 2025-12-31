package org.gautelis.ipto.graphql;

import graphql.ExecutionInput;
import graphql.ExecutionResult;
import graphql.GraphQL;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.util.Collections;
import java.util.Map;

@Path("/graphql")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class GraphQLResource {
    private final GraphQL graphQL;

    @Inject
    public GraphQLResource(GraphQL graphQL) {
        this.graphQL = graphQL;
    }

    @POST
    public Response execute(GraphQLRequest request) {
        if (request == null || request.query() == null || request.query().isBlank()) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(Map.of("error", "Missing GraphQL query"))
                    .build();
        }

        Map<String, Object> variables = request.variables() == null
                ? Collections.emptyMap()
                : request.variables();

        ExecutionInput.Builder input = ExecutionInput.newExecutionInput()
                .query(request.query())
                .variables(variables);

        if (request.operationName() != null && !request.operationName().isBlank()) {
            input.operationName(request.operationName());
        }

        ExecutionResult result = graphQL.execute(input.build());
        return Response.ok(result.toSpecification()).build();
    }
}
