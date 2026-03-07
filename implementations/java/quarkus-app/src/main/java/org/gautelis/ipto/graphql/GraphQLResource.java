/*
 * Copyright (C) 2025-2026 Frode Randers
 * All rights reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
import org.gautelis.ipto.api.UnitResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;

@Path("/graphql")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class GraphQLResource {
    private static final Logger log = LoggerFactory.getLogger(GraphQLResource.class);

    private final GraphQL graphQL;

    @Inject
    public GraphQLResource(GraphQL graphQL) {
        this.graphQL = graphQL;
    }

    @POST
    public Response execute(GraphQLRequest request) {
        log.debug("GraphQLResource::execute({})", request);

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

        log.trace("=> result: {}", result);
        return Response.ok(result.toSpecification()).build();
    }
}
