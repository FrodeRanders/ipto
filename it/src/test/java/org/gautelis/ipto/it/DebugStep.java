package org.gautelis.ipto.it;

import graphql.ExecutionInput;
import graphql.ExecutionResult;
import graphql.GraphQL;
import graphql.GraphQLError;
import graphql.language.SourceLocation;
import org.gautelis.ipto.graphql.configuration.Configurator;
import org.gautelis.ipto.repo.RepositoryFactory;
import org.gautelis.ipto.repo.model.Repository;
import org.gautelis.ipto.repo.model.Unit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

public class DebugStep {
    private static final Logger log = LoggerFactory.getLogger(DebugStep.class);

    public static void main(String... args) {
        try (InputStreamReader reader = new InputStreamReader(
                Objects.requireNonNull(GraphQLIT.class.getResourceAsStream("schema2.graphqls"))
        )) {
            Repository repo = RepositoryFactory.getRepository();
            Optional<GraphQL> _graphQL = Configurator.load(repo, reader, null, System.out);

            if (_graphQL.isEmpty()) {
                log.error("Could not load configuration");
                System.exit(1);
            }

            final int tenantId = 1;
            long unitId;
            {
                Unit unit = repo.createUnit(tenantId, "graphql test");

                unit.withAttribute("dce:title", String.class, attr -> {
                    ArrayList<String> value = attr.getValueVector();
                    value.add("abc");
                });

                repo.storeUnit(unit);

                unitId = unit.getUnitId();
            }

            String query = """
                query Unit($id: UnitIdentification!) {
                  unit(id: $id) {
                    title
                  }
                }
                """;

            graphql.GraphQL graphQL = _graphQL.get();
            ExecutionResult result = graphQL.execute(
                    ExecutionInput.newExecutionInput()
                            .query(query)
                            .variables(Map.of(
                                    "id", Map.of(
                                      "tenantId", tenantId,
                                      "unitId",   unitId
                                    )
                                )
                            )
                            .build());

            List<GraphQLError> errors = result.getErrors();
            if (errors.isEmpty()) {
                log.info("Result: {}", (Object) result.getData());

            } else {
                for (GraphQLError error : errors) {
                    log.error("error: {}: {}", error.getMessage(), error);

                    List<SourceLocation> locations = error.getLocations();
                    if (null != locations) {
                        for (SourceLocation location : locations) {
                            log.error("location: {}: {}", location.getLine(), location);
                        }
                    }
                }
            }
        } catch (IOException ioe) {
            ioe.printStackTrace(System.err);
        }
    }
}
