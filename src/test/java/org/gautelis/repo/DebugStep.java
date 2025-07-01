package org.gautelis.repo;

import graphql.ExecutionResult;
import graphql.GraphQL;
import graphql.GraphQLError;
import graphql.language.SourceLocation;
import org.gautelis.repo.model.Repository;
import org.gautelis.repo.model.Unit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class DebugStep {
    private static final Logger log = LoggerFactory.getLogger(DebugStep.class);

    public static void main(String... args) {
        try (InputStreamReader sdl = new InputStreamReader(
                Objects.requireNonNull(RepositoryTest.class.getResourceAsStream("unit-schema.graphqls"))
        )) {
            Repository repo = RepositoryFactory.getRepository();
            Optional<GraphQL> graphQl = repo.loadConfiguration(sdl);
            if (graphQl.isEmpty()) {
                log.error("Could not load configuration");
                System.exit(1);
            }

            final int tenantId = 1;
            long unitId;
            {

                Unit unit = repo.createUnit(tenantId, "graphql test");

                unit.withAttribute("dc:title", String.class, attr -> {
                    ArrayList<String> value = attr.getValue();
                    value.add("abc");
                });

                repo.storeUnit(unit);

                unitId = unit.getUnitId();
            }

            String query = String.format("""
                    query {
                      unit (id: { tenantId: %d, unitId: %d }) {
                        title
                      }
                    }
                   """, tenantId, unitId);
            log.info(query);

            GraphQL gql = graphQl.get();
            ExecutionResult result = gql.execute(query);

            List<GraphQLError> errors = result.getErrors();
            if (errors.isEmpty()) {
                log.info("Result: {}", (Object) result.getData());

            } else {
                for (GraphQLError error : errors) {
                    log.error("error: {}: {}", error.getMessage(), error);

                    List<SourceLocation> locations = error.getLocations();
                    for (SourceLocation location : locations) {
                        log.error("location: {}: {}", location.getLine(), location);
                    }
                }
            }
        } catch (IOException ioe) {
            ioe.printStackTrace(System.err);
        }
    }
}
