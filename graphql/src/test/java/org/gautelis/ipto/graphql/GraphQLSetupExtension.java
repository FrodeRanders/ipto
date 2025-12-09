package org.gautelis.ipto.graphql;

import graphql.GraphQL;
import org.gautelis.ipto.graphql.configuration.Configurator;
import org.gautelis.ipto.repo.RepositoryFactory;
import org.gautelis.ipto.repo.model.Repository;
import org.junit.jupiter.api.extension.*;

import java.io.InputStreamReader;
import java.util.Objects;
import java.util.Optional;

import static org.junit.jupiter.api.extension.ExtensionContext.Namespace.GLOBAL;

public class GraphQLSetupExtension implements BeforeAllCallback, ParameterResolver {

    private static final String KEY = "GLOBAL_GRAPHQL";

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        // Ensure GraphQL is initialized once in the root context
        ExtensionContext.Store store = context.getRoot().getStore(GLOBAL);
        store.getOrComputeIfAbsent(KEY, k -> {
            try {
                try (InputStreamReader reader = new InputStreamReader(
                        Objects.requireNonNull(GraphQLTest.class.getResourceAsStream("schema2.graphqls"))
                )) {
                    Repository repo = RepositoryFactory.getRepository();
                    Optional<GraphQL> _graphQL = Configurator.load(repo, reader, System.out);

                    if (_graphQL.isEmpty()) {
                        throw new RuntimeException("Failed to load configuration");
                    }

                    return new GraphQLResource(_graphQL.get());
                }
            } catch (Exception e) {
                throw new RuntimeException("Failed to initialize GraphQL", e);
            }
        });
    }

    private GraphQL getGraphQL(ExtensionContext context) {
        GraphQLResource resource = (GraphQLResource) context.getRoot()
                .getStore(GLOBAL)
                .get(KEY, GraphQLResource.class);
        return resource.graphQL;
    }

    @Override
    public boolean supportsParameter(
            ParameterContext parameterContext,
            ExtensionContext extensionContext
    ) {
        return parameterContext.getParameter().getType() == GraphQL.class;
    }

    @Override
    public Object resolveParameter(
            ParameterContext parameterContext,
            ExtensionContext extensionContext
    ) {
        return getGraphQL(extensionContext);
    }

    private static class GraphQLResource implements AutoCloseable {

        final GraphQL graphQL;

        GraphQLResource(GraphQL graphQL) {
            this.graphQL = graphQL;
        }

        @Override
        public void close() {
            // TODO Teardown?
        }
    }
}
