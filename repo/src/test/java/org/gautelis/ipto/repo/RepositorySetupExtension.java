package org.gautelis.ipto.repo;

import graphql.GraphQL;
import org.gautelis.ipto.repo.model.Repository;
import org.junit.jupiter.api.extension.*;

import static org.junit.jupiter.api.extension.ExtensionContext.Namespace.GLOBAL;

public class RepositorySetupExtension implements BeforeAllCallback, ParameterResolver {

    private static final String KEY = "GLOBAL_REPO";

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        // Ensure the Repository is initialized once in the root context
        ExtensionContext.Store store = context.getRoot().getStore(GLOBAL);
        store.getOrComputeIfAbsent(KEY, k -> {
            try {
                Repository repo = RepositoryFactory.getRepository();
                return new RepositoryResource(repo);

            } catch (Exception e) {
                throw new RuntimeException("Failed to initialize Repository", e);
            }
        });
    }

    private Repository getRepository(ExtensionContext context) {
        RepositoryResource resource = context.getRoot()
                .getStore(GLOBAL)
                .get(KEY, RepositoryResource.class);
        return resource.repo;
    }

    @Override
    public boolean supportsParameter(
            ParameterContext parameterContext,
            ExtensionContext extensionContext
    ) {
        return parameterContext.getParameter().getType() == Repository.class;
    }

    @Override
    public Object resolveParameter(
            ParameterContext parameterContext,
            ExtensionContext extensionContext
    ) {
        return getRepository(extensionContext);
    }

    private static class RepositoryResource implements AutoCloseable {

        final Repository repo;

        RepositoryResource(Repository repo) {
            this.repo = repo;
        }

        @Override
        public void close() {
            // TODO Teardown?
        }
    }
}
