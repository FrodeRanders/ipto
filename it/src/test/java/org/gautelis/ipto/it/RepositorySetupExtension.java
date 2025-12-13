package org.gautelis.ipto.it;

import org.gautelis.ipto.repo.RepositoryFactory;
import org.gautelis.ipto.repo.model.Repository;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolver;

import java.util.Objects;

import static org.junit.jupiter.api.extension.ExtensionContext.Namespace.GLOBAL;

public class RepositorySetupExtension implements BeforeAllCallback, ParameterResolver {

    private static final String KEY = "GLOBAL_REPO";

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        // Ensure the Repository is initialized once in the root context
        ExtensionContext.Store store = context.getRoot().getStore(GLOBAL);
        store.computeIfAbsent(KEY, k -> {
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
        return Objects.requireNonNull(resource, "Lookup failed").repo;
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
