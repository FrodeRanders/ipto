package org.gautelis.ipto.it;

import graphql.GraphQL;
import org.gautelis.ipto.repo.model.Repository;
import org.gautelis.ipto.repo.RepositoryFactory;
import org.gautelis.ipto.graphql.configuration.Configurator;
import org.junit.jupiter.api.extension.*;

import java.io.InputStreamReader;
import java.util.Objects;
import java.util.Optional;

public class IptoSetupExtension implements BeforeAllCallback, ParameterResolver {

    private static final ExtensionContext.Namespace GLOBAL =
            ExtensionContext.Namespace.create("org.gautelis.ipto", "it");

    private static final String KEY = "GLOBAL_IPTO_ENV";
    private static final String CONFIG = "schema2.graphqls";

    @Override
    public void beforeAll(ExtensionContext context) {
        // Initialize once per test run (per fork/JVM) in the root store.
        context.getRoot()
                .getStore(GLOBAL)
                .computeIfAbsent(KEY, k -> new IptoEnvironment(), IptoEnvironment.class);
    }

    private IptoEnvironment environment(ExtensionContext context) {
        IptoEnvironment env = context.getRoot().getStore(GLOBAL).get(KEY, IptoEnvironment.class);
        return Objects.requireNonNull(env, "Lookup failed");
    }

    @Override
    public boolean supportsParameter(ParameterContext pc, ExtensionContext ec) {
        Class<?> t = pc.getParameter().getType();
        return t == Repository.class || t == GraphQL.class;
    }

    @Override
    public Object resolveParameter(ParameterContext pc, ExtensionContext ec) {
        Class<?> t = pc.getParameter().getType();
        IptoEnvironment env = environment(ec);

        if (t == Repository.class) return env.repo;
        if (t == GraphQL.class) return env.graphQL;

        throw new ParameterResolutionException("Unsupported parameter: " + t.getName());
    }

    /**
     * Root-scoped environment. Created once; closed once.
     */
    static final class IptoEnvironment implements AutoCloseable {

        final Repository repo;
        final GraphQL graphQL;

        IptoEnvironment() {
            try {
                // First initialize Repository (which relies on bundled configuration configuration.xml)
                this.repo = RepositoryFactory.getRepository();

                // Next, load GraphQL config (which registers attributes/templates)
                try (InputStreamReader reader = new InputStreamReader(
                        Objects.requireNonNull(
                                IptoSetupExtension.class.getResourceAsStream(CONFIG),
                                CONFIG + " not found on classpath next to IptoSetupExtension"
                        )
                )) {
                    Optional<GraphQL> g = Configurator.load(repo, reader, System.out);
                    if (g.isEmpty()) {
                        throw new IllegalStateException("Failed to load GraphQL configuration");
                    }
                    this.graphQL = g.get();
                }

            } catch (Exception e) {
                throw new RuntimeException("Failed to initialize IPTO test environment", e);
            }
        }

        @Override
        public void close() {
        }
    }
}
