package org.gautelis.ipto.bootstrap;

import graphql.GraphQL;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.gautelis.ipto.config.IptoConfig;
import org.gautelis.ipto.graphql.configuration.Configurator;
import org.gautelis.ipto.graphql.configuration.OperationsWireParameters;
import org.gautelis.ipto.repo.model.Repository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.quarkus.runtime.Startup;

import java.io.InputStreamReader;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;

@Startup
@ApplicationScoped
public class IptoBootstrap {
    private static final Logger log = LoggerFactory.getLogger(IptoBootstrap.class);

    private final Repository repository;
    private final IptoConfig config;
    private final Instance<IptoOperationsWiring> wiring;

    private volatile GraphQL graphQL;

    @Inject
    public IptoBootstrap(
            Repository repository,
            IptoConfig config,
            Instance<IptoOperationsWiring> wiring
    ) {
        this.repository = repository;
        this.config = config;
        this.wiring = wiring;
    }

    @PostConstruct
    void init() {
        String sdlResource = config.graphql().sdlResource();
        if (sdlResource == null || sdlResource.isBlank()) {
            log.info("No GraphQL SDL configured; skipping GraphQL bootstrap");
            return;
        }

        log.info("*** Bootstrapping IPTO from {} ***", sdlResource);

        try (InputStreamReader reader = new InputStreamReader(
                Objects.requireNonNull(
                        IptoBootstrap.class.getResourceAsStream(sdlResource),
                        sdlResource + " not found on classpath"
                )
        )) {
            Consumer<OperationsWireParameters> operationsWireBlock = null;
            if (!wiring.isUnsatisfied()) {
                operationsWireBlock = params -> wiring.forEach(w -> w.wire(params));
            }

            Optional<GraphQL> gql = Configurator.load(repository, reader, operationsWireBlock, System.out);
            if (gql.isEmpty()) {
                throw new IllegalStateException("Failed to load GraphQL configuration");
            }
            this.graphQL = gql.get();

            repository.sync(); // see note in Configurator::load

        } catch (Exception e) {
            throw new RuntimeException("Failed to bootstrap IPTO", e);
        }
    }

    public GraphQL graphQL() {
        return graphQL;
    }
}
