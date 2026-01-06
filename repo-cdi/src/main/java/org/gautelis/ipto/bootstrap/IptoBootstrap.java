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
package org.gautelis.ipto.bootstrap;

import graphql.GraphQL;
import graphql.schema.DataFetcher;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.gautelis.ipto.config.IptoConfig;
import org.gautelis.ipto.graphql.configuration.Configurator;
import org.gautelis.ipto.graphql.configuration.OperationsWireParameters;
import org.gautelis.ipto.graphql.model.Query;
import org.gautelis.ipto.repo.model.Repository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.quarkus.runtime.Startup;

import java.io.InputStreamReader;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;

import static org.gautelis.ipto.graphql.runtime.RuntimeOperators.MAPPER;
import static org.gautelis.ipto.graphql.runtime.RuntimeService.headHex;

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
            //Consumer<OperationsWireParameters> operationsWireBlock = null;
            //if (!wiring.isUnsatisfied()) {
            //    operationsWireBlock = params -> wiring.forEach(w -> w.wire(params));
            //}
            //
            //Optional<GraphQL> gql = Configurator.load(repository, reader, operationsWireBlock, System.out);

            Optional<GraphQL> gql = Configurator.load(repository, reader, IptoBootstrap::wireOperations, System.out);
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

    static void wireOperations(OperationsWireParameters params) {
        // Query::yrkanRaw(id : UnitIdentification!) : Bytes
        {
            String type = "Query";
            String operationName = "yrkanRaw";
            String parameterName = "id";
            String outputType = "Bytes";

            DataFetcher<?> rawUnitById = env -> {
                //**** Executed at runtime **********************************
                // This closure captures its environment, so at runtime
                // the wiring preamble will be available.
                //***********************************************************
                if (log.isTraceEnabled()) {
                    log.trace("\u21a9 {}::{}({}) : {}", type, operationName, env.getArguments(), outputType);
                }

                Query.UnitIdentification id = MAPPER.convertValue(env.getArgument(parameterName), Query.UnitIdentification.class);
                return params.runtimeService().loadRawUnit(id.tenantId(), id.unitId());
            };

            params.runtimeWiring().type(type, t -> t.dataFetcher(operationName, rawUnitById));
            log.info("\u21af Wiring: {}::{}(...) : {}", type, operationName, outputType);
        }

        // Query::yrkan(id : UnitIdentification!) : Yrkan
        {
            String type = "Query";
            String operationName = "yrkan";
            String parameterName = "id";
            String outputType = "Yrkan";

            DataFetcher<?> unitById = env -> {
                //**** Executed at runtime **********************************
                // This closure captures its environment, so at runtime
                // the wiring preamble will be available.
                //***********************************************************
                if (log.isTraceEnabled()) {
                    log.trace("\u21a9 {}::{}({}) : {}", type, operationName, env.getArguments(), outputType);
                }

                Query.UnitIdentification id = MAPPER.convertValue(env.getArgument(parameterName), Query.UnitIdentification.class);
                return params.runtimeService().loadUnit(id.tenantId(), id.unitId());
            };

            params.runtimeWiring().type(type, t -> t.dataFetcher(operationName, unitById));
            log.info("\u21af Wiring: {}::{}(...) : {}", type, operationName, outputType);
        }

        // Query::yrkandenRaw(filter: Filter!) : Bytes
        {
            String type = "Query";
            String operationName = "yrkandenRaw";
            String parameterName = "filter";
            String outputType = "Bytes";

            DataFetcher<?> rawUnitsByFilter = env -> {
                //**** Executed at runtime **********************************
                // This closure captures its environment, so at runtime
                // the wiring preamble will be available.
                //***********************************************************

                if (log.isTraceEnabled()) {
                    log.trace("\u21a9 {}::{}({}) : {}", type, operationName, env.getArguments(), outputType);
                }

                Query.Filter filter = MAPPER.convertValue(env.getArgument(parameterName), Query.Filter.class);

                return params.runtimeService().searchRaw(filter);
            };

            params.runtimeWiring().type(type, t -> t.dataFetcher(operationName, rawUnitsByFilter));
            log.info("\u21af Wiring: {}::{}(...) : {}", type, operationName, outputType);
        }

        // Query::yrkanden(filter: Filter!) : [Yrkan]
        {
            String type = "Query";
            String operationName = "yrkanden";
            String parameterName = "filter";
            String outputType = "[Yrkan]";

            DataFetcher<?> unitsByFilter = env -> {
                //**** Executed at runtime **********************************
                // This closure captures its environment, so at runtime
                // the wiring preamble will be available.
                //***********************************************************
                if (log.isTraceEnabled()) {
                    log.trace("\u21a9 {}::{}({}) : {}", type, operationName, env.getArguments(), outputType);
                }

                Query.Filter filter = MAPPER.convertValue(env.getArgument(parameterName), Query.Filter.class);

                return params.runtimeService().search(filter);
            };

            params.runtimeWiring().type(type, t -> t.dataFetcher(operationName, unitsByFilter));
            log.info("\u21af Wiring: {}::{}(...) : {}", type, operationName, outputType);
        }

        // Mutations::lagraUnitRaw(data : Bytes!) : Dataleverans
        {
            String type = "Mutation";
            String operationName = "lagraUnitRaw";
            String parameterName = "data";
            String outputType = "Dataleverans";

            DataFetcher<?> storeJson = env -> {
                //**** Executed at runtime **********************************
                // This closure captures its environment, so at runtime
                // the wiring preamble will be available.
                //***********************************************************

                Map<String, Object> args = env.getArguments();
                byte[] bytes = (byte[]) args.get("data"); // Connection to schema

                if (log.isTraceEnabled()) {
                    log.trace("\u21a9 {}::{}({}) : {}", type, operationName, headHex(bytes, 16), outputType);
                }

                return params.runtimeService().storeRawUnit(bytes);
            };

            params.runtimeWiring().type(type, t -> t.dataFetcher(operationName, storeJson));
            log.info("\u21af Wiring: {}::{}(...) : {}", type, operationName, outputType);
        }
    }
}
