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
package org.gautelis.ipto.it;

import graphql.GraphQL;
import graphql.schema.DataFetcher;
import org.gautelis.ipto.graphql.configuration.OperationsWireParameters;
import org.gautelis.ipto.graphql.model.Query;
import org.gautelis.ipto.repo.model.Repository;
import org.gautelis.ipto.repo.RepositoryFactory;
import org.gautelis.ipto.graphql.configuration.Configurator;
import org.junit.jupiter.api.extension.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStreamReader;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static org.gautelis.ipto.graphql.runtime.RuntimeService.headHex;
import static org.gautelis.ipto.graphql.runtime.RuntimeOperators.MAPPER;

public class IptoSetupExtension implements BeforeAllCallback, ParameterResolver {
    private static final Logger log = LoggerFactory.getLogger(IptoSetupExtension.class);

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
                    Optional<GraphQL> g = Configurator.load(repo, reader, IptoEnvironment::wireOperations, System.out);
                    if (g.isEmpty()) {
                        throw new IllegalStateException("Failed to load GraphQL configuration");
                    }
                    this.graphQL = g.get();

                    repo.sync(); // see note in Configurator::load
                }

            } catch (Exception e) {
                throw new RuntimeException("Failed to initialize IPTO test environment", e);
            }
        }

        @Override
        public void close() {
        }

        private static void wireOperations(OperationsWireParameters params) {
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
                        log.trace("↩ {}::{}({}) : {}", type, operationName, env.getArguments(), outputType);
                    }

                    Query.UnitIdentification id = MAPPER.convertValue(env.getArgument(parameterName), Query.UnitIdentification.class);
                    return params.runtimeService().loadRawUnit(id.tenantId(), id.unitId());
                };

                params.runtimeWiring().type(type, t -> t.dataFetcher(operationName, rawUnitById));
                log.info("↯ Wiring: {}::{}(...) : {}", type, operationName, outputType);
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
                        log.trace("↩ {}::{}({}) : {}", type, operationName, env.getArguments(), outputType);
                    }

                    Query.UnitIdentification id = MAPPER.convertValue(env.getArgument(parameterName), Query.UnitIdentification.class);
                    return params.runtimeService().loadUnit(id.tenantId(), id.unitId());
                };

                params.runtimeWiring().type(type, t -> t.dataFetcher(operationName, unitById));
                log.info("↯ Wiring: {}::{}(...) : {}", type, operationName, outputType);
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
                        log.trace("↩ {}::{}({}) : {}", type, operationName, env.getArguments(), outputType);
                    }

                    Query.Filter filter = MAPPER.convertValue(env.getArgument(parameterName), Query.Filter.class);

                    return params.runtimeService().searchRaw(filter);
                };

                params.runtimeWiring().type(type, t -> t.dataFetcher(operationName, rawUnitsByFilter));
                log.info("↯ Wiring: {}::{}(...) : {}", type, operationName, outputType);
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
                        log.trace("↩ {}::{}({}) : {}", type, operationName, env.getArguments(), outputType);
                    }

                    Query.Filter filter = MAPPER.convertValue(env.getArgument(parameterName), Query.Filter.class);

                    return params.runtimeService().search(filter);
                };

                params.runtimeWiring().type(type, t -> t.dataFetcher(operationName, unitsByFilter));
                log.info("↯ Wiring: {}::{}(...) : {}", type, operationName, outputType);
            }

            // Mutations::lagraUnitRaw(data : Bytes!) : Bytes
            {
                String type = "Mutation";
                String operationName = "lagraUnitRaw";
                String parameterName = "data";
                String outputType = "Bytes";

                DataFetcher<?> storeJson = env -> {
                    //**** Executed at runtime **********************************
                    // This closure captures its environment, so at runtime
                    // the wiring preamble will be available.
                    //***********************************************************

                    Map<String, Object> args = env.getArguments();
                    byte[] bytes = (byte[]) args.get("data"); // Connection to schema

                    if (log.isTraceEnabled()) {
                        log.trace("↩ {}::{}({}) : {}", type, operationName, headHex(bytes, 16), outputType);
                    }

                    return params.runtimeService().storeRawUnit(bytes);
                };

                params.runtimeWiring().type(type, t -> t.dataFetcher(operationName, storeJson));
                log.info("↯ Wiring: {}::{}(...) : {}", type, operationName, outputType);
            }
        }
    }
}
