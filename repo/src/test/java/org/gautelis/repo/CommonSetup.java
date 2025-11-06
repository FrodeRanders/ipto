/*
 * Copyright (C) 2024-2025 Frode Randers
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
package org.gautelis.repo;

import graphql.GraphQL;
import org.gautelis.repo.model.Repository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Objects;
import java.util.Optional;

/**
 *
 */
public class CommonSetup {
    private static final Logger log = LoggerFactory.getLogger(CommonSetup.class);

    private static GraphQL graphQL = null;

    public static GraphQL setUp() throws IOException {
        if (null != graphQL) {
            return graphQL;
        }

        Optional<GraphQL> _graphQL;
        try (InputStreamReader sdl = new InputStreamReader(
                Objects.requireNonNull(CommonSetup.class.getResourceAsStream("unit-schema.graphqls"))
        )) {
            Repository repo = RepositoryFactory.getRepository();
            _graphQL = repo.loadConfiguration(sdl);
            if (_graphQL.isEmpty()) {
                throw new RuntimeException("Failed to load configuration");
            }
        }

        graphQL = _graphQL.get();
        return graphQL;
    }
}
