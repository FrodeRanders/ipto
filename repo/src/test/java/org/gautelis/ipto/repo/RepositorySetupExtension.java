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
package org.gautelis.ipto.repo;

import org.gautelis.ipto.repo.model.Repository;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolver;

import java.util.Objects;

import static org.junit.jupiter.api.extension.ExtensionContext.Namespace.GLOBAL;

/**
 * Annotate test classes with
 *   @ExtendWith(RepositorySetupExtension.class)
 */
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
