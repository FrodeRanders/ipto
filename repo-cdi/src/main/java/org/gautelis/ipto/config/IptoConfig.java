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
package org.gautelis.ipto.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

import java.util.List;

@ConfigMapping(prefix = "ipto")
public interface IptoConfig {
    Repository repository();

    Graphql graphql();

    interface Repository {
        @WithDefault("org.gautelis.ipto.repo.search.query.adapters.PostgresAdapter")
        String databaseAdapter();

        @WithDefault("1000")
        int eventsThreshold();

        @WithDefault("org.gautelis.ipto.repo.listeners.NopActionListener")
        List<String> eventsListeners();

        Cache cache();
    }

    interface Cache {
        @WithDefault("true")
        boolean lookBehind();

        @WithDefault("1000")
        int maxSize();

        @WithDefault("60")
        int idleCheckInterval();
    }

    interface Graphql {
        @WithDefault("")
        String sdlResource();
    }
}
