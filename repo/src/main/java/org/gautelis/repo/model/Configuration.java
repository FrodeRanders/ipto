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
package org.gautelis.repo.model;

import org.gautelis.vopn.db.Database;
import org.gautelis.vopn.lang.Configurable;

public interface Configuration extends Database.Configuration {
    @Configurable(property = "repository.database.adapter", value = "org.gautelis.repo.db.postgresql.Adapter")
    String databaseAdapter();

    @Configurable(property = "repository.events.threshold", value = /* ActionEvent.Type.CREATED */ "60")
    int eventsThreshold();

    @Configurable(property = "repository.events.listeners", value = "org.gautelis.repo.listeners.LoggingActionListener")
    String[] eventsListeners();

    @Configurable(property = "repository.cache.look_behind", value = "true")
    boolean cacheLookBehind();

    @Configurable(property = "repository.cache.max_size", value = "1000")
    int cacheMaxSize();

    @Configurable(property = "repository.cache.idle_check_interval", value = "60")
    int cacheIdleCheckInterval();
}
