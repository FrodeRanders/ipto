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

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import org.gautelis.ipto.bootstrap.IptoBootstrap;
import org.gautelis.ipto.repo.RepositoryFactory;
import org.gautelis.ipto.repo.exceptions.ConfigurationException;
import org.gautelis.ipto.repo.listeners.ActionListener;
import org.gautelis.ipto.repo.model.Configuration;
import org.gautelis.ipto.repo.model.Context;
import org.gautelis.ipto.repo.model.Repository;
import org.gautelis.ipto.repo.model.Statements;
import org.gautelis.ipto.repo.search.query.DatabaseAdapter;
import org.gautelis.ipto.repo.utils.PluginsHelper;
import org.gautelis.vopn.lang.ConfigurationTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.util.*;

@ApplicationScoped
public class IptoProducers {
    private static final Logger log = LoggerFactory.getLogger(IptoProducers.class);

    @Produces
    @ApplicationScoped
    public Statements statements() {
        try {
            return ConfigurationTool.bindProperties(
                    Statements.class,
                    ConfigurationTool.loadFromResource(RepositoryFactory.class, "sql-statements.xml")
            );
        } catch (IOException ioe) {
            String info = "Failed to load configured SQL statements: " + ioe.getMessage();
            throw new ConfigurationException(info, ioe);
        }
    }

    @Produces
    @ApplicationScoped
    public Configuration configuration(IptoConfig config) {
        Properties properties = new Properties();
        properties.setProperty("repository.database.adapter",
                config.repository().databaseAdapter());
        properties.setProperty("repository.events.threshold",
                String.valueOf(config.repository().eventsThreshold()));
        properties.setProperty("repository.events.listeners",
                String.join(",", config.repository().eventsListeners()));
        properties.setProperty("repository.cache.look_behind",
                String.valueOf(config.repository().cache().lookBehind()));
        properties.setProperty("repository.cache.max_size",
                String.valueOf(config.repository().cache().maxSize()));
        properties.setProperty(
                "repository.cache.idle_check_interval",
                String.valueOf(config.repository().cache().idleCheckInterval())
        );

        return ConfigurationTool.bindProperties(Configuration.class, properties);
    }

    @Produces
    @ApplicationScoped
    public DatabaseAdapter databaseAdapter(Configuration config) {
        Optional<DatabaseAdapter> dbAdapter = PluginsHelper.getPlugin(
                config.databaseAdapter(),
                DatabaseAdapter.class
        );
        if (dbAdapter.isPresent()) {
            return dbAdapter.get();
        }
        throw new ConfigurationException("Failed to instantiate database adapter");
    }

    @Produces
    @ApplicationScoped
    public Map<String, ActionListener> actionListeners(
            Configuration config,
            DataSource dataSource
    ) {
        Map<String, ActionListener> actionListeners = new HashMap<>();
        for (String eventListener : config.eventsListeners()) {
            if (actionListeners.containsKey(eventListener)) {
                log.warn("Event listener {} already registered, skipping", eventListener);
                continue;
            }
            Optional<ActionListener> listener = PluginsHelper.getPlugin(eventListener, ActionListener.class);
            if (listener.isPresent()) {
                ActionListener actionListener = listener.get();
                actionListener.initialize(dataSource);
                actionListeners.put(eventListener, actionListener);
                log.info("Initiated listener: {}", actionListener.getClass().getCanonicalName());

            } else {
                log.error("Failed to instantiate event listener {}", eventListener);
            }
        }

        return actionListeners;
    }

    @Produces
    @ApplicationScoped
    public Context context(
            DataSource dataSource,
            Configuration config,
            Statements statements,
            DatabaseAdapter adapter
    ) {
        return new Context(dataSource, config, statements, adapter);
    }

    @Produces
    @ApplicationScoped
    public Repository repository(
            Context context,
            Configuration config,
            Map<String, ActionListener> actionListeners
    ) {
        return new Repository(context, config.eventsThreshold(), actionListeners);
    }

    @Produces
    @ApplicationScoped
    public graphql.GraphQL graphQL(IptoBootstrap bootstrap) {
        graphql.GraphQL graphQL = bootstrap.graphQL();
        if (graphQL == null) {
            throw new IllegalStateException("GraphQL has not been initialized; check the ipto.graphql sdl-resource");
        }
        return graphQL;
    }
}
