/*
 * Copyright (C) 2024-2026 Frode Randers
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

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.gautelis.ipto.repo.exceptions.ConfigurationException;
import org.gautelis.ipto.repo.listeners.ActionListener;
import org.gautelis.ipto.repo.model.Configuration;
import org.gautelis.ipto.repo.model.Context;
import org.gautelis.ipto.repo.model.Repository;
import org.gautelis.ipto.repo.model.Statements;
import org.gautelis.ipto.repo.search.query.DatabaseAdapter;
import org.gautelis.ipto.repo.utils.PluginsHelper;
import org.gautelis.vopn.db.Database;
import org.gautelis.vopn.db.utils.PostgreSQL;
import org.gautelis.vopn.db.utils.Manager;
import org.gautelis.vopn.db.utils.Options;
import org.gautelis.vopn.lang.ConfigurationTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.*;


public class RepositoryFactory {
    private static final Logger log = LoggerFactory.getLogger(RepositoryFactory.class);
    private static final Logger statistics = LoggerFactory.getLogger("STATISTICS");

    private static final boolean DEBUG_DATABASE_SETUP = false;



    private static Repository repo;

    private RepositoryFactory() {}

    private static Statements getStatements(Properties properties) {
        return ConfigurationTool.bindProperties(Statements.class, properties);
    }

    private static Statements getStatements() throws IOException  {
        return getStatements(ConfigurationTool.loadFromResource(RepositoryFactory.class, "sql-statements.xml"));
    }

    private static Configuration getConfiguration(Properties properties) {
        return ConfigurationTool.bindProperties(Configuration.class, properties);
    }

    private static Configuration getConfiguration(String configurationFile) throws IOException {
        return getConfiguration(ConfigurationTool.loadFromResource(RepositoryFactory.class, configurationFile));
    }

    public static Repository getRepository() {
        if (null != repo) {
            return repo;
        }

        log.info("*** Booting repository service ***");

        Statements statements;
        try {
            statements = getStatements();
        }
        catch (IOException ioe) {
            String info = "Failed to load configured SQL statements: " + ioe.getMessage();
            log.error(info, ioe);
            throw new ConfigurationException(info, ioe);
        }

        //
        Configuration config;
        try {
            String dbProvider = System.getenv("IPTO_DB_PROVIDER");
            if (null == dbProvider || dbProvider.isEmpty()) {
                config = getConfiguration("configuration_pg.xml");
            } else {
                config = switch (dbProvider) {
                    case "DB2" -> getConfiguration("configuration_db2.xml");
                    case "PG" -> getConfiguration("configuration_pg.xml");
                    default -> getConfiguration("configuration_pg.xml");
                };
            }
        }
        catch (IOException ioe) {
            String info = "Failed to load configuration: " + ioe.getMessage();
            log.error(info, ioe);
            throw new ConfigurationException(info, ioe);
        }
        int eventThreshold = config.eventsThreshold();

        // Database
        HikariConfig hConfig = new HikariConfig();
        String url = config.url();
        if (null == url || url.isEmpty()) {
            url = "jdbc:" + config.manager() + "://" + config.server() + ":" + config.port() +"/" + config.database();
            log.info("Synthesizing a JDBC URL: " + url);
        }
        hConfig.setJdbcUrl(url);
        String user = config.user();
        if (null != user && !user.isEmpty()) {
            hConfig.setUsername(user);
        }
        String password = config.password();
        if (null != password && !password.isEmpty()) {
            hConfig.setPassword(password);
        }
        hConfig.setMaximumPoolSize(config.maxActive());
        HikariDataSource dataSource = new HikariDataSource(hConfig);

        //
        Collection<String> eventListeners = new ArrayList<>();
        Collections.addAll(eventListeners, config.eventsListeners());

        // Setup event listeners
        Map<String, ActionListener> actionListeners = new HashMap<>();
        eventListeners.forEach(eventListener -> {
            if (actionListeners.containsKey(eventListener)) {
                log.warn("Event listener {} already registered, skipping", eventListener);
                return;
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
        });

        // Setup database adapter
        DatabaseAdapter searchAdapter;
        Optional<DatabaseAdapter> dbAdapter = PluginsHelper.getPlugin(config.databaseAdapter(), DatabaseAdapter.class);
        if (dbAdapter.isPresent()) {
            searchAdapter = dbAdapter.get();

        } else {
            log.error("Failed to instantiate database adapter {}", config.databaseAdapter());
            throw new ConfigurationException("Failed to instantiate database adapter");
        }

        //
        Context context = new Context(dataSource, config, statements, searchAdapter);
        repo = new Repository(context, eventThreshold, actionListeners);
        return repo;
    }
}
