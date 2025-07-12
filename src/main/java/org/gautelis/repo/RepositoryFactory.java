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

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.gautelis.repo.exceptions.ConfigurationException;
import org.gautelis.repo.listeners.ActionListener;
import org.gautelis.repo.model.Configuration;
import org.gautelis.repo.model.Context;
import org.gautelis.repo.model.Repository;
import org.gautelis.repo.model.Statements;
import org.gautelis.repo.search.query.DatabaseAdapter;
import org.gautelis.repo.utils.PluginsHelper;
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

    private static Configuration getConfiguration() throws IOException {
        return getConfiguration(ConfigurationTool.loadFromResource(RepositoryFactory.class, "configuration.xml"));
    }

    public static <T extends DataSource> Repository getRepository(Database.DataSourcePreparation<T> dataSourcePreparer) {
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
            config = getConfiguration();
        }
        catch (IOException ioe) {
            String info = "Failed to load configuration: " + ioe.getMessage();
            log.error(info, ioe);
            throw new ConfigurationException(info, ioe);
        }
        int eventThreshold = config.eventsThreshold();

        /*
        DataSource dataSource;
        try {
            dataSource = Database.getDataSource(config, dataSourcePreparer);
            prepareDatabase(config.manager(), dataSource);
        }
        catch (DatabaseException dbe) {
            String info = "Failed to load data source according to configuration: " + dbe.getMessage();
            log.error(info, dbe);
            throw new ConfigurationException(info, dbe);
        }
        */

        HikariConfig hConfig = new HikariConfig();
        String url = "jdbc:" + config.manager() + "://" + config.server() + ":" + config.port() +"/" + config.database();
        hConfig.setJdbcUrl(url);
        hConfig.setUsername(config.user());
        hConfig.setPassword(config.password());
        hConfig.setMaximumPoolSize(20);
        HikariDataSource dataSource = new HikariDataSource(hConfig);

        // For now -- TODO Fix Configurable to allow collection return values
        Collection<String> eventListeners = new ArrayList<>();
        String _eventListener = config.eventsListeners(); // for now
        eventListeners.add(_eventListener);

        // Setup event listeners
        Map<String, ActionListener> actionListeners = new HashMap<>();
        for (String eventListener : eventListeners) {
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

    public static Repository getRepository() {
        return getRepository(new PostgresDataSourcePreparer());
    }

    /*
     * Helper functionality
     */
    public static void prepareDatabase(String dbm, DataSource dataSource) throws ConfigurationException {
        Objects.requireNonNull(dbm, "dbm");
        Objects.requireNonNull(dataSource, "dataSource");

        try {
            Options options = Options.getDefault();
            options.debug = DEBUG_DATABASE_SETUP;
            Manager mngr = new PostgreSQL(dataSource, options);

            prepare("schema.sql", dbm, mngr, new PrintWriter(System.out));
            prepare("boot.sql", dbm, mngr, new PrintWriter(System.out));

        } catch (Throwable t) {
            String info = "Failed to prepare internal database: ";
            info += t.getMessage();
            log.error(info, t);
            throw new ConfigurationException(info, t);
        }
    }

    /**
     * Prepare the database by running script.
     * <p>
     *
     * @throws Exception if fails to load configuration or fails to create database objects
     */
    private static void prepare(String filename, String dbm, Manager manager, PrintWriter out) throws Exception {
        Objects.requireNonNull(filename, "fileName");
        Objects.requireNonNull(dbm, "dbm");
        Objects.requireNonNull(manager, "manager");
        Objects.requireNonNull(out, "out");

        String workingDirectory = System.getProperty("user.dir");
        File file = Path.of(workingDirectory, "db", dbm, filename).toFile();
        if (file.exists()) {
            log.info("Sourcing {}", file.getAbsolutePath());
            try (InputStreamReader reader = new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8)) {
                manager.execute(filename, reader, out);
            }
        } else {
            try (InputStream is = RepositoryFactory.class.getResourceAsStream(filename)) {
                log.info("Sourcing bundled {}", filename);

                if (null == is) {
                    throw new ConfigurationException("No bundled file " + filename + " found");
                }
                manager.execute(filename, new InputStreamReader(is), out);
            }
        }
    }
}
