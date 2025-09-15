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

import org.gautelis.repo.model.utils.TimingData;
import org.gautelis.repo.search.query.DatabaseAdapter;

import javax.sql.DataSource;

/**
 *
 */
public final class Context {
    private final DataSource ds;
    private final Configuration cfg;
    private final Statements stmts;
    private final DatabaseAdapter adapter;
    private final TimingData timingData = new TimingData();

    public Context(
            DataSource ds, Configuration cfg, Statements stmts, DatabaseAdapter adapter
    ) {
        this.ds = ds;
        this.cfg = cfg;
        this.stmts = stmts;
        this.adapter = adapter;
    }

    public DataSource getDataSource() {
        return ds;
    }

    public Configuration getConfig() {
        return cfg;
    }

    public Statements getStatements() {
        return stmts;
    }

    public DatabaseAdapter getDatabaseAdapter() {
        return adapter;
    }

    public TimingData getTimingData() {
        return timingData;
    }
}
