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
package org.gautelis.repo.search.query;

import org.gautelis.repo.db.Adapter;
import org.gautelis.repo.db.Column;
import org.gautelis.repo.model.attributes.Value;
import org.gautelis.repo.model.utils.TimingData;
import org.gautelis.repo.search.UnitSearch;
import org.gautelis.repo.search.model.*;
import org.gautelis.repo.utils.CheckedConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Database adapter base class implementation.
 */
public abstract class DatabaseAdapter implements Adapter {
    private static final Logger log = LoggerFactory.getLogger(DatabaseAdapter.class);

    public DatabaseAdapter() {}

    public abstract String getTimePattern();

    public abstract String asTimeLiteral(String timeStr);

    public abstract String asTimeLiteral(Instant instant);

    //---------------------------------------------------------------
    // GENERIC STRUCTURAL
    //---------------------------------------------------------------
    public abstract void search(
            Connection conn,
            UnitSearch sd,
            TimingData timimgData,
            CheckedConsumer<ResultSet> rsBlock
    ) throws IllegalArgumentException;

    //---------------------------------------------------------------
    // DBMS SPECIFIC GENERATORS
    //---------------------------------------------------------------
    abstract protected SearchExpression optimize(
            SearchExpression sex
    );

    public record GeneratedStatement(
            String statement,
            Collection<SearchItem<?>> preparedItems,
            Map<String, SearchItem<?>> commonConstraintValues
    ) {}

    protected abstract GeneratedStatement generateStatement(
            UnitSearch sd
    );
}
