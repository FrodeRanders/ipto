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
package org.gautelis.repo.search.query.postgresql;

import org.gautelis.repo.search.model.SearchItem;
import org.gautelis.repo.search.query.CommonAdapter;
import org.gautelis.repo.search.UnitSearch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Locale;

/**
 *
 */
public class PostgresAdapter extends CommonAdapter {
    protected static final Logger log = LoggerFactory.getLogger(PostgresAdapter.class);

    public static String POSTGRES_TIME_PATTERN = "YYYY-MM-DD HH24:MI:SS.MS";

    public PostgresAdapter() {
    }

    private static final DateTimeFormatter PG_FMT =
            DateTimeFormatter.ofPattern(INSTANT_TIME_PATTERN)
                    .withLocale(Locale.ENGLISH)
                    .withZone(ZoneOffset.UTC);   // or your zone

    @Override
    public String asTimeLiteral(String timeStr) {
        //--------------------------------------------------
        // 'timeStr' format should match result
        //    org.gautelis.repo.search.query.CommonAdapter.INSTANT_TIME_PATTERN
        //--------------------------------------------------
        return "TO_TIMESTAMP('" +
                timeStr.replace('\'', ' ') +
                "', '" + POSTGRES_TIME_PATTERN + "')";
    }

    public String asTimeLiteral(Instant instant) {
        return "TO_TIMESTAMP('" +
                PG_FMT.format(instant) +
                "', '" + POSTGRES_TIME_PATTERN + "')";
    }

    protected GeneratedStatement generateStatement(
            UnitSearch sd
    ) throws IllegalArgumentException {
        GeneratedStatement generatedStatement = super.generateStatement(sd);
        StringBuilder buf = new StringBuilder(generatedStatement.statement());

        // Paging and/or limiting search results
        int pageOffset = sd.getPageOffset();
        if (pageOffset > 0) {
            buf.append("OFFSET ").append(pageOffset).append(" ROWS ");

            int pageSize = sd.getPageSize();
            if (pageSize > 0) {
                buf.append("FETCH NEXT ").append(pageSize).append(" ROWS ONLY "); // ANSI:ism
            }
        } else {
            int selectionSize = sd.getSelectionSize();
            if (selectionSize > 0) {
                buf.append("LIMIT ").append(selectionSize);
            }
        }

        return new GeneratedStatement(buf.toString(), generatedStatement.preparedItems(), generatedStatement.commonConstraintValues());
    }
}
