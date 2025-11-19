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
package org.gautelis.repo.search.query.adapters;

import org.gautelis.repo.db.Database;
import org.gautelis.repo.search.UnitSearch;
import org.gautelis.repo.search.query.CommonAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

/**
 *
 */
public class DB2Adapter extends CommonAdapter {
    protected static final Logger log = LoggerFactory.getLogger(DB2Adapter.class);

    //
    public static final String DB2_TIME_PATTERN = "YYYY-MM-DD HH24:MI:SS.FF3";

    public DB2Adapter() {
    }

    public boolean useClob() {
        return true;
    }

    private static final DateTimeFormatter DB2_FMT =
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
                "', '" + DB2_TIME_PATTERN + "')";
    }

    public String asTimeLiteral(Instant instant) {
        return "TO_TIMESTAMP('" +
                DB2_FMT.format(instant) +
                "', '" + DB2_TIME_PATTERN + "')";
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

    public String getDbVersion(DataSource dataSource) {
        String sql = """
                SELECT service_level || ' FP' || fixpack_num AS db2_version
                FROM SYSIBMADM.ENV_INST_INFO;
                """;
        String[] version = {"unknown"};
        Database.useReadonlyStatement(dataSource, sql, rs -> {
            if (rs.next()) {
                version[0] = rs.getString("db2_version");
            }
        });
        return version[0];
    }
}
