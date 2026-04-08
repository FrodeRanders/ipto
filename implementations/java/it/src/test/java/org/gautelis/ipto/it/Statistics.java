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
package org.gautelis.ipto.it;

import com.sun.management.OperatingSystemMXBean;
import org.gautelis.ipto.repo.db.Database;
import org.gautelis.ipto.repo.db.Table;
import org.gautelis.ipto.repo.model.Repository;
import org.gautelis.ipto.repo.search.query.adapters.PostgresAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Locale;
import java.util.stream.IntStream;

import static org.gautelis.vopn.lang.Number.asHumanApproximate;

public class Statistics {
    private static final Logger statistics = LoggerFactory.getLogger("STATISTICS");

    public static void dumpStatistics(Repository repo) {
        StringBuilder buf = new StringBuilder("\n===================================================================================================\n");
        Runtime runtime = Runtime.getRuntime();
        buf.append("OS: ")
                .append(System.getProperty("os.name")).append(" ")
                .append(System.getProperty("os.version")).append(" (")
                .append(System.getProperty("os.arch")).append(") ")
                .append("with ").append(runtime.availableProcessors()).append(" cores (physical and/or hyperthreaded)\n");

        OperatingSystemMXBean osBean =
                (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
        buf.append("Phys memory: ")
                .append("total=").append(asHumanApproximate(osBean.getTotalMemorySize(), " ").replaceAll("\\u00a0"," ")).append(" ")
                .append("free=").append(asHumanApproximate(osBean.getFreeMemorySize(), " ").replaceAll("\\u00a0"," ")).append("\n");

        buf.append("JVM: ")
                .append("vm=").append(System.getProperty("java.vm.name")).append(" (").append(System.getProperty("java.vm.version")).append(") ")
                .append("mem-total=").append(asHumanApproximate(runtime.totalMemory(), " ").replaceAll("\\u00a0"," ")).append(" ")
                .append("mem-free=").append(asHumanApproximate(runtime.freeMemory(), " ").replaceAll("\\u00a0"," ")).append("\n");

        repo.withDataSource(dataSource -> {
            /*
             * DB server version
             */
            buf.append("DB: ")
                    .append(repo.getDatabaseAdapter().getDbVersion(dataSource)).append("\n");
            buf.append("\n");

            /*
             * Count all units
             */
            String sql = "SELECT COUNT(*) FROM " + Table.UNIT_KERNEL.getTableName();

            Database.useReadonlyStatement(dataSource, sql, rs -> {
                if (rs.next()) {
                    buf.append("Units: total=").append(rs.getLong(1)).append(" ");
                }
            });

            /*
             * Number of locks
             */
            Database.useReadonlyStatement(dataSource, "SELECT COUNT(*) FROM " + Table.UNIT_LOCK.getTableName(), rs -> {
                if (rs.next()) {
                    buf.append("locks=").append(rs.getLong(1)).append(" ");
                }
            });

            /*
             * Number of mappings from units to attribute values
             */
            Database.useReadonlyStatement(dataSource, "SELECT COUNT(*) FROM " + Table.ATTRIBUTE_VALUE.getTableName(), rs -> {
                if (rs.next()) {
                    buf.append("vectors=").append(rs.getLong(1)).append("\n");
                }
            });

            buf.append("Values: ");

            /*
             * Number of string values
             */
            Database.useReadonlyStatement(dataSource, "SELECT COUNT(*) FROM " + Table.ATTRIBUTE_STRING_VALUE_VECTOR.getTableName(), rs -> {
                if (rs.next()) {
                    buf.append("string=").append(rs.getLong(1)).append(" ");
                }
            });

            /*
             * Number of time values
             */
            Database.useReadonlyStatement(dataSource, "SELECT COUNT(*) FROM " + Table.ATTRIBUTE_TIME_VALUE_VECTOR.getTableName(), rs -> {
                if (rs.next()) {
                    buf.append("time=").append(rs.getLong(1)).append(" ");
                }
            });

            /*
             * Number of integer values
             */
            Database.useReadonlyStatement(dataSource, "SELECT COUNT(*) FROM " + Table.ATTRIBUTE_INTEGER_VALUE_VECTOR.getTableName(), rs -> {
                if (rs.next()) {
                    buf.append("integer=").append(rs.getLong(1)).append(" ");
                }
            });

            /*
             * Number of long values
             */
            Database.useReadonlyStatement(dataSource, "SELECT COUNT(*) FROM " + Table.ATTRIBUTE_LONG_VALUE_VECTOR.getTableName(), rs -> {
                if (rs.next()) {
                    buf.append("long=").append(rs.getLong(1)).append(" ");
                }
            });

            /*
             * Number of double values
             */
            Database.useReadonlyStatement(dataSource, "SELECT COUNT(*) FROM " + Table.ATTRIBUTE_DOUBLE_VALUE_VECTOR.getTableName(), rs -> {
                if (rs.next()) {
                    buf.append("double=").append(rs.getLong(1)).append(" ");
                }
            });

            /*
             * Number of boolean values
             */
            Database.useReadonlyStatement(dataSource, "SELECT COUNT(*) FROM " + Table.ATTRIBUTE_BOOLEAN_VALUE_VECTOR.getTableName(), rs -> {
                if (rs.next()) {
                    buf.append("boolean=").append(rs.getLong(1)).append(" ");
                }
            });

            /*
             * Number of data values
             */
            Database.useReadonlyStatement(dataSource, "SELECT COUNT(*) FROM " + Table.ATTRIBUTE_DATA_VALUE_VECTOR.getTableName(), rs -> {
                if (rs.next()) {
                    buf.append("data=").append(rs.getLong(1)).append(" ");
                }
            });

            /*
             * Number of record values
             */
            Database.useReadonlyStatement(dataSource, "SELECT COUNT(*) FROM " + Table.ATTRIBUTE_RECORD_VALUE_VECTOR.getTableName(), rs -> {
                if (rs.next()) {
                    buf.append("record=").append(rs.getLong(1)).append("\n");
                }
            });

            /*
             * Number of internal associations
             */
            Database.useReadonlyStatement(dataSource, "SELECT COUNT(*) FROM " + Table.INTERNAL_RELATION.getTableName(), rs -> {
                if (rs.next()) {
                    buf.append("Assocs: internal=").append(rs.getLong(1)).append(" ");
                }
            });

            /*
             * Number of external associations
             */
            Database.useReadonlyStatement(dataSource, "SELECT COUNT(*) FROM " + Table.EXTERNAL_ASSOCIATION.getTableName(), rs -> {
                if (rs.next()) {
                    buf.append("external=").append(rs.getLong(1)).append("\n");
                }
            });

            statistics.info(buf.toString());
            statistics.info("\n{}", repo.getTimingData().report());

            // We currently only do this on PostgreSQL (and not DB2)
            if (repo.getDatabaseAdapter() instanceof PostgresAdapter) {
                statistics.info("Index behaviour: \n{}", dumpIndexUse(dataSource));
                String statementStats = dumpPostgresStatementStats(dataSource);
                statistics.info("PostgreSQL statement stats:\n{}", statementStats);
                System.out.println(statementStats);
            }
        });
    }

    public static void resetPostgresStats(Repository repo) {
        if (!(repo.getDatabaseAdapter() instanceof PostgresAdapter)) {
            return;
        }

        try {
            repo.withConnection(conn -> {
                try {
                    conn.setReadOnly(false);
                    try (java.sql.Statement stmt = conn.createStatement()) {
                        try (java.sql.ResultSet rs = stmt.executeQuery("SELECT pg_stat_reset()")) {
                            if (rs.next()) {
                                // scalar return value intentionally ignored
                            }
                        }
                    }
                } catch (java.sql.SQLException sqle) {
                    throw new RuntimeException(sqle);
                }
            });
        } catch (Throwable t) {
            statistics.warn("Failed to reset pg_stat counters: {}", t.getMessage());
        }

        try {
            repo.withConnection(conn -> {
                try {
                    conn.setReadOnly(false);
                    try (java.sql.Statement stmt = conn.createStatement()) {
                        try (java.sql.ResultSet rs = stmt.executeQuery("SELECT pg_stat_statements_reset()")) {
                            if (rs.next()) {
                                // scalar return value intentionally ignored
                            }
                        }
                    }
                } catch (java.sql.SQLException sqle) {
                    throw new RuntimeException(sqle);
                }
            });
        } catch (Throwable t) {
            statistics.warn("Failed to reset pg_stat_statements counters: {}", t.getMessage());
        }
    }

    private record StatementStat(String calls, String totalSeconds, String avgMillis, String rows, String query) {}

    private static String dumpPostgresStatementStats(DataSource dataSource, String title, String orderByClause) {
        String sql = """
                SELECT
                    to_char(calls, '999,999,999,999') AS calls,
                    to_char(round((total_exec_time / 1000.0)::numeric, 3), '999,999,999,990.000') AS total_seconds,
                    to_char(round(mean_exec_time::numeric, 3), '999,999,999,990.000') AS avg_millis,
                    to_char(rows, '999,999,999,999') AS rows,
                    regexp_replace(query, '\\s+', ' ', 'g') AS query
                FROM pg_stat_statements
                WHERE lower(query) LIKE '%%repo_%%'
                ORDER BY %s
                LIMIT 15
                """.formatted(orderByClause);

        final Collection<StatementStat> statementStats = new ArrayList<>();
        try {
            Database.useReadonlyStatement(dataSource, sql, rs -> {
                while (rs.next()) {
                    int i = 0;
                    statementStats.add(new StatementStat(
                            rs.getString(++i).trim(),
                            rs.getString(++i).trim(),
                            rs.getString(++i).trim(),
                            rs.getString(++i).trim(),
                            rs.getString(++i).trim()
                    ));
                }
            });
        } catch (Throwable t) {
            return title + "\npg_stat_statements unavailable: " + t.getMessage() + "\n";
        }

        if (statementStats.isEmpty()) {
            return title + "\nNo pg_stat_statements data found.\n";
        }

        String[] cols = {"Calls", "Total s", "Avg ms", "Rows", "Query"};
        int[] colSize = Arrays.stream(cols).flatMapToInt(str -> IntStream.of(str.length() + 1)).toArray();
        for (StatementStat stat : statementStats) {
            colSize[0] = Math.max(colSize[0], stat.calls().length());
            colSize[1] = Math.max(colSize[1], stat.totalSeconds().length());
            colSize[2] = Math.max(colSize[2], stat.avgMillis().length());
            colSize[3] = Math.max(colSize[3], stat.rows().length());
            colSize[4] = Math.max(colSize[4], Math.min(stat.query().length(), 140));
        }

        StringBuilder line = new StringBuilder();
        for (int i = 0; i < colSize.length; i++) {
            line.append("-".repeat(Math.max(0, colSize[i] + 1)));
            if (i < colSize.length - 1) {
                line.append("+-");
            }
        }
        line.append("\n");

        StringBuilder hdrFormat = new StringBuilder();
        for (int i = 0; i < cols.length; i++) {
            hdrFormat.append("%").append(colSize[i]).append("s");
            if (i < cols.length - 1) {
                hdrFormat.append(" | ");
            }
        }
        hdrFormat.append("\n");

        final String format =
                "%" + colSize[0] + "s | " +
                "%" + colSize[1] + "s | " +
                "%" + colSize[2] + "s | " +
                "%" + colSize[3] + "s | " +
                "%-" + colSize[4] + "s\n";

        StringBuilder buf = new StringBuilder();
        buf.append(title).append("\n");
        buf.append(line);
        buf.append(String.format(Locale.ROOT, hdrFormat.toString(), (Object[]) cols));
        buf.append(line);
        statementStats.forEach(stat -> buf.append(String.format(
                Locale.ROOT,
                format,
                stat.calls(),
                stat.totalSeconds(),
                stat.avgMillis(),
                stat.rows(),
                stat.query().length() > colSize[4] ? stat.query().substring(0, colSize[4]) : stat.query()
        )));
        buf.append("\n");
        return buf.toString();
    }

    public static String dumpPostgresStatementStats(DataSource dataSource) {
        return dumpPostgresStatementStats(dataSource, "Top PostgreSQL statements by total time", "total_exec_time DESC")
                + dumpPostgresStatementStats(dataSource, "Top PostgreSQL statements by calls", "calls DESC, total_exec_time DESC");
    }

    private record IndexData(String tablename, String totalseqscan, String totalindexscan, String tablerows, String tablesize) {}

    private static class IndexDataComparator implements Comparator<IndexData> {
        @Override
        public int compare(final IndexData o1, final IndexData o2) {
            String tableName1 = o1.tablename();
            return tableName1.compareTo(o2.tablename());
        }
    }


    public static String dumpIndexUse(DataSource dataSource) {
        String sql = """
                SELECT
                    relname                                               AS tablename,
                    to_char(seq_scan, '999,999,999,999')                  AS totalseqscan,
                    to_char(idx_scan, '999,999,999,999')                  AS totalindexscan,
                    to_char(n_live_tup, '999,999,999,999')                AS tablerows,
                    pg_size_pretty(pg_relation_size(relname :: regclass)) AS tablesize
                FROM pg_catalog.pg_stat_user_tables
                ORDER BY relname ASC;
                """;

        final Collection<IndexData> indexData = new ArrayList<>();

        Database.useReadonlyStatement(dataSource, sql, rs -> {
            while (rs.next()) {
                int i = 0;
                indexData.add(new IndexData(
                        rs.getString(++i).trim(),
                        rs.getString(++i).trim(),
                        rs.getString(++i).trim(),
                        rs.getString(++i).trim(),
                        rs.getString(++i).trim()
                ));
            }
        });

        // Determine table column minimal size (based on headers)
        String[] cols = {"Tablename", "Total seq scan", "Total index scan", "Rows in table", "Size of table"};
        int[] colSize = Arrays.stream(cols).flatMapToInt(str -> IntStream.of(str.length() + 1)).toArray(); // minimum

        // Adjust column size based on actual data
        for (IndexData idd : indexData) {
            colSize[0] = Math.max(colSize[0], idd.tablename().length());
            colSize[1] = Math.max(colSize[1], idd.totalseqscan().length());
            colSize[2] = Math.max(colSize[2], idd.totalindexscan().length());
            colSize[3] = Math.max(colSize[3], idd.tablerows().length());
            colSize[4] = Math.max(colSize[4], idd.tablesize().length());
        }

        StringBuilder hdrFormat = new StringBuilder();
        for (int i = 0; i < cols.length; i++) {
            hdrFormat.append("%").append(colSize[i]).append("s");
            if (i < cols.length - 1) {
                hdrFormat.append(" | ");
            }
        }
        hdrFormat.append("\n");

        StringBuilder line = new StringBuilder();
        for (int i = 0; i < colSize.length; i++) {
            line.append("-".repeat(Math.max(0, colSize[i] + 1)));
            if (i < colSize.length - 1) {
                line.append("+-");
            }
        }
        line.append("\n");

        // Preamble (format) for table data
        StringBuilder buf = new StringBuilder();
        buf.append(line);
        buf.append(String.format(hdrFormat.toString(), (Object[]) cols));
        buf.append(line);

        final String format =
            "%" + colSize[0] + "s | " +
            "%" + colSize[1] + "s | " +
            "%" + colSize[2] + "s | " +
            "%" + colSize[3] + "s | " +
            "%" + colSize[4] + "s\n";

        // Actual table data
        indexData.stream().sorted(new IndexDataComparator()).forEach(idd -> buf.append(String.format(
                format,
                idd.tablename(),
                idd.totalseqscan(),
                idd.totalindexscan(),
                idd.tablerows(),
                idd.tablesize()
        )));
        buf.append("\n");
        return buf.toString();
    }
}
