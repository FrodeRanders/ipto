package org.gautelis.repo;

import com.sun.management.OperatingSystemMXBean;
import org.gautelis.repo.db.Database;
import org.gautelis.repo.model.Repository;
import org.gautelis.repo.model.utils.MovingAverage;
import org.gautelis.vopn.lang.TimeDelta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;

import static org.gautelis.vopn.lang.Number.asHumanApproximate;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.stream.IntStream;

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
        });


        repo.withDataSource(dataSource -> {
            /*
             * DB server version
             */
            String sql = "SELECT version()"; // PostgreSQL specific, certainly.

            Database.useReadonlyStatement(dataSource, sql, rs -> {
                if (rs.next()) {
                    buf.append("DB: ")
                            .append(rs.getString("version")).append("\n");
                }
            });
            buf.append("\n");

            /*
             * Count all units
             */
            sql = "SELECT COUNT(*) FROM repo_unit";

            Database.useReadonlyStatement(dataSource, sql, rs -> {
                if (rs.next()) {
                    buf.append("Units: total=").append(rs.getLong(1)).append(" ");
                }
            });

            /*
             * Number of locks
             */
            Database.useReadonlyStatement(dataSource, "SELECT COUNT(*) FROM repo_lock", rs -> {
                if (rs.next()) {
                    buf.append("locks=").append(rs.getLong(1)).append(" ");
                }
            });

            /*
             * Number of mappings from units to attribute values
             */
            Database.useReadonlyStatement(dataSource, "SELECT COUNT(*) FROM repo_attribute_value", rs -> {
                if (rs.next()) {
                    buf.append("vectors=").append(rs.getLong(1)).append("\n");
                }
            });

            buf.append("Values: ");

            /*
             * Number of string values
             */
            Database.useReadonlyStatement(dataSource, "SELECT COUNT(*) FROM repo_string_vector", rs -> {
                if (rs.next()) {
                    buf.append("string=").append(rs.getLong(1)).append(" ");
                }
            });

            /*
             * Number of time values
             */
            Database.useReadonlyStatement(dataSource, "SELECT COUNT(*) FROM repo_time_vector", rs -> {
                if (rs.next()) {
                    buf.append("time=").append(rs.getLong(1)).append(" ");
                }
            });

            /*
             * Number of integer values
             */
            Database.useReadonlyStatement(dataSource, "SELECT COUNT(*) FROM repo_integer_vector", rs -> {
                if (rs.next()) {
                    buf.append("integer=").append(rs.getLong(1)).append(" ");
                }
            });

            /*
             * Number of long values
             */
            Database.useReadonlyStatement(dataSource, "SELECT COUNT(*) FROM repo_long_vector", rs -> {
                if (rs.next()) {
                    buf.append("long=").append(rs.getLong(1)).append(" ");
                }
            });

            /*
             * Number of double values
             */
            Database.useReadonlyStatement(dataSource, "SELECT COUNT(*) FROM repo_double_vector", rs -> {
                if (rs.next()) {
                    buf.append("double=").append(rs.getLong(1)).append(" ");
                }
            });

            /*
             * Number of boolean values
             */
            Database.useReadonlyStatement(dataSource, "SELECT COUNT(*) FROM repo_boolean_vector", rs -> {
                if (rs.next()) {
                    buf.append("boolean=").append(rs.getLong(1)).append(" ");
                }
            });

            /*
             * Number of data values
             */
            Database.useReadonlyStatement(dataSource, "SELECT COUNT(*) FROM repo_data_vector", rs -> {
                if (rs.next()) {
                    buf.append("data=").append(rs.getLong(1)).append(" ");
                }
            });

            /*
             * Number of record values
             */
            Database.useReadonlyStatement(dataSource, "SELECT COUNT(*) FROM repo_record_vector", rs -> {
                if (rs.next()) {
                    buf.append("record=").append(rs.getLong(1)).append("\n");
                }
            });

            /*
             * Number of internal associations
             */
            Database.useReadonlyStatement(dataSource, "SELECT COUNT(*) FROM repo_internal_assoc", rs -> {
                if (rs.next()) {
                    buf.append("Assocs: internal=").append(rs.getLong(1)).append(" ");
                }
            });

            /*
             * Number of external associations
             */
            Database.useReadonlyStatement(dataSource, "SELECT COUNT(*) FROM repo_external_assoc", rs -> {
                if (rs.next()) {
                    buf.append("external=").append(rs.getLong(1)).append("\n");
                }
            });

            statistics.info(buf.toString());
            statistics.info("\n{}", repo.getTimingData().report());
            statistics.info("Index behaviour: \n{}", dumpIndexUse(dataSource));
        });
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

        record IndexData(String tablename, String totalseqscan, String totalindexscan, String tablerows, String tablesize) {}
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

        String format = "";
        format += "%" + colSize[0] + "s | ";
        format += "%" + colSize[1] + "s | ";
        format += "%" + colSize[2] + "s | ";
        format += "%" + colSize[3] + "s | ";
        format += "%" + colSize[4] + "s\n";

        // Actual table data
        for (IndexData idd : indexData) {
            buf.append(String.format(
                    format,
                    idd.tablename(),
                    idd.totalseqscan(),
                    idd.totalindexscan(),
                    idd.tablerows(),
                    idd.tablesize()
            ));
        }
        buf.append("\n");
        return buf.toString();
    }
}
