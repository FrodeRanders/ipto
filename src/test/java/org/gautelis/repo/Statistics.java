package org.gautelis.repo;

import com.sun.management.OperatingSystemMXBean;
import org.gautelis.repo.db.Database;
import org.gautelis.repo.model.Repository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gautelis.vopn.lang.Number.asHumanApproximate;

import java.lang.management.ManagementFactory;

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

        buf.append("\n");

        repo.withDataSource(dataSource -> {
            /*
             * Count all units
             */
            String sql = "SELECT COUNT(*) FROM repo_unit";

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
        });
        statistics.info(buf.toString());
        statistics.info("\n{}", repo.getTimingData().report());
    }
}
