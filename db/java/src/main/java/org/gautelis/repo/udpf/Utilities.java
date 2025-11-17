package org.gautelis.repo.udpf;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Java implemented procedure.
 *
 * Using very old Java version (as an assumption) with no try-with-resources and so on.
 */
public class Utilities {
    /**
     * Logs java details to table 'JAVA_ENV_LOG'.
     *
     * CREATE TABLE JAVA_ENV_LOG
     * (
     *     TS           TIMESTAMP NOT NULL,
     *     JAVA_VERSION VARCHAR(64),
     *     JAVA_VENDOR  VARCHAR(128),
     *     JAVA_HOME    VARCHAR(512)
     * );
     *
     * Register procedure as:
     *
     * CREATE OR REPLACE PROCEDURE LOG_JAVA_VERSION()
     * LANGUAGE JAVA
     * PARAMETER STYLE JAVA
     * MODIFIES SQL DATA
     * FENCED
     * EXTERNAL NAME 'IPTOJAR:org.gautelis.repo.udpf.Utilities.log_java_version';
     *
     * @throws SQLException
     */
    public static void log_java_version() throws SQLException {
        String version = System.getProperty("java.version");
        String vendor = System.getProperty("java.vendor");
        String home = System.getProperty("java.home");

        Connection con = DriverManager.getConnection("jdbc:default:connection");
        try {
            String sql = "INSERT INTO JAVA_ENV_LOG(TS, JAVA_VERSION, JAVA_VENDOR, JAVA_HOME) VALUES (CURRENT_TIMESTAMP, ?, ?, ?)";

            PreparedStatement ps = con.prepareStatement(sql);
            try {
                ps.setString(1, version);
                ps.setString(2, vendor);
                ps.setString(3, home);
                ps.executeUpdate();
            } finally {
                ps.close();
            }
        } finally {
            con.close();
        }
    }
}


