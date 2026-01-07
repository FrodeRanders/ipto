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
package org.gautelis.ipto.repo.udpf;

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
     * EXTERNAL NAME 'IPTOJAR:org.gautelis.ipto.repo.udpf.Utilities.log_java_version';
     *
     * @throws SQLException
     */
    public static void log_java_version() throws SQLException {
        String version = System.getProperty("java.version");
        String vendor = System.getProperty("java.vendor");
        String home = System.getProperty("java.home");

        try (Connection con = DriverManager.getConnection("jdbc:default:connection")) {
            String sql = "INSERT INTO JAVA_ENV_LOG(TS, JAVA_VERSION, JAVA_VENDOR, JAVA_HOME) VALUES (CURRENT_TIMESTAMP, ?, ?, ?)";

            try (PreparedStatement ps = con.prepareStatement(sql)) {
                ps.setString(1, version);
                ps.setString(2, vendor);
                ps.setString(3, home);
                ps.executeUpdate();
            }
        }
    }
}


