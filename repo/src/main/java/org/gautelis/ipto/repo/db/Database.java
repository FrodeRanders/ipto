/*
 * Copyright (C) 2024-2026 Frode Randers
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
package org.gautelis.ipto.repo.db;

import org.gautelis.ipto.repo.exceptions.*;
import org.gautelis.ipto.repo.utils.CheckedConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;

/**
 *
 */
public final class Database {
    private static final Logger log = LoggerFactory.getLogger(Database.class);

    // Used in deadlock handling
    private static final int DEADLOCK_MAX_RETRIES = 100;
    private static final int DEADLOCK_SLEEP_TIME = 200; // milliseconds

    private Database() {
    }

    /**
     * Support for explicit logging of SQL exceptions to error log.
     */
    public static String squeeze(SQLException sqle) {
        SQLException e = sqle;
        StringBuilder buf = new StringBuilder();
        while (e != null) {
            buf.append("Exception [");
            buf.append(e.getMessage());
            buf.append("], SQLstate(");
            buf.append(e.getSQLState());
            buf.append("), Vendor code(");
            buf.append(e.getErrorCode());
            buf.append(")\n");
            e = e.getNextException();
        }
        return buf.toString();
    }

    /**
     * Support for explicit logging of SQL warnings to warning log.
     */
    public static String squeeze(SQLWarning sqlw) {
        SQLWarning w = sqlw;
        StringBuilder buf = new StringBuilder();
        while (w != null) {
            buf.append("Warning [");
            buf.append(w.getMessage());
            buf.append("], SQLstate(");
            buf.append(w.getSQLState());
            buf.append("), Vendor code(");
            buf.append(w.getErrorCode());
            buf.append(")\n");
            w = w.getNextWarning();
        }
        return buf.toString();
    }

    /**
     * Manages call to Statement.execute(), providing support for deadlock
     * detection and statement reruns.
     */
    private static boolean execute(Statement stmt, String sql) throws SQLException {
        SQLException sqle;
        int i = DEADLOCK_MAX_RETRIES;
        boolean isResults;
        do {
            try {
                isResults = stmt.execute(sql);

                // Handle warning, if applicable
                SQLWarning warning = stmt.getWarnings();
                if (null != warning) {
                    log.warn(squeeze(warning));
                }

                return isResults;

            } catch (SQLException se) {
                sqle = se;
                // Is this SQLException a deadlock? (40001)
                if (se.getSQLState() != null && se.getSQLState().startsWith("40")) {
                    log.info("Database deadlock has occurred during execute, trying again");
                    try {
                        Thread.sleep(DEADLOCK_SLEEP_TIME);
                    } catch (Exception ignore) {
                    }
                } else /* other SQLException */ {
                    throw se;
                }
            }
            if (log.isTraceEnabled()) {
                log.trace("execute, retries={}", i);
            }
        } while (--i > 0);
        log.error("Giving up deadlock retry");
        throw sqle; // ok
    }

    /**
     * Manages call to Statement.executeBatch(), providing support for deadlock
     * detection and statement reruns.
     */
    public static int[] executeBatch(Statement stmt) throws SQLException {
        SQLException sqle = null;
        int i = DEADLOCK_MAX_RETRIES;
        int[] isResults = {};
        do {
            try {
                isResults = stmt.executeBatch();

                // Handle warning, if applicable
                SQLWarning warning = stmt.getWarnings();
                if (null != warning) {
                    log.warn(squeeze(warning));
                }

                return isResults;

            } catch (SQLException se) {
                // Is this SQLException a deadlock? (40001)
                if (se.getSQLState() != null && se.getSQLState().startsWith("40")) {
                    log.info("Database deadlock has occurred during batch execute, trying again");
                    try {
                        Thread.sleep(DEADLOCK_SLEEP_TIME);
                    } catch (Exception e) {
                        // ignore
                    }
                } else /* other SQLException */ {
                    throw se;
                }
            }
            if (log.isTraceEnabled()) {
                log.trace("batch execution, retries={}", i);
            }
        } while (--i > 0);

        return isResults;
    }

    /**
     * Manages call to PreparedStatement.execute(), providing support for deadlock
     * detection and statement reruns.
     */
    public static boolean execute(PreparedStatement pStmt) throws SQLException {
        SQLException sqle;
        int i = DEADLOCK_MAX_RETRIES;
        boolean isResults;
        do {
            try {
                isResults = pStmt.execute();

                // Handle warning, if applicable
                SQLWarning warning = pStmt.getWarnings();
                if (null != warning) {
                    log.warn(squeeze(warning));
                }

                return isResults;

            } catch (SQLException se) {
                sqle = se;
                // Is this SQLException a deadlock? (40001)
                if (se.getSQLState() != null && se.getSQLState().startsWith("40")) {
                    log.info("Database deadlock has occurred during execute, trying again");
                    try {
                        Thread.sleep(DEADLOCK_SLEEP_TIME);
                    } catch (Exception e) {
                        // ignore
                    }
                } else /* other SQLException */ {
                    throw se;
                }
            }
            if (log.isTraceEnabled()) {
                log.trace("execute, retries={}", i);
            }
        } while (--i > 0);
        log.error("Giving up deadlock retry");
        throw sqle; // ok
    }

    /**
     * Manages call to CallableStatement.execute(), providing support for deadlock
     * detection and statement reruns.
     */
    public static boolean execute(CallableStatement cStmt) throws SQLException {
        SQLException sqle;
        int i = DEADLOCK_MAX_RETRIES;
        boolean isResults;
        do {
            try {
                isResults = cStmt.execute();

                // Handle warning, if applicable
                SQLWarning warning = cStmt.getWarnings();
                if (null != warning) {
                    log.warn(squeeze(warning));
                }

                return isResults;

            } catch (SQLException se) {
                sqle = se;
                // Is this SQLException a deadlock? (40001)
                if (se.getSQLState() != null && se.getSQLState().startsWith("40")) {
                    log.info("Database deadlock has occurred during execute, trying again");
                    try {
                        Thread.sleep(DEADLOCK_SLEEP_TIME);
                    } catch (Exception e) {
                        // ignore
                    }
                } else /* other SQLException */ {
                    throw se;
                }
            }
            if (log.isTraceEnabled()) {
                log.trace("call execution, retries={}", i);
            }
        } while (--i > 0);
        log.error("Giving up deadlock retry");
        throw sqle; // ok
    }

    /**
     * Manages call to PreparedStatement.executeQuery(), providing support for deadlock
     * detection and statement reruns.
     */
    public static ResultSet executeQuery(PreparedStatement pStmt) throws SQLException {
        SQLException sqle;
        ResultSet rs;
        int i = DEADLOCK_MAX_RETRIES;
        do {
            try {
                rs = pStmt.executeQuery();

                // Handle warning, if applicable
                SQLWarning stmtWarning = pStmt.getWarnings();
                if (null != stmtWarning) {
                    log.warn(squeeze(stmtWarning));
                }

                SQLWarning rsWarning = rs.getWarnings();
                if (null != rsWarning) {
                    log.warn(squeeze(rsWarning));
                }

                return rs;

            } catch (SQLException se) {
                sqle = se;
                // Is this SQLException a deadlock? (40001)
                if (se.getSQLState() != null && se.getSQLState().startsWith("40")) {
                    log.info("Database deadlock has occurred during executeQuery, trying again");
                    try {
                        Thread.sleep(DEADLOCK_SLEEP_TIME);
                    } catch (Exception e) {
                        // ignore
                    }
                } else /* other SQLException */ {
                    throw se;
                }
            }
            if (log.isTraceEnabled()) {
                log.trace("executeQuery, retries={}", i);
            }
        } while (--i > 0);
        log.error("Giving up deadlock retry");
        throw sqle; // ok
    }

    /**
     * Manages call to Statement.executeQuery(), providing support for deadlock
     * detection and statement reruns.
     */
    public static ResultSet executeQuery(Statement stmt, String sql) throws SQLException {
        SQLException sqle;
        ResultSet rs;
        int i = DEADLOCK_MAX_RETRIES;
        do {
            try {
                rs = stmt.executeQuery(sql);

                // Handle warning, if applicable
                SQLWarning stmtWarning = stmt.getWarnings();
                if (null != stmtWarning) {
                    log.warn(squeeze(stmtWarning));
                }

                SQLWarning rsWarning = rs.getWarnings();
                if (null != rsWarning) {
                    log.warn(squeeze(rsWarning));
                }

                return rs;

            } catch (SQLException se) {
                sqle = se;
                // Is this SQLException a deadlock? (40001)
                if (se.getSQLState() != null && se.getSQLState().startsWith("40")) {
                    log.info("Database deadlock has occurred during executeQuery, trying again");
                    try {
                        Thread.sleep(DEADLOCK_SLEEP_TIME);
                    } catch (Exception e) {
                        // ignore
                    }
                } else /* other SQLException */ {
                    throw se;
                }
            }
            if (log.isTraceEnabled()) {
                log.trace("executeQuery, retries={}", i);
            }
        } while (--i > 0);
        log.error("Giving up deadlock retry");
        throw sqle; // ok
    }

    /**
     * Manages call to PreparedStatement.executeUpdate(), providing support for deadlock
     * detection and statement reruns.
     */
    public static int executeUpdate(PreparedStatement pStmt) throws SQLException {
        SQLException sqle;
        int i = DEADLOCK_MAX_RETRIES;
        int rows;
        do {
            try {
                rows = pStmt.executeUpdate();

                // Handle warning, if applicable
                SQLWarning warning = pStmt.getWarnings();
                if (null != warning) {
                    log.warn(squeeze(warning));
                }

                return rows;

            } catch (SQLException se) {
                sqle = se;
                // Is this SQLException a deadlock? (40001)
                if (se.getSQLState() != null && se.getSQLState().startsWith("40")) {
                    log.info("Database deadlock has occurred during executeUpdate, trying again");
                    try {
                        Thread.sleep(DEADLOCK_SLEEP_TIME);
                    } catch (Exception e) {
                        // ignore
                    }
                } else /* other SQLException */ {
                    throw se;
                }
            }
            if (log.isTraceEnabled()) {
                log.trace("executeUpdate, retries={}", i);
            }
        } while (--i > 0);
        log.error("Giving up deadlock retry");
        throw sqle; // ok
    }

    /**
     * Manages call to Statement.executeUpdate(), providing support for deadlock
     * detection and statement reruns.
     */
    public static int executeUpdate(Statement stmt, String sql) throws SQLException {
        SQLException sqle;
        int i = DEADLOCK_MAX_RETRIES;
        int rows;
        do {
            try {
                rows = stmt.executeUpdate(sql);

                // Handle warning, if applicable
                SQLWarning warning = stmt.getWarnings();
                if (null != warning) {
                    log.warn(squeeze(warning));
                }

                return rows;

            } catch (SQLException se) {
                sqle = se;
                // Is this SQLException a deadlock? (40001)
                if (se.getSQLState() != null && se.getSQLState().startsWith("40")) {
                    log.info("Database deadlock has occurred during executeUpdate, trying again");
                    try {
                        Thread.sleep(DEADLOCK_SLEEP_TIME);
                    } catch (Exception e) {
                        // ignore
                    }
                } else /* other SQLException */ {
                    throw se;
                }
            }
            if (log.isTraceEnabled()) {
                log.trace("executeUpdate, retries={}", i);
            }
        } while (--i > 0);
        log.error("Giving up deadlock retry");
        throw sqle; // ok
    }

    /**
     * Execute around method for using a readonly database connection.
     *
     * @param ds    the DataSource to use for the connection
     * @param cBlock the connection lambda
     * @throws DatabaseConnectionException
     * @throws DatabaseReadException
     * @throws ConfigurationException
     */
    public static void useReadonlyConnection(
            DataSource ds, CheckedConsumer<Connection> cBlock
    ) throws DatabaseConnectionException, DatabaseReadException, ConfigurationException {
        try (Connection conn = ds.getConnection()) {
            conn.setReadOnly(true);
            cBlock.accept(conn);

        } catch (RuntimeException r) {
            Throwable t = r.getCause();
            if (t instanceof BaseException) {
                switch (t) {
                    case DatabaseConnectionException databaseConnectionException -> throw databaseConnectionException;
                    case DatabaseReadException databaseReadException -> throw databaseReadException;
                    case ConfigurationException configurationException -> throw configurationException;
                    default -> {
                    }
                }
            }
            throw r;
        } catch (SQLException sqle) {
            String info = "Failed to use readonly connection: " + Database.squeeze(sqle);
            log.warn(info);
            throw new DatabaseConnectionException(info, sqle);
        }
    }

    /**
     * Execute around method for using a read-write database connection.
     *
     * @param ds    the DataSource to use for the connection
     * @param cBlock the connection lambda
     * @throws DatabaseConnectionException
     * @throws DatabaseReadException
     * @throws ConfigurationException
     */
    public static void useConnection(
            DataSource ds, CheckedConsumer<Connection> cBlock
    ) throws DatabaseConnectionException, DatabaseReadException, ConfigurationException {
        try (Connection conn = ds.getConnection()) {
            conn.setReadOnly(false);
            cBlock.accept(conn);

        } catch (RuntimeException r) {
            Throwable t = r.getCause();
            if (t instanceof BaseException) {
                switch (t) {
                    case DatabaseConnectionException databaseConnectionException -> throw databaseConnectionException;
                    case DatabaseReadException databaseReadException -> throw databaseReadException;
                    case ConfigurationException configurationException -> throw configurationException;
                    default -> {
                    }
                }
            }
            throw r;
        } catch (SQLException sqle) {
            String info = "Failed to use read/write connection: " + Database.squeeze(sqle);
            log.warn(info);
            throw new DatabaseConnectionException(info, sqle);
        }
    }

    /**
     * Execute-around method for using a prepared statement on a readonly connection
     *
     * @param ds the DataSource to use for the connection
     * @param sql the SQL statement to use
     * @param sBlock the prepared statement lambda
     * @throws DatabaseConnectionException
     * @throws DatabaseReadException
     * @throws ConfigurationException
     */
    public static void useReadonlyPreparedStatement(
            DataSource ds, String sql, CheckedConsumer<PreparedStatement> sBlock
    ) throws DatabaseConnectionException, DatabaseReadException {

        useReadonlyConnection(ds, conn ->
            useReadonlyPreparedStatement(conn, sql, sBlock)
        );
    }

    /**
     * Execute-around method for using a readonly prepared statement on an existing connection
     *
     * @param conn
     * @param sql  the SQL statement to use
     * @param sBlock the prepared statement lambda
     * @throws DatabaseReadException
     */
    public static void useReadonlyPreparedStatement(
            Connection conn, String sql, CheckedConsumer<PreparedStatement> sBlock
    ) throws DatabaseReadException {
        try {
            try (PreparedStatement pStmt = conn.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)) {
                sBlock.accept(pStmt);
            }
        } catch (SQLException sqle) {
            String info = "Failed to execute readonly prepared statement: " + Database.squeeze(sqle);
            log.warn(info);
            throw new DatabaseReadException(info, sqle);
        }
    }

    /**
     * Execute-around method for using a readonly prepared statement on an existing connection
     *
     * @param conn
     * @param sql  the SQL statement to use
     * @param sBlock the prepared statement lambda
     * @param rsBlock the resultset lambda
     * @throws DatabaseReadException
     */
    public static void useReadonlyPreparedStatement(
            Connection conn, String sql, CheckedConsumer<PreparedStatement> sBlock, CheckedConsumer<ResultSet> rsBlock
    ) throws DatabaseReadException {
        try {
            try (PreparedStatement pStmt = conn.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)) {
                sBlock.accept(pStmt);

                try (ResultSet rs = Database.executeQuery(pStmt)) {
                    rsBlock.accept(rs);
                }
            }
        } catch (SQLException sqle) {
            String info = "Failed to execute readonly prepared statement: " + Database.squeeze(sqle);
            log.warn(info);
            throw new DatabaseReadException(info, sqle);
        }
    }

    /**
     * Execute-around method for using a prepared statement on a read-write connection
     *
     * @param ds    the DataSource to use for the connection
     * @param sql  the SQL statement to use
     * @param sBlock the prepared statement lambda
     * @throws DatabaseConnectionException
     * @throws DatabaseWriteException
     */
    public static void usePreparedStatement(
            DataSource ds, String sql, CheckedConsumer<PreparedStatement> sBlock
    ) throws DatabaseConnectionException, DatabaseWriteException {
        useConnection(ds, conn -> {
            conn.setAutoCommit(false);
            usePreparedStatement(conn, sql, sBlock);
            conn.commit();
        });
    }

    /**
     * Execute-around method for using a prepared statement on a read-write connection
     *
     * @param conn the connection to operate upon
     * @param sql the SQL statement to use
     * @param sBlock the prepared statement lambda
     * @throws DatabaseWriteException
     */
    public static void usePreparedStatement(
            Connection conn, String sql, CheckedConsumer<PreparedStatement> sBlock
    ) throws DatabaseWriteException {
        try {
            try (PreparedStatement pStmt = conn.prepareStatement(sql)) {
                sBlock.accept(pStmt);
            }
        } catch (SQLException sqle) {
            String info = "Failed to execute prepared statement: " + Database.squeeze(sqle);
            log.warn(info);
            throw new DatabaseWriteException(info, sqle);
        }
    }

    /**
     * Execute-around method for using a statement on a readonly connection
     *
     * @param ds    the DataSource to use for the connection
     * @param sql  the SQL statement to use
     * @param rsBlock the resultset lambda
     * @throws DatabaseConnectionException
     * @throws DatabaseReadException
     * @throws ConfigurationException
     */
    public static void useReadonlyStatement(
            DataSource ds, String sql, CheckedConsumer<ResultSet> rsBlock
    ) throws DatabaseConnectionException, DatabaseReadException {

        useReadonlyConnection(ds, conn ->
                useReadonlyStatement(conn, sql, rsBlock)
        );
    }

    /**
     * Execute-around method for using a statement on a readonly connection
     *
     * @param ds    the DataSource to use for the connection
     * @param sql  the SQL statement to use
     * @param sBlock the statment lambda
     * @param rsBlock the resultset lambda
     * @throws DatabaseConnectionException
     * @throws DatabaseReadException
     * @throws ConfigurationException
     */
    public static void useReadonlyStatement(
            DataSource ds, String sql, CheckedConsumer<Statement> sBlock, CheckedConsumer<ResultSet> rsBlock
    ) throws DatabaseConnectionException, DatabaseReadException {

        useReadonlyConnection(ds, conn ->
                useReadonlyStatement(conn, sql, sBlock, rsBlock)
        );
    }

    /**
     * Execute-around method for using a readonly statement on an existing connection
     *
     * @param conn
     * @param sql  the SQL statement to use
     * @param sBlock the statement lambda
     * @param rsBlock the resultset lambda
     * @throws DatabaseReadException
     */
    public static void useReadonlyStatement(
            Connection conn, String sql, CheckedConsumer<Statement> sBlock, CheckedConsumer<ResultSet> rsBlock
    ) throws DatabaseReadException {
        try {
            try (Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)) {
                sBlock.accept(stmt);
                try (ResultSet rs = Database.executeQuery(stmt, sql)) {
                    rsBlock.accept(rs);
                }
            }
        } catch (SQLException sqle) {
            String info = "Failed to execute readonly statement: " + Database.squeeze(sqle);
            log.warn(info);
            throw new DatabaseReadException(info, sqle);
        }
    }

    /**
     * Execute-around method for using a readonly statement on an existing connection
     *
     * @param conn
     * @param sql  the SQL statement to use
     * @param rsBlock the resultset lambda
     * @throws DatabaseReadException
     */
    public static void useReadonlyStatement(
            Connection conn, String sql, CheckedConsumer<ResultSet> rsBlock
    ) throws DatabaseReadException {
        try {
            try (Statement _stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
                 ResultSet rs = Database.executeQuery(_stmt, sql)) {
                rsBlock.accept(rs);
            }
        } catch (SQLException sqle) {
            String info = "Failed to consume resultset while executing readonly statement: " + Database.squeeze(sqle);
            log.warn(info);
            throw new DatabaseReadException(info, sqle);
        }
    }
}
