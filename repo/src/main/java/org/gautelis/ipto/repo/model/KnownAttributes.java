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
package org.gautelis.ipto.repo.model;

import org.gautelis.ipto.repo.db.Database;
import org.gautelis.ipto.repo.exceptions.ConfigurationException;
import org.gautelis.ipto.repo.exceptions.DatabaseConnectionException;
import org.gautelis.ipto.repo.exceptions.DatabaseReadException;
import org.gautelis.ipto.repo.exceptions.DatabaseWriteException;
import org.gautelis.ipto.repo.model.AttributeType;
import org.gautelis.ipto.repo.model.utils.TimedExecution;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;


/**
 *
 */
public final class KnownAttributes {
    private static final Logger log = LoggerFactory.getLogger(KnownAttributes.class);

    public static class AttributeInfo {
        public int id = 0;
        public String qualName = null;
        public String name = null;
        public String alias = null;
        public int type = 0; // illegal initial value
        public boolean forcedScalar = false;
        public Timestamp created = null;
    }

    private static final Map<String, AttributeInfo> attributes = new HashMap<>();

    public static synchronized boolean canChangeAttribute(Context ctx, int attrId)
            throws DatabaseConnectionException, DatabaseReadException {
        String sql = """
            SELECT 1
            FROM repo_attribute_value
            WHERE attrid = ?
            FETCH FIRST 1 ROWS ONLY
            """;

        boolean[] hasValues = { false };
        Database.useReadonlyConnection(ctx.getDataSource(), conn ->
            Database.useReadonlyPreparedStatement(conn, sql,
                    pStmt -> pStmt.setInt(1, attrId),
                    rs -> hasValues[0] = rs.next()
            )
        );

        return !hasValues[0];
    }

    public static synchronized boolean canChangeAttribute(Context ctx, String attributeName)
            throws DatabaseConnectionException, DatabaseReadException {
        Integer attrId = fetchAttributeNameToIdMap(ctx).get(attributeName);
        if (attrId == null) {
            return true;
        }
        return canChangeAttribute(ctx, attrId);
    }

    public static synchronized Optional<AttributeInfo> createAttribute(
            Context ctx,
            String alias,
            String attrName,
            String qualifiedName,
            AttributeType type,
            boolean isArray
    ) throws DatabaseConnectionException, DatabaseReadException, ConfigurationException {
        Map<String, AttributeInfo> data = fetchAttributes(ctx);
        AttributeInfo existing = data.get(attrName);
        if (existing != null) {
            return Optional.of(existing);
        }

        AttributeInfo[] created = { null };
        String sql = """
            INSERT INTO repo_attribute (attrtype, scalar, attrname, qualname, alias)
            VALUES (?,?,?,?,?)
            """;

        Database.useConnection(ctx.getDataSource(), conn -> {
            try {
                conn.setAutoCommit(false);

                String[] generatedColumns = { "attrid" };
                try (PreparedStatement pStmt = conn.prepareStatement(sql, generatedColumns)) {
                    int i = 0;
                    pStmt.setInt(++i, type.getType());
                    pStmt.setBoolean(++i, !isArray); // Note negation
                    pStmt.setString(++i, attrName);
                    pStmt.setString(++i, qualifiedName);
                    pStmt.setString(++i, alias);

                    Database.executeUpdate(pStmt);

                    try (ResultSet rs = pStmt.getGeneratedKeys()) {
                        if (rs.next()) {
                            AttributeInfo info = new AttributeInfo();
                            info.id = rs.getInt(1);
                            info.qualName = qualifiedName;
                            info.name = attrName;
                            info.alias = alias;
                            info.type = type.getType();
                            info.forcedScalar = !isArray;
                            created[0] = info;
                        } else {
                            String info = "↯ Failed to determine auto-generated attribute ID";
                            throw new ConfigurationException(info);
                        }
                    }
                }

                conn.commit();
            } catch (SQLException sqle) {
                String sqlState = sqle.getSQLState();

                try {
                    conn.rollback();
                } catch (SQLException rbe) {
                    log.error("↯ Failed to rollback attribute insert: {}", Database.squeeze(rbe), rbe);
                }

                if (sqlState != null && sqlState.startsWith("23")) {
                    if (log.isTraceEnabled()) {
                        log.trace("↯ Attribute '{}' seems to already have been loaded", attrName);
                    }
                    attributes.clear();
                    AttributeInfo refreshed = fetchAttributes(ctx).get(attrName);
                    created[0] = refreshed;
                    return;
                }
                throw new DatabaseWriteException("Failed to store attribute: " + Database.squeeze(sqle), sqle);
            }
        });

        if (created[0] != null) {
            attributes.put(created[0].name, created[0]);
        }

        return Optional.ofNullable(created[0]);
    }

    /**
     * Fetch all data from all known attributes.
     * <p>
     */
    private static synchronized Map<String, AttributeInfo> fetchAttributes(Context ctx) throws DatabaseConnectionException, DatabaseReadException {

        if (attributes.isEmpty()) {
            if (log.isTraceEnabled()) {
                log.trace("Fetching known attributes");
            }

            TimedExecution.run(ctx.getTimingData(), "fetch known attributes", () -> Database.useReadonlyPreparedStatement(ctx.getDataSource(), ctx.getStatements().attributeGetAll(), pStmt -> {
                try (ResultSet rs = Database.executeQuery(pStmt)) {
                    while (rs.next()) {
                        AttributeInfo info = new AttributeInfo();
                        info.id = rs.getInt("attrid");
                        info.qualName = rs.getString("qualname");
                        info.name = rs.getString("attrname");
                        info.alias = rs.getString("alias");
                        info.type = rs.getInt("attrtype");
                        info.forcedScalar = rs.getBoolean("scalar");
                        info.created = rs.getTimestamp("created");

                        attributes.put(info.name, info);
                    }
                }
            }));
        }
        return attributes;
    }

    /**
     * Fetch attribute name to id map
     */
    public static Map<String, Integer> fetchAttributeNameToIdMap(Context ctx) {
        Map<String, Integer> name2Id = new HashMap<>();

        Map<String, AttributeInfo> attributes = fetchAttributes(ctx);
        for (Map.Entry<String, AttributeInfo> entry : attributes.entrySet()) {
            AttributeInfo attribute = entry.getValue();

            String alias = attribute.alias;
            String name = attribute.name;
            String qualName = attribute.qualName;

            name2Id.put(alias, attribute.id);
            name2Id.put(name, attribute.id);
            name2Id.put(qualName, attribute.id);
        }

        return name2Id;
    }

    /**
     * Get attribute identified by id
     *
     * @param attrId id of attribute
     * @return AttributeInfo if attribute exists
     */
    /* package accessible only */
    static Optional<AttributeInfo> getAttribute(Context ctx, int attrId) throws DatabaseConnectionException, DatabaseReadException {

        Map<String, AttributeInfo> data = fetchAttributes(ctx);

        for (AttributeInfo info : data.values()) {
            if (info.id == attrId) {
                return Optional.of(info);
            }
        }
        return Optional.empty();
    }

    /**
     * Get attribute identified by name.
     *
     * @param name name of attribute
     * @return AttributeInfo if attribute exists
     */
    /* package accessible only */
    static Optional<AttributeInfo> getAttribute(Context ctx, String name) throws DatabaseConnectionException, DatabaseReadException {
        Map<String, AttributeInfo> info = fetchAttributes(ctx);
        return Optional.ofNullable(info.get(name));
    }
}
