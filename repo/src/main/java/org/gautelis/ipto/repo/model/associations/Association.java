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
package org.gautelis.ipto.repo.model.associations;

import org.gautelis.ipto.repo.db.Database;
import org.gautelis.ipto.repo.exceptions.*;
import org.gautelis.ipto.repo.model.AssociationType;
import org.gautelis.ipto.repo.model.Context;
import org.gautelis.ipto.repo.model.Unit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;

/**
 * This is any kind of association between a unit and some external entity.
 * <p>
 * The single/multiple association integrity is maintained
 * internally in AssociationManager.
 * <p>
 * It is always safe to create an association of a specific
 * type for a unit since existing associations are removed
 * if the specified association type does not support
 * multiple associations.
 */
public class Association {
    private static final Logger log = LoggerFactory.getLogger(Association.class);

    private final int tenantId;
    private final long unitId;
    private final AssociationType type;
    private final String assocString;

    // Used when resurrecting association
    /* package accessible only */
    Association(ResultSet rs) throws DatabaseReadException, AssociationTypeException {
        try {
            tenantId = rs.getInt("tenantid");
            unitId = rs.getLong("unitid");
            int _assocType = rs.getInt("assoctype");
            type = AssociationType.of(_assocType);
            assocString = rs.getString("assocstring");
            // ignore the assocId
        } catch (SQLException sqle) {
            throw new DatabaseReadException(sqle);
        }
    }

    /**
     * Creates an association to an external entity.
     *
     * @throws InvalidParameterException
     */
    /* Should be package accessible only */
    public static void create(
            Context ctx,
            int tenantId,
            long unitId,
            AssociationType assocType,
            String assocString
    ) throws DatabaseConnectionException, DatabaseWriteException, InvalidParameterException, ConfigurationException {

        try (Connection conn = ctx.getDataSource().getConnection()) {
            conn.setReadOnly(false);
            conn.setAutoCommit(false);

            try {
                // Prepare insert by (possibly) removing all existing (right)
                // associations of this type.
                if (!assocType.allowsMultiples()) {
                    // There can be only one...
                    try (PreparedStatement pStmt = conn.prepareStatement(ctx.getStatements().removeAllRightExternalAssocs())) {
                        int i = 0;
                        pStmt.setInt(++i, tenantId);
                        pStmt.setLong(++i, unitId);
                        pStmt.setInt(++i, assocType.getType());
                        Database.executeUpdate(pStmt);
                    }
                }

                // Insert association
                try (PreparedStatement pStmt = conn.prepareStatement(ctx.getStatements().storeExternalAssoc())) {
                    int i = 0;
                    pStmt.setInt(++i, tenantId);
                    pStmt.setLong(++i, unitId);
                    pStmt.setInt(++i, assocType.getType());
                    pStmt.setString(++i, assocString);
                    Database.executeUpdate(pStmt);
                }

                conn.commit();

                if (log.isTraceEnabled()) {
                    log.trace("Created association {} ({}) from {} to {}",
                            assocType, assocType.getType(), Unit.id2String(tenantId, unitId), assocString);
                }

            } catch (SQLException sqle) {
                // Were we violating the integrity constraint? (23000)
                if (sqle.getSQLState() != null && sqle.getSQLState().startsWith("23")) {
                    // This should *not* happen since we qualify each association with a
                    // unique id.
                    log.warn("Duplicate association from {} to {}", Unit.id2String(tenantId, unitId), assocString);

                } else {
                    conn.rollback();

                    log.error("Transaction rollback due to: {}", Database.squeeze(sqle));
                    throw new DatabaseWriteException(sqle);
                }
            }
        } catch (SQLException sqle) {
            throw new DatabaseConnectionException(sqle);
        }
    }

    /**
     * Removes a specific association.
     * <p>
     * If multiple associations are allowed, the remaining associations
     * are left intact.
     *
     * @throws InvalidParameterException
     */
    /* Should be package accessible only */
    public static void remove(
            Context ctx,
            int tenantId,
            long unitId,
            AssociationType assocType,
            String assocString
    ) throws DatabaseConnectionException, DatabaseWriteException, InvalidParameterException {
        Objects.requireNonNull(assocType, "assocType");
        Objects.requireNonNull(assocString, "assocString");

        if (assocString.isEmpty()) {
            throw new InvalidParameterException("Invalid empty association string");
        }

        try (Connection conn = ctx.getDataSource().getConnection()) {
            conn.setReadOnly(false);
            conn.setAutoCommit(false);

            // Get all associations to assocString
            try (PreparedStatement pStmt1 = conn.prepareStatement(ctx.getStatements().getAllSpecificExternalAssocs())) {
                int i = 0;
                pStmt1.setInt(++i, tenantId);
                pStmt1.setLong(++i, unitId);
                pStmt1.setInt(++i, assocType.getType());
                pStmt1.setString(++i, assocString);

                try (ResultSet rs = Database.executeQuery(pStmt1)) {
                    if (rs.next()) {
                        try (PreparedStatement pStmt2 = conn.prepareStatement(ctx.getStatements().removeSpecificExternalAssoc())) {
                            int j = 0;
                            pStmt2.setInt(++j, tenantId);
                            pStmt2.setLong(++j, unitId);
                            pStmt2.setInt(++j, assocType.getType());
                            pStmt2.setString(++j, assocString);
                            Database.executeUpdate(pStmt2);

                            conn.commit();

                            if (log.isTraceEnabled()) {
                                log.trace("Removed association {} ({}) from {} to {}",
                                        assocType, assocType.getType(), Unit.id2String(tenantId, unitId), assocString);
                            }
                        }
                    } else /* empty resultset */ {
                        // Could be worth noting...
                        log.info("Ignoring request to remove void association {} from {} to {}",
                                assocType, Unit.id2String(tenantId, unitId), assocString);
                    }
                }
            } catch (SQLException sqle) {
                conn.rollback();

                log.error("Transaction rollback due to: {}", Database.squeeze(sqle));
                throw new DatabaseWriteException(sqle);
            }
        } catch (SQLException sqle) {
            throw new DatabaseConnectionException(sqle);
        }
    }

    public String getAssocString() {
        return assocString;
    }

    public AssociationType getType() {
        return type;
    }

    public int getTenantId() {
        return tenantId;
    }

    public long getUnitId() {
        return unitId;
    }

    public String toString() {
        return "Association " + getType()
                + " (" + getType().getType() + ") "
                + " from " + Unit.id2String(getTenantId(), getUnitId())
                + " to " + assocString;
    }
}
