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
package org.gautelis.ipto.repo.model.relations;

import org.gautelis.ipto.repo.db.Database;
import org.gautelis.ipto.repo.exceptions.*;
import org.gautelis.ipto.repo.model.AssociationType;
import org.gautelis.ipto.repo.model.Context;
import org.gautelis.ipto.repo.model.RelationType;
import org.gautelis.ipto.repo.model.Unit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * This is any kind of relation between a unit and another unit.
 * <p>
 * Relations are directional. In the Java API, the unit identified by
 * {@code tenantId}/{@code unitId} is the left side of the relation, while the
 * unit identified by {@code relationTenantId}/{@code relationUnitId} is the
 * right side. A "right" lookup therefore starts from a unit and follows the
 * relation outward to the related unit. A corresponding "left" lookup starts
 * from the related unit and finds units that point to it.
 * <p>
 * The single/multiple relation integrity is maintained
 * internally in RelationManager.
 */
public class Relation {
    private static final Logger log = LoggerFactory.getLogger(Relation.class);

    private final int tenantId;
    private final long unitId;
    private final RelationType type;

    private final int relationTenantId;
    private final long relationUnitId;

    // Used when resurrecting association
    /* package accessible only */
    Relation(ResultSet rs) throws DatabaseReadException, AssociationTypeException {
        try {
            tenantId = rs.getInt("tenantid");
            unitId = rs.getLong("unitid");
            int _relationType = rs.getInt("reltype");
            type = RelationType.of(_relationType);
            relationTenantId = rs.getInt("reltenantid");
            relationUnitId = rs.getLong("relunitid");
        } catch (SQLException sqle) {
            throw new DatabaseReadException(sqle);
        }
    }

    /**
     * Creates a relation between units.
     *
     * @param ctx repository context
     * @param tenantId tenant identifier of the left-side unit
     * @param unitId unit identifier of the left-side unit
     * @param relationType relation type to create
     * @param relationTenantId tenant identifier of the right-side unit
     * @param relationUnitId unit identifier of the right-side unit
     * @throws DatabaseConnectionException if a database connection cannot be obtained
     * @throws DatabaseWriteException if the relation cannot be written
     * @throws InvalidParameterException if the relation request is invalid
     * @throws ConfigurationException if repository configuration is inconsistent
     */
    /* Should be package accessible only */
    public static void create(
            Context ctx,
            int tenantId,
            long unitId,
            RelationType relationType,
            int relationTenantId,
            long relationUnitId
    ) throws DatabaseConnectionException, DatabaseWriteException, InvalidParameterException, ConfigurationException {

        try (Connection conn = ctx.getDataSource().getConnection()) {
            conn.setReadOnly(false);
            conn.setAutoCommit(false);

            try {
                // Prepare insert by (possibly) removing all existing (right)
                // associations of this type.
                if (!relationType.allowsMultiples()) {
                    // There can be only one...
                    try (PreparedStatement pStmt = conn.prepareStatement(ctx.getStatements().removeAllRightInternalRelations())) {
                        int i = 0;
                        pStmt.setInt(++i, tenantId);
                        pStmt.setLong(++i, unitId);
                        pStmt.setInt(++i, relationType.getType());
                        Database.executeUpdate(pStmt);
                    }
                }

                // Insert association
                try (PreparedStatement pStmt = conn.prepareStatement(ctx.getStatements().storeInternalRelation())) {
                    int i = 0;
                    pStmt.setInt(++i, tenantId);
                    pStmt.setLong(++i, unitId);
                    pStmt.setInt(++i, relationType.getType());
                    pStmt.setInt(++i, relationTenantId);
                    pStmt.setLong(++i, relationUnitId);
                    Database.executeUpdate(pStmt);
                }
                conn.commit();

                if (log.isTraceEnabled()) {
                    log.trace("Created relation {} from {} to {}",
                            relationType, Unit.id2String(tenantId, unitId), Unit.id2String(relationTenantId, relationUnitId));
                }

            } catch (SQLException sqle) {
                // Were we violating the integrity constraint? (23000)
                if (sqle.getSQLState() != null && sqle.getSQLState().startsWith("23")) {
                    // Relation already exists - ignore
                    log.debug("Relation {} already exists from {}", relationType, Unit.id2String(tenantId, unitId));

                } else {
                    conn.rollback();

                    log.error("Transaction rollback due to: {}", Database.squeeze(sqle));
                    throw new DatabaseWriteException(sqle);
                }
            }
        } catch (SQLException sqle) {
            String info = "Failed to rollback: " + Database.squeeze(sqle);
            throw new DatabaseConnectionException(info, sqle);
        }
    }


    /**
     * Removes a specific relation.
     * <p>
     * If multiple associations are allowed, the remaining associations
     * are left intact.
     *
     * @param ctx repository context
     * @param tenantId tenant identifier of the left-side unit
     * @param unitId unit identifier of the left-side unit
     * @param relationType relation type to remove
     * @param relationTenantId tenant identifier of the right-side unit
     * @param relationUnitId unit identifier of the right-side unit
     * @throws DatabaseConnectionException if a database connection cannot be obtained
     * @throws DatabaseWriteException if the relation cannot be removed
     * @throws InvalidParameterException if the relation request is invalid
     */
    /* Should be package accessible only */
    public static void remove(
            Context ctx,
            int tenantId,
            long unitId,
            AssociationType relationType,
            int relationTenantId,
            long relationUnitId
    ) throws DatabaseConnectionException, DatabaseWriteException, InvalidParameterException {

        Database.usePreparedStatement(ctx.getDataSource(), ctx.getStatements().removeSpecificInternalRelation(), pStmt -> {
            int i = 0;
            pStmt.setInt(++i, tenantId);
            pStmt.setLong(++i, unitId);
            pStmt.setInt(++i, relationType.getType());
            pStmt.setInt(++i, relationTenantId);
            pStmt.setLong(++i, relationUnitId);
            Database.executeUpdate(pStmt);

            if (log.isTraceEnabled()) {
                log.trace("Removed relation {} from {}", relationType, Unit.id2String(tenantId, unitId));
            }
        });
    }

    /**
     * Returns the tenant id of the right-side unit.
     *
     * @return right-side tenant id
     */
    public int getRelationTenantId() {
        return relationTenantId;
    }

    /**
     * Returns the unit id of the right-side unit.
     *
     * @return right-side unit id
     */
    public long getRelationUnitId() {
        return relationUnitId;
    }

    /**
     * Returns the relation type.
     *
     * @return relation type
     */
    public RelationType getType() {
        return type;
    }

    /**
     * Returns the tenant id of the left-side unit.
     *
     * @return left-side tenant id
     */
    public int getTenantId() {
        return tenantId;
    }

    /**
     * Returns the unit id of the left-side unit.
     *
     * @return left-side unit id
     */
    public long getUnitId() {
        return unitId;
    }

    public String toString() {
        return getType()
                + " (" + getType().getType() + ") "
                + " from " + Unit.id2String(getTenantId(), getUnitId())
                + " to " + Unit.id2String(relationTenantId, relationUnitId);
    }
}
