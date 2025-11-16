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
package org.gautelis.repo.model.associations;


import org.gautelis.repo.db.Database;
import org.gautelis.repo.exceptions.*;
import org.gautelis.repo.model.AssociationType;
import org.gautelis.repo.model.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.LinkedList;

/* Should be package accessible only */
public class AssociationManager {

    private static final Logger log = LoggerFactory.getLogger(AssociationManager.class);


    /**
     * Removes all associations (of all types) for specified unit
     * <p>
     * Since this method is normally called during unit destruction,
     * we will accept a connection on which to operate to facilitate
     * transaction handling. Be sure to call conn.commit() or
     * conn.rollback() sometime after this call.
     *
     * @param conn a connection on which to operate
     * @throws DatabaseWriteException if problems to write data
     * <p>
     */
    /* Should be package accessible only */
    public static void removeAllAssociations(
            Context ctx,
            Connection conn,
            int tenantId,
            long unitId
    ) throws DatabaseWriteException {

        // Remove all internal associations where the specified unit
        // either sits to the left (right associations) or to the
        // right (left associations) in the table.

        try (PreparedStatement pStmt = conn.prepareStatement(ctx.getStatements().assocRemoveAllInternalAssocs())) {
            int i = 0;
            pStmt.setInt(++i, tenantId);
            pStmt.setLong(++i, unitId);
            pStmt.setInt(++i, tenantId); // assoctenantid
            pStmt.setLong(++i, unitId); // assocunitid
            Database.executeUpdate(pStmt);

        } catch (SQLException sqle) {
            throw new DatabaseWriteException(sqle);
        }

        // Remove all external associations where the specified unit
        // sits to the left (right association) in the table.
        try (PreparedStatement pStmt = conn.prepareStatement(ctx.getStatements().assocRemoveAllExternalAssocs())) {
            int i = 0;
            pStmt.setInt(++i, tenantId);
            pStmt.setLong(++i, unitId);
            Database.executeUpdate(pStmt);

        } catch (SQLException sqle) {
            throw new DatabaseWriteException(sqle);
        }

        if (log.isDebugEnabled()) {
            log.debug("Removed all associations from/to: {}.{}", tenantId, unitId);
        }
    }

    /**
     * Resurrects associations from a recordset.
     * <p>
     * @throws DatabaseReadException if problems to read data
     * @throws AssociationTypeException if read association is unknown
     */
    private static Association resurrectAssoc(ResultSet rs) throws DatabaseReadException, AssociationTypeException {
        try {
            int _assocType = rs.getInt("assoctype");
            AssociationType assocType = AssociationType.of(_assocType);

            Association assoc;

            switch (assocType) {
                case PARENT_CHILD_RELATION -> {
                    log.trace("Resurrecting parent-child relation from resultset");
                    assoc = new ParentChildRelation(rs);
                }
                case CASE_ASSOCIATION -> {
                    log.trace("Resurrecting case association from resultset");
                    assoc = new CaseAssociation(rs);
                }
                case REPLACEMENT_RELATION -> {
                    log.trace("Resurrecting replacement relation from resultset");
                    assoc = new ReplacementRelation(rs);
                }
                default -> {
                    log.warn("Can not resurrect association of unknown type");
                    assoc = null; // Currently so
                }
            }
            return assoc;
        } catch (SQLException sqle) {
            throw new DatabaseReadException(sqle);
        }
    }


    /**
     * Gets <B>one</B> right association of specified type from the
     * specified unit.
     * <p>
     * If multiple associations are allowed, only the <I>first</I>
     * in some respect will be returned. No ordering is done so this
     * method will return an unspecified association.
     * <p>
     * A typical right (internal) association could be the (one)
     * parent container where a unit is located.
     *
     * @throws InvalidParameterException if assocType is invalid
     * @throws DatabaseConnectionException if problems with acquiring connection (or rolling back)
     * @throws DatabaseReadException if problems to read data
     */
    /* package accessible only */
    static Association getRightAssociation(
            Context ctx, int tenantId, long unitId, AssociationType assocType
    ) throws DatabaseConnectionException, DatabaseReadException, InvalidParameterException {

        if (assocType == AssociationType.UNKNOWN) {
            throw new InvalidParameterException("Unknown association type");
        }

        String statement;
        if (assocType.isRelational()) {
            statement = ctx.getStatements().assocGetRightInternalAssoc();
        } else {
            statement = ctx.getStatements().assocGetAllRightExternalAssocs();
        }

        Association[] association = { null };

        Database.useReadonlyPreparedStatement(ctx.getDataSource(), statement, pStmt -> {
            int i = 0;
            pStmt.setInt(++i, tenantId);
            pStmt.setLong(++i, unitId);
            pStmt.setInt(++i, assocType.getType());
            try (ResultSet rs = Database.executeQuery(pStmt)) {
                // Pick first if several rows
                if (rs.next()) {
                    association[0] = resurrectAssoc(rs);
                }
            }
        });

        return association[0];
    }


    /**
     * Gets one (if only one exists) or many (if multiple exists) right associations
     * of the specified type for the specified unit.
     * <p>
     * A typical right internal association could be the one parent container
     * where a unit is placed. A typical right external association
     * could be the (possibly multiple) cases that a unit is associated with.
     *
     * @throws InvalidParameterException if assocType is invalid
     * @throws DatabaseConnectionException if problems with acquiring connection (or rolling back)
     * @throws DatabaseReadException if problems to read data
     */
    /* Should be package accessible only */
    public static Collection<Association> getRightAssociations(
            Context ctx, int tenantId, long unitId, AssociationType assocType
    ) throws DatabaseConnectionException, DatabaseReadException, InvalidParameterException {

        if (assocType == AssociationType.UNKNOWN) {
            throw new InvalidParameterException("Unknown association type");
        }

        String statement;
        if (assocType.isRelational()) {
            statement = ctx.getStatements().assocGetAllRightInternalAssocs();
        } else {
            statement = ctx.getStatements().assocGetAllRightExternalAssocs();
        }

        Collection<Association> v = new LinkedList<>();

        Database.useReadonlyPreparedStatement(ctx.getDataSource(), statement, pStmt -> {
            int i = 0;
            pStmt.setInt(++i, tenantId);
            pStmt.setLong(++i, unitId);
            pStmt.setInt(++i, assocType.getType());
            try (ResultSet rs = Database.executeQuery(pStmt)) {
                while (rs.next()) {
                    v.add(resurrectAssoc(rs));
                }
            }
        });

        return v;
    }


    /**
     * Counts right associations of the specified type for the specified unit.
     *
     * @throws InvalidParameterException if assocType is invalid
     * @throws DatabaseConnectionException if problems with acquiring connection (or rolling back)
     * @throws DatabaseReadException if problems to read data
     */
    /* package accessible only */
    static int countRightAssociations(
            Context ctx, int tenantId, long unitId, AssociationType assocType
    ) throws DatabaseConnectionException, DatabaseReadException, InvalidParameterException {

        if (assocType == AssociationType.UNKNOWN) {
            throw new InvalidParameterException("Unknown association type");
        }

        String statement;
        if (assocType.isRelational()) {
            statement = ctx.getStatements().assocCountRightInternalAssocs();
        } else {
            statement = ctx.getStatements().assocCountRightExternalAssocs();
        }

        int[] count = {0};

        Database.useReadonlyPreparedStatement(ctx.getDataSource(), statement, pStmt -> {
            int i = 0;
            pStmt.setInt(++i, tenantId);
            pStmt.setLong(++i, unitId);
            pStmt.setInt(++i, assocType.getType());
            try (ResultSet rs = Database.executeQuery(pStmt)) {
                if (rs.next()) {
                    count[0] = rs.getInt(1);
                }
            }
        });

        return count[0];
    }

    /**
     * Counts left internal associations of the specified type for the specified unit.
     *
     * @throws InvalidParameterException if assocType is invalid or relational
     * @throws DatabaseConnectionException if problems with acquiring connection (or rolling back)
     * @throws DatabaseReadException if problems to read data
     */
    /* package accessible only */
    static int countLeftAssociations(
            Context ctx, AssociationType assocType, int assocTenantId, long assocUnitId
    ) throws DatabaseConnectionException, DatabaseReadException, InvalidParameterException {

        if (assocType == AssociationType.UNKNOWN) {
            throw new InvalidParameterException("Unknown association type");
        }

        if (!assocType.isRelational()) {
            throw new InvalidParameterException("Invalid association type. Association is external.");
        }

        int[] count = {0};

        Database.useReadonlyPreparedStatement(ctx.getDataSource(), ctx.getStatements().assocCountLeftInternalAssocs(), pStmt -> {
            int i = 0;
            pStmt.setInt(++i, assocType.getType());
            pStmt.setInt(++i, assocTenantId);
            pStmt.setLong(++i, assocUnitId);
            try (ResultSet rs = Database.executeQuery(pStmt)) {
                if (rs.next()) {
                    count[0] = rs.getInt(1);
                }
            }
        });

        return count[0];
    }

    /**
     * Counts left external associations of the specified type for the specified unit.
     *
     * @throws InvalidParameterException if assocType is invalid or relational
     * @throws DatabaseConnectionException if problems with acquiring connection (or rolling back)
     * @throws DatabaseReadException if problems to read data
     */
    /* package accessible only */
    static int countLeftAssociations(
            Context ctx, AssociationType assocType, String assocString
    ) throws DatabaseConnectionException, DatabaseReadException, InvalidParameterException {

        if (assocType == AssociationType.UNKNOWN) {
            throw new InvalidParameterException("Unknown association type");
        }

        if (assocType.isRelational()) {
            throw new InvalidParameterException("Invalid association type. Association is internal.");
        }

        int[] count = {0};

        Database.useReadonlyPreparedStatement(ctx.getDataSource(), ctx.getStatements().assocCountLeftExternalAssocs(), pStmt -> {
            int i = 0;
            pStmt.setInt(++i, assocType.getType());
            pStmt.setString(++i, assocString);
            try (ResultSet rs = Database.executeQuery(pStmt)) {
                rs.next();
                count[0] = rs.getInt(1);
            }
        });

        return count[0];
    }

    /**
     * Gets all left internal associations of the specified type for the specified unit.
     * <p>
     * A typical left internal association could be the (multiple)
     * units placed in a specific container.
     *
     * @throws InvalidParameterException if assocType is invalid or relational
     * @throws DatabaseConnectionException if problems with acquiring connection (or rolling back)
     * @throws DatabaseReadException if problems to read data
     */
    /* package accessible only */
    Collection<Association> getLeftAssociations(
            Context ctx, AssociationType assocType, int assocTenantId, long assocUnitId
    ) throws DatabaseConnectionException, DatabaseReadException, InvalidParameterException {

        if (assocType == AssociationType.UNKNOWN) {
            throw new InvalidParameterException("Unknown association type");
        }

        if (!assocType.isRelational()) {
            throw new InvalidParameterException("Invalid association type. Association is external.");
        }

        Collection<Association> v = new LinkedList<>();

        Database.useReadonlyPreparedStatement(ctx.getDataSource(), ctx.getStatements().assocGetAllLeftInternalAssocs(), pStmt -> {
            int i = 0;
            pStmt.setInt(++i, assocType.getType());
            pStmt.setInt(++i, assocTenantId);
            pStmt.setLong(++i, assocUnitId);
            try (ResultSet rs = Database.executeQuery(pStmt)) {
                while (rs.next()) {
                    v.add(resurrectAssoc(rs));
                }
            }
        });

        return v;
    }

    /**
     * Gets all left external associations of the specified type for the specified unit.
     * <p>
     * A typical left external association could be the (possibly multiple) records
     * associated with a specific case.
     *
     * @throws InvalidParameterException if assocType is invalid or relational
     * @throws DatabaseConnectionException if problems with acquiring connection (or rolling back)
     * @throws DatabaseReadException if problems to read data
     */
    /* package accessible only */
    Collection<Association> getLeftAssociations(
            Context ctx, AssociationType assocType, String assocString
    ) throws DatabaseConnectionException, DatabaseReadException, InvalidParameterException {

        if (assocType == AssociationType.UNKNOWN) {
            throw new InvalidParameterException("Unknown association type");
        }

        if (assocType.isRelational()) {
            throw new InvalidParameterException("Invalid association type. Association is internal.");
        }

        Collection<Association> v = new LinkedList<>();

        Database.useReadonlyPreparedStatement(ctx.getDataSource(), ctx.getStatements().assocGetAllLeftExternalAssocs(), pStmt -> {
            int i = 0;
            pStmt.setInt(++i, assocType.getType());
            pStmt.setString(++i, assocString);
            try (ResultSet rs = Database.executeQuery(pStmt)) {
                while (rs.next()) {
                    v.add(resurrectAssoc(rs));
                }
            }
        });

        return v;
    }
}
