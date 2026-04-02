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
import org.gautelis.ipto.repo.exceptions.AssociationTypeException;
import org.gautelis.ipto.repo.exceptions.DatabaseConnectionException;
import org.gautelis.ipto.repo.exceptions.DatabaseReadException;
import org.gautelis.ipto.repo.exceptions.DatabaseWriteException;
import org.gautelis.ipto.repo.exceptions.InvalidParameterException;
import org.gautelis.ipto.repo.model.AssociationType;
import org.gautelis.ipto.repo.model.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.LinkedList;

/**
 * Internal helper for loading and removing repository associations.
 * <p>
 * The manager uses the same directional convention as the search subsystem:
 * right associations are retrieved from a known unit out to external
 * identifiers, while left associations are resolved from a known external
 * identifier back to units.
 */
public class AssociationManager {
    private static final Logger log = LoggerFactory.getLogger(AssociationManager.class);

    /**
     * Removes all external associations for specified unit.
     *
     * @param ctx repository context
     * @param conn active connection participating in a wider transaction
     * @param tenantId tenant identifier
     * @param unitId unit identifier
     * @throws DatabaseWriteException if association cleanup fails
     */
    /* package accessible only */
    public static void removeAllAssociations(
            Context ctx,
            Connection conn,
            int tenantId,
            long unitId
    ) throws DatabaseWriteException {
        try (PreparedStatement pStmt = conn.prepareStatement(ctx.getStatements().removeAllExternalAssocs())) {
            int i = 0;
            pStmt.setInt(++i, tenantId);
            pStmt.setLong(++i, unitId);
            Database.executeUpdate(pStmt);
        } catch (SQLException sqle) {
            throw new DatabaseWriteException(sqle);
        }

        if (log.isDebugEnabled()) {
            log.debug("Removed all associations from: {}.{}", tenantId, unitId);
        }
    }

    private static Association resurrectAssociation(ResultSet rs) throws DatabaseReadException, AssociationTypeException {
        try {
            int _assocType = rs.getInt("assoctype");
            AssociationType assocType = AssociationType.of(_assocType);

            return switch (assocType) {
                case CASE_ASSOCIATION -> {
                    log.trace("Resurrecting case association from resultset");
                    yield new CaseAssociation(rs);
                }
                default -> {
                    log.warn("Can not resurrect association of non-association type {}", assocType);
                    yield null;
                }
            };
        } catch (SQLException sqle) {
            throw new DatabaseReadException(sqle);
        }
    }

    /**
     * Returns one right-side association of the requested type for a known
     * left-side unit.
     *
     * @param ctx repository context
     * @param tenantId tenant id of the left-side unit
     * @param unitId unit id of the left-side unit
     * @param assocType association type to resolve
     * @return one matching right-side association, or {@code null} if absent
     * @throws DatabaseConnectionException if a database connection cannot be obtained
     * @throws DatabaseReadException if association lookup fails
     * @throws InvalidParameterException if the request is invalid
     */
    /* package accessible only */
    static Association getRightAssociation(
            Context ctx, int tenantId, long unitId, AssociationType assocType
    ) throws DatabaseConnectionException, DatabaseReadException, InvalidParameterException {

        Association[] association = { null };

        Database.useReadonlyPreparedStatement(ctx.getDataSource(), ctx.getStatements().getAllRightExternalAssocs(), pStmt -> {
            int i = 0;
            pStmt.setInt(++i, tenantId);
            pStmt.setLong(++i, unitId);
            pStmt.setInt(++i, assocType.getType());
            try (ResultSet rs = Database.executeQuery(pStmt)) {
                if (rs.next()) {
                    association[0] = resurrectAssociation(rs);
                }
            }
        });

        return association[0];
    }

    /**
     * Returns all right-side associations of the requested type for a known
     * left-side unit.
     * <p>
     * For example, a right lookup on a unit returns the external case ids, file
     * ids, or similar identifiers associated with that unit.
     *
     * @param ctx repository context
     * @param tenantId tenant id of the left-side unit
     * @param unitId unit id of the left-side unit
     * @param assocType association type to resolve
     * @return matching right-side associations
     * @throws DatabaseConnectionException if a database connection cannot be obtained
     * @throws DatabaseReadException if association lookup fails
     * @throws InvalidParameterException if the request is invalid
     */
    public static Collection<Association> getRightAssociations(
            Context ctx, int tenantId, long unitId, AssociationType assocType
    ) throws DatabaseConnectionException, DatabaseReadException, InvalidParameterException {

        Collection<Association> v = new LinkedList<>();

        Database.useReadonlyPreparedStatement(ctx.getDataSource(), ctx.getStatements().getAllRightExternalAssocs(), pStmt -> {
            int i = 0;
            pStmt.setInt(++i, tenantId);
            pStmt.setLong(++i, unitId);
            pStmt.setInt(++i, assocType.getType());
            try (ResultSet rs = Database.executeQuery(pStmt)) {
                while (rs.next()) {
                    Association association = resurrectAssociation(rs);
                    if (association != null) {
                        v.add(association);
                    }
                }
            }
        });

        return v;
    }

    /**
     * Counts right-side associations of one type for a known left-side unit.
     *
     * @param ctx repository context
     * @param tenantId tenant id of the left-side unit
     * @param unitId unit id of the left-side unit
     * @param assocType association type to count
     * @return number of matching right-side associations
     * @throws DatabaseConnectionException if a database connection cannot be obtained
     * @throws DatabaseReadException if association lookup fails
     * @throws InvalidParameterException if the request is invalid
     */
    /* package accessible only */
    static int countRightAssociations(
            Context ctx, int tenantId, long unitId, AssociationType assocType
    ) throws DatabaseConnectionException, DatabaseReadException, InvalidParameterException {

        int[] count = {0};

        Database.useReadonlyPreparedStatement(ctx.getDataSource(), ctx.getStatements().countRightExternalAssocs(), pStmt -> {
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
     * Counts left-side associations of one type for a known right-side external
     * identifier.
     *
     * @param ctx repository context
     * @param assocType association type to count
     * @param assocString the right-side external identifier
     * @return number of matching left-side associations
     * @throws DatabaseConnectionException if a database connection cannot be obtained
     * @throws DatabaseReadException if association lookup fails
     * @throws InvalidParameterException if the request is invalid
     */
    /* package accessible only */
    static int countLeftAssociations(
            Context ctx, AssociationType assocType, String assocString
    ) throws DatabaseConnectionException, DatabaseReadException, InvalidParameterException {

        int[] count = {0};

        Database.useReadonlyPreparedStatement(ctx.getDataSource(), ctx.getStatements().countLeftExternalAssocs(), pStmt -> {
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
     * Returns all left-side associations of the requested type for a known
     * right-side external identifier.
     * <p>
     * A left lookup answers the inverse question: which units are associated
     * with the supplied external identifier?
     *
     * @param ctx repository context
     * @param assocType association type to resolve
     * @param assocString the right-side external identifier
     * @return matching left-side associations
     * @throws DatabaseConnectionException if a database connection cannot be obtained
     * @throws DatabaseReadException if association lookup fails
     * @throws InvalidParameterException if the request is invalid
     */
    /* package accessible only */
    Collection<Association> getLeftAssociations(
            Context ctx, AssociationType assocType, String assocString
    ) throws DatabaseConnectionException, DatabaseReadException, InvalidParameterException {

        Collection<Association> v = new LinkedList<>();

        Database.useReadonlyPreparedStatement(ctx.getDataSource(), ctx.getStatements().getAllLeftExternalAssocs(), pStmt -> {
            int i = 0;
            pStmt.setInt(++i, assocType.getType());
            pStmt.setString(++i, assocString);
            try (ResultSet rs = Database.executeQuery(pStmt)) {
                while (rs.next()) {
                    Association association = resurrectAssociation(rs);
                    if (association != null) {
                        v.add(association);
                    }
                }
            }
        });

        return v;
    }
}
