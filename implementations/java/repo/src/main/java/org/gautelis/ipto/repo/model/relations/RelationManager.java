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
import org.gautelis.ipto.repo.exceptions.AssociationTypeException;
import org.gautelis.ipto.repo.exceptions.DatabaseConnectionException;
import org.gautelis.ipto.repo.exceptions.DatabaseReadException;
import org.gautelis.ipto.repo.exceptions.DatabaseWriteException;
import org.gautelis.ipto.repo.exceptions.InvalidParameterException;
import org.gautelis.ipto.repo.model.AssociationType;
import org.gautelis.ipto.repo.model.Context;
import org.gautelis.ipto.repo.model.RelationType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.LinkedList;

/**
 * Internal helper for loading and removing repository relations.
 * <p>
 * The manager uses the repository-wide directional convention:
 * right relations are the relations stored on, or followed from, a known unit;
 * left relations are the inverse view, that is, units that point to a known
 * related unit.
 */
public class RelationManager {
    private static final Logger log = LoggerFactory.getLogger(RelationManager.class);

    /**
     * Removes all relations for specified unit.
     *
     * @param ctx repository context
     * @param conn active connection participating in a wider transaction
     * @param tenantId tenant identifier
     * @param unitId unit identifier
     * @throws DatabaseWriteException if relation cleanup fails
     */
    /* package accessible only */
    public static void removeAllRelations(
            Context ctx,
            Connection conn,
            int tenantId,
            long unitId
    ) throws DatabaseWriteException {
        try (PreparedStatement pStmt = conn.prepareStatement(ctx.getStatements().removeAllInternalRelations())) {
            int i = 0;
            pStmt.setInt(++i, tenantId);
            pStmt.setLong(++i, unitId);
            pStmt.setInt(++i, tenantId); // reltenantid
            pStmt.setLong(++i, unitId); // relunitid
            Database.executeUpdate(pStmt);
        } catch (SQLException sqle) {
            throw new DatabaseWriteException(sqle);
        }

        if (log.isDebugEnabled()) {
            log.debug("Removed all relations from/to: {}.{}", tenantId, unitId);
        }
    }

    private static Relation resurrectRelation(ResultSet rs) throws DatabaseReadException, AssociationTypeException {
        try {
            int _relType = rs.getInt("reltype");
            RelationType relType = RelationType.of(_relType);

            return switch (relType) {
                case PARENT_CHILD_RELATION -> {
                    log.trace("Resurrecting parent-child relation from resultset");
                    yield new ParentChildRelation(rs);
                }
                case REPLACEMENT_RELATION -> {
                    log.trace("Resurrecting replacement relation from resultset");
                    yield new ReplacementRelation(rs);
                }
                default -> {
                    log.warn("Can not resurrect relation of type {}", relType);
                    yield null;
                }
            };
        } catch (SQLException sqle) {
            throw new DatabaseReadException(sqle);
        }
    }

    /**
     * Returns one right-side relation of the requested type for a known
     * left-side unit.
     *
     * @param ctx repository context
     * @param tenantId tenant id of the left-side unit
     * @param unitId unit id of the left-side unit
     * @param relType relation type to resolve
     * @return one matching right-side relation, or {@code null} if absent
     * @throws DatabaseConnectionException if a database connection cannot be obtained
     * @throws DatabaseReadException if relation lookup fails
     * @throws InvalidParameterException if the request is invalid
     */
    /* package accessible only */
    static Relation getRightRelation(
            Context ctx, int tenantId, long unitId, RelationType relType
    ) throws DatabaseConnectionException, DatabaseReadException, InvalidParameterException {

        Relation[] relation = { null };

        Database.useReadonlyPreparedStatement(ctx.getDataSource(), ctx.getStatements().getRightInternalRelation(), pStmt -> {
            int i = 0;
            pStmt.setInt(++i, tenantId);
            pStmt.setLong(++i, unitId);
            pStmt.setInt(++i, relType.getType());
            try (ResultSet rs = Database.executeQuery(pStmt)) {
                if (rs.next()) {
                    relation[0] = resurrectRelation(rs);
                }
            }
        });

        return relation[0];
    }

    /**
     * Returns all right-side relations of the requested type for a known
     * left-side unit.
     * <p>
     * For example, for {@code PARENT_CHILD_RELATION}, a right lookup on a
     * parent returns its children.
     *
     * @param ctx repository context
     * @param tenantId tenant id of the left-side unit
     * @param unitId unit id of the left-side unit
     * @param relType relation type to resolve
     * @return matching right-side relations
     * @throws DatabaseConnectionException if a database connection cannot be obtained
     * @throws DatabaseReadException if relation lookup fails
     * @throws InvalidParameterException if the request is invalid
     */
    public static Collection<Relation> getRightRelations(
            Context ctx, int tenantId, long unitId, RelationType relType
    ) throws DatabaseConnectionException, DatabaseReadException, InvalidParameterException {

        Collection<Relation> v = new LinkedList<>();

        Database.useReadonlyPreparedStatement(ctx.getDataSource(), ctx.getStatements().getAllRightInternalRelations(), pStmt -> {
            int i = 0;
            pStmt.setInt(++i, tenantId);
            pStmt.setLong(++i, unitId);
            pStmt.setInt(++i, relType.getType());
            try (ResultSet rs = Database.executeQuery(pStmt)) {
                while (rs.next()) {
                    Relation relation = resurrectRelation(rs);
                    if (relation != null) {
                        v.add(relation);
                    }
                }
            }
        });

        return v;
    }

    /**
     * Counts right-side relations of one type for a known left-side unit.
     *
     * @param ctx repository context
     * @param tenantId tenant id of the left-side unit
     * @param unitId unit id of the left-side unit
     * @param relType relation type to count
     * @return number of matching right-side relations
     * @throws DatabaseConnectionException if a database connection cannot be obtained
     * @throws DatabaseReadException if relation lookup fails
     * @throws InvalidParameterException if the request is invalid
     */
    /* package accessible only */
    static int countRightRelations(
            Context ctx, int tenantId, long unitId, RelationType relType
    ) throws DatabaseConnectionException, DatabaseReadException, InvalidParameterException {

        int[] count = {0};

        Database.useReadonlyPreparedStatement(ctx.getDataSource(), ctx.getStatements().countRightInternalRelations(), pStmt -> {
            int i = 0;
            pStmt.setInt(++i, tenantId);
            pStmt.setLong(++i, unitId);
            pStmt.setInt(++i, relType.getType());
            try (ResultSet rs = Database.executeQuery(pStmt)) {
                if (rs.next()) {
                    count[0] = rs.getInt(1);
                }
            }
        });

        return count[0];
    }

    /**
     * Counts left-side relations of one type for a known right-side unit.
     * <p>
     * For example, for {@code PARENT_CHILD_RELATION}, a left count on a child
     * reports how many parents point to it.
     *
     * @param ctx repository context
     * @param relType relation type to count
     * @param relTenantId tenant id of the right-side unit
     * @param relUnitId unit id of the right-side unit
     * @return number of matching left-side relations
     * @throws DatabaseConnectionException if a database connection cannot be obtained
     * @throws DatabaseReadException if relation lookup fails
     * @throws InvalidParameterException if the request is invalid
     */
    /* package accessible only */
    static int countLeftRelations(
            Context ctx, RelationType relType, int relTenantId, long relUnitId
    ) throws DatabaseConnectionException, DatabaseReadException, InvalidParameterException {

        int[] count = {0};

        Database.useReadonlyPreparedStatement(ctx.getDataSource(), ctx.getStatements().countLeftInternalRelations(), pStmt -> {
            int i = 0;
            pStmt.setInt(++i, relType.getType());
            pStmt.setInt(++i, relTenantId);
            pStmt.setLong(++i, relUnitId);
            try (ResultSet rs = Database.executeQuery(pStmt)) {
                if (rs.next()) {
                    count[0] = rs.getInt(1);
                }
            }
        });

        return count[0];
    }

    /**
     * Returns all left-side relations of the requested type for a known
     * right-side unit.
     * <p>
     * For example, for {@code PARENT_CHILD_RELATION}, a left lookup on a child
     * returns the parents that point to it.
     *
     * @param ctx repository context
     * @param relType relation type to resolve
     * @param relTenantId tenant id of the right-side unit
     * @param relUnitId unit id of the right-side unit
     * @return matching left-side relations
     * @throws DatabaseConnectionException if a database connection cannot be obtained
     * @throws DatabaseReadException if relation lookup fails
     * @throws InvalidParameterException if the request is invalid
     */
    /* package accessible only */
    Collection<Relation> getLeftRelations(
            Context ctx, RelationType relType, int relTenantId, long relUnitId
    ) throws DatabaseConnectionException, DatabaseReadException, InvalidParameterException {

        Collection<Relation> v = new LinkedList<>();

        Database.useReadonlyPreparedStatement(ctx.getDataSource(), ctx.getStatements().getAllLeftInternalRelations(), pStmt -> {
            int i = 0;
            pStmt.setInt(++i, relType.getType());
            pStmt.setInt(++i, relTenantId);
            pStmt.setLong(++i, relUnitId);
            try (ResultSet rs = Database.executeQuery(pStmt)) {
                while (rs.next()) {
                    Relation relation = resurrectRelation(rs);
                    if (relation != null) {
                        v.add(relation);
                    }
                }
            }
        });

        return v;
    }
}
