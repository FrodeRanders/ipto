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

/* Should be package accessible only */
public class RelationManager {
    private static final Logger log = LoggerFactory.getLogger(RelationManager.class);

    /**
     * Removes all relations for specified unit.
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
     * Gets one (if only one exists) or many (if multiple exists) right relations
     * of the specified type for the specified unit.
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
