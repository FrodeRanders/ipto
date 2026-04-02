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
package org.gautelis.ipto.repo.model.locks;

import org.gautelis.ipto.repo.db.Database;
import org.gautelis.ipto.repo.exceptions.*;
import org.gautelis.ipto.repo.model.Context;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;

/**
 * Immutable information about one lock held on a unit.
 * <p>
 * Locks are persisted in the repository and used to coordinate administrative
 * and write-oriented operations on units.
 */
public class Lock {

    /**
     * Purpose of lock
     */
    public final String purpose;

    /**
     * Lock type
     */
    public final LockType type;

    /**
     * Time when lock was placed
     */
    public final java.sql.Timestamp lockTime;

    /**
     * Time when lock expires if auto-unlock applies to unit
     */
    public final java.sql.Timestamp expireTime;

    /* package accessible only */
    Lock(
            String purpose,
            LockType type,
            java.sql.Timestamp lockTime,
            java.sql.Timestamp expireTime
    ) {
        this.purpose = purpose;
        this.type = type;
        this.lockTime = lockTime;
        this.expireTime = expireTime;
    }

    /**
     * Indicates whether a unit is locked at or above one specific lock level.
     *
     * @param ctx the repository context
     * @param tenantId the tenant id
     * @param unitId the unit id
     * @param lockLevel the minimum lock level to check for
     * @return {@code true} if a matching lock exists
     */
    /* Should be package accessible only */
    public static boolean isLocked(Context ctx, int tenantId, long unitId, LockType lockLevel) throws DatabaseConnectionException, DatabaseReadException {

        boolean[] locked = {false};

        Database.useReadonlyPreparedStatement(ctx.getDataSource(), ctx.getStatements().lockGetAll(), pStmt -> {
            int _lockLevel = lockLevel.getType();

            int i = 0;
            pStmt.setInt(++i, tenantId);
            pStmt.setLong(++i, unitId);
            try (ResultSet rs = Database.executeQuery(pStmt)) {
                while (rs.next()) {
                    int lockType = rs.getInt("locktype");
                    if (lockType >= _lockLevel) {
                        locked[0] = true;
                        break;
                    }
                }
            }
        });
        return locked[0];
    }

    /**
     * Indicates whether a unit currently has any lock.
     *
     * @param ctx the repository context
     * @param tenantId the tenant id
     * @param unitId the unit id
     * @return {@code true} if any lock exists for the unit
     */
    /* Should be package accessible only */
    public static boolean isLocked(Context ctx, int tenantId, long unitId) throws DatabaseConnectionException, DatabaseReadException {

        boolean[] locked = {false};

        Database.useReadonlyPreparedStatement(ctx.getDataSource(), ctx.getStatements().lockGetAll(), pStmt -> {
            int i = 0;
            pStmt.setInt(++i, tenantId);
            pStmt.setLong(++i, unitId);
            try (ResultSet rs = Database.executeQuery(pStmt)) {
                if (rs.next()) {
                    locked[0] = true;
                }
            }
        });
        return locked[0];
    }

    /**
     * Places a lock on a unit if it is currently unlocked.
     *
     * @param ctx the repository context
     * @param tenantId the tenant id
     * @param unitId the unit id
     * @param type the lock type to create
     * @param purpose informational purpose text for the lock
     * @return {@code true} if the lock was created, {@code false} if the unit
     *         was already locked
     */
    /* Should be package accessible only */
    public static boolean lock(
            Context ctx,
            int tenantId,
            long unitId,
            LockType type,
            String purpose
    ) throws DatabaseConnectionException, DatabaseWriteException, DatabaseReadException, ConfigurationException {

        if (isLocked(ctx, tenantId, unitId)) {
            // NOT CURRENTLY IMPLEMENTED SINCE WE ONLY
            // SUPPORT LOCKS THAT NEVER EXPIRE AT THE MOMENT.
            // RETURN LOCK FAILURE NOW, BUT RETURN CORRECT
            // VALUE WHEN IMPLEMENTED.
            return false;

        } else {
            // Create lock
            Database.usePreparedStatement(ctx.getDataSource(), ctx.getStatements().lockInsert(), pStmt -> {
                int i = 0;
                pStmt.setInt(++i, tenantId);
                pStmt.setLong(++i, unitId);
                if (null != purpose) {
                    pStmt.setString(++i, purpose);
                } else {
                    pStmt.setNull(++i, java.sql.Types.VARCHAR);
                }
                pStmt.setInt(++i, type.getType());
                pStmt.setNull(++i, java.sql.Types.TIMESTAMP); // ignore -- lock is infinite
                Database.executeUpdate(pStmt);
            });
            return true;
        }
    }

    /**
     * Returns all active locks on one unit.
     *
     * @param ctx the repository context
     * @param tenantId the tenant id
     * @param unitId the unit id
     * @return the active locks
     * @see Lock
     */
    /* Should be package accessible only */
    public static Collection<Lock> getLocks(
            Context ctx, int tenantId, long unitId
    ) throws DatabaseConnectionException, DatabaseReadException {

        Collection<Lock> lockInfo = new ArrayList<>();

        Database.useReadonlyPreparedStatement(ctx.getDataSource(), ctx.getStatements().lockGetAll(), pStmt -> {
            int i = 0;
            pStmt.setInt(++i, tenantId);
            pStmt.setLong(++i, unitId);
            try (ResultSet rs = Database.executeQuery(pStmt)) {

                while (rs.next()) {
                    String purpose = rs.getString("purpose");
                    int _type = rs.getInt("locktype");
                    LockType type = LockType.of(_type);
                    java.sql.Timestamp lockTime = rs.getTimestamp("locktime");
                    java.sql.Timestamp expireTime = rs.getTimestamp("expire");

                    lockInfo.add(new Lock(purpose, type, lockTime, expireTime));
                }
            }
        });
        return lockInfo;
    }

    /**
     * Removes all locks from one unit.
     *
     * @param ctx the repository context
     * @param tenantId the tenant id
     * @param unitId the unit id
     */
    /* Should be package accessible only */
    public static void unlock(Context ctx, int tenantId, long unitId) throws DatabaseConnectionException, DatabaseReadException, DatabaseWriteException {
        if (!isLocked(ctx, tenantId, unitId)) {
            // No lock active for unit
            return;
        }

        Database.usePreparedStatement(ctx.getDataSource(), ctx.getStatements().lockDeleteAll(), pStmt -> {
            int i = 0;
            pStmt.setInt(++i, tenantId);
            pStmt.setLong(++i, unitId);
            Database.executeUpdate(pStmt);
        });
    }

    /**
     * Returns the informational purpose text associated with the lock.
     *
     * @return the lock purpose, if any
     */
    public String getPurpose() {
        return purpose;
    }

    /**
     * Returns the lock type.
     *
     * @return the lock type
     */
    public LockType getType() {
        return type;
    }

    /**
     * Returns the lock creation time.
     *
     * @return the lock timestamp
     */
    public java.sql.Timestamp getLockTime() {
        return lockTime;
    }

    /**
     * Returns the lock expiration time, if any.
     *
     * @return the expiration timestamp, or {@code null} for non-expiring locks
     */
    public java.sql.Timestamp getExpireTime() {
        return expireTime;
    }

    /**
     * Returns a textual description of the lock.
     *
     * @return a diagnostic string representation
     */
    public String toString() {
        String info = type + " lock set on " + lockTime;
        if (expireTime != null) {
            info += ", expiring " + expireTime + ",";
        }
        info += ": " + purpose;
        return info;
    }
}


