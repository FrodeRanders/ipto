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
package org.gautelis.repo.model.locks;

import org.gautelis.repo.db.Database;
import org.gautelis.repo.exceptions.*;
import org.gautelis.repo.model.Context;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;

/**
 * Wraps lock information.
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
     * Is unit locked?
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
     * Lock unit.
     *
     * @param purpose purpose of lock
     * @return true if lock was successfully placed on unit, false otherwise
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
                pStmt.setString(++i, purpose);
                pStmt.setInt(++i, type.getType());
                pStmt.setNull(++i, java.sql.Types.TIMESTAMP); // ignore -- lock is infinite
                Database.executeUpdate(pStmt);
            });
            return true;
        }
    }

    /**
     * Gets information on locks.
     *
     * @return LockInfo containing the information
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
                    int _type = rs.getInt("type");
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
     * Unlock unit.
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
     * Get purpose of lock, i.e. some informational
     * note on why lock was placed. This information is
     * most often made automatically and may (in fact)
     * not be very informational.
     */
    public String getPurpose() {
        return purpose;
    }

    /**
     * Get locktype
     */
    public LockType getType() {
        return type;
    }

    /**
     * Get time when lock was placed
     */
    public java.sql.Timestamp getLockTime() {
        return lockTime;
    }

    /**
     * Get time when lock expires if auto-unlock
     * applies to the unit or null if no expiration
     * time is set.
     *
     * @return Timestamp if expiration time apply or null if not
     */
    public java.sql.Timestamp getExpireTime() {
        return expireTime;
    }

    /**
     * Overridden method from @see java.lang.Object
     *
     * @return Returns created String
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



