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
package org.gautelis.ipto.repo.model.cache;

import org.gautelis.ipto.repo.exceptions.SystemInconsistencyException;
import org.gautelis.ipto.repo.model.Unit;

import java.util.Date;
import java.util.Optional;

/* This cache keeps the LATEST VERSION of a unit */

final class UnitCacheEntry {

    private final int tenantId;
    private final long unitId;
    private final int unitVersion;
    private final String key;
    private final Unit unit;
    private Date date = new Date();
    private int totalAccessCount = 0;
    private int deltaAccessCount = 0;

    /**
     * Creates a cache entry.
     */
    UnitCacheEntry(String _key, Unit _unit) {
        //
        tenantId = _unit.getTenantId();
        unitId = _unit.getUnitId();
        unitVersion = _unit.getVersion();

        //
        key = _key;
        unit = _unit;
    }

    /**
     * Creates a cache entry inheriting statistics from an old entry.
     */
    UnitCacheEntry(String _key, Unit _unit, UnitCacheEntry oldEntry) {
        //
        tenantId = _unit.getTenantId();
        unitId = _unit.getUnitId();
        unitVersion = _unit.getVersion();

        //
        key = _key;
        unit = _unit;

        // Inherit statistics
        if (null != oldEntry) {
            totalAccessCount = oldEntry.getTotalAccessCount();
            deltaAccessCount = oldEntry.getDeltaAccessCount();
        }
    }

    /**
     * Returns key associated with this entry
     */
    String getKey() {
        return key;
    }

    /**
     * Returns a <B>COPY</B> of the unit associated with this entry
     */
    Optional<Unit> getUnit() throws SystemInconsistencyException {
        // Here we may choose to return the unit itself, providing a strong
        // reference to the cached unit (which will stick it to the cache)
        // or a clone.
        // The idea at this point is to return a clone, since the caller may
        // actually modify the object. If it was readonly and we could prevent
        // it from being modified, we do not have to clone.
        // At this moment we clone.
        try {
            return Optional.of((Unit) unit.clone());

        } catch (CloneNotSupportedException cnse) {
            String info = "Failed to clone cached entry (unit): " + unit;
            throw new SystemInconsistencyException(info);
        }
    }

    /**
     * Resets timestamp to Now!
     * <p>
     * This method is called when we have a cache hit.
     */
    void touch() {
        date = new Date();
        totalAccessCount++;
        deltaAccessCount++;
    }

    /**
     * Reset delta access count
     */
    void resetDeltaAccessCount() {
        deltaAccessCount = 0;
    }

    /**
     * Checks if entry is older than 'indice'
     */
    boolean isOlderThan(Date indice) {
        return date.before(indice);
    }

    /**
     * Gets date when connection was last in use (or touched)
     */
    Date getDate() {
        return date;
    }

    /**
     * Gets total number of times entry has been hit (in cache)
     */
    int getTotalAccessCount() {
        return totalAccessCount;
    }

    /**
     * Gets number of times entry has been hit (in cache)
     * since last time resetDeltaAccessCount() was called.
     */
    int getDeltaAccessCount() {
        return deltaAccessCount;
    }

    /**
     * Gets tenantId of contained unit.
     */
    int getTenantId() {
        return tenantId;
    }

    /**
     * Gets unitid of contained unit.
     */
    long getUnitId() {
        return unitId;
    }

    /**
     * Gets version of contained unit.
     */
    int getVersion() {
        return unitVersion;
    }
}
