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
package org.gautelis.repo.model.cache;

import org.gautelis.repo.db.Database;
import org.gautelis.repo.exceptions.DatabaseConnectionException;
import org.gautelis.repo.exceptions.DatabaseReadException;
import org.gautelis.repo.exceptions.SystemInconsistencyException;
import org.gautelis.repo.model.Context;
import org.gautelis.repo.model.Unit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.ref.SoftReference;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

/**
 *
 */
public final class UnitFactory {
    private static final Logger log = LoggerFactory.getLogger(UnitFactory.class);
    private final static Map<String, SoftReference<UnitCacheEntry>> unitCache = new HashMap<>();

    private final static Object mgrLock = new Object();
    private static UnitCacheManager cacheMgr = null;

    private static final boolean CLASSIC_LOAD = false; // JSON load instead of classic

    /**
     * Checks existence of unit
     */
    public static boolean unitExists(
            Context ctx, int tenantId, long unitId
    ) throws DatabaseConnectionException, DatabaseReadException {

        boolean[] exists = { false };

        Database.useReadonlyPreparedStatement(ctx.getDataSource(), ctx.getStatements().unitExists(), pStmt -> {
            int i = 0;
            pStmt.setInt(++i, tenantId);
            pStmt.setLong(++i, unitId);
            try (ResultSet rs = Database.executeQuery(pStmt)) {
                exists[0] = rs.next();
            }
        });

        return exists[0];
    }

    /**
     * Fetch a unit
     */
    public static Optional<Unit> resurrectUnit(
            Context ctx, int tenantId, long unitId
    ) throws DatabaseConnectionException, DatabaseReadException {
        // First, check cache
        {
            Optional<Unit> unit = cacheLookup(ctx, tenantId, unitId);
            if (unit.isPresent()) {
                return unit;
            }
        }

        if (CLASSIC_LOAD) {
            // Not in cache, continue reading from database
            Unit[] unit = { null };
            Database.useReadonlyPreparedStatement(ctx.getDataSource(), ctx.getStatements().unitGet(), pStmt -> {
                int i = 0;
                pStmt.setInt(++i, tenantId);
                pStmt.setLong(++i, unitId);

                try (ResultSet rs = Database.executeQuery(pStmt)) {
                    if (rs.next()) {
                        unit[0] = resurrect(ctx, rs);
                        cacheStore(ctx, unit[0]);
                    }
                }
            });

            return Optional.ofNullable(unit[0]);

        } else {
            // This pulls attributes as well as unit, as opposed to classic load, but saves
            // the extra step of later having to pull attributes.
            String sql = "SELECT extract_unit_json(?, ?) AS unit_json";

            Unit[] unit = { null };
            Database.useReadonlyPreparedStatement(ctx.getDataSource(), sql, pStmt -> {
                int i = 0;
                pStmt.setInt(++i, tenantId);
                pStmt.setLong(++i, unitId);

                try (ResultSet rs = Database.executeQuery(pStmt)) {
                    if (rs.next()) {
                        String json = rs.getString("unit_json");
                        log.trace("Resurrecting unit {}.{}: {}} ", tenantId, unitId, json);

                        unit[0] = resurrect(ctx, json);
                        cacheStore(ctx, unit[0]);
                    }
                }
            });

            return Optional.ofNullable(unit[0]);
        }
    }

    /**
     * Fetch a unit from a row in the resultset.
     */
    public static Optional<Unit> resurrectUnit(
            Context ctx, ResultSet rs
    ) throws DatabaseReadException {
        try {
            // First, check cache
            {
                int tenantId = rs.getInt("tenantid");
                long unitId = rs.getLong("unitid");
                int unitVer = rs.getInt("unitver");

                Optional<Unit> unit = cacheLookup(tenantId, unitId, unitVer);
                if (unit.isPresent()) {
                    return unit;
                }
            }

            // Not in cache, continue reading from resultset
            Unit unit = resurrect(ctx, rs);
            cacheStore(ctx, unit);
            return Optional.of(unit);

        } catch (SQLException sqle) {
            log.error(Database.squeeze(sqle));
            throw new DatabaseReadException(sqle);
        }
    }

    /**
     * Fetch a unit from a row in the resultset.
     */
    private static Unit resurrect(
            Context ctx, ResultSet rs
    ) throws DatabaseReadException {
        return new Unit(ctx, rs);
    }

    /**
     * Fetch a unit from a JSON.
     */
    private static Unit resurrect(
            Context ctx, String json
    ) throws DatabaseReadException {
        return new Unit(ctx, json);
    }

    public static void flush() {
        long gcCount = 0L;
        long flushCount = 0L;
        synchronized (unitCache) {
            try {
                if (!unitCache.isEmpty()) {
                    if (log.isTraceEnabled())
                        log.trace("Unit cache: Flushing {} units", unitCache.size());

                    Iterator<SoftReference<UnitCacheEntry>> eit = unitCache.values().iterator();
                    while (eit.hasNext()) {
                        SoftReference<UnitCacheEntry> ref = eit.next();
                        UnitCacheEntry entry = ref.get();

                        if (null == entry || entry.isCleared()) {
                            gcCount++;
                        } else {
                            flushCount++;
                        }
                        eit.remove();
                    }
                }
            } catch (Throwable t) {
                log.info("Unit cache explicit flush failure: {}", t.getMessage());
            }
        }
        log.info("Unit cache flushed {} live units. {} were already garbage collected.", flushCount, gcCount);
    }

    /**
     * Looks up some unit in cache. Since 'unitVer' may not refer to
     * the latest version and only the latest version is cached, this
     * lookup is likely to fail
     */
    private static Optional<Unit> cacheLookup(
            int tenantId, long unitId, int unitVer
    ) {
        String key = Unit.id2String(tenantId, unitId);

        if (log.isTraceEnabled()) {
            log.trace("Looking up unit {}", key);
        }
        synchronized (unitCache) {
            SoftReference<UnitCacheEntry> ref = unitCache.get(key);
            if (null == ref) {
                if (log.isTraceEnabled()) {
                    log.trace("Unit {} not in cache", key);
                }
                return Optional.empty();
            }

            UnitCacheEntry entry = ref.get();
            if (null == entry || entry.isCleared()) {
                unitCache.remove(key); // since cached unit was garbage collected
                if (log.isTraceEnabled()) {
                    log.trace("Unit {} was garbage collected - removed entry from cache", key);
                }
                return Optional.empty();
            }

            // A cache hit!
            entry.touch();
            return entry.getUnit();
        }
    }

    /**
     * Looks up unit. If it exists in cache, it is returned.
     */
    private static Optional<Unit> cacheLookup(
            Context ctx, int tenantId, long unitId
    ) {
        String key = Unit.id2String(tenantId, unitId);

        if (log.isTraceEnabled()) {
            log.trace("Looking up unit {}", key);
        }
        synchronized (unitCache) {
            SoftReference<UnitCacheEntry> ref = unitCache.get(key);
            if (null == ref) {
                if (log.isTraceEnabled()) {
                    log.trace("Unit {} not in cache", key);
                }
                return Optional.empty();
            }

            UnitCacheEntry entry = ref.get();
            if (null == entry || entry.isCleared()) {
                unitCache.remove(key); // since cached unit was garbage collected
                if (log.isTraceEnabled()) {
                    log.trace("Unit {} was garbage collected - removed entry from cache", key);
                }
                return Optional.empty();
            }

            // This is a cache hit!
            entry.touch();
            return entry.getUnit();
        }
    }

    /**
     * Stores unit to cache if cache has not reached max size.
     */
    private static void cacheStore(
            Context ctx, Unit unit
    ) {
        String key = unit.getReference();

        // Determine maximal cache size (dynamic)
        int idleCheckInterval = 1000 * ctx.getConfig().cacheIdleCheckInterval();
        int maxCacheSize = ctx.getConfig().cacheMaxSize();

        //
        try {
            synchronized (unitCache) {
                if (unitCache.containsKey(key)) {
                    UnitCacheEntry existing = null;

                    // Check if we should replace entry
                    SoftReference<UnitCacheEntry> ref = unitCache.get(key);
                    if (null != ref) {
                        existing = ref.get();
                        if (null == existing || existing.isCleared()) {
                            unitCache.remove(key);
                        }
                    }

                    // Create new entry inheriting statistics from old entry, if it was not garbage collected already
                    Unit clonedUnit = (Unit) unit.clone();
                    UnitCacheEntry newEntry = new UnitCacheEntry(key, clonedUnit, existing);
                    unitCache.put(key, new SoftReference<>(newEntry));

                } else {
                    // Not in cache already, make new entry if cache is not full.
                    if (unitCache.size() < maxCacheSize) {
                        Unit clonedUnit = (Unit) unit.clone();
                        UnitCacheEntry e = new UnitCacheEntry(key, clonedUnit);
                        unitCache.put(key, new SoftReference<>(e));

                    } else /* cache is full */ {
                        log.info("The unit cache is full ({} entries) - doing a flush based on statistics", maxCacheSize);

                        // Cache is full and we will do some short-term processing right now.
                        //==============================================
                        // Algorithm:
                        // Rule 1)
                        //   Keep units that has been accessed since
                        //   the last time we ran this algorithm.
                        //
                        // Rule 2)
                        //   Discard units regardless of when they were latest accessed.
                        //
                        // Rule 3)
                        //   Also, we are removing cache entries that refers
                        //   to units that was garbage collected.
                        //==============================================

                        Iterator<SoftReference<UnitCacheEntry>> eit = unitCache.values().iterator();
                        while (eit.hasNext()) {
                            SoftReference<UnitCacheEntry> ref = eit.next();
                            UnitCacheEntry entry = ref.get();

                            if (null == entry || entry.isCleared()) {
                                eit.remove(); // Rule 3)
                                continue;
                            }

                            if (/* force transient units out of cache? */ false) {
                                eit.remove(); // Rule 2)
                                continue;
                            }

                            if (entry.getDeltaAccessCount() == 0) {
                                eit.remove(); // Rule 1)
                                continue;
                            }

                            entry.resetDeltaAccessCount();
                        }
                    }
                }
            }
        } catch (CloneNotSupportedException cnse) {
            String info = "Failed to clone cached unit: " + unit;
            SystemInconsistencyException sie = new SystemInconsistencyException(info);
            log.error(info, sie);
            throw sie;

        } finally {
            if (idleCheckInterval > 0) {
                startCacheManager(ctx, idleCheckInterval);

            } else {
                if (log.isDebugEnabled()) {
                    log.debug("Ignoring cache manager start request: The cache manager was inhibited.");
                }
            }
        }
    }

    /**
     * Start the cache manager, that controls the "leaking" of entries,
     * possibly creating a new one.
     */
    private static void startCacheManager(
            Context ctx, int idleCheckInterval
    ) {

        // We only want one such thread active at any time,
        // so we protect its creation
        synchronized (mgrLock) {
            if (cacheMgr == null) {
                // maintainer is created and started
                log.info("Creating and starting cache manager");
                cacheMgr = new UnitCacheManager(ctx, unitCache, idleCheckInterval);
            } else {
                if (!cacheMgr.isAlive()) {
                    // maintainer is re-created and started
                    if (log.isDebugEnabled()) {
                        log.debug("Re-creating and starting cache manager");
                    }
                    cacheMgr = new UnitCacheManager(ctx, unitCache, idleCheckInterval);
                } else {
                    if (log.isTraceEnabled()) {
                        log.trace("Cache manager is alive");
                    }
                }
            }
        }
    }

    /**
     * Possibly stop the cache manager
     */
    public static void shutdown() {
        synchronized (mgrLock) {
            if (null != cacheMgr) {
                cacheMgr.shutdown();
            }
        }
    }
}





