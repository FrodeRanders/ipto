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

import org.gautelis.ipto.repo.db.Database;
import org.gautelis.ipto.repo.exceptions.DatabaseConnectionException;
import org.gautelis.ipto.repo.exceptions.DatabaseReadException;
import org.gautelis.ipto.repo.exceptions.SystemInconsistencyException;
import org.gautelis.ipto.repo.model.Context;
import org.gautelis.ipto.repo.model.Unit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.sql.*;
import java.util.Optional;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

/**
 *
 */
public final class UnitFactory {
    private static final Logger log = LoggerFactory.getLogger(UnitFactory.class);
    private static final Object cacheLock = new Object();
    private static volatile Cache<String, UnitCacheEntry> unitCache = null;
    private static volatile int cacheMaxSize = -1;
    private static volatile int cacheIdleCheckIntervalMs = -1;

    public enum LoadStrategy {
        RESULTSET_BASED,
        JSON_BASED
    }

    public static final LoadStrategy loadStrategy = LoadStrategy.JSON_BASED;

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
            Context ctx, int tenantId, long unitId, int unitVersion
    ) throws DatabaseConnectionException, DatabaseReadException {
        // First, check cache
        if (unitVersion > 0) {
            Optional<Unit> unit = cacheLookup(ctx, tenantId, unitId, unitVersion);
            if (unit.isPresent()) {
                return unit;
            }
        } else {
            Optional<Unit> unit = cacheLookup(ctx, tenantId, unitId);
            if (unit.isPresent()) {
                return unit;
            }
        }

        switch (loadStrategy) {
            case RESULTSET_BASED: {
                // Not in cache, continue reading from database
                Unit[] unit = { null };
                if (unitVersion > 0) {
                    Database.useReadonlyPreparedStatement(ctx.getDataSource(), ctx.getStatements().unitGet(), pStmt -> {
                        int i = 0;
                        pStmt.setInt(++i, tenantId);
                        pStmt.setLong(++i, unitId);
                        pStmt.setInt(++i, unitVersion);

                        try (ResultSet rs = Database.executeQuery(pStmt)) {
                            if (rs.next()) {
                                log.trace("Resurrecting unit {}.{}:{}", tenantId, unitId, unitVersion);
                                unit[0] = resurrect(ctx, rs);
                                cacheStore(ctx, unit[0]);
                            }
                        }
                    });
                } else {
                    Database.useReadonlyPreparedStatement(ctx.getDataSource(), ctx.getStatements().unitGetLatest(), pStmt -> {
                        int i = 0;
                        pStmt.setInt(++i, tenantId);
                        pStmt.setLong(++i, unitId);

                        try (ResultSet rs = Database.executeQuery(pStmt)) {
                            if (rs.next()) {
                                log.trace("Resurrecting unit {}.{} (latest)", tenantId, unitId);
                                unit[0] = resurrect(ctx, rs);
                                cacheStore(ctx, unit[0]);
                            }
                        }
                    });
                }

                return Optional.ofNullable(unit[0]);
            }

            case JSON_BASED: {
                // This pulls attributes as well as unit, as opposed to classic load, and saves
                // the extra step of having to pull attributes later.
                String sql = "CALL extract_unit_json(?, ?, ?, ?)";

                Unit[] unit = { null };
                Database.useConnection(ctx.getDataSource(), conn -> {
                    try (CallableStatement cStmt = conn.prepareCall(sql)) {
                        cStmt.setInt(1, tenantId);
                        cStmt.setLong(2, unitId);
                        if (unitVersion > 0) {
                            cStmt.setInt(3, unitVersion);
                        } else {
                            cStmt.setInt(3, -1);
                        }
                        if (ctx.useClob()) {
                            // DB2, ...
                            cStmt.registerOutParameter(4, java.sql.Types.CLOB);
                        } else {
                            // PostgreSQL, ...
                            cStmt.registerOutParameter(4, java.sql.Types.OTHER);
                        }
                        cStmt.execute();

                        Object out = cStmt.getObject(4);

                        if (out == null) {
                            log.debug("Null result in 'extract_unit_json'");

                        } else {
                            String json = switch (out) {
                                case String s -> s; /* If we are lucky */
                                case Clob clob -> clob.getSubString(1L, (int) clob.length()); /* DB2 */
                                case org.postgresql.util.PGobject pg -> pg.getValue(); /* PostgreSQL */
                                default -> out.toString(); // Fall back on driver behaviour
                            };

                            if (log.isTraceEnabled()) {
                                if (unitVersion > 0) {
                                    log.trace("Resurrecting unit {}.{}:{}", tenantId, unitId, unitVersion);
                                } else {
                                    log.trace("Resurrecting unit {}.{} (latest)", tenantId, unitId);
                                }
                            }

                            unit[0] = resurrect(ctx, json);
                            cacheStore(ctx, unit[0]);
                        }
                    }
                });

                return Optional.ofNullable(unit[0]);
            }
            default:
                return  Optional.empty();
        }
    }

    /**
     * Fetch a unit
     */
    public static Optional<Unit> resurrectUnit(
            Context ctx, int tenantId, long unitId
    ) throws DatabaseConnectionException, DatabaseReadException {
        return resurrectUnit(ctx, tenantId, unitId, 0);
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

                Optional<Unit> unit = cacheLookup(ctx, tenantId, unitId, unitVer);
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
     * Fetch a unit from JSON.
     */
    private static Unit resurrect(
            Context ctx, String json
    ) throws DatabaseReadException {
        return new Unit(ctx, json);
    }

    private static Cache<String, UnitCacheEntry> ensureCache(
            Context ctx
    ) {
        int requestedMaxSize = ctx.getConfig().cacheMaxSize();
        int requestedIdleCheckMs = 1000 * ctx.getConfig().cacheIdleCheckInterval();

        Cache<String, UnitCacheEntry> cache = unitCache;
        if (cache != null && cacheMaxSize == requestedMaxSize && cacheIdleCheckIntervalMs == requestedIdleCheckMs) {
            return cache;
        }

        synchronized (cacheLock) {
            cache = unitCache;
            if (cache != null && cacheMaxSize == requestedMaxSize && cacheIdleCheckIntervalMs == requestedIdleCheckMs) {
                return cache;
            }

            if (requestedMaxSize <= 0) {
                if (cache != null) {
                    cache.invalidateAll();
                    unitCache = null;
                }
                cacheMaxSize = requestedMaxSize;
                cacheIdleCheckIntervalMs = requestedIdleCheckMs;
                if (log.isDebugEnabled()) {
                    log.debug("Unit cache disabled (max size <= 0)");
                }
                return null;
            }

            Caffeine<Object, Object> builder = Caffeine.newBuilder()
                    .maximumSize(requestedMaxSize);
            if (requestedIdleCheckMs > 0) {
                builder.expireAfterAccess(Duration.ofMillis(requestedIdleCheckMs));
            }

            Cache<String, UnitCacheEntry> newCache = builder.build();
            Cache<String, UnitCacheEntry> oldCache = unitCache;
            unitCache = newCache;
            cacheMaxSize = requestedMaxSize;
            cacheIdleCheckIntervalMs = requestedIdleCheckMs;

            long preservedEntries = 0L;
            if (oldCache != null) {
                preservedEntries = oldCache.estimatedSize();
                newCache.putAll(oldCache.asMap());
                oldCache.invalidateAll();
            }

            if (log.isInfoEnabled()) {
                if (requestedIdleCheckMs > 0) {
                    log.info("Created unit cache (maxSize={}, expireAfterAccessMs={}, preservedEntries={})",
                            requestedMaxSize, requestedIdleCheckMs, preservedEntries);
                } else {
                    log.info("Created unit cache (maxSize={}, expireAfterAccess=disabled, preservedEntries={})",
                            requestedMaxSize, preservedEntries);
                }
            }
        }

        return unitCache;
    }

    public static void flush() {
        Cache<String, UnitCacheEntry> cache = unitCache;
        if (cache == null) {
            if (log.isTraceEnabled()) {
                log.trace("Unit cache flush requested, but cache is not initialized.");
            }
            return;
        }

        long size = cache.estimatedSize();
        cache.invalidateAll();
        cache.cleanUp();
        log.info("Unit cache flushed {} entries.", size);
    }

    /**
     * Looks up some unit in cache. Since 'unitVersion' may not refer to
     * the latest version and only the latest version is cached, this
     * lookup is likely to fail
     */
    private static Optional<Unit> cacheLookup(
            Context ctx, int tenantId, long unitId, int unitVersion
    ) {
        String key = Unit.id2String(tenantId, unitId);

        if (log.isTraceEnabled()) {
            log.trace("Looking up unit {}", key);
        }
        Cache<String, UnitCacheEntry> cache = ensureCache(ctx);
        if (cache == null) {
            if (log.isTraceEnabled()) {
                log.trace("Unit {} not in cache (cache not initialized)", key);
            }
            return Optional.empty();
        }

        UnitCacheEntry entry = cache.getIfPresent(key);
        if (entry == null) {
            if (log.isTraceEnabled()) {
                log.trace("Unit {} not in cache", key);
            }
            return Optional.empty();
        }

        if (entry.getVersion() != unitVersion) {
            if (log.isTraceEnabled()) {
                log.trace("Cached unit {} not of version {}", key, unitVersion);
            }
            return Optional.empty();
        }

        // A cache hit!
        entry.touch();
        return entry.getUnit();
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
        Cache<String, UnitCacheEntry> cache = ensureCache(ctx);
        if (cache == null) {
            if (log.isTraceEnabled()) {
                log.trace("Unit {} not in cache (cache not initialized)", key);
            }
            return Optional.empty();
        }

        UnitCacheEntry entry = cache.getIfPresent(key);
        if (entry == null) {
            if (log.isTraceEnabled()) {
                log.trace("Unit {} not in cache", key);
            }
            return Optional.empty();
        }

        // This is a cache hit!
        entry.touch();
        return entry.getUnit();
    }

    /**
     * Stores unit to cache if cache has not reached max size.
     */
    private static void cacheStore(
            Context ctx, Unit unit
    ) {
        String key = unit.getReference();

        Cache<String, UnitCacheEntry> cache = ensureCache(ctx);
        if (cache == null) {
            return;
        }

        try {
            UnitCacheEntry existing = cache.getIfPresent(key);
            Unit clonedUnit = (Unit) unit.clone();
            UnitCacheEntry newEntry = new UnitCacheEntry(key, clonedUnit, existing);
            cache.put(key, newEntry);
        } catch (CloneNotSupportedException cnse) {
            String info = "Failed to clone cached unit: " + unit;
            SystemInconsistencyException sie = new SystemInconsistencyException(info);
            log.error(info, sie);
            throw sie;
        }
    }

    /**
     * Possibly stop the cache manager
     */
    public static void shutdown() {
        flush();
    }
}
