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

import org.gautelis.repo.model.Context;
import org.gautelis.repo.model.Unit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.ref.SoftReference;

import java.util.Date;
import java.util.Iterator;
import java.util.Map;

/**
 *
 */
final class UnitCacheManager extends Thread {
    private static final Logger log = LoggerFactory.getLogger(UnitCacheManager.class);
    private final Map<String, SoftReference<UnitCacheEntry>> unitCache;
    private final int idleCheckInterval; // in millisecs

    //
    private final Context ctx;
    private boolean shutdown = false;

    UnitCacheManager(
            Context ctx, Map<String, SoftReference<UnitCacheEntry>> unitCache, int idleCheckInterval
    ) {
        this.ctx = ctx;
        this.unitCache = unitCache;
        this.idleCheckInterval = idleCheckInterval;

        start();
    }

    /**
     * Shut down maintainer
     */
    void shutdown() {
        synchronized (unitCache) {
            shutdown = true;
        }
    }

    public void run() {

        if (log.isTraceEnabled())
            log.trace("Unit cache: Leak maintainer started");

        Date lastRun = new Date();

        while (true) {
            long gcCount = 0L;
            long flushCount = 0L;
            synchronized (unitCache) {
                try {
                    // Cease with operations if no units exists in cache
                    // (actually regardless of if the cache functionality
                    // was used or not) or if explicitly requested to
                    // do so.
                    if (unitCache.isEmpty() || shutdown) {
                        if (log.isTraceEnabled())
                            log.trace("Unit cache: Cache maintainer exiting");
                        return;
                    }

                    try {
                        unitCache.wait(idleCheckInterval);
                    } catch (InterruptedException e) {
                        // Ignore
                    }

                    if (shutdown) {
                        if (log.isTraceEnabled())
                            log.trace("Unit cache: Cache maintainer shutdown");
                        return;
                    }

                    if (log.isTraceEnabled())
                        log.trace("Unit cache: Cache maintenance engaging (~{} units)", unitCache.size());

                    if (!unitCache.isEmpty()) {
                        Iterator<SoftReference<UnitCacheEntry>> eit = unitCache.values().iterator();
                        while (eit.hasNext()) {
                            SoftReference<UnitCacheEntry> ref = eit.next();
                            UnitCacheEntry entry = ref.get();

                            if (null == entry || entry.isCleared()) {
                                eit.remove(); // entry was garbage collected
                                gcCount++;
                                continue;
                            }

                            int tenantId = entry.getTenantId();
                            long unitId = entry.getUnitId();

                            // If this entry has timed out, we will just remove it from
                            // the hashtable, otherwise we will query its status.
                            if (entry.isOlderThan(lastRun)) {
                                eit.remove(); // current unit is discarded
                                flushCount++;

                                if (log.isTraceEnabled()) {
                                    String unitRef = Unit.id2String(tenantId, unitId);
                                    log.trace("Auto-flushing unit {} (timeout)", unitRef);
                                }
                            } else {
                                entry.resetDeltaAccessCount();
                            }
                        }
                    }
                    lastRun = new Date();

                } catch (Throwable t) {
                    log.info("Unit cache auto-flush failure: {}", t.getMessage());
                }
            }

            if (log.isTraceEnabled())
                log.trace("Unit cache: Ceased operation: Flushed {} units and dropped {} garbage collected units", flushCount, gcCount);
        }
    }
}






