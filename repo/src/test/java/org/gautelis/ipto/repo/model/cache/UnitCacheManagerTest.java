/*
 * Copyright (C) 2025-2026 Frode Randers
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

import org.gautelis.ipto.repo.model.Unit;
import org.junit.jupiter.api.Test;

import java.lang.ref.SoftReference;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertTrue;

class UnitCacheManagerTest {

    @Test
    void removesStaleEntriesAndExits() throws Exception {
        Map<String, SoftReference<UnitCacheEntry>> cache = new HashMap<>();
        Unit unit = new Unit(null, unitJson(12, 34, 1));
        UnitCacheEntry entry = new UnitCacheEntry(Unit.id2String(12, 34), unit);
        cache.put(entry.getKey(), new SoftReference<>(entry));

        UnitCacheManager manager = new UnitCacheManager(null, cache, 10);

        long deadline = System.currentTimeMillis() + 1000;
        while (!cache.isEmpty() && System.currentTimeMillis() < deadline) {
            //noinspection BusyWait
            Thread.sleep(10); // OK since we are testing
        }

        assertTrue(cache.isEmpty());
        manager.join(200);
    }

    @Test
    void shutdownStopsManagerWithoutTouchingEntries() throws Exception {
        Map<String, SoftReference<UnitCacheEntry>> cache = new HashMap<>();
        Unit unit = new Unit(null, unitJson(2, 3, 1));
        UnitCacheEntry entry = new UnitCacheEntry(Unit.id2String(2, 3), unit);
        cache.put(entry.getKey(), new SoftReference<>(entry));

        UnitCacheManager manager = new UnitCacheManager(null, cache, 1000);
        manager.shutdown();
        manager.join(500);

        assertTrue(manager.getState() == Thread.State.TERMINATED);
        assertTrue(!cache.isEmpty());
    }

    private static String unitJson(int tenantId, long unitId, int version) {
        String corrId = UUID.randomUUID().toString();
        return "{"
                + "\"tenantid\":" + tenantId + ","
                + "\"unitid\":" + unitId + ","
                + "\"corrid\":\"" + corrId + "\","
                + "\"status\":30,"
                + "\"created\":\"2024-08-26T10:15:30Z\","
                + "\"unitver\":" + version + ","
                + "\"unitname\":\"unit-" + unitId + "\","
                + "\"modified\":\"2024-08-26T10:16:30Z\","
                + "\"isreadonly\":false,"
                + "\"attributes\":[]"
                + "}";
    }
}
