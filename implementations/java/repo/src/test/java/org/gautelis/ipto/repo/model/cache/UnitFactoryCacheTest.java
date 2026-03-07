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

import org.gautelis.ipto.repo.model.Context;
import org.gautelis.ipto.repo.model.Unit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

class UnitFactoryCacheTest {

    @AfterEach
    void resetCacheState() throws Exception {
        CacheTestSupport.resetCacheState();
    }

    @Test
    void flushClearsCachedEntries() throws Exception {
        var config = CacheTestSupport.newConfig(10, 60);
        Context ctx = new Context(null, config, null, null);

        var cache = CacheTestSupport.ensureCache(ctx);
        Unit unit = new Unit(null, unitJson(4, 5, 1));
        UnitCacheEntry entry = new UnitCacheEntry(Unit.id2String(4, 5), unit);
        cache.put(entry.getKey(), entry);
        cache.put("cleared", new UnitCacheEntry("cleared", unit));

        UnitFactory.flush();

        assertEquals(0L, cache.estimatedSize());
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
