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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;

class UnitCacheTest {

    @AfterEach
    void resetCacheState() throws Exception {
        CacheTestSupport.resetCacheState();
    }

    @Test
    void rebuildPreservesEntriesOnConfigChange() throws Exception {
        CacheTestSupport.TestConfig configHandler = CacheTestSupport.newConfigHandler(10, 60);
        var config = configHandler.proxy();
        Context ctx = new Context(null, config, null, null);

        var cache = CacheTestSupport.ensureCache(ctx);

        Unit unit = new Unit(null, unitJson(12, 34, 1));
        UnitCacheEntry entry = new UnitCacheEntry(Unit.id2String(12, 34), unit);
        cache.put(entry.getKey(), entry);

        configHandler.setMaxSize(5);
        configHandler.setIdleCheckInterval(10);

        var rebuilt = CacheTestSupport.ensureCache(ctx);
        assertNotSame(cache, rebuilt);
        assertNotNull(rebuilt.getIfPresent(entry.getKey()));
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
