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
            Thread.sleep(10);
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
