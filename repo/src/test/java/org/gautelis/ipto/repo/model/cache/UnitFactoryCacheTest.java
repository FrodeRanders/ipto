package org.gautelis.ipto.repo.model.cache;

import org.gautelis.ipto.repo.model.Unit;
import org.junit.jupiter.api.Test;

import java.lang.ref.SoftReference;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertTrue;

class UnitFactoryCacheTest {

    @Test
    void flushClearsCachedEntries() throws Exception {
        Map<String, SoftReference<UnitCacheEntry>> cache = getUnitCache();
        Map<String, SoftReference<UnitCacheEntry>> snapshot = new HashMap<>(cache);

        try {
            Unit unit = new Unit(null, unitJson(4, 5, 1));
            UnitCacheEntry entry = new UnitCacheEntry(Unit.id2String(4, 5), unit);
            cache.put(entry.getKey(), new SoftReference<>(entry));
            cache.put("cleared", new SoftReference<>(null));

            UnitFactory.flush();

            assertTrue(cache.isEmpty());
        } finally {
            cache.clear();
            cache.putAll(snapshot);
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, SoftReference<UnitCacheEntry>> getUnitCache() throws Exception {
        Field field = UnitFactory.class.getDeclaredField("unitCache");
        field.setAccessible(true);
        return (Map<String, SoftReference<UnitCacheEntry>>) field.get(null);
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
