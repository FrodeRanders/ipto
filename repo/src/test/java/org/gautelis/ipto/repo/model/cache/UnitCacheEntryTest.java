package org.gautelis.ipto.repo.model.cache;

import org.gautelis.ipto.repo.model.Unit;
import org.junit.jupiter.api.Test;

import java.util.Date;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class UnitCacheEntryTest {

    @Test
    void exposesUnitIdentityAndCountsAccess() {
        Unit unit = new Unit(null, unitJson(7, 42, 3));
        UnitCacheEntry entry = new UnitCacheEntry(Unit.id2String(7, 42), unit);

        assertEquals(7, entry.getTenantId());
        assertEquals(42L, entry.getUnitId());
        assertEquals(3, entry.getVersion());
        assertEquals(0, entry.getTotalAccessCount());
        assertEquals(0, entry.getDeltaAccessCount());

        Date before = entry.getDate();
        entry.touch();
        Date after = entry.getDate();

        assertFalse(after.before(before));
        assertEquals(1, entry.getTotalAccessCount());
        assertEquals(1, entry.getDeltaAccessCount());

        entry.resetDeltaAccessCount();
        assertEquals(0, entry.getDeltaAccessCount());
    }

    @Test
    void returnsCloneOfCachedUnit() {
        Unit unit = new Unit(null, unitJson(1, 2, 1));
        UnitCacheEntry entry = new UnitCacheEntry(Unit.id2String(1, 2), unit);

        Optional<Unit> cached = entry.getUnit();
        assertTrue(cached.isPresent());
        assertNotSame(unit, cached.get());
        assertEquals(unit.getReference(), cached.get().getReference());
    }

    @Test
    void isOlderThanUsesEntryDate() {
        Unit unit = new Unit(null, unitJson(9, 11, 1));
        UnitCacheEntry entry = new UnitCacheEntry(Unit.id2String(9, 11), unit);

        Date later = new Date(entry.getDate().getTime() + 1);
        assertTrue(entry.isOlderThan(later));
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
