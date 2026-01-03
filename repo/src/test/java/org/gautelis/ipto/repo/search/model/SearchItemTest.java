package org.gautelis.ipto.repo.search.model;

import org.junit.jupiter.api.Test;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Locale;
import java.util.TimeZone;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SearchItemTest {

    @Test
    void earlyAndLateSetDayBoundaries() throws Exception {
        TimeZone original = TimeZone.getDefault();
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        try {
            Timestamp early = SearchItem.early("8/26/24", Locale.US);
            Timestamp late = SearchItem.late("8/26/24", Locale.US);

            LocalDateTime earlyLdt = LocalDateTime.ofInstant(early.toInstant(), ZoneOffset.UTC);
            LocalDateTime lateLdt = LocalDateTime.ofInstant(late.toInstant(), ZoneOffset.UTC);

            assertEquals(0, earlyLdt.getHour());
            assertEquals(0, earlyLdt.getMinute());
            assertEquals(0, earlyLdt.getSecond());
            assertEquals(0, earlyLdt.getNano());

            assertEquals(23, lateLdt.getHour());
            assertEquals(59, lateLdt.getMinute());
            assertEquals(59, lateLdt.getSecond());
            assertEquals(999_000_000, lateLdt.getNano());
        } finally {
            TimeZone.setDefault(original);
        }
    }
}
