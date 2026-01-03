package org.gautelis.ipto.repo.utils;

import org.junit.jupiter.api.Test;

import java.sql.Timestamp;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TimeHelperTest {

    @Test
    void parseTimestampHandlesMicroseconds() {
        Timestamp ts = TimeHelper.parseTimestamp("2024-08-26T19:15:23.123456");

        Instant expected = Instant.parse("2024-08-26T19:15:23.123456Z");
        assertEquals(expected, ts.toInstant());
    }

    @Test
    void instantTimestampRoundTrip() {
        Instant instant = Instant.parse("2024-08-26T19:15:23.789Z");

        Timestamp ts = TimeHelper.instant2Timestamp(instant);
        assertEquals(instant, TimeHelper.timestamp2Instant(ts));
    }
}
