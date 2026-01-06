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
