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
