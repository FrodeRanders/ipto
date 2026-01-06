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
package org.gautelis.ipto.repo.model;

import org.gautelis.ipto.repo.exceptions.AttributeTypeException;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

class AttributeTypeTest {

    @Test
    void ofResolvesKnownTypeIds() throws Exception {
        assertEquals(AttributeType.STRING, AttributeType.of(1));
        assertEquals(AttributeType.TIME, AttributeType.of(2));
        assertEquals(AttributeType.RECORD, AttributeType.of(99));
    }

    @Test
    void ofRejectsUnknownTypeIds() {
        assertThrows(AttributeTypeException.class, () -> AttributeType.of(1234));
    }

    @Test
    void ofResolvesKnownTypeNames() throws Exception {
        assertEquals(AttributeType.BOOLEAN, AttributeType.of("BOOLEAN"));
        assertEquals(AttributeType.DATA, AttributeType.of("DATA"));
    }

    @Test
    void ofRejectsUnknownTypeNames() {
        assertThrows(AttributeTypeException.class, () -> AttributeType.of("MISSING"));
    }

    @Test
    void fromJavaClassMapsKnownTypes() {
        assertEquals(AttributeType.STRING, AttributeType.fromJavaClass(String.class).orElseThrow());
        assertEquals(AttributeType.TIME, AttributeType.fromJavaClass(Instant.class).orElseThrow());
    }

    @Test
    void fromJavaClassReturnsEmptyWhenUnmapped() {
        assertTrue(AttributeType.fromJavaClass(AttributeTypeTest.class).isEmpty());
    }
}
