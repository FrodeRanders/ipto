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
import org.gautelis.ipto.repo.model.attributes.Attribute;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

class AttributeTypeTest {

    @Test
    void ofResolvesKnownTypeIds() {
        assertEquals(AttributeType.STRING, AttributeType.of(1));
        assertEquals(AttributeType.TIME, AttributeType.of(2));
        assertEquals(AttributeType.INTEGER, AttributeType.of(3));
        assertEquals(AttributeType.LONG, AttributeType.of(4));
        assertEquals(AttributeType.DOUBLE, AttributeType.of(5));
        assertEquals(AttributeType.BOOLEAN, AttributeType.of(6));
        assertEquals(AttributeType.DATA, AttributeType.of(7));
        assertEquals(AttributeType.RECORD, AttributeType.of(99));
    }

    @Test
    void ofRejectsUnknownTypeIds() {
        assertThrows(AttributeTypeException.class, () -> AttributeType.of(1234));
    }

    @Test
    void ofResolvesKnownTypeNames() {
        assertEquals(AttributeType.STRING, AttributeType.of("STRING"));
        assertEquals(AttributeType.TIME, AttributeType.of("TIME"));
        assertEquals(AttributeType.INTEGER, AttributeType.of("INTEGER"));
        assertEquals(AttributeType.LONG, AttributeType.of("LONG"));
        assertEquals(AttributeType.DOUBLE, AttributeType.of("DOUBLE"));
        assertEquals(AttributeType.BOOLEAN, AttributeType.of("BOOLEAN"));
        assertEquals(AttributeType.DATA, AttributeType.of("DATA"));
        assertEquals(AttributeType.RECORD, AttributeType.of("RECORD"));
    }

    @Test
    void ofRejectsUnknownTypeNames() {
        assertThrows(AttributeTypeException.class, () -> AttributeType.of("MISSING"));
    }

    @Test
    void fromJavaClassMapsKnownTypes() {
        assertEquals(AttributeType.STRING, AttributeType.fromJavaClass(String.class).orElseThrow());
        assertEquals(AttributeType.TIME, AttributeType.fromJavaClass(Instant.class).orElseThrow());
        assertEquals(AttributeType.INTEGER, AttributeType.fromJavaClass(Integer.class).orElseThrow());
        assertEquals(AttributeType.LONG, AttributeType.fromJavaClass(Long.class).orElseThrow());
        assertEquals(AttributeType.DOUBLE, AttributeType.fromJavaClass(Double.class).orElseThrow());
        assertEquals(AttributeType.BOOLEAN, AttributeType.fromJavaClass(Boolean.class).orElseThrow());
        assertEquals(AttributeType.DATA, AttributeType.fromJavaClass(Object.class).orElseThrow());
        assertEquals(AttributeType.RECORD, AttributeType.fromJavaClass(Attribute.class).orElseThrow());
    }

    @Test
    void fromJavaClassReturnsEmptyWhenUnmapped() {
        assertTrue(AttributeType.fromJavaClass(AttributeTypeTest.class).isEmpty());
    }
}
