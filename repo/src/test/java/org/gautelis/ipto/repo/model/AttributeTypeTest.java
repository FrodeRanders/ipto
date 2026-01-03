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
