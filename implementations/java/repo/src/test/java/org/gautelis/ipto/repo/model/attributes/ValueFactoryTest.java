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
package org.gautelis.ipto.repo.model.attributes;

import org.gautelis.ipto.repo.model.AttributeType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.*;

class ValueFactoryTest {

    @Test
    void createValueBuildsConcreteTypes() throws Exception {
        Value<String> stringValue = Value.createValue(AttributeType.STRING);
        assertInstanceOf(StringValue.class, stringValue);
        assertEquals(AttributeType.STRING, stringValue.getType());
        assertEquals(String.class, stringValue.getConcreteType());

        Value<Integer> intValue = Value.createValue(AttributeType.INTEGER);
        assertInstanceOf(IntegerValue.class, intValue);
        assertEquals(AttributeType.INTEGER, intValue.getType());
        assertEquals(Integer.class, intValue.getConcreteType());
    }

    @Test
    void newValuesAreScalarAndModified() throws Exception {
        Value<String> stringValue = Value.createValue(AttributeType.STRING);

        assertTrue(stringValue.isScalar());
        assertFalse(stringValue.isVector());
        assertTrue(stringValue.isNew());
        assertTrue(stringValue.isModified());
    }

    @Test
    void setScalarAndVectorValues() throws Exception {
        Value<String> stringValue = Value.createValue(AttributeType.STRING);

        stringValue.set("alpha");
        assertEquals("alpha", stringValue.getScalar());
        assertTrue(stringValue.isScalar());

        ArrayList<String> moreValues = new ArrayList<>();
        moreValues.add("beta");
        moreValues.add("gamma");
        stringValue.set(moreValues);

        assertEquals(3, stringValue.getSize());
        assertTrue(stringValue.isVector());
    }

    @Test
    void verifyHonorsType() throws Exception {
        Value<Integer> intValue = Value.createValue(AttributeType.INTEGER);

        assertTrue(intValue.verify(1));
        assertFalse(intValue.verify("1"));
    }
}
