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
package org.gautelis.ipto.repo.model.cache;

import org.gautelis.ipto.repo.model.AttributeType;
import org.gautelis.ipto.repo.model.Unit;
import org.gautelis.ipto.repo.model.attributes.Attribute;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class UnitDeepCloneTest {

    @Test
    void deepCloneIsolatedFromMutations() throws Exception {
        Unit unit = new Unit(null, unitJson());
        UnitCacheEntry entry = new UnitCacheEntry(Unit.id2String(1, 2), unit);

        Unit clone1 = entry.getUnit().orElseThrow();

        Attribute<?> title = clone1.getAttribute("title").orElseThrow();
        @SuppressWarnings("unchecked")
        ArrayList<String> titleValues = (ArrayList<String>) title.getValueVector();
        titleValues.clear();
        titleValues.add("changed");

        Attribute<?> count = clone1.getAttribute("count").orElseThrow();
        @SuppressWarnings("unchecked")
        ArrayList<Integer> countValues = (ArrayList<Integer>) count.getValueVector();
        countValues.add(99);

        Attribute<?> blob = clone1.getAttribute("blob").orElseThrow();
        @SuppressWarnings("unchecked")
        ArrayList<Object> blobValues = (ArrayList<Object>) blob.getValueVector();
        byte[] mutated = (byte[]) blobValues.getFirst();
        mutated[0] = 9;

        Attribute<?> record = clone1.getAttribute("record").orElseThrow();
        Attribute<?> nested = (Attribute<?>) record.getValueVector().getFirst();
        @SuppressWarnings("unchecked")
        ArrayList<String> nestedValues = (ArrayList<String>) nested.getValueVector();
        nestedValues.clear();
        nestedValues.add("mutated");

        Unit clone2 = entry.getUnit().orElseThrow();

        Attribute<?> title2 = clone2.getAttribute("title").orElseThrow();
        assertEquals(Arrays.asList("a", "b"), title2.getValueVector());

        Attribute<?> count2 = clone2.getAttribute("count").orElseThrow();
        assertEquals(Arrays.asList(1, 2), count2.getValueVector());

        Attribute<?> blob2 = clone2.getAttribute("blob").orElseThrow();
        byte[] bytes2 = (byte[]) blob2.getValueVector().getFirst();
        assertTrue(Arrays.equals(new byte[] {1, 2, 3}, bytes2));

        Attribute<?> record2 = clone2.getAttribute("record").orElseThrow();
        Attribute<?> nested2 = (Attribute<?>) record2.getValueVector().getFirst();
        assertEquals(Arrays.asList("x"), nested2.getValueVector());

        assertNotEquals(titleValues, title2.getValueVector());
    }

    private static String unitJson() {
        String corrId = UUID.randomUUID().toString();
        String blob = Base64.getEncoder().encodeToString(new byte[] {1, 2, 3});

        return "{"
                + "\"tenantid\":1,"
                + "\"unitid\":2,"
                + "\"corrid\":\"" + corrId + "\","
                + "\"status\":30,"
                + "\"created\":\"2024-08-26T10:15:30Z\","
                + "\"unitver\":1,"
                + "\"unitname\":\"unit-2\","
                + "\"modified\":\"2024-08-26T10:16:30Z\","
                + "\"isreadonly\":false,"
                + "\"attributes\":["
                + "{"
                + "\"attrid\":1001,"
                + "\"unitverfrom\":1,"
                + "\"unitverto\":1,"
                + "\"valueid\":2001,"
                + "\"attrname\":\"title\","
                + "\"alias\":\"\","
                + "\"attrtype\":" + AttributeType.STRING.getType() + ","
                + "\"value\":[\"a\",\"b\"]"
                + "},"
                + "{"
                + "\"attrid\":1002,"
                + "\"unitverfrom\":1,"
                + "\"unitverto\":1,"
                + "\"valueid\":2002,"
                + "\"attrname\":\"nested\","
                + "\"alias\":\"\","
                + "\"attrtype\":" + AttributeType.STRING.getType() + ","
                + "\"value\":[\"x\"]"
                + "},"
                + "{"
                + "\"attrid\":1003,"
                + "\"unitverfrom\":1,"
                + "\"unitverto\":1,"
                + "\"valueid\":2003,"
                + "\"attrname\":\"count\","
                + "\"alias\":\"\","
                + "\"attrtype\":" + AttributeType.INTEGER.getType() + ","
                + "\"value\":[1,2]"
                + "},"
                + "{"
                + "\"attrid\":1004,"
                + "\"unitverfrom\":1,"
                + "\"unitverto\":1,"
                + "\"valueid\":2004,"
                + "\"attrname\":\"blob\","
                + "\"alias\":\"\","
                + "\"attrtype\":" + AttributeType.DATA.getType() + ","
                + "\"value\":[\"" + blob + "\"]"
                + "},"
                + "{"
                + "\"attrid\":1005,"
                + "\"unitverfrom\":1,"
                + "\"unitverto\":1,"
                + "\"valueid\":2005,"
                + "\"attrname\":\"record\","
                + "\"alias\":\"\","
                + "\"attrtype\":" + AttributeType.RECORD.getType() + ","
                + "\"value\":[{\"ref_attrid\":1002,\"ref_valueid\":2002}]"
                + "}"
                + "]"
                + "}";
    }
}
