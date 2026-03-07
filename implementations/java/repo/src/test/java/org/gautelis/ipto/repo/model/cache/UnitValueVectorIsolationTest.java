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

import java.util.Arrays;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;

class UnitValueVectorIsolationTest {

    @Test
    void valueVectorsAreIsolatedBetweenClones() throws Exception {
        Unit unit = new Unit(null, unitJson());
        UnitCacheEntry entry = new UnitCacheEntry(Unit.id2String(1, 2), unit);

        Unit clone1 = entry.getUnit().orElseThrow();
        Unit clone2 = entry.getUnit().orElseThrow();

        Attribute<String> titles1 = clone1.getStringAttribute("title").orElseThrow();
        Attribute<String> titles2 = clone2.getStringAttribute("title").orElseThrow();

        var list1 = titles1.getValueVector();
        var list2 = titles2.getValueVector();

        assertNotSame(list1, list2);

        list1.add("c");
        assertEquals(Arrays.asList("a", "b"), list2);
    }

    private static String unitJson() {
        String corrId = UUID.randomUUID().toString();
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
                + "}"
                + "]"
                + "}";
    }
}
