/*
 * Copyright (C) 2024-2025 Frode Randers
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
package org.gautelis.repo;

import org.gautelis.repo.model.Repository;
import org.gautelis.repo.model.Unit;
import org.gautelis.repo.model.attributes.Attribute;
import org.gautelis.repo.model.attributes.Value;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.fail;

/**
 *
 */
@Tag("records")
public class RecordsTest {
    private static final Logger log = LoggerFactory.getLogger(RecordsTest.class);

    @BeforeAll
    public static void setUp() throws IOException {
        CommonSetup.setUp();
    }

    @Test
    public void test() {
        Repository repo = RepositoryFactory.getRepository();
        final int tenantId = 1;
        final String unitTitle = "Handling of *some order id*";

        Unit unit = repo.createUnit(tenantId, "a record instance");

        unit.withAttributeValue("dce:title", String.class, value -> {
            value.add(unitTitle);
        });

        unit.withAttributeValue("dmo:orderId", String.class, value -> {
            value.add("*some order id*");
        });

        unit.withRecordAttribute("dmo:shipment", recrd -> {
            recrd.withNestedAttributeValue(unit, "dmo:shipmentId", String.class, value -> {
                value.add("*some shipment id*");
            });

            recrd.withNestedAttributeValue(unit, "dmo:deadline", Instant.class, value -> {
                value.add(Instant.now());
            });

            recrd.withNestedAttributeValue(unit, "dmo:reading", Double.class, value -> {
                value.add(Math.PI);
                value.add(Math.E);
            });

            // Test where we add an attribute that also exists at unit level.
            recrd.withNestedAttributeValue(unit, "dce:title", String.class, value -> {
                value.add("Handling of " + "*some shipment id*");
            });
        });

        repo.storeUnit(unit);

        Optional<Unit> _sameUnit = repo.getUnit(unit.getTenantId(), unit.getUnitId());
        if (_sameUnit.isEmpty()) {
            fail("Unit " + unit.getReference() + " exists, but is not retrievable");
        }

        Unit sameUnit = _sameUnit.get();
        Optional<Attribute<String>> _title = sameUnit.getStringAttribute("dce:title");
        if (_title.isEmpty()) {
            fail("Attribute 'dce:title' not found in unit " + unit.getReference() + " but is known to exist");
        }
        Attribute<String> title = _title.get();
        ArrayList<String> valueVector = title.getValueVector();

        if (valueVector.size() != 1) {
            fail("Attribute 'dce:title' must have exactly one value");
        }

        if (!valueVector.get(0).equals(unitTitle)) {
            fail("Attribute 'dce:title' does not have expected value: \"" + unitTitle + "\" != \"" + valueVector.get(0) + "\"");
        }
    }
}
