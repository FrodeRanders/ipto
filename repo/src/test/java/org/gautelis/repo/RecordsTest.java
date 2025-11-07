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
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;

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
    @SuppressWarnings("unchecked")
    public void test() {
        Repository repo = RepositoryFactory.getRepository();
        final int tenantId = 1;

        Unit unit = repo.createUnit(tenantId, "a record instance");

        unit.withAttributeValue("dc:title", String.class, value -> {
            value.add("Handling of " + "*some order id*");
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
        });

        repo.storeUnit(unit);
    }
}
