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
package org.gautelis.ipto.graphql.runtime.wiring;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SubscriptionWiringPolicyTest {
    @Test
    void defaultsToScaffold() {
        String previous = System.getProperty(SubscriptionWiringPolicy.PROPERTY);
        try {
            System.clearProperty(SubscriptionWiringPolicy.PROPERTY);
            assertEquals(SubscriptionWiringPolicy.SCAFFOLD, SubscriptionWiringPolicy.resolve());
        } finally {
            restore(previous);
        }
    }

    @Test
    void resolvesFromSystemProperty() {
        String previous = System.getProperty(SubscriptionWiringPolicy.PROPERTY);
        try {
            System.setProperty(SubscriptionWiringPolicy.PROPERTY, "fail_fast");
            assertEquals(SubscriptionWiringPolicy.FAIL_FAST, SubscriptionWiringPolicy.resolve());
        } finally {
            restore(previous);
        }
    }

    @Test
    void rejectsUnknownValue() {
        assertThrows(
                IllegalArgumentException.class,
                () -> SubscriptionWiringPolicy.parse("whatever")
        );
    }

    private static void restore(String previous) {
        if (previous == null) {
            System.clearProperty(SubscriptionWiringPolicy.PROPERTY);
        } else {
            System.setProperty(SubscriptionWiringPolicy.PROPERTY, previous);
        }
    }
}
