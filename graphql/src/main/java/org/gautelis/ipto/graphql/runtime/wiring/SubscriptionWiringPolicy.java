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

import java.util.Locale;

public enum SubscriptionWiringPolicy {
    SCAFFOLD,
    FAIL_FAST;

    public static final String PROPERTY = "ipto.graphql.subscription.policy";
    public static final String ENV = "IPTO_GRAPHQL_SUBSCRIPTION_POLICY";

    public static SubscriptionWiringPolicy resolve() {
        String value = System.getProperty(PROPERTY);
        if (value == null || value.isBlank()) {
            value = System.getenv(ENV);
        }
        if (value == null || value.isBlank()) {
            return SCAFFOLD;
        }
        return parse(value);
    }

    static SubscriptionWiringPolicy parse(String value) {
        String normalized = value.trim().toLowerCase(Locale.ROOT);
        return switch (normalized) {
            case "scaffold", "lenient", "runtime_error" -> SCAFFOLD;
            case "fail_fast", "fail-fast", "strict" -> FAIL_FAST;
            default -> throw new IllegalArgumentException(
                    "Unknown subscription wiring policy '" + value + "'. "
                            + "Valid values are scaffold|fail_fast (property "
                            + PROPERTY + ", env " + ENV + ")"
            );
        };
    }
}
