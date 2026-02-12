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
package org.gautelis.ipto.graphql.model;

import java.util.Locale;
import java.util.Optional;

public enum RuntimeOperation {
    LOAD_UNIT,
    LOAD_UNIT_RAW,
    LOAD_BY_CORRID,
    LOAD_RAW_PAYLOAD_BY_CORRID,
    SEARCH,
    SEARCH_RAW,
    SEARCH_RAW_PAYLOAD,
    STORE_RAW_UNIT,
    CUSTOM,
    MANUAL;

    public static Optional<RuntimeOperation> parse(String value) {
        if (value == null) {
            return Optional.empty();
        }
        String normalized = value.trim();
        if (normalized.isEmpty()) {
            return Optional.empty();
        }
        try {
            return Optional.of(RuntimeOperation.valueOf(normalized.toUpperCase(Locale.ROOT)));
        } catch (IllegalArgumentException iae) {
            return Optional.empty();
        }
    }
}

