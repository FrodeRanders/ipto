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
package org.gautelis.repo.search.model;

import org.gautelis.repo.db.Column;
import org.gautelis.repo.model.attributes.Type;

import java.time.Instant;

/**
 *
 */
public class TimeUnitSearchItem extends UnitSearchItem<Instant> {

    private final Instant instant;

    public TimeUnitSearchItem(Column column, Operator operator, Instant instant) {
        super(Type.TIME, column, operator);
        this.instant = instant;
    }

    public Instant getValue() {
        return instant;
    }
}
