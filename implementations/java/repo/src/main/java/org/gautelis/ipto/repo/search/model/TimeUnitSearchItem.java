/*
 * Copyright (C) 2024-2026 Frode Randers
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
package org.gautelis.ipto.repo.search.model;

import org.gautelis.ipto.repo.db.Column;
import org.gautelis.ipto.repo.model.AttributeType;

import java.time.Instant;

/**
 *
 */
public class TimeUnitSearchItem extends UnitSearchItem<Instant> {

    private final Instant value;

    public TimeUnitSearchItem(Column column, Operator operator, Instant instant) {
        // This isn't an attribute, but we currently use AttributeType.STRING
        // to tell how we want to treat the value when searching.
        super(AttributeType.TIME, column, operator);
        this.value = instant;
    }

    public Instant getValue() {
        return value;
    }

    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder("TimeUnitSearchItem{");
        buf.append(super.toString());
        buf.append(", value='").append(value).append("'");
        buf.append("}");
        return buf.toString();
    }
}
