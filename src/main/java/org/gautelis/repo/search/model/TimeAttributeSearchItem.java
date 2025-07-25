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

import org.gautelis.repo.model.attributes.Type;
import org.gautelis.repo.search.query.LeafExpression;
import org.gautelis.repo.utils.TimeHelper;

import java.time.Instant;

public class TimeAttributeSearchItem extends AttributeSearchItem<Instant> {

    private final Instant value;

    public TimeAttributeSearchItem(int attrId, Operator operator, Instant value) {
        super(Type.TIME, operator, attrId);
        this.value = value;
    }

    public Instant getValue() {
        return value;
    }

    /**
     * Generates constraint "date attribute == value" for
     * specified attribute id.
     */
    public static LeafExpression<TimeAttributeSearchItem> constrainOnEQ(int attrId, Instant value) {
        return new LeafExpression<>(new TimeAttributeSearchItem(attrId, Operator.EQ, value));
    }

    /**
     * Generates constraint "date attribute == value" for
     * specified attribute id.
     */
    public static LeafExpression<TimeAttributeSearchItem> constrainOnEQ(int attrId, String value) {
        return new LeafExpression<>(new TimeAttributeSearchItem(attrId, Operator.EQ, TimeHelper.parseInstant(value)));
    }
}
