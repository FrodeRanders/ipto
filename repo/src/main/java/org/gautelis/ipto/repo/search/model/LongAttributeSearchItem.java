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
package org.gautelis.ipto.repo.search.model;

import org.gautelis.ipto.repo.model.AttributeType;
import org.gautelis.ipto.repo.search.query.LeafExpression;

public class LongAttributeSearchItem extends AttributeSearchItem<Long> {

    private final long value;

    public LongAttributeSearchItem(String attrName, Operator operator, long value) {
        super(AttributeType.LONG, operator, attrName);
        this.value = value;
    }

    public Long getValue() {
        return value;
    }

    /**
     * Generates constraint "long attribute == value" for
     * specified attribute.
     */
    public static LeafExpression<LongAttributeSearchItem> constrainOnEQ(String attrName, long value) {
        return new LeafExpression<>(new LongAttributeSearchItem(attrName, Operator.EQ, value));
    }

    /**
     * Generates constraint "long attribute == value" for
     * specified attribute.
     */
    public static LeafExpression<LongAttributeSearchItem> constrainOnEQ(String attrName, String value) {
        return new LeafExpression<>(new LongAttributeSearchItem(attrName, Operator.EQ, Long.parseLong(value)));
    }
}
