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

public class LongAttributeSearchItem extends AttributeSearchItem<Long> {

    private final long value;

    public LongAttributeSearchItem(int attrId, Operator operator, long value) {
        super(Type.LONG, operator, attrId);
        this.value = value;
    }

    public Long getValue() {
        return value;
    }

    /**
     * Generates constraint "long attribute == value" for
     * specified attribute id.
     */
    public static LeafExpression<LongAttributeSearchItem> constrainOnEQ(int attrId, long value) {
        return new LeafExpression<>(new LongAttributeSearchItem(attrId, Operator.EQ, value));
    }

    /**
     * Generates constraint "long attribute == value" for
     * specified attribute id.
     */
    public static LeafExpression<LongAttributeSearchItem> constrainOnEQ(int attrId, String value) {
        return new LeafExpression<>(new LongAttributeSearchItem(attrId, Operator.EQ, Long.parseLong(value)));
    }
}
