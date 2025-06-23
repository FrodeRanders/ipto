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

public class DoubleAttributeSearchItem extends AttributeSearchItem<Double> {

    private final double value;

    public DoubleAttributeSearchItem(int attrId, Operator operator, double value) {
        super(Type.DOUBLE, operator, attrId);
        this.value = value;
    }

    public Double getValue() {
        return value;
    }

    /**
     * Generates constraint "double attribute == value" for
     * specified attribute id.
     */
    public static LeafExpression<DoubleAttributeSearchItem> constrainOnEQ(int attrId, double value) {
        return new LeafExpression<>(new DoubleAttributeSearchItem(attrId, Operator.EQ, value));
    }

    /**
     * Generates constraint "double attribute == value" for
     * specified attribute id.
     */
    public static LeafExpression<DoubleAttributeSearchItem> constrainOnEQ(int attrId, String value) {
        return new LeafExpression<>(new DoubleAttributeSearchItem(attrId, Operator.EQ, Double.parseDouble(value)));
    }
}
