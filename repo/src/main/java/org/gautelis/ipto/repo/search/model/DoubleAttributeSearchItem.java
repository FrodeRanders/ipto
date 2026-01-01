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

public class DoubleAttributeSearchItem extends AttributeSearchItem<Double> {

    private final double value;

    public DoubleAttributeSearchItem(String attrName, Operator operator, double value) {
        super(AttributeType.DOUBLE, operator, attrName);
        this.value = value;
    }

    public Double getValue() {
        return value;
    }

    /**
     * Generates constraint "double attribute == value" for
     * specified attribute.
     */
    public static LeafExpression<DoubleAttributeSearchItem> constrainOnEQ(String attrName, double value) {
        return new LeafExpression<>(new DoubleAttributeSearchItem(attrName, Operator.EQ, value));
    }

    /**
     * Generates constraint "double attribute == value" for
     * specified attribute.
     */
    public static LeafExpression<DoubleAttributeSearchItem> constrainOnEQ(String attrName, String value) {
        return new LeafExpression<>(new DoubleAttributeSearchItem(attrName, Operator.EQ, Double.parseDouble(value)));
    }

    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder("DoubleAttributeSearchItem{");
        buf.append(super.toString());
        buf.append(", value='").append(value).append("'");
        buf.append("}");
        return buf.toString();
    }
}
