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

import java.util.Objects;

public class StringAttributeSearchItem extends AttributeSearchItem<String> {

    private final String value;

    public StringAttributeSearchItem(String attrName, Operator operator, String value) {
        super(AttributeType.STRING, operator, attrName);
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    /**
     * Generates constraint "string attribute == value" for
     * specified attribute.
     */
    public static LeafExpression<StringAttributeSearchItem> constrainOnEQ(String attrName, String value) {
        Objects.requireNonNull(value, "value");

        value = value.replace('\'', ' ').replace('\"', ' ');
        value = value.replace('*', '%');
        boolean useLIKE = value.indexOf('%') >= 0 || value.indexOf('_') >= 0;  // Uses wildcard

        if (useLIKE) {
            return new LeafExpression<>(new StringAttributeSearchItem(attrName, Operator.LIKE, value));
        } else {
            return new LeafExpression<>(new StringAttributeSearchItem(attrName, Operator.EQ, value));
        }
    }
}
