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
import org.gautelis.repo.exceptions.InvalidParameterException;
import org.gautelis.repo.model.AttributeType;
import org.gautelis.repo.search.query.LeafExpression;

import java.util.Objects;


/**
 *
 */
public class StringUnitSearchItem extends UnitSearchItem<String> {

    private final String value;

    public StringUnitSearchItem(
            Column column, Operator operator, String value
    ) {
        super(AttributeType.STRING, column, operator);
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    /**
     * Generates constraint "unit has specific name", identified by 'name'.
     * <p>
     * Will handle string and character wildcards in name.
     *
     * @throws InvalidParameterException if no name was specified.
     */
    public static LeafExpression<StringUnitSearchItem> constrainToSpecificName(
            String name
    ) throws InvalidParameterException {
        Objects.requireNonNull(name, "name");

        if (name.isEmpty()) {
            throw new InvalidParameterException("No name provided");
        }

        name = name.replace('*', '%');
        boolean useLIKE = (name.indexOf('%') >= 0 || name.indexOf('_') >= 0);  // Uses wildcard

        if (useLIKE) {
            return new LeafExpression<>(new StringUnitSearchItem(Column.UNIT_NAME, Operator.LIKE, name));
        } else {
            return new LeafExpression<>(new StringUnitSearchItem(Column.UNIT_NAME, Operator.EQ, name));
        }
    }
}
