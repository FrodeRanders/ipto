/*
 * Copyright (C) 2025 Frode Randers
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
package org.gautelis.ipto.repo.search.query;

import org.gautelis.ipto.repo.db.Column;
import org.gautelis.ipto.repo.exceptions.InvalidParameterException;
import org.gautelis.ipto.repo.model.Unit;
import org.gautelis.ipto.repo.model.attributes.Attribute;
import org.gautelis.ipto.repo.model.AttributeType;
import org.gautelis.ipto.repo.search.model.*;

import java.time.Instant;
import java.util.Locale;
import java.util.Objects;

public class QueryBuilder {
    private QueryBuilder() {}

    /**
     * Helper method that adds two search expressions with logical
     * operator AND.
     *
     * @param left existing search expression
     * @param right existing search expression
     * @return search expression
     * @throws InvalidParameterException
     */
    public static SearchExpression assembleAnd(
            SearchExpression left, SearchExpression right
    ) {
        return new AndExpression(left, right);
    }

    /**
     * Helper method that adds a search item to an expression with logical
     * operator AND (item).
     *
     * @param left existing expression
     * @param right item to add
     * @return search expression
     * @throws InvalidParameterException
     */
    public static SearchExpression assembleAnd(
            SearchExpression left, LeafExpression<?> right
    ) {
        return new AndExpression(left, right);
    }

    public static SearchExpression assembleAnd(
            SearchExpression left, SearchItem<?> item
    ) {
        return assembleAnd(left, new LeafExpression<>(item));
    }

    /**
     * Helper method that adds a search item to an expression with logical
     * operator AND NOT (item).
     *
     * @param left existing expression
     * @param right item to add
     * @return search expression
     * @throws InvalidParameterException
     */
    public static SearchExpression assembleAndNot(
            SearchExpression left, LeafExpression<?> right
    ) {
        return new AndExpression(left, new NotExpression(right));
    }

    public static SearchExpression assembleAndNot(
            SearchExpression left, SearchItem<?> item
    ) {
        return assembleAndNot(left, new LeafExpression<>(item));
    }

    /**
     * Helper method that adds two search expressions with logical
     * operator OR.
     *
     * @param left existing search expression
     * @param right existing search expression
     * @return search expression
     * @throws InvalidParameterException
     */
    public static SearchExpression assembleOr(
            SearchExpression left, SearchExpression right
    ) {
        return new OrExpression(left, right);
    }

    /**
     * Helper method that adds a search item to an expression with logical
     * operator OR (item).
     *
     * @param left existing expression
     * @param right item to add
     * @return search expression
     * @throws InvalidParameterException
     */
    public static SearchExpression assembleOr(
            SearchExpression left, LeafExpression<?> right
    ) {
        return new OrExpression(left, right);
    }

    public static SearchExpression assembleOr(
            SearchExpression left, SearchItem<?> item
    ) {
        return assembleOr(left, new LeafExpression<>(item));
    }


    /**
     * Helper method that adds a search item to an expression with logical
     * operator OR NOT (item).
     *
     * @param left existing expression
     * @param right item to add
     * @return search expression
     * @throws InvalidParameterException
     */
    public static SearchExpression assembleOrNot(
            SearchExpression left, LeafExpression<?> right
    ) {
        return new OrExpression(left, new NotExpression(right));
    }

    public static SearchExpression assembleOrNot(
            SearchExpression left, SearchItem<?> item
    ) {
        return assembleOrNot(left, new LeafExpression<>(item));
    }


    /**
     * Generates constraint "attribute == value" for specified attribute.
     * <p>
     * This method will handle the various attribute types, EXCEPT 'DATA' and 'RECORD'.
     */
    public static LeafExpression<? extends AttributeSearchItem<?>> constrainOnValueEQ(
            Attribute<?> attribute, String value, Locale locale
    ) throws NumberFormatException, InvalidParameterException {

        String attrName = attribute.getName();
        AttributeType type = attribute.getType();

        return switch (type) {
            case STRING -> StringAttributeSearchItem.constrainOnEQ(attrName, value);
            case TIME -> TimeAttributeSearchItem.constrainOnEQ(attrName, value); // ISO time
            case INTEGER -> IntegerAttributeSearchItem.constrainOnEQ(attrName, value);
            case LONG -> LongAttributeSearchItem.constrainOnEQ(attrName, value);
            case DOUBLE -> DoubleAttributeSearchItem.constrainOnEQ(attrName, value);
            case BOOLEAN -> BooleanAttributeSearchItem.constrainOnEQ(attrName, value);
            default -> throw new InvalidParameterException("Attribute type " + type + " is not searchable: " + attribute);
        };
    }

    /**
     * Generates constraint "unit has specific status", identified by
     * the enum Unit.Status.
     *
     * @param status
     * @return
     */
     public static LeafExpression<IntegerUnitSearchItem> constrainToSpecificStatus(Unit.Status status) {
        return new LeafExpression<>(new IntegerUnitSearchItem(Column.UNIT_KERNEL_STATUS, Operator.EQ, status.getStatus()));
     }

     /**
     * Generates constraint "unit has specific tenant", identified by tenantId.
     */
     public static LeafExpression<IntegerUnitSearchItem> constrainToSpecificTenant(int tenantId) {
        return new LeafExpression<>(new IntegerUnitSearchItem(Column.UNIT_KERNEL_TENANTID, Operator.EQ, tenantId));
     }

     /**
     * Generates constraint "unit is effective/operative/in use"
     */
     public static LeafExpression<IntegerUnitSearchItem> constrainToEffective() {
        return new LeafExpression<>(new IntegerUnitSearchItem(Column.UNIT_KERNEL_STATUS, Operator.GEQ, Unit.Status.EFFECTIVE.getStatus()));
     }

     /**
     * Generates a constraint "unit was created before" (strict less than)
     */
     public static LeafExpression<TimeUnitSearchItem> constrainToCreatedBefore(Instant instant) {
        Objects.requireNonNull(instant, "instant");
        return new LeafExpression<>(new TimeUnitSearchItem(Column.UNIT_KERNEL_CREATED, Operator.LT, instant));
     }

     /**
     * Generates a constraint "unit was created after (inclusive)"
     */
     public static LeafExpression<TimeUnitSearchItem> constrainToCreatedAfter(Instant instant) {
        Objects.requireNonNull(instant, "instant");
        return new LeafExpression<>(new TimeUnitSearchItem(Column.UNIT_KERNEL_CREATED, Operator.GEQ, instant));
     }
}


