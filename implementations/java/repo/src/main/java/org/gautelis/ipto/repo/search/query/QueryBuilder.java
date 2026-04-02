/*
 * Copyright (C) 2025-2026 Frode Randers
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

/**
 * Factory methods for constructing search-expression trees.
 * <p>
 * This class provides a small programmatic DSL for combining
 * {@link SearchExpression} instances and creating common leaf predicates over
 * unit fields and typed attributes.
 */
public class QueryBuilder {
    private QueryBuilder() {}

    /**
     * Combines two search expressions with logical {@code AND}.
     *
     * @param left the left-hand expression
     * @param right the right-hand expression
     * @return the combined expression
     */
    public static SearchExpression assembleAnd(
            SearchExpression left, SearchExpression right
    ) {
        return new AndExpression(left, right);
    }

    /**
     * Combines an existing expression with one leaf predicate using logical
     * {@code AND}.
     *
     * @param left the existing expression
     * @param right the leaf predicate to append
     * @return the combined expression
     */
    public static SearchExpression assembleAnd(
            SearchExpression left, LeafExpression<?> right
    ) {
        return new AndExpression(left, right);
    }

    /**
     * Combines an existing expression with one search item using logical
     * {@code AND}.
     *
     * @param left the existing expression
     * @param item the predicate to append
     * @return the combined expression
     */
    public static SearchExpression assembleAnd(
            SearchExpression left, SearchItem<?> item
    ) {
        return assembleAnd(left, new LeafExpression<>(item));
    }

    /**
     * Combines an existing expression with the negation of one leaf predicate
     * using logical {@code AND}.
     *
     * @param left the existing expression
     * @param right the leaf predicate to negate and append
     * @return the combined expression
     */
    public static SearchExpression assembleAndNot(
            SearchExpression left, LeafExpression<?> right
    ) {
        return new AndExpression(left, new NotExpression(right));
    }

    /**
     * Combines an existing expression with the negation of one search item
     * using logical {@code AND}.
     *
     * @param left the existing expression
     * @param item the predicate to negate and append
     * @return the combined expression
     */
    public static SearchExpression assembleAndNot(
            SearchExpression left, SearchItem<?> item
    ) {
        return assembleAndNot(left, new LeafExpression<>(item));
    }

    /**
     * Combines two search expressions with logical {@code OR}.
     *
     * @param left the left-hand expression
     * @param right the right-hand expression
     * @return the combined expression
     */
    public static SearchExpression assembleOr(
            SearchExpression left, SearchExpression right
    ) {
        return new OrExpression(left, right);
    }

    /**
     * Combines an existing expression with one leaf predicate using logical
     * {@code OR}.
     *
     * @param left the existing expression
     * @param right the leaf predicate to append
     * @return the combined expression
     */
    public static SearchExpression assembleOr(
            SearchExpression left, LeafExpression<?> right
    ) {
        return new OrExpression(left, right);
    }

    /**
     * Combines an existing expression with one search item using logical
     * {@code OR}.
     *
     * @param left the existing expression
     * @param item the predicate to append
     * @return the combined expression
     */
    public static SearchExpression assembleOr(
            SearchExpression left, SearchItem<?> item
    ) {
        return assembleOr(left, new LeafExpression<>(item));
    }


    /**
     * Combines an existing expression with the negation of one leaf predicate
     * using logical {@code OR}.
     *
     * @param left the existing expression
     * @param right the leaf predicate to negate and append
     * @return the combined expression
     */
    public static SearchExpression assembleOrNot(
            SearchExpression left, LeafExpression<?> right
    ) {
        return new OrExpression(left, new NotExpression(right));
    }

    /**
     * Combines an existing expression with the negation of one search item
     * using logical {@code OR}.
     *
     * @param left the existing expression
     * @param item the predicate to negate and append
     * @return the combined expression
     */
    public static SearchExpression assembleOrNot(
            SearchExpression left, SearchItem<?> item
    ) {
        return assembleOrNot(left, new LeafExpression<>(item));
    }


    /**
     * Creates an equality predicate for one configured attribute.
     * <p>
     * The value is interpreted according to the declared attribute type. Data
     * and record attributes are not searchable through this helper.
     *
     * @param attribute the configured attribute
     * @param value the textual value to parse
     * @param locale the locale to use for locale-sensitive parsing
     * @return a typed leaf expression
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
     * Creates a predicate matching one specific unit status.
     *
     * @param status the status to match
     * @return a unit-status predicate
     */
     public static LeafExpression<IntegerUnitSearchItem> constrainToSpecificStatus(Unit.Status status) {
        return new LeafExpression<>(new IntegerUnitSearchItem(Column.UNIT_KERNEL_STATUS, Operator.EQ, status.getStatus()));
     }

     /**
     * Creates a predicate matching one specific tenant id.
     *
     * @param tenantId the tenant id to match
     * @return a tenant predicate
     */
     public static LeafExpression<IntegerUnitSearchItem> constrainToSpecificTenant(int tenantId) {
        return new LeafExpression<>(new IntegerUnitSearchItem(Column.UNIT_KERNEL_TENANTID, Operator.EQ, tenantId));
     }

     /**
     * Creates a predicate matching effective or later unit states.
     *
     * @return a status predicate for effective units
     */
     public static LeafExpression<IntegerUnitSearchItem> constrainToEffective() {
        return new LeafExpression<>(new IntegerUnitSearchItem(Column.UNIT_KERNEL_STATUS, Operator.GEQ, Unit.Status.EFFECTIVE.getStatus()));
     }

     /**
     * Creates a strict upper-bound predicate on creation time.
     *
     * @param instant the exclusive creation-time upper bound
     * @return a creation-time predicate
     */
     public static LeafExpression<TimeUnitSearchItem> constrainToCreatedBefore(Instant instant) {
        Objects.requireNonNull(instant, "instant");
        return new LeafExpression<>(new TimeUnitSearchItem(Column.UNIT_KERNEL_CREATED, Operator.LT, instant));
     }

     /**
     * Creates an inclusive lower-bound predicate on creation time.
     *
     * @param instant the inclusive creation-time lower bound
     * @return a creation-time predicate
     */
     public static LeafExpression<TimeUnitSearchItem> constrainToCreatedAfter(Instant instant) {
        Objects.requireNonNull(instant, "instant");
        return new LeafExpression<>(new TimeUnitSearchItem(Column.UNIT_KERNEL_CREATED, Operator.GEQ, instant));
     }
}

