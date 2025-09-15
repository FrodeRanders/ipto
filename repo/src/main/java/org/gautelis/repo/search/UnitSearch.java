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
package org.gautelis.repo.search;

import org.gautelis.repo.search.query.SearchExpression;
import org.gautelis.repo.search.query.SearchOrder;

import java.util.Objects;

/**
 * Data used when searching for units in the database.
 */
public class UnitSearch {
    private final int pageOffset;
    private final int pageSize;
    private final int selectionSize;

    private final SearchExpression expression;
    private final SearchOrder order;

    /**
     * Creates a bundle of data containing search expression, using default sort order.
     * <p/>
     * @param expression
     */
    public UnitSearch(
            SearchExpression expression
    ) {
        Objects.requireNonNull(expression, "expression");

        this.expression = expression;
        this.order = SearchOrder.getDefaultOrder();
        this.pageOffset = 0;
        this.pageSize = 0;
        this.selectionSize = 0;
    }

    /**
     * Creates a bundle of data containing search expression, how to sort results.
     * <p/>
     * @param expression
     * @param order
     */
    public UnitSearch(
            SearchExpression expression,
            SearchOrder order
    ) {
        Objects.requireNonNull(expression, "expression");
        Objects.requireNonNull(order, "order");

        this.expression = expression;
        this.order = order;
        this.pageOffset = 0;
        this.pageSize = 0;
        this.selectionSize = 0;
    }

    /**
     * Creates a bundle of data containing search expression, how to sort results,
     * as well as how to limit search results to 'selectionSize'.
     * <p/>
     * @param expression
     * @param order
     * @param selectionSize
     */
    public UnitSearch(
            SearchExpression expression,
            SearchOrder order,
            int selectionSize
    ) {
        Objects.requireNonNull(expression, "expression");
        Objects.requireNonNull(order, "order");

        this.expression = expression;
        this.order = order;
        this.pageOffset = 0;
        this.pageSize = 0;
        this.selectionSize = selectionSize;
    }

    /**
     * Creates a bundle of data containing search expression, how to sort results,
     * as well as how to page among search results.
     * </p>
     * @param expression
     * @param order
     * @param pageOffset
     * @param pageSize
     */
    public UnitSearch(
            SearchExpression expression,
            SearchOrder order,
            int pageOffset,
            int pageSize
    ) {
        Objects.requireNonNull(expression, "expression");
        Objects.requireNonNull(order, "order");

        this.expression = expression;
        this.order = order;
        this.pageOffset = pageOffset;
        this.pageSize = pageSize;
        this.selectionSize = 0;
    }

    public SearchExpression getExpression() {
        return expression;
    }

    public SearchOrder getOrder() {
        return order;
    }

    /**
     * Offset of page in result, measured in rows.
     */
    public int getPageOffset() {
        return pageOffset;
    }

    /**
     * Size of page.
     */
    public int getPageSize() {
        return pageSize;
    }

    /**
     * Maximal size of selection.
     */
    public int getSelectionSize() {
        return selectionSize;
    }
}
