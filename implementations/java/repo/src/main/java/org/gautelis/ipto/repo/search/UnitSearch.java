/*
 * Copyright (C) 2024-2026 Frode Randers
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
package org.gautelis.ipto.repo.search;

import org.gautelis.ipto.repo.search.query.SearchExpression;
import org.gautelis.ipto.repo.search.query.SearchOrder;
import org.gautelis.ipto.repo.search.query.SearchStrategy;

import java.util.Objects;

/**
 * Data used when searching for units in the database.
 */
public class UnitSearch {
    private final int pageOffset;
    private final int pageSize;
    private final int selectionSize;

    private final SearchExpression expression;
    private final SearchStrategy strategy;
    private final SearchOrder order;

    /**
     * Creates a bundle of data containing search expression, using default sort order.
     *
     * @param expression search expression to evaluate
     * @param strategy SQL compilation strategy to use
     */
    public UnitSearch(
            SearchExpression expression,
            SearchStrategy strategy
    ) {
        Objects.requireNonNull(expression, "expression");

        this.expression = expression;
        this.strategy = strategy;
        this.order = SearchOrder.getDefaultOrder();
        this.pageOffset = 0;
        this.pageSize = 0;
        this.selectionSize = 0;
    }

    /**
     * Creates a bundle of data containing search expression, how to sort results.
     *
     * @param expression search expression to evaluate
     * @param strategy SQL compilation strategy to use
     * @param order result ordering
     */
    public UnitSearch(
            SearchExpression expression,
            SearchStrategy strategy,
            SearchOrder order
    ) {
        Objects.requireNonNull(expression, "expression");
        Objects.requireNonNull(order, "order");

        this.expression = expression;
        this.strategy = strategy;
        this.order = order;
        this.pageOffset = 0;
        this.pageSize = 0;
        this.selectionSize = 0;
    }

    /**
     * Creates a bundle of data containing search expression, how to sort results,
     * as well as how to limit search results to 'selectionSize'.
     *
     * @param expression search expression to evaluate
     * @param strategy SQL compilation strategy to use
     * @param order result ordering
     * @param selectionSize maximum number of rows to return
     */
    public UnitSearch(
            SearchExpression expression,
            SearchStrategy strategy,
            SearchOrder order,
            int selectionSize
    ) {
        Objects.requireNonNull(expression, "expression");
        Objects.requireNonNull(order, "order");

        this.expression = expression;
        this.strategy = strategy;
        this.order = order;
        this.pageOffset = 0;
        this.pageSize = 0;
        this.selectionSize = selectionSize;
    }

    /**
     * Creates a bundle of data containing search expression, how to sort results,
     * as well as how to page among search results.
     *
     * @param expression search expression to evaluate
     * @param strategy SQL compilation strategy to use
     * @param order result ordering
     * @param pageOffset row offset of the first result in the page
     * @param pageSize maximum number of rows in the page
     */
    public UnitSearch(
            SearchExpression expression,
            SearchStrategy strategy,
            SearchOrder order,
            int pageOffset,
            int pageSize
    ) {
        Objects.requireNonNull(expression, "expression");
        Objects.requireNonNull(order, "order");

        this.expression = expression;
        this.strategy = strategy;
        this.order = order;
        this.pageOffset = pageOffset;
        this.pageSize = pageSize;
        this.selectionSize = 0;
    }

    /**
     * Returns the search expression.
     *
     * @return search expression
     */
    public SearchExpression getExpression() {
        return expression;
    }

    /**
     * Returns the SQL compilation strategy.
     *
     * @return search strategy
     */
    public SearchStrategy getStrategy() {
        return strategy;
    }

    /**
     * Returns the requested sort order.
     *
     * @return sort order
     */
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
