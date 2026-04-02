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

import org.gautelis.ipto.repo.search.model.Operator;
import org.gautelis.ipto.repo.search.model.SearchItem;

import java.util.Map;
import java.util.Objects;

/**
 * Base class for binary boolean search expressions.
 */
public sealed abstract class BinaryExpression implements SearchExpression
        permits AndExpression, OrExpression {

    protected final SearchExpression left, right;
    protected final Operator operator;

    /**
     * Creates a binary expression over two subexpressions.
     *
     * @param operator the boolean operator joining the subexpressions
     * @param left the left-hand expression
     * @param right the right-hand expression
     */
    protected BinaryExpression(Operator operator, SearchExpression left, SearchExpression right) {
        Objects.requireNonNull(operator, "operator");
        Objects.requireNonNull(left, "left");
        Objects.requireNonNull(right, "right");

        this.operator = operator;
        this.left = left;
        this.right = right;
    }

    /**
     * Returns the boolean operator joining the two subexpressions.
     *
     * @return the expression operator
     */
    protected Operator operator() {
        return operator;
    }

    /**
     * Returns the left-hand subexpression.
     *
     * @return the left-hand expression
     */
    public SearchExpression getLeft() {
        return left;
    }

    /**
     * Returns the right-hand subexpression.
     *
     * @return the right-hand expression
     */
    public SearchExpression getRight() {
        return right;
    }

    @Override public final String toSql(
            SearchStrategy strategy,
            boolean usePrepare,
            Map<String, SearchItem<?>> commonConstraintValues,
            Map<String, Integer> attributeNameToId
    ) {
        StringBuilder sb = new StringBuilder();
        sb.append("(").append(left.toSql(strategy, usePrepare, commonConstraintValues, attributeNameToId)).append(" OR ");
        sb.append(" ").append(operator).append(" ");
        sb.append(right.toSql(strategy, usePrepare, commonConstraintValues, attributeNameToId)).append(")");
        return sb.toString();
    }
}
