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

/**
 * Boolean conjunction of two child search expressions.
 */
public final class AndExpression extends BinaryExpression implements SearchExpression {
    /**
     * Creates a conjunction expression.
     *
     * @param left left-hand child expression
     * @param right right-hand child expression
     */
    public AndExpression(SearchExpression left, SearchExpression right) {
        super(Operator.AND, left, right);
    }
}
