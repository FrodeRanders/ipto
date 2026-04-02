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

import org.gautelis.ipto.repo.search.model.SearchItem;

import java.util.Map;

/**
 * Backend-neutral boolean search-expression tree.
 * <p>
 * A search expression is composed of leaf predicates and boolean operators. The
 * Java search subsystem can build this tree programmatically or parse it from
 * the textual query language before translating it into SQL through the search
 * adapters.
 */
public sealed interface SearchExpression
        permits LeafExpression, BinaryExpression, AndExpression, OrExpression, NotExpression {

    /**
     * Renders this expression into SQL for the chosen search strategy.
     *
     * @param strategy the SQL-generation strategy to use
     * @param usePrepare whether generated SQL should use prepared placeholders
     * @param commonConstraintValues receives extracted prepared-statement values
     * @param attributeNameToId mapping from attribute names to catalog ids
     * @return the generated SQL fragment
     */
    String toSql(SearchStrategy strategy, boolean usePrepare, Map<String, SearchItem<?>> commonConstraintValues, Map<String, Integer> attributeNameToId);
}
