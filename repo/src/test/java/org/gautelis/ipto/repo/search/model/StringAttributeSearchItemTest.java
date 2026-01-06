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
package org.gautelis.ipto.repo.search.model;

import org.gautelis.ipto.repo.search.query.LeafExpression;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class StringAttributeSearchItemTest {

    @Test
    void constrainOnEQUsesEqualityWhenNoWildcards() {
        LeafExpression<StringAttributeSearchItem> expr =
                StringAttributeSearchItem.constrainOnEQ("title", "Hello");

        StringAttributeSearchItem item = expr.getItem();
        assertEquals("title", item.getAttrName());
        assertEquals(Operator.EQ, item.getOperator());
        assertEquals("Hello", item.getValue());
    }

    @Test
    void constrainOnEQConvertsWildcardsToLike() {
        LeafExpression<StringAttributeSearchItem> expr =
                StringAttributeSearchItem.constrainOnEQ("title", "Hello*world");

        StringAttributeSearchItem item = expr.getItem();
        assertEquals(Operator.LIKE, item.getOperator());
        assertEquals("Hello%world", item.getValue());
    }

    @Test
    void constrainOnEQTreatsUnderscoreAsWildcard() {
        LeafExpression<StringAttributeSearchItem> expr =
                StringAttributeSearchItem.constrainOnEQ("title", "x_y");

        StringAttributeSearchItem item = expr.getItem();
        assertEquals(Operator.LIKE, item.getOperator());
        assertEquals("x_y", item.getValue());
    }

    @Test
    void constrainOnEQSanitizesQuotes() {
        LeafExpression<StringAttributeSearchItem> expr =
                StringAttributeSearchItem.constrainOnEQ("title", "O'Hara\"s");

        StringAttributeSearchItem item = expr.getItem();
        assertEquals("O Hara s", item.getValue());
    }
}
