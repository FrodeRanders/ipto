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
