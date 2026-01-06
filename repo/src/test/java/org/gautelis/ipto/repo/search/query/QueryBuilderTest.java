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
import org.gautelis.ipto.repo.model.AttributeType;
import org.gautelis.ipto.repo.model.Unit;
import org.gautelis.ipto.repo.model.attributes.Attribute;
import org.gautelis.ipto.repo.search.model.*;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Locale;

import static org.junit.jupiter.api.Assertions.*;

class QueryBuilderTest {

    @Test
    void constrainOnValueEQBuildsStringSearchItem() throws Exception {
        Attribute<String> attribute = new Attribute<>(1, "title", null, AttributeType.STRING);

        LeafExpression<? extends AttributeSearchItem<?>> expr =
                QueryBuilder.constrainOnValueEQ(attribute, "alpha", Locale.ROOT);

        assertInstanceOf(StringAttributeSearchItem.class, expr.getItem());
        StringAttributeSearchItem item = (StringAttributeSearchItem) expr.getItem();
        assertEquals("title", item.getAttrName());
        assertEquals(Operator.EQ, item.getOperator());
        assertEquals("alpha", item.getValue());
    }

    @Test
    void constrainOnValueEQBuildsTypedItems() throws Exception {
        Attribute<Long> attribute = new Attribute<>(2, "size", null, AttributeType.LONG);

        LeafExpression<? extends AttributeSearchItem<?>> expr =
                QueryBuilder.constrainOnValueEQ(attribute, "42", Locale.ROOT);

        assertInstanceOf(LongAttributeSearchItem.class, expr.getItem());
        LongAttributeSearchItem item = (LongAttributeSearchItem) expr.getItem();
        assertEquals(42L, item.getValue());
        assertEquals(Operator.EQ, item.getOperator());
    }

    @Test
    void constrainOnValueEQRejectsUnsupportedTypes() throws Exception {
        Attribute<Object> attribute = new Attribute<>(3, "payload", null, AttributeType.DATA);

        assertThrows(InvalidParameterException.class,
                () -> QueryBuilder.constrainOnValueEQ(attribute, "x", Locale.ROOT));
    }

    @Test
    void constrainToSpecificStatusUsesStatusColumn() {
        LeafExpression<IntegerUnitSearchItem> expr =
                QueryBuilder.constrainToSpecificStatus(Unit.Status.ARCHIVED);

        IntegerUnitSearchItem item = expr.getItem();
        assertEquals(Column.UNIT_KERNEL_STATUS, item.getColumn());
        assertEquals(Operator.EQ, item.getOperator());
        assertEquals(Unit.Status.ARCHIVED.getStatus(), item.getValue());
    }

    @Test
    void constrainToCreatedBeforeAndAfterUseExpectedOperators() {
        Instant instant = Instant.parse("2024-08-26T10:15:30Z");

        TimeUnitSearchItem before = QueryBuilder.constrainToCreatedBefore(instant).getItem();
        assertEquals(Column.UNIT_KERNEL_CREATED, before.getColumn());
        assertEquals(Operator.LT, before.getOperator());
        assertEquals(instant, before.getValue());

        TimeUnitSearchItem after = QueryBuilder.constrainToCreatedAfter(instant).getItem();
        assertEquals(Column.UNIT_KERNEL_CREATED, after.getColumn());
        assertEquals(Operator.GEQ, after.getOperator());
        assertEquals(instant, after.getValue());
    }

    @Test
    void constrainToCreatedBeforeRejectsNullInstant() {
        assertThrows(NullPointerException.class, () -> QueryBuilder.constrainToCreatedBefore(null));
    }
}
