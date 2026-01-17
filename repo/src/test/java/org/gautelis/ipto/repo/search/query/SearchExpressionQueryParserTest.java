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

import org.gautelis.ipto.repo.model.AttributeType;
import org.gautelis.ipto.repo.model.AssociationType;
import org.gautelis.ipto.repo.model.RelationType;
import org.gautelis.ipto.repo.model.Unit;
import org.gautelis.ipto.repo.exceptions.InvalidParameterException;
import org.gautelis.ipto.repo.search.model.AttributeSearchItem;
import org.gautelis.ipto.repo.search.model.IntegerUnitSearchItem;
import org.gautelis.ipto.repo.search.model.LeftAssociationSearchItem;
import org.gautelis.ipto.repo.search.model.LeftRelationSearchItem;
import org.gautelis.ipto.repo.search.model.Operator;
import org.gautelis.ipto.repo.search.model.RightAssociationSearchItem;
import org.gautelis.ipto.repo.search.model.RightRelationSearchItem;
import org.gautelis.ipto.repo.search.model.StringAttributeSearchItem;
import org.gautelis.ipto.repo.search.model.UnitSearchItem;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SearchExpressionQueryParserTest {

    @Test
    void parseAndExpressionWithAttributeAndUnitConstraints() {
        SearchExpression expr = SearchExpressionQueryParser.parse(
                "tenantid = 1 AND dcterms:title = \"Hello\"",
                name -> Optional.of(new SearchExpressionQueryParser.ResolvedAttribute(name, AttributeType.STRING))
        );

        AndExpression andExpr = assertInstanceOf(AndExpression.class, expr);
        LeafExpression<?> left = assertInstanceOf(LeafExpression.class, andExpr.getLeft());
        LeafExpression<?> right = assertInstanceOf(LeafExpression.class, andExpr.getRight());

        UnitSearchItem<?> unitItem = assertInstanceOf(UnitSearchItem.class, left.getItem());
        IntegerUnitSearchItem tenantItem = assertInstanceOf(IntegerUnitSearchItem.class, unitItem);
        assertEquals(1, tenantItem.getValue());
        assertEquals(Operator.EQ, tenantItem.getOperator());

        AttributeSearchItem<?> attrItem = assertInstanceOf(AttributeSearchItem.class, right.getItem());
        StringAttributeSearchItem titleItem = assertInstanceOf(StringAttributeSearchItem.class, attrItem);
        assertEquals("dcterms:title", titleItem.getAttrName());
        assertEquals("Hello", titleItem.getValue());
        assertEquals(Operator.EQ, titleItem.getOperator());
    }

    @Test
    void parseWildcardUsesLikeOperator() {
        SearchExpression expr = SearchExpressionQueryParser.parse(
                "unitname = \"Case*\"",
                name -> Optional.empty()
        );

        LeafExpression<?> leaf = assertInstanceOf(LeafExpression.class, expr);
        UnitSearchItem<?> unitItem = assertInstanceOf(UnitSearchItem.class, leaf.getItem());
        assertEquals(Operator.LIKE, unitItem.getOperator());
        assertEquals("Case%", unitItem.getValue());
    }

    @Test
    void parseLeftRelationConstraintWithUnitRef() {
        SearchExpression expr = SearchExpressionQueryParser.parse(
                "relation:left:parent-child = 1.2",
                name -> Optional.empty()
        );

        LeafExpression<?> leaf = assertInstanceOf(LeafExpression.class, expr);
        LeftRelationSearchItem item = assertInstanceOf(LeftRelationSearchItem.class, leaf.getItem());
        Unit.Id ref = item.getValue();
        assertEquals(RelationType.PARENT_CHILD_RELATION, item.getType());
        assertEquals(1, ref.tenantId());
        assertEquals(2L, ref.unitId());
    }

    @Test
    void parseRightRelationConstraintWithVersionedUnitRef() {
        SearchExpression expr = SearchExpressionQueryParser.parse(
                "relation:right:replacement = \"7.42:3\"",
                name -> Optional.empty()
        );

        LeafExpression<?> leaf = assertInstanceOf(LeafExpression.class, expr);
        RightRelationSearchItem item = assertInstanceOf(RightRelationSearchItem.class, leaf.getItem());
        Unit.Id ref = item.getValue();
        assertEquals(RelationType.REPLACEMENT_RELATION, item.getType());
        assertEquals(7, ref.tenantId());
        assertEquals(42L, ref.unitId());
    }

    @Test
    void parseLeftAssociationConstraint() {
        SearchExpression expr = SearchExpressionQueryParser.parse(
                "association:left:case = \"case-123\"",
                name -> Optional.empty()
        );

        LeafExpression<?> leaf = assertInstanceOf(LeafExpression.class, expr);
        LeftAssociationSearchItem item = assertInstanceOf(LeftAssociationSearchItem.class, leaf.getItem());
        assertEquals(AssociationType.CASE_ASSOCIATION, item.getType());
        assertEquals("case-123", item.getValue());
    }

    @Test
    void parseRightAssociationConstraint() {
        SearchExpression expr = SearchExpressionQueryParser.parse(
                "association:right:case = \"case-999\"",
                name -> Optional.empty()
        );

        LeafExpression<?> leaf = assertInstanceOf(LeafExpression.class, expr);
        RightAssociationSearchItem item = assertInstanceOf(RightAssociationSearchItem.class, leaf.getItem());
        assertEquals(AssociationType.CASE_ASSOCIATION, item.getType());
        assertEquals("case-999", item.getValue());
    }

    @Test
    void parseRelationConstraintRequiresEq() {
        assertThrows(InvalidParameterException.class, () -> SearchExpressionQueryParser.parse(
                "relation:left:parent-child != 1.2",
                name -> Optional.empty()
        ));
    }

    @Test
    void parseRelationConstraintRejectsUnknownType() {
        assertThrows(InvalidParameterException.class, () -> SearchExpressionQueryParser.parse(
                "relation:left:unknown = 1.2",
                name -> Optional.empty()
        ));
    }

    @Test
    void parseRelationConstraintRejectsInvalidUnitRef() {
        assertThrows(InvalidParameterException.class, () -> SearchExpressionQueryParser.parse(
                "relation:left:parent-child = \"bad.ref\"",
                name -> Optional.empty()
        ));
    }

    @Test
    void parseRelationConstraintRejectsConflictingSide() {
        assertThrows(InvalidParameterException.class, () -> SearchExpressionQueryParser.parse(
                "relation-left:right:parent-child = 1.2",
                name -> Optional.empty()
        ));
    }

    @Test
    void parseAssociationConstraintRequiresEq() {
        assertThrows(InvalidParameterException.class, () -> SearchExpressionQueryParser.parse(
                "association:left:case != \"case-1\"",
                name -> Optional.empty()
        ));
    }

    @Test
    void parseAssociationConstraintRejectsUnknownType() {
        assertThrows(InvalidParameterException.class, () -> SearchExpressionQueryParser.parse(
                "association:right:unknown = \"case-2\"",
                name -> Optional.empty()
        ));
    }

    @Test
    void parseRelationConstraintSupportsPrefixSideSyntax() {
        SearchExpression expr = SearchExpressionQueryParser.parse(
                "relation-right:parent-child = 2.10",
                name -> Optional.empty()
        );

        LeafExpression<?> leaf = assertInstanceOf(LeafExpression.class, expr);
        RightRelationSearchItem item = assertInstanceOf(RightRelationSearchItem.class, leaf.getItem());
        Unit.Id ref = item.getValue();
        assertEquals(RelationType.PARENT_CHILD_RELATION, item.getType());
        assertEquals(2, ref.tenantId());
        assertEquals(10L, ref.unitId());
    }
}
