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
package org.gautelis.repo.search.model;

import org.gautelis.repo.exceptions.InvalidParameterException;
import org.gautelis.repo.model.AssociationType;
import org.gautelis.repo.model.Unit;
import org.gautelis.repo.search.query.LeafExpression;

public class RightRelationSearchItem extends RelationSearchItem<Unit.Id> {
    private final Unit.Id leftId;

    protected RightRelationSearchItem(AssociationType type, Operator operator, Unit.Id leftId) {
        super(type, operator);
        this.leftId = leftId;
    }

    /**
     * Generates constraint "Unit{tenantId, unitId} --relation--> Unit",
     * e.g. corresponding to retrieving 'parents' of a specific 'child'
     * <p>
     */
    public static LeafExpression<RightRelationSearchItem> constrainOnRightRelationEQ(
            AssociationType type,
            Unit.Id leftId
    ) throws NumberFormatException, InvalidParameterException {
        return new LeafExpression<>(new RightRelationSearchItem(type, Operator.EQ, leftId));
    }

    @Override
    public Unit.Id getValue() {
        return leftId;
    }
}
