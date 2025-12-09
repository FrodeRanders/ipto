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
package org.gautelis.ipto.repo.search.model;

import org.gautelis.ipto.repo.exceptions.InvalidParameterException;
import org.gautelis.ipto.repo.model.AssociationType;
import org.gautelis.ipto.repo.model.Unit;
import org.gautelis.ipto.repo.search.query.LeafExpression;

public class LeftRelationSearchItem extends RelationSearchItem<Unit.Id> {
    private final Unit.Id rightId;

    protected LeftRelationSearchItem(AssociationType type, Operator operator, Unit.Id rightId) {
        super(type, operator);
        this.rightId = rightId;
    }

    /**
     * Generates constraint "Unit <--relation-- Unit{tenantId, unitId}",
     * e.g. corresponding to retrieving 'children' of a specific 'parent'.
     * <p>
     */
    public static LeafExpression<LeftRelationSearchItem> constrainOnLeftRelationEQ(
            AssociationType type,
            Unit.Id rightId
    ) throws NumberFormatException, InvalidParameterException {
        return new LeafExpression<>(new LeftRelationSearchItem(type, Operator.EQ, rightId));
    }

    @Override
    public Unit.Id getValue() {
        return rightId;
    }
}
