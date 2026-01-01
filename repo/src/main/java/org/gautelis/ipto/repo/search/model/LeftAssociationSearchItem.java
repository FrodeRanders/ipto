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

public class LeftAssociationSearchItem extends AssociationSearchItem<Unit.Id> {
    private final Unit.Id id;

    protected LeftAssociationSearchItem(AssociationType type, Operator operator, Unit.Id id) {
        super(type, operator);
        this.id = id;
    }

    /**
     * Generates constraint "Unit <--association-- {assocString}".
     * <p>
     */
    public static LeafExpression<LeftAssociationSearchItem> constrainOnLeftAssociation(
            AssociationType type,
            Unit.Id id
    ) throws NumberFormatException, InvalidParameterException {
        return new LeafExpression<>(new LeftAssociationSearchItem(type, Operator.EQ, id));
    }

    @Override
    public Unit.Id getValue() {
        return id;
    }

    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder("LeftAssociationSearchItem{");
        buf.append(super.toString());
        buf.append(", id='").append(id).append("'");
        buf.append("}");
        return buf.toString();
    }
}
