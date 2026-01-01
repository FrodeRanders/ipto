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
import org.gautelis.ipto.repo.search.query.LeafExpression;

public class RightAssociationSearchItem extends AssociationSearchItem<String> {

    private final String assocString;

    protected RightAssociationSearchItem(AssociationType type, Operator operator, String assocString) {
        super(type, operator);
        this.assocString = assocString;
    }

    /**
     * Generates constraint "Unit --association--> {assocString}".
     * <p>
     */
    public static LeafExpression<RightAssociationSearchItem> constrainOnRightAssociationEQ(
            AssociationType type,
            String assocString
    ) throws NumberFormatException, InvalidParameterException {
        return new LeafExpression<>(new RightAssociationSearchItem(type, Operator.EQ, assocString));
    }

    @Override
    public String getValue() {
        return assocString;
    }

    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder("RightAssociationSearchItem{");
        buf.append(super.toString());
        buf.append(", assoc='").append(assocString).append("'");
        buf.append("}");
        return buf.toString();
    }
}
