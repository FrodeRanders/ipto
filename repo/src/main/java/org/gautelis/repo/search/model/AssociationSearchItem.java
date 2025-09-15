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
import org.gautelis.repo.search.query.LeafExpression;

public abstract class AssociationSearchItem<T> extends SearchItem<T> {

    protected AssociationSearchItem(AssociationType type, Operator operator) {
        super(Variant.ASSOCIATION, type, operator);
    }

    /**
     * Generates constraint "Unit --external association--> {assocString}".
     * <p>
     */
    public static LeafExpression<AssociationSearchItem<?>> constrainOnRightExternalAssociation(
            String assocString
    ) throws NumberFormatException, InvalidParameterException {
        throw new InvalidParameterException("External associations are currently not custom searchable");
    }

    /**
     * Generates constraint "Unit <--external association-- {assocString}".
     * <p>
     */
    public static LeafExpression<AssociationSearchItem<?>> constrainOnLeftExternalAssociation(
            String assocString
    ) throws NumberFormatException, InvalidParameterException {
        throw new InvalidParameterException("External associations are currently not custom searchable");
    }

    /**
     * Generates constraint "Unit{tenantId, unitId} --internal relation--> Unit",
     * e.g. corresponding to retrieving 'parents' of a specific 'child'
     * <p>
     */
    public static LeafExpression<AssociationSearchItem<?>> constrainOnRightInternalRelation(
            int tenantId, long unitId
    ) throws NumberFormatException, InvalidParameterException {
        throw new InvalidParameterException("Internal relations are currently not custom searchable");
    }

    /**
     * Generates constraint "Unit <--internal relation-- Unit{tenantId, unitId}",
     * e.g. corresponding to retrieving 'children' of a specific 'parent'.
     * <p>
     */
    public static LeafExpression<AssociationSearchItem<?>> constrainOnLeftInternalRelation(
            int tenantId, long unitId
    ) throws NumberFormatException, InvalidParameterException {
        throw new InvalidParameterException("Internal relations are currently not custom searchable");
    }
}
