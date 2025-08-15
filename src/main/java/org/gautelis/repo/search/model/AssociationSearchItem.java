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
import org.gautelis.repo.model.associations.Association;
import org.gautelis.repo.search.query.LeafExpression;

import java.util.Locale;


public abstract class AssociationSearchItem<T> extends SearchItem<T> {

    protected AssociationSearchItem(AssociationType type, Operator operator) {
        super(Variant.ASSOCIATION, type, operator);
    }

    /**
     * Generates constraint "association == value" for specified association.
     * <p>
     * This method will handle the various attribute types.
     */
    public static LeafExpression<AssociationSearchItem<?>> constrainOnAssociationEQ(
            Association association, String value, Locale locale
    ) throws NumberFormatException, InvalidParameterException {

        AssociationType type = association.getType();

        //return switch (type) {
        //    default -> throw new InvalidParameterException("Association type " + type + " is not searchable: " + attribute);
        //};
        throw new InvalidParameterException("Association type " + type + " is currently not searchable: " + association);
    }
}
