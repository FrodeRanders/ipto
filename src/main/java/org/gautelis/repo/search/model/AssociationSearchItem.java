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
import org.gautelis.repo.model.attributes.Type;
import org.gautelis.repo.model.attributes.Attribute;
import org.gautelis.repo.search.query.LeafExpression;

import java.text.ParseException;
import java.util.Locale;


public abstract class AssociationSearchItem<T> extends SearchItem<T> {

    private final int attrId;

    protected AssociationSearchItem(Type type, Operator operator, int attrId) {
        super(Variant.ATTRIBUTE, type, operator);
        this.attrId = attrId;
    }

    public int getAttrId() {
        return attrId;
    }

    /**
     * Generates constraint "attribute == value" for specified attribute.
     * <p>
     * This method will handle the various attribute types.
     */
    public static LeafExpression<AssociationSearchItem<?>> constrainOnAssociationEQ(
            Attribute<?> attribute, String value, Locale locale
    ) throws NumberFormatException, InvalidParameterException {

        int attrId = attribute.getAttrId();
        Type type = attribute.getType();

        // TODO
        //return switch (type) {
        //    default -> throw new InvalidParameterException("Association type " + type + " is not searchable: " + attribute);
        //};
        throw new InvalidParameterException("Association type " + type + " is currently not searchable: " + attribute);
    }
}
