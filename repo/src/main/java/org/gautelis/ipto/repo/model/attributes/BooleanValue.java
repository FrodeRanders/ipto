/*
 * Copyright (C) 2024-2026 Frode Randers
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
package org.gautelis.ipto.repo.model.attributes;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.ObjectNode;
import org.gautelis.ipto.repo.exceptions.AttributeTypeException;
import org.gautelis.ipto.repo.exceptions.AttributeValueException;
import org.gautelis.ipto.repo.exceptions.DatabaseReadException;
import org.gautelis.ipto.repo.model.AttributeType;

import java.sql.*;
import java.util.ArrayList;

final class BooleanValue extends Value<Boolean> {
    /**
     * Creates a <I>new</I> boolean value
     */
    BooleanValue() {
    }

    /**
     * Creates an <I>existing</I> boolean value
     */
    BooleanValue(ArrayNode node) throws JacksonException {
        super(node);
        inflate(node);
    }

    /**
     * Creates an <I>existing</I> boolean value
     */
    BooleanValue(ResultSet rs) throws DatabaseReadException {
        super(rs);
    }

    /**
     * Inflate an <I>existing</I> boolean value from a result set.
     * <p>
     * Called from the Value constructor.
     */
    /* package accessible only */
    void inflateSingleElement(ResultSet rs) throws DatabaseReadException {
        try {
            Boolean value = rs.getBoolean("bool_val");
            values.add(value);

        } catch (SQLException sqle) {
            throw new DatabaseReadException(sqle);
        }
    }

    private void inflate(ArrayNode node)  {
        node.forEach(value -> values.add(value.asBoolean()));
    }

    @Override
    public AttributeType getType() {
        return AttributeType.BOOLEAN;
    }

    @Override
    public Class<Boolean> getConcreteType() {
        return Boolean.class;
    }

    public void set(ArrayList<Boolean> values) throws AttributeTypeException {
        if (null == values || values.isEmpty()) {
            this.values.clear();
        } else {
            this.values.addAll(values);
        }
    }

    public boolean verify(Object value) {
        return value instanceof Boolean;
    }

    public void set(Boolean value) {
        values.add(value);
    }

    public Boolean getScalar() {
        return values.getFirst();
    }

    /* package accessible only */
    void toJson(
            ArrayNode _ignored,
            ObjectNode attributeNode,
            boolean _isChatty,
            boolean _forPersistence
    ) throws AttributeTypeException, AttributeValueException {
        ArrayNode array = attributeNode.putArray(VALUE_PROPERTY_NAME);
        for (Boolean value : values) {
            array.add(value);
        }
    }
}
