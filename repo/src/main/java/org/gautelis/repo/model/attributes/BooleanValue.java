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
package org.gautelis.repo.model.attributes;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.gautelis.repo.exceptions.AttributeTypeException;
import org.gautelis.repo.exceptions.AttributeValueException;
import org.gautelis.repo.exceptions.DatabaseReadException;
import org.gautelis.repo.model.AttributeType;

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
    BooleanValue(ArrayNode node) throws JsonProcessingException {
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
     * Inflate an <I>existing</I> string value from a result set.
     * <p>
     * Called from the Value constructor.
     */
    /* package accessible only */
    void inflateSingleElement(ResultSet rs) throws DatabaseReadException {
        try {
            /* -------------------- Result set layout -------------------- *
             * valueid,                       -- value vector id
             * attrid, attrtype, attrname,    -- attribute
             * parent_valueid, record_idx,    -- records
             * depth,
             * idx,
             * string_val,    -- string value at index idx
             * time_val,      -- time value at index idx
             * int_val,       -- int value at index idx
             * long_val,      -- long value at index idx
             * double_val,    -- double value at index idx
             * bool_val,      -- boolean value at index idx
             * data_val       -- data value at index idx
             * ----------------------------------------------------------- */
            Boolean value = rs.getBoolean("bool_val");
            values.add(value);

        } catch (SQLException sqle) {
            throw new DatabaseReadException(sqle);
        }
    }

    /* package accessible only */
    void inflate(ArrayNode node)  {
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
    void toInternalJson(
            ArrayNode ignored,
            ObjectNode attributeNode
    ) throws AttributeTypeException, AttributeValueException {
        ArrayNode array = attributeNode.putArray(COLUMN_NAME);
        for (Boolean value : values) {
            array.add(value);
        }
    }

    /* package accessible only */
    void toExternalJson(
            ArrayNode ignored,
            ObjectNode attributeNode
    ) throws AttributeTypeException, AttributeValueException {
        ArrayNode array = attributeNode.putArray(COLUMN_NAME);
        for (Boolean value : values) {
            array.add(value);
        }
    }
}
