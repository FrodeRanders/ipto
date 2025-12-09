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

final class LongValue extends Value<Long> {
    /**
     * Creates a <I>new</I> long value
     */
    LongValue() {
        super();
    }

    /**
     * Creates an <I>existing</I> long value
     */
    LongValue(ArrayNode node) throws JacksonException {
        super(node);
        inflate(node);
    }

    /**
     * Creates an <I>existing</I> long value
     */
    LongValue(ResultSet rs) throws DatabaseReadException {
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
            Long value = rs.getLong("long_val");
            values.add(value);

        } catch (SQLException sqle) {
            throw new DatabaseReadException(sqle);
        }
    }

    private void inflate(ArrayNode node) {
        node.forEach(value -> values.add(value.asLong()));
    }

    @Override
    public AttributeType getType() {
        return AttributeType.LONG;
    }

    @Override
    public Class<Long> getConcreteType() {
        return Long.class;
    }

    public void set(ArrayList<Long> values) throws AttributeTypeException {
        if (null == values || values.isEmpty()) {
            this.values.clear();
        } else {
            this.values.addAll(values);
        }
    }

    public boolean verify(Object value) {
        return value instanceof Integer;
    }

    public void set(Long value) {
        values.add(value);
    }

    public Long getScalar() {
        return values.getFirst();
    }

    /* package accessible only */
    void toJson(
            ArrayNode _ignored,
            ObjectNode attributeNode,
            boolean _isChatty
    ) throws AttributeTypeException, AttributeValueException {
        ArrayNode array = attributeNode.putArray(VALUE_PROPERTY_NAME);
        for (Long value : values) {
            array.add(value);
        }
    }
}
