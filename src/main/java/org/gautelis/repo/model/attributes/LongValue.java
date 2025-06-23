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
import org.gautelis.repo.db.Database;
import org.gautelis.repo.exceptions.AttributeTypeException;
import org.gautelis.repo.exceptions.AttributeValueException;
import org.gautelis.repo.exceptions.DatabaseReadException;
import org.gautelis.repo.exceptions.DatabaseWriteException;
import org.gautelis.repo.model.Context;
import org.gautelis.repo.model.Unit;

import java.sql.*;
import java.util.ArrayList;

final class LongValue extends Value<Long> {

    /* package accessible only */
    final static String COLUMN_NAME = "long_val";

    /**
     * Creates a <I>new</I> long value
     */
    LongValue() {
        super();
    }

    /**
     * Creates an <I>existing</I> long value
     */
    LongValue(ArrayNode node) throws JsonProcessingException {
        super(node);
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
             * parent_valueid, compound_idx,  -- compounds
             * depth,
             * string_idx, string_val,  -- string value at index string_idx
             * time_idx, time_val,      -- time value at index time_idx
             * int_idx, int_val,        -- int value at index int_idx
             * long_idx, long_val,      -- long value at index long_idx
             * double_idx, double_val,  -- double value at index double_idx
             * bool_idx, bool_val,      -- boolean value at index bool_idx
             * data_idx, data_val       -- data value at index data_idx
             * ----------------------------------------------------------- */
            Long value = rs.getLong(COLUMN_NAME); // long_val
            values.add(value);

        } catch (SQLException sqle) {
            throw new DatabaseReadException(sqle);
        }
    }

    /* package accessible only */
    void inflate(ArrayNode node) {
        node.forEach(value -> values.add(value.asLong()));
    }

    @Override
    public Type getType() {
        return Type.LONG;
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
    void injectJson(
            ArrayNode ignored,
            ObjectNode attributeNode,
            boolean complete,
            boolean flat
    ) throws AttributeTypeException, AttributeValueException {
        ArrayNode array = attributeNode.putArray(COLUMN_NAME);
        for (Long value : values) {
            array.add(value);
        }
    }
}
