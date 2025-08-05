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

final class DoubleValue extends Value<Double> {

    /* package accessible only */
    final static String COLUMN_NAME = "double_val";

    /**
     * Creates a <I>new</I> double value
     */
    DoubleValue() {
        super();
    }

    /**
     * Creates an <I>existing</I> double value
     */
    DoubleValue(ArrayNode node) throws JsonProcessingException {
        super(node);
        inflate(node);
    }

    /**
     * Creates an <I>existing</I> double value
     */
    DoubleValue(ResultSet rs) throws DatabaseReadException {
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
             * string_idx, string_val,  -- string value at index string_idx
             * time_idx, time_val,      -- time value at index time_idx
             * int_idx, int_val,        -- int value at index int_idx
             * long_idx, long_val,      -- long value at index long_idx
             * double_idx, double_val,  -- double value at index double_idx
             * bool_idx, bool_val,      -- boolean value at index bool_idx
             * data_idx, data_val       -- data value at index data_idx
             * ----------------------------------------------------------- */

            Double value = rs.getDouble(COLUMN_NAME); // double_val
            values.add(value);

        } catch (SQLException sqle) {
            throw new DatabaseReadException(sqle);
        }
    }

    /* package accessible only */
    void inflate(ArrayNode node) {
        node.forEach(value -> values.add(value.asDouble()));
    }

    @Override
    public Type getType() {
        return Type.DOUBLE;
    }

    @Override
    public Class<Double> getConcreteType() {
        return Double.class;
    }

    public void set(ArrayList<Double> values) throws AttributeTypeException {
        if (null == values || values.isEmpty()) {
            this.values.clear();
        } else {
            this.values.addAll(values);
        }
    }

    public boolean verify(Object value) {
        return value instanceof Double;
    }

    public void set(Double value) {
        values.add(value);
    }

    public Double getScalar() {
        return values.getFirst();
    }

    /* package accessible only */
    void toJson(
            ArrayNode ignored,
            ObjectNode attributeNode,
            boolean complete,
            boolean flat
    ) throws AttributeTypeException, AttributeValueException {
        ArrayNode array = attributeNode.putArray(COLUMN_NAME);
        for (Double value : values) {
            array.add(value);
        }
    }
}
