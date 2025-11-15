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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.gautelis.repo.exceptions.*;
import org.gautelis.repo.model.AttributeType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;


public abstract class Value<T> {
    static final Logger log = LoggerFactory.getLogger(Value.class);

    protected final static String VALUE_PROPERTY_NAME = "value";

    protected final ArrayList<T> values = new ArrayList<>();
    private int initialHashCode;
    private boolean isNew;

    /**
     * Creates a <I>new</I> attribute value.
     * <p>
     * Called from derived objects.
     */
    protected Value() {
        isNew = true;

        // Mark current status, so we can detect changes later...
        initialHashCode = values.hashCode();
    }

    /**
     * Creates an <I>existing</I> attribute value from a resultset.
     * <p>
     * Called from derived objects.
     */
    protected Value(ArrayNode node) {
        isNew = false;

        // Mark current status, so we can detect changes later...
        initialHashCode = values.hashCode();
    }

    /**
     * Creates an <I>existing</I> attribute value from a resultset.
     * <p>
     * Called from derived objects.
     */
    protected Value(ResultSet rs) throws DatabaseReadException {
        isNew = false;

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

            // We will only pick the value vector elements associated with
            // the current (attribute) value.
            final long valueId = rs.getLong("valueid");

            while (/* we have an index (into the value vector) */ !rs.wasNull()) {

                // Get value element from vector
                inflateSingleElement(rs);

                // Boundary at next valueid
                if (rs.next()) {
                    // Verify that we are still referring to the same valueId,
                    // and that we are referring to the next value index.
                    long nextValueId = rs.getLong("valueid");

                    if (/* we have a next index */ !rs.wasNull() &&
                        /* same valueid */ valueId == nextValueId) {
                        continue;
                    }
                }
                break;
            }

            // Mark current status, so we can detect changes later...
            initialHashCode = values.hashCode();

        } catch (SQLException sqle) {
            throw new DatabaseReadException(sqle);
        }
    }

    /**
     * Creates a <I>new</I> attribute value of type 'type'
     */
    /* package accessible only */
    static <T> Value<T> createValue(AttributeType type) throws AttributeTypeException {
        Value<?> value = switch (type) {
            case STRING -> new StringValue();
            case TIME -> new TimeValue();
            case INTEGER -> new IntegerValue();
            case LONG -> new LongValue();
            case DOUBLE -> new DoubleValue();
            case BOOLEAN -> new BooleanValue();
            case DATA -> new DataValue();
            case RECORD -> new RecordValue();
        };

        //noinspection unchecked
        return (Value<T>) value;
    }

    protected static <T> Value<T> inflateValue(
            AttributeType type,
            JsonNode node
    ) throws AttributeTypeException, JsonProcessingException {
        Value<?> value = switch (type) {
            case STRING -> new StringValue((ArrayNode) node.get(StringValue.VALUE_PROPERTY_NAME));
            case TIME -> new TimeValue((ArrayNode) node.get(TimeValue.VALUE_PROPERTY_NAME));
            case INTEGER -> new IntegerValue((ArrayNode) node.get(IntegerValue.VALUE_PROPERTY_NAME));
            case LONG -> new LongValue((ArrayNode) node.get(LongValue.VALUE_PROPERTY_NAME));
            case DOUBLE -> new DoubleValue((ArrayNode) node.get(DoubleValue.VALUE_PROPERTY_NAME));
            case BOOLEAN -> new BooleanValue((ArrayNode) node.get(BooleanValue.VALUE_PROPERTY_NAME));
            case DATA -> new DataValue((ArrayNode) node.get(DataValue.VALUE_PROPERTY_NAME));
            case RECORD -> new RecordValue((ArrayNode) node.get(RecordValue.VALUE_PROPERTY_NAME));
        };

        //noinspection unchecked
        return (Value<T>) value;
    }

    /**
     * Inflates an <I>existing</I> attribute value from a result set.
     * <p>
     * This is a <I>helper method</I> called by Attribute when
     * creating attribute values, since Attribute knows
     * nothing about the various value types (StringValue, ...)
     */
    protected static <T> Value<T> inflateValue(
            AttributeType type,
            ResultSet rs
    ) throws AttributeTypeException, DatabaseReadException {
        Value<?> value = switch (type) {
            case STRING -> new StringValue(rs);
            case TIME -> new TimeValue(rs);
            case INTEGER -> new IntegerValue(rs);
            case LONG -> new LongValue(rs);
            case DOUBLE -> new DoubleValue(rs);
            case BOOLEAN -> new BooleanValue(rs);
            case DATA -> new DataValue(rs);
            case RECORD -> new RecordValue(rs);
        };

        //noinspection unchecked
        return (Value<T>) value;
    }

    /**
     * Actually inflate values from the resultset.
     * <p>
     * This method must be overridden by each value type (StringValue, ...)
     */
    /* package accessible only */
    abstract void inflate(ArrayNode node);

    /**
     * Actually inflate values from the resultset.
     * <p>
     * This method must be overridden by each value type (StringValue, ...)
     */
    /* package accessible only */
    abstract void inflateSingleElement(ResultSet rs) throws DatabaseReadException;

    /**
     * Gets type of attribute.
     */
    public abstract AttributeType getType();

    /**
     * Gets concrete Java type of attribute.
     */
    public abstract Class<T> getConcreteType();

    /**
     * Returns dimension information.
     * An attribute is scalar if there only exists
     * one value for the attribute. We view scalar
     * attributes as a special case of the more general
     * situation with multiple values for the attribute.
     * <p>
     * If no values are associated with an attribute
     * we assume scalar.
     */
    public boolean isScalar() {
        return values.size() <= 1;
    }

    public boolean isVector() {
        return values.size() > 1;
    }

    /**
     * Returns number of values associated with attribute
     */
    public int getSize() {
        return values.size();
    }

    /**
     * Mark attribute value as stored (to database)
     */
    protected void setStored() {
        initialHashCode = values.hashCode();
        isNew = false;
    }

    /**
     * Get all values
     */
    public ArrayList<T> get() {
        return values;
    }

    public boolean isNew() {
        return isNew;
    }

    /**
     * Have any values been modified?
     */
    public boolean isModified() {
        return initialHashCode != values.hashCode();
    }

    /**
     * Sets values. The types of the values must
     * be compatible with the attribute type, unless
     * an AttributeTypeException exception is thrown.
     */
    public abstract void set(ArrayList<T> values)
            throws AttributeTypeException;

    /**
     * Treats value as a scalar and returns first element
     * in vector. If no element was found, null is returned.
     */
    public abstract T getScalar();

    /* Package accessible only */
    void copy(Value<T> other) {
        values.addAll(other.values);
    }

    /*--------------------------------------------------------
     * Override the proper setter/getter and ignore the rest
     *-------------------------------------------------------*/

    /**
     * Verifies that value is of right type
     */
    public abstract boolean verify(Object value);

    /**
     * Default setter for values
     */
    public abstract void set(T value);

    /* package accessible only */
    abstract void toInternalJson(
            ArrayNode attributes, ObjectNode attributeNode
    ) throws AttributeTypeException, AttributeValueException;

    /* package accessible only */
    abstract void toExternalJson(
            ArrayNode attributes, ObjectNode attributeNode
    ) throws AttributeTypeException, AttributeValueException;

    /**
     * Return String representation of object.
     *
     * @return Returns created String
     */
    @Override
    public String toString() {
        return values.toString();
    }
}
