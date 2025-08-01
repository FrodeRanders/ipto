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
import org.gautelis.repo.model.KnownAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;

/**
 *
 */
public class Attribute<T> {
    private static final Logger log = LoggerFactory.getLogger(Attribute.class);

    private int id;
    private long valueId = -1L; // initially invalid
    private String name;
    private Type type;
    private Value<T> value = null;

    /**
     * Creates a <B>new</B> attribute.
     * <p>
     * Remember that attribute ids are defined among the known attributes,
     * so what we do is to associate an attribute id with a certain unit.
     * Individual values of the attribute may be provided
     * as soon as we have an attribute to contain them.
     */
    /* Should be package accessible only */
    public Attribute(
            int attrId, String name, Type type
    ) throws AttributeTypeException {
        this.id = attrId;
        this.name = name.trim();
        this.type = type;

        value = Value.createValue(type);
    }

    public Attribute(
            KnownAttributes.AttributeInfo attributeInfo
    ) throws AttributeTypeException {
        this(attributeInfo.id, attributeInfo.name, Type.of(attributeInfo.type));
    }

    /**
     * Inflates an attribute from a result set.
     */
    /* Should be package accessible only */
    public Attribute(
            ResultSet rs
    ) throws DatabaseReadException, AttributeTypeException {
        readEntry(rs);
    }

    /**
     * Inflates an attribute from JSON.
     */
    /* Should be package accessible only */
    public Attribute(
            JsonNode node
    ) throws JsonProcessingException {
        readEntry(node);
    }

    /**
     * Copy constructor.
     * NOTE!
     *   Does not copy field 'valueId' which will be assigned a new value.
     */
    @SuppressWarnings("CopyConstructorMissesField")
    public Attribute(Attribute<T> other) {
        this(other.getAttrId(), other.getName(), other.getType());
        value.copy(other.value);
    }

    /**
     * Gets attribute type
     */
    public Type getType() {
        return value.getType();
    }

    /**
     * Gets attribute concrete Java type
     * @return
     */
    public Class<T> getConcreteType() {
        return value.getConcreteType();
    }

    /**
     * Gets attribute value (vector).
     */
    public ArrayList<T> getValue() {
        return value.get();
    }

    /**
     * Gets attribute size.
     */
    public int getSize() {
        return value.getSize();
    }

    /**
     * Gets attribute name.
     *
     * @return String name of attribute
     */
    public String getName() {
        return name;
    }

    /**
     * Gets attribute id.
     *
     * @return int id of attribute
     */
    public int getAttrId() {
        return id;
    }

    /* package accessible only */
    long getValueId() {
        return valueId;
    }

    public void injectJson(ArrayNode attributes, ObjectNode attributeNode, boolean complete, boolean flat) {
        if (complete) {
            String _type = type.name().toLowerCase();
            if (value.isScalar()) {
                _type += "-scalar";
            } else {
                _type += "-vector";
            }
            attributeNode.put("@type", _type);
        }

        attributeNode.put("attrid",   id);
        if (/* has been saved and thus is valid? */ valueId > 0) {
            attributeNode.put("valueid", valueId);
        } else {
            attributeNode.putNull("valueid");
        }

        attributeNode.put("attrtype", type.getType());
        attributeNode.put("name", name);

        if (!flat && Type.RECORD == type) {
            // "hide" unit attributes with local array in record attribute
            attributes = attributeNode.putArray("attributes");
        }

        value.injectJson(attributes, attributeNode, complete, flat);
    }

    private void readEntry(JsonNode node) throws JsonProcessingException {
        // TODO -- needs an overhaul to accommodate record attributes
        // Get attribute information
        id = node.path("attrid").asInt();
        valueId = node.path("valueid").asLong();
        name = node.path("attrname").asText();
        type = Type.of(node.path("attrtype").asInt());

        // Continue with value vector
        value = Value.inflateValue(type, node);
    }

    private void readEntry(ResultSet rs) throws DatabaseReadException, AttributeTypeException {
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

            // Get attribute information
            id = rs.getInt("attrid");
            valueId = rs.getLong("valueid");
            name = rs.getString("attrname");
            type = Type.of(rs.getInt("attrtype"));

            // Continue with value vector
            value = Value.inflateValue(type, rs);

        } catch (SQLException sqle) {
            throw new DatabaseReadException(sqle);
        }
    }

    /**
     * Checks if attributes were modified
     *
     * @return True if modified, else false
     */
    public boolean isModified() {
        return value.isModified();
    }

    public boolean isNew() {
        return value.isNew() || valueId <= 0;
    }

    /**
     * Overridden method from {@link Object }
     */
    @Override
    public String toString() {
        return "Attribute{" + id + "(" + name + ")" +
                (value.isNew() ? "*" : "") +
                (value.isModified() ? "~" : "") +
                "=" + value.toString() + "}";
    }
}



