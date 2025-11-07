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

    public record Reference(int id, String name) {}

    // Attribute related
    private int id;
    private String name;
    private String alias;
    private AttributeType type;

    // Instance (value) related
    private int unitVersionFrom = 1;
    private int unitVersionTo = 1;
    private long valueId = -1L; // initially invalid
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
            int id, String name, String alias, AttributeType type
    ) throws AttributeTypeException {
        this.id = id;
        this.name = (null != name) ? name.trim() : null;
        this.alias = (null != alias) ? alias.trim() : null;
        this.type = type;

        value = Value.createValue(type);
    }

    public Attribute(
            KnownAttributes.AttributeInfo attributeInfo
    ) throws AttributeTypeException {
        this(attributeInfo.id, attributeInfo.name, attributeInfo.alias, AttributeType.of(attributeInfo.type));
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
        this(other.getId(), other.getName(), other.getAlias(), other.getType());
        value.copy(other.value);
    }

    /**
     * Gets attribute type
     */
    public AttributeType getType() {
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
    public Value<T> getValue() {
        return value;
    }

    /**
     * Gets attribute value (vector).
     */
    public ArrayList<T> getValueVector() {
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
     * Gets attribute alias (if any).
     *
     * @return String alias of attribute
     */
    public String getAlias() {
        return alias;
    }

    /**
     * Gets attribute id.
     *
     * @return int id of attribute
     */
    public int getId() {
        return id;
    }

    public long getValueId() {
        return valueId;
    }

    public void toInternalJson(ArrayNode attributes, ObjectNode attributeNode) {
        if (false) {
            String _type = type.name().toLowerCase();
            if (value.isScalar()) {
                _type += "-scalar";
            } else {
                _type += "-vector";
            }
            attributeNode.put("@type", _type);
        }

        boolean _isModified = isModified();

        attributeNode.put("ismodified", _isModified);
        attributeNode.put("attrname", name);
        attributeNode.put("attrid", id);
        attributeNode.put("attrtype", type.getType());
        attributeNode.put("untverfrom", unitVersionFrom);
        attributeNode.put("untverto", unitVersionTo);

        // if (/* has been saved and thus is valid? */ valueId > 0) {
        //     attributeNode.put("valueid", valueId);
        // } else {
        //     attributeNode.putNull("valueid");
        // }

        value.toInternalJson(attributes, attributeNode);
    }

    public void toExternalJson(ArrayNode attributes, ObjectNode attributeNode) {
        String _type = type.name().toLowerCase();
        if (value.isScalar()) {
            _type += "-scalar";
        } else {
            _type += "-vector";
        }
        attributeNode.put("@type", _type);

        attributeNode.put("attrname", name);
        attributeNode.put("attrid", id);
        attributeNode.put("attrtype", type.name());

        if (AttributeType.RECORD == type) {
            // "hide" unit attributes with local array in record attribute
            attributes = attributeNode.putArray("attributes");
        }

        value.toExternalJson(attributes, attributeNode);
    }

    private void readEntry(JsonNode node) throws JsonProcessingException {
        // Get attribute information
        id = node.path("attrid").asInt();
        unitVersionFrom = node.path("unitverfrom").asInt();
        unitVersionTo = node.path("unitverto").asInt();
        valueId = node.path("valueid").asLong();
        name = node.path("attrname").asText();
        type = AttributeType.of(node.path("attrtype").asInt());

        // Continue with value vector
        value = Value.inflateValue(type, node);
    }

    private void readEntry(ResultSet rs) throws DatabaseReadException, AttributeTypeException {
        try {
            // Get attribute information
            id = rs.getInt("attrid");
            valueId = rs.getLong("valueid");
            name = rs.getString("attrname");
            type = AttributeType.of(rs.getInt("attrtype"));

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

    public void setStored() {
        value.setStored();
    }

    /**
     * Overridden method from {@link Object }
     */
    @Override
    public String toString() {
        return "Attribute{" + id + "(" + name + ")[" +
                unitVersionFrom + "-" + unitVersionTo + "]:" +
                type.name() +
                (value.isNew() ? "*" : "") +
                (value.isModified() ? "~" : "") +
                "=" + value.toString() + "}";
    }
}



