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
import org.gautelis.ipto.repo.utils.TimeHelper;

import java.sql.*;
import java.time.Instant;
import java.util.ArrayList;

final class TimeValue extends Value<Instant> {
    /**
     * Creates a <I>new</I> time value
     */
    TimeValue() {
    }

    /**
     * Creates an <I>existing</I> time value
     */
    TimeValue(ArrayNode node) throws JacksonException {
        super(node);
        inflate(node);
    }

    /**
     * Creates an <I>existing</I> date value
     */
    TimeValue(ResultSet rs) throws DatabaseReadException {
        super(rs);
    }

    /**
     * Inflate an <I>existing</I> time value from a result set.
     * <p>
     * Called from the Value constructor.
     */
    /* package accessible only */
    void inflateSingleElement(ResultSet rs) throws DatabaseReadException {
        try {
            Timestamp value = rs.getTimestamp("time_val");
            values.add(value.toInstant());

        } catch (SQLException sqle) {
            throw new DatabaseReadException(sqle);
        }
    }

    private void inflate(ArrayNode node) {
        try {
            node.forEach(value -> {
                String _time = value.asString();
                if (_time.toLowerCase().endsWith("z")) {
                    values.add(Instant.parse(_time));
                } else {
                    // Kludge!!!
                    values.add(Instant.parse(_time + "Z"));
                }
            });
        } catch (Throwable t) {
            log.info("Failed to parse value as timestamp: {}", node, t);
        }
    }

    @Override
    public AttributeType getType() {
        return AttributeType.TIME;
    }

    @Override
    public Class<Instant> getConcreteType() {
        return Instant.class;
    }

    public void set(ArrayList<Instant> values) throws AttributeTypeException {
        if (null == values || values.isEmpty()) {
            this.values.clear();
        } else {
            this.values.addAll(values);
        }
    }

    public boolean verify(Object value) {
        return value instanceof Instant;
    }

    public void set(Instant value) {
        values.add(value);
    }

    public Instant getScalar() {
         return values.getFirst();
    }

    /* package accessible only */
    void toJson(
            ArrayNode _ignored,
            ObjectNode attributeNode,
            boolean _isChatty
    ) throws AttributeTypeException, AttributeValueException {
        ArrayNode array = attributeNode.putArray(VALUE_PROPERTY_NAME);
        for (Instant value : values) {
            array.add(value.toString());
        }
    }
}
