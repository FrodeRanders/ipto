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
import java.util.Base64;

final class DataValue extends Value<Object> {
    /**
     * Creates a <I>new</I> data value
     */
    DataValue() {
    }

    /**
     * Creates an <I>existing</I> data value
     */
    DataValue(ArrayNode node) throws JacksonException {
        super(node);
        inflate(node);
    }

    /**
     * Creates an <I>existing</I> data value
     */
    DataValue(ResultSet rs) throws DatabaseReadException {
        super(rs);
    }

    /**
     * Inflate an <I>existing</I> data value from a result set.
     * <p>
     * Called from the Value constructor.
     */
    /* package accessible only */
    void inflateSingleElement(ResultSet rs) throws DatabaseReadException {
        // This is not a very good implementation, to say the least,
        // and experimental at best.
        //
        // We will in fact cache the blob in memory, which is Not A Good Thing To Do.
        // Consider implementing blob data retrieval as an InputStream
        // data.getBinaryStream() on access - which will of course need a
        // connection.
        //
        // Also, the current SQL type used with Oracle does not match reading
        // with ResultSet.getBlob(). This handling should be synchronized among
        // the supported databases.
        //
        try {
            byte[] value;
            try {
                // SQL Server: Works with the VARBINARY type
                Blob data = rs.getBlob("data_val");
                long length = data.length();
                value = data.getBytes(1, (int) length);

            } catch (Throwable ignore) {
                // Oracle: Works with the RAW type
                value = (byte[]) rs.getObject(VALUE_PROPERTY_NAME);
            }
            values.add(value);

        } catch (SQLException sqle) {
            throw new DatabaseReadException(sqle);
        }
    }

    private void inflate(ArrayNode node) {
        node.forEach(value -> values.add( Base64.getDecoder().decode(value.asString())));
    }

    @Override
    public AttributeType getType() {
        return AttributeType.DATA;
    }

    @Override
    public Class<Object> getConcreteType() {
        return Object.class;
    }

    public void set(ArrayList<Object> values) throws AttributeTypeException {
        if (null == values || values.isEmpty()) {
            this.values.clear();
        } else {
            // Accept byte[] only!
            for (Object o : values) {
                if (null == o)
                    continue;

                if (!(o instanceof byte[])) {
                    throw new AttributeTypeException("Incompatible values does not match DATA_VALUE");
                }
            }

            // Assign
            this.values.addAll(values);
        }
    }

    public boolean verify(Object value) {
        return value instanceof byte[];
    }

    public void set(Object value) {
        values.add(value);
    }

    public Object getScalar() {
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
        for (Object _value : values) {
            byte[] value = (byte[]) _value; // Assumption
            String b64 = Base64.getEncoder().encodeToString(value);
            array.add(b64);
        }
    }
}
