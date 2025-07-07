/*
 * Copyright (C) 2024 Frode Randers
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
import org.gautelis.repo.exceptions.*;

import java.sql.*;
import java.util.ArrayList;

final class RecordValue extends Value<Attribute<?>> {

    /* package accessible only */
    final static String COLUMN_NAME = "record_val";

    /**
     * Creates a <I>new</I> long value
     */
    RecordValue() {
        super();
    }

    /**
     * Creates an <I>existing</I> record value
     */
    RecordValue(ArrayNode node) throws JsonProcessingException {
        super(node);
    }

    /**
     * Creates an <I>existing</I> record value
     */
    RecordValue(ResultSet rs) throws DatabaseReadException {
        super(rs);
    }

    /**
     * Inflate an <I>existing</I> string value from a result set.
     * <p>
     * Called from the Value constructor.
     */
    /* package accessible only */
    void inflateSingleElement(ResultSet rs) throws DatabaseReadException {
        /* Do nothing! Nested attributes are injected in Unit */
    }

    /* package accessible only */
    void inflate(ArrayNode node) {
        // TODO, if we go through with JSON loading
    }

    @Override
    public Type getType() {
        return Type.RECORD;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Class<Attribute<?>> getConcreteType() {
        return (Class<Attribute<?>>) (Class<?>) Attribute.class;
    }

    public void set(ArrayList<Attribute<?>> values) throws AttributeTypeException {
        if (null == values || values.isEmpty()) {
            this.values.clear();
        } else {
            this.values.addAll(values);
        }
    }

    public boolean verify(Object value) {
        return value instanceof Attribute<?>;
    }

    public void set(Attribute<?> value) {
        values.add(value);
    }

    public Attribute<?> getScalar() {
        return values.getFirst();
    }

    /* package accessible only */
    void injectJson(
            ArrayNode attributes,
            ObjectNode attributeNode,
            boolean complete,
            boolean flat
    ) throws AttributeTypeException, AttributeValueException {
        ArrayNode valueNode = null;
        if (flat) {
            valueNode = attributeNode.putArray(COLUMN_NAME);
        }

        for (Attribute<?> nestedAttribute : values) {
            // 'attributes' either in 'unit' (if flat) or in
            // record parent attribute if not.
            ObjectNode nestedAttributeNode = attributes.addObject();
            nestedAttribute.injectJson(attributes, nestedAttributeNode, complete, flat);

            // If flat (and thus having the nested attribute in the unit), then
            // we need to describe additional relations.
            if (flat && null != valueNode) {
                // Add reference to that attribute within this record attribute
                ObjectNode nestedAttributeRefNode = valueNode.addObject();

                int attrId = nestedAttribute.getAttrId();
                nestedAttributeRefNode.put("ref_attrid", attrId);

                long valueId = nestedAttribute.getValueId();
                if (valueId > 0) {
                    nestedAttributeRefNode.put("ref_valueid", valueId);
                } else {
                    nestedAttributeRefNode.putNull("ref_valueid");
                }
            }
        }
    }
}
