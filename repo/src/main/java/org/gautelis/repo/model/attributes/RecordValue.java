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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.gautelis.repo.exceptions.*;
import org.gautelis.repo.model.AttributeType;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

public final class RecordValue extends Value<Attribute<?>> {
    public record AttributeReference(int refAttrId, long refValueId) {}
    private final Collection<AttributeReference> claimedReferences = new ArrayList<>();

    /**
     * Creates a <I>new</I> record value
     */
    RecordValue() {
        super();
    }

    /**
     * Creates an <I>existing</I> record value
     */
    RecordValue(ArrayNode node) throws JsonProcessingException {
        super(node);
        inflate(node);
    }

    /**
     * Creates an <I>existing</I> record value
     */
    RecordValue(ResultSet rs) throws DatabaseReadException {
        super(rs);
    }

    public Collection<AttributeReference> getClaimedReferences() {
        return claimedReferences;
    }

    /**
     * Have any values been modified?
     * TODO! NOT SURE IF THIS IS NEEDED.
    public boolean isModified() {
        // The generic 'isModified()' in superclass 'Value' only checks structure.
        // In this case it means that it does not check if individual attributes
        // in record 'values' has been modified.
        boolean _isModified = super.isModified();
        if (_isModified) {
            return true;
        }

        // Specialisation for record values
        for (Attribute<?> value : values) {
            if (value.isModified()) {
                return true;
            }
        }
        return false;
    }
    */

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
        /*
         * 'node' structure:
         * [
         *    {"ref_attrid":1001,"ref_valueid":49132},
         *    {"ref_attrid":1002,"ref_valueid":49133},
         *    {"ref_attrid":1003,"ref_valueid":49134}
         * ]
         *
         * If we go through with JSON loading, these values
         * are actually referring to attributes that will
         * be instantiated soon -- so just to be clear,
         * they don't exist yet.
         *
         * Therefore, there is nothing to be done and
         * the values have to be "injected" one-by-one
         * as they are later instantiated. So, we 'claim'
         * these attributes for this record.
         */

        for (JsonNode element : node) {
            int refAttrId = element.get("ref_attrid").asInt();
            long refValueId = element.get("ref_valueid").asLong();
            claimedReferences.add(new AttributeReference(refAttrId, refValueId));
        }
    }

    @Override
    public AttributeType getType() {
        return AttributeType.RECORD;
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
    void toInternalJson(
            ArrayNode attributes,
            ObjectNode attributeNode
    ) throws AttributeTypeException, AttributeValueException {
        ArrayNode valueNode = null;
        valueNode = attributeNode.putArray(VALUE_PROPERTY_NAME);

        for (Attribute<?> nestedAttribute : values) {
            // 'attributes' either in 'unit' (if flat) or in
            // record parent attribute if not.
            ObjectNode nestedAttributeNode = attributes.addObject();
            nestedAttribute.toInternalJson(attributes, nestedAttributeNode);

            // Attributes are nested at unit level, so
            // we need to describe additional relations.
            if (null != valueNode) {
                // Add reference to that attribute within this record attribute
                ObjectNode nestedAttributeRefNode = valueNode.addObject();

                int attrId = nestedAttribute.getId();
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

    void toExternalJson(
            ArrayNode attributes,
            ObjectNode attributeNode
    ) throws AttributeTypeException, AttributeValueException {
        for (Attribute<?> nestedAttribute : values) {
            // 'attributes' are represented in record parent attribute.
            ObjectNode nestedAttributeNode = attributes.addObject();
            nestedAttribute.toExternalJson(attributes, nestedAttributeNode);
        }
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        if (!claimedReferences.isEmpty()) {
            result.append("[");
            result.append(claimedReferences.stream()
                    .map(ref -> "#" + ref.refAttrId + ":" + ref.refValueId)
                    .collect(Collectors.joining(", ")));
            result.append("]+");
        }
        result.append(values);
        return result.toString();
    }
}
