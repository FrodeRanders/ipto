/*
 * Copyright (C) 2025-2026 Frode Randers
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
package org.gautelis.ipto.graphql.runtime.service;

import org.gautelis.ipto.graphql.runtime.box.AttributeBox;
import org.gautelis.ipto.graphql.runtime.box.Box;
import org.gautelis.ipto.graphql.runtime.box.RecordBox;
import org.gautelis.ipto.repo.model.AttributeType;
import org.gautelis.ipto.repo.model.attributes.Attribute;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

final class RuntimeAttributeResolver {
    private final Logger log;

    RuntimeAttributeResolver(Logger log) {
        this.log = log;
    }

    Object getValueArray(
            List<String> fieldNames,
            RecordBox box,
            boolean isMandatory
    ) {
        log.trace("↪ RuntimeService::getValueArray({}, {}, {})", fieldNames, box, isMandatory);

        ResolvedAttribute resolved = resolveFromRecord(fieldNames, box, isMandatory);
        if (resolved == null) {
            return null;
        }
        return asArrayOrBoxes(resolved.attribute(), box, resolved.fieldName(), isMandatory);
    }

    Object getAttributeArray(
            List<String> fieldNames,
            AttributeBox box,
            boolean isMandatory
    ) {
        log.trace("↪ RuntimeService::getAttributeArray({}, {}, {})", fieldNames, box, isMandatory);

        ResolvedAttribute resolved = resolveFromAttributeBox(fieldNames, box, isMandatory);
        if (resolved == null) {
            return null;
        }
        return asArrayOrBoxes(resolved.attribute(), box, resolved.fieldName(), isMandatory);
    }

    Object getValueScalar(
            List<String> fieldNames,
            RecordBox box,
            boolean isMandatory
    ) {
        log.trace("↪ RuntimeService::getValueScalar({}, {}, {})", fieldNames, box, isMandatory);

        ResolvedAttribute resolved = resolveFromRecord(fieldNames, box, isMandatory);
        if (resolved == null) {
            return null;
        }
        return asScalarOrRecordBox(resolved.attribute(), box, resolved.fieldName(), isMandatory);
    }

    Object getAttributeScalar(
            List<String> fieldNames,
            AttributeBox box,
            boolean isMandatory
    ) {
        log.trace("↪ RuntimeService::getAttributeScalar({}, {}, {})", fieldNames, box, isMandatory);

        ResolvedAttribute resolved = resolveFromAttributeBox(fieldNames, box, isMandatory);
        if (resolved == null) {
            return null;
        }
        return asScalarOrRecordBox(resolved.attribute(), box, resolved.fieldName(), isMandatory);
    }

    private ResolvedAttribute resolveFromRecord(
            List<String> fieldNames,
            RecordBox box,
            boolean isMandatory
    ) {
        Attribute<?> found = null;
        String resolvedName = null;
        for (String fieldName : fieldNames) {
            resolvedName = fieldName;
            found = lookupRecordAttribute(box, fieldName);
            if (found != null) {
                break;
            }
        }
        if (found == null) {
            log.trace("↪ Attribute(s) not present: {}", fieldNames);
            if (isMandatory) {
                log.info("↪ Mandatory field(s) not present: {}", fieldNames);
            }
            return null;
        }
        return new ResolvedAttribute(found, resolvedName);
    }

    private ResolvedAttribute resolveFromAttributeBox(
            List<String> fieldNames,
            AttributeBox box,
            boolean isMandatory
    ) {
        String fieldName = null;
        Attribute<?> attribute = null;

        Iterator<String> fnit = fieldNames.iterator();
        if (fnit.hasNext()) {
            fieldName = fnit.next();
            attribute = box.getAttribute(fieldName);
            if (attribute == null) {
                log.trace("↪ No attribute '{}'.", fieldName);
            }
            while (attribute == null && fnit.hasNext()) {
                fieldName = fnit.next();
                log.debug("↪  ... trying '{}'.", fieldName);
                attribute = box.getAttribute(fieldName);
                if (attribute == null) {
                    log.trace("↪ No attribute '{}'.", fieldName);
                }
            }
        }
        if (attribute == null) {
            if (isMandatory) {
                log.info("↪ Mandatory field(s) not present: {}", fieldNames);
            }
            return null;
        }
        return new ResolvedAttribute(attribute, fieldName);
    }

    private Attribute<?> lookupRecordAttribute(RecordBox box, String fieldName) {
        Attribute<Attribute<?>> recordAttribute = box.getRecordAttribute();
        ArrayList<Attribute<?>> values = recordAttribute.getValueVector();
        for (Attribute<?> attr : values) {
            if (attr.getAlias().equals(fieldName)) {
                return attr;
            }
        }
        return null;
    }

    private Object asArrayOrBoxes(
            Attribute<?> attribute,
            Box outer,
            String fieldName,
            boolean isMandatory
    ) {
        ArrayList<?> values = attribute.getValueVector();
        if (values.isEmpty()) {
            log.trace("↪ No values for attribute '{}'.", fieldName);
            if (isMandatory) {
                log.info("↪ Mandatory value(s) for field '{}' not present", fieldName);
            }
            return null;
        }

        if (!AttributeType.RECORD.equals(attribute.getType())) {
            return values;
        }
        return createChildBoxes(attribute, outer);
    }

    private Object asScalarOrRecordBox(
            Attribute<?> attribute,
            Box outer,
            String fieldName,
            boolean isMandatory
    ) {
        ArrayList<?> values = attribute.getValueVector();
        if (values.isEmpty()) {
            log.trace("↪ No values for attribute '{}'.", fieldName);
            if (isMandatory) {
                log.info("↪ Mandatory value(s) for field '{}' not present", fieldName);
            }
            return null;
        }

        if (!AttributeType.RECORD.equals(attribute.getType())) {
            return values.getFirst();
        }
        return createRecordBox(attribute, outer, values);
    }

    private ArrayList<Box> createChildBoxes(Attribute<?> attribute, Box outer) {
        @SuppressWarnings("unchecked")
        ArrayList<Attribute<?>> children = (ArrayList<Attribute<?>>) attribute.getValueVector();
        ArrayList<Box> boxes = new ArrayList<>();
        children.forEach(child -> {
            if (!AttributeType.RECORD.equals(child.getType())) {
                boxes.add(new AttributeBox(outer, Map.of(child.getAlias(), child)));
            } else {
                @SuppressWarnings("unchecked")
                Attribute<Attribute<?>> childAsRecord = (Attribute<Attribute<?>>) child;
                boxes.add(new RecordBox(outer, childAsRecord, Map.of(child.getAlias(), child)));
            }
        });
        return boxes;
    }

    private RecordBox createRecordBox(Attribute<?> attribute, Box outer, ArrayList<?> values) {
        Map<String, Attribute<?>> attributes = new HashMap<>();
        @SuppressWarnings("unchecked")
        ArrayList<Attribute<?>> children = (ArrayList<Attribute<?>>) values;
        children.forEach(attr -> attributes.put(attr.getAlias(), attr));

        @SuppressWarnings("unchecked")
        Attribute<Attribute<?>> attributeAsRecord = (Attribute<Attribute<?>>) attribute;
        return new RecordBox(outer, attributeAsRecord, attributes);
    }

    private record ResolvedAttribute(Attribute<?> attribute, String fieldName) {}
}
