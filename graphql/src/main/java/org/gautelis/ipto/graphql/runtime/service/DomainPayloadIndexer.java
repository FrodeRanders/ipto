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

import org.gautelis.ipto.graphql.model.CatalogAttribute;
import org.gautelis.ipto.graphql.model.CatalogRecord;
import org.gautelis.ipto.graphql.model.CatalogTemplate;
import org.gautelis.ipto.repo.model.AttributeType;
import org.gautelis.ipto.repo.model.Unit;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.ObjectNode;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.gautelis.ipto.graphql.runtime.wiring.RuntimeOperators.MAPPER;

final class DomainPayloadIndexer {
    private DomainPayloadIndexer() {
    }

    static UUID extractCorrId(ObjectNode payload) {
        JsonNode idNode = payload.path("id");
        if (idNode.isTextual()) {
            return UUID.fromString(idNode.asText().trim());
        }
        return UUID.randomUUID();
    }

    static void validateVersion(
            Optional<Unit> existing,
            ObjectNode payload,
            UUID corrId
    ) {
        if (existing.isEmpty()) {
            return;
        }
        JsonNode versionNode = payload.path("version");
        if (!versionNode.isNumber()) {
            return;
        }

        int expected = existing.get().getVersion() + 1;
        int provided = versionNode.asInt();
        if (provided != expected) {
            throw new IllegalArgumentException(
                    "Version mismatch for corrid " + corrId + ": expected " + expected + " but got " + provided
            );
        }
    }

    static ObjectNode toIptoUnit(
            ObjectNode payload,
            byte[] rawPayload,
            int tenantId,
            String templateName,
            CatalogTemplate template,
            Map<String, CatalogRecord> recordsByAttributeName,
            UUID corrId,
            Long existingUnitId,
            String existingUnitName
    ) {
        ObjectNode unit = MAPPER.createObjectNode();
        unit.put("@type", "ipto:unit");
        unit.put("@version", 2);
        unit.put("tenantid", tenantId);
        if (existingUnitId == null) {
            unit.putNull("unitid");
        } else {
            unit.put("unitid", existingUnitId);
        }
        unit.putNull("unitver");
        unit.put("corrid", corrId.toString());
        unit.put("status", Unit.Status.EFFECTIVE.getStatus());

        String unitName = existingUnitName;
        if (unitName == null || unitName.isBlank()) {
            unitName = templateName.toLowerCase() + "-" + corrId;
        }
        unit.put("unitname", unitName);

        String now = Instant.now().toString();
        unit.put("created", now);
        unit.put("modified", now);

        ArrayNode attrs = MAPPER.createArrayNode();
        attrs.add(buildDataScalar("raw_payload", "ffa:raw_payload", rawPayload));

        for (CatalogAttribute field : template.fields()) {
            JsonNode valueNode = resolveNode(payload, field.alias(), field.attrName());
            if (valueNode == null || valueNode.isNull() || valueNode.isMissingNode()) {
                continue;
            }
            List<ObjectNode> projected = projectAttribute(field, valueNode, recordsByAttributeName, 0);
            for (ObjectNode projectedAttribute : projected) {
                attrs.add(projectedAttribute);
            }
        }

        unit.set("attributes", attrs);
        return unit;
    }

    private static List<ObjectNode> projectAttribute(
            CatalogAttribute attribute,
            JsonNode valueNode,
            Map<String, CatalogRecord> recordsByAttributeName,
            int depth
    ) {
        if (depth > 8) {
            return List.of();
        }

        if (attribute.attrType() == AttributeType.RECORD) {
            CatalogRecord record = recordsByAttributeName.get(attribute.attrName());
            if (record == null) {
                return List.of();
            }
            return projectRecordAttribute(attribute, record, valueNode, recordsByAttributeName, depth + 1);
        }

        ArrayNode values = extractScalarValues(valueNode, attribute.attrType());
        if (values.isEmpty()) {
            return List.of();
        }
        return List.of(buildScalar(attribute, values));
    }

    private static List<ObjectNode> projectRecordAttribute(
            CatalogAttribute parentAttribute,
            CatalogRecord record,
            JsonNode valueNode,
            Map<String, CatalogRecord> recordsByAttributeName,
            int depth
    ) {
        List<ObjectNode> projected = new ArrayList<>();

        if (valueNode.isArray()) {
            for (JsonNode entry : valueNode) {
                ArrayNode nested = projectRecordFields(record, entry, recordsByAttributeName, depth);
                if (!nested.isEmpty()) {
                    projected.add(buildRecord(parentAttribute, /* is array entry */ true, nested));
                }
            }
            return projected;
        }

        ArrayNode nested = projectRecordFields(record, valueNode, recordsByAttributeName, depth);
        if (!nested.isEmpty()) {
            projected.add(buildRecord(parentAttribute, parentAttribute.isArray(), nested));
        }
        return projected;
    }

    private static ArrayNode projectRecordFields(
            CatalogRecord record,
            JsonNode node,
            Map<String, CatalogRecord> recordsByAttributeName,
            int depth
    ) {
        ArrayNode nested = MAPPER.createArrayNode();
        if (node == null || node.isNull() || node.isMissingNode()) {
            return nested;
        }

        for (CatalogAttribute field : record.fields()) {
            JsonNode child = resolveNode(node, field.alias(), field.attrName());
            if (child == null || child.isNull() || child.isMissingNode()) {
                continue;
            }
            List<ObjectNode> projected = projectAttribute(field, child, recordsByAttributeName, depth + 1);
            for (ObjectNode projectedChild : projected) {
                nested.add(projectedChild);
            }
        }
        return nested;
    }

    private static JsonNode resolveNode(
            JsonNode node,
            String alias,
            String attrName
    ) {
        if (node == null || node.isNull() || node.isMissingNode()) {
            return null;
        }

        JsonNode byAlias = node.path(alias);
        if (!byAlias.isMissingNode()) {
            return byAlias;
        }

        String localName = attrName;
        int idx = attrName.indexOf(':');
        if (idx >= 0 && idx + 1 < attrName.length()) {
            localName = attrName.substring(idx + 1);
        }
        JsonNode byLocal = node.path(localName);
        if (!byLocal.isMissingNode()) {
            return byLocal;
        }

        JsonNode valueObject = node.path("varde");
        if (!valueObject.isMissingNode() && valueObject.isObject()) {
            JsonNode nested = resolveNode(valueObject, alias, attrName);
            if (nested != null && !nested.isMissingNode()) {
                return nested;
            }
        }

        return null;
    }

    private static ArrayNode extractScalarValues(JsonNode valueNode, AttributeType type) {
        ArrayNode values = MAPPER.createArrayNode();
        if (valueNode == null || valueNode.isNull() || valueNode.isMissingNode()) {
            return values;
        }

        if (valueNode.isArray()) {
            for (JsonNode item : valueNode) {
                addScalarValue(values, unwrapValue(item), type);
            }
            return values;
        }

        addScalarValue(values, unwrapValue(valueNode), type);
        return values;
    }

    private static JsonNode unwrapValue(JsonNode node) {
        if (node != null && node.isObject()) {
            JsonNode nested = node.path("varde");
            if (!nested.isMissingNode() && !nested.isNull()) {
                return nested;
            }
            JsonNode value = node.path("value");
            if (!value.isMissingNode() && !value.isNull()) {
                return value;
            }
        }
        return node;
    }

    private static void addScalarValue(ArrayNode target, JsonNode value, AttributeType type) {
        if (value == null || value.isNull() || value.isMissingNode()) {
            return;
        }

        switch (type) {
            case STRING, TIME -> target.add(value.asText());
            case INTEGER -> target.add(value.asInt());
            case LONG -> target.add(value.asLong());
            case DOUBLE -> target.add(value.asDouble());
            case BOOLEAN -> target.add(value.asBoolean());
            case DATA -> {
                if (value.isTextual()) {
                    target.add(value.asText());
                } else {
                    byte[] bytes = MAPPER.writeValueAsBytes(value);
                    target.add(Base64.getEncoder().encodeToString(bytes));
                }
            }
            default -> {
            }
        }
    }

    private static ObjectNode buildScalar(CatalogAttribute attribute, ArrayNode values) {
        String typeToken = switch (attribute.attrType()) {
            case STRING -> "string";
            case TIME -> "time";
            case INTEGER -> "integer";
            case LONG -> "long";
            case DOUBLE -> "double";
            case BOOLEAN -> "boolean";
            case DATA -> "data";
            default -> throw new IllegalArgumentException("Unsupported scalar type: " + attribute.attrType());
        };

        ObjectNode node = MAPPER.createObjectNode();
        node.put("@type", "ipto:" + typeToken + "-" + (attribute.isArray() ? "vector" : "scalar"));
        node.put("alias", attribute.alias());
        node.put("attrtype", attribute.attrType().name());
        node.put("attrname", attribute.attrName());
        node.set("value", values);
        return node;
    }

    private static ObjectNode buildRecord(
            CatalogAttribute attribute,
            boolean asArrayEntry,
            ArrayNode attributes
    ) {
        ObjectNode node = MAPPER.createObjectNode();
        node.put("@type", "ipto:record-" + (asArrayEntry ? "vector" : "scalar"));
        node.put("alias", attribute.alias());
        node.put("attrtype", "RECORD");
        node.put("attrname", attribute.attrName());
        node.set("attributes", attributes);
        return node;
    }

    private static ObjectNode buildDataScalar(String alias, String attrName, byte[] payload) {
        ArrayNode values = MAPPER.createArrayNode();
        values.add(Base64.getEncoder().encodeToString(payload));

        ObjectNode node = MAPPER.createObjectNode();
        node.put("@type", "ipto:data-scalar");
        node.put("alias", alias);
        node.put("attrtype", "DATA");
        node.put("attrname", attrName);
        node.set("value", values);
        return node;
    }
}
