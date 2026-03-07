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

import com.networknt.schema.*;
import graphql.schema.idl.RuntimeWiring;
import org.gautelis.ipto.graphql.configuration.Configurator;
import org.gautelis.ipto.graphql.model.CatalogAttribute;
import org.gautelis.ipto.graphql.model.CatalogRecord;
import org.gautelis.ipto.graphql.model.CatalogTemplate;
import org.gautelis.ipto.graphql.model.GqlOperationShape;
import org.gautelis.ipto.graphql.model.Query;
import org.gautelis.ipto.graphql.runtime.box.AttributeBox;
import org.gautelis.ipto.graphql.runtime.box.Box;
import org.gautelis.ipto.graphql.runtime.box.RecordBox;
import org.gautelis.ipto.graphql.runtime.wiring.RuntimeOperators;
import org.gautelis.ipto.graphql.runtime.wiring.SubscriptionWiringPolicy;
import org.gautelis.ipto.repo.model.Repository;
import org.gautelis.ipto.repo.model.Unit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ObjectNode;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;


public class RuntimeService {
    private static final Logger log = LoggerFactory.getLogger(RuntimeService.class);

    private final Repository repo;
    private final Map</* attribute alias */ String, CatalogAttribute> allAttributesByAlias = new HashMap<>();
    private final Map<String, CatalogTemplate> templatesByName = new HashMap<>();
    private final Map<String, CatalogRecord> recordsByAttributeName = new HashMap<>();
    private final Map<Integer, CatalogAttribute> attributesById = new HashMap<>();

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Schema unitSchema;
    private final RuntimeOperationInvoker operationInvoker;
    private final RuntimeAttributeResolver attributeResolver;
    private final RuntimeSearchService searchService;
    private final RuntimeUnitService unitService;

    public RuntimeService(
            Repository repo,
            Configurator.CatalogViewpoint catalogView
    ) {
        this.repo = repo;

        // Rearrange attributes found in catalog view, since
        // we will refer to them through their aliases
        for (CatalogAttribute attribute : catalogView.attributes().values()) {
            String alias = attribute.alias();
            if (alias != null && !alias.isEmpty()) {
                allAttributesByAlias.put(alias, attribute);
            }
            attributesById.put(attribute.attrId(), attribute);
        }

        for (CatalogTemplate template : catalogView.templates().values()) {
            templatesByName.put(template.templateName(), template);
        }
        for (CatalogRecord record : catalogView.records().values()) {
            CatalogAttribute recordAttribute = attributesById.get(record.recordAttrId());
            if (recordAttribute != null) {
                recordsByAttributeName.put(recordAttribute.attrName(), record);
            }
        }

        SchemaRegistry schemaRegistry = SchemaRegistry.withDefaultDialect(SpecificationVersion.DRAFT_2020_12);
        unitSchema = schemaRegistry.getSchema(SchemaLocation.of("classpath:schema/unit.json"));
        if (null != unitSchema) {
            unitSchema.initializeValidators();
        } else {
            log.warn("No schema for validation");
        }

        operationInvoker = new RuntimeOperationInvoker(this);
        attributeResolver = new RuntimeAttributeResolver(log);
        searchService = new RuntimeSearchService(repo, allAttributesByAlias, log);
        unitService = new RuntimeUnitService(repo, log);
    }

    public static String headHex(byte[] bytes, int n) {
        int len = Math.min(bytes.length, n);
        String hex = HexFormat.of().formatHex(bytes, 0, len);
        return hex.replaceAll("..(?!$)", "$0 ");
    }

    public void wire(
            RuntimeWiring.Builder runtimeWiring,
            Configurator.GqlViewpoint gqlViewpoint,
            SubscriptionWiringPolicy subscriptionWiringPolicy
    ) {
        RuntimeOperators.wireRecords(runtimeWiring, this, gqlViewpoint);
        RuntimeOperators.wireUnions(runtimeWiring, gqlViewpoint);
        RuntimeOperators.wireOperations(runtimeWiring, this, gqlViewpoint, subscriptionWiringPolicy);
    }

    public Object invokeOperation(
            GqlOperationShape operation,
            Map<String, Object> arguments
    ) {
        return operationInvoker.invokeOperation(operation, arguments);
    }

    public Object storeRawUnit(byte[] bytes) {
        log.trace("↪ RuntimeService::storeRawUnit({}...)", headHex(bytes, 16));

        String json = new String(bytes);

        List<com.networknt.schema.Error> errors = unitSchema.validate(json, InputFormat.JSON,
                executionContext -> executionContext
                        .executionConfig(executionConfig -> executionConfig.formatAssertionsEnabled(true)));

        if (errors.isEmpty()) {
            JsonNode root = MAPPER.readTree(json);
            Unit unit = repo.storeUnit(root);
            return unit.asJson(/* pretty? */ false).getBytes(StandardCharsets.UTF_8);

        } else {
            StringBuilder buf = new StringBuilder();
            errors.forEach(e -> buf.append('\n').append(e.getMessage()));
            log.info("↪ JSON validation errors: {}", buf);
            return Map.of("validation-errors", buf.toString());
        }
    }

    public Object storeDomainRawUnit(
            int tenantId,
            String templateName,
            byte[] bytes
    ) {
        log.trace("↪ RuntimeService::storeDomainRawUnit(template={}, tenant={}, payload={}...)", templateName, tenantId, headHex(bytes, 16));

        JsonNode payload = MAPPER.readTree(bytes);
        if (!(payload instanceof ObjectNode payloadObject)) {
            throw new IllegalArgumentException("Domain payload must be a JSON object");
        }

        CatalogTemplate template = templatesByName.get(templateName);
        if (template == null) {
            throw new IllegalArgumentException("Unknown template: " + templateName);
        }

        UUID corrId = DomainPayloadIndexer.extractCorrId(payloadObject);
        Optional<Unit> existing = repo.getUnit(tenantId, corrId);
        DomainPayloadIndexer.validateVersion(existing, payloadObject, corrId);

        Long existingUnitId = existing.map(Unit::getUnitId).orElse(null);
        String existingUnitName = existing.flatMap(Unit::getName).orElse(null);

        ObjectNode unit = DomainPayloadIndexer.toIptoUnit(
                payloadObject,
                bytes,
                tenantId,
                templateName,
                template,
                recordsByAttributeName,
                corrId,
                existingUnitId,
                existingUnitName
        );

        Unit stored = repo.storeUnit(unit);
        return stored.asJson(false).getBytes(StandardCharsets.UTF_8);
    }

    public Box loadUnit(int tenantId, long unitId) {
        return unitService.loadUnit(tenantId, unitId);
    }

    public Box loadUnitByCorrId(int tenantId, UUID corrId) {
        return unitService.loadUnitByCorrId(tenantId, corrId);
    }



    public byte[] loadRawUnit(int tenantId, long unitId) {
        return unitService.loadRawUnit(tenantId, unitId);
    }

    public byte[] loadRawPayload(int tenantId, long unitId) {
        return unitService.loadRawPayload(tenantId, unitId);
    }

    public byte[] loadRawPayloadByCorrId(int tenantId, UUID corrId) {
        return unitService.loadRawPayloadByCorrId(tenantId, corrId);
    }

    public Object getValueArray(
            List<String> fieldNames,
            RecordBox box,
            boolean isMandatory
    ) {
        return attributeResolver.getValueArray(fieldNames, box, isMandatory);
    }

    public Object getAttributeArray(
            List<String> fieldNames,
            AttributeBox box,
            boolean isMandatory
    ) {
        return attributeResolver.getAttributeArray(fieldNames, box, isMandatory);
    }

    public Object getAttributeArray(
            List<String> fieldNames,
            AttributeBox box
    ) {
        return getAttributeArray(fieldNames, box, false);
    }

    public Object getValueScalar(
            List<String> fieldNames,
            RecordBox box,
            boolean isMandatory
    ) {
        return attributeResolver.getValueScalar(fieldNames, box, isMandatory);
    }

    public Object getAttributeScalar(
            List<String> fieldNames,
            AttributeBox box,
            boolean isMandatory
    ) {
        return attributeResolver.getAttributeScalar(fieldNames, box, isMandatory);
    }

    public Object getAttributeScalar(
            List<String> fieldNames,
            AttributeBox box
    ) {
        return getAttributeScalar(fieldNames, box,false);
    }

    public List<Box> search(
            Query.Filter filter
    ) {
        return searchService.search(filter);
    }

    public byte[] searchRaw(
            Query.Filter filter
    ) {
        return searchService.searchRaw(filter);
    }

    public byte[] searchRawPayload(
            Query.Filter filter
    ) {
        return searchService.searchRawPayload(filter, unitService::loadRawPayload);
    }
}
