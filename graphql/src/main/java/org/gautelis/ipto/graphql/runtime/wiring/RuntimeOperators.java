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
package org.gautelis.ipto.graphql.runtime.wiring;

import graphql.schema.DataFetcher;
import graphql.schema.TypeResolver;
import graphql.schema.idl.RuntimeWiring;
import graphql.schema.idl.errors.StrictModeWiringException;
import org.gautelis.ipto.graphql.configuration.Configurator;
import org.gautelis.ipto.graphql.model.*;
import org.gautelis.ipto.graphql.runtime.box.AttributeBox;
import org.gautelis.ipto.graphql.runtime.box.Box;
import org.gautelis.ipto.graphql.runtime.box.RecordBox;
import org.gautelis.ipto.graphql.runtime.service.RuntimeService;
import org.gautelis.ipto.repo.model.AttributeType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.databind.ObjectMapper;

import java.util.*;

public class RuntimeOperators {
    private static final Logger log = LoggerFactory.getLogger(RuntimeOperators.class);

    public static final ObjectMapper MAPPER = new ObjectMapper();

    /* package accessible only */
    public static void wireRecords(
            RuntimeWiring.Builder runtimeWiring,
            RuntimeService runtimeService,
            Configurator.GqlViewpoint gqlViewpoint
    ) {
        // Pre-index by alias/type name to keep runtime fetchers simple and fast.
        Map<String, GqlAttributeShape> attributes = new HashMap<>();
        for (GqlAttributeShape shape : gqlViewpoint.attributes().values()) {
            attributes.put(shape.alias(), shape);
        }
        Map<String, GqlUnionShape> gqlUnions  = gqlViewpoint.unions();
        Map<String, GqlRecordShape> gqlRecords = new HashMap<>();
        for (GqlRecordShape shape : gqlViewpoint.records().values()) {
            gqlRecords.put(shape.typeName(), shape);
        }

        for (String key : gqlRecords.keySet()) {
            GqlRecordShape gqlRecord = gqlRecords.get(key);

            final String typeName = gqlRecord.typeName();

            Iterator<GqlFieldShape> fit = gqlRecord.fields().iterator();

            runtimeWiring.type(typeName, builder -> {
                while (fit.hasNext()) {
                    GqlFieldShape field = fit.next();

                    final String fieldName = field.fieldName();
                    final String backingAttributeName = field.usedAttributeName();
                    final String fieldType = field.gqlTypeRef();
                    final boolean isArray = field.isArray();
                    final boolean isMandatory = field.isMandatory();

                    String attributeNameForField = null;
                    for (GqlAttributeShape shape : attributes.values()) {
                        if (shape.name().equals(backingAttributeName)) {
                            // GraphQL field names and repo attribute aliases may differ.
                            attributeNameForField = shape.alias(); // may be same as fieldName
                        }
                    }

                    GqlAttributeShape attributeShape = attributes.get(attributeNameForField);
                    boolean isRecord = AttributeType.RECORD.name().equalsIgnoreCase(attributeShape.typeName());
                    final String resolvedAttributeAlias = attributeNameForField;
                    final GqlAttributeShape resolvedAttributeShape = attributeShape;

                    //log.debug("↯ Wiring field '{}' for record '{}'", fieldName, typeName);

                    final List<String> fieldNames = new ArrayList<>();

                    if (null != attributeNameForField && !attributeNameForField.isEmpty()) {
                        fieldNames.add(attributeNameForField);
                    } else {
                        fieldNames.add(fieldName);
                    }

                    GqlUnionShape union = gqlUnions.get(fieldType);
                    if (null != union) {
                        // This field refers to a union type, so the
                        // actual instances will be of a union member type.
                        // We add all member record aliases so lookups can resolve
                        // attribute storage keyed by concrete member attribute names.
                        List<String> unionMemberTypes = union.members().stream().map(UnionMember::memberType).toList();
                        log.debug("↯ Record '{}' contains a union field '{} : {}' with members {}",
                                typeName, fieldName, union.unionName(), unionMemberTypes
                        );

                        for (String memberType : unionMemberTypes) {
                            for (GqlRecordShape recrd : gqlRecords.values()) {
                                String unionMember = recrd.typeName();
                                if (unionMember.equals(memberType)) {
                                    String attributeEnumName = recrd.attributeEnumName();
                                    log.debug("↯ Adding alternative {} for union {}", memberType, union.unionName());
                                    fieldNames.add(attributeEnumName);
                                }
                            }
                        }
                    }

                    // Tell GraphQL how to fetch this specific (record) type
                    DataFetcher<?> fetcher = env -> {
                        //**** Executed at runtime **********************************
                        // This closure captures its environment, so at runtime
                        // the wiring preamble will be available.
                        //***********************************************************
                        Object source = env.getSource();
                        if (source == null) {
                            log.warn("No source");
                            return null;
                        }
                        if (!(source instanceof Box box)) {
                            throw new IllegalStateException(
                                    "Expected Box as GraphQL source for '" + typeName + "." + fieldName
                                            + "' but got " + source.getClass().getCanonicalName()
                            );
                        }
                        if (!(box instanceof AttributeBox attributeBox)) {
                            throw new IllegalStateException(
                                    "Unexpected Box implementation '" + box.getClass().getCanonicalName()
                                            + "' for '" + typeName + "." + fieldName + "'"
                            );
                        }

                        if (resolvedAttributeShape == null) {
                            throw new IllegalStateException(
                                    "No attribute shape found for field '" + typeName + "." + fieldName
                                            + "' using attribute alias '" + resolvedAttributeAlias + "'"
                            );
                        }

                        log.trace("↩ Fetching {}attribute '{}' from record '{}': {}",
                                isRecord ? "record " : "",
                                isArray ? fieldName + "[]" : fieldName,
                                typeName, box.getUnit().getReference());

                        if (attributeBox instanceof RecordBox recordBox) {
                            // RecordBox means field names target attributes inside a nested record value.
                            if (isArray) {
                                return runtimeService.getValueArray(fieldNames, recordBox, isMandatory);
                            } else {
                                return runtimeService.getValueScalar(fieldNames, recordBox, isMandatory);
                            }
                        } else {
                            // AttributeBox means field names target top-level unit attributes.
                            if (isArray) {
                                return runtimeService.getAttributeArray(fieldNames, attributeBox);
                            } else {
                                return runtimeService.getAttributeScalar(fieldNames, attributeBox);
                            }
                        }
                    };
                    builder.dataFetcher(fieldName, fetcher);
                    log.info("↯ Wiring: {} > {}", typeName, fieldName);
                }

                return builder;
            });
        }
    }

    /* package accessible only */
    public static void wireUnions(
            RuntimeWiring.Builder runtimeWiring,
            Configurator.GqlViewpoint gqlViewpoint
    ) {
        Map<String, GqlUnionShape> gqlUnions  = gqlViewpoint.unions();
        Map<String, GqlRecordShape> gqlRecords = new HashMap<>();
        for (GqlRecordShape shape : gqlViewpoint.records().values()) {
            gqlRecords.put(shape.typeName(), shape);
        }

        for (String key : gqlUnions.keySet()) {
            GqlUnionShape  gqlUnion = gqlUnions.get(key);

            String unionName = gqlUnion.unionName();

            Map</* record attribute alias */ String, /* record type */ String> aliasToTypeName = new HashMap<>();

            List<UnionMember> members = gqlUnion.members();
            for (UnionMember member : members) {
                String memberName = member.memberType();
                log.info("↯ Wiring union: {} > {}", unionName, memberName);

                GqlRecordShape gqlRecord = gqlRecords.get(memberName);
                String attributeAlias = gqlRecord.attributeEnumName();

                log.trace("↯ Union '{}': attribute alias '{}' => type '{}'", unionName, attributeAlias, gqlRecord.typeName());
                aliasToTypeName.put(attributeAlias, gqlRecord.typeName());
            }

            try {
                // Plan ahead (as soon as I get my match together :)
                //   - Derive recordAttributeId from "value"
                //   - Look up GraphQL type name from idToName
                //   - Return that object type

                TypeResolver unionResolver = env -> {
                    //**** Executed at runtime **********************************
                    // This closure captures its environment, so at runtime
                    // the wiring preamble will be available.
                    //***********************************************************
                    Object value = env.getObject(); // box
                    log.debug("↩ Union: '{}' <- {}", unionName, value);

                    if (value instanceof RecordBox recordBox) {
                        String typeName = aliasToTypeName.get(recordBox.getRecordAttribute().getAlias());
                        return env.getSchema().getObjectType(typeName);
                    }
                    log.warn("↩ No resolver for union '{}': No (record) box: {}", unionName, value);
                    return null;
                };

                runtimeWiring.type(unionName,t -> t.typeResolver(unionResolver));

            } catch (StrictModeWiringException smwe) {
                log.warn("↯ Could not wire unions for type {}", unionName, smwe);
            }
        }
    }

    /* package accessible only */
    public static void wireOperations(
            RuntimeWiring.Builder runtimeWiring,
            RuntimeService runtimeService,
            Configurator.GqlViewpoint gqlViewpoint,
            SubscriptionWiringPolicy subscriptionWiringPolicy
    ) {
        for (GqlOperationShape operation : gqlViewpoint.operations().values()) {
            String typeName = operation.typeName();
            String operationName = operation.operationName();
            String outputType = operation.outputTypeName();
            RuntimeOperation runtimeOperation = operation.runtimeOperation();

            if (runtimeOperation == RuntimeOperation.CUSTOM || runtimeOperation == RuntimeOperation.MANUAL) {
                log.info("↯ Skipping auto-wiring for {}::{} (runtime operation: {})", typeName, operationName, runtimeOperation);
                continue;
            }

            DataFetcher<?> fetcher;
            if (operation.category() == SchemaOperation.SUBSCRIPTION) {
                // Subscription shapes are accepted in SDL, but runtime delivery is not implemented yet.
                if (subscriptionWiringPolicy == SubscriptionWiringPolicy.FAIL_FAST) {
                    throw new IllegalStateException(
                            "Subscription operation '" + typeName + "::" + operationName + "' is defined in SDL, "
                                    + "but runtime subscription support is not implemented. "
                                    + "Set " + SubscriptionWiringPolicy.PROPERTY + "=scaffold "
                                    + "or " + SubscriptionWiringPolicy.ENV + "=scaffold to keep scaffolded runtime errors."
                    );
                }
                fetcher = unsupportedSubscriptionFetcher(typeName, operationName);
            } else {
                fetcher = env -> {
                    if (log.isTraceEnabled()) {
                        log.trace("↩ {}::{}({}) : {}", typeName, operationName, env.getArguments(), outputType);
                    }
                    return runtimeService.invokeOperation(operation, env.getArguments());
                };
            }

            runtimeWiring.type(typeName, t -> t.dataFetcher(operationName, fetcher));
            if (operation.category() == SchemaOperation.SUBSCRIPTION) {
                log.warn(
                        "↯ Wiring subscription scaffold: {}::{}(...) : {} (policy={})",
                        typeName, operationName, outputType, subscriptionWiringPolicy
                );
            } else {
                log.info("↯ Wiring operation: {}::{}(...) : {}", typeName, operationName, outputType);
            }
        }
    }

    private static DataFetcher<?> unsupportedSubscriptionFetcher(
            String typeName,
            String operationName
    ) {
        return env -> {
            throw new UnsupportedOperationException(
                    "Subscription operation '" + typeName + "::" + operationName
                            + "' is defined but runtime subscription support is not implemented"
            );
        };
    }
}
