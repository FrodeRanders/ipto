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
package org.gautelis.ipto.graphql.configuration;

import graphql.language.Argument;
import graphql.language.Directive;
import graphql.language.EnumValue;
import graphql.language.FieldDefinition;
import graphql.language.ObjectTypeDefinition;
import graphql.language.OperationTypeDefinition;
import graphql.language.StringValue;
import graphql.language.Value;
import graphql.schema.idl.TypeDefinitionRegistry;
import org.gautelis.ipto.graphql.model.FieldAliasResolver;
import org.gautelis.ipto.graphql.model.GqlAttributeShape;
import org.gautelis.ipto.graphql.model.GqlFieldShape;
import org.gautelis.ipto.graphql.model.SchemaOperation;
import org.gautelis.ipto.graphql.model.TypeDefinition;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

final class SdlObjectShapes {
    private SdlObjectShapes() {}

    static List<ObjectTypeDefinition> domainObjectTypes(TypeDefinitionRegistry registry) {
        Set<String> operationTypeNames = operationTypeNames(registry);
        List<ObjectTypeDefinition> result = new ArrayList<>();
        for (ObjectTypeDefinition type : registry.getTypes(ObjectTypeDefinition.class)) {
            String normalized = type.getName().toLowerCase(Locale.ROOT);
            if (!operationTypeNames.contains(normalized)) {
                result.add(type);
            }
        }
        return result;
    }

    static Set<String> operationTypeNames(TypeDefinitionRegistry registry) {
        Set<String> names = new HashSet<>();
        for (String typeName : operationTypeMap(registry).keySet()) {
            names.add(typeName.toLowerCase(Locale.ROOT));
        }
        return names;
    }

    static Map<String, SchemaOperation> operationTypeMap(TypeDefinitionRegistry registry) {
        Map<String, SchemaOperation> operationTypes = new HashMap<>();
        // GraphQL defaults; overridden when schema { query: ..., mutation: ... } is present.
        operationTypes.put("Query", SchemaOperation.QUERY);
        operationTypes.put("Mutation", SchemaOperation.MUTATION);
        operationTypes.put("Subscription", SchemaOperation.SUBSCRIPTION);

        registry.schemaDefinition().ifPresent(schema -> {
            for (OperationTypeDefinition operationType : schema.getOperationTypeDefinitions()) {
                SchemaOperation category = switch (operationType.getName().toLowerCase(Locale.ROOT)) {
                    case "query" -> SchemaOperation.QUERY;
                    case "mutation" -> SchemaOperation.MUTATION;
                    case "subscription" -> SchemaOperation.SUBSCRIPTION;
                    default -> null;
                };
                if (category != null) {
                    operationTypes.put(operationType.getTypeName().getName(), category);
                }
            }
        });
        return operationTypes;
    }

    static List<GqlFieldShape> deriveFields(
            ObjectTypeDefinition type,
            Map<String, GqlAttributeShape> attributes,
            Logger log,
            boolean logMissing
    ) {
        List<GqlFieldShape> fields = new ArrayList<>();
        String typeName = type.getName();

        for (FieldDefinition field : type.getFieldDefinitions()) {
            String fieldName = field.getName();
            TypeDefinition fieldType = TypeDefinition.of(field.getType());

            String attributeName = resolveFieldAttributeName(field, fieldName, attributes);
            if (attributeName != null) {
                fields.add(new GqlFieldShape(
                        typeName,
                        fieldName,
                        fieldType.typeName(),
                        fieldType.isArray(),
                        fieldType.isMandatory(),
                        attributeName
                ));
            } else if (logMissing) {
                log.debug("↯ Not a valid field definition: {}", fieldName);
            }
        }
        return fields;
    }

    private static String resolveFieldAttributeName(
            FieldDefinition field,
            String fieldName,
            Map<String, GqlAttributeShape> attributes
    ) {
        // @use(attribute: ...) allows SDL field name to diverge from repo attribute naming.
        List<Directive> useDirectives = field.getDirectives("use");
        if (!useDirectives.isEmpty()) {
            for (Directive useDirective : useDirectives) {
                Argument arg = useDirective.getArgument("attribute");
                String attributeKey = extractName(arg != null ? arg.getValue() : null);
                if (attributeKey == null || attributeKey.isBlank()) {
                    continue;
                }
                var attribute = FieldAliasResolver.resolveGqlAttribute(attributes, attributeKey);
                if (attribute.isPresent()) {
                    return attribute.get().name();
                }
            }
            return null;
        }

        return FieldAliasResolver.resolveGqlAttribute(attributes, fieldName)
                .map(GqlAttributeShape::name)
                .orElse(null);
    }

    static String extractName(Value<?> value) {
        if (value instanceof EnumValue enumValue) {
            return enumValue.getName();
        }
        if (value instanceof StringValue stringValue) {
            return stringValue.getValue();
        }
        return null;
    }
}
