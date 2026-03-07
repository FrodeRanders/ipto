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

import graphql.language.*;
import graphql.schema.idl.TypeDefinitionRegistry;
import org.gautelis.ipto.graphql.model.GqlOperationShape;
import org.gautelis.ipto.graphql.model.OperationKey;
import org.gautelis.ipto.graphql.model.ParameterDefinition;
import org.gautelis.ipto.graphql.model.RuntimeOperation;
import org.gautelis.ipto.graphql.model.SchemaOperation;
import org.gautelis.ipto.graphql.model.TypeDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public final class Operations {
    private static final Logger log = LoggerFactory.getLogger(Operations.class);

    private Operations() {}

    static Map<OperationKey, GqlOperationShape> derive(TypeDefinitionRegistry registry, Map<String, SchemaOperation> operationTypes) {
        Map<OperationKey, GqlOperationShape> operations = new HashMap<>();

        for (ObjectTypeDefinition type : registry.getTypes(ObjectTypeDefinition.class)) {
            SchemaOperation operation = operationTypes.get(type.getName());
            if (operation == null) {
                continue;
            }

            switch (operation) {
                case QUERY, MUTATION -> deriveOperations(type, operation, operations);
                case SUBSCRIPTION -> deriveSubscriptionOperations(type, operations);
            }
        }

        return operations;
    }

    private static void deriveOperations(
            ObjectTypeDefinition type,
            SchemaOperation category,
            Map<OperationKey, GqlOperationShape> operations
    ) {
        String typeName = type.getName();
        for (FieldDefinition f : type.getFieldDefinitions()) {
            GqlOperationShape shape = deriveOperationShape(typeName, category, f);
            operations.put(shape.key(), shape);
        }
    }

    private static void deriveSubscriptionOperations(ObjectTypeDefinition type, Map<OperationKey, GqlOperationShape> operations) {
        if (type.getFieldDefinitions().isEmpty()) {
            log.info("↯ Subscription root '{}' has no fields", type.getName());
            return;
        }

        String typeName = type.getName();
        for (FieldDefinition f : type.getFieldDefinitions()) {
            GqlOperationShape shape = deriveOperationShape(typeName, SchemaOperation.SUBSCRIPTION, f);
            operations.put(shape.key(), shape);

            log.warn(
                    "↯ Derived subscription operation '{}::{}'; runtime subscription support is not implemented yet",
                    typeName,
                    shape.operationName()
            );
        }
    }

    private static GqlOperationShape deriveOperationShape(
            String typeName,
            SchemaOperation category,
            FieldDefinition field
    ) {
        String operationName = field.getName();
        TypeDefinition resultType = TypeDefinition.of(field.getType());
        List<ParameterDefinition> params = deriveParameters(field.getInputValueDefinitions());
        RuntimeOperation runtimeOperation = deriveRuntimeOperation(field).orElse(null);

        return new GqlOperationShape(
                typeName,
                operationName,
                category,
                params,
                resultType.typeName(),
                runtimeOperation
        );
    }

    private static List<ParameterDefinition> deriveParameters(List<InputValueDefinition> inputs) {
        List<ParameterDefinition> params = new ArrayList<>();
        for (InputValueDefinition ivd : inputs) {
            params.add(new ParameterDefinition(ivd.getName(), TypeDefinition.of(ivd.getType())));
        }
        return params;
    }

    private static Optional<RuntimeOperation> deriveRuntimeOperation(FieldDefinition field) {
        for (Directive directive : field.getDirectives()) {
            if (!"ipto".equals(directive.getName())) {
                continue;
            }
            for (Argument argument : directive.getArguments()) {
                if (!"operation".equals(argument.getName())) {
                    continue;
                }
                Value<?> value = argument.getValue();
                if (value instanceof StringValue sv) {
                    return RuntimeOperation.parse(sv.getValue());
                }
                if (value instanceof EnumValue ev) {
                    return RuntimeOperation.parse(ev.getName());
                }
            }
        }
        return Optional.empty();
    }
}
