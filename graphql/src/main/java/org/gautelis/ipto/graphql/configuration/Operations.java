package org.gautelis.ipto.graphql.configuration;

import graphql.language.*;
import graphql.schema.idl.TypeDefinitionRegistry;
import org.gautelis.ipto.graphql.model.GqlOperationShape;
import org.gautelis.ipto.graphql.model.ParameterDefinition;
import org.gautelis.ipto.graphql.model.SchemaOperation;
import org.gautelis.ipto.graphql.model.TypeDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class Operations {
    private static final Logger log = LoggerFactory.getLogger(Operations.class);

    private static final ParameterDefinition[] T = {};

    private Operations() {}

    static Map<String, GqlOperationShape> derive(TypeDefinitionRegistry registry, Map<String, SchemaOperation> operationTypes) {
        Map<String, GqlOperationShape> operations = new HashMap<>();

        for (ObjectTypeDefinition type : registry.getTypes(ObjectTypeDefinition.class)) {
            List<Directive> directives = type.getDirectives();
            boolean isOperation = true;
            for (Directive directive : directives) {
                String name = directive.getName();

                // Kind of negative logic here, treat as operation if neither 'unit' nor 'record'
                isOperation &= !"unit".equals(name);
                isOperation &= !"record".equals(name);
            }

            if (isOperation) {
                // Handle Query and Mutation
                SchemaOperation operation = operationTypes.get(type.getName());
                if (operation != null) {
                    switch (operation) {
                        case QUERY -> deriveQueryOperations(type, operations);
                        case MUTATION -> deriveMutationOperations(type, operations);
                    };
                }
            }
        }

        return operations;
    }

    private static void deriveQueryOperations(ObjectTypeDefinition type, Map<String, GqlOperationShape> operations) {
        String typeName = type.getName();

        for (FieldDefinition f : type.getFieldDefinitions()) {
            final String operationName = f.getName(); // field name
            final TypeDefinition resultType = TypeDefinition.of(f.getType());
            List<InputValueDefinition> inputs = f.getInputValueDefinitions();

            List<ParameterDefinition> params = new ArrayList<>();
            for (InputValueDefinition ivd : inputs) {
                params.add(
                        new ParameterDefinition(ivd.getName(), TypeDefinition.of(ivd.getType()))
                );
            }

            operations.put(operationName, new GqlOperationShape(typeName, operationName, SchemaOperation.QUERY, params.toArray(T), resultType.typeName()));
        }
    }

    private static void deriveMutationOperations(ObjectTypeDefinition type, Map<String, GqlOperationShape> operations) {
        String typeName = type.getName();

        for (FieldDefinition f : type.getFieldDefinitions()) {
            final String operationName = f.getName(); // field name
            final TypeDefinition resultType = TypeDefinition.of(f.getType());
            List<InputValueDefinition> inputs = f.getInputValueDefinitions();

            List<ParameterDefinition> params = new ArrayList<>();
            for (InputValueDefinition ivd : inputs) {
                params.add(
                        new ParameterDefinition(ivd.getName(), TypeDefinition.of(ivd.getType()))
                );
            }

            operations.put(operationName, new GqlOperationShape(typeName, operationName, SchemaOperation.MUTATION, params.toArray(T), resultType.typeName()));

        }
    }
}