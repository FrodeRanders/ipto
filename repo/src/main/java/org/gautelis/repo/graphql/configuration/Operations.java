package org.gautelis.repo.graphql.configuration;

import graphql.language.*;
import graphql.schema.idl.TypeDefinitionRegistry;
import org.gautelis.repo.graphql.model.GqlOperationShape;
import org.gautelis.repo.graphql.model.SchemaOperation;
import org.gautelis.repo.graphql.model.TypeDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class Operations {
    private static final Logger log = LoggerFactory.getLogger(Operations.class);

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

            if (!inputs.isEmpty()) {
                InputValueDefinition input = inputs.getFirst();
                String inputName = input.getName();
                TypeDefinition inputType = TypeDefinition.of(input.getType());

                operations.put(operationName, new GqlOperationShape(typeName, operationName, SchemaOperation.QUERY.name(), inputName, inputType.typeName(), resultType.typeName()));
            }
        }
    }

    private static void deriveMutationOperations(ObjectTypeDefinition type, Map<String, GqlOperationShape> operations) {
        String typeName = type.getName();

        for (FieldDefinition f : type.getFieldDefinitions()) {
            final String operationName = f.getName(); // field name
            final TypeDefinition resultType = TypeDefinition.of(f.getType());

            List<InputValueDefinition> inputs = f.getInputValueDefinitions();

            if (!inputs.isEmpty()) {
                InputValueDefinition input = inputs.getFirst();
                String inputName = input.getName();
                TypeDefinition inputType = TypeDefinition.of(input.getType());

                operations.put(operationName, new GqlOperationShape(typeName, operationName, SchemaOperation.MUTATION.name(), inputName, inputType.typeName(), resultType.typeName()));
            }
        }
    }
}