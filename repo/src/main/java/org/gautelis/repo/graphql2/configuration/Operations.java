package org.gautelis.repo.graphql2.configuration;

import graphql.language.*;
import graphql.schema.idl.TypeDefinitionRegistry;
import org.gautelis.repo.graphql2.model.OperationDef;
import org.gautelis.repo.graphql2.model.external.TypeDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;


public final class Operations {
    private static final Logger log = LoggerFactory.getLogger(Operations.class);

    private Operations() {}

    enum SchemaOperation {QUERY, MUTATION}

    static Map<String, OperationDef> derive(TypeDefinitionRegistry registry) {
        Map<String, SchemaOperation> operationTypes = new HashMap<>();

        Optional<SchemaDefinition> _schemaDefinition = registry.schemaDefinition();
        if (_schemaDefinition.isPresent()) {
            SchemaDefinition schemaDefinition = _schemaDefinition.get();

            List<OperationTypeDefinition> otds = schemaDefinition.getOperationTypeDefinitions();
            for (OperationTypeDefinition otd : otds) {
                switch (otd.getName()) {
                    case "query" -> {
                        operationTypes.put(otd.getTypeName().getName(), SchemaOperation.QUERY);
                    }
                    case "mutation" -> {
                        operationTypes.put(otd.getTypeName().getName(), SchemaOperation.MUTATION);
                    }
                }
            }
        }

        Map<String, OperationDef> operations = new HashMap<>();

        for (ObjectTypeDefinition type : registry.getTypes(ObjectTypeDefinition.class)) {
            List<Directive> directives = type.getDirectives();
            boolean isOperation = true;
            for (Directive directive : directives) {
                String name = directive.getName();

                // Kind of negative logic here, treat as operation if not 'unit' nor 'record'
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

    private static void deriveQueryOperations(ObjectTypeDefinition type, Map<String, OperationDef> operations) {
        for (FieldDefinition f : type.getFieldDefinitions()) {
            // Operation name
            final String fieldName = f.getName();
            final TypeDef resultType = TypeDef.of(f.getType());

            List<InputValueDefinition> inputs = f.getInputValueDefinitions();

            if (!inputs.isEmpty()) {
                InputValueDefinition input = inputs.getFirst();
                String inputName = input.getName();
                TypeDef typeDef = TypeDef.of(input.getType());
            }

            operations.put(/* operation name */ fieldName, new OperationDef(fieldName, null, null)); // TODO
        }
    }

    private static void deriveMutationOperations(ObjectTypeDefinition type, Map<String, OperationDef> operations) {
        for (FieldDefinition f : type.getFieldDefinitions()) {
            // Operation name
            final String fieldName = f.getName();
            final TypeDef resultType = TypeDef.of(f.getType());

            List<InputValueDefinition> inputs = f.getInputValueDefinitions();

            if (!inputs.isEmpty()) {
                InputValueDefinition input = inputs.getFirst();
                String inputName = input.getName();
                TypeDef typeDef = TypeDef.of(input.getType());
            }

            operations.put(/* operation name */ fieldName, new OperationDef(fieldName, null, null)); // TODO
        }
    }
}