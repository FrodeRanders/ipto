package org.gautelis.repo.graphql.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import graphql.language.*;
import graphql.schema.DataFetcher;
import graphql.schema.idl.RuntimeWiring;
import org.gautelis.repo.graphql.runtime.Box;
import org.gautelis.repo.graphql.runtime.RuntimeService;
import org.gautelis.repo.model.Repository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class OperationsConfigurator {
    private static final Logger log = LoggerFactory.getLogger(OperationsConfigurator.class);

    /* package visible only */
    record UnitIdentification(int tenantId, long unitId) {}

    public enum Operator {
        GT   (org.gautelis.repo.search.model.Operator.GT),
        GEQ  (org.gautelis.repo.search.model.Operator.GEQ),
        EQ   (org.gautelis.repo.search.model.Operator.EQ),
        LEQ  (org.gautelis.repo.search.model.Operator.LEQ),
        LT   (org.gautelis.repo.search.model.Operator.LT),
        LIKE (org.gautelis.repo.search.model.Operator.LIKE),
        NEQ  (org.gautelis.repo.search.model.Operator.NEQ);

        private final org.gautelis.repo.search.model.Operator iptoOp;
        Operator(org.gautelis.repo.search.model.Operator iptoOp) {
            this.iptoOp = iptoOp;
        }

        public org.gautelis.repo.search.model.Operator iptoOp() {
            return iptoOp;
        }
    }

    public record AttributeExpression(String attr, Operator op, String value) {}

    public enum Logical {
        AND  (org.gautelis.repo.search.model.Operator.AND),
        OR   (org.gautelis.repo.search.model.Operator.OR);

        private final org.gautelis.repo.search.model.Operator iptoOp;
        Logical(org.gautelis.repo.search.model.Operator iptoOp) {
            this.iptoOp = iptoOp;
        }

        public org.gautelis.repo.search.model.Operator iptoOp() {
            return iptoOp;
        }
    }

    public record TreeExpression(Logical op, Node left, Node right) {}

    public record Node(AttributeExpression attrExpr, TreeExpression treeExpr) {}

    public record Filter(int tenantId, Node where, int offset, int size) {}

    //
    enum SchemaOperation {QUERY, MUTATION}

    private final Repository repo;
    private final Map<String, SchemaOperation> operations;

    private ObjectMapper objectMapper = new ObjectMapper();

    /* package accessible only */
    OperationsConfigurator(Repository repo, Map<String, SchemaOperation> operations) {
        this.repo = repo;
        this.operations = operations;
    }

    /* package visible only */
    void load(
            ObjectTypeDefinition type,
            Map<String, Configurator.ExistingDatatypeMeta> datatypes,
            Map<String, Configurator.ProposedAttributeMeta> attributesSchemaView,
            Map<Integer, Configurator.ProposedAttributeMeta> attributesIptoView,
            RuntimeWiring.Builder runtimeWiring,
            RuntimeService repoService
    ) {
        SchemaOperation operation = operations.get(type.getName());
        if (operation != null) {
            switch (operation) {
                case QUERY -> loadQuery(type, datatypes, attributesSchemaView, attributesIptoView, runtimeWiring, repoService);
                case MUTATION -> loadMutation(type, datatypes, attributesSchemaView, attributesIptoView, runtimeWiring, repoService);
            }
        }
    }

    private void loadQuery(
            ObjectTypeDefinition type,
            Map<String, Configurator.ExistingDatatypeMeta> datatypes,
            Map<String, Configurator.ProposedAttributeMeta> attributesSchemaView,
            Map<Integer, Configurator.ProposedAttributeMeta> attributesIptoView,
            RuntimeWiring.Builder runtimeWiring,
            RuntimeService repoService
    ) {
        for (FieldDefinition f : type.getFieldDefinitions()) {
            // Operation name
            final String fieldName = f.getName();
            final TypeDefinition resultType = TypeDefinition.get(f.getType());

            List<InputValueDefinition> inputs = f.getInputValueDefinitions();

            if (!inputs.isEmpty()) {
                InputValueDefinition input = inputs.getFirst();
                String inputName = input.getName();
                TypeDefinition typeDef = TypeDefinition.get(input.getType());

                switch (typeDef.typeName()) {
                    // "Hardcoded" point lookup for specific unit
                    case "UnitIdentification" -> {
                        if (resultType.typeName().equals("Bytes")) {
                            DataFetcher<?> rawUnitById = env -> {
                                //**** Executed at runtime **********************************
                                // My mission in life is to resolve a specific query
                                // (the current 'fieldName').
                                // Everything needed at runtime is accessible right
                                // now so it is captured for later.
                                //***********************************************************
                                if (log.isTraceEnabled()) {
                                    log.trace("{}::{}(id : {}) : {}", type.getName(), fieldName, env.getArgument(inputName), resultType.typeName());
                                }

                                UnitIdentification id = objectMapper.convertValue(env.getArgument(inputName), UnitIdentification.class);
                                return repoService.loadRawUnit(id.tenantId(), id.unitId());
                            };

                            runtimeWiring.type(type.getName(), t -> t.dataFetcher(fieldName, rawUnitById));

                        } else {
                            DataFetcher<?> unitById = env -> {
                                //**** Executed at runtime **********************************
                                // My mission in life is to resolve a specific query
                                // (the current 'fieldName').
                                // Everything needed at runtime is accessible right
                                // now so it is captured for later.
                                //***********************************************************
                                if (log.isTraceEnabled()) {
                                    log.trace("{}::{}(id : {}) : {}", type.getName(), fieldName, env.getArgument(inputName), resultType.typeName());
                                }

                                UnitIdentification id = objectMapper.convertValue(env.getArgument(inputName), UnitIdentification.class);
                                return repoService.loadUnit(id.tenantId(), id.unitId());
                            };

                            runtimeWiring.type(type.getName(), t -> t.dataFetcher(fieldName, unitById));
                        }
                        log.info("Wiring: {}::{}(...) : {}", type.getName(), fieldName, resultType.typeName());
                    }

                    //
                    case "Filter" -> {
                        if (resultType.typeName().equals("Bytes")) {
                            DataFetcher<?> rawUnitsByFilter = env -> {
                                //**** Executed at runtime **********************************
                                // My mission in life is to resolve a specific query
                                // (the current 'fieldName').
                                // Everything needed at runtime is accessible right
                                // now so it is captured for later.
                                //***********************************************************
                                Map<String, Object> args = env.getArguments();
                                if (log.isTraceEnabled()) {
                                    log.trace("{}::{}({}) : {}", type.getName(), fieldName, args, resultType.typeName());
                                }

                                Filter filter = objectMapper.convertValue(env.getArgument(inputName), Filter.class);

                                return repoService.searchRaw(filter);
                            };

                            runtimeWiring.type(type.getName(), t -> t.dataFetcher(fieldName, rawUnitsByFilter));

                        } else {
                            DataFetcher<?> unitsByFilter = env -> {
                                //**** Executed at runtime **********************************
                                // My mission in life is to resolve a specific query
                                // (the current 'fieldName').
                                // Everything needed at runtime is accessible right
                                // now so it is captured for later.
                                //***********************************************************
                                Map<String, Object> args = env.getArguments();
                                if (log.isTraceEnabled()) {
                                    log.trace("{}::{}({}) : {}", type.getName(), fieldName, args, resultType.typeName());
                                }

                                Filter filter = objectMapper.convertValue(env.getArgument(inputName), Filter.class);

                                return repoService.search(filter);
                            };

                            runtimeWiring.type(type.getName(), t -> t.dataFetcher(fieldName, unitsByFilter));
                        }
                        log.info("Wiring: {}::{}(...) : {}", type.getName(), fieldName, resultType.typeName());
                    }
                }
            }
        }
    }

    private void loadMutation(
            ObjectTypeDefinition type,
            Map<String, Configurator.ExistingDatatypeMeta> datatypes,
            Map<String, Configurator.ProposedAttributeMeta> attributesSchemaView,
            Map<Integer, Configurator.ProposedAttributeMeta> attributesIptoView,
            RuntimeWiring.Builder runtimeWiring,
            RuntimeService repoService
    ) {
        for (FieldDefinition f : type.getFieldDefinitions()) {
            // Operation name
            final String fieldName = f.getName();
            final TypeDefinition resultType = TypeDefinition.get(f.getType());

            List<InputValueDefinition> inputs = f.getInputValueDefinitions();

            if (!inputs.isEmpty()) {
                InputValueDefinition input = inputs.getFirst();
                String inputName = input.getName();
                TypeDefinition typeDef = TypeDefinition.get(input.getType());

                switch (typeDef.typeName()) {
                     case "Bytes" -> {
                        if (resultType.typeName().equals("Bytes")) {
                            DataFetcher<?> storeRawUnit = env -> {
                                //**** Executed at runtime **********************************
                                // My mission in life is to resolve a specific query
                                // (the current 'fieldName').
                                // Everything needed at runtime is accessible right
                                // now so it is captured for later.
                                //***********************************************************
                                Map<String, Object> args = env.getArguments();
                                if (log.isTraceEnabled()) {
                                    log.trace("{}::{}({}) : {}", type.getName(), fieldName, args, resultType.typeName());
                                }

                                byte[] json = (byte[]) args.get(inputName);
                                return repoService.storeRawUnit(json);
                            };

                            runtimeWiring.type(type.getName(), t -> t.dataFetcher(fieldName, storeRawUnit));
                        }
                        log.info("Wiring: {}::{}(...) : {}", type.getName(), fieldName, resultType.typeName());
                    }
                }
            }
        }
    }
}