/*
 * Copyright (C) 2024-2025 Frode Randers
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
package org.gautelis.repo;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import graphql.ExecutionInput;
import graphql.ExecutionResult;
import graphql.GraphQLError;
import graphql.language.SourceLocation;
import org.gautelis.repo.graphql2.configuration.Configurator;
import org.gautelis.repo.graphql2.model.IntRep;
import org.gautelis.repo.model.Repository;
import org.gautelis.repo.model.Unit;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 *
 */
@Tag("GraphQL")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class GraphQLTest {
    private static final Logger log = LoggerFactory.getLogger(GraphQLTest.class);

    ObjectMapper MAPPER = new ObjectMapper();

    private static graphql.GraphQL graphQL = null;

    @BeforeAll
    public static void setUp() throws IOException {
        graphQL = CommonSetup.setUp();
    }

    private long createUnit(int tenantId, String aSpecificString, Instant aSpecificInstant) {
        Repository repo = RepositoryFactory.getRepository();
        Unit unit = repo.createUnit(tenantId, "graphql 'unit' test");

        unit.withAttributeValue("dc:title", String.class, value -> {
            value.add("abc");
        });

        unit.withAttributeValue("ORDER_ID", String.class, value -> {
            value.add("*some order id*");
        });

        unit.withRecordAttribute("SHIPMENT", recrd -> {
            recrd.withNestedAttributeValue(unit, "SHIPMENT_ID", String.class, value -> {
                value.add(aSpecificString);
            });

            recrd.withNestedAttributeValue(unit, "DEADLINE", Instant.class, value -> {
                value.add(aSpecificInstant);
            });

            recrd.withNestedAttributeValue(unit, "READING", Double.class, value -> {
                value.add(Math.PI);
                value.add(Math.E);
            });
        });

        repo.storeUnit(unit);
        return unit.getUnitId();
    }

    private void dumpMap(Map<String, ?> root) {
        try {
            dump(root, 0);
            System.out.println();
        } catch (Exception e) {
            System.err.println("Error dumping map: " + e.getMessage());
        }
    }

    private void dump(Object obj, int depth) throws Exception {
        JsonNode node = MAPPER.valueToTree(obj);
        System.out.println(MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(node));
    }

    @Test
    @Order(1)
    public void configuration() throws IOException {
        try (InputStreamReader sdl = new InputStreamReader(
                Objects.requireNonNull(GraphQLTest.class.getResourceAsStream("unit-schema.graphqls"))
        )) {
            IntRep graphQLIr = Configurator.loadFromFile(sdl);
            graphQLIr.dumpIr("GraphQL SDL", System.out);

            Repository repo = RepositoryFactory.getRepository();
            Configurator.loadFromCatalog(repo);

            /*
            IntRep iptoIr = Configurator.loadFromDB(repo);
            dumpIr("IPTO", iptoIr);
            */

            //Repository repo = RepositoryFactory.getRepository();
            //Configurator.reconcile(sdl, repo);
        }
    }

    @Test
    @Order(2)
    public void retrieve() {
        final int tenantId = 1;
        final long unitId = createUnit(tenantId, "*shipment id 1*", Instant.now());

        String query = """
            query Unit($id: UnitIdentification!) {
              order(id: $id) {
                orderId
                shipment {
                    shipmentId
                    deadline
                    reading
                }
              }
            }
            """;
        System.out.println("--------------------------------------------------------------");
        log.info(query);
        System.out.println(query);

        ExecutionResult result = graphQL.execute(
                ExecutionInput.newExecutionInput()
                        .query(query)
                        .variables(Map.of(
                            "id", Map.of(
                                 "tenantId", tenantId,
                                 "unitId",   unitId
                                 )
                            )
                        )
                        .build());

        List<GraphQLError> errors = result.getErrors();
        if (errors.isEmpty()) {
            log.info("Result: {}", (Object) result.getData());

            System.out.print("--> ");
            dumpMap(result.getData());

        } else {
            for (GraphQLError error : errors) {
                log.error("error: {}: {}", error.getMessage(), error);

                List<SourceLocation> locations = error.getLocations();
                if (null != locations) {
                    for (SourceLocation location : locations) {
                        log.error("location: {}: {}", location.getLine(), location);
                    }
                }
            }
        }
        System.out.println("--------------------------------------------------------------");
    }

    @Test
    @Order(3)
    public void retrieveRaw() {
        final int tenantId = 1;
        final long unitId = createUnit(tenantId, "*shipment id 2*", Instant.now());

        String query = """
            query Unit($id: UnitIdentification!) {
              orderRaw(id: $id)
            }
            """;
        log.info(query);
        System.out.println(query);

        ExecutionResult result = graphQL.execute(
                ExecutionInput.newExecutionInput()
                        .query(query)
                        .variables(Map.of(
                                        "id", Map.of(
                                                "tenantId", tenantId,
                                                "unitId",   unitId
                                        )
                                )
                        )
                        .build());


        List<GraphQLError> errors = result.getErrors();
        if (errors.isEmpty()) {
            Map<String, String> map = result.getData();
            String b64 = map.get("orderRaw"); // name same as fieldname in query type

            final Base64.Decoder DEC = Base64.getDecoder();
            String json = new String(DEC.decode(b64.getBytes(StandardCharsets.UTF_8)), StandardCharsets.UTF_8);

            log.info("Result (base64 encoded): {}", b64);
            log.info("Result (String/JSON): {}", json);

            System.out.print("--> ");
            System.out.println(json);

        } else {
            for (GraphQLError error : errors) {
                log.error("error: {}: {}", error.getMessage(), error);

                List<SourceLocation> locations = error.getLocations();
                if (null != locations) {
                    for (SourceLocation location : locations) {
                        log.error("location: {}: {}", location.getLine(), location);
                    }
                }
            }
        }
        System.out.println("--------------------------------------------------------------");
    }

    @Test
    @Order(4)
    public void search() {
        final int tenantId = 1;
        final String specificString = "*shipment id 3*";
        final long _unitId = createUnit(tenantId, specificString, Instant.now());

        String query = """
            query Units($filter: Filter!) {
              orders(filter: $filter) {
                shipment {
                    deadline
                }
              }
            }
            """;
        log.info(query);
        System.out.println(query);

        Map<String, Object> where = Map.of(
            "attrExpr", Map.of(
                        "attr", "SHIPMENT_ID",
                        "op", "EQ",
                        "value", specificString
                    )
        );

        ExecutionResult result = graphQL.execute(
                ExecutionInput.newExecutionInput()
                        .query(query)
                        .variables(
                                Map.of(
                                        "filter", Map.of(
                                                "tenantId", 1,
                                                "where", where
                                        )
                                )
                        )
                        .build());

        List<GraphQLError> errors = result.getErrors();
        if (errors.isEmpty()) {
            log.info("Result: {}", (Object) result.getData());

            System.out.print("--> ");
            dumpMap(result.getData());

        } else {
            for (GraphQLError error : errors) {
                log.error("error: {}: {}", error.getMessage(), error);

                List<SourceLocation> locations = error.getLocations();
                if (null != locations) {
                    for (SourceLocation location : locations) {
                        log.error("location: {}: {}", location.getLine(), location);
                    }
                }
            }
        }
        System.out.println("--------------------------------------------------------------");
    }

    @Test
    @Order(5)
    public void searchRaw() {
        final int tenantId = 1;
        final String specificString = "*shipment id 4*";
        final long _unitId = createUnit(tenantId, specificString, Instant.now());

        String query = """
            query Units($filter: Filter!) {
              ordersRaw(filter: $filter)
            }
            """;
        log.info(query);
        System.out.println(query);

        Map<String, Object> where = Map.of(
                "attrExpr", Map.of(
                        "attr", "SHIPMENT_ID",
                        "op", "EQ",
                        "value", specificString
                )
        );

        ExecutionResult result = graphQL.execute(
                ExecutionInput.newExecutionInput()
                        .query(query)
                        .variables(
                                Map.of(
                                        "filter", Map.of(
                                                "tenantId", 1,
                                                "where", where
                                        )
                                )
                        )
                        .build());

        List<GraphQLError> errors = result.getErrors();
        if (errors.isEmpty()) {
            Map<String, String> map = result.getData();
            String b64 = map.get("ordersRaw"); // name same as fieldname in query type

            final Base64.Decoder DEC = Base64.getDecoder();
            String json = new String(DEC.decode(b64.getBytes(StandardCharsets.UTF_8)), StandardCharsets.UTF_8);

            log.info("Result (base64 encoded): {}", b64);
            log.info("Result (String/JSON): {}", json);

            System.out.print("--> ");
            System.out.println(json);

        } else {
            for (GraphQLError error : errors) {
                log.error("error: {}: {}", error.getMessage(), error);

                List<SourceLocation> locations = error.getLocations();
                if (null != locations) {
                    for (SourceLocation location : locations) {
                        log.error("location: {}: {}", location.getLine(), location);
                    }
                }
            }
        }
        System.out.println("--------------------------------------------------------------");
    }
}
