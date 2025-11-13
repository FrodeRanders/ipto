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
import com.fasterxml.uuid.Generators;
import graphql.ExecutionInput;
import graphql.ExecutionResult;
import graphql.GraphQLError;
import graphql.language.SourceLocation;

import org.gautelis.repo.model.Repository;
import org.gautelis.repo.model.Unit;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.fail;

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
        final String orderId = Generators.timeBasedEpochGenerator().generate().toString(); // UUID v7

        unit.withAttributeValue("dce:title", String.class, value -> {
            value.add("abc");
        });

        unit.withAttributeValue("dmo:orderId", String.class, value -> {
            value.add(orderId);
        });

        unit.withRecordAttribute("dmo:shipment", recrd -> {
            recrd.withNestedAttributeValue(unit, "dmo:shipmentId", String.class, value -> {
                value.add(aSpecificString);
            });

            recrd.withNestedAttributeValue(unit, "dmo:deadline", Instant.class, value -> {
                value.add(aSpecificInstant);
            });

            recrd.withNestedAttributeValue(unit, "dmo:reading", Double.class, value -> {
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
    public void retrieveWithInlineLiteral() {
        final String shipmentId1 = Generators.timeBasedEpochGenerator().generate().toString();
        final String shipmentId2 = Generators.timeBasedEpochGenerator().generate().toString();

        final int tenantId = 1;
        final long unitId1 = createUnit(tenantId, shipmentId1, Instant.now());
        final long unitId2 = createUnit(tenantId, shipmentId2, Instant.now());

        String query = """
            query Unit {
              order1: order(id: { tenantId: %d, unitId: %d }) {
                orderId
                shipment {
                  shipmentId
                  deadline
                  reading
                }
              }
              order2: order(id: { tenantId: %d, unitId: %d }) {
                orderId
                shipment {
                  shipmentId
                  deadline
                  reading
                }
              }
            }
            """;
        query = String.format(query, tenantId, unitId1, tenantId, unitId2);

        System.out.println("--------------------------------------------------------------");
        log.info(query);
        System.out.println(query);

        ExecutionResult result = graphQL.execute(
                ExecutionInput.newExecutionInput()
                        .query(query)
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
    @Order(2)
    public void retrieveUsingVariable() {
        final String shipmentId = Generators.timeBasedEpochGenerator().generate().toString(); // UUID v7

        final int tenantId = 1;
        final long unitId = createUnit(tenantId, shipmentId, Instant.now());

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
        final String shipmentId = Generators.timeBasedEpochGenerator().generate().toString(); // UUID v7

        final int tenantId = 1;
        final long unitId = createUnit(tenantId, shipmentId, Instant.now());

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
            if (null != b64) {
                final Base64.Decoder DEC = Base64.getDecoder();
                String json = new String(DEC.decode(b64.getBytes(StandardCharsets.UTF_8)), StandardCharsets.UTF_8);

                log.info("Result (base64 encoded): {}", b64);
                log.info("Result (String/JSON): {}", json);

                System.out.print("--> ");
                System.out.println(json);
            } else {
                fail("No search results found (where it is expected)");
            }
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
        final String shipmentId = Generators.timeBasedEpochGenerator().generate().toString(); // UUID v7

        final int tenantId = 1;
        final long _unitId = createUnit(tenantId, shipmentId, Instant.now());

        String query = """
            query Orders($filter: Filter!) {
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
                        "attr", "shipmentId", // OBS! Expressed using GQL name
                        "op", "EQ",
                        "value", shipmentId
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
    public void searchWithInlineLiteral() {
        final String shipmentId1 = Generators.timeBasedEpochGenerator().generate().toString();
        final String shipmentId2 = Generators.timeBasedEpochGenerator().generate().toString();

        final int tenantId = 1;
        final long _unitId1 = createUnit(tenantId, shipmentId1, Instant.now());
        final long _unitId2 = createUnit(tenantId, shipmentId2, Instant.now());

        String query = """
            query Shipments {
              shipment1: orders(filter: {tenantId: %d, where: {attrExpr: {attr: shipmentId, op: EQ, value: "%s"}}}) {
                shipment {
                    deadline
                }
              }
              shipment2: orders(filter: {tenantId: %d, where: {attrExpr: {attr: shipmentId, op: EQ, value: "%s"}}}) {
                shipment {
                    deadline
                }
              }
            }
            """;

        query = String.format(query, tenantId, shipmentId1, tenantId, shipmentId2);

        log.info(query);
        System.out.println(query);

        ExecutionResult result = graphQL.execute(
                ExecutionInput.newExecutionInput()
                        .query(query)
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
    @Order(6)
    public void searchRaw() {
        final String shipmentId = Generators.timeBasedEpochGenerator().generate().toString(); // UUID v7

        final int tenantId = 1;
        final long _unitId = createUnit(tenantId, shipmentId, Instant.now());

        String query = """
            query Orders($filter: Filter!) {
              ordersRaw(filter: $filter)
            }
            """;
        log.info(query);
        System.out.println(query);

        Map<String, Object> where = Map.of(
                "attrExpr", Map.of(
                        "attr", "shipmentId", // OBS! Expressed using GQL name
                        "op", "EQ",
                        "value", shipmentId
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
            if (null != b64) {
                final Base64.Decoder DEC = Base64.getDecoder();
                String json = new String(DEC.decode(b64.getBytes(StandardCharsets.UTF_8)), StandardCharsets.UTF_8);

                log.info("Result (base64 encoded): {}", b64);
                log.info("Result (String/JSON): {}", json);

                System.out.print("--> ");
                System.out.println(json);
            } else {
                fail("No search results found (where it is expected)");
            }
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
