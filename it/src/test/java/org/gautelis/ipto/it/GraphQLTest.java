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
package org.gautelis.ipto.it;

import com.fasterxml.uuid.Generators;
import graphql.ExecutionInput;
import graphql.ExecutionResult;
import graphql.GraphQL;
import graphql.GraphQLError;
import graphql.language.SourceLocation;
import org.gautelis.ipto.repo.RepositoryFactory;
import org.gautelis.ipto.repo.model.Repository;
import org.gautelis.ipto.repo.model.Unit;
import org.gautelis.ipto.repo.model.attributes.Attribute;
import org.gautelis.ipto.repo.model.attributes.RecordAttribute;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;

import static org.junit.jupiter.api.Assertions.fail;

/**
 *
 */
@Tag("GraphQL")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(GraphQLSetupExtension.class)
public class GraphQLTest {
    private static final Logger log = LoggerFactory.getLogger(GraphQLTest.class);

    private final ObjectMapper MAPPER = new ObjectMapper();

    private long createUnit(int tenantId, String aSpecificString, Instant aSpecificInstant) {
        Repository repo = RepositoryFactory.getRepository();
        final UUID processId = Generators.timeBasedEpochGenerator().generate(); // UUID v7

        Unit yrkan = repo.createUnit(tenantId, processId);

        yrkan.withRecordAttribute("ffa:fysisk_person", person -> {
            person.withNestedAttributeValue("ffa:personnummer", String.class, value -> {
                value.add("19121212-1212");
            });
        });

        yrkan.withAttributeValue("dce:description", String.class, value -> {
            value.add("Yrkan om vård av husdjur");
        });

        yrkan.withAttributeValue("ffa:producerade_resultat", Attribute.class, resultat -> {
            Optional<Attribute<?>> rattenTill = repo.instantiateAttribute("ffa:ratten_till_period");
            if (rattenTill.isPresent()) {
                RecordAttribute rattenTillRecord = RecordAttribute.from(yrkan, rattenTill.get());

                rattenTillRecord.withNestedAttributeValue("ffa:ersattningstyp", String.class, ersattningstyp -> {
                    ersattningstyp.add("HUNDBIDRAG");
                });

                rattenTillRecord.withNestedAttributeValue("ffa:omfattning", String.class, omfattning -> {
                    omfattning.add("HEL");
                });

                Attribute<?> attribute = rattenTill.get();
                resultat.add(attribute);
            }

            Optional<Attribute<?>> ersattning = repo.instantiateAttribute("ffa:ersattning");
            if (ersattning.isPresent()) {
                RecordAttribute ersattningRecord = RecordAttribute.from(yrkan, ersattning.get());

                ersattningRecord.withNestedAttributeValue("ffa:ersattningstyp", String.class, value -> {
                    value.add("HUNDBIDRAG");
                });

                ersattningRecord.withNestedAttribute("ffa:belopp", Attribute.class, belopp -> {
                    RecordAttribute beloppRecord = RecordAttribute.wrap(yrkan, belopp);

                    beloppRecord.withNestedAttributeValue("ffa:beloppsvarde", Double.class, beloppsvarde -> {
                        beloppsvarde.add(1000.0);
                    });
                    beloppRecord.withNestedAttributeValue("ffa:beloppsperiodisering", String.class, beloppsperiodisering -> {
                        beloppsperiodisering.add("PER_DAG");
                    });
                    beloppRecord.withNestedAttributeValue("ffa:valuta", String.class, valuta -> {
                        valuta.add("SEK");
                    });
                    beloppRecord.withNestedAttributeValue("ffa:skattestatus", String.class, skattestatus -> {
                        skattestatus.add("SKATTEPLIKTIG");
                    });
                });

                ersattningRecord.withNestedAttribute("ffa:period", Attribute.class, period -> {
                    RecordAttribute periodRecord = RecordAttribute.wrap(yrkan, period);

                    periodRecord.withNestedAttributeValue("ffa:from", Instant.class, value -> {
                        value.add(Instant.now());
                    });
                    periodRecord.withNestedAttributeValue("ffa:tom", Instant.class, value -> {
                        value.add(Instant.now());
                    });
                });

                resultat.add(ersattningRecord.getDelegate());
            }
        });

        yrkan.withRecordAttribute("ffa:beslut", beslut -> {
            beslut.withNestedAttributeValue("dce:date", Instant.class, datum -> {
                datum.add(aSpecificInstant);
            });

            beslut.withNestedAttributeValue("ffa:beslutsfattare", String.class, beslutsfattare -> {
                beslutsfattare.add(aSpecificString);
            });

            beslut.withNestedAttributeValue("ffa:beslutstyp", String.class, beslutstyp -> {
                beslutstyp.add("SLUTLIGT");
            });

            beslut.withNestedAttributeValue("ffa:beslutsutfall", String.class, beslutsutfall -> {
                beslutsutfall.add("BEVILJAT");
            });

            beslut.withNestedAttributeValue("ffa:organisation", String.class, organisation -> {
                organisation.add("Myndigheten");
            });

            beslut.withNestedAttributeValue("ffa:lagrum", String.class, lagrum -> {
                lagrum.add("FL_P38");
            });

            beslut.withNestedAttributeValue("ffa:avslagsanledning", String.class, avslagsanledning -> {
                // Ingen
            });
        });

        repo.storeUnit(yrkan);
        return yrkan.getUnitId();
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
    public void retrieveWithInlineLiteral(GraphQL graphQL) {
        final String beslutsfattare1 = Generators.timeBasedEpochGenerator().generate().toString();
        final String beslutsfattare2 = Generators.timeBasedEpochGenerator().generate().toString();

        final int tenantId = 1;
        final long unitId1 = createUnit(tenantId, beslutsfattare1, Instant.now());
        final long unitId2 = createUnit(tenantId, beslutsfattare2, Instant.now());

        String query = """
            query Unit {
              yrkan1: yrkan(id: { tenantId: %d, unitId: %d }) {
                person {
                  ... on FysiskPerson {
                    personnummer
                  }
                  ... on JuridiskPerson {
                    orgnummer
                  }
                }
                producerade_resultat {
                  ... on Ersattning {
                    ersattningstyp
                    belopp {
                      beloppsvarde
                    }
                  }
                }
              }
            
              yrkan2: yrkan(id: { tenantId: %d, unitId: %d }) {
                person {
                  ... on FysiskPerson {
                    personnummer
                  }
                  ... on JuridiskPerson {
                    orgnummer
                  }
                }
                beslut {
                  datum
                  beslutsfattare
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
    public void retrieveUsingVariable(GraphQL graphQL) {
        final String shipmentId = Generators.timeBasedEpochGenerator().generate().toString(); // UUID v7

        final int tenantId = 1;
        final long unitId = createUnit(tenantId, shipmentId, Instant.now());

        String query = """
            query Unit($id: UnitIdentification!) {
              yrkan(id: $id) {
                person {
                  ... on FysiskPerson {
                    personnummer
                  }
                  ... on JuridiskPerson {
                    orgnummer
                  }
                }
                producerade_resultat {
                  ... on Ersattning {
                     ersattningstyp
                     belopp {
                       beloppsvarde
                     }
                  }
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
    public void retrieveRaw(GraphQL graphQL) {
        final String beslutsfattare = Generators.timeBasedEpochGenerator().generate().toString(); // UUID v7

        final int tenantId = 1;
        final long unitId = createUnit(tenantId, beslutsfattare, Instant.now());

        String query = """
            query Unit($id: UnitIdentification!) {
              yrkanRaw(id: $id)
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
            String b64 = map.get("yrkanRaw"); // OBS! name same as fieldname in query type
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
    public void search(GraphQL graphQL) {
        final String beslutsfattare = Generators.timeBasedEpochGenerator().generate().toString(); // UUID v7

        final int tenantId = 1;
        final long _unitId = createUnit(tenantId, beslutsfattare, Instant.now());

        String query = """
            query Unit($filter: Filter!) {
              yrkanden(filter: $filter) {
                person {
                  ... on FysiskPerson {
                    personnummer
                  }
                  ... on JuridiskPerson {
                    orgnummer
                  }
                }
                producerade_resultat {
                  ... on Ersattning {
                     ersattningstyp
                     belopp {
                       beloppsvarde
                     }
                  }
                }
                beslut {
                  datum
                  beslutsfattare
                }
              }
            }
            """;
        log.info(query);
        System.out.println(query);

        Map<String, Object> where = Map.of(
            "attrExpr", Map.of(
                        "attr", "beslutsfattare", // OBS! Expressed using GQL name
                        "op", "EQ",
                        "value", beslutsfattare
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
    public void searchWithInlineLiteral(GraphQL graphQL) {
        final String beslutsfattare1 = Generators.timeBasedEpochGenerator().generate().toString();
        final String beslutsfattare2 = Generators.timeBasedEpochGenerator().generate().toString();

        final int tenantId = 1;
        final long _unitId1 = createUnit(tenantId, beslutsfattare1, Instant.now());
        final long _unitId2 = createUnit(tenantId, beslutsfattare2, Instant.now());

        String query = """
            query Unit {
              yrkanden1: yrkanden(filter: {tenantId: %d, where: {attrExpr: {attr: beslutsfattare, op: EQ, value: "%s"}}}) {
                person {
                  ... on FysiskPerson {
                    personnummer
                  }
                  ... on JuridiskPerson {
                    orgnummer
                  }
                }
              }
              yrkanden2: yrkanden(filter: {tenantId: %d, where: {attrExpr: {attr: beslutsfattare, op: EQ, value: "%s"}}}) {
                person {
                  ... on FysiskPerson {
                    personnummer
                  }
                  ... on JuridiskPerson {
                    orgnummer
                  }
                }
              }
            }
            """;

        query = String.format(query, tenantId, beslutsfattare1, tenantId, beslutsfattare2);

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
    public void searchRaw(GraphQL graphQL) {
        final String beslutsfattare = Generators.timeBasedEpochGenerator().generate().toString(); // UUID v7

        final int tenantId = 1;
        final long _unitId = createUnit(tenantId, beslutsfattare, Instant.now());

        String query = """
            query Unit($filter: Filter!) {
              yrkandenRaw(filter: $filter)
            }
            """;
        log.info(query);
        System.out.println(query);

        Map<String, Object> where = Map.of(
                "attrExpr", Map.of(
                        "attr", "beslutsfattare", // OBS! Expressed using GQL name
                        "op", "EQ",
                        "value", beslutsfattare
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
            String b64 = map.get("yrkandenRaw"); // OBS! name same as fieldname in query type
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
