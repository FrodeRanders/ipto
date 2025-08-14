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

import com.fasterxml.uuid.Generators;
import graphql.ExecutionInput;
import graphql.ExecutionResult;
import graphql.GraphQL;
import graphql.GraphQLError;
import graphql.language.SourceLocation;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.gautelis.repo.exceptions.BaseException;
import org.gautelis.repo.model.Repository;
import org.gautelis.repo.model.Unit;
import org.gautelis.repo.model.associations.Type;
import org.gautelis.repo.model.attributes.Attribute;
import org.gautelis.repo.model.attributes.RecordAttribute;
import org.gautelis.repo.model.locks.Lock;
import org.gautelis.repo.model.utils.MovingAverage;
import org.gautelis.repo.search.model.*;
import org.gautelis.repo.search.query.DatabaseAdapter;
import org.gautelis.repo.search.query.QueryBuilder;
import org.gautelis.repo.search.query.SearchExpression;
import org.gautelis.repo.search.query.SearchOrder;
import org.gautelis.repo.search.UnitSearch;
import org.gautelis.vopn.lang.TimeDelta;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.runners.MethodSorters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

import static org.gautelis.repo.Statistics.dumpStatistics;


/**
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class RepositoryTest extends TestCase {
    private static final Logger log = LoggerFactory.getLogger(RepositoryTest.class);

    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public RepositoryTest(String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( RepositoryTest.class );
    }

    private static GraphQL graphQL = null;

    @Before
    public void setUp() throws IOException {
        if (null != graphQL) {
            return;
        }

        Optional<GraphQL> _graphQL;
        try (InputStreamReader sdl = new InputStreamReader(
                Objects.requireNonNull(RepositoryTest.class.getResourceAsStream("unit-schema.graphqls"))
        )) {
            Repository repo = RepositoryFactory.getRepository();
            _graphQL = repo.loadConfiguration(sdl);
            if (_graphQL.isEmpty()) {
                fail("Could not load configuration");
            }
        }

        graphQL = _graphQL.get();
    }

    private long createUnit(int tenantId, String aSpecificString, Instant aSpecificInstant) {
        Repository repo = RepositoryFactory.getRepository();
        Unit unit = repo.createUnit(tenantId, "graphql 'unit' test");

        unit.withAttributeValue("dc:title", String.class, value -> {
            value.add("abc");
        });

        unit.withAttribute("SHIPMENT", Attribute.class, attr -> {
            RecordAttribute recrd = new RecordAttribute(attr);

            recrd.withNestedAttributeValue(unit, "ORDER_ID", String.class, value -> {
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
        dump(root, 0);
        System.out.println();
    }

    private void dump(Object obj, int depth) {
        String indent = "  ".repeat(depth);

        if (obj instanceof Map<?, ?> map) {
            map.forEach((k, v) -> {
                System.out.println();
                System.out.print(indent + k + ":");
                dump(v, depth + 1);
            });

        } else if (obj instanceof Collection<?> col) {
            System.out.println();
            System.out.print(indent + "[");
            for (Object item : col) {
                dump(item, depth + 1);
                System.out.println();
                System.out.print(indent + ",");
            }
            System.out.println();
            System.out.print(indent + "]");

        } else {
            System.out.print(" " + obj);
        }
    }

    public void test1GraphQL() {
        final int tenantId = 1;
        final long unitId = createUnit(tenantId, "*order id 1*", Instant.now());

        String query = """
            query Unit($id: UnitIdentification!) {
              order(id: $id) {
                shipment {
                    orderId
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

    public void test2GraphQL() {
        final int tenantId = 1;
        final long unitId = createUnit(tenantId, "*order id 2*", Instant.now());

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

    public void test3GraphQL() {
        final int tenantId = 1;
        final String specificString = "*order id 3*";
        final long _unitId = createUnit(tenantId, specificString, Instant.now());

        String query = """
            query Units($filter: Filter!) {
              orders(filter: $filter) {
                edges {
                  shipment {
                    orderId
                    deadline
                  }
                }
                pageInfo {
                  hasNextPage
                  hasPreviousPage
                  startCursor
                  endCursor
                }
              }
            }
            """;
        log.info(query);
        System.out.println(query);

        Map<String, Object> where = Map.of(
            "attrExpr", Map.of(
                        "attr", "ORDER_ID",
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

    public void test4GraphQL() {
        final int tenantId = 1;
        final String specificString = "*order id 3*";
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
                        "attr", "ORDER_ID",
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

    @SuppressWarnings("unchecked")
    public void test5Record() {
        Repository repo = RepositoryFactory.getRepository();
        final int tenantId = 1;

        Unit unit = repo.createUnit(tenantId, "a record instance");

        unit.withAttribute("SHIPMENT", Attribute.class, attr -> {
            RecordAttribute recrd = new RecordAttribute(attr);

            recrd.withNestedAttributeValue(unit, "ORDER_ID", String.class, value -> {
                value.add("*order id*");
            });

            recrd.withNestedAttributeValue(unit, "DEADLINE", Instant.class, value -> {
                value.add(Instant.now());
            });

            recrd.withNestedAttributeValue(unit, "READING", Double.class, value -> {
                value.add(Math.PI);
                value.add(Math.E);
            });
        });

        unit.withAttributeValue("dc:title", String.class, value -> {
            value.add("Handling of " + "*order id*");
        });

        repo.storeUnit(unit);
    }

    public void test6Repository() {
        Repository repo = RepositoryFactory.getRepository();

        final int tenantId = 1; // For the sake of exercising, this is the tenant of units we will create

        final int numberOfParents = 5000; //
        final int numberOfChildren = 100; //

        try {
            Instant firstParentCreated = null;
            Instant someInstant = null;
            String someSpecificString = Generators.timeBasedEpochGenerator().generate().toString();
            int numberOfUnitsToHaveSpecificString = 1;

            // With resultset paging, we want to skip the first 'pageOffset' results and
            // acquire the 'pageSize' next results. 'pageOffset' and 'pageSize' has precedence over
            // 'selectionSize' (which counts from the first result).
            int pageOffset = 5;  // skip 'pageOffset' first results
            int pageSize = 5;    // pick next 'pageSize' results

            System.out.println("Generating " + (numberOfParents * numberOfChildren) + " units...");
            System.out.flush();

            MovingAverage averageTPI = new MovingAverage(); // Average timer per iteration

            for (int j = 1; j < numberOfParents + 1; j++) {
                Instant startTime = Instant.now();

                Unit parentUnit = repo.createUnit(tenantId, "parent-" + j);
                parentUnit.withAttributeValue("dc:title", String.class, value -> {
                    value.add("First value");
                    value.add("Second value");
                    value.add("Third value");
                });
                repo.storeUnit(parentUnit);

                averageTPI.update(startTime, /* endTime */ Instant.now());

                if (null == firstParentCreated) {
                    firstParentCreated = parentUnit.getCreationTime().get();
                }

                if (false) {
                    // Works, but not part of test (at the moment)
                    repo.lockUnit(parentUnit, Lock.Type.EXISTENCE, "test");
                }

                for (int i = 1; i < numberOfChildren + 1; i++) {
                    long count = (j-1)*numberOfChildren + i;
                    if (count % 1000 == 0) { // depends on parameters (numberOfParents and numberOfChildren) > 1000
                        long iterationsLeft = (numberOfParents * numberOfChildren) - count;
                        double timeLeft = iterationsLeft * averageTPI.getAverage();

                        String approxTimeLeft = TimeDelta.asHumanApproximate(BigInteger.valueOf(Math.round(timeLeft)));
                        System.out.print(
                                "\r  " + count
                                + "   [ETL: "
                                + approxTimeLeft + "]                         \r"); // trailing needed
                        System.out.flush();
                    }

                    startTime = Instant.now();
                    Unit childUnit = repo.createUnit(tenantId, "child-" + j + "-" + i);

                    //
                    parentUnit.withAttribute("dc:title", String.class, attr -> {
                        try {
                            childUnit.addAttribute(attr);
                        } catch (BaseException be) {
                            System.err.println("Failed to add attribute: " + be.getMessage());
                        }
                    });

                    final Instant now = Instant.now();

                    childUnit.withAttributeValue("dc:date", Instant.class, value -> {
                        value.add(now);
                    });

                    childUnit.withAttribute("SHIPMENT", Attribute.class, attr -> {
                        RecordAttribute recrd = new RecordAttribute(attr);

                        recrd.withNestedAttributeValue(childUnit, "ORDER_ID", String.class, value -> {
                            value.add("*order id*");
                        });

                        recrd.withNestedAttributeValue(childUnit, "DEADLINE", Instant.class, value -> {
                            value.add(now);
                        });

                        recrd.withNestedAttributeValue(childUnit, "READING", Double.class, value -> {
                            value.add(Math.PI);
                            value.add(Math.E);
                        });
                    });

                    // Some unique unit will get unique attribute
                    if (/* i within page, that we will search for later down under */
                            i > pageOffset && i == pageOffset + pageSize && numberOfUnitsToHaveSpecificString-- > 0
                    ) {
                        childUnit.withAttributeValue("dc:title", String.class, value -> {
                            value.add(someSpecificString);
                        });

                        someInstant = now.minus(Duration.ofHours(2)); // For testing purposes
                    }

                    //
                    repo.storeUnit(childUnit);
                    averageTPI.update(startTime, /* endTime */ Instant.now());

                    if (false) {
                        // Works, but not part of test (at the moment)
                        // Add a relation to parent unit
                        repo.addRelation(parentUnit, Type.PARENT_CHILD_RELATION, childUnit);
                    }

                }
                System.out.flush();

                if (false) {
                    System.out.println("Children of " + parentUnit.getName().orElse("parent") + " (" + parentUnit.getReference() + "):");
                    parentUnit.getRelations(Type.PARENT_CHILD_RELATION).forEach(
                            relatedUnit -> System.out.println("  " + relatedUnit.getName().orElse("child") + " (" + relatedUnit.getReference() + ")")
                    );
                }
            }

            // In order to search here in test, we will need some "internal" objects,
            // such as Context, DataSource, etc.
            {
                // Unit constraints
                SearchExpression expr = QueryBuilder.constrainToSpecificTenant(tenantId);
                expr = QueryBuilder.assembleAnd(expr, QueryBuilder.constrainToSpecificStatus(Unit.Status.EFFECTIVE));
                expr = QueryBuilder.assembleAnd(expr, QueryBuilder.constrainToCreatedAfter(firstParentCreated));

                // First attribute constraint
                Optional<Integer> _timeAttributeId = repo.attributeNameToId("dc:date");
                int[] timeAttributeId = { 0 };
                _timeAttributeId.ifPresent(attrId -> timeAttributeId[0] = attrId);

                SearchItem<Instant> timestampSearchItem = new TimeAttributeSearchItem(timeAttributeId[0], Operator.GEQ, someInstant);
                expr = QueryBuilder.assembleAnd(expr, timestampSearchItem);

                // Second attribute constraint
                Optional<Integer> _stringAttributeId = repo.attributeNameToId("dc:title");
                int[] stringAttributeId = { 0 };
                _stringAttributeId.ifPresent(attrId -> stringAttributeId[0] = attrId);

                SearchItem<String> stringSearchItem = new StringAttributeSearchItem(stringAttributeId[0], Operator.EQ, someSpecificString);
                expr = QueryBuilder.assembleAnd(expr, stringSearchItem);

                // Result set constraints (paging)
                SearchOrder order = SearchOrder.getDefaultOrder(); // descending on creation time
                UnitSearch usd = new UnitSearch(expr, order, /* selectionSize */ 5);

                // Build SQL statement for search
                DatabaseAdapter searchAdapter = repo.getDatabaseAdapter();

                Collection<Unit.Id> unitId = new ArrayList<>();
                repo.withConnection(conn -> searchAdapter.search(conn, usd, repo.getTimingData(), rs -> {
                    while (rs.next()) {
                        int j = 0;
                        int _tenantId = rs.getInt(++j);
                        long _unitId = rs.getLong(++j);
                        Timestamp _created = rs.getTimestamp(++j);

                        System.out.println("\nFound: tenantId=" + _tenantId + " unitId=" + _unitId + " created=" + _created);
                        unitId.add(new Unit.Id(_tenantId, _unitId));
                    }
                }));

                if (unitId.isEmpty()) {
                    fail("Failed to find known unit corresponding to search");
                } else {
                    for (Unit.Id id : unitId) {
                        try {
                            Optional<Unit> _unit = repo.getUnit(id.tenantId(), id.unitId());
                            if (_unit.isPresent()) {
                                Unit unit = _unit.get();
                                System.out.println(unit);
                                System.out.println("JSON: " + unit.asJson(/* complete? */ true, /* pretty? */ true, /* flat? */ false));
                            } else {
                                fail("Failed to resurrect unit that is known to exist");
                            }
                        } catch (Throwable t) {
                            log.error(t.getMessage(), t);
                        }
                    }
                }
            }

            dumpStatistics(repo);

        } catch (Throwable t) {
            String info = t.getMessage();
            log.error(info, t);

            fail(info);
        }
    }
}
