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
import org.gautelis.repo.model.attributes.CompoundAttribute;
import org.gautelis.repo.model.locks.Lock;
import org.gautelis.repo.model.utils.MovingAverage;
import org.gautelis.repo.search.model.*;
import org.gautelis.repo.search.query.DatabaseAdapter;
import org.gautelis.repo.search.query.QueryBuilder;
import org.gautelis.repo.search.query.SearchExpression;
import org.gautelis.repo.search.query.SearchOrder;
import org.gautelis.repo.search.UnitSearch;
import org.gautelis.vopn.lang.TimeDelta;
import org.junit.FixMethodOrder;
import org.junit.runners.MethodSorters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigInteger;
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

    public void test1ConfigLoad() throws IOException {
        try (InputStreamReader sdl = new InputStreamReader(
                Objects.requireNonNull(RepositoryTest.class.getResourceAsStream("unit-schema.graphqls"))
        )) {
            Repository repo = RepositoryFactory.getRepository();
            Optional<GraphQL> _graphQL = repo.loadConfiguration(sdl);
            if (_graphQL.isEmpty()) {
                fail("Could not load configuration");
            }

            final int tenantId = 1;
            long unitId;
            {

                Unit unit = repo.createUnit(tenantId, "graphql test");

                unit.withAttribute("dc:title", String.class, attr -> {
                    ArrayList<String> value = attr.getValue();
                    value.add("abc");
                });

                repo.storeUnit(unit);

                unitId = unit.getUnitId();
            }

            String query = """
                query Unit($id: UnitIdentification!) {
                  unit(id: $id) {
                    title
                  }
                }
                """;
            System.out.println("-------------------------------------------------------------------");
            log.info(query);
            System.out.println(query);

            GraphQL graphQL = _graphQL.get();
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
                System.out.println("-------------------------------------------------------------------");
                System.out.print("Result: ");
                System.out.println((Object) result.getData());

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
            System.out.println("===================================================================");
        }
    }

    @SuppressWarnings("unchecked")
    public void test2Compound() {
        Repository repo = RepositoryFactory.getRepository();
        final int tenantId = 1;

        Unit unit = repo.createUnit(tenantId, "a shipment instance");

        unit.withAttribute("SHIPMENT", Attribute.class, attr -> {
            CompoundAttribute compound = new CompoundAttribute(attr);

            compound.withNestedAttribute(unit, "ORDER_ID", String.class, nestedAttr -> {
                ArrayList<String> value = nestedAttr.getValue();
                value.add("*order id*");
            });

            compound.withNestedAttribute(unit, "DEADLINE", Instant.class, nestedAttr -> {
                ArrayList<Instant> value = nestedAttr.getValue();
                value.add(Instant.now());
            });

            compound.withNestedAttribute(unit, "READING", Double.class, nestedAttr -> {
                ArrayList<Double> value = nestedAttr.getValue();
                value.add(Math.PI);
                value.add(Math.E);
            });
        });

        unit.withAttribute("dc:title", String.class, attr -> {
            ArrayList<String> value = attr.getValue();
            value.add("Handling of " + "*order id*");
        });

        repo.storeUnit(unit);
    }

    public void test3Repository() {
        Repository repo = RepositoryFactory.getRepository();

        final int tenantId = 1; // For the sake of exercising, this is the tenant of units we will create

        final int numberOfParents = 50; //
        final int numberOfChildren = 10; //

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
                        CompoundAttribute compound = new CompoundAttribute(attr);

                        compound.withNestedAttributeValue(childUnit, "ORDER_ID", String.class, value -> {
                            value.add("*order id*");
                        });

                        compound.withNestedAttributeValue(childUnit, "DEADLINE", Instant.class, value -> {
                            value.add(now);
                        });

                        compound.withNestedAttributeValue(childUnit, "READING", Double.class, value -> {
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
