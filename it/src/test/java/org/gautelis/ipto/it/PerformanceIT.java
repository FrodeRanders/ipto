/*
 * Copyright (C) 2024-2026 Frode Randers
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
import org.gautelis.ipto.repo.db.Column;
import org.gautelis.ipto.repo.exceptions.BaseException;
import org.gautelis.ipto.repo.model.AssociationType;
import org.gautelis.ipto.repo.model.Repository;
import org.gautelis.ipto.repo.model.Unit;
import org.gautelis.ipto.repo.model.attributes.Attribute;
import org.gautelis.ipto.repo.model.attributes.RecordAttribute;
import org.gautelis.ipto.repo.model.locks.LockType;
import org.gautelis.ipto.repo.model.utils.RunningStatistics;
import org.gautelis.ipto.repo.search.UnitSearch;
import org.gautelis.ipto.repo.search.model.*;
import org.gautelis.ipto.repo.search.query.*;
import org.gautelis.vopn.lang.TimeDelta;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Optional;
import java.util.Vector;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.gautelis.ipto.it.Statistics.dumpStatistics;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 *
 */
@Tag("performance")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@IptoIT
public class PerformanceIT {
    private static final Logger log = LoggerFactory.getLogger(PerformanceIT.class);

    @Test
    @Order(1)
    public void concurrent(Repository repo) {

        final int numberOfUnits = 1000;

        final int tenantId = 1; // SCRATCH
        final String someKnownAttribute = "dcterms:description";

        Runtime runtime = Runtime.getRuntime();
        int numProcessors = runtime.availableProcessors();
        int numThreads = Math.max(numProcessors/2, 1);
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);

        System.out.println("---------------------------------------------------------------------------------------");
        System.out.println(" Running concurrent test with " + numThreads + " threads, creating " + numberOfUnits + " units with subsequent searches");
        System.out.println();
        System.out.println(" If this is run immediately after database startup and without warm-up, the");
        System.out.println(" statistics may not be accurate. In fact, this test may *be* the warm-up.");
        System.out.println("---------------------------------------------------------------------------------------");
        System.out.flush();

        RunningStatistics storeStats = new RunningStatistics();
        RunningStatistics searchOnNameStats = new RunningStatistics();
        RunningStatistics searchOnAttribStats = new RunningStatistics();

        try /* NO! try with resource ('executor') */ {
            Vector<String> unitNames = new Vector<>(numberOfUnits);
            Vector<String> attribValues = new Vector<>(numberOfUnits);

            for (int i=0; i<numberOfUnits; i++) {

                executor.submit(() -> {
                    Instant startTime = Instant.now();
                    {
                        String uniqueName = Generators.timeBasedEpochGenerator().generate().toString();
                        Unit parentUnit = repo.createUnit(tenantId, uniqueName);
                        unitNames.add(uniqueName);

                        parentUnit.withAttributeValue("dcterms:title", String.class, value -> {
                            value.add("First value");
                            value.add("Second value");
                            value.add("Third value");
                        });
                        parentUnit.withAttributeValue(someKnownAttribute, String.class, value -> {
                            String uniqueDescription = Generators.timeBasedEpochGenerator().generate().toString();
                            value.add(uniqueDescription);
                            attribValues.add(uniqueDescription);
                        });

                        repo.storeUnit(parentUnit);
                    }
                    storeStats.addSample(startTime, /* endTime */ Instant.now());
                });
            }

            for (int i=0; i<numberOfUnits; i++) {
                final int j = i;

                executor.submit(() -> {
                    Instant startTime = Instant.now();
                    {
                        // Unit constraints
                        SearchExpression expr = QueryBuilder.constrainToSpecificTenant(tenantId);
                        expr = QueryBuilder.assembleAnd(expr, QueryBuilder.constrainToSpecificStatus(Unit.Status.EFFECTIVE));

                        // Unit version constraint
                        String unitName = unitNames.get(j);
                        SearchItem<String> nameSearchItem = new StringUnitSearchItem(Column.UNIT_VERSION_UNITNAME, Operator.EQ, unitName);
                        expr = QueryBuilder.assembleAnd(expr, nameSearchItem);

                        // Result set constraints (paging)
                        SearchOrder order = SearchOrder.getDefaultOrder(); // descending on creation time
                        UnitSearch usd = new UnitSearch(expr, SearchStrategy.SET_OPS, order, /* no selection limit */ 0);

                        // Build SQL statement for search
                        DatabaseAdapter searchAdapter = repo.getDatabaseAdapter();

                        try {
                            Collection<Unit.Id> unitId = new ArrayList<>();
                            repo.withConnection(conn -> searchAdapter.search(conn, usd, repo.getTimingData(), rs -> {
                                while (rs.next()) {
                                    int k = 0;
                                    int _tenantId = rs.getInt(++k);
                                    long _unitId = rs.getLong(++k);
                                    int _unitVer = rs.getInt(++k);
                                    Timestamp _created = rs.getTimestamp(++k);
                                    Timestamp _modified = rs.getTimestamp(++k);

                                    //log.trace("Concurrent search: Found: unit=" + _tenantId + "." + _unitId + ":" + _unitVer + " created=" + _created + " modified=" + _modified);
                                    unitId.add(new Unit.Id(_tenantId, _unitId));
                                }
                            }));

                            if (unitId.isEmpty()) {
                                fail("Failed to find known unit corresponding to search");
                            }
                        } catch (SQLException sqle) {
                            fail(sqle.getMessage());
                        }
                    }
                    searchOnNameStats.addSample(startTime, /* endTime */ Instant.now());
                });
            }

            for (int i=0; i<numberOfUnits; i++) {
                final int j = i;

                executor.submit(() -> {
                    Instant startTime = Instant.now();
                    {
                        // Unit constraints
                        SearchExpression expr = QueryBuilder.constrainToSpecificTenant(tenantId);
                        expr = QueryBuilder.assembleAnd(expr, QueryBuilder.constrainToSpecificStatus(Unit.Status.EFFECTIVE));

                        // Attribute constraint
                        String attribValue = attribValues.get(j);
                        SearchItem<String> nameSearchItem = new StringAttributeSearchItem(someKnownAttribute, Operator.EQ, attribValue);
                        expr = QueryBuilder.assembleAnd(expr, nameSearchItem);

                        // Result set constraints (paging)
                        SearchOrder order = SearchOrder.getDefaultOrder(); // descending on creation time
                        UnitSearch usd = new UnitSearch(expr, SearchStrategy.SET_OPS, order, /* no selection limit */ 0);

                        // Build SQL statement for search
                        DatabaseAdapter searchAdapter = repo.getDatabaseAdapter();

                        try {
                            Collection<Unit.Id> unitId = new ArrayList<>();
                            repo.withConnection(conn -> searchAdapter.search(conn, usd, repo.getTimingData(), rs -> {
                                while (rs.next()) {
                                    int k = 0;
                                    int _tenantId = rs.getInt(++k);
                                    long _unitId = rs.getLong(++k);
                                    int _unitVer = rs.getInt(++k);
                                    Timestamp _created = rs.getTimestamp(++k);
                                    Timestamp _modified = rs.getTimestamp(++k);

                                    //log.trace("Concurrent search: Found: unit=" + _tenantId + "." + _unitId + ":" + _unitVer + " created=" + _created + " modified=" + _modified);
                                    unitId.add(new Unit.Id(_tenantId, _unitId));
                                }
                            }));

                            if (unitId.isEmpty()) {
                                fail("Failed to find known unit corresponding to search for \"" + someKnownAttribute + " = '" + attribValue + "'\"");
                            }
                        } catch (SQLException sqle) {
                            fail(sqle.getMessage());
                        }
                    }
                    searchOnAttribStats.addSample(startTime, /* endTime */ Instant.now());
                });
            }

            // No more jobs coming...
            executor.shutdown();

            // Await completion
            System.out.println("Awaiting completion...");
            boolean finished = executor.awaitTermination(1, TimeUnit.MINUTES);
            assertTrue(finished, "All tasks did not finish in time");

        } catch (Throwable t) {
            String info = t.getMessage();
            log.error(info, t);

            fail(info);

        } finally {
            executor.shutdownNow();
        }

        String info = String.format(
                "Concurrent count=%d, store-time=%.2fms unit-search-time=%.2fms attrib-search-time=%.2fms",
                numberOfUnits, storeStats.getMean(), searchOnNameStats.getMean(), searchOnAttribStats.getMean()
        );
        log.info(info);
        System.out.println(info);
        System.out.println();
        System.out.flush();
    }

    @Test
    @Order(2)
    public void test(Repository repo) {
        final int tenantId = 1; // For the sake of exercising, this is the tenant of units we will create

        final int numberOfParents = 100; //
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

            System.out.println("---------------------------------------------------------------------------------------");
            System.out.println(" Generating " + (numberOfParents * numberOfChildren) + " units...");
            System.out.println("---------------------------------------------------------------------------------------");
            System.out.flush();

            RunningStatistics averageTPI = new RunningStatistics(); // Average time per iteration

            for (int j = 1; j < numberOfParents + 1; j++) {
                Instant startTime = Instant.now();

                Unit parentUnit = repo.createUnit(tenantId, "parent-" + j);
                parentUnit.withAttributeValue("dcterms:title", String.class, value -> {
                    value.add("First value");
                    value.add("Second value");
                    value.add("Third value");
                });
                parentUnit.withAttributeValue("dcterms:description", String.class, value -> value.add("A test unit"));
                repo.storeUnit(parentUnit);

                averageTPI.addSample(startTime, /* endTime */ Instant.now());

                if (null == firstParentCreated) {
                    firstParentCreated = parentUnit.getCreationTime().get();
                }

                if (true) {
                    parentUnit.withAttributeValue("dcterms:title", String.class, value -> {
                        value.clear();
                        value.add("Replaced value");
                    });
                    repo.storeUnit(parentUnit);
                }

                if (false) {
                    // Works, but not part of test (at the moment)
                    repo.lockUnit(parentUnit, LockType.EXISTENCE, "test");
                }

                for (int i = 1; i < numberOfChildren + 1; i++) {
                    long count = (j-1)*numberOfChildren + i;
                    if (count % 1000 == 0) { // depends on parameters (numberOfParents and numberOfChildren) > 1000
                        long iterationsLeft = (numberOfParents * numberOfChildren) - count;
                        double timeLeft = iterationsLeft * averageTPI.getMean();

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
                    parentUnit.withAttribute("dcterms:title", String.class, attr -> {
                        try {
                            childUnit.addAttribute(attr);
                        } catch (BaseException be) {
                            System.err.println("Failed to add attribute: " + be.getMessage());
                        }
                    });

                    final Instant now = Instant.now();

                    childUnit.withAttributeValue("dcterms:date", Instant.class, value -> value.add(now));

                    childUnit.withAttributeValue("ffa:producerade_resultat", Attribute.class, resultat -> {
                        Optional<Attribute<?>> rattenTill = repo.instantiateAttribute("ffa:ratten_till_period");
                        if (rattenTill.isPresent()) {
                            RecordAttribute rattenTillRecord = RecordAttribute.from(childUnit, rattenTill.get());

                            rattenTillRecord.withNestedAttributeValue("ffa:ersattningstyp", String.class, ersattningstyp -> ersattningstyp.add("HUNDBIDRAG"));

                            rattenTillRecord.withNestedAttributeValue("ffa:omfattning", String.class, omfattning -> omfattning.add("HEL"));

                            Attribute<?> attribute = rattenTill.get();
                            resultat.add(attribute);
                        }
                    });

                    // Some unique unit will get unique attribute
                    if (/* i within page, that we will search for later down under */
                            i > pageOffset && i == pageOffset + pageSize && numberOfUnitsToHaveSpecificString-- > 0
                    ) {
                        childUnit.withAttributeValue("dcterms:title", String.class, value -> value.add(someSpecificString));

                        someInstant = now.minus(Duration.ofHours(2)); // For testing purposes
                    }

                    //
                    repo.storeUnit(childUnit);
                    averageTPI.addSample(startTime, /* endTime */ Instant.now());

                    if (false) {
                        // Works, but not part of test (at the moment)
                        // Add a relation to parent unit
                        repo.addRelation(parentUnit, AssociationType.PARENT_CHILD_RELATION, childUnit);
                    }

                }
                System.out.flush();

                if (false) {
                    System.out.println("Children of " + parentUnit.getName().orElse("parent") + " (" + parentUnit.getReference() + "):");
                    parentUnit.getRelations(AssociationType.PARENT_CHILD_RELATION).forEach(
                            relatedUnit -> System.out.println("  " + relatedUnit.getName().orElse("child") + " (" + relatedUnit.getReference() + ")")
                    );
                }
            }

            // In order to search here in test, we will need some "internal" objects,
            // such as Context, DataSource, etc.
            {
                SearchExpression expr;
                if (true) {
                    //---------------------------------------------------------------------------------
                    // Variant 1: A textual representation of criteria, expressed in terms of unit
                    // and attribute constraints. This textual representation is parsed and give
                    // us an AST (abstract syntax tree), the SearchExpression, that we use for
                    // searching.
                    //---------------------------------------------------------------------------------
                    String text = String.format(
                            "tenantid = %d AND status = EFFECTIVE AND created >= \"%s\" AND dcterms:date >= \"%s\" AND dcterms:title = \"%s\"",
                            tenantId,
                            firstParentCreated,
                            someInstant,
                            someSpecificString
                    );
                    expr = SearchTextQueryParser.parse(text, repo, SearchTextQueryParser.AttributeNameMode.NAMES);

                } else {
                    //---------------------------------------------------------------------------------
                    // Variant 2: We build the AST, the SearchExpression, directly ourselves with
                    // help from a query builder, that expresses constraints on unit and attributes.
                    //---------------------------------------------------------------------------------

                    // Unit constraints
                    expr = QueryBuilder.constrainToSpecificTenant(tenantId);
                    expr = QueryBuilder.assembleAnd(expr, QueryBuilder.constrainToSpecificStatus(Unit.Status.EFFECTIVE));
                    expr = QueryBuilder.assembleAnd(expr, QueryBuilder.constrainToCreatedAfter(firstParentCreated));

                    // First attribute constraint
                    SearchItem<Instant> timestampSearchItem = new TimeAttributeSearchItem("dcterms:date", Operator.GEQ, someInstant);
                    expr = QueryBuilder.assembleAnd(expr, timestampSearchItem);

                    // Second attribute constraint
                    SearchItem<String> stringSearchItem = new StringAttributeSearchItem("dcterms:title", Operator.EQ, someSpecificString);
                    expr = QueryBuilder.assembleAnd(expr, stringSearchItem);
                }

                // Result set constraints (paging)
                SearchOrder order = SearchOrder.getDefaultOrder(); // descending on creation time
                UnitSearch usd = new UnitSearch(expr, SearchStrategy.SET_OPS, order, /* selectionSize */ 5);

                // Build SQL statement for search
                DatabaseAdapter searchAdapter = repo.getDatabaseAdapter();

                Collection<Unit.Id> unitId = new ArrayList<>();
                repo.withConnection(conn -> searchAdapter.search(conn, usd, repo.getTimingData(), rs -> {
                    while (rs.next()) {
                        int j = 0;
                        int _tenantId = rs.getInt(++j);
                        long _unitId = rs.getLong(++j);
                        int _unitVer = rs.getInt(++j);
                        Timestamp _created = rs.getTimestamp(++j);
                        Timestamp _modified = rs.getTimestamp(++j);

                        System.out.println("\nFound: unit=" + _tenantId + "." + _unitId + ":" + _unitVer + " created=" + _created + " modified=" + _modified);
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
                                System.out.println("--> " + unit.asJson(/* pretty? */ true));
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

    private int getAttributeId(String attributeName, Repository repo) {
        Optional<Integer> attributeId = repo.attributeNameToId(attributeName);
        if (attributeId.isEmpty()) {
            throw new RuntimeException("Unknown attribute: " + attributeName);
        }
        return attributeId.get();
    }
}
