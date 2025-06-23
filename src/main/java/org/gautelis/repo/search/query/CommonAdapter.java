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
package org.gautelis.repo.search.query;

import org.gautelis.repo.db.Database;
import org.gautelis.repo.model.utils.TimedExecution;
import org.gautelis.repo.model.utils.TimingData;
import org.gautelis.repo.search.UnitSearch;
import org.gautelis.repo.search.model.*;
import org.gautelis.repo.utils.CheckedConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.time.Instant;
import java.util.*;

import static org.gautelis.repo.db.Column.*;
import static org.gautelis.repo.db.Table.*;
import static org.gautelis.repo.utils.TimeHelper.instant2Timestamp;

public abstract class CommonAdapter extends DatabaseAdapter {
    protected static final Logger log = LoggerFactory.getLogger(CommonAdapter.class);

    private final boolean USE_PREPARED_STATEMENT = true;

    //------------------------------------------------------------------
    // OBSERVE: This is *not* the DB-specific format used for instance
    //          with TO_TIMESTAMP(<string>, <format>). This pattern is
    //          used to format an Instance, so it is suitable as a
    //          <string> in a TO_TIMESTAMP construct.
    //------------------------------------------------------------------
    public static String INSTANT_TIME_PATTERN = "yyyy-MM-dd HH:mm:ss.SSS";

    /**
     * Unload issues for this database adapter.
     * <p>
     */
    public void unload() {
        /* Nothing to see here */
    }

    public String getTimePattern() {
        return INSTANT_TIME_PATTERN;
    }

    public String asTimeLiteral(String timeStr) {
        return "{ts '" + timeStr.replace('\'', ' ') + "'}";
    }

    public String asTimeLiteral(Instant instant) {
        return "{ts '" + instant.toString().replace('\'', ' ') + "'}"; // TODO
    }

    /**
     * Joins a collection of strings, using the specified separator.
     *
     * @param c collectio of strings
     * @param separator
     * @return
     */
    private String join(Collection<String> c, String separator) {
        Objects.requireNonNull(c, "c");
        Objects.requireNonNull(separator, "separator");

        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (String item : c) {
            sb.append(first ? "" : separator).append(item);
            first = false;
        }
        return sb.toString();
    }

    protected SearchExpression optimize(
            SearchExpression sex
    ) {
        // No optimisation
        return sex;
    }

    /**
     * Separates constraints into unit- and attribute constraints,
     * and enumerates the latter -- giving them unique names: c2, c2, c3, ...
     * <p>
     * @param expression
     * @param unitLeaves
     * @param attributeLeaves
     */
    private void identifyConstraints(
            SearchExpression expression,
            Collection<LeafExpression<?>> unitLeaves,
            Collection<LeafExpression<?>> attributeLeaves
    ) {
        Objects.requireNonNull(expression, "expression");
        Objects.requireNonNull(unitLeaves, "unitLeaves");
        Objects.requireNonNull(attributeLeaves, "attributeLeaves");

        switch (expression) {
            case LeafExpression<?> leaf -> {
                if (leaf.getItem() instanceof UnitSearchItem) {
                    unitLeaves.add(leaf);
                } else if (leaf.getItem() instanceof AttributeSearchItem<?>) {
                    attributeLeaves.add(leaf);
                    String reference = "c" + attributeLeaves.size();
                    leaf.setReference(reference);
                }
            }
            case NotExpression(SearchExpression inner) -> {
                    identifyConstraints(inner, unitLeaves, attributeLeaves);
            }
            case BinaryExpression binExpr -> {
                SearchExpression left = binExpr.getLeft();
                identifyConstraints(left, unitLeaves, attributeLeaves);

                SearchExpression right = binExpr.getRight();
                identifyConstraints(right, unitLeaves, attributeLeaves);
            }
            default -> {
            }
        }
    }

    /**
     * Builds attribute constraint logic, referring to the names assigned earlier:
     * Results in things like: ((SELECT * FROM c1) INTERSECT (SELECT * FROM c2)) UNION ...
     *
     * @param expression
     * @return
     */
    private Optional<String> buildAttributeConstraintLogic(
            SearchExpression expression
    ) {
        Objects.requireNonNull(expression, "expression");

        switch (expression) {
            case LeafExpression<?> leaf -> {
                if (leaf.getItem() instanceof AttributeSearchItem<?>) {
                    Optional<String> reference = leaf.getReference();
                    if (reference.isPresent()) {
                        return Optional.of("(SELECT * FROM " + reference.get() + ")");
                    } else {
                        String info = "Leaf expression must have a reference.";
                        throw new IllegalArgumentException(info);
                    }
                } else {
                    return Optional.empty();
                }
            }
            case NotExpression(SearchExpression inner) -> {
                Optional<String> innerLogic = buildAttributeConstraintLogic(inner);
                if (innerLogic.isPresent()) {
                    return Optional.of("NOT (" + innerLogic.get() + ")");
                }
            }
            case BinaryExpression binExpr -> {
                Optional<String> left = buildAttributeConstraintLogic(binExpr.getLeft());
                Optional<String> right = buildAttributeConstraintLogic(binExpr.getRight());

                boolean hasLeft = left.isPresent();
                boolean hasRight = right.isPresent();
                boolean hasCombination = hasLeft && hasRight;

                if (hasCombination) {
                    String sql = "(";
                    sql += left.get();
                    sql += switch (binExpr.operator()) {
                        case AND -> " INTERSECT ";
                        case OR -> " UNION ";
                        default -> throw new IllegalStateException("Unexpected operator: " + binExpr.operator());
                    };
                    sql += right.get();
                    sql += ")";
                    return Optional.of(sql);
                } else if (hasLeft) {
                    return left;
                } else if (hasRight) {
                    return right;
                }
            }
            default -> {
            }
        }
        return Optional.empty();
    }

    /**
     * Builds unit constraint logic.
     * Results in things like: UNIT_STATUS = ... AND UNIT_CREATED >= ...
     * @param expression
     * @return
     */
    private Optional<String> buildUnitConstraintLogic(
            SearchExpression expression,
            Collection<SearchItem<?>> preparedItems,
            Map<String, SearchItem<?>> commonConstraintValues
    ) {
        Objects.requireNonNull(expression, "expression");
        Objects.requireNonNull(preparedItems, "preparedItems");
        Objects.requireNonNull(commonConstraintValues, "commonConstraintValues");

        switch (expression) {
            case LeafExpression<?> leaf -> {
                if (leaf.getItem() instanceof UnitSearchItem<?> usi) {
                    preparedItems.add(usi);
                    return Optional.of(leaf.toSql(USE_PREPARED_STATEMENT, commonConstraintValues));
                } else {
                    return Optional.empty();
                }
            }
            case NotExpression notExpr -> {
                return Optional.of(notExpr.toSql(USE_PREPARED_STATEMENT, commonConstraintValues));
            }
            case BinaryExpression binExpr -> {
                Optional<String> left = buildUnitConstraintLogic(binExpr.getLeft(), preparedItems, commonConstraintValues);
                Optional<String> right = buildUnitConstraintLogic(binExpr.getRight(), preparedItems, commonConstraintValues);

                boolean hasLeft = left.isPresent();
                boolean hasRight = right.isPresent();
                boolean hasCombination = hasLeft && hasRight;

                if (hasCombination) {
                    String sql = "(" + left.get() + " " + binExpr.operator() + " " + right.get() + ")";
                    return Optional.of(sql);
                } else if (hasLeft) {
                    return left;
                } else if (hasRight) {
                    return right;
                }
            }
            default -> {
            }
        }
        return Optional.empty();
    }

    /**
     * Generates SQL statements for individual attribute constraints.
     *
     * @param leaf
     * @param constraints
     */
    private void generateAttributeConstraint(
            LeafExpression<?> leaf,
            Collection<String> constraints,
            Map<String, SearchItem<?>> commonConstraintValues,
            Collection<SearchItem<?>> preparedItems
    ) {
        Objects.requireNonNull(leaf, "leaf");
        Objects.requireNonNull(constraints, "constraints");
        Objects.requireNonNull(commonConstraintValues, "commonConstraintValues");
        Objects.requireNonNull(preparedItems, "preparedItems");

        SearchItem<?> item = leaf.getItem();
        if (item instanceof AttributeSearchItem<?> asi) {
            preparedItems.add(asi);
            constraints.add(leaf.toSql(USE_PREPARED_STATEMENT, commonConstraintValues));
        }
    }

    /**
     *
     * @param sd
     * @return
     * @throws IllegalArgumentException
     */
    protected GeneratedStatement generateStatement(
            UnitSearch sd
    ) {
        Objects.requireNonNull(sd, "sd");

        Collection<SearchItem<?>> preparedItems = new ArrayList<>();
        Map<String, SearchItem<?>> commonConstraintValues = new HashMap<>();

        //
        SearchExpression expression = optimize(sd.getExpression());

        // Identify attribute constraints: c1, c2, ...
        Collection<LeafExpression<?>> unitLeaves = new LinkedList<>();
        Collection<LeafExpression<?>> attributeLeaves = new LinkedList<>();
        identifyConstraints(expression, unitLeaves, attributeLeaves);

        // Some unit constraints *may* affect attribute search, such as tenantId.
        for (LeafExpression<?> leaf : unitLeaves) {
            if (leaf.getItem() instanceof UnitSearchItem<?> usi) {
                switch (usi.getColumn()) {
                    case UNIT_TENANTID -> commonConstraintValues.put(UNIT_TENANTID.toString(), usi);
                }
            }
        }

        // Generate SQL for individual attribute constraints
        Collection<String> attributeConstraints = new LinkedList<>();
        for (LeafExpression<?> leaf : attributeLeaves) {
            generateAttributeConstraint(leaf, attributeConstraints, commonConstraintValues, preparedItems);
        }

        // Generate SQL for attribute constraint logic,
        // i.e. along the lines of "(c1 INTERSECT c2) UNION ..."
        Optional<String> attributeConstraintLogic = buildAttributeConstraintLogic(expression);

        // Generate SQL for unit constraints
        Optional<String> unitConstraints = buildUnitConstraintLogic(expression, preparedItems, commonConstraintValues);

        // Generate SQL for the ORDER BY-clause
        StringBuilder orderBy = new StringBuilder();
        SearchOrder order = sd.getOrder();
        for (int i=0; i < order.columns().length; i++) {
            orderBy.append(order.columns()[i]);
            orderBy.append(order.ascending()[i] ? " ASC" : " DESC");
        }

        /*-----------------------------------------------------------------------------------
         * WITH
         *      (* attribute constraints *)
         *      c1 AS (SELECT av.tenantid, av.unitid
         *             FROM repo_attribute_value av
         *                      JOIN repo_time_vector vv ON av.valueid = vv.valueid
         *             WHERE av.tenantid = 1
         *               AND av.attrid = 7
         *               AND vv.val >= TIMESTAMP '2025-06-01T14:42:19.183831Z'),
         *
         *      c2 AS (SELECT av.tenantid, av.unitid
         *             FROM repo_attribute_value av
         *                      JOIN repo_string_vector vv ON av.valueid = vv.valueid
         *             WHERE av.tenantid = 1
         *               AND av.attrid = 1
         *               AND lower(vv.val) = lower('01972bf1-8953-7da4-9fbd-03d00eb7b33d')),
         *
         *      (* attribute constraint logic *)
         *      final AS ((SELECT * FROM c1) INTERSECT (SELECT * FROM c2))
         *
         * SELECT ut.tenantid, ut.unitid, ut.created
         * FROM repo_unit ut
         *      JOIN final f USING (tenantid, unitid)
         * WHERE
         *      (* unit constraints *)
         *      ((ut.tenantid = 1 AND ut.status = 30) AND ut.created >= TIMESTAMP '2025-06-01T14:42:19.155499Z')
         *
         * ORDER BY
         *      (* order by *)
         *      ut.created DESC
         *----------------------------------------------------------------------------------*/
        String statement = "";
        if (!attributeLeaves.isEmpty() && attributeConstraintLogic.isPresent()) {
            statement += "WITH " + join(attributeConstraints, ", ") + ", ";
            statement += "final AS " + attributeConstraintLogic.get() + " ";
        }
        statement += "SELECT " + UNIT_TENANTID + ", " + UNIT_UNITID + ", " + UNIT_CREATED + " ";
        statement += "FROM " + UNIT + " ";
        statement += "JOIN final f USING (tenantid, unitid) ";
        if (!unitLeaves.isEmpty() && unitConstraints.isPresent()) {
            statement += "WHERE " + unitConstraints.get() + " ";
        }
        if (!orderBy.isEmpty()) {
            statement += "ORDER BY " + orderBy + " ";
        }

        return new GeneratedStatement(statement, preparedItems, commonConstraintValues);
    }

    @Override
    public void search(
            Connection conn,
            UnitSearch sd,
            TimingData timingData,
            CheckedConsumer<ResultSet> rsBlock
    ) throws IllegalArgumentException {
        Objects.requireNonNull(conn, "conn");
        Objects.requireNonNull(sd, "sd");
        Objects.requireNonNull(timingData, "timingData");
        Objects.requireNonNull(rsBlock, "rsBlock");

        GeneratedStatement generatedStatement = generateStatement(sd);
        String statement = generatedStatement.statement();
        log.debug("Search statement: {}", statement);

        Collection<SearchItem<?>> preparedItems = generatedStatement.preparedItems();
        Map<String, SearchItem<?>> ccv = generatedStatement.commonConstraintValues();

        TimedExecution.run(timingData, "custom search", () -> {
            if (USE_PREPARED_STATEMENT) {
                Database.useReadonlyPreparedStatement(conn, statement,
                    pStmt -> {
                        int selectionSize = sd.getSelectionSize();
                        if (selectionSize > 0) {
                            pStmt.setMaxRows(selectionSize);
                        }

                        int i = 0;
                        for (SearchItem<?> item : preparedItems) {
                            if (item instanceof AttributeSearchItem<?>) {
                                if (ccv.containsKey(UNIT_TENANTID.toString())) {
                                    int tenantId = (Integer) ccv.get(UNIT_TENANTID.toString()).getValue();
                                    pStmt.setInt(++i, tenantId);
                                }
                            }
                            switch (item.getType()) {
                                case STRING -> pStmt.setString(++i, (String) item.getValue());
                                case TIME -> pStmt.setTimestamp(++i, instant2Timestamp((Instant) item.getValue()));
                                case INTEGER -> pStmt.setInt(++i, (Integer) item.getValue());
                                case LONG -> pStmt.setLong(++i, (Long) item.getValue());
                                case DOUBLE -> pStmt.setDouble(++i, (Double) item.getValue());
                                case BOOLEAN -> pStmt.setBoolean(++i, (Boolean) item.getValue());
                            }
                        }
                    },
                    rsBlock
                );
            } else {
                Database.useReadonlyStatement(conn, statement,
                        stmt -> stmt.setMaxRows(sd.getSelectionSize()),
                        rsBlock
                );
            }
        });
    }
}
