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
package org.gautelis.ipto.repo.search.query;

import org.gautelis.ipto.repo.db.Database;
import org.gautelis.ipto.repo.exceptions.InvalidParameterException;
import org.gautelis.ipto.repo.model.utils.TimedExecution;
import org.gautelis.ipto.repo.model.utils.TimingData;
import org.gautelis.ipto.repo.search.UnitSearch;
import org.gautelis.ipto.repo.search.model.*;
import org.gautelis.ipto.repo.utils.CheckedConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.time.Instant;
import java.util.*;

import static org.gautelis.ipto.repo.db.Column.*;
import static org.gautelis.ipto.repo.db.Table.*;
import static org.gautelis.ipto.repo.model.AttributeType.*;
import static org.gautelis.ipto.repo.utils.TimeHelper.instant2Timestamp;

public abstract class CommonAdapter extends DatabaseAdapter {
    protected static final Logger log = LoggerFactory.getLogger(CommonAdapter.class);

    private final boolean USE_PREPARED_STATEMENT = true;

    //------------------------------------------------------------------
    // OBSERVE: This is *not* the DB-specific format used for instance
    //          with TO_TIMESTAMP(<string>, <format>). This pattern is
    //          used to format an Instance, so it is suitable as a
    //          <string> in a TO_TIMESTAMP construct.
    //------------------------------------------------------------------
    public static final String INSTANT_TIME_PATTERN = "yyyy-MM-dd HH:mm:ss.SSS";


    /**
     * Strategy hook with default suggestion 'EXISTS', which generally performs
     * better with LIMIT + kernel constraints.
     * TODO add analysis of search expression
     */
    protected SearchStrategy chooseStrategy(
            UnitSearch sd,
            SearchExpression expression
    ) {
        // Some heuristics.
        //    EXISTS is typically preferable with kernel constraints and LIMIT.
        //
        return SearchStrategy.SET_OPS;
    }

    protected CommonAdapter() {
    }

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
        // Currently no optimisation :-p
        return sex;
    }

    /**
     * Separates constraints into unit- and attribute constraints,
     * and enumerates the latter -- giving them unique names: c1, c2, c3, ...
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
                    String label = "c" + attributeLeaves.size();
                    leaf.setLabel(label);
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
            default -> {}
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
                    Optional<String> label = leaf.getLabel();
                    if (label.isPresent()) {
                        return Optional.of("(SELECT " + UNIT_VERSION_TENANTID.plain() + ", " + UNIT_VERSION_UNITID.plain() + " FROM " + label.get() + ")");
                    } else {
                        String info = "Leaf expression must have a label";
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
                boolean hasBoth = hasLeft && hasRight;

                if (hasBoth) {
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
            default -> {}
        }
        return Optional.empty();
    }

    private Optional<String> buildAttributeExistsLogic(
            SearchExpression expression,
            Map<String, String> attributeLeafSqlByLabel
    ) {
        Objects.requireNonNull(expression, "expression");
        Objects.requireNonNull(attributeLeafSqlByLabel, "attributeLeafSqlByLabel");

        switch (expression) {
            case LeafExpression<?> leaf -> {
                if (leaf.getItem() instanceof AttributeSearchItem<?>) {
                    String label = leaf.getLabel().orElseThrow(() ->
                            new IllegalArgumentException("Leaf expression must have a label"));

                    String cteSql = attributeLeafSqlByLabel.get(label);
                    if (cteSql == null) {
                        throw new IllegalStateException("No SQL for label: " + label);
                    }
                    return Optional.of(cteSql);
                }
                return Optional.empty();
            }

            case NotExpression(SearchExpression inner) -> {
                Optional<String> innerLogic = buildAttributeExistsLogic(inner, attributeLeafSqlByLabel);
                return innerLogic.map(s -> "NOT (" + s + ")");
            }

            case BinaryExpression binExpr -> {
                Optional<String> left = buildAttributeExistsLogic(binExpr.getLeft(), attributeLeafSqlByLabel);
                Optional<String> right = buildAttributeExistsLogic(binExpr.getRight(), attributeLeafSqlByLabel);

                boolean hasLeft = left.isPresent();
                boolean hasRight = right.isPresent();
                boolean hasBoth = hasLeft && hasRight;

                if (hasBoth) {
                    String op = switch (binExpr.operator()) {
                        case AND -> " AND ";
                        case OR -> " OR ";
                        default -> throw new IllegalStateException("Unexpected operator: " + binExpr.operator());
                    };
                    return Optional.of("(" + left.get() + op + right.get() + ")");
                } else if (hasLeft) {
                    return left;
                } else {
                    return right; // may be empty
                }
            }

            default -> {
                return Optional.empty();
            }
        }
    }

    /**
     * Builds unit constraint logic.
     * Results in things like: UNIT_STATUS = ... AND UNIT_CREATED >= ...
     * @param expression
     * @return
     */
    private Optional<String> buildUnitConstraintLogic(
            SearchStrategy strategy,
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
                    return Optional.of(leaf.toSql(strategy, USE_PREPARED_STATEMENT, commonConstraintValues, attributeNameToId));
                } else {
                    return Optional.empty();
                }
            }
            case NotExpression notExpr -> {
                return Optional.of(notExpr.toSql(strategy, USE_PREPARED_STATEMENT, commonConstraintValues, attributeNameToId));
            }
            case BinaryExpression binExpr -> {
                Optional<String> left = buildUnitConstraintLogic(strategy, binExpr.getLeft(), preparedItems, commonConstraintValues);
                Optional<String> right = buildUnitConstraintLogic(strategy, binExpr.getRight(), preparedItems, commonConstraintValues);

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
            default -> {}
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
            SearchStrategy strategy,
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
            constraints.add(leaf.toSql(strategy, USE_PREPARED_STATEMENT, commonConstraintValues, attributeNameToId));
        }
    }

    protected GeneratedStatement generateStatement(UnitSearch sd) {
        Objects.requireNonNull(sd, "sd");

        Map<String, SearchItem<?>> commonConstraintValues = new HashMap<>();

        SearchExpression expression = optimize(sd.getExpression());
        SearchStrategy strategy = sd.getStrategy();

        // Identify constraints: label attribute leaves as c1, c2, ...
        Collection<LeafExpression<?>> unitLeaves = new LinkedList<>();
        Collection<LeafExpression<?>> attributeLeaves = new LinkedList<>();
        identifyConstraints(expression, unitLeaves, attributeLeaves);

        // Generate SQL for attribute leaves (via leaf.toSql)
        Collection<SearchItem<?>> preparedAttributeItems = new ArrayList<>();
        Map<String, String> attributeLeafSqlByLabel = new LinkedHashMap<>();
        Collection<String> attributeConstraints = new LinkedList<>();
        for (LeafExpression<?> leaf : attributeLeaves) {
            SearchItem<?> item = leaf.getItem();
            if (item instanceof AttributeSearchItem<?> asi) {
                preparedAttributeItems.add(asi);
                String cteSql = leaf.toSql(strategy, USE_PREPARED_STATEMENT, commonConstraintValues, attributeNameToId);
                attributeConstraints.add(cteSql);

                // Keep a map label -> cteSql
                String label = leaf.getLabel().orElseThrow(() ->
                        new IllegalArgumentException("Attribute leaf must have a label"));
                attributeLeafSqlByLabel.put(label, cteSql);
            }
        }

        // Unit constraints
        Collection<SearchItem<?>> preparedUnitItems = new ArrayList<>();
        Optional<String> unitConstraints = buildUnitConstraintLogic(strategy, expression, preparedUnitItems, commonConstraintValues);

        // ORDER BY
        StringBuilder orderBy = new StringBuilder();
        SearchOrder order = sd.getOrder();
        for (int i = 0; i < order.columns().length; i++) {
            orderBy.append(order.columns()[i]);
            orderBy.append(order.ascending()[i] ? " ASC" : " DESC");
        }

        Collection<SearchItem<?>> preparedItems = new ArrayList<>();
        switch (strategy) {
            case SET_OPS -> {
                preparedItems.addAll(preparedAttributeItems);
                preparedItems.addAll(preparedUnitItems);
            }
            case EXISTS -> {
                preparedItems.addAll(preparedUnitItems);
                preparedItems.addAll(preparedAttributeItems);
            }
        }

        return switch (strategy) {
            case SET_OPS -> generateStatementSetOps(
                    expression,
                    unitLeaves, unitConstraints.orElse(""),
                    attributeLeaves, attributeConstraints,
                    orderBy.toString(),
                    preparedItems,
                    commonConstraintValues
            );

            case EXISTS -> generateStatementExists(
                    expression,
                    unitLeaves, unitConstraints.orElse(""),
                    attributeLeaves, attributeLeafSqlByLabel,
                    orderBy.toString(),
                    preparedItems,
                    commonConstraintValues
            );
        };
    }

    private GeneratedStatement generateStatementSetOps(
            SearchExpression expression,
            Collection<LeafExpression<?>> unitLeaves,
            String unitConstraints,
            Collection<LeafExpression<?>> attributeLeaves,
            Collection<String> attributeConstraints,
            String orderBy,
            Collection<SearchItem<?>> preparedItems,
            Map<String, SearchItem<?>> commonConstraintValues
    ) {
        Optional<String> attributeConstraintLogic = buildAttributeConstraintLogic(expression);

        String statement = "";
        if (!attributeLeaves.isEmpty() && attributeConstraintLogic.isPresent()) {
            statement += "WITH " + join(attributeConstraints, ", ") + ", ";
            statement += "final AS " + attributeConstraintLogic.get() + " ";
        }

        statement += "SELECT " + UNIT_KERNEL_TENANTID + ", " + UNIT_KERNEL_UNITID + ", " +
                UNIT_VERSION_UNITVER + ", " + UNIT_KERNEL_CREATED + ", " + UNIT_VERSION_MODIFIED + " ";

        statement += "FROM " + UNIT_KERNEL + ", " + UNIT_VERSION + " ";
        if (!attributeLeaves.isEmpty() && attributeConstraintLogic.isPresent()) {
            statement += "JOIN final f USING (" + UNIT_KERNEL_TENANTID.plain() + ", " + UNIT_KERNEL_UNITID.plain() + ") ";
        }

        statement += "WHERE " + UNIT_KERNEL_TENANTID + " = " + UNIT_VERSION_TENANTID + " ";
        statement += "AND " + UNIT_KERNEL_UNITID + " = " + UNIT_VERSION_UNITID + " ";
        statement += "AND " + UNIT_KERNEL_LASTVER + " = " + UNIT_VERSION_UNITVER + " ";

        if (!unitLeaves.isEmpty() && !unitConstraints.isEmpty()) {
            statement += "AND " + unitConstraints + " ";
        }

        if (!orderBy.isEmpty()) {
            statement += "ORDER BY " + orderBy + " ";
        }

        return new GeneratedStatement(statement, preparedItems, commonConstraintValues);
    }

    private GeneratedStatement generateStatementExists(
            SearchExpression expression,
            Collection<LeafExpression<?>> unitLeaves,
            String unitConstraints,
            Collection<LeafExpression<?>> attributeLeaves,
            Map<String, String> attributeLeafSqlByLabel,
            String orderBy,
            Collection<SearchItem<?>> preparedItems,
            Map<String, SearchItem<?>> commonConstraintValues
    ) {
        //
        Optional<String> attributeExistsLogic = buildAttributeExistsLogic(expression, attributeLeafSqlByLabel);

        //
        String statement = "";
        statement += "SELECT " + UNIT_KERNEL_TENANTID + ", " + UNIT_KERNEL_UNITID + ", " +
                UNIT_VERSION_UNITVER + ", " + UNIT_KERNEL_CREATED + ", " + UNIT_VERSION_MODIFIED + " ";

        statement += "FROM " + UNIT_KERNEL + " ";
        statement += "JOIN " + UNIT_VERSION + " ON " +
                UNIT_KERNEL_TENANTID + " = " + UNIT_VERSION_TENANTID + " AND " +
                UNIT_KERNEL_UNITID + " = " + UNIT_VERSION_UNITID + " AND " +
                UNIT_KERNEL_LASTVER + " = " + UNIT_VERSION_UNITVER + " ";

        statement += "WHERE ";

        boolean noWhereClause = true;
        if (!unitLeaves.isEmpty() && !unitConstraints.isEmpty()) {
            statement += unitConstraints + " ";
            noWhereClause = false;
        }

        if (!attributeLeaves.isEmpty() && attributeExistsLogic.isPresent()) {
            if (!noWhereClause) {
                statement += "AND ";
            }
            statement += attributeExistsLogic.get() + " ";
            noWhereClause = false;
        }

        if (noWhereClause) {
            statement += "1 = 1 ";
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
                            //log.trace("Prepared item: {}", item);
                            if (item instanceof AttributeSearchItem<?>) {
                                if (ccv.containsKey(UNIT_KERNEL_TENANTID.toString())) {
                                    int tenantId = (Integer) ccv.get(UNIT_KERNEL_TENANTID.toString()).getValue();
                                    pStmt.setInt(++i, tenantId);
                                }
                            }
                            Object value = item.getValue();
                            switch (item.getType()) {
                                case STRING -> {
                                    if (null != value)
                                        pStmt.setString(++i, (String) value);
                                    else
                                        pStmt.setNull(++i, java.sql.Types.VARCHAR);
                                }
                                case TIME -> {
                                    if (null != value)
                                        pStmt.setTimestamp(++i, instant2Timestamp((Instant) value));
                                    else
                                        pStmt.setNull(++i, java.sql.Types.TIMESTAMP);
                                }
                                case INTEGER -> {
                                    if (null != value)
                                        pStmt.setInt(++i, (Integer) value);
                                    else
                                        pStmt.setNull(++i, java.sql.Types.INTEGER);
                                }
                                case LONG -> {
                                    if (null != value)
                                        pStmt.setLong(++i, (Long) value);
                                    else
                                        pStmt.setNull(++i, java.sql.Types.BIGINT);
                                }
                                case DOUBLE -> {
                                    if (null != value)
                                        pStmt.setDouble(++i, (Double) value);
                                    else
                                        pStmt.setNull(++i, java.sql.Types.DOUBLE);
                                }
                                case BOOLEAN -> {
                                    if (null != value)
                                        pStmt.setBoolean(++i, (Boolean) value);
                                    else
                                        pStmt.setNull(++i, java.sql.Types.BOOLEAN);
                                }
                                default -> {
                                    // DATA and RECORD are not searchable
                                    throw new InvalidParameterException("Attribute type not searchable: " + item.getType());
                                }
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
