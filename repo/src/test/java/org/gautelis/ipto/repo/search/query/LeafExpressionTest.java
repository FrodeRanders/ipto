package org.gautelis.ipto.repo.search.query;

import org.gautelis.ipto.repo.db.Column;
import org.gautelis.ipto.repo.search.model.IntegerUnitSearchItem;
import org.gautelis.ipto.repo.search.model.Operator;
import org.gautelis.ipto.repo.search.model.TimeUnitSearchItem;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class LeafExpressionTest {

    @Test
    void toSqlRendersUnitConstraint() {
        LeafExpression<IntegerUnitSearchItem> expr =
                new LeafExpression<>(new IntegerUnitSearchItem(Column.UNIT_KERNEL_TENANTID, Operator.EQ, 7));

        String sql = expr.toSql(SearchStrategy.SET_OPS, false, Map.of(), Map.of());

        assertEquals("uk.tenantid = 7", sql);
    }

    @Test
    void toSqlRendersTimeConstraintWithLiteral() {
        Instant instant = Instant.parse("2024-08-26T10:15:30Z");
        LeafExpression<TimeUnitSearchItem> expr =
                new LeafExpression<>(new TimeUnitSearchItem(Column.UNIT_KERNEL_CREATED, Operator.GEQ, instant));

        String sql = expr.toSql(SearchStrategy.EXISTS, false, Map.of(), Map.of());

        assertEquals("uk.created >= TIMESTAMP '2024-08-26T10:15:30Z'", sql);
    }
}
