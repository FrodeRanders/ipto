package org.gautelis.ipto.repo.search.query;

import org.gautelis.ipto.repo.db.Column;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class SearchOrderTest {

    @Test
    void getDefaultOrderUsesCreationDescending() {
        SearchOrder order = SearchOrder.getDefaultOrder();

        assertArrayEquals(new Column[]{Column.UNIT_KERNEL_CREATED}, order.columns());
        assertArrayEquals(new boolean[]{false}, order.ascending());
    }

    @Test
    void orderByUnitIdRespectsAscending() {
        SearchOrder order = SearchOrder.orderByUnitId(true);

        assertArrayEquals(new Column[]{Column.UNIT_KERNEL_UNITID}, order.columns());
        assertArrayEquals(new boolean[]{true}, order.ascending());
    }

    @Test
    void constructorRejectsMismatchedVectors() {
        Column[] columns = {Column.UNIT_KERNEL_UNITID, Column.UNIT_KERNEL_CREATED};
        boolean[] ascending = {true};

        assertThrows(IllegalArgumentException.class, () -> new SearchOrder(columns, ascending));
    }
}
