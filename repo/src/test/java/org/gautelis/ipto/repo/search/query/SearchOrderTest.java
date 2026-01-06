/*
 * Copyright (C) 2025-2026 Frode Randers
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
