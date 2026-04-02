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

import org.gautelis.ipto.repo.db.Column;

/**
 * Ordering specification for repository searches.
 *
 * @param columns the columns to order by, in priority order
 * @param ascending per-column sort directions matching {@code columns}
 */
public record SearchOrder(Column[] columns, boolean[] ascending) {

    /**
     * Creates an explicit search-order specification.
     *
     * @param columns the ordered columns
     * @param ascending the corresponding sort directions
     * @throws IllegalArgumentException if the vectors have different lengths
     */
    public SearchOrder(
            final Column[] columns, final boolean[] ascending
    ) {
        this.columns = columns;
        this.ascending = ascending;

        if (this.columns.length != this.ascending.length) {
            throw new IllegalArgumentException("Parameter vectors should have same length");
        }
    }

    /**
     * Returns the default search order.
     *
     * @return creation time descending
     */
    public static SearchOrder getDefaultOrder() {
        return orderByCreation(false);
    }


    /**
     * Creates an order by creation time.
     *
     * @param ascending whether the sort should be ascending
     * @return the requested search order
     */
    public static SearchOrder orderByCreation(boolean ascending) {
        Column[] order = new Column[1];
        order[0] = Column.UNIT_KERNEL_CREATED;
        boolean[] asc = new boolean[1];
        asc[0] = ascending;

        return new SearchOrder(order, asc);
    }

    /**
     * Creates an order by unit id.
     *
     * @param ascending whether the sort should be ascending
     * @return the requested search order
     */
    public static SearchOrder orderByUnitId(boolean ascending) {
        Column[] order = new Column[1];
        order[0] = Column.UNIT_KERNEL_UNITID;
        boolean[] asc = new boolean[1];
        asc[0] = ascending;

        return new SearchOrder(order, asc);
    }

    /**
     * Creates an order by last-modified timestamp.
     *
     * @param ascending whether the sort should be ascending
     * @return the requested search order
     */
    public static SearchOrder orderByModified(boolean ascending) {
        Column[] order = new Column[1];
        order[0] = Column.UNIT_VERSION_MODIFIED;
        boolean[] asc = new boolean[1];
        asc[0] = ascending;

        return new SearchOrder(order, asc);
    }
}
