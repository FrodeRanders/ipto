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

public record SearchOrder(Column[] columns, boolean[] ascending) {

    /**
     * @throws IllegalArgumentException
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
     * Creates a basic search order, ordering on creation date in
     * descending order.
     *
     * @return the default search order
     */
    public static SearchOrder getDefaultOrder() {
        return orderByCreation(false);
    }


    /**
     * Creates a basic search order, ordering on creation date.
     */
    public static SearchOrder orderByCreation(boolean ascending) {
        Column[] order = new Column[1];
        order[0] = Column.UNIT_KERNEL_CREATED;
        boolean[] asc = new boolean[1];
        asc[0] = ascending;

        return new SearchOrder(order, asc);
    }

    /**
     * Creates a basic search order, ordering on unit id.
     */
    public static SearchOrder orderByUnitId(boolean ascending) {
        Column[] order = new Column[1];
        order[0] = Column.UNIT_KERNEL_UNITID;
        boolean[] asc = new boolean[1];
        asc[0] = ascending;

        return new SearchOrder(order, asc);
    }

    /**
     * Creates a basic search order, ordering on last modified date.
     */
    public static SearchOrder orderByModified(boolean ascending) {
        Column[] order = new Column[1];
        order[0] = Column.UNIT_VERSION_MODIFIED;
        boolean[] asc = new boolean[1];
        asc[0] = ascending;

        return new SearchOrder(order, asc);
    }
}
