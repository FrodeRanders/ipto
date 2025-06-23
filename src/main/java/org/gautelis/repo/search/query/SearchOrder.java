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

import org.gautelis.repo.db.Column;

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
     * Creates a basic search order, ordering on modification date in
     * descending order.
     *
     * @return the default search order
     */
    public static SearchOrder getDefaultOrder() {
        Column[] order = new Column[1];
        order[0] = Column.UNIT_CREATED;
        boolean[] asc = new boolean[1]; // default false, i.e. order descending
        return new SearchOrder(order, asc);
    }

    /**
     * Appends columns search order to buf.
     * <p>
     * An example could be the string ", ut.created" (including the leading comma).
     *
     * @param buf
     */
    public void appendColumns(StringBuilder buf) {
        if (columns.length > 0) {
            for (Column column : columns) {
                buf.append(", ");
                buf.append(column);
            }
            buf.append(" ");
        }
    }
}
