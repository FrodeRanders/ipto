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
package org.gautelis.ipto.graphql.model;

/**
 * Common GraphQL input shapes used by the Java runtime layer.
 */
public class Query {

    /**
     * Identifies a stored unit by tenant and unit id.
     *
     * @param tenantId tenant identifier
     * @param unitId unit identifier
     */
    public record UnitIdentification(int tenantId, long unitId) {}

    /**
     * Identifies a domain payload by correlation id.
     *
     * @param corrId correlation identifier
     */
    public record YrkanIdentification(String corrId) {}

    /**
     * Search filter supplied to GraphQL search operations.
     *
     * @param tenantId tenant scope for the search
     * @param where textual search predicate
     * @param offset row offset for paging
     * @param size page size
     * @param orderBy optional sort field
     * @param orderDirection optional sort direction
     */
    public record Filter(
            int tenantId,
            String where,
            int offset,
            int size,
            String orderBy,
            String orderDirection
    ) {}
}
