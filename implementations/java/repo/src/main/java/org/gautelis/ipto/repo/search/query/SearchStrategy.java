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

/**
 * SQL-generation strategies supported by the search subsystem.
 * <p>
 * {@link #SET_OPS} builds SQL from labeled constraint subqueries combined with
 * set operators such as {@code INTERSECT} and {@code UNION}. {@link #EXISTS}
 * drives the search from the unit kernel and appends predicate-specific
 * {@code EXISTS} clauses.
 */
public enum SearchStrategy {
    SET_OPS,
    EXISTS
}
