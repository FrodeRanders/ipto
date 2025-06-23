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
package org.gautelis.repo.db;

public enum Table {
    // [U]ni[t]
    UNIT("repo_unit", "ut"),

    // [I]nternal [a]ssociation
    INTERNAL_ASSOCIATION("repo_internal_assoc", "ia"),

    // [E]xternal [a]ssociation
    EXTERNAL_ASSOCIATION("repo_external_assoc", "ea"),

    // [A]ttribute [v]alue
    ATTRIBUTE_VALUE("repo_attribute_value", "av"),

    // Attribute [v]alue [v]ectors
    ATTRIBUTE_STRING_VALUE_VECTOR("repo_string_vector", "vv"),
    ATTRIBUTE_INTEGER_VALUE_VECTOR("repo_integer_vector", "vv"),
    ATTRIBUTE_LONG_VALUE_VECTOR("repo_long_vector", "vv"),
    ATTRIBUTE_TIME_VALUE_VECTOR("repo_time_vector", "vv"),
    ATTRIBUTE_DOUBLE_VALUE_VECTOR("repo_double_vector", "vv"),
    ATTRIBUTE_BOOLEAN_VALUE_VECTOR("repo_boolean_vector", "vv"),
    ATTRIBUTE_COMPOUND_VALUE_VECTOR("repo_compound_vector", "vv");

    private final String tableName;
    private final String tableAlias;
    private final String compound;

    Table(String tableName, String tableAlias) {
        this.tableName = tableName;
        this.tableAlias = tableAlias;
        this.compound = tableName + " " + tableAlias;
    }

    private String getTableName() {
        return tableName;
    }

    private String getTableAlias() {
        return tableAlias;
    }

    @Override
    public String toString() {
        return compound;
    }
}
