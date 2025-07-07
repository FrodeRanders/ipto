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

public enum Column {
    // [U]ni[t] columns
    UNIT_TENANTID("ut", "tenantid", false),
    UNIT_UNITID("ut", "unitid", false),
    UNIT_STATUS("ut", "status", true),
    UNIT_CORRID("ut", "corrid", true),
    UNIT_NAME("ut", "name", true),
    UNIT_CREATED("ut", "created", true),

    // [I]nternal [a]ssociation
    INTERNAL_ASSOC_TENANTID("ia", "tenantid", false),
    INTERNAL_ASSOC_UNITID("ia", "unitid", false),
    INTERNAL_ASSOC_TYPE("ia", "assoctype", false),
    INTERNAL_ASSOC_TO_TENANTID("ia", "assoctenantid", false),
    INTERNAL_ASSOC_TO_UNITID("ia", "assocunitid", false),

    // [E]xternal [a]ssociation
    EXTERNAL_ASSOC_TENANTID("ea", "tenantid", false),
    EXTERNAL_ASSOC_UNITID("ea", "unitid", false),
    EXTERNAL_ASSOC_TYPE("ea", "assoctype", false),
    EXTERNAL_ASSOC_STRING("ea", "assocstring", false),

    // [A]ttribute [v]alue columns
    ATTRIBUTE_VALUE_TENANTID("av", "tenantid", false),
    ATTRIBUTE_VALUE_UNITID("av", "unitid", false),
    ATTRIBUTE_VALUE_ATTRID("av", "attrid", false),
    ATTRIBUTE_VALUE_VALUEID("av", "valueid", false),

    // Attribute [v]alue [v]ector entry
    ATTRIBUTE_VALUE_VECTOR_TENANTID("vv", "tenantid", false),
    ATTRIBUTE_VALUE_VECTOR_UNITID("vv", "unitid", false),
    ATTRIBUTE_VALUE_VECTOR_VALUEID("vv", "valueid", false),
    ATTRIBUTE_VALUE_VECTOR_ENTRY ("vv", "val", false);


    private final String tableAlias;
    private final String columnName;
    private final String recrd;
    private final boolean supportsOrderBy;

    Column(String tableAlias,
           String columnName, boolean supportsOrderBy) {
        this.tableAlias = tableAlias;
        this.columnName = columnName;
        this.recrd = tableAlias + "." + columnName;

        this.supportsOrderBy = supportsOrderBy;
    }

    private String getColumnName() {
        return columnName;
    }

    private boolean supportsOrderBy() {
        return supportsOrderBy;
    }

    @Override
    public String toString() {
        return recrd;
    }
}
