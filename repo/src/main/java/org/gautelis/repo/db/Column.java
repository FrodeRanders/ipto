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

    // [U]nit [k]ernel columns
    UNIT_KERNEL_TENANTID(Table.UNIT_KERNEL.getTableAlias(), "tenantid", false),
    UNIT_KERNEL_UNITID(Table.UNIT_KERNEL.getTableAlias(), "unitid", false),
    UNIT_KERNEL_CORRID(Table.UNIT_KERNEL.getTableAlias(), "corrid", true),
    UNIT_KERNEL_STATUS(Table.UNIT_KERNEL.getTableAlias(), "status", true),
    UNIT_KERNEL_LASTVER(Table.UNIT_KERNEL.getTableAlias(), "lastver", false),
    UNIT_KERNEL_CREATED(Table.UNIT_KERNEL.getTableAlias(), "created", true),

    // [U]nit [v]ersion columns
    UNIT_VERSION_TENANTID(Table.UNIT_VERSION.getTableAlias(), "tenantid", false),
    UNIT_VERSION_UNITID(Table.UNIT_VERSION.getTableAlias(), "unitid", false),
    UNIT_VERSION_UNITVER(Table.UNIT_VERSION.getTableAlias(), "unitver", false),
    UNIT_VERSION_UNITNAME(Table.UNIT_VERSION.getTableAlias(), "unitname", false),
    UNIT_VERSION_MODIFIED(Table.UNIT_VERSION.getTableAlias(), "modified", true),

    // [I]nternal [a]ssociation
    INTERNAL_ASSOC_TENANTID(Table.INTERNAL_ASSOCIATION.getTableAlias(), "tenantid", false),
    INTERNAL_ASSOC_UNITID(Table.INTERNAL_ASSOCIATION.getTableAlias(), "unitid", false),
    INTERNAL_ASSOC_TYPE(Table.INTERNAL_ASSOCIATION.getTableAlias(), "assoctype", false),
    INTERNAL_ASSOC_TO_TENANTID(Table.INTERNAL_ASSOCIATION.getTableAlias(), "assoctenantid", false),
    INTERNAL_ASSOC_TO_UNITID(Table.INTERNAL_ASSOCIATION.getTableAlias(), "assocunitid", false),

    // [E]xternal [a]ssociation
    EXTERNAL_ASSOC_TENANTID(Table.EXTERNAL_ASSOCIATION.getTableAlias(), "tenantid", false),
    EXTERNAL_ASSOC_UNITID(Table.EXTERNAL_ASSOCIATION.getTableAlias(), "unitid", false),
    EXTERNAL_ASSOC_TYPE(Table.EXTERNAL_ASSOCIATION.getTableAlias(), "assoctype", false),
    EXTERNAL_ASSOC_STRING(Table.EXTERNAL_ASSOCIATION.getTableAlias(), "assocstring", false),

    // [A]ttribute [v]alue columns
    ATTRIBUTE_VALUE_TENANTID(Table.ATTRIBUTE_VALUE.getTableAlias(), "tenantid", false),
    ATTRIBUTE_VALUE_UNITID(Table.ATTRIBUTE_VALUE.getTableAlias(), "unitid", false),
    ATTRIBUTE_VALUE_ATTRID(Table.ATTRIBUTE_VALUE.getTableAlias(), "attrid", false),
    ATTRIBUTE_VALUE_ATTRVER(Table.ATTRIBUTE_VALUE.getTableAlias(), "attrver", false),
    ATTRIBUTE_VALUE_VERFROM(Table.ATTRIBUTE_VALUE.getTableAlias(), "unitverfrom", false),
    ATTRIBUTE_VALUE_VERTO(Table.ATTRIBUTE_VALUE.getTableAlias(), "unitverto", false),
    ATTRIBUTE_VALUE_VALUEID(Table.ATTRIBUTE_VALUE.getTableAlias(), "valueid", false),

    // Attribute [v]alue [v]ector (multiple) entry, alias same for all value vectors, so picking one
    ATTRIBUTE_VALUE_VECTOR_TENANTID(Table.ATTRIBUTE_INTEGER_VALUE_VECTOR.getTableAlias(), "tenantid", false),
    ATTRIBUTE_VALUE_VECTOR_UNITID(Table.ATTRIBUTE_INTEGER_VALUE_VECTOR.getTableAlias(), "unitid", false),
    ATTRIBUTE_VALUE_VECTOR_VALUEID(Table.ATTRIBUTE_INTEGER_VALUE_VECTOR.getTableAlias(), "valueid", false),
    ATTRIBUTE_VALUE_VECTOR_ENTRY (Table.ATTRIBUTE_INTEGER_VALUE_VECTOR.getTableAlias(), "value", false);


    private final String tableAlias;
    private final String columnName;
    private final String recrd;
    private final boolean supportsOrderBy;

    Column(String tableAlias, String columnName,  boolean supportsOrderBy) {
        this.tableAlias = tableAlias;
        this.columnName = columnName; // unqualified name
        this.recrd = tableAlias + "." + columnName; // qualified name
        this.supportsOrderBy = supportsOrderBy;
    }

    public String plain() {
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
