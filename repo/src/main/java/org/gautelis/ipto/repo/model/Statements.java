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
package org.gautelis.ipto.repo.model;

import org.gautelis.vopn.lang.Configurable;

public interface Statements {
    @Configurable(property = "sql.tenant.get_all")
    String tenantsGetAll();

    @Configurable(property = "sql.unit.delete")
    String unitDelete();

    @Configurable(property = "sql.unit.exists")
    String unitExists();

    @Configurable(property = "sql.unit.get_status")
    String unitGetStatus();

    @Configurable(property = "sql.unit.get")
    String unitGet();

    @Configurable(property = "sql.unit.get_latest")
    String unitGetLatest();

    @Configurable(property = "sql.unit.set_status")
    String unitSetStatus();

    @Configurable(property = "sql.unit.get_attributes")
    String unitGetAttributes();

    @Configurable(property = "sql.attribute.insert")
    String attributeInsert();

    @Configurable(property = "sql.attribute.delete")
    String attributeDelete();

    @Configurable(property = "sql.attribute.get_all")
    String attributeGetAll();

    @Configurable(property = "sql.lock.delete_all")
    String lockDeleteAll();

    @Configurable(property = "sql.lock.get_all")
    String lockGetAll();

    @Configurable(property = "sql.lock.insert")
    String lockInsert();

    @Configurable(property = "sql.log.get_entries")
    String logGetEntries();

    @Configurable(property = "sql.log.delete_entries")
    String logDeleteEntries();

    @Configurable(property = "sql.assoc.count_left_external_assocs")
    String assocCountLeftExternalAssocs();

    @Configurable(property = "sql.assoc.count_left_internal_assocs")
    String assocCountLeftInternalAssocs();

    @Configurable(property = "sql.assoc.count_right_external_assocs")
    String assocCountRightExternalAssocs();

    @Configurable(property = "sql.assoc.count_right_internal_assocs")
    String assocCountRightInternalAssocs();

    @Configurable(property = "sql.assoc.get_all_left_external_assocs")
    String assocGetAllLeftExternalAssocs();

    @Configurable(property = "sql.assoc.get_all_left_internal_assocs")
    String assocGetAllLeftInternalAssocs();

    @Configurable(property = "sql.assoc.get_all_right_external_assocs")
    String assocGetAllRightExternalAssocs();

    @Configurable(property = "sql.assoc.get_all_right_internal_assocs")
    String assocGetAllRightInternalAssocs();

    @Configurable(property = "sql.assoc.get_all_specific_external_assocs")
    String assocGetAllSpecificExternalAssocs();

    @Configurable(property = "sql.assoc.get_right_internal_assoc")
    String assocGetRightInternalAssoc();

    @Configurable(property = "sql.assoc.remove_all_external_assocs")
    String assocRemoveAllExternalAssocs();

    @Configurable(property = "sql.assoc.remove_all_internal_assocs")
    String assocRemoveAllInternalAssocs();

    @Configurable(property = "sql.assoc.remove_all_right_external_assocs")
    String assocRemoveAllRightExternalAssocs();

    @Configurable(property = "sql.assoc.remove_all_right_internal_assocs")
    String assocRemoveAllRightInternalAssocs();

    @Configurable(property = "sql.assoc.remove_specific_external_assoc")
    String assocRemoveSpecificExternalAssoc();

    @Configurable(property = "sql.assoc.remove_specific_internal_assoc")
    String assocRemoveSpecificInternalAssoc();

    @Configurable(property = "sql.assoc.store_external_assoc")
    String assocStoreExternalAssoc();

    @Configurable(property = "sql.assoc.store_internal_assoc")
    String assocStoreInternalAssoc();
}