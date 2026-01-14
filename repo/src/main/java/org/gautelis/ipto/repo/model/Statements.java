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

    @Configurable(property = "sql.unit.attribute.add")
    String unitAttributeAdd();

    @Configurable(property = "sql.unit.attribute.remove")
    String unitAttributeRemove();

    @Configurable(property = "sql.attribute.get_all")
    String attributeGetAll();

    @Configurable(property = "sql.attribute.create")
    String attributeCreate();

    @Configurable(property = "sql.attribute.in_use")
    String attributeInUse();

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
    String countLeftExternalAssocs();

    @Configurable(property = "sql.relation.count_left_internal_relations")
    String countLeftInternalRelations();

    @Configurable(property = "sql.assoc.count_right_external_assocs")
    String countRightExternalAssocs();

    @Configurable(property = "sql.relation.count_right_internal_relations")
    String countRightInternalRelations();

    @Configurable(property = "sql.assoc.get_all_left_external_assocs")
    String getAllLeftExternalAssocs();

    @Configurable(property = "sql.relation.get_all_left_internal_relations")
    String getAllLeftInternalRelations();

    @Configurable(property = "sql.assoc.get_all_right_external_assocs")
    String getAllRightExternalAssocs();

    @Configurable(property = "sql.relation.get_all_right_internal_relations")
    String getAllRightInternalRelations();

    @Configurable(property = "sql.assoc.get_all_specific_external_assocs")
    String getAllSpecificExternalAssocs();

    @Configurable(property = "sql.relation.get_right_internal_relation")
    String getRightInternalRelation();

    @Configurable(property = "sql.assoc.remove_all_external_assocs")
    String removeAllExternalAssocs();

    @Configurable(property = "sql.relation.remove_all_internal_relations")
    String removeAllInternalRelations();

    @Configurable(property = "sql.assoc.remove_all_right_external_assocs")
    String removeAllRightExternalAssocs();

    @Configurable(property = "sql.relation.remove_all_right_internal_relations")
    String removeAllRightInternalRelations();

    @Configurable(property = "sql.assoc.remove_specific_external_assoc")
    String removeSpecificExternalAssoc();

    @Configurable(property = "sql.relation.remove_specific_internal_relation")
    String removeSpecificInternalRelation();

    @Configurable(property = "sql.assoc.store_external_assoc")
    String storeExternalAssoc();

    @Configurable(property = "sql.relation.store_internal_relation")
    String storeInternalRelation();
}