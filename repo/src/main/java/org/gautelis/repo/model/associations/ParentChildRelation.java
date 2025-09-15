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
package org.gautelis.repo.model.associations;

import org.gautelis.repo.exceptions.AssociationTypeException;
import org.gautelis.repo.exceptions.DatabaseConnectionException;
import org.gautelis.repo.exceptions.DatabaseReadException;
import org.gautelis.repo.exceptions.InvalidParameterException;
import org.gautelis.repo.model.Context;

import java.sql.ResultSet;

import static org.gautelis.repo.model.AssociationType.PARENT_CHILD_RELATION;

/**
 *
 */
class ParentChildRelation extends InternalRelation {

    ParentChildRelation(ResultSet rs) throws DatabaseReadException, AssociationTypeException {
        super(rs);
    }

    /**
     * Counts number of parents that a unit is associated with.
     * <p>
     * Currently, this number is restricted to 0 or 1.
     */
    protected static int countParents(
            Context ctx, int tenantId, long unitId
    ) throws DatabaseConnectionException, DatabaseReadException, InvalidParameterException {

        return AssociationManager.countRightAssociations(ctx, tenantId, unitId, PARENT_CHILD_RELATION);
    }

    /**
     * Counts number of children (for a specific parent).
     */
    protected static int countChildren(
            Context ctx, int tenantId, long unitId
    ) throws DatabaseConnectionException, DatabaseReadException, InvalidParameterException {

        return AssociationManager.countLeftAssociations(ctx, PARENT_CHILD_RELATION, tenantId, unitId);
    }

    public int getParentTenantId() {
        return getRelationTenantId();
    }

    public long getParentUnitId() {
        return getRelationUnitId();
    }
}
