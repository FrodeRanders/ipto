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

import static org.gautelis.repo.model.AssociationType.CASE_ASSOCIATION;

/**
 *
 */
/* package accessible only */
final class CaseAssociation extends ExternalAssociation {

    /* package accessible only */
    CaseAssociation(ResultSet rs) throws DatabaseReadException, AssociationTypeException {
        super(rs);
    }

    /**
     * Counts number of cases that a unit is associated with.
     */
    /* package accessible only */
    static int countCases(
            Context ctx, int tenantId, long unitId
    ) throws DatabaseConnectionException, DatabaseReadException, InvalidParameterException {

        return AssociationManager.countRightAssociations(ctx, tenantId, unitId, CASE_ASSOCIATION);
    }

    /**
     * Counts number of units associated with a specific case.
     * <p>
     * The case is identified through a case reference string.
     */
    /* package accessible only */
    static int countUnits(
            Context ctx, String caseRef
    ) throws DatabaseConnectionException, DatabaseReadException, InvalidParameterException {

        return AssociationManager.countLeftAssociations(ctx, CASE_ASSOCIATION, caseRef);
    }

    /**
     * Gets case reference
     */
    /* package accessible only */
    String getCaseRef() {
        return getAssocString();
    }
}
