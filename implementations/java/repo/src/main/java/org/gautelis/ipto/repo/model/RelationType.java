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
package org.gautelis.ipto.repo.model;

import org.gautelis.ipto.repo.exceptions.AssociationTypeException;

public enum RelationType implements Type {
    PARENT_CHILD_RELATION(1, /* allows multiples? */ true),
    REPLACEMENT_RELATION(3, /* allows multiples? */ false);

    private final int type;

    // Does this association accept multiple associations/relations?
    private final boolean allowsMultiples;

    RelationType(int type, boolean allowsMultiples) {
        this.type = type;
        this.allowsMultiples = allowsMultiples;
    }

    public static RelationType of(int type) throws AssociationTypeException {
        for (RelationType t : RelationType.values()) {
            if (t.type == type) {
                return t;
            }
        }
        throw new AssociationTypeException("Unknown relation type: " + type);
    }

    public int getType() {
        return type;
    }

    public boolean allowsMultiples() {
        return allowsMultiples;
    }
}
