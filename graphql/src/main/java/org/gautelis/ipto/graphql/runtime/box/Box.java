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
package org.gautelis.ipto.graphql.runtime.box;

import org.gautelis.ipto.repo.model.Unit;

import java.util.*;

public sealed abstract class Box permits AttributeBox {
    protected final Unit unit;

    protected Box(Unit unit) {
        Objects.requireNonNull(unit, "unit");
        this.unit = unit;
    }

    /* package accessible only */
    Box(Box parent) {
        this(Objects.requireNonNull(parent).getUnit());
    }

    public Unit getUnit() { return unit; }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("unit=").append(unit.getReference());
        return sb.toString();
    }
}
