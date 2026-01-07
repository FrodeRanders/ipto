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
package org.gautelis.ipto.graphql.runtime;

import org.gautelis.ipto.repo.model.Unit;
import org.gautelis.ipto.repo.model.attributes.Attribute;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Objects;

public class PrimitiveBox extends Box {
    private static final Logger log = LoggerFactory.getLogger(PrimitiveBox.class);

    private final Attribute<?> attribute;
    private final ArrayList<?> values;

    /* package accessible only */
    PrimitiveBox(Unit unit, Attribute<?> attribute, ArrayList<?> values) {
        super(unit);

        Objects.requireNonNull(attribute, "attribut");
        this.attribute = attribute;

        Objects.requireNonNull(values, "values");
        this.values = values;

        log.trace("Created {}", this);
    }

    /* package accessible only */
    PrimitiveBox(Box parent, Attribute<?> attribute, ArrayList<?> values) {
        this(Objects.requireNonNull(parent).getUnit(), attribute, values);
    }

    /* package accessible only */
    Attribute<?> getAttribute() {
        return attribute;
    }

    /* package accessible only */
    ArrayList<?> getValues() {
        return values;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("PrimitiveBox{");
        sb.append(super.toString());
        sb.append(", attribute='").append(attribute.getName()).append('\'');
        sb.append(", values=").append(values);
        sb.append("}");
        return sb.toString();
    }
}
