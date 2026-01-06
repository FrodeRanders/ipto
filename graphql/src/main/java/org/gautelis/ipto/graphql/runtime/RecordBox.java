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

import java.util.*;

public class RecordBox extends AttributeBox {
    private static final Logger log = LoggerFactory.getLogger(RecordBox.class);

    private final Attribute<Attribute<?>> recordAttribute;

    /* package accessible only */
    RecordBox(Unit unit, Attribute<Attribute<?>> recordAttribute, Map</* field name */ String, Attribute<?>> attributes) {
        super(unit, attributes);

        Objects.requireNonNull(recordAttribute, "recordAttribute");
        this.recordAttribute = recordAttribute;

        log.trace("\u2193 Created {}", this);
    }

    /* package accessible only */
    RecordBox(Box parent, Attribute<Attribute<?>> recordAttribute, Map</* field name */ String, Attribute<?>> attributes) {
        this(Objects.requireNonNull(parent).getUnit(), recordAttribute, attributes);
    }

    public Attribute<Attribute<?>> getRecordAttribute() {
        return recordAttribute;
    }


    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("RecordBox{");
        sb.append("record-attribute='").append(recordAttribute.getName()).append('\'');
        sb.append(", attribute-alias='").append(recordAttribute.getAlias()).append('\'');
        sb.append(", ").append(super.toString());
        sb.append("}");
        return sb.toString();
    }
}
