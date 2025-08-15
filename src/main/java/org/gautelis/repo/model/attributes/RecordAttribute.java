/*
 * Copyright (C) 2025 Frode Randers
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
package org.gautelis.repo.model.attributes;

import org.gautelis.repo.model.AttributeType;
import org.gautelis.repo.model.Unit;

import java.util.ArrayList;
import java.util.Objects;

public class RecordAttribute {
    private final Attribute<Attribute<?>> delegate;

    public RecordAttribute(Attribute<?> attr) {
        Objects.requireNonNull(attr, "attr");
        if (attr.getType() != AttributeType.RECORD) {
            throw new IllegalArgumentException("attribute must be RECORD (i.e. Attribute<Attribute<?>>)");
        }
        this.delegate = asCompound(attr);
    }

    @SuppressWarnings("unchecked")
    private static Attribute<Attribute<?>> asCompound(Attribute<?> attr) {
        return (Attribute<Attribute<?>>) attr;
    }

    public <A> void withNestedAttribute(Unit unit, String name, Class<A> type, AttributeRunnable<A> runnable) {
        Objects.requireNonNull(unit, "unit");
        unit.withAttribute(delegate, name, type, runnable);
    }

    public <A> void withNestedAttributeValue(Unit unit, String name, Class<A> type, AttributeValueRunnable<A> runnable) {
        withNestedAttribute(unit, name, type, attr -> {
            ArrayList<A> value = attr.getValueVector();
            runnable.run(value);
        });
    }

    public ArrayList<Attribute<?>> getValue() {
        return delegate.getValueVector();
    }

    @Override
    public String toString() {
        return "CompoundAttribute{delegate=" + delegate.toString() + "}";
    }
}