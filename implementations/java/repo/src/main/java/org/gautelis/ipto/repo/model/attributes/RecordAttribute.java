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
package org.gautelis.ipto.repo.model.attributes;

import org.gautelis.ipto.repo.model.AttributeType;
import org.gautelis.ipto.repo.model.Unit;

import java.util.ArrayList;
import java.util.Objects;

public class RecordAttribute {
    private final Attribute<Attribute<?>> delegate;
    private final Unit unit;

    private RecordAttribute(Attribute<Attribute<?>> attr, Unit unit) {
        Objects.requireNonNull(attr, "attr");
        Objects.requireNonNull(unit, "unit");

        this.delegate = attr;
        this.unit = unit;
    }

    public RecordAttribute(Unit unit, Attribute<?> attr) {
        this(asRecord(attr), unit);
    }

    public static RecordAttribute from(Unit unit, Attribute<?> attr) {
        return new RecordAttribute(unit, attr);
    }

    public static RecordAttribute wrap(Unit unit, Attribute<Attribute> attr) {
        @SuppressWarnings("unchecked")
        Attribute<Attribute<?>> nestedRecordAttr = (Attribute<Attribute<?>>) (Attribute<?>) attr;
        return new RecordAttribute(nestedRecordAttr, unit);
    }


    @SuppressWarnings("unchecked")
    private static Attribute<Attribute<?>> asRecord(Attribute<?> attr) {
        Objects.requireNonNull(attr, "attr");

        if (attr.getType() != AttributeType.RECORD) {
            throw new IllegalArgumentException("attribute must be RECORD (i.e. Attribute<Attribute<?>>)");
        }
        return (Attribute<Attribute<?>>) attr;
    }

    /**
     * Access a nested record attribute inside this record.
     *
     * Example:
     *
     * unit.withRecordAttribute("outerRecord", outer -> {
     *     outer.withRecordAttribute(unit, "innerRecord", inner -> {
     *         inner.withNestedAttributeValue(unit, "dmo:foo", String.class, values -> {
     *             values.add("bar");
     *         });
     *     });
     * });
     */
    public void withRecordAttribute(
            String name,
            RecordAttributeRunnable runnable
    ) {
        withRecordAttribute(name, /* createIfMissing */ true, runnable);
    }

    public void withRecordAttribute(
            String name,
            boolean createIfMissing,
            RecordAttributeRunnable runnable
    ) {
        Objects.requireNonNull(runnable, "runnable");
        runnable.run(requireRecordAttribute(name, createIfMissing));
    }

    public <A> void withNestedAttribute(String name, Class<A> type, AttributeRunnable<A> runnable) {
        unit.withAttribute(delegate, name, type, runnable);
    }

    public <A> void withNestedAttributeValue(String name, Class<A> type, AttributeValueRunnable<A> runnable) {
        withNestedAttribute(name, type, asAttributeRunnable(runnable));
    }

    private static <A> AttributeRunnable<A> asAttributeRunnable(AttributeValueRunnable<A> runnable) {
        return attr -> {
            ArrayList<A> value = attr.getValueVector();
            runnable.run(value);
        };
    }

    private RecordAttribute requireRecordAttribute(String name, boolean createIfMissing) {
         Objects.requireNonNull(name, "name");

        final RecordAttribute[] resolved = { null };
        unit.withAttribute(
                delegate,
                name,
                Attribute.class, // Java type for a record attribute’s value elements
                createIfMissing,
                attr -> {
                    // attr is the nested attribute (by name) inside this record.
                    if (AttributeType.RECORD != attr.getType()) {
                        throw new IllegalArgumentException("Not a record attribute: " + name);
                    }
                    resolved[0] = new RecordAttribute(unit, attr);
                }
        );
        if (resolved[0] == null) {
            throw new IllegalArgumentException("Unknown attribute: " + name);
        }
        return resolved[0];
    }

    public Attribute<Attribute<?>> getDelegate() {
        return delegate;
    }

    public ArrayList<Attribute<?>> getValue() {
        return delegate.getValueVector();
    }

    @Override
    public String toString() {
        return "RecordAttribute{unit=" + unit.toString() + ", delegate=" + delegate.toString() + "}";
    }
}
