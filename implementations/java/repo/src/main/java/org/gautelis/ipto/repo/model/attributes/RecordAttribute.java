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

/**
 * Convenience wrapper for navigating a record-valued attribute.
 * <p>
 * Record attributes store nested {@link Attribute} instances. This wrapper
 * exposes the same callback-oriented access style as {@link Unit}, but scoped
 * to one record attribute and its nested members.
 */
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

    /**
     * Wraps a repository attribute known to be a record attribute.
     *
     * @param unit the owning unit
     * @param attr the underlying record attribute
     * @return a record-attribute wrapper
     */
    public static RecordAttribute from(Unit unit, Attribute<?> attr) {
        return new RecordAttribute(unit, attr);
    }

    /**
     * Wraps an already typed record attribute.
     *
     * @param unit the owning unit
     * @param attr the underlying record attribute
     * @return a record-attribute wrapper
     */
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
     * Resolves or creates a nested record attribute inside this record and
     * passes it to the callback.
     *
     * @param name the nested record attribute name
     * @param runnable callback receiving the resolved nested record
     */
    public void withRecordAttribute(
            String name,
            RecordAttributeRunnable runnable
    ) {
        withRecordAttribute(name, /* createIfMissing */ true, runnable);
    }

    /**
     * Resolves a nested record attribute and passes it to the callback.
     *
     * @param name the nested record attribute name
     * @param createIfMissing whether the nested record attribute should be
     *                        created when absent
     * @param runnable callback receiving the resolved nested record
     */
    public void withRecordAttribute(
            String name,
            boolean createIfMissing,
            RecordAttributeRunnable runnable
    ) {
        Objects.requireNonNull(runnable, "runnable");
        runnable.run(requireRecordAttribute(name, createIfMissing));
    }

    /**
     * Resolves a nested non-record attribute and passes the full attribute
     * object to the callback.
     *
     * @param name the nested attribute name
     * @param type the expected Java element type
     * @param runnable callback receiving the resolved attribute
     * @param <A> the expected Java element type
     */
    public <A> void withNestedAttribute(String name, Class<A> type, AttributeRunnable<A> runnable) {
        unit.withAttribute(delegate, name, type, runnable);
    }

    /**
     * Resolves a nested non-record attribute and passes only its value vector to
     * the callback.
     *
     * @param name the nested attribute name
     * @param type the expected Java element type
     * @param runnable callback receiving the resolved value vector
     * @param <A> the expected Java element type
     */
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

    /**
     * Returns the wrapped repository attribute.
     *
     * @return the underlying record attribute
     */
    public Attribute<Attribute<?>> getDelegate() {
        return delegate;
    }

    /**
     * Returns the nested attributes stored in this record.
     *
     * @return the value vector containing nested attributes
     */
    public ArrayList<Attribute<?>> getValue() {
        return delegate.getValueVector();
    }

    @Override
    public String toString() {
        return "RecordAttribute{unit=" + unit.toString() + ", delegate=" + delegate.toString() + "}";
    }
}
