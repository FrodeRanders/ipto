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
package org.gautelis.ipto.repo.model;

import org.gautelis.ipto.repo.exceptions.AttributeTypeException;
import org.gautelis.ipto.repo.model.attributes.Attribute;

import java.time.Instant;
import java.util.Arrays;
import java.util.Optional;

public enum AttributeType implements Type {
    STRING(1, String.class),
    TIME(2, Instant.class),
    INTEGER(3, Integer.class),
    LONG(4, Long.class),
    DOUBLE(5, Double.class),
    BOOLEAN(6, Boolean.class),
    DATA(7, Object.class), // Not searchable
    RECORD(99, Attribute.class);

    //
    private final int type;
    private final Class<?> javaClass; // compensate for type erasure

    AttributeType(int type, Class<?> javaClass) {
        this.type = type;
        this.javaClass = javaClass;
    }

    public static AttributeType of(int type) throws AttributeTypeException {
        for (AttributeType t : AttributeType.values()) {
            if (t.type == type) {
                return t;
            }
        }
        throw new AttributeTypeException("Unknown attribute type: " + type);
    }

    public static AttributeType of(String typeName) throws AttributeTypeException {
        for (AttributeType t : AttributeType.values()) {
            if (t.name().equals(typeName)) {
                return t;
            }
        }
        throw new AttributeTypeException("Unknown attribute type: " + typeName);
    }

    public int getType() {
        return type;
    }

    public Class<?> getJavaClass() {
        return javaClass;
    }

    public static Optional<AttributeType> fromJavaClass(Class<?> clazz) {
        return Arrays.stream(values())
                .filter(t -> t.getJavaClass().equals(clazz))
                .findFirst();
    }
}
