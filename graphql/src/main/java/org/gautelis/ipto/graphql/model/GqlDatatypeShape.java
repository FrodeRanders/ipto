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
package org.gautelis.ipto.graphql.model;

/*
 * enum DataTypes @datatypeRegistry {
 *    STRING    @datatype(id: 1)
 *    TIME      @datatype(id: 2)
 *    ...
 *    RECORD    @datatype(id: 99)
 * }
 *
 *    STRING    @datatype(id: 1)
 *      ^                     ^
 *      | (a)                 | (b)
 */
public class GqlDatatypeShape {
    public final String name;       // (a)
    public final int id;            // (b)

    public GqlDatatypeShape(String name, int id) {
        this.name = name;
        this.id = id;
    }

    public boolean equals(CatalogDatatype other) {
        return name.equals(other.name()) && id == other.type();
    }

    @Override
    public String toString() {
        String info = "GqlDatatypeShape{";
        info += "name='" + name + '\'';
        info += ", id=" + id;
        info += '}';
        return info;
    }
}
