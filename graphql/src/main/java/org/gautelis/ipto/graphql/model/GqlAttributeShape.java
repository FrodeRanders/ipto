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
 * enum Attributes @attributeRegistry {
 *     "The name given to the resource. It''s a human-readable identifier that provides a concise representation of the resource''s content."
 *     title @attribute(datatype: STRING, array: false, name: "dcterms:title", uri: "http://purl.org/dc/terms/title", description: "Namnet som ges till resursen...")
 *     ...
 *     shipmentId  @attribute(datatype: STRING)
 *     shipment    @attribute(datatype: RECORD, array: false)
 * }
 *
 * title @attribute(datatype: STRING, array: false, name: "dcterms:title", qualname: "http:...", description: "...")
 *   ^                          ^              ^                   ^                    ^                       ^
 *   | (a)                      | (c)          | (d)               | (e)                | (f)                   | (g)
 */
public record GqlAttributeShape(
        String alias,       // (a)
        String typeName,    // (c)
        boolean isArray,    // (d)
        String name,        // (e)
        String qualName,    // (f)
        String description  // (g)
) {
    public AttributeKey key() {
        return new AttributeKey(alias, name, qualName);
    }

    @Override
    public String toString() {
        String info = "GqlAttributeShape{";
        info += "alias=";
        if (null != alias) {
            info += '\'' + alias + '\'';
        }
        info += ", attribute-type='" + typeName + '\'';
        info += ", attribute-name=";
        if (null != name) {
            info += '\'' + name + '\'';
        }
        info += ", attribute-qname=";
        if (null != qualName) {
            info += '\'' + qualName + '\'';
        }
        info += ", is-array=" + isArray;
        info += ", description=...";
        info += '}';
        return info;
    }
}
