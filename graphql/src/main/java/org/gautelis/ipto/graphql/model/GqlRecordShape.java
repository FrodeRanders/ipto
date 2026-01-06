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

import graphql.language.ObjectTypeDefinition;
import graphql.language.Type;

import java.util.List;

/*
 * type Shipment @record(attribute: shipment) {
 *    shipmentId : String    @use(attribute: shipmentId)  <-- a FIELD in this record
 *
 * type Shipment @record(attribute: shipment) {
 *        ^                           ^
 *        | (a)                       | (b)
 *
 * Details about individual fields are found in GqlFieldShape
 */
public record GqlRecordShape(
        String typeName,           // (a)
        String attributeEnumName,  // (b)
        String attributeName,
        List<GqlFieldShape> fields
) {
    public boolean equals(CatalogRecord other) {
        return typeName.equals(other.recordName);
    }

    @Override
    public String toString() {
        String info = "GqlRecordShape{";
        info += "record-name='" + typeName + '\'';
        info += ", attribute-enum-name='" + attributeEnumName + '\'';
        info += ", attribute-name='" + attributeName + '\'';
        info += ", fields=[";
        for (GqlFieldShape field : fields) {
            info += field + ", ";
        }
        info += "]}";
        return info;
    }
}
