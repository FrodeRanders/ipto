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
 * type Shipment @record(attribute: shipment) {
 *    shipmentId : String    @use(attribute: shipmentId)  <-- a FIELD in this record
 *    ...
 * }
 *
 * type Shipment @record(attribute: shipment) {
 *        ^
 *        | (a)
 *
 *    shipmentId  : String  @use(attribute: shipmentId)
 *        ^           ^                         ^
 *        | (b)       | (c)                     | (d)
 */
public record GqlFieldShape(
        String typeName,          // (a)
        String fieldName,         // (b)
        String gqlTypeRef,        // from (c)
        boolean isArray,          // from (c)
        boolean isMandatory,      // from (c)
        String usedAttributeName  // (d)
) {
    @Override
    public String toString() {
        String info = "GqlFieldShape{";
        info += "type-name='" + typeName + '\'';
        info += ", field-name='" + fieldName + '\'';
        info += ", type-ref='" + gqlTypeRef + '\'';
        info += ", used-attribute='" + usedAttributeName + '\'';
        info += ", is-array=" + isArray;
        info += ", is-mandatory=" + isMandatory;
        info += "]}";
        return info;
    }
}
