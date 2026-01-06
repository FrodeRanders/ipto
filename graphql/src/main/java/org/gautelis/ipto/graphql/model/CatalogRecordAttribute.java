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

import org.gautelis.ipto.repo.model.AttributeType;

public record CatalogRecordAttribute(
        int    attrId,
        String attrName,          // canonical registry name
        AttributeType attrType,   // your enum
        boolean isVector,
        boolean isRecord,         // true for compound/record attributes
        String  recordName    // if isRecord, which record it represents
) {
    @Override
    public String toString() {
        String info = "CatalogAttribute{";
        info += "attribute-id=" + attrId;
        info += ", attribute-name='" + attrName + '\'';
        info += ", attribute-type=" + attrType.name();
        info += ", is-vector=" + isVector;
        info += ", is-record=" + isRecord;
        if (isRecord) {
            info += ", record-name='" + recordName + '\'';
        }
        info += "}";
        return info;
    }
}
