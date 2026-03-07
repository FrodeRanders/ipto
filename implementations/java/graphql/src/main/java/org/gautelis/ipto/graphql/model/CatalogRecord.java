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

import java.util.List;

public record CatalogRecord(
        int recordAttrId,
        String recordName,
        List<CatalogAttribute> fields
) {
    public CatalogRecord {
        fields = List.copyOf(fields);
    }

    public CatalogRecord(String recordName) {
        this(-1, recordName, List.of());
    }

    public CatalogRecord withRecordId(int id) {
        return new CatalogRecord(id, recordName, fields);
    }

    public RecordKey key() {
        return new RecordKey(recordName);
    }

    @Override
    public String toString() {
        String info = "CatalogRecord{";
        info += "record-id=" + recordAttrId;
        info += ", record-name='" + recordName + '\'';
        info += ", fields=[";
        for (CatalogAttribute field : fields) {
            info += field.toString();
            info += ", ";
        }
        info += "]}";
        return info;
    }
}
