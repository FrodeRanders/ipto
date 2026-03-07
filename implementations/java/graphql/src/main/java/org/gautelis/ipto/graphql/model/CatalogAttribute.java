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
import org.gautelis.ipto.repo.model.attributes.Attribute;

public record CatalogAttribute(
        int attrId,
        String alias,
        String attrName,
        String qualifiedName,
        AttributeType attrType,
        boolean isArray
) {
    public CatalogAttribute(String alias, String attrName, String qualifiedName, AttributeType attrType, boolean isArray) {
        this(Attribute.INVALID_ATTRID, alias, attrName, qualifiedName, attrType, isArray);
    }

    public CatalogAttribute withAttrId(int id) {
        return new CatalogAttribute(id, alias, attrName, qualifiedName, attrType, isArray);
    }

    public AttributeKey key() {
        return new AttributeKey(alias, attrName, qualifiedName);
    }

    @Override
    public String toString() {
        String info = "CatalogAttribute{";
        info += "attribute-id=" + attrId;
        info += ", alias=";
        if (null != alias) {
            info += '\'' + alias + '\'';
        }
        info += ", attribute-name='" + attrName + '\'';
        info += ", attribute-qname='" + qualifiedName + '\'';
        info += ", attribute-type=" + attrType.name();
        info += ", is-array=" + isArray;
        info += "}";
        return info;
    }
}
