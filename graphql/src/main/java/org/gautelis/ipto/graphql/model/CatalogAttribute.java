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

public class CatalogAttribute {
    private int attrId = Attribute.INVALID_ATTRID;
    private final String alias;
    private final String attrName;
    private final String qualifiedName;
    private final AttributeType attrType;
    private final boolean isArray;

    public CatalogAttribute(String alias, String attrName, String qualifiedName, AttributeType attrType, boolean isArray) {
        this.alias = alias;
        this.attrName = attrName;
        this.qualifiedName = qualifiedName;
        this.attrType = attrType;
        this.isArray = isArray;
    }

    public void setAttrId(int attrId) {
        this.attrId = attrId;
    }

    public int attrId() {
        return attrId;
    }

    public String alias() {
        return alias;
    }

    public String attrName() {
        return attrName;
    }

    public String qualifiedName() {
        return qualifiedName;
    }

    public AttributeType attrType() {
        return attrType;
    }

    public boolean isArray() {
        return isArray;
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
