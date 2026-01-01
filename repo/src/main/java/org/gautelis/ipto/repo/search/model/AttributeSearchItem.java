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
package org.gautelis.ipto.repo.search.model;

import org.gautelis.ipto.repo.model.AttributeType;


public abstract class AttributeSearchItem<T> extends SearchItem<T> {

    private final String attrName;

    protected AttributeSearchItem(AttributeType type, Operator operator, String attrName) {
        super(type, operator);
        this.attrName = attrName;
    }

    public String getAttrName() {
        return attrName;
    }

    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();
        buf.append(super.toString());
        buf.append(", attribute-name='").append(attrName).append("'");
        return buf.toString();
    }
}
