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
package org.gautelis.repo.search.model;

import org.gautelis.repo.model.AttributeType;


public abstract class AttributeSearchItem<T> extends SearchItem<T> {

    private final int attrId;

    protected AttributeSearchItem(AttributeType type, Operator operator, int attrId) {
        super(type, operator);
        this.attrId = attrId;
    }

    public int getAttrId() {
        return attrId;
    }
}
