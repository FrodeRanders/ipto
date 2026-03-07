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

/*
 * type PurchaseOrder @template {
 *    orderId  : String    @use(attribute: ORDER_ID)  <-- a FIELD in this unit
 *    ...
 * }
 *
 * type PurchaseOrder @template(name: Order) {
 *             ^                   ^
 *             | (a)               | (c)
 *
 * Details about individual fields are found in GqlFieldShape
 */
public record GqlTemplateShape(
        String typeName,           // (a)
        String templateName,       // (c)
        List<GqlFieldShape> fields
) {
    public TemplateKey key() {
        return new TemplateKey(typeName);
    }

    @Override
    public String toString() {
        String info = "GqlTemplateShape{";
        info += "type-name='" + typeName + '\'';
        if (null != templateName) {
            info += ", template-name=" + templateName;
        }
        info += ", fields=[";
        for (GqlFieldShape field : fields) {
            info += field + ", ";
        }
        info += "]}";
        return info;
    }
}
