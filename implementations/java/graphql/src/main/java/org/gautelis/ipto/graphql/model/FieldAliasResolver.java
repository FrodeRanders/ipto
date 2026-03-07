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

import java.util.Map;
import java.util.Optional;

public final class FieldAliasResolver {
    private FieldAliasResolver() {}

    public static Optional<GqlAttributeShape> resolveGqlAttribute(
            Map<String, GqlAttributeShape> attributesByAlias,
            String fieldOrNameOrQualifiedName
    ) {
        // GraphQL layer may refer to aliases, backend names, or qualified names.
        if (fieldOrNameOrQualifiedName == null || fieldOrNameOrQualifiedName.isBlank()) {
            return Optional.empty();
        }
        GqlAttributeShape byAlias = attributesByAlias.get(fieldOrNameOrQualifiedName);
        if (byAlias != null) {
            return Optional.of(byAlias);
        }
        for (GqlAttributeShape attribute : attributesByAlias.values()) {
            if (fieldOrNameOrQualifiedName.equals(attribute.name())
                    || fieldOrNameOrQualifiedName.equals(attribute.qualName())) {
                return Optional.of(attribute);
            }
        }
        return Optional.empty();
    }

    public static Optional<CatalogAttribute> resolveCatalogAttribute(
            Map<String, CatalogAttribute> attributesByAlias,
            String fieldOrNameOrQualifiedName
    ) {
        // Keep lookup behavior symmetrical between SDL and catalog viewpoints.
        if (fieldOrNameOrQualifiedName == null || fieldOrNameOrQualifiedName.isBlank()) {
            return Optional.empty();
        }
        CatalogAttribute byAlias = attributesByAlias.get(fieldOrNameOrQualifiedName);
        if (byAlias != null) {
            return Optional.of(byAlias);
        }
        for (CatalogAttribute attribute : attributesByAlias.values()) {
            if (fieldOrNameOrQualifiedName.equals(attribute.attrName())
                    || fieldOrNameOrQualifiedName.equals(attribute.qualifiedName())) {
                return Optional.of(attribute);
            }
        }
        return Optional.empty();
    }
}
