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
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FieldAliasResolverTest {
    @Test
    void resolvesGqlByAliasNameAndQualifiedName() {
        GqlAttributeShape title = new GqlAttributeShape(
                "title",
                "STRING",
                false,
                "dcterms:title",
                "http://purl.org/dc/terms/title",
                null
        );
        Map<String, GqlAttributeShape> attributes = Map.of("title", title);

        assertEquals("title", FieldAliasResolver.resolveGqlAttribute(attributes, "title").orElseThrow().alias());
        assertEquals("title", FieldAliasResolver.resolveGqlAttribute(attributes, "dcterms:title").orElseThrow().alias());
        assertEquals("title", FieldAliasResolver.resolveGqlAttribute(attributes, "http://purl.org/dc/terms/title").orElseThrow().alias());
    }

    @Test
    void resolvesCatalogByAliasNameAndQualifiedName() {
        CatalogAttribute title = new CatalogAttribute(
                1,
                "title",
                "dcterms:title",
                "http://purl.org/dc/terms/title",
                AttributeType.STRING,
                false
        );
        Map<String, CatalogAttribute> attributes = Map.of("title", title);

        assertEquals("title", FieldAliasResolver.resolveCatalogAttribute(attributes, "title").orElseThrow().alias());
        assertEquals("title", FieldAliasResolver.resolveCatalogAttribute(attributes, "dcterms:title").orElseThrow().alias());
        assertEquals("title", FieldAliasResolver.resolveCatalogAttribute(attributes, "http://purl.org/dc/terms/title").orElseThrow().alias());
    }

    @Test
    void returnsEmptyForUnknownOrBlankValues() {
        Map<String, GqlAttributeShape> gql = Map.of();
        Map<String, CatalogAttribute> catalog = Map.of();

        assertTrue(FieldAliasResolver.resolveGqlAttribute(gql, null).isEmpty());
        assertTrue(FieldAliasResolver.resolveGqlAttribute(gql, " ").isEmpty());
        assertTrue(FieldAliasResolver.resolveCatalogAttribute(catalog, "unknown").isEmpty());
    }
}
