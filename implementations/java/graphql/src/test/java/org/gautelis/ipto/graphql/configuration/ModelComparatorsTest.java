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
package org.gautelis.ipto.graphql.configuration;

import org.gautelis.ipto.graphql.model.CatalogAttribute;
import org.gautelis.ipto.graphql.model.CatalogRecord;
import org.gautelis.ipto.graphql.model.CatalogTemplate;
import org.gautelis.ipto.graphql.model.GqlAttributeShape;
import org.gautelis.ipto.graphql.model.GqlFieldShape;
import org.gautelis.ipto.graphql.model.GqlRecordShape;
import org.gautelis.ipto.graphql.model.GqlTemplateShape;
import org.gautelis.ipto.repo.model.AttributeType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ModelComparatorsTest {
    @Test
    void recordMatchesIgnoresFieldCardinalityDifference() {
        GqlRecordShape gql = new GqlRecordShape(
                "Yrkan",
                "yrkan",
                "ffa:yrkan",
                List.of(new GqlFieldShape("Yrkan", "beskrivning", "String", false, false, "dcterms:description"))
        );
        CatalogRecord catalog = new CatalogRecord(
                18,
                "Yrkan",
                List.of(new CatalogAttribute(
                        9,
                        "beskrivning",
                        "dcterms:description",
                        "http://purl.org/dc/terms/description",
                        AttributeType.STRING,
                        true
                ))
        );

        Map<String, GqlAttributeShape> gqlAttributesByAlias = Map.of(
                "beskrivning",
                new GqlAttributeShape(
                        "beskrivning",
                        "STRING",
                        false,
                        "dcterms:description",
                        "http://purl.org/dc/terms/description",
                        null
                )
        );

        assertTrue(ModelComparators.recordMatches(gql, catalog, gqlAttributesByAlias));
    }

    @Test
    void templateMatchesIgnoresFieldCardinalityDifference() {
        GqlTemplateShape gql = new GqlTemplateShape(
                "Yrkan",
                "Yrkan",
                List.of(new GqlFieldShape("Yrkan", "datum", "DateTime", false, false, "dcterms:date"))
        );
        CatalogTemplate catalog = new CatalogTemplate(
                1,
                "Yrkan",
                List.of(new CatalogAttribute(
                        90,
                        "datum",
                        "dcterms:date",
                        "http://purl.org/dc/terms/date",
                        AttributeType.TIME,
                        true
                ))
        );

        Map<String, GqlAttributeShape> gqlAttributesByAlias = Map.of(
                "datum",
                new GqlAttributeShape(
                        "datum",
                        "TIME",
                        false,
                        "dcterms:date",
                        "http://purl.org/dc/terms/date",
                        null
                )
        );

        assertTrue(ModelComparators.templateMatches(gql, catalog, gqlAttributesByAlias));
    }

    @Test
    void stillDetectsTypeMismatch() {
        GqlRecordShape gql = new GqlRecordShape(
                "Yrkan",
                "yrkan",
                "ffa:yrkan",
                List.of(new GqlFieldShape("Yrkan", "beskrivning", "String", false, false, "dcterms:description"))
        );
        CatalogRecord catalog = new CatalogRecord(
                18,
                "Yrkan",
                List.of(new CatalogAttribute(
                        9,
                        "beskrivning",
                        "dcterms:description",
                        "http://purl.org/dc/terms/description",
                        AttributeType.TIME,
                        false
                ))
        );

        Map<String, GqlAttributeShape> gqlAttributesByAlias = Map.of(
                "beskrivning",
                new GqlAttributeShape(
                        "beskrivning",
                        "STRING",
                        false,
                        "dcterms:description",
                        "http://purl.org/dc/terms/description",
                        null
                )
        );

        assertFalse(ModelComparators.recordMatches(gql, catalog, gqlAttributesByAlias));
    }

    @Test
    void providesReasonCodesForMismatches() {
        GqlRecordShape gql = new GqlRecordShape(
                "Yrkan",
                "yrkan",
                "ffa:yrkan",
                List.of(new GqlFieldShape("Yrkan", "datum", "DateTime", false, false, "dcterms:date"))
        );
        CatalogRecord catalog = new CatalogRecord(
                18,
                "Yrkan",
                List.of(new CatalogAttribute(
                        90,
                        "other_alias",
                        "dcterms:description",
                        "http://purl.org/dc/terms/description",
                        AttributeType.STRING,
                        false
                ))
        );

        Map<String, GqlAttributeShape> gqlAttributesByAlias = Map.of(
                "datum",
                new GqlAttributeShape(
                        "datum",
                        "TIME",
                        false,
                        "dcterms:date",
                        "http://purl.org/dc/terms/date",
                        null
                )
        );

        List<String> reasons = ModelComparators.recordMismatchReasons(gql, catalog, gqlAttributesByAlias);
        assertTrue(reasons.stream().anyMatch(r -> r.startsWith("FIELD_ALIAS_MISMATCH")));
        assertTrue(reasons.stream().anyMatch(r -> r.startsWith("FIELD_BACKEND_NAME_MISMATCH")));
        assertTrue(reasons.stream().anyMatch(r -> r.startsWith("FIELD_TYPE_MISMATCH")));
    }
}
