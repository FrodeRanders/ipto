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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

final class ModelComparators {
    private ModelComparators() {
    }

    static boolean attributeMatches(
            GqlAttributeShape gql,
            CatalogAttribute catalog
    ) {
        return attributeMismatchReasons(gql, catalog).isEmpty();
    }

    static boolean recordMatches(
            GqlRecordShape gql,
            CatalogRecord catalog,
            Map<String, GqlAttributeShape> gqlAttributesByAlias
    ) {
        return recordMismatchReasons(gql, catalog, gqlAttributesByAlias).isEmpty();
    }

    static boolean templateMatches(
            GqlTemplateShape gql,
            CatalogTemplate catalog,
            Map<String, GqlAttributeShape> gqlAttributesByAlias
    ) {
        return templateMismatchReasons(gql, catalog, gqlAttributesByAlias).isEmpty();
    }

    static List<String> attributeMismatchReasons(
            GqlAttributeShape gql,
            CatalogAttribute catalog
    ) {
        List<String> reasons = new ArrayList<>();
        if (gql == null || catalog == null) {
            reasons.add("NULL_VALUE");
            return reasons;
        }
        if (!Objects.equals(gql.alias(), catalog.alias())) {
            reasons.add("ALIAS_MISMATCH");
        }
        if (!Objects.equals(gql.name(), catalog.attrName())) {
            reasons.add("BACKEND_NAME_MISMATCH");
        }
        if (!Objects.equals(gql.qualName(), catalog.qualifiedName())) {
            reasons.add("QUALIFIED_NAME_MISMATCH");
        }
        if (!Objects.equals(gql.typeName(), catalog.attrType().name())) {
            reasons.add("TYPE_MISMATCH");
        }
        if (gql.isArray() != catalog.isArray()) {
            reasons.add("CARDINALITY_MISMATCH");
        }
        return reasons;
    }

    static List<String> recordMismatchReasons(
            GqlRecordShape gql,
            CatalogRecord catalog,
            Map<String, GqlAttributeShape> gqlAttributesByAlias
    ) {
        List<String> reasons = new ArrayList<>();
        if (gql == null || catalog == null) {
            reasons.add("NULL_VALUE");
            return reasons;
        }
        if (!Objects.equals(gql.typeName(), catalog.recordName())) {
            reasons.add("RECORD_NAME_MISMATCH");
            return reasons;
        }
        reasons.addAll(fieldMismatchReasons(gql.fields(), catalog.fields(), gqlAttributesByAlias));
        return reasons;
    }

    static List<String> templateMismatchReasons(
            GqlTemplateShape gql,
            CatalogTemplate catalog,
            Map<String, GqlAttributeShape> gqlAttributesByAlias
    ) {
        List<String> reasons = new ArrayList<>();
        if (gql == null || catalog == null) {
            reasons.add("NULL_VALUE");
            return reasons;
        }
        if (!Objects.equals(gql.templateName(), catalog.templateName())) {
            reasons.add("TEMPLATE_NAME_MISMATCH");
            return reasons;
        }
        reasons.addAll(fieldMismatchReasons(gql.fields(), catalog.fields(), gqlAttributesByAlias));
        return reasons;
    }

    private static List<String> fieldMismatchReasons(
            List<GqlFieldShape> gqlFields,
            List<CatalogAttribute> catalogFields,
            Map<String, GqlAttributeShape> gqlAttributesByAlias
    ) {
        List<String> reasons = new ArrayList<>();
        // Record/template elements are stored positionally in repo tables,
        // so we detect both missing aliases and order drift.
        if (gqlFields.size() != catalogFields.size()) {
            reasons.add("FIELD_COUNT_MISMATCH");
            return reasons;
        }

        Map<String, Integer> catalogFieldPositionByAlias = new HashMap<>();
        for (int i = 0; i < catalogFields.size(); i++) {
            catalogFieldPositionByAlias.put(catalogFields.get(i).alias(), i);
        }

        for (int i = 0; i < gqlFields.size(); i++) {
            GqlFieldShape gqlField = gqlFields.get(i);
            CatalogAttribute catalogField = catalogFields.get(i);

            // Field alias in record/template should match catalog alias from template elements.
            if (!Objects.equals(gqlField.fieldName(), catalogField.alias())) {
                Integer actualPosition = catalogFieldPositionByAlias.get(gqlField.fieldName());
                if (actualPosition != null) {
                    reasons.add("FIELD_ORDER_MISMATCH[idx=" + i + "]");
                } else {
                    reasons.add("FIELD_ALIAS_MISMATCH[idx=" + i + "]");
                }
            }
            // Backend attribute identity should match repo attribute name and qualified name.
            if (!Objects.equals(gqlField.usedAttributeName(), catalogField.attrName())) {
                reasons.add("FIELD_BACKEND_NAME_MISMATCH[idx=" + i + "]");
            }

            GqlAttributeShape gqlAttribute = findByBackendName(gqlAttributesByAlias, gqlField.usedAttributeName());
            if (gqlAttribute == null) {
                reasons.add("FIELD_ATTRIBUTE_SHAPE_MISSING[idx=" + i + "]");
                continue;
            }
            if (!Objects.equals(gqlAttribute.typeName(), catalogField.attrType().name())) {
                reasons.add("FIELD_TYPE_MISMATCH[idx=" + i + "]");
            }
            // Field cardinality is defined by SDL for query shape, but catalog persists
            // attribute cardinality globally; it is not reliable for template/record field matching.
        }
        return reasons;
    }

    private static GqlAttributeShape findByBackendName(
            Map<String, GqlAttributeShape> gqlAttributesByAlias,
            String backendAttributeName
    ) {
        for (GqlAttributeShape shape : gqlAttributesByAlias.values()) {
            if (Objects.equals(shape.name(), backendAttributeName)) {
                return shape;
            }
        }
        return null;
    }
}
