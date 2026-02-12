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

import org.gautelis.ipto.graphql.model.*;
import org.gautelis.ipto.repo.model.Repository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

final class CatalogReconciler {
    private static final Logger log = LoggerFactory.getLogger(CatalogReconciler.class);

    private CatalogReconciler() {}

    static void reconcile(
            Repository repo,
            Configurator.GqlViewpoint gqlViewpoint,
            Configurator.CatalogViewpoint catalogViewpoint,
            PrintStream progress
    ) {
        // Work on index snapshots so newly created catalog entries can be
        // matched later in the same reconciliation pass.
        Map<AttributeKey, CatalogAttribute> catalogAttributesByKey = new HashMap<>(catalogViewpoint.attributes());
        Map<RecordKey, CatalogRecord> catalogRecordsByKey = new HashMap<>(catalogViewpoint.records());
        Map<TemplateKey, CatalogTemplate> catalogTemplatesByKey = new HashMap<>(catalogViewpoint.templates());

        Map<String, GqlAttributeShape> gqlAttributesByAlias = indexGqlAttributesByAlias(gqlViewpoint.attributes().values());
        Map<String, CatalogAttribute> catalogAttributesByAlias = indexCatalogAttributesByAlias(catalogViewpoint.attributes().values());

        for (String key : gqlViewpoint.datatypes().keySet()) {
            if (!catalogViewpoint.datatypes().containsKey(key)) {
                log.error("↯ Datatype '{}' not found in catalog", key);
                progress(progress, "Datatype '" + key + "' not found in catalog");
                continue;
            }
            GqlDatatypeShape gqlDatatype = gqlViewpoint.datatypes().get(key);
            CatalogDatatype iptoDatatype = catalogViewpoint.datatypes().get(key);
            if (!Objects.equals(gqlDatatype.name, iptoDatatype.name())
                    || gqlDatatype.id != iptoDatatype.type()) {
                log.error("↯ GraphQL SDL and catalog datatype do not match: {} != {}", gqlDatatype, iptoDatatype);
                progress(progress, "GraphQL SDL and catalog datatype do not match: " + gqlDatatype +  " != " + iptoDatatype);
            }
        }

        for (GqlAttributeShape gqlAttribute : gqlViewpoint.attributes().values()) {
            AttributeKey key = gqlAttribute.key();
            CatalogAttribute iptoAttribute = catalogAttributesByKey.get(key);
            if (iptoAttribute == null) {
                // Try to match existing catalog attributes by alias/name/qname and update in place
                // if the attribute can still be changed (i.e. not in use).
                iptoAttribute = findExistingCatalogAttribute(catalogAttributesByAlias, gqlAttribute);
            }

            if (iptoAttribute == null) {
                log.info("↯ Reconciling attribute '{}'...", gqlAttribute.alias());
                progress(progress, "Reconciling attribute '" + gqlAttribute.alias() + "'...");

                CatalogAttribute attribute = CatalogApplier.addAttribute(repo, gqlAttribute, progress);
                catalogViewpoint.attributes().put(attribute.key(), attribute);
                catalogAttributesByKey.put(attribute.key(), attribute);
                catalogAttributesByAlias.put(attribute.alias(), attribute);
                continue;
            }

            List<String> reasons = ModelComparators.attributeMismatchReasons(gqlAttribute, iptoAttribute);
            if (!reasons.isEmpty()) {
                Optional<CatalogAttribute> updated = CatalogApplier.updateAttributeIfMutable(
                        repo,
                        iptoAttribute,
                        gqlAttribute,
                        progress
                );
                if (updated.isPresent()) {
                    CatalogAttribute refreshed = updated.get();
                    catalogViewpoint.attributes().remove(iptoAttribute.key());
                    catalogViewpoint.attributes().put(refreshed.key(), refreshed);
                    catalogAttributesByKey.remove(iptoAttribute.key());
                    catalogAttributesByKey.put(refreshed.key(), refreshed);
                    if (iptoAttribute.alias() != null) {
                        catalogAttributesByAlias.remove(iptoAttribute.alias());
                    }
                    catalogAttributesByAlias.put(refreshed.alias(), refreshed);
                    continue;
                }

                String detail = formatReasons(reasons);
                log.error("↯ GraphQL SDL and catalog attribute do not match [{}]: {} != {}", detail, gqlAttribute, iptoAttribute);
                progress(progress, "GraphQL SDL and catalog attribute do not match [" + detail + "]: " + gqlAttribute + " != " + iptoAttribute);
            }
        }

        for (GqlRecordShape gqlRecord : gqlViewpoint.records().values()) {
            RecordKey key = gqlRecord.key();
            if (!catalogRecordsByKey.containsKey(key)) {
                log.info("↯ Reconciling record '{}'...", gqlRecord.typeName());
                progress(progress, "Reconciling record '" + gqlRecord.typeName() + "'...");

                CatalogRecord record = CatalogApplier.addRecord(
                        repo,
                        gqlRecord,
                        catalogAttributesByAlias,
                        progress
                );
                catalogViewpoint.records().put(record.key(), record);
                catalogRecordsByKey.put(record.key(), record);
                continue;
            }
            CatalogRecord iptoRecord = catalogRecordsByKey.get(key);
            List<String> reasons = ModelComparators.recordMismatchReasons(gqlRecord, iptoRecord, gqlAttributesByAlias);
            if (!reasons.isEmpty()) {
                String detail = formatReasons(reasons);
                log.error("↯ GraphQL SDL and catalog record do not match [{}]: {} != {}", detail, gqlRecord, iptoRecord);
                progress(progress, "GraphQL SDL and catalog record do not match [" + detail + "]: " + gqlRecord + " != " + iptoRecord);
            }
        }

        for (GqlTemplateShape gqlTemplate : gqlViewpoint.templates().values()) {
            TemplateKey key = gqlTemplate.key();
            if (!catalogTemplatesByKey.containsKey(key)) {
                log.info("↯ Reconciling template '{}'...", gqlTemplate.typeName());
                progress(progress, "Reconciling template '" + gqlTemplate.typeName() + "'...");

                CatalogTemplate template = CatalogApplier.addUnitTemplate(
                        repo,
                        gqlTemplate,
                        catalogAttributesByAlias,
                        progress
                );
                catalogViewpoint.templates().put(template.key(), template);
                catalogTemplatesByKey.put(template.key(), template);
                continue;
            }
            CatalogTemplate iptoTemplate = catalogTemplatesByKey.get(key);

            List<String> reasons = ModelComparators.templateMismatchReasons(gqlTemplate, iptoTemplate, gqlAttributesByAlias);
            if (!reasons.isEmpty()) {
                String detail = formatReasons(reasons);
                log.error("↯ GraphQL SDL and catalog template do not match [{}]: {} != {}", detail, gqlTemplate, iptoTemplate);
                progress(progress, "GraphQL SDL and catalog template do not match [" + detail + "]: " + gqlTemplate + " != " + iptoTemplate);
            }
        }
    }

    private static void progress(PrintStream progress, String message) {
        if (progress != null) {
            progress.println(message);
        }
    }

    private static String formatReasons(List<String> reasons) {
        return String.join(", ", reasons);
    }

    private static Map<String, GqlAttributeShape> indexGqlAttributesByAlias(Collection<GqlAttributeShape> attributes) {
        Map<String, GqlAttributeShape> indexed = new HashMap<>();
        for (GqlAttributeShape attribute : attributes) {
            indexed.put(attribute.alias(), attribute);
        }
        return indexed;
    }

    private static Map<String, CatalogAttribute> indexCatalogAttributesByAlias(Collection<CatalogAttribute> attributes) {
        Map<String, CatalogAttribute> indexed = new HashMap<>();
        for (CatalogAttribute attribute : attributes) {
            indexed.put(attribute.alias(), attribute);
        }
        return indexed;
    }

    private static CatalogAttribute findExistingCatalogAttribute(
            Map<String, CatalogAttribute> catalogAttributesByAlias,
            GqlAttributeShape gqlAttribute
    ) {
        if (gqlAttribute == null) {
            return null;
        }

        CatalogAttribute byAlias = catalogAttributesByAlias.get(gqlAttribute.alias());
        if (byAlias != null) {
            return byAlias;
        }

        Optional<CatalogAttribute> byBackendName = FieldAliasResolver.resolveCatalogAttribute(
                catalogAttributesByAlias, gqlAttribute.name()
        );
        if (byBackendName.isPresent()) {
            return byBackendName.get();
        }

        return FieldAliasResolver.resolveCatalogAttribute(
                catalogAttributesByAlias, gqlAttribute.qualName()
        ).orElse(null);
    }
}
