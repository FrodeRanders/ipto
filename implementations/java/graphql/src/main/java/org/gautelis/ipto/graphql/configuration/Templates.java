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

import graphql.language.*;
import graphql.schema.idl.TypeDefinitionRegistry;
import org.gautelis.ipto.repo.db.Database;
import org.gautelis.ipto.graphql.model.*;
import org.gautelis.ipto.repo.model.AttributeType;
import org.gautelis.ipto.repo.model.Repository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public final class Templates {
    private static final Logger log = LoggerFactory.getLogger(Templates.class);

    private Templates() {}

    /*
     * type PurchaseOrder @template {
     *    orderId  : String    @use(attribute: ORDER_ID)
     *    shipment : Shipment! @use(attribute: SHIPMENT)
     * }
     *
     * type PurchaseOrder @template(name: Order) {
     *             ^                    ^
     *             | (a)                | (b)
     *
     *    orderId  : String    @use(attribute: ORDER_ID)
     *     ^           ^                         ^
     *     | (c)       | (d)                     | (e)
     */
    static Map<String, GqlTemplateShape> derive(
            TypeDefinitionRegistry registry,
            Map<String, GqlAttributeShape> attributes
    ) {
        Map<String, GqlTemplateShape> templates = new HashMap<>();

        for (ObjectTypeDefinition type : SdlObjectShapes.domainObjectTypes(registry)) {
            // --- (a) ---
            final String typeName = type.getName();

            // --- (b) ---
            List<Directive> templateDirectivesOnType = type.getDirectives("template");
            if (templateDirectivesOnType.isEmpty()) {
                continue;
            }

            String templateName = null; // INVALID

            for (Directive directive : templateDirectivesOnType) {
                Argument arg = directive.getArgument("name");
                if (null != arg) {
                    Value<?> value = arg.getValue();
                    templateName = SdlObjectShapes.extractName(value);
                    if (templateName == null || templateName.isBlank()) {
                        log.warn("↯ Unsupported @template(name: ...) value type '{}'", value.getClass().getSimpleName());
                    }
                }
            }

            if (null == templateName || templateName.isEmpty()) {
                templateName = typeName;
            }

            List<GqlFieldShape> templateFields = SdlObjectShapes.deriveFields(type, attributes, log, false);
            templates.put(typeName, new GqlTemplateShape(typeName, templateName, templateFields));
            log.trace("↯ Defining shape for {}: {}", typeName, templates.get(typeName));
        }

        return templates;
    }

    static Map<String, CatalogTemplate> read(
            Repository repository
    ) {
        Map<String, CatalogTemplate> templates = new HashMap<>();

        // repo_unit_template (
        //    templateid INT,   --
        //    name       TEXT   -- type name
        // )
        //
        // repo_template_elements (
        //    templateid INT,   --
        //    attrid     INT,   -- global attribute id
        //    idx        INT,   -- order / display position
        //    alias      TEXT   -- field name inside unit (template)
        // )
        //
        String sql = """
            SELECT ute.templateid, ut.name,
                   ute.idx, ute.attrid, ute.alias, a.attrname, a.qualname, a.attrtype, a.scalar
            FROM repo_unit_template AS ut
            LEFT JOIN repo_unit_template_elements ute
                ON ut.templateid = ute.templateid
            LEFT JOIN repo_attribute a
                ON ute.attrid = a.attrid
            ORDER BY ute.templateid, ute.idx;
        """;

        try {
            repository.withConnection(conn -> Database.useReadonlyPreparedStatement(conn, sql, pStmt -> {
                try (ResultSet rs = pStmt.executeQuery()) {
                    List<CatalogTemplate> catalogTemplates = new ArrayList<>();

                    Integer currentId = null;
                    String currentTemplateName = null;
                    List<CatalogAttribute> currentFields = new ArrayList<>();

                    while (rs.next()) {
                        // Template part
                        int templateId = rs.getInt("templateid");
                        String templateName = rs.getString("name");

                        // Field part
                        int idx = rs.getInt("idx");
                        if (rs.wasNull()) {
                            continue;
                        }

                        int fieldAttrId = rs.getInt("attrid");
                        String fieldAlias = rs.getString("alias");
                        String fieldAttrName = rs.getString("attrname");
                        String fieldQualname = rs.getString("qualname");
                        int fieldAttrType = rs.getInt("attrtype");
                        boolean isArray = !rs.getBoolean("scalar"); // Note negation

                        if (currentId == null || templateId != currentId) {
                            // boundary => flush previous
                            if (currentId != null) {
                                catalogTemplates.add(new CatalogTemplate(currentId, currentTemplateName, currentFields));
                            }
                            currentId = templateId;
                            currentTemplateName = templateName;
                            currentFields = new ArrayList<>();
                        }

                        CatalogAttribute attribute = new CatalogAttribute(
                                fieldAttrId,
                                fieldAlias, fieldAttrName, fieldQualname,
                                AttributeType.of(fieldAttrType), isArray
                        );
                        currentFields.add(attribute);
                    }
                    if (currentId != null) {
                        catalogTemplates.add(new CatalogTemplate(currentId, currentTemplateName, currentFields));
                    }

                    for (CatalogTemplate template : catalogTemplates) {
                        templates.put(template.templateName(), template);
                    }
                }
            }));
        } catch (SQLException sqle) {
            log.error("↯ Failed to load existing template: {}", Database.squeeze(sqle));
        }

        return templates;
    }
}
