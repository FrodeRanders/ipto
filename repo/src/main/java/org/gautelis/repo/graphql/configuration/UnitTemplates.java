package org.gautelis.repo.graphql.configuration;

import graphql.language.*;
import graphql.schema.idl.TypeDefinitionRegistry;
import org.gautelis.repo.db.Database;
import org.gautelis.repo.graphql.model.*;
import org.gautelis.repo.graphql.model.TypeDefinition;
import org.gautelis.repo.model.AttributeType;
import org.gautelis.repo.model.Repository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public final class UnitTemplates {
    private static final Logger log = LoggerFactory.getLogger(UnitTemplates.class);

    private UnitTemplates() {}

    /*
     * type PurchaseOrder @unit {
     *    orderId  : String    @use(attribute: ORDER_ID)
     *    shipment : Shipment! @use(attribute: SHIPMENT)
     * }
     *
     * type PurchaseOrder @unit(name: purchase_order) {
     *             ^                    ^
     *             | (a)                | (b)
     *
     *    orderId  : String    @use(attribute: ORDER_ID)
     *     ^           ^                         ^
     *     | (c)       | (d)                     | (e)
     */
    static Map<String, GqlUnitTemplateShape> derive(
            TypeDefinitionRegistry registry,
            Map<String, GqlAttributeShape> attributes
    ) {
        Map<String, GqlUnitTemplateShape> units = new HashMap<>();

        for (ObjectTypeDefinition type : registry.getTypes(ObjectTypeDefinition.class)) {
            // --- (a) ---
            final String typeName = type.getName();

            // Filter operations // TODO hard coded for the time being
            if ("query".equalsIgnoreCase(typeName)
                    || "mutation".equalsIgnoreCase(typeName)
                    || "subscription".equalsIgnoreCase(typeName)) {
                continue;
            }

            // Filter unit template definitions, that has a @unit directive
            List<Directive> unitDirectivesOnType = type.getDirectives("unit");
            if (unitDirectivesOnType.isEmpty()) {
                continue;
            }

            // --- (b) ---
            String templateName = null; // INVALID

            for (Directive directive : unitDirectivesOnType) {
                Argument arg = directive.getArgument("name");
                if (null != arg) {
                    StringValue _name = (StringValue) arg.getValue();
                    templateName = _name.getValue();
                }
            }

            if (null != templateName && !templateName.isEmpty()) {
                 List<GqlFieldShape> unitFields = new ArrayList<>();

                // Handle field definitions on types
                for (FieldDefinition f : type.getFieldDefinitions()) {
                    // --- (c) ---
                    final String fieldName = f.getName();

                    // --- (d) ---
                    final TypeDefinition fieldType = TypeDefinition.of(f.getType());

                    // Handle @use directive on field definitions
                    List<Directive> useDirectives = f.getDirectives("use");
                    if (!useDirectives.isEmpty()) {
                        for (Directive useDirective : useDirectives) {
                            // @use "attribute" argument
                            Argument arg = useDirective.getArgument("attribute");
                            EnumValue value = (EnumValue) arg.getValue();

                            GqlAttributeShape fieldAttributeDef = attributes.get(value.getName());
                            if (null != fieldAttributeDef) {
                                // --- (e) ---
                                String fieldAttributeName = fieldAttributeDef.name;

                                unitFields.add(new GqlFieldShape(typeName, fieldName, fieldType.typeName(), fieldType.isArray(), fieldType.isMandatory(), fieldAttributeName));
                                break; // In the unlikely case there are several @use
                            }
                        }
                    } else {
                        // No @use, so we will fall back on aliases
                        GqlAttributeShape attributeShape = attributes.get(fieldName);
                        if (null != attributeShape) {
                            unitFields.add(new GqlFieldShape(typeName, fieldName, fieldType.typeName(), fieldType.isArray(), fieldType.isMandatory(), attributeShape.name));
                        }
                    }
                }
                units.put(typeName, new GqlUnitTemplateShape(typeName, templateName, unitFields));
                log.trace("\u21af Defining shape for {}: {}", typeName, units.get(typeName));
            }
        }

        return units;
    }

    static Map<String, CatalogUnitTemplate> read(
            Repository repository
    ) {
        Map<String, CatalogUnitTemplate> templates = new HashMap<>();

        // repo_unit_template (
        //    templateid INT,   -- from @unit(id: …)
        //    name       TEXT   -- type name
        // )
        //
        // repo_unit template_elements (
        //    templateid INT,   -- from @unit(id: …)
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
            repository.withConnection(conn -> {
                Database.useReadonlyPreparedStatement(conn, sql, pStmt -> {
                    try (ResultSet rs = pStmt.executeQuery()) {
                        List<CatalogUnitTemplate> catalogTemplates = new ArrayList<>();

                        Integer currentId = null;
                        CatalogUnitTemplate currentTemplate = null;

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
                                if (currentTemplate != null) catalogTemplates.add(currentTemplate);
                                currentId = templateId;
                                currentTemplate = new CatalogUnitTemplate(templateId, templateName);
                            }

                            CatalogAttribute attribute = new CatalogAttribute(
                                    fieldAlias, fieldAttrName, fieldQualname,
                                    AttributeType.of(fieldAttrType), isArray
                            );
                            attribute.setAttrId(fieldAttrId);
                            currentTemplate.addField(attribute);
                        }
                        if (currentTemplate != null) catalogTemplates.add(currentTemplate); // flush last one

                        for (CatalogUnitTemplate template : catalogTemplates) {
                            templates.put(template.templateName, template);
                        }
                    }
                });
            });
        } catch (SQLException sqle) {
            log.error("\u21af Failed to load existing template: {}", Database.squeeze(sqle));
        }

        return templates;
    }
}