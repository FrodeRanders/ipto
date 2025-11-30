package org.gautelis.repo.graphql.configuration;

import graphql.language.*;
import graphql.schema.idl.TypeDefinitionRegistry;
import org.gautelis.repo.db.Database;
import org.gautelis.repo.exceptions.Stacktrace;
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


public final class Templates {
    private static final Logger log = LoggerFactory.getLogger(Templates.class);

    private Templates() {}

    /*
     * type PurchaseOrder @unit(id: 42) {
     *    orderId  : String    @use(attribute: ORDER_ID)
     *    shipment : Shipment! @use(attribute: SHIPMENT)
     * }
     *
     * type PurchaseOrder @unit(id: 42) {
     *             ^                 ^
     *             | (a)             | (b)
     *
     *    orderId  : String    @use(attribute: ORDER_ID)
     *     ^           ^                         ^
     *     | (c)       | (d)                     | (e)
     */
    static Map<String, GqlUnitShape> derive(TypeDefinitionRegistry registry, Map<String, GqlAttributeShape> attributes) {
        Map<String, GqlUnitShape> units = new HashMap<>();

        for (ObjectTypeDefinition type : registry.getTypes(ObjectTypeDefinition.class)) {
            // --- (a) ---
            final String typeName = type.getName();

            // Filter operations // TODO hard coded for the time being
            if ("query".equalsIgnoreCase(typeName)
                    || "mutation".equalsIgnoreCase(typeName)
                    || "subscription".equalsIgnoreCase(typeName)) {
                log.debug("Ignoring type: {}", typeName);
                continue;
            }

            // Filter unit template definitions, that has a @unit directive
            List<Directive> unitDirectivesOnType = type.getDirectives("unit");
            if (unitDirectivesOnType.isEmpty()) {
                log.debug("Ignoring type: {}", typeName);
                continue;
            }

            // --- (b) ---
            int templateId = -1; // INVALID
            String templateName = null; // INVALID

            for (Directive directive : unitDirectivesOnType) {
                Argument arg = directive.getArgument("id");
                if (null != arg) {
                    IntValue _id = (IntValue) arg.getValue();
                    templateId = _id.getValue().intValue();
                }

                arg = directive.getArgument("name");
                if (null != arg) {
                    StringValue _name = (StringValue) arg.getValue();
                    templateName = _name.getValue();
                }
            }

            if (/* VALID? */ templateId > 0 || (null != templateName && !templateName.isEmpty())) {
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
                        // No @use, but we will fall back on aliases
                        GqlAttributeShape attributeShape = attributes.get(fieldName);
                        if (null != attributeShape) {

                            unitFields.add(new GqlFieldShape(typeName, fieldName, fieldType.typeName(), fieldType.isArray(), fieldType.isMandatory(), attributeShape.name));
                        }
                    }
                }
                units.put(typeName, new GqlUnitShape(typeName, templateId, templateName, unitFields));
                log.trace("Defining shape for {}: {}", typeName, units.get(typeName));
            }
        }

        return units;
    }

    static Map<String, CatalogUnit> read(Repository repository) {
        Map<String, CatalogUnit> templates = new HashMap<>();

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
                        List<CatalogUnit> catalogTemplates = new ArrayList<>();

                        Integer currentId = null;
                        CatalogUnit currentTemplate = null;

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
                                currentTemplate = new CatalogUnit(templateId, templateName);
                            }

                            currentTemplate.addField(
                                    new CatalogAttribute(
                                            fieldAttrId, fieldAlias, fieldAttrName, fieldQualname,
                                            AttributeType.of(fieldAttrType), isArray
                                    )
                            );
                        }
                        if (currentTemplate != null) catalogTemplates.add(currentTemplate); // flush last one

                        for (CatalogUnit template : catalogTemplates) {
                            templates.put(template.templateName, template);
                        }
                    }
                });
            });
        } catch (SQLException sqle) {
            log.error("Failed to load existing template: {}", Database.squeeze(sqle));
        }

        return templates;
    }
}