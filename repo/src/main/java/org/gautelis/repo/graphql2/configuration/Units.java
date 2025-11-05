package org.gautelis.repo.graphql2.configuration;

import graphql.language.*;
import graphql.schema.idl.TypeDefinitionRegistry;
import org.gautelis.repo.db.Database;
import org.gautelis.repo.graphql2.model.*;
import org.gautelis.repo.graphql2.model.TypeDef;
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


public final class Units {
    private static final Logger log = LoggerFactory.getLogger(Units.class);

    private Units() {}

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
            final String unitName = type.getName();

            List<Directive> unitDirectivesOnType = type.getDirectives("unit");
            if (!unitDirectivesOnType.isEmpty()) {

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
                        final TypeDef fieldType = TypeDef.of(f.getType());

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
                                    String fieldAttributeName = fieldAttributeDef.attributeName;

                                    unitFields.add(new GqlFieldShape(unitName, fieldName, fieldType.typeName(), fieldType.isArray(), fieldType.isMandatory(), fieldAttributeName));
                                    break; // In the unlikely case there are several @use
                                }
                            }
                        }
                    }
                    units.put(unitName, new GqlUnitShape(unitName, templateId, templateName, unitFields));
                }
            }
        }

        return units;
    }

    static Map<String, CatalogTemplate> read(Repository repository) {
        Map<String, CatalogTemplate> templates = new HashMap<>();

        // repo_template_elements (
        //    templateid INT  NOT NULL,   -- from @unit(id: …)
        //    attrid     INT  NOT NULL,   -- global attribute id
        //    alias      TEXT NOT NULL,   -- field name inside unit (template)
        //    idx        INT  NOT NULL,   -- order / display position
        // )
        //
        // repo_unit_template (
        //    templateid INT  NOT NULL,   -- from @unit(id: …)
        //    name       TEXT NOT NULL,   -- type name
        // )
        String sql = """
            SELECT ut.templateid, ut.name,
                   te.idx, te.attrid, te.alias, a.attrname, a.qualname, a.attrtype, a.scalar
            FROM repo_unit_template AS ut
            LEFT JOIN repo_template_elements te
                ON ut.templateid = te.templateid
            LEFT JOIN repo_attribute a
                ON te.attrid = a.attrid
            ORDER BY ut.templateid, te.idx;
        """;

        try {
            repository.withConnection(conn -> {
                Database.useReadonlyPreparedStatement(conn, sql, pStmt -> {
                    try (ResultSet rs = pStmt.executeQuery()) {
                        List<CatalogTemplate> catalogTemplates = new ArrayList<>();

                        Integer currentId = null;
                        CatalogTemplate currentTemplate = null;

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
                                currentTemplate = new CatalogTemplate(templateName, templateId);
                            }

                            currentTemplate.addField(
                                    new CatalogAttribute(
                                            fieldAttrId, fieldAlias, fieldAttrName, fieldQualname,
                                            AttributeType.of(fieldAttrType), isArray
                                    )
                            );
                        }
                        if (currentTemplate != null) catalogTemplates.add(currentTemplate); // flush last one

                        for (CatalogTemplate template : catalogTemplates) {
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