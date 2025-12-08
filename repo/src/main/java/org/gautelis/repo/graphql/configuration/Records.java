package org.gautelis.repo.graphql.configuration;

import graphql.language.*;
import graphql.schema.idl.TypeDefinitionRegistry;
import org.gautelis.repo.db.Database;
import org.gautelis.repo.exceptions.ConfigurationException;
import org.gautelis.repo.exceptions.Stacktrace;
import org.gautelis.repo.graphql.model.*;
import org.gautelis.repo.graphql.model.TypeDefinition;
import org.gautelis.repo.model.AttributeType;
import org.gautelis.repo.model.Repository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;


public final class Records {
    private static final Logger log = LoggerFactory.getLogger(Records.class);

    private Records() {
    }

    /*
     * type Shipment @record(attribute: SHIPMENT) {
     *    shipmentId  : String  @use(attribute: SHIPMENT_ID)
     *    deadline : DateTime   @use(attribute: DEADLINE)
     *    reading  : [Float]    @use(attribute: READING)
     * }
     *
     * type Shipment @record(attribute: SHIPMENT) {
     *        ^                            ^
     *        | (a)                        | (b)
     *
     *    shipmentId  : String  @use(attribute: SHIPMENT_ID)
     *        ^           ^                         ^
     *        | (c)       | (d)                     | (e)
     */
    static Map<String, GqlRecordShape> derive(TypeDefinitionRegistry registry, Map<String, GqlAttributeShape> attributes) {
        Map<String, GqlRecordShape> records = new HashMap<>();

        for (ObjectTypeDefinition type : registry.getTypes(ObjectTypeDefinition.class)) {
            // --- (a) ---
            String typeName = type.getName();

            // Filter operations // TODO hard coded for the time being
            if ("query".equalsIgnoreCase(typeName)
             || "mutation".equalsIgnoreCase(typeName)
             || "subscription".equalsIgnoreCase(typeName)) {
                continue;
            }

            // Filter unit template definitions, that has a @unit directive
            List<Directive> unitDirectivesOnType = type.getDirectives("unit");
            if (!unitDirectivesOnType.isEmpty()) {
                continue;
            }

            String recordAttributeName = null;

            // --- (b) ---
            List<Directive> recordDirectivesOnType = type.getDirectives("record");
            if (!recordDirectivesOnType.isEmpty()) {
                for (Directive directive : recordDirectivesOnType) {
                    Argument arg = directive.getArgument("attribute");
                    if (null != arg) {
                        EnumValue alias = (EnumValue) arg.getValue();
                        recordAttributeName = alias.getName();
                    }
                }
            } else {
                // Assume record name same as attribute name
                GqlAttributeShape attribute = attributes.get(typeName);
                if (null != attribute) {
                    // We have a match, so stick with it...
                    recordAttributeName = attribute.name; // or even typeName

                } else {
                    String info = "\u21af Not a valid record definition: " + typeName;
                    log.error(info);
                    System.out.println(info);
                    throw new ConfigurationException(info, new Exception("Synthetic exception"));
                }
            }

            if (null != recordAttributeName && !recordAttributeName.isEmpty()) {
                GqlAttributeShape recordAttributeDef = attributes.get(recordAttributeName);
                if (null != recordAttributeDef) {
                    final String catalogAttributeName = recordAttributeDef.name;

                    List<GqlFieldShape> recordFields = new ArrayList<>();
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

                                    recordFields.add(new GqlFieldShape(typeName, fieldName, fieldType.typeName(), fieldType.isArray(), fieldType.isMandatory(), fieldAttributeName));
                                    break; // In the unlikely case there are several @use
                                } else {
                                    log.debug("\u21af Not a valid record field definition (with @use): " + fieldName);
                                }
                            }
                        } else {
                            // No @use, but we will fall back on aliases
                            GqlAttributeShape attributeShape = attributes.get(fieldName);
                            if (null != attributeShape) {
                                recordFields.add(new GqlFieldShape(typeName, fieldName, fieldType.typeName(), fieldType.isArray(), fieldType.isMandatory(), attributeShape.name));
                            } else {
                                log.debug("\u21af Not a valid record field definition: " + fieldName);
                            }
                        }
                    }

                    records.put(typeName, new GqlRecordShape(typeName, recordAttributeName, catalogAttributeName, recordFields));
                    log.trace("\u21af Defining shape for {}: {}", typeName, records.get(typeName));
                }
            } else {
                String info = "\u21af Not a valid record definition: " + typeName;
                log.error(info);
                System.out.println(info);
                throw new ConfigurationException(info, new Exception("Synthetic exception"));
            }
        }

        return records;
    }

    static Map<String, CatalogRecord> read(Repository repository) {
        Map<String, CatalogRecord> records = new HashMap<>();

        // repo_attribute (
        //    attrid      INT       NOT NULL,  -- id of attribute (serial)
        //    qualname    TEXT      NOT NULL,  -- qualified name of attribute
        //    attrname    TEXT      NOT NULL,  -- name of attribute
        //    attrtype    INT       NOT NULL,  -- defined in org.gautelis.repo.model.attributes.Type
        //    scalar      BOOLEAN   NOT NULL DEFAULT FALSE,
        //    created     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        // )
        //
        // repo_record_template (
        //    recordid  INT NOT NULL,
        //    name      TEXT NOT NULL, -- type name
        // )
        //
        // repo_record_template_elements (
        //    recordid       INT  NOT NULL,      -- attribute id of record attribute
        //    attrid         INT  NOT NULL,      -- sub-attribute
        //    idx            INT  NOT NULL,
        //    alias          TEXT NULL,
        // )
        //
        String sql = """
            SELECT rte.recordid AS record_id, recrd.name AS record_name,
                   rte.idx, rte.attrid AS field_attrid,
                   child.qualname AS field_qualname, child.attrname AS field_attrname,
                   child.attrtype AS field_attrtype, child.scalar AS field_scalar,
                   rte.alias AS field_alias
            FROM repo_record_template_elements AS rte
            JOIN repo_record_template AS recrd ON recrd.recordid = rte.recordid
            JOIN repo_attribute AS parent ON parent.attrid = rte.recordid
            JOIN repo_attribute AS child ON child.attrid = rte.attrid
            ORDER BY rte.recordid, rte.idx ASC
        """;

        try {
            repository.withConnection(conn -> {
                Database.useReadonlyPreparedStatement(conn, sql, pStmt -> {
                    try (ResultSet rs = pStmt.executeQuery()) {
                        List<CatalogRecord> catalogRecords = new ArrayList<>();

                        Integer currentId = null;
                        CatalogRecord currentRecord = null;

                        while (rs.next()) {
                            // Record part
                            int recordId = rs.getInt("record_id");
                            String recordName = rs.getString("record_name");

                            // Field part
                            int recordFieldAttrId = rs.getInt("field_attrid");
                            String recordFieldAttrName = rs.getString("field_attrname");
                            String recordFieldQualname = rs.getString("field_qualname");
                            int recordFieldAttrType = rs.getInt("field_attrtype");
                            boolean isArray = !rs.getBoolean("field_scalar"); // Note negation
                            String alias = rs.getString("field_alias");


                            if (currentId == null || recordId != currentId) {
                                // boundary => flush previous
                                if (currentRecord != null) catalogRecords.add(currentRecord);
                                currentId = recordId;
                                currentRecord = new CatalogRecord(recordId, recordName);
                            }

                            CatalogAttribute attribute = new CatalogAttribute(
                                    alias, recordFieldAttrName,
                                    recordFieldQualname, AttributeType.of(recordFieldAttrType),
                                    isArray
                            );
                            attribute.setAttrId(recordFieldAttrId);
                            currentRecord.addField(attribute);
                        }
                        if (currentRecord != null) catalogRecords.add(currentRecord); // flush last one

                        for (CatalogRecord iptoRecord : catalogRecords) {
                            records.put(iptoRecord.recordName, iptoRecord);
                        }
                    }
                });
            });
        } catch (SQLException sqle) {
            log.error("\u21af Failed to load existing record: {}", Database.squeeze(sqle));
        }

        return records;
    }
}