package org.gautelis.repo.graphql2.configuration;

import graphql.language.*;
import graphql.schema.idl.TypeDefinitionRegistry;
import org.gautelis.repo.db.Database;
import org.gautelis.repo.exceptions.ConfigurationException;
import org.gautelis.repo.graphql2.model.*;
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
            String recordName = type.getName();

            List<Directive> recordDirectivesOnType = type.getDirectives("record");
            if (!recordDirectivesOnType.isEmpty()) {

                // --- (b) ---
                String recordAttributeName = null;
                for (Directive directive : recordDirectivesOnType) {
                    Argument arg = directive.getArgument("attribute");
                    if (null != arg) {
                        EnumValue alias = (EnumValue) arg.getValue();
                        recordAttributeName = alias.getName();
                    }
                }

                if (null != recordAttributeName && !recordAttributeName.isEmpty()) {
                    GqlAttributeShape recordAttributeDef = attributes.get(recordAttributeName);
                    if (null != recordAttributeDef) {
                        final int recordAttributeId = recordAttributeDef.attributeId;

                        List<GqlFieldShape> recordFields = new ArrayList<>();
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

                                        recordFields.add(new GqlFieldShape(recordName, fieldName, fieldType.typeName(), fieldType.isArray(), fieldType.isMandatory(), fieldAttributeName));
                                        break; // In the unlikely case there are several @use
                                    }
                                }
                            }
                        }
                        records.put(recordName, new GqlRecordShape(recordName, recordAttributeName, recordFields));
                    }
                } else {
                    String info = "Not a valid record definition: " + recordName;
                    log.error(info);
                    System.out.println(info);
                    throw new ConfigurationException(info);
                }
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
        //    created     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        // )
        //
        // repo_record_template (
        //    record_attrid  INT  NOT NULL,      -- attrId of the RECORD attribute
        //    idx            INT  NOT NULL,
        //    field_attrid   INT  NOT NULL,      -- sub-attribute
        //    alias          TEXT NULL,
        //    required       BOOLEAN NOT NULL DEFAULT FALSE,
        // )
        String sql = """
            SELECT rt.record_attrid, recrd.attrname AS record_attrname, recrd.attrtype AS record_attrtype,
                   rt.idx, 
                   rt.field_attrid, field.qualname AS field_qualname, field.attrname AS field_attrname, 
                   field.attrtype AS field_attrtype, field.scalar,
                   rt.alias, rt.required
            FROM repo_record_template AS rt
            JOIN repo_attribute AS recrd ON recrd.attrid = rt.record_attrid
            JOIN repo_attribute AS field ON field.attrid = rt.field_attrid
            ORDER BY rt.record_attrid, rt.idx, rt.field_attrid ASC
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
                            int recordAttrId = rs.getInt("record_attrid");
                            String recordAttrName = rs.getString("record_attrname");
                            //int recordAttrType = rs.getInt("record_attrtype"); // AttributeType.RECORD

                            // Field part
                            int idx = rs.getInt("idx");

                            int recordFieldAttrId = rs.getInt("field_attrid");
                            String recordFieldAttrName = rs.getString("field_attrname");
                            String recordFieldQualname = rs.getString("field_qualname");
                            int recordFieldAttrType = rs.getInt("field_attrtype");

                            boolean isArray = !rs.getBoolean("scalar"); // Note negation
                            String alias = rs.getString("alias");
                            //boolean isRequired = rs.getBoolean("required"); TODO

                            if (currentId == null || recordAttrId != currentId) {
                                // boundary => flush previous
                                if (currentRecord != null) catalogRecords.add(currentRecord);
                                currentId = recordAttrId;
                                currentRecord = new CatalogRecord(recordAttrName, recordAttrId);
                            }

                            currentRecord.addField(
                                    new CatalogAttribute(recordFieldAttrId, alias, recordFieldAttrName, recordFieldQualname, AttributeType.of(recordFieldAttrType), isArray)
                            );
                        }
                        if (currentRecord != null) catalogRecords.add(currentRecord); // flush last one

                        for (CatalogRecord iptoRecord : catalogRecords) {
                            records.put(iptoRecord.recordName, iptoRecord);
                        }
                    }
                });
            });
        } catch (SQLException sqle) {
            log.error("Failed to load existing record: {}", Database.squeeze(sqle));
        }

        return records;
    }
}