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
import org.gautelis.ipto.repo.exceptions.ConfigurationException;
import org.gautelis.ipto.graphql.model.*;
import org.gautelis.ipto.repo.model.AttributeType;
import org.gautelis.ipto.repo.model.Repository;
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

        for (ObjectTypeDefinition type : SdlObjectShapes.domainObjectTypes(registry)) {
            // --- (a) ---
            String typeName = type.getName();

            String recordAttributeName = null;

            // --- (b) ---
            List<Directive> recordDirectivesOnType = type.getDirectives("record");
            if (!recordDirectivesOnType.isEmpty()) {
                for (Directive directive : recordDirectivesOnType) {
                    Argument arg = directive.getArgument("attribute");
                    if (null != arg) {
                        recordAttributeName = SdlObjectShapes.extractName(arg.getValue());
                    }
                }
            } else {
                // Assume record name same as attribute name
                GqlAttributeShape attribute = attributes.get(typeName);
                if (null != attribute) {
                    // We have a match, so stick with it...
                    recordAttributeName = attribute.name(); // or even typeName

                } else {
                    String info = "↯ Not a valid record definition: " + typeName;
                    log.error(info);
                    throw new ConfigurationException(info, new Exception("Synthetic exception"));
                }
            }

            if (null != recordAttributeName && !recordAttributeName.isEmpty()) {
                GqlAttributeShape recordAttributeDef = attributes.get(recordAttributeName);
                if (null != recordAttributeDef) {
                    final String catalogAttributeName = recordAttributeDef.name();
                    List<GqlFieldShape> recordFields = SdlObjectShapes.deriveFields(type, attributes, log, true);

                    records.put(typeName, new GqlRecordShape(typeName, recordAttributeName, catalogAttributeName, recordFields));
                    log.trace("↯ Defining shape for {}: {}", typeName, records.get(typeName));
                }
            } else {
                String info = "↯ Not a valid record definition: " + typeName;
                log.error(info);
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
            repository.withConnection(conn -> Database.useReadonlyPreparedStatement(conn, sql, pStmt -> {
                try (ResultSet rs = pStmt.executeQuery()) {
                    List<CatalogRecord> catalogRecords = new ArrayList<>();

                    Integer currentId = null;
                    String currentRecordName = null;
                    List<CatalogAttribute> currentFields = new ArrayList<>();

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
                            if (currentId != null) {
                                catalogRecords.add(new CatalogRecord(currentId, currentRecordName, currentFields));
                            }
                            currentId = recordId;
                            currentRecordName = recordName;
                            currentFields = new ArrayList<>();
                        }

                        CatalogAttribute attribute = new CatalogAttribute(
                                recordFieldAttrId,
                                alias, recordFieldAttrName,
                                recordFieldQualname, AttributeType.of(recordFieldAttrType),
                                isArray
                        );
                        currentFields.add(attribute);
                    }
                    if (currentId != null) {
                        catalogRecords.add(new CatalogRecord(currentId, currentRecordName, currentFields));
                    }

                    for (CatalogRecord iptoRecord : catalogRecords) {
                        records.put(iptoRecord.recordName(), iptoRecord);
                    }
                }
            }));
        } catch (SQLException sqle) {
            log.error("↯ Failed to load existing record: {}", Database.squeeze(sqle));
        }

        return records;
    }
}
