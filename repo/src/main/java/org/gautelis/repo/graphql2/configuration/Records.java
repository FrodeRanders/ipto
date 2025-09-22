package org.gautelis.repo.graphql2.configuration;

import graphql.language.*;
import graphql.schema.idl.TypeDefinitionRegistry;
import org.gautelis.repo.db.Database;
import org.gautelis.repo.exceptions.ConfigurationException;
import org.gautelis.repo.graphql2.model.AttributeDef;
import org.gautelis.repo.graphql2.model.RecordDef;
import org.gautelis.repo.graphql2.model.TypeDef;
import org.gautelis.repo.graphql2.model.TypeFieldDef;
import org.gautelis.repo.graphql2.model.external.ExternalAttributeDef;
import org.gautelis.repo.graphql2.model.external.ExternalRecordDef;
import org.gautelis.repo.graphql2.model.external.ExternalTypeFieldDef;
import org.gautelis.repo.graphql2.model.internal.InternalRecordDef;
import org.gautelis.repo.graphql2.model.internal.InternalTypeFieldDef;
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
    static Map<String, ExternalRecordDef> derive(TypeDefinitionRegistry registry, Map<String, ExternalAttributeDef> attributes) {
        Map<String, ExternalRecordDef> records = new HashMap<>();

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
                    AttributeDef recordAttributeDef = attributes.get(recordAttributeName);
                    if (null != recordAttributeDef) {
                        final int recordAttributeId = recordAttributeDef.attributeId;

                        List<TypeFieldDef> recordFields = new ArrayList<>();
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

                                    ExternalAttributeDef fieldAttributeDef = attributes.get(value.getName());
                                    if (null != fieldAttributeDef) {
                                        // --- (e) ---
                                        String fieldAttributeName = fieldAttributeDef.attributeName;
                                        int fieldAttributeId = fieldAttributeDef.attributeId;

                                        if (fieldAttributeDef.isArray != fieldType.isArray()) {
                                            String info = "Definition of field " + fieldName + " in record " + recordName + " is invalid: " + f.getType() + " differs with respect to array capability from definition of attribute " + fieldAttributeName;
                                            log.error(info);
                                            System.out.println(info);
                                            throw new ConfigurationException(info);
                                        }

                                        recordFields.add(new ExternalTypeFieldDef(fieldName, fieldType, fieldAttributeName, fieldAttributeId));
                                        break; // In the unlikely case there are several @use
                                    }
                                }
                            }
                        }
                        records.put(recordName, new ExternalRecordDef(recordName, recordAttributeName, recordAttributeId, recordFields));
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

    public static final class IptoRecordDef {
        public final String recordName;
        public final int recordAttrId;
        private final List<TypeFieldDef> fields = new ArrayList<>();

        public IptoRecordDef(String recordName, int recordAttrid) {
            this.recordName = recordName;
            this.recordAttrId = recordAttrid;
        }

        public IptoRecordDef(int recordAttrid) {
            this(null, recordAttrid);
        }

        public void add(TypeFieldDef field) {
            fields.add(field);
        }

        public List<TypeFieldDef> attributes() {
            return Collections.unmodifiableList(fields);
        }

        public InternalRecordDef toRecordDef() {
            return new InternalRecordDef(
                    /* Ipto specific attribute name */ recordName,
                    /* Ipto specific attribute id */ recordAttrId,
                    fields
            );
        }
    }

    static Map<String, InternalRecordDef> read(Repository repository) {
        Map<String, InternalRecordDef> records = new HashMap<>();

        // repo_record_template (
        //    record_attrid  INT  NOT NULL,      -- attrId of the RECORD attribute
        //    idx              INT  NOT NULL,
        //    child_attrid     INT  NOT NULL,      -- sub-attribute
        //    alias            TEXT NULL,
        //    required         BOOLEAN NOT NULL DEFAULT FALSE,
        // }
        String sql = """
                SELECT record_attrid, idx, child_attrid, alias, required
                FROM repo_record_template
                ORDER BY record_attrid, idx, child_attrid ASC
                """;

        try {
            repository.withConnection(conn -> {
                Database.useReadonlyPreparedStatement(conn, sql, pStmt -> {
                    try (ResultSet rs = pStmt.executeQuery()) {
                        List<IptoRecordDef> iptoRecords = new ArrayList<>();

                        Integer currentId = null;
                        IptoRecordDef currentRecord = null;

                        while (rs.next()) {
                            int recordAttrId = rs.getInt("record_attrid");
                            int idx = rs.getInt("idx");
                            int recordFieldAttrId = rs.getInt("child_attrid");
                            String alias = rs.getString("alias");
                            boolean required = rs.getBoolean("required");

                            if (currentId == null || recordAttrId != currentId) {
                                // boundary => flush previous
                                if (currentRecord != null) iptoRecords.add(currentRecord);
                                currentId = recordAttrId;

                                Optional<String> iptoRecordName = repository.attributeIdToName(recordAttrId);
                                if (iptoRecordName.isPresent()) {
                                    currentRecord = new IptoRecordDef(iptoRecordName.get(), recordAttrId);
                                } else {
                                    log.error("Record attribute id " + recordAttrId + " not found");
                                    currentRecord = null;
                                    continue;
                                }
                            }

                            currentRecord.add(new InternalTypeFieldDef(
                                            /* Ipto specific attribute name */ alias,
                                            /* Ipto specific attribugte id */ recordFieldAttrId
                                    )
                            );
                        }
                        if (currentRecord != null) iptoRecords.add(currentRecord); // flush last one

                        for (IptoRecordDef iptoRecord : iptoRecords) {
                            records.put(iptoRecord.recordName, iptoRecord.toRecordDef());
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