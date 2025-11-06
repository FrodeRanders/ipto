package org.gautelis.repo.graphql.configuration;

import graphql.language.*;
import graphql.schema.DataFetcher;
import graphql.schema.idl.RuntimeWiring;
import org.gautelis.repo.db.Database;
import org.gautelis.repo.graphql.runtime.RuntimeService;
import org.gautelis.repo.graphql.runtime.Box;
import org.gautelis.repo.model.Repository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class RecordConfigurator {
    private static final Logger log = LoggerFactory.getLogger(RecordConfigurator.class);

    private record ExistingRecordMeta(
            int recordAttrId, int idx, int childAttrId, String alias, boolean required
    ) {
        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder("Existing record for ");
            sb.append("parent=").append(recordAttrId).append(" & child=").append(childAttrId).append("{");
            sb.append("alias=").append(alias);
            sb.append(", required=").append(required);
            sb.append("}");
            return sb.toString();
        }
    }

    private final Repository repo;

    /* package accessible only */
    RecordConfigurator(Repository repo) {
        this.repo = repo;
    }

    private Map<String, ExistingRecordMeta> loadExisting() {
        Map<String, ExistingRecordMeta> existingTemplates = new HashMap<>();

        // repo_record_template (
        //    record_attrid  INT  NOT NULL,      -- attrId of the RECORD attribute
        //    idx              INT  NOT NULL,
        //    field_attrid     INT  NOT NULL,      -- sub-attribute
        //    alias            TEXT NULL,
        //    required         BOOLEAN NOT NULL DEFAULT FALSE,
        // }
        String sql = """
                SELECT record_attrid, idx, field_attrid, alias, required
                FROM repo_record_template
                ORDER BY record_attrid, idx, field_attrid ASC
                """;

        try {
            repo.withConnection(conn -> {
                Database.useReadonlyPreparedStatement(conn, sql, pStmt -> {
                    try (ResultSet rs = pStmt.executeQuery()) {
                        while (rs.next()) {

                            int recordAttrid = rs.getInt("record_attrid");
                            int idx = rs.getInt("idx");
                            int childAttrId = rs.getInt("field_attrid");
                            String alias = rs.getString("alias");
                            boolean required = rs.getBoolean("required");

                            ExistingRecordMeta metadata = new ExistingRecordMeta(recordAttrid, idx, childAttrId, alias, required);
                            existingTemplates.put(recordAttrid + "." + childAttrId, metadata);
                            log.trace(metadata.toString());
                        }
                    }
                });
            });
        } catch (SQLException sqle) {
            log.error("Failed to load existing record: {}", Database.squeeze(sqle));
        }

        return existingTemplates;
    }

    /* package accessible only */
    void load(
            ObjectTypeDefinition type,
            List<Directive> recordDirectivesOnType,
            Map<String, Configurator.ProposedAttributeMeta> attributes,
            RuntimeWiring.Builder runtimeWiring,
            RuntimeService repoService,
            Collection<String> info
    ) {
        Map<String, ExistingRecordMeta> existingTemplates = loadExisting();

        String recordName = null;
        for (Directive directive : recordDirectivesOnType) {
            info.add(String.format("Record: %s", type.getName()));

            Argument arg = directive.getArgument("attribute");
            if (null != arg) {
                EnumValue alias = (EnumValue) arg.getValue();
                recordName = alias.getName();

                info.add(" -> " + recordName);
            }
            info.add("\n");
        }

        if (null != recordName && !recordName.isEmpty()) {
            // repo_record_template (
            //    record_attrid  INT  NOT NULL,      -- attrId of the RECORD attribute
            //    idx              INT  NOT NULL,
            //    field_attrid     INT  NOT NULL,      -- sub-attribute
            //    alias            TEXT NULL,
            //    required         BOOLEAN NOT NULL DEFAULT FALSE,
            // }
            String sql = """
                    INSERT INTO repo_record_template (record_attrid, idx, field_attrid, alias, required)
                    VALUES (?,?,?,?,?)
                    """;

            final String _recordName = recordName;

            runtimeWiring.type(type.getName(), builder -> {
                try {
                    repo.withConnection(conn -> {
                        try {
                            conn.setAutoCommit(false);

                            Database.usePreparedStatement(conn, sql, pStmt -> {
                                Configurator.ProposedAttributeMeta recordMeta = attributes.get(_recordName);
                                if (null != recordMeta) {
                                    final int recordAttrid = recordMeta.attrId();

                                    int idx = 0;
                                    NEXT_FIELD:
                                    for (FieldDefinition f : type.getFieldDefinitions()) {
                                        final String fieldName = f.getName();
                                        final TypeDefinition fieldType = TypeDefinition.get(f.getType());

                                        ++idx;
                                        pStmt.clearParameters();
                                        pStmt.setInt(1, recordAttrid);
                                        pStmt.setInt(2, idx);

                                        info.add("   " + fieldName);

                                        // Handle @use directive on field definitions
                                        boolean identical = false;

                                        List<Directive> useDirectives = f.getDirectives("use");
                                        if (!useDirectives.isEmpty()) {
                                            for (Directive useDirective : useDirectives) {
                                                // @use "attribute" argument
                                                Argument arg = useDirective.getArgument("attribute");
                                                EnumValue value = (EnumValue) arg.getValue();

                                                Configurator.ProposedAttributeMeta attributeMeta = attributes.get(value.getName());
                                                if (null != attributeMeta) {
                                                    int childAttrid = attributeMeta.attrId();
                                                    String alias = fieldName;

                                                    // ----------------------------------------------------------------------
                                                    // First check whether this template already exists in local database
                                                    // ----------------------------------------------------------------------
                                                    String key = recordAttrid + "." + childAttrid;
                                                    ExistingRecordMeta existingTemplate = existingTemplates.get(key);
                                                    if (null != existingTemplate) {
                                                        if (!existingTemplate.alias.equals(alias)) {
                                                            log.warn("Failed to load record {}.{}. New definition differs on existing 'alias' {} -- skipping", recordAttrid, childAttrid, existingTemplate.alias);
                                                            info.add("\t// SKIPPING //\n");
                                                            continue NEXT_FIELD;
                                                        }

                                                        if (existingTemplate.idx != idx) {
                                                            log.warn("Failed to load record {}.{}. New definition differs on existing 'index' {} -- skipping", recordAttrid, childAttrid, existingTemplate.idx);
                                                            info.add("\t// SKIPPING //\n");
                                                            continue NEXT_FIELD;
                                                        }

                                                        // In this case, this field is considered equal
                                                        info.add("\t-- ignoring\n");

                                                        identical = true;
                                                    }

                                                    if (!identical) {
                                                        pStmt.setInt(3, childAttrid);
                                                        pStmt.setString(4, alias);
                                                        pStmt.setBoolean(5, true); // required

                                                        info.add(" -> ");
                                                        info.add(value.getName());
                                                    }

                                                    // ----------------------------------------------------------------------
                                                    // Tell GraphQL how to fetch this specific (record) type
                                                    // ----------------------------------------------------------------------
                                                    DataFetcher<?> fetcher = env -> {
                                                        //**** Executed at runtime **********************************
                                                        // My mission in life is to resolve a specific attribute
                                                        // (the current 'fieldName') in a specific type (the current
                                                        // 'recordName'). Everything needed at runtime is accessible
                                                        // right now so it is captured for later.
                                                        //***********************************************************
                                                        Box box = env.getSource();
                                                        if (null == box) {
                                                            log.warn("No box");
                                                            return null;
                                                        }

                                                        log.trace("Fetching attribute {} from record {}: {}.{}", fieldType.isArray() ? fieldName + "[]" : fieldName, _recordName, box.getTenantId(), box.getUnitId());

                                                        // REPLACE return repoService.getRecord(box, childAttrid, _idx);
                                                        if (fieldType.isArray()) {
                                                            return repoService.getArray(box, childAttrid);
                                                        } else {
                                                            return repoService.getScalar(box, childAttrid);
                                                        }
                                                    };
                                                    builder.dataFetcher(fieldName, fetcher);
                                                    log.info("Wiring: {} {}", type.getName(), fieldName);

                                                    //
                                                    if (identical) {
                                                        continue NEXT_FIELD;
                                                    }

                                                    break; // In case there are several @use
                                                }
                                            }
                                        }

                                        // ----------------------------------------------------------------------
                                        // Store
                                        // ----------------------------------------------------------------------
                                        Database.execute(pStmt);

                                        info.add("\n");
                                    }
                                }
                            });

                            conn.commit();

                        } catch (SQLException sqle) {
                            log.error("Failed to store record element: {}", Database.squeeze(sqle));

                            try {
                                conn.rollback();
                            } catch (SQLException rbe) {
                                log.error("Failed to rollback transaction: {}", Database.squeeze(rbe), rbe);
                            }
                        }
                    });
                } catch (SQLException sqle) {
                    log.error("Failed to store record: {}", Database.squeeze(sqle));
                }
                return builder;
            });
        }
    }
}
