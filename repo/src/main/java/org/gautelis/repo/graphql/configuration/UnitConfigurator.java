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

class UnitConfigurator {
    private static final Logger log = LoggerFactory.getLogger(UnitConfigurator.class);

    private record ExistingUnitMeta(int templateId, String name, int attrId, String alias, int idx) {
        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder("Existing unit template for ");
            sb.append(name).append(" {");
            sb.append("attrId=").append(templateId);
            sb.append(", attr-attrId=").append(attrId);
            sb.append(", alias=").append(alias);
            sb.append(", idx=").append(idx);
            sb.append("}");
            return sb.toString();
        }
    }

    private final Repository repo;

    /* package accessible only */
    UnitConfigurator(Repository repo) {
        this.repo = repo;
    }

    private Map<String, ExistingUnitMeta> loadExisting() {
        Map<String, ExistingUnitMeta> existingTemplates = new HashMap<>();

        String sql = """
                SELECT rut.templateid, rut.name, rte.attrid, rte.alias, rte.idx
                FROM repo_unit_template rut
                INNER JOIN repo_template_elements rte ON (rut.templateid = rte.templateid)
                ORDER BY rut.templateid ASC
                """;

        try {
            repo.withConnection(conn -> {
                Database.useReadonlyPreparedStatement(conn, sql, pStmt -> {
                    try (ResultSet rs = pStmt.executeQuery()) {
                        while (rs.next()) {
                            int templateId = rs.getInt("templateid");
                            String name = rs.getString("name");
                            int attrId = rs.getInt("attrid");
                            String alias = rs.getString("alias");
                            int idx = rs.getInt("idx");

                            ExistingUnitMeta metadata = new ExistingUnitMeta(templateId, name, attrId, alias, idx);
                            String key = name + "." + alias;
                            existingTemplates.put(key, metadata);
                            log.debug(metadata.toString());
                        }
                    }
                });
            });
        } catch (SQLException sqle) {
            log.error("Failed to load existing unit templates: {}", Database.squeeze(sqle));
        }

        return existingTemplates;
    }

    /* package accessible only */
    void load(
            ObjectTypeDefinition type,
            List<Directive> unitDirectivesOnType,
            Map<String, Configurator.ProposedAttributeMeta> attributes,
            RuntimeWiring.Builder runtimeWiring,
            RuntimeService repoService,
            Collection<String> info
    ) {
        Map<String, ExistingUnitMeta> existingTemplates = loadExisting();

        int templateId = -1; // INVALID
        for (Directive directive : unitDirectivesOnType) {
            info.add(String.format("Unit: %s (", type.getName()));
            Argument arg = directive.getArgument("id");
            if (null != arg) {
                info.add(arg.getName());
                IntValue _id = (IntValue) arg.getValue();
                templateId = _id.getValue().intValue();
                info.add("=" + templateId);
            }
            info.add(")\n");
        }

        if (/* VALID? */ templateId > 0) {
            final String unitName = type.getName();
            final int _templateId = templateId;

            runtimeWiring.type(unitName, builder -> {
                // ----------------------------------------------------------------------
                // Store
                // ----------------------------------------------------------------------
                // repo_unit_template (
                //    templateid INT  NOT NULL,   -- from @unit(attrId: …)
                //    name      TEXT NOT NULL,    -- type name
                // }
                String templateSql = """
                        INSERT INTO repo_unit_template (templateid, name)
                        VALUES (?, ?)
                        """;

                // repo_template_elements (
                //     templateid INT  NOT NULL,   -- from @unit(attrId: …)
                //     attrid     INT  NOT NULL,   -- global attribute attrId
                //     alias      TEXT NOT NULL,   -- field name inside unit (template)
                //     idx        INT  NULL,       -- optional order / display position
                // }
                String elementsSql = """
                        INSERT INTO repo_template_elements (templateid, attrid, alias, idx)
                        VALUES (?,?,?,?)
                        """;

                try {
                    repo.withConnection(conn -> {
                        try {
                            conn.setAutoCommit(false);

                            Database.usePreparedStatement(conn, templateSql, templateStmt -> {
                                try {
                                    templateStmt.setInt(1, _templateId);
                                    templateStmt.setString(2, unitName);

                                    Database.execute(templateStmt);

                                } catch (SQLException sqle) {
                                    String sqlState = sqle.getSQLState();
                                    conn.rollback();

                                    if (sqlState.startsWith("23")) {
                                        // 23505 : duplicate key value violates unique constraint "repo_unit_template_pk"
                                        log.info("Unit template {} seems to already have been loaded", unitName);
                                    } else {
                                        throw sqle;
                                    }
                                }
                            });

                            Database.usePreparedStatement(conn, elementsSql, elementsStmt -> {
                                // Handle field definitions on types
                                int idx = 0;

                                NEXT_FIELD:
                                for (FieldDefinition f : type.getFieldDefinitions()) {
                                    ++idx;
                                    final String fieldName = f.getName();
                                    final TypeDefinition fieldType = TypeDefinition.get(f.getType());

                                    elementsStmt.clearParameters();
                                    elementsStmt.setInt(1, _templateId);

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
                                                int attrId = attributeMeta.attrId();
                                                boolean isVector = attributeMeta.isVector();

                                                // ----------------------------------------------------------------------
                                                // First check whether this unit template already exists in local database
                                                // ----------------------------------------------------------------------
                                                final String key = unitName + "." + fieldName;
                                                ExistingUnitMeta existingTemplate = existingTemplates.get(key);
                                                if (null != existingTemplate) {
                                                    // Already known
                                                    if (existingTemplate.attrId != attrId) {
                                                        log.warn("Will not load unit template {}.{}. New definition differs on existing 'attribute' {} -- skipping", unitName, fieldName, existingTemplate.attrId);
                                                        return;
                                                    }

                                                    if (existingTemplate.idx != idx) {
                                                        log.warn("Will not load unit template {}.{}. New definition differs on existing 'index' {} -- skipping", unitName, fieldName, existingTemplate.idx);
                                                        return;
                                                    }

                                                    // ...and identical
                                                    info.add("\t-- ignoring\n");
                                                    identical = true;
                                                }

                                                if (!identical) {
                                                    elementsStmt.setInt(2, attrId);
                                                    elementsStmt.setString(3, fieldName);
                                                    elementsStmt.setInt(4, idx);

                                                    info.add(" -> ");
                                                    info.add(value.getName());
                                                }


                                                // ----------------------------------------------------------------------
                                                // Tell GraphQL how to fetch this specific unit type
                                                // by attaching generic field fetchers to every @unit type
                                                // ----------------------------------------------------------------------
                                                DataFetcher<?> fetcher = env -> {
                                                    //**** Executed at runtime **********************************
                                                    // My mission in life is to resolve a specific attribute
                                                    // (the current 'fieldName') in a specific type (the current
                                                    // 'unitName'). Everything needed at runtime is accessible
                                                    // right now so it is captured for later.
                                                    //***********************************************************
                                                    Box box = env.getSource();
                                                    if (null == box) {
                                                        log.warn("No box");
                                                        return null;
                                                    }

                                                    log.trace("Fetching attribute {} from unit {}: {}.{}", fieldType.isArray() ? fieldName + "[]" : fieldName, unitName, box.getTenantId(), box.getUnitId());

                                                    if (fieldType.isArray()) {
                                                        return repoService.getArray(box, attrId);
                                                    } else {
                                                        return repoService.getScalar(box, attrId);
                                                    }
                                                };
                                                builder.dataFetcher(fieldName, fetcher);
                                                log.info("Wiring: {}>{}", unitName, fieldName);

                                                //
                                                if (identical) {
                                                    continue NEXT_FIELD;
                                                }

                                                break; // in case there are several @use
                                            }
                                        }
                                    }

                                    Database.execute(elementsStmt);

                                    info.add("\n");
                                }
                            });

                            conn.commit();

                        } catch (SQLException sqle) {
                            log.error("Failed to store unit entry: {}", Database.squeeze(sqle));

                            try {
                                conn.rollback();
                            } catch (SQLException rbe) {
                                log.error("Failed to rollback transaction: {}", Database.squeeze(rbe), rbe);
                            }
                        }
                    });
                } catch (SQLException sqle) {
                    log.error("Failed to store unit: {}", Database.squeeze(sqle));
                }
                return builder;
            });
        }
    }
}
