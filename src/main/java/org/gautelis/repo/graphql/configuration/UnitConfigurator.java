package org.gautelis.repo.graphql.configuration;

import graphql.language.*;
import graphql.schema.DataFetcher;
import graphql.schema.idl.RuntimeWiring;
import org.gautelis.repo.db.Database;
import org.gautelis.repo.graphql.runtime.RepositoryService;
import org.gautelis.repo.graphql.runtime.UnitSnapshot;
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

    private record ExistingUnitTemplatesMeta(int templateId, String name, int attrId, String alias, int idx) {
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

    /* package private */
    UnitConfigurator(Repository repo) {
        this.repo = repo;
    }

    private Map<String, ExistingUnitTemplatesMeta> loadExisting() {
        Map<String, ExistingUnitTemplatesMeta> existingTemplates = new HashMap<>();

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

                            ExistingUnitTemplatesMeta metadata = new ExistingUnitTemplatesMeta(templateId, name, attrId, alias, idx);
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

    /* package private */
    void load(
            ObjectTypeDefinition type,
            List<Directive> templateDirectivesOnType,
            Map<String, Configurator.ProposedAttributeMeta> attributes,
            RuntimeWiring.Builder runtimeWiring,
            RepositoryService repoService,
            Collection<String> info
    ) {
        Map<String, ExistingUnitTemplatesMeta> existingTemplates = loadExisting();

        int templateId = -1; // INVALID
        for (Directive directive : templateDirectivesOnType) {
            info.add(String.format("Unit template: %s (", type.getName()));
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
            final String templateName = type.getName();
            final int _templateId = templateId;

            runtimeWiring.type(templateName, builder -> {
                // ----------------------------------------------------------------------
                // Store
                // ----------------------------------------------------------------------
                // repo_unit_template (
                //    templateid INT  NOT NULL,   -- from @template(attrId: …)
                //    name      TEXT NOT NULL,    -- type name
                // }
                String templateSql = """
                        INSERT INTO repo_unit_template (templateid, name)
                        VALUES (?, ?)
                        """;

                // repo_template_elements (
                //     templateid INT  NOT NULL,   -- from @template(attrId: …)
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
                                    templateStmt.setString(2, templateName);

                                    Database.execute(templateStmt);

                                } catch (SQLException sqle) {
                                    String sqlState = sqle.getSQLState();
                                    conn.rollback();

                                    if (sqlState.startsWith("23")) {
                                        // 23505 : duplicate key value violates unique constraint "repo_unit_template_pk"
                                        log.info("Unit template {} seems to already have been loaded", templateName);
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
                                    final String nameInSchema = f.getName();

                                    boolean isDefinedAsArray = false;
                                    boolean isMandatory = false;
                                    String fieldTypeName = null;

                                    if (f.getType() instanceof ListType listType) {
                                        isDefinedAsArray = true;
                                        if (listType.getType() instanceof NonNullType nonNullType) {
                                            isMandatory = true;
                                            fieldTypeName = ((TypeName) nonNullType.getType()).getName();
                                            log.trace("Signature: {} [{}!]", nameInSchema, fieldTypeName);
                                        }
                                        else {
                                            fieldTypeName = ((TypeName) listType.getType()).getName();
                                            log.trace("Signature: {} [{}]", nameInSchema, fieldTypeName);
                                        }
                                    }
                                    else if (f.getType() instanceof NonNullType nonNullType) {
                                        isMandatory = true;
                                        if (nonNullType.getType() instanceof ListType listType) {
                                            isDefinedAsArray = true;
                                            fieldTypeName = ((TypeName) listType.getType()).getName();
                                            log.trace("Signature: {} [{}]!", nameInSchema, fieldTypeName);
                                        }
                                        else {
                                            fieldTypeName = ((TypeName) nonNullType.getType()).getName();
                                            log.trace("Signature: {} {}!", nameInSchema, fieldTypeName);
                                        }
                                    }
                                    else if (f.getType() instanceof TypeName typeName) {
                                        fieldTypeName = typeName.getName();
                                        log.trace("Signature: {} {}", nameInSchema, fieldTypeName);
                                    }

                                    elementsStmt.clearParameters();
                                    elementsStmt.setInt(1, _templateId);

                                    info.add("   " + nameInSchema);

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
                                                // First check whether this template already exists in local database
                                                // ----------------------------------------------------------------------
                                                final String key = templateName + "." + nameInSchema;
                                                ExistingUnitTemplatesMeta existingTemplate = existingTemplates.get(key);
                                                if (null != existingTemplate) {
                                                    // Already known
                                                    if (existingTemplate.attrId != attrId) {
                                                        log.warn("Will not load unit template {}.{}. New definition differs on existing 'attribute' {} -- skipping", templateName, nameInSchema, existingTemplate.attrId);
                                                        return;
                                                    }

                                                    if (existingTemplate.idx != idx) {
                                                        log.warn("Will not load unit template {}.{}. New definition differs on existing 'index' {} -- skipping", templateName, nameInSchema, existingTemplate.idx);
                                                        return;
                                                    }

                                                    // ...and identical
                                                    info.add("\t-- ignoring\n");
                                                    identical = true;
                                                }

                                                if (!identical) {
                                                    elementsStmt.setInt(2, attrId);
                                                    elementsStmt.setString(3, nameInSchema);
                                                    elementsStmt.setInt(4, idx);

                                                    info.add(" -> ");
                                                    info.add(value.getName());
                                                }


                                                // ----------------------------------------------------------------------
                                                // Tell GraphQL how to fetch this specific template type
                                                // by attaching generic field fetchers to every @template type
                                                // ----------------------------------------------------------------------
                                                final int _idx = idx;
                                                final boolean _isMandatory = isMandatory;
                                                final boolean _isArray = isDefinedAsArray;

                                                DataFetcher<?> fetcher = env -> {
                                                    // Hey, my mission in life is to provide support for
                                                    // resolving a single attribute 'nameInSchema' in a
                                                    // single type 'templateName'. Everything I need later,
                                                    // I have access to right now so I capture this information
                                                    // for later.
                                                    UnitSnapshot snap = env.getSource();
                                                    if (null == snap) {
                                                        log.warn("No snap");
                                                        return null;
                                                    }

                                                    log.trace("Fetching attribute {} from unit {}: {}.{}", _isArray ? nameInSchema + "[]" : nameInSchema, templateName, snap.getTenantId(), snap.getUnitId());

                                                    if (_isArray) {
                                                        return repoService.getVector(snap, attrId);
                                                    } else {
                                                        return repoService.getScalar(snap, attrId);
                                                    }
                                                };
                                                builder.dataFetcher(nameInSchema, fetcher);
                                                log.info("Wiring: {}>{}", templateName, nameInSchema);

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
                            log.error("Failed to store unit template entry: {}", Database.squeeze(sqle));

                            try {
                                conn.rollback();
                            } catch (SQLException rbe) {
                                log.error("Failed to rollback transaction: {}", Database.squeeze(rbe), rbe);
                            }
                        }
                    });
                } catch (SQLException sqle) {
                    log.error("Failed to store unit template: {}", Database.squeeze(sqle));
                }
                return builder;
            });
        }
    }
}
