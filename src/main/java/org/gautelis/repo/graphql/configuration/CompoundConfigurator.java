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

class CompoundConfigurator {
    private static final Logger log = LoggerFactory.getLogger(CompoundConfigurator.class);

    private record ExistingCompoundTemplatesMeta(
            int compoundAttrId, int idx, int childAttrId, String alias,boolean required
    ) {
        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder("Existing compound template for ");
            sb.append("parent=").append(compoundAttrId).append(" & child=").append(childAttrId).append("{");
            sb.append("alias=").append(alias);
            sb.append(", required=").append(required);
            sb.append("}");
            return sb.toString();
        }
    }

    private final Repository repo;

    /* package private */
    CompoundConfigurator(Repository repo) {
        this.repo = repo;
    }

    private Map<String, ExistingCompoundTemplatesMeta> loadExisting() {
        Map<String, ExistingCompoundTemplatesMeta> existingTemplates = new HashMap<>();

        // repo_compound_template (
        //    compound_attrid  INT  NOT NULL,      -- attrId of the COMPOUND attribute
        //    idx              INT  NOT NULL,
        //    child_attrid     INT  NOT NULL,      -- sub-attribute
        //    alias            TEXT NULL,
        //    required         BOOLEAN NOT NULL DEFAULT FALSE,
        // }
        String sql = """
                SELECT compound_attrid, idx, child_attrid, alias, required
                FROM repo_compound_template
                ORDER BY compound_attrid, idx, child_attrid ASC
                """;

        try {
            repo.withConnection(conn -> {
                Database.useReadonlyPreparedStatement(conn, sql, pStmt -> {
                    try (ResultSet rs = pStmt.executeQuery()) {
                        while (rs.next()) {

                            int compoundAttrid = rs.getInt("compound_attrid");
                            int idx = rs.getInt("idx");
                            int childAttrId = rs.getInt("child_attrid");
                            String alias = rs.getString("alias");
                            boolean required = rs.getBoolean("required");

                            ExistingCompoundTemplatesMeta metadata = new ExistingCompoundTemplatesMeta(compoundAttrid, idx, childAttrId, alias, required);
                            existingTemplates.put(compoundAttrid + "." + childAttrId, metadata);
                            log.trace(metadata.toString());
                        }
                    }
                });
            });
        } catch (SQLException sqle) {
            log.error("Failed to load existing compound templates: {}", Database.squeeze(sqle));
        }

        return existingTemplates;
    }

    /* package private */
    void load(
            ObjectTypeDefinition type,
            List<Directive> compoundAttributeDirectivesOnType,
            Map<String, Configurator.ProposedAttributeMeta> attributes,
            RuntimeWiring.Builder runtimeWiring,
            RepositoryService repoService,
            Collection<String> info
    ) {
        Map<String, ExistingCompoundTemplatesMeta> existingTemplates = loadExisting();

        String compoundAttributeName = null;
        for (Directive directive : compoundAttributeDirectivesOnType) {
            info.add(String.format("Compound attribute: %s", type.getName()));

            Argument arg = directive.getArgument("attribute");
            if (null != arg) {
                EnumValue alias = (EnumValue) arg.getValue();
                compoundAttributeName = alias.getName();

                info.add(" -> " + compoundAttributeName);
            }
            info.add("\n");
        }

        if (null != compoundAttributeName && !compoundAttributeName.isEmpty()) {
            // repo_compound_template (
            //    compound_attrid  INT  NOT NULL,      -- attrId of the COMPOUND attribute
            //    idx              INT  NOT NULL,
            //    child_attrid     INT  NOT NULL,      -- sub-attribute
            //    alias            TEXT NULL,
            //    required         BOOLEAN NOT NULL DEFAULT FALSE,
            // }
            String sql = """
                    INSERT INTO repo_compound_template (compound_attrid, idx, child_attrid, alias, required)
                    VALUES (?,?,?,?,?)
                    """;

            final String _compoundAttributeName = compoundAttributeName;

            runtimeWiring.type(type.getName(), builder -> {
                try {
                    repo.withConnection(conn -> {
                        try {
                            conn.setAutoCommit(false);

                            Database.usePreparedStatement(conn, sql, pStmt -> {
                                Configurator.ProposedAttributeMeta compoundAttributeMeta = attributes.get(_compoundAttributeName);
                                if (null != compoundAttributeMeta) {
                                    final int compoundAttrid = compoundAttributeMeta.attrId();

                                    int idx = 0;
                                    NEXT_FIELD:
                                    for (FieldDefinition f : type.getFieldDefinitions()) {
                                        String nameInSchema = f.getName();

                                        ++idx;
                                        pStmt.clearParameters();
                                        pStmt.setInt(1, compoundAttrid);
                                        pStmt.setInt(2, idx);

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
                                                    int childAttrid = attributeMeta.attrId();
                                                    String alias = nameInSchema;
                                                    // required?

                                                    // ----------------------------------------------------------------------
                                                    // First check whether this template already exists in local database
                                                    // ----------------------------------------------------------------------
                                                    String key = compoundAttrid + "." + childAttrid;
                                                    ExistingCompoundTemplatesMeta existingTemplate = existingTemplates.get(key);
                                                    if (null != existingTemplate) {
                                                        if (!existingTemplate.alias.equals(alias)) {
                                                            log.warn("Failed to load compound template {}.{}. New definition differs on existing 'alias' {} -- skipping", compoundAttrid, childAttrid, existingTemplate.alias);
                                                            info.add("\t// SKIPPING //\n");
                                                            continue NEXT_FIELD;
                                                        }

                                                        if (existingTemplate.idx != idx) {
                                                            log.warn("Failed to load compound template {}.{}. New definition differs on existing 'index' {} -- skipping", compoundAttrid, childAttrid, existingTemplate.idx);
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
                                                    // Tell GraphQL how to fetch this specific (compound) type
                                                    // ----------------------------------------------------------------------
                                                    final int _idx = idx;

                                                    DataFetcher<?> fetcher = env -> {
                                                        UnitSnapshot snap = env.getSource();
                                                        if (null == snap) {
                                                            log.warn("No snap");
                                                            return null;
                                                        }

                                                        log.trace("Fetching data from compound {} ({}) [{}] ", childAttrid, nameInSchema, _idx);
                                                        return repoService.getCompound(snap, childAttrid, _idx);
                                                    };
                                                    builder.dataFetcher(nameInSchema, fetcher);
                                                    log.info("Wiring: {} {}", type.getName(), nameInSchema);

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
                            log.error("Failed to store compound attribute element: {}", Database.squeeze(sqle));

                            try {
                                conn.rollback();
                            } catch (SQLException rbe) {
                                log.error("Failed to rollback transaction: {}", Database.squeeze(rbe), rbe);
                            }
                        }
                    });
                } catch (SQLException sqle) {
                    log.error("Failed to store compound attribute: {}", Database.squeeze(sqle));
                }
                return builder;
            });
        }
    }
}
