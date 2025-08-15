package org.gautelis.repo.graphql.configuration;

import graphql.language.*;
import graphql.schema.idl.RuntimeWiring;
import org.gautelis.repo.db.Database;
import org.gautelis.repo.graphql.runtime.RuntimeService;
import org.gautelis.repo.model.Repository;
import org.gautelis.repo.model.AttributeType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.gautelis.repo.model.AttributeType.RECORD;

class AttributeConfigurator {
    private static final Logger log = LoggerFactory.getLogger(AttributeConfigurator.class);

    private record ExistingAttributeMeta(int id, int attrType, boolean vector, String attrName, String qualName) {
        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder("Existing attribute metadata for ");
            sb.append(qualName).append(" {");
            sb.append("alias=").append(attrName);
            sb.append(", type=").append(AttributeType.of(attrType));
            sb.append(", vector=").append(vector);
            sb.append("}");
            return sb.toString();
        }
    }

    private final Repository repo;


    /* package accessible only */
    AttributeConfigurator(Repository repo) {
        this.repo = repo;
    }

    private Map<String, ExistingAttributeMeta> loadExisting() {
        Map<String, ExistingAttributeMeta> existingAttributes = new HashMap<>();

        String sql = """
                SELECT attrid, attrtype, scalar, attrname, qualname
                FROM repo_attribute
                """;

        try {
            repo.withConnection(conn -> {
                Database.useReadonlyPreparedStatement(conn, sql, pStmt -> {
                    try (ResultSet rs = pStmt.executeQuery()) {
                        while (rs.next()) {
                            int attrId = rs.getInt("attrid");
                            int attrType = rs.getInt("attrtype");
                            boolean vector = !rs.getBoolean("scalar"); // Note negation
                            String attrName = rs.getString("attrname");
                            String qualName = rs.getString("qualname");

                            ExistingAttributeMeta metadata = new ExistingAttributeMeta(attrId, attrType, vector, attrName, qualName);
                            existingAttributes.put(qualName, metadata);
                            log.trace(metadata.toString());
                        }
                    }
                });
            });
        } catch (SQLException sqle) {
            log.error("Failed to load existing attributes: {}", Database.squeeze(sqle));
        }

        return existingAttributes;
    }

    /* package accessible only */
    void load(
            EnumTypeDefinition enumType,
            Map<String, Configurator.ExistingDatatypeMeta> datatypes,
            Map<String, Configurator.ProposedAttributeMeta> attributesSchemaView,
            Map<Integer, Configurator.ProposedAttributeMeta> attributesIptoView,
            RuntimeWiring.Builder runtimeWiring,
            RuntimeService repoService
    ) {
        Map<String, ExistingAttributeMeta> existingAttributes = loadExisting();

        String sql = """
                INSERT INTO repo_attribute (attrid, attrtype, scalar, attrname, qualname)
                VALUES (?,?,?,?,?)
                """;

        try {
            repo.withConnection(conn -> {
                try {
                    conn.setAutoCommit(false);

                    Database.usePreparedStatement(conn, sql, pStmt -> {
                        for (EnumValueDefinition enumValueDefinition : enumType.getEnumValueDefinitions()) {
                            String nameInSchema = enumValueDefinition.getName();

                            List<Directive> enumValueDirectives = enumValueDefinition.getDirectives();
                            for (Directive enumValueDirective : enumValueDirectives) {
                                pStmt.clearParameters();
                                String info = "";

                                // 1: attrid -------------------------------------------------------
                                int attrId = -1; // INVALID
                                Argument arg = enumValueDirective.getArgument("id");
                                if (null != arg) {
                                    IntValue _id = (IntValue) arg.getValue();
                                    attrId = _id.getValue().intValue();
                                    pStmt.setInt(1, attrId);

                                    info += "(" + arg.getName();
                                    info += "=" + attrId;
                                }

                                // 2: attrtype -------------------------------------------------------
                                int attrType = -1; // INVALID
                                arg = enumValueDirective.getArgument("datatype");
                                if (null != arg) {
                                    EnumValue datatype = (EnumValue) arg.getValue();
                                    Configurator.ExistingDatatypeMeta datatypeMeta = datatypes.get(datatype.getName());
                                    if (null != datatypeMeta) {
                                        attrType = datatypeMeta.id();
                                        pStmt.setInt(2, attrType);

                                        info += ", " + arg.getName();
                                        info += "=" + datatype.getName();
                                    } else {
                                        log.error("Not a valid datatype: {}", datatype.getName());
                                    }
                                } else {
                                    // If no datatype is specified, we assume this is a record
                                    attrType = RECORD.getType();
                                    pStmt.setInt(2, attrType);
                                }

                                // 3: array -------------------------------------------------------
                                boolean isArray = false;
                                arg = enumValueDirective.getArgument("array");
                                if (null != arg) {
                                    // NOTE: 'array' is optional
                                    BooleanValue vector = (BooleanValue) arg.getValue();
                                    isArray = vector.isValue();

                                    pStmt.setBoolean(3, !vector.isValue()); // Note: negation

                                    info += ", " + arg.getName();
                                    info += "=" + vector.isValue();
                                } else {
                                    // default array
                                    pStmt.setBoolean(3, !isArray); // Note: negation
                                }

                                // 4: attribute name in ipto (i.e. an alias) --------------------------
                                String nameInIpto;
                                arg = enumValueDirective.getArgument("alias");
                                if (null != arg) {
                                    StringValue alias = (StringValue) arg.getValue();
                                    nameInIpto = alias.getValue();

                                    pStmt.setString(4, nameInIpto);

                                    info += ", " + arg.getName();
                                    info += "=" + nameInIpto;
                                } else {
                                    // field name will have to do
                                    nameInIpto = nameInSchema;
                                    pStmt.setString(4, nameInIpto);
                                }

                                // 5: qualname -------------------------------------------------------
                                String qualName;
                                arg = enumValueDirective.getArgument("uri");
                                if (null != arg) {
                                    StringValue uri = (StringValue) arg.getValue();
                                    qualName = uri.getValue();

                                    pStmt.setString(5, qualName);

                                    info += ", " + arg.getName();
                                    info += "=" + qualName;
                                } else {
                                    // field name will have to do
                                    qualName = nameInSchema;
                                    pStmt.setString(5, qualName);
                                }

                                // 6: description -------------------------------------------------------
                                String description = null;
                                arg = enumValueDirective.getArgument("description");
                                if (null != arg) {
                                    StringValue vector = (StringValue) arg.getValue();
                                    description = vector.getValue();

                                    info += ", " + arg.getName();
                                    info += "=" + description;
                                }

                                info += ")";

                                if (/* VALID? */ attrId > 0) {

                                    // ----------------------------------------------------------------------
                                    // First check whether this attribute already exists in local database
                                    // ----------------------------------------------------------------------
                                    boolean identical = false;

                                    ExistingAttributeMeta existingAttribute = existingAttributes.get(qualName);
                                    if (null != existingAttribute) {
                                        log.trace("Checking attribute {} {alias={}, type={}, vector={}}", qualName, nameInIpto, AttributeType.of(attrType), isArray);

                                        // This attribute has already been loaded -- check similarity
                                        if (existingAttribute.vector != isArray) {
                                            log.warn("Failed to load attribute {}. New definition differs on existing 'dimensionality' (vector) {} -- skipping", qualName, existingAttribute.vector);
                                            continue;
                                        }

                                        if (!existingAttribute.attrName.equals(nameInIpto)) {
                                            log.warn("Failed to load attribute {}. New definition differs on existing 'attribute name' {} -- skipping", qualName, existingAttribute.attrName);
                                            continue;
                                        }

                                        if (existingAttribute.attrType != attrType) {
                                            log.warn("Failed to load attribute {}. New definition differs on existing 'attribute type' {} -- skipping", qualName, AttributeType.of(existingAttribute.attrType));
                                            continue;
                                        }

                                        if (existingAttribute.id != attrId) {
                                            log.warn("Failed to load attribute {}. New definition differs on existing 'attribute ID' {} -- skipping", qualName, existingAttribute.id);
                                            continue;
                                        }

                                        identical = true;
                                    }

                                    // We have several names for this attribute;
                                    //  - the name used in the schema,
                                    //  - the name used in IPTO,
                                    //  - a qualified name (assumed globally unique).
                                    //
                                    // We need to be able to look up attributes both from a schema viewpoint
                                    // (name in schema) as well as from an IPTO viewpoint (name in IPTO).
                                    Configurator.ProposedAttributeMeta attributeMeta = new Configurator.ProposedAttributeMeta(attrId, nameInSchema, attrType, isArray, nameInIpto, qualName, description);
                                    attributesSchemaView.put(nameInSchema, attributeMeta);
                                    attributesIptoView.put(attrId, attributeMeta);

                                    //
                                    if (identical) {
                                        log.info("Attribute {} exists already -- ignoring", qualName);
                                        continue;
                                    }

                                    // ----------------------------------------------------------------------
                                    // Store
                                    // ----------------------------------------------------------------------
                                    log.info("Loading attribute: {} {}", qualName, info);
                                    Database.execute(pStmt);
                                }
                            }
                        }
                    });

                    conn.commit();

                } catch (Throwable t) {
                    log.error("Failed to store attribute: {}", t.getMessage());
                    if (t.getCause() instanceof SQLException sqle) {
                        log.error("  ^--- {}", Database.squeeze(sqle));
                        String sqlState = sqle.getSQLState();

                        try {
                            conn.rollback();
                        } catch (SQLException rbe) {
                            log.error("Failed to rollback transaction: {}", Database.squeeze(rbe), rbe);
                        }

                        if (sqlState.startsWith("23")) {
                            // 23505 : duplicate key value violates unique constraint "repo_attribute_pk"
                            log.info("Attributes seems to already have been loaded");
                        }
                    }
                }
            });
        } catch (SQLException sqle) {
            log.error("Failed to store attributes: {}", Database.squeeze(sqle));
        }
    }
}
