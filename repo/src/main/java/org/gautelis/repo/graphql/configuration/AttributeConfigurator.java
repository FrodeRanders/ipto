package org.gautelis.repo.graphql.configuration;

import com.fasterxml.uuid.Generators;
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

    private record ExistingAttributeMeta(int id, int attrType, boolean isArray, String name, String qualName, String alias) {
        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder("Existing attribute ");
            if (null != alias) {
                sb.append("for '").append(alias).append("' ");
            }
            sb.append("{");
            sb.append("id=").append(id);
            sb.append(", name='").append(name).append('\'');
            sb.append(", qual-name='").append(qualName).append('\'');
            sb.append(", type=").append(AttributeType.of(attrType));
            sb.append(", is-array=").append(isArray);
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
                SELECT attrid, attrtype, scalar, attrname, qualname, alias
                FROM repo_attribute
                """;

        try {
            repo.withConnection(conn -> {
                Database.useReadonlyPreparedStatement(conn, sql, pStmt -> {
                    try (ResultSet rs = pStmt.executeQuery()) {
                        while (rs.next()) {
                            int attrId = rs.getInt("attrid");
                            int attrType = rs.getInt("attrtype");
                            boolean isArray = !rs.getBoolean("scalar"); // Note negation
                            String name = rs.getString("attrname");
                            String qualName = rs.getString("qualname");
                            String alias = rs.getString("alias");
                            if (rs.wasNull()) {
                                alias = null;
                            }

                            ExistingAttributeMeta metadata = new ExistingAttributeMeta(attrId, attrType, isArray, name, qualName, alias);
                            existingAttributes.put(name, metadata);
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
                INSERT INTO repo_attribute (attrid, attrtype, scalar, attrname, qualname, alias)
                VALUES (?,?,?,?,?,?)
                """;

        try {
            repo.withConnection(conn -> {
                try {
                    conn.setAutoCommit(false);

                    Database.usePreparedStatement(conn, sql, pStmt -> {
                        for (EnumValueDefinition enumValueDefinition : enumType.getEnumValueDefinitions()) {
                            String fieldNameInSchema = enumValueDefinition.getName(); // i.e. attribute alias

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

                                // 4: attribute name in ipto -----------------------------------------
                                String nameInIpto;
                                arg = enumValueDirective.getArgument("name");
                                if (null != arg) {
                                    StringValue name = (StringValue) arg.getValue();
                                    nameInIpto = name.getValue();

                                    pStmt.setString(4, nameInIpto);

                                    info += ", " + arg.getName();
                                    info += "='" + nameInIpto + '\'';
                                } else {
                                    // field name will have to do
                                    nameInIpto = fieldNameInSchema;
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
                                    info += "='" + qualName + '\'';
                                } else {
                                    // attribute name will have to do
                                    qualName = nameInIpto;
                                    pStmt.setString(5, qualName);
                                }

                                // 6: alias -------------------------------------------------------------
                                {
                                    pStmt.setString(6, fieldNameInSchema);
                                }

                                // 7: description -------------------------------------------------------
                                String description = null;
                                arg = enumValueDirective.getArgument("description");
                                if (null != arg) {
                                    StringValue vector = (StringValue) arg.getValue();
                                    description = vector.getValue();

                                    // TODO Handle description (repo_attribute_description)
                                    info += ", " + arg.getName() + "=<truncated>";
                                }

                                info += ")";

                                if (/* VALID? */ attrId > 0) {
                                    // ----------------------------------------------------------------------
                                    // First check whether this attribute already exists in catalog
                                    // ----------------------------------------------------------------------
                                    boolean identical = false;

                                    log.trace("Checking attribute {} {id={}, name={}, qual-name={}, type={}, vector={}}", attrId, fieldNameInSchema, nameInIpto, qualName, AttributeType.of(attrType), isArray);

                                    ExistingAttributeMeta existingAttribute = existingAttributes.get(nameInIpto);
                                    if (null != existingAttribute) {

                                        // This attribute has already been loaded -- check similarity
                                        if (existingAttribute.isArray() != isArray) {
                                            log.warn("Failed to load attribute {}. New definition differs on existing 'dimensionality' (vector) {} -- skipping", qualName, existingAttribute.isArray);
                                            continue;
                                        }

                                        if (!existingAttribute.name().equals(nameInIpto)) {
                                            log.warn("Failed to load attribute {}. New definition differs on existing 'attribute name' {} -- skipping", qualName, existingAttribute.name);
                                            continue;
                                        }

                                        if (null != existingAttribute.alias() && !fieldNameInSchema.equals(existingAttribute.alias())) {
                                            log.warn("Failed to load attribute {}. New definition differs on existing 'attribute alias' {} -- skipping", qualName, existingAttribute.alias);
                                            continue;
                                        }

                                        if (existingAttribute.attrType() != attrType) {
                                            log.warn("Failed to load attribute {}. New definition differs on existing 'attribute type' {} -- skipping", qualName, AttributeType.of(existingAttribute.attrType));
                                            continue;
                                        }

                                        if (existingAttribute.id() != attrId) {
                                            log.warn("Failed to load attribute {}. New definition differs on existing 'attribute ID' {} -- skipping", qualName, existingAttribute.id);
                                            continue;
                                        }

                                        identical = true;
                                    }

                                    // We have several names for this attribute;
                                    //  - the name used in the schema, i.e. an "alias"
                                    //  - the name used in IPTO, i.e. the attribute name
                                    //  - a qualified name (assumed globally unique) for the attribute
                                    //
                                    // We need to be able to look up attributes both from a schema viewpoint
                                    // (name in schema) as well as from an IPTO viewpoint (name in IPTO).
                                    Configurator.ProposedAttributeMeta attributeMeta = new Configurator.ProposedAttributeMeta(attrId, fieldNameInSchema, attrType, isArray, nameInIpto, qualName, description);
                                    attributesSchemaView.put(fieldNameInSchema, attributeMeta);
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
