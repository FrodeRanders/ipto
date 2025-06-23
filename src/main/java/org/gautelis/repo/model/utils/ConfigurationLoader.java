package org.gautelis.repo.model.utils;

import graphql.language.*;
import graphql.schema.idl.SchemaParser;
import graphql.schema.idl.TypeDefinitionRegistry;
import org.gautelis.repo.db.Database;
import org.gautelis.repo.exceptions.AttributeTypeException;
import org.gautelis.repo.model.Repository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Reader;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

import static org.gautelis.repo.model.attributes.Type.COMPOUND;
import static org.gautelis.repo.model.attributes.Type.of;

public class ConfigurationLoader {
    private static final Logger log = LoggerFactory.getLogger(ConfigurationLoader.class);

    //
    public record ExistingDatatypeMeta(int id, String sdlName) {}

    public record ProposedAttributeMeta(int id, String sdlName, int attrType) {}

    private final Repository repo;

    public ConfigurationLoader(Repository repo) {
        this.repo = repo;
    }

    public void load(Reader sdl) {
        SchemaParser schemaParser = new SchemaParser();
        TypeDefinitionRegistry reg = schemaParser.parse(sdl);

        // Load attributes
        AttributeLoader attributeLoader = new AttributeLoader(repo);
        Map<String, ExistingDatatypeMeta> datatypes = new HashMap<>();
        Map<String, ProposedAttributeMeta> attributes = new HashMap<>();

        try {
            for (EnumTypeDefinition enumeration : reg.getTypes(EnumTypeDefinition.class)) {
                List<Directive> enumDirectives = enumeration.getDirectives();
                for (Directive directive : enumDirectives) {
                    switch (directive.getName()) {
                        case "datatypeRegistry" -> {
                            // -----------------------------------------------------------
                            // This is not configurable, but it is part of the
                            // configuration SDL as reference.
                            // -----------------------------------------------------------
                            if (!verifyDatatypes(enumeration, datatypes)) {
                                return;
                            }
                        }

                        case "attributeRegistry" -> attributeLoader.load(enumeration, datatypes, attributes);

                        default -> {
                        }
                    }
                }
            }

            // Iterate over types
            UnitTemplateLoader unitTemplateLoader = new UnitTemplateLoader(repo);
            CompoundTemplateLoader compoundTemplateLoader = new CompoundTemplateLoader(repo);

            for (ObjectTypeDefinition type : reg.getTypes(ObjectTypeDefinition.class)) {
                Collection<String> info = new ArrayList<>();
                info.add("\n");

                // Handle @template directives on object types
                int templateId = -1; // INVALID
                List<Directive> templateDirectivesOnType = type.getDirectives("template");
                if (!templateDirectivesOnType.isEmpty()) {
                    unitTemplateLoader.load(type, templateDirectivesOnType, attributes, info);
                } else {
                    List<Directive> compoundAttributeDirectivesOnType = type.getDirectives("compound");
                    if (!compoundAttributeDirectivesOnType.isEmpty()) {
                        compoundTemplateLoader.load(type, compoundAttributeDirectivesOnType, attributes, info);
                    } else {
                        info.add(String.format("Ignoring %s...\n", type.getName()));
                    }
                }

                log.info(String.join("", info));
            }
        } catch (Throwable t) {
            log.error("Failed load SDL: {}", t.getMessage(), t);

            if (t.getCause() instanceof SQLException sqle) {
                log.error("  ^-- more details: {}", Database.squeeze(sqle));
            }
        }
    }


    private boolean verifyDatatypes(
            EnumTypeDefinition enumeration,
            Map<String, ExistingDatatypeMeta> datatypes
    ) {
        for (EnumValueDefinition enumValueDefinition : enumeration.getEnumValueDefinitions()) {
            List<Directive> enumValueDirectives = enumValueDefinition.getDirectives();
            for (Directive enumValueDirective : enumValueDirectives) {
                String info = "";

                int id = -1; // INVALID
                Argument arg = enumValueDirective.getArgument("id");
                if (null != arg) {
                    info += "(" + arg.getName();
                    IntValue _id = (IntValue) arg.getValue();
                    id = _id.getValue().intValue();
                    info += "=" + id;

                    // Validation
                    try {
                        var _officialType = of(_id.getValue().intValue());
                    } catch (AttributeTypeException ate) {
                        log.error("Not an official data type: {} with numeric id {}", enumValueDefinition.getName(), _id.getValue().intValue(), ate);
                        return false;
                    }
                }

                arg = enumValueDirective.getArgument("type");
                if (null != arg) {
                    // NOTE: 'COMPOUND' does not have a particular dbType
                    info += ", " + arg.getName();
                    StringValue _type = (StringValue) arg.getValue();
                    info += "=" + _type.getValue();
                }
                info += ")";

                if (/* VALID? */ id > 0) {
                    log.debug("Valid datatype: {} {}", enumValueDefinition.getName(), info);

                    datatypes.put(enumValueDefinition.getName(),
                            new ExistingDatatypeMeta(id, enumValueDefinition.getName())
                    );
                }
            }
        }
        return true;
    }

    private static class AttributeLoader {
        private record ExistingAttributeMeta(int id, int attrType, boolean vector, String attrName, String qualName) {
            @Override
            public String toString() {
                StringBuilder sb = new StringBuilder("Existing attribute metadata for ");
                sb.append(qualName).append(" {");
                sb.append("alias=").append(attrName);
                sb.append(", type=").append(org.gautelis.repo.model.attributes.Type.of(attrType));
                sb.append(", vector=").append(vector);
                sb.append("}");
                return sb.toString();
            }
        }

        private final Repository repo;

        AttributeLoader(Repository repo) {
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

        private void load(
                EnumTypeDefinition enumeration,
                Map<String, ExistingDatatypeMeta> datatypes,
                Map<String, ProposedAttributeMeta> attributesLookup
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
                            for (EnumValueDefinition enumValueDefinition : enumeration.getEnumValueDefinitions()) {
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
                                        ExistingDatatypeMeta datatypeMeta = datatypes.get(datatype.getName());
                                        if (null != datatypeMeta) {
                                            attrType = datatypeMeta.id;
                                            pStmt.setInt(2, attrType);

                                            info += ", " + arg.getName();
                                            info += "=" + datatype.getName();
                                        } else {
                                            log.error("Not a valid datatype: {}", datatype.getName());
                                        }
                                    } else {
                                        // If no datatype is specified, we assume this is a compound
                                        attrType = COMPOUND.getType();
                                        pStmt.setInt(2, attrType);
                                    }


                                    // 3: vector -------------------------------------------------------
                                    boolean isVector = false;
                                    arg = enumValueDirective.getArgument("vector");
                                    if (null != arg) {
                                        // NOTE: 'vector' is optional
                                        BooleanValue vector = (BooleanValue) arg.getValue();
                                        isVector = vector.isValue();

                                        pStmt.setBoolean(3, !vector.isValue()); // Note: negation

                                        info += ", " + arg.getName();
                                        info += "=" + vector.isValue();
                                    } else {
                                        // default vector
                                        pStmt.setBoolean(3, !isVector); // Note: negation
                                    }

                                    // 4: attrname -------------------------------------------------------
                                    String attrName = null;
                                    arg = enumValueDirective.getArgument("alias");
                                    if (null != arg) {
                                        StringValue alias = (StringValue) arg.getValue();
                                        attrName = alias.getValue();

                                        pStmt.setString(4, attrName);

                                        info += ", " + arg.getName();
                                        info += "=" + attrName;
                                    } else {
                                        // field name will have to do
                                        attrName = enumValueDefinition.getName();
                                        pStmt.setString(4, attrName);
                                    }

                                    // 5: qualname -------------------------------------------------------
                                    String qualName = null;
                                    arg = enumValueDirective.getArgument("uri");
                                    if (null != arg) {
                                        StringValue uri = (StringValue) arg.getValue();
                                        qualName = uri.getValue();

                                        pStmt.setString(5, qualName);

                                        info += ", " + arg.getName();
                                        info += "=" + qualName;
                                    } else {
                                        // field name will have to do
                                        qualName = enumValueDefinition.getName();
                                        pStmt.setString(5, qualName);
                                    }

                                    // 6: description -------------------------------------------------------
                                    arg = enumValueDirective.getArgument("description");
                                    if (null != arg) {
                                        StringValue vector = (StringValue) arg.getValue();

                                        info += ", " + arg.getName();
                                        info += "=" + vector.getValue();
                                    }

                                    info += ")";

                                    if (/* VALID? */ attrId > 0) {

                                        // ----------------------------------------------------------------------
                                        // First check whether this attribute already exists in local database
                                        // ----------------------------------------------------------------------
                                        boolean identical = false;

                                        ExistingAttributeMeta existingAttribute = existingAttributes.get(qualName);
                                        if (null != existingAttribute) {
                                            log.trace("Checking attribute {} {alias={}, type={}, vector={}}", qualName, attrName, org.gautelis.repo.model.attributes.Type.of(attrType), isVector);

                                            // This attribute has already been loaded -- check similarity
                                            if (existingAttribute.vector != isVector) {
                                                log.warn("Failed to load attribute {}. New definition differs on existing 'dimensionality' (vector) {} -- skipping", qualName, existingAttribute.vector);
                                                continue;
                                            }

                                            if (!existingAttribute.attrName.equals(attrName)) {
                                                log.warn("Failed to load attribute {}. New definition differs on existing 'attribute name' {} -- skipping", qualName, existingAttribute.attrName);
                                                continue;
                                            }

                                            if (existingAttribute.attrType != attrType) {
                                                log.warn("Failed to load attribute {}. New definition differs on existing 'attribute type' {} -- skipping", qualName, org.gautelis.repo.model.attributes.Type.of(existingAttribute.attrType));
                                                continue;
                                            }

                                            if (existingAttribute.id != attrId) {
                                                log.warn("Failed to load attribute {}. New definition differs on existing 'attribute ID' {} -- skipping", qualName, existingAttribute.id);
                                                continue;
                                            }

                                            identical = true;
                                        }

                                        // We have a qualified name, but we populate the lookup table
                                        // based on enum value definition name from the SDL.
                                        attributesLookup.put(enumValueDefinition.getName(), new ProposedAttributeMeta(attrId, enumValueDefinition.getName(), attrType));

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

    private static class UnitTemplateLoader {
        private record ExistingUnitTemplatesMeta(int templateId, String name, int attrId, String alias, int idx) {
            @Override
            public String toString() {
                StringBuilder sb = new StringBuilder("Existing unit template for ");
                sb.append(name).append(" {");
                sb.append("id=").append(templateId);
                sb.append(", attr-id=").append(attrId);
                sb.append(", alias=").append(alias);
                sb.append(", idx=").append(idx);
                sb.append("}");
                return sb.toString();
            }
        }

        private final Repository repo;

        UnitTemplateLoader(Repository repo) {
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
                                existingTemplates.put(name + "." + alias, metadata);
                                log.info(metadata.toString());
                            }
                        }
                    });
                });
            } catch (SQLException sqle) {
                log.error("Failed to load existing unit templates: {}", Database.squeeze(sqle));
            }

            return existingTemplates;
        }

        private void load(
                ObjectTypeDefinition type,
                List<Directive> templateDirectivesOnType,
                Map<String, ProposedAttributeMeta> attributes,
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
                String templateName = type.getName();

                // ----------------------------------------------------------------------
                // Store
                // ----------------------------------------------------------------------
                // repo_unit_template (
                //    templateid INT  NOT NULL,   -- from @template(id: …)
                //    name      TEXT NOT NULL,    -- type name
                // }
                String templateSql = """
                        INSERT INTO repo_unit_template (templateid, name)
                        VALUES (?, ?)
                        """;

                // repo_template_elements (
                //     templateid INT  NOT NULL,   -- from @template(id: …)
                //     attrid     INT  NOT NULL,   -- global attribute id
                //     alias      TEXT NOT NULL,   -- field name inside unit (template)
                //     idx        INT  NULL,       -- optional order / display position
                // }
                String elementsSql = """
                        INSERT INTO repo_template_elements (templateid, attrid, alias, idx)
                        VALUES (?,?,?,?)
                        """;

                final int _templateId = templateId;
                try {
                    repo.withConnection(conn -> {
                        try {
                            conn.setAutoCommit(false);

                            Database.usePreparedStatement(conn, templateSql, templateStmt -> {
                                templateStmt.setInt(1, _templateId);
                                templateStmt.setString(2, templateName);

                                Database.execute(templateStmt);

                                Database.usePreparedStatement(conn, elementsSql, elementsStmt -> {
                                    // Handle field definitions on types
                                    int idx = 0;
                                    for (FieldDefinition f : type.getFieldDefinitions()) {
                                        ++idx;
                                        String alias = f.getName();

                                        elementsStmt.clearParameters();
                                        elementsStmt.setInt(1, _templateId);

                                        info.add("   " + f.getName());

                                        // Handle @use directive on field definitions
                                        List<Directive> useDirectives = f.getDirectives("use");
                                        if (!useDirectives.isEmpty()) {
                                            for (Directive useDirective : useDirectives) {
                                                // @use "attribute" argument
                                                Argument arg = useDirective.getArgument("attribute");
                                                EnumValue value = (EnumValue) arg.getValue();

                                                ProposedAttributeMeta attributeMeta = attributes.get(value.getName());
                                                if (null != attributeMeta) {
                                                    int attrId = attributeMeta.id();

                                                    // ----------------------------------------------------------------------
                                                    // First check whether this template already exists in local database
                                                    // ----------------------------------------------------------------------
                                                    String key = templateName + "." + alias;
                                                    ExistingUnitTemplatesMeta existingTemplate = existingTemplates.get(key);
                                                    if (null != existingTemplate) {

                                                        if (existingTemplate.attrId != attrId) {
                                                            log.warn("Failed to load unit template {}.{}. New definition differs on existing 'attribute' {} -- skipping", templateName, alias, existingTemplate.attrId);
                                                            return;
                                                        }

                                                        if (existingTemplate.idx != idx) {
                                                            log.warn("Failed to load unit template {}.{}. New definition differs on existing 'index' {} -- skipping", templateName, alias, existingTemplate.idx);
                                                            return;
                                                        }
                                                    }

                                                    elementsStmt.setInt(2, attrId);
                                                    elementsStmt.setString(3, alias);
                                                    elementsStmt.setInt(4, idx);

                                                    info.add(" -> ");
                                                    info.add(value.getName());
                                                }
                                            }
                                        }

                                        Database.execute(elementsStmt);

                                        info.add("\n");
                                    }
                                });
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
            }
        }
    }

    private static class CompoundTemplateLoader {

        private record ExistingCompoundTemplatesMeta(int compoundAttrId, int idx, int childAttrId, String alias,
                                                     boolean required) {
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

        CompoundTemplateLoader(Repository repo) {
            this.repo = repo;
        }

        private Map<String, ExistingCompoundTemplatesMeta> loadExisting() {
            Map<String, ExistingCompoundTemplatesMeta> existingTemplates = new HashMap<>();

            // repo_compound_template (
            //    compound_attrid  INT  NOT NULL,      -- id of the COMPOUND attribute
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

        private void load(
                ObjectTypeDefinition type,
                List<Directive> compoundAttributeDirectivesOnType,
                Map<String, ProposedAttributeMeta> attributes,
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
                //    compound_attrid  INT  NOT NULL,      -- id of the COMPOUND attribute
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
                try {
                    repo.withConnection(conn -> {
                        try {
                            conn.setAutoCommit(false);

                            Database.usePreparedStatement(conn, sql, pStmt -> {
                                ProposedAttributeMeta compoundAttributeMeta = attributes.get(_compoundAttributeName);
                                if (null != compoundAttributeMeta) {
                                    final int compoundAttrid = compoundAttributeMeta.id();

                                    int idx = 0;
                       NEXT_FIELD:  for (FieldDefinition f : type.getFieldDefinitions()) {
                                        ++idx;
                                        pStmt.clearParameters();
                                        pStmt.setInt(1, compoundAttrid);
                                        pStmt.setInt(2, idx);

                                        info.add("   " + f.getName());

                                        // Handle @use directive on field definitions
                                        List<Directive> useDirectives = f.getDirectives("use");
                                        if (!useDirectives.isEmpty()) {
                                            for (Directive useDirective : useDirectives) {
                                                // @use "attribute" argument
                                                Argument arg = useDirective.getArgument("attribute");
                                                EnumValue value = (EnumValue) arg.getValue();

                                                ProposedAttributeMeta attributeMeta = attributes.get(value.getName());
                                                if (null != attributeMeta) {
                                                    int childAttrid = attributeMeta.id();
                                                    String alias = f.getName();
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
                                                        continue NEXT_FIELD;
                                                    }

                                                    pStmt.setInt(3, childAttrid);
                                                    pStmt.setString(4, alias);
                                                    pStmt.setBoolean(5, true); // required

                                                    info.add(" -> ");
                                                    info.add(value.getName());
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
            }
        }
    }
}
