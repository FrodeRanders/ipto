package org.gautelis.repo.graphql;

import com.fasterxml.jackson.databind.ObjectMapper;
import graphql.GraphQL;
import graphql.language.*;
import graphql.schema.DataFetcher;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLOutputType;
import graphql.schema.idl.RuntimeWiring;
import graphql.schema.idl.SchemaGenerator;
import graphql.schema.idl.SchemaParser;
import graphql.schema.idl.TypeDefinitionRegistry;
import org.gautelis.repo.db.Database;
import org.gautelis.repo.exceptions.AttributeTypeException;
import org.gautelis.repo.graphql.scalars.BytesScalar;
import org.gautelis.repo.graphql.scalars.DateTimeScalar;
import org.gautelis.repo.graphql.scalars.LongScalar;
import org.gautelis.repo.model.Repository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Reader;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

import static org.gautelis.repo.model.attributes.Type.COMPOUND;
import static org.gautelis.repo.model.attributes.Type.of;

public class Configurator {
    private static final Logger log = LoggerFactory.getLogger(Configurator.class);

    //
    public record ExistingDatatypeMeta(int id, String nameInSchema) {}

    public record ProposedAttributeMeta(int attrId, String nameInSchema, int attrType, boolean isVector, String nameInIpto, String qualifiedName, String description) {}

    private final Repository repo;
    private final RepositoryService repoService;
    private final TypeDefinitionRegistry registry;

    private final Map<String, ExistingDatatypeMeta> datatypes = new HashMap<>();
    private final Map<String, ProposedAttributeMeta> attributesSchemaView = new HashMap<>();
    private final Map<Integer, ProposedAttributeMeta> attributesIptoView = new HashMap<>();

    public Configurator(Repository repo, Reader reader) {
        this.repo = repo;
        this.repoService = new RepositoryService(repo, datatypes, attributesIptoView);
        this.registry = new SchemaParser().parse(reader);
    }

    public Optional<GraphQL> load() {
        RuntimeWiring.Builder runtimeWiring = RuntimeWiring.newRuntimeWiring();

        // Register (known) scalars
        runtimeWiring.scalar(LongScalar.INSTANCE)
                     .scalar(DateTimeScalar.INSTANCE)
                     .scalar(BytesScalar.INSTANCE);

        wireQueries(runtimeWiring);

        // Load attributes
        AttributeLoader attributeLoader = new AttributeLoader(repo);

        try {
            for (EnumTypeDefinition enumeration : registry.getTypes(EnumTypeDefinition.class)) {
                List<Directive> enumDirectives = enumeration.getDirectives();
                for (Directive directive : enumDirectives) {
                    switch (directive.getName()) {
                        case "datatypeRegistry" -> {
                            // -----------------------------------------------------------
                            // This is not configurable, but it is part of the
                            // configuration SDL as reference.
                            // -----------------------------------------------------------
                            if (!verifyDatatypes(enumeration, datatypes)) {
                                return Optional.empty();
                            }
                        }
                        case "attributeRegistry" -> attributeLoader.load(enumeration, datatypes, attributesSchemaView, attributesIptoView, runtimeWiring, repoService);
                        default -> {
                        }
                    }
                }
            }

            // Iterate over types
            UnitTemplateLoader unitTemplateLoader = new UnitTemplateLoader(repo);
            CompoundTemplateLoader compoundTemplateLoader = new CompoundTemplateLoader(repo);

            for (ObjectTypeDefinition type : registry.getTypes(ObjectTypeDefinition.class)) {
                Collection<String> info = new ArrayList<>();
                info.add("\n");

                // Handle @template directives on object types
                List<Directive> templateDirectivesOnType = type.getDirectives("template");
                if (!templateDirectivesOnType.isEmpty()) {
                    unitTemplateLoader.load(type, templateDirectivesOnType, attributesSchemaView, runtimeWiring, repoService, info);

                } else {
                    // Handle @compound directives on object types
                    List<Directive> compoundAttributeDirectivesOnType = type.getDirectives("compound");
                    if (!compoundAttributeDirectivesOnType.isEmpty()) {
                        compoundTemplateLoader.load(type, compoundAttributeDirectivesOnType, attributesSchemaView, runtimeWiring, repoService, info);
                    } else {
                        // Query or Mutation?
                        type.getFieldDefinitions().forEach(field -> {
                            if (field.getType() instanceof TypeName typeName) {
                                log.trace("{}(...) : {}", field.getName(), typeName.getName());
                            }
                        });
                    }
                }

                log.info(String.join("", info));
            }
        } catch (Throwable t) {
            log.error("Failed to load configuration: {}", t.getMessage(), t);

            if (t.getCause() instanceof SQLException sqle) {
                log.error("  ^-- more details: {}", Database.squeeze(sqle));
            }
        }

        return Optional.of(
                GraphQL.newGraphQL(
                    new SchemaGenerator().makeExecutableSchema(registry, runtimeWiring.build())
                ).build()
        );
    }

    private void wireQueries(RuntimeWiring.Builder runtimeWiring) {
        record UnitIdentification(int tenantId, long unitId) {}
        ObjectMapper objectMapper = new ObjectMapper();

        // Point lookup for specific unit
        DataFetcher<?> unitById = env -> {
            log.trace("Query::unit(id : {})", (Object) env.getArgument("id"));

            UnitIdentification id = objectMapper.convertValue(env.getArgument("id"), UnitIdentification.class);
            return repoService.loadUnit(id.tenantId(), id.unitId());
        };

        runtimeWiring.type("Query", t -> t.dataFetcher("unit", unitById));
        log.info("Wiring: Query unit");
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
                        log.error("Not an official data type: {} with numeric attrId {}", enumValueDefinition.getName(), _id.getValue().intValue(), ate);
                        return false;
                    }
                }

                arg = enumValueDirective.getArgument("basictype");
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
                EnumTypeDefinition enumType,
                Map<String, ExistingDatatypeMeta> datatypes,
                Map<String, ProposedAttributeMeta> attributesSchemaView,
                Map<Integer, ProposedAttributeMeta> attributesIptoView,
                RuntimeWiring.Builder runtimeWiring,
                RepositoryService repoService
        ) {
            Map<String, ExistingAttributeMeta> existingAttributes = loadExisting();

            runtimeWiring.type(enumType.getName(), builder -> {
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

                                        // 4: attribute name in ipto (i.e. an alias) --------------------------
                                        String nameInIpto = null;
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
                                                log.trace("Checking attribute {} {alias={}, type={}, vector={}}", qualName, nameInIpto, org.gautelis.repo.model.attributes.Type.of(attrType), isVector);

                                                // This attribute has already been loaded -- check similarity
                                                if (existingAttribute.vector != isVector) {
                                                    log.warn("Failed to load attribute {}. New definition differs on existing 'dimensionality' (vector) {} -- skipping", qualName, existingAttribute.vector);
                                                    continue;
                                                }

                                                if (!existingAttribute.attrName.equals(nameInIpto)) {
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

                                            // We have several names for this attribute;
                                            //  - the name used in the schema,
                                            //  - the name used in IPTO,
                                            //  - a qualified name (assumed globally unique).
                                            //
                                            // We need to be able to look up attributes both from a schema viewpoint
                                            // (name in schema) as well as from an IPTO viewpoint (name in IPTO).
                                            ProposedAttributeMeta attributeMeta = new ProposedAttributeMeta(attrId, nameInSchema, attrType, isVector, nameInIpto, qualName, description);
                                            attributesSchemaView.put(nameInSchema, attributeMeta);
                                            attributesIptoView.put(attrId, attributeMeta);

                                            // ----------------------------------------------------------------------
                                            // Tell GraphQL how to fetch this specific attribute, given that the
                                            // unit has already been retrieved and stored in cache (i.e. the
                                            // DataFetchingEnvironment)
                                            // ----------------------------------------------------------------------
                                            final int _attrId = attrId;
                                            final String _attrName = nameInIpto; // TODO

                                            DataFetcher<?> fetcher;
                                            if (isVector) {
                                                fetcher = env -> {
                                                    log.trace("Fetching vector attribute {} ({})", _attrId, _attrName);
                                                    UnitSnapshot snap = env.getSource();
                                                    return repoService.getVector(snap, _attrId);
                                                };
                                            } else {
                                                fetcher = env -> {
                                                    log.trace("Fetching scalar attribute {} ({})", _attrId, _attrName);
                                                    UnitSnapshot snap = env.getSource();
                                                    return repoService.getScalar(snap, _attrId);
                                                };
                                            }
                                            builder.dataFetcher(enumValueDefinition.getName(), fetcher);
                                            log.info("Wiring: {} {}", enumType.getName(), enumValueDefinition.getName());

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

                return builder;
            });
        }
    }

    private static class UnitTemplateLoader {
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

        private void load(
                ObjectTypeDefinition type,
                List<Directive> templateDirectivesOnType,
                Map<String, ProposedAttributeMeta> attributes,
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
                                        String nameInSchema = f.getName();

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

                                                ProposedAttributeMeta attributeMeta = attributes.get(value.getName());
                                                if (null != attributeMeta) {
                                                    int attrId = attributeMeta.attrId();
                                                    boolean isVector = attributeMeta.isVector();

                                                    // ----------------------------------------------------------------------
                                                    // First check whether this template already exists in local database
                                                    // ----------------------------------------------------------------------
                                                    String key = templateName + "." + nameInSchema;
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

                                                    DataFetcher<?> fetcher = env -> {
                                                        // Hey, my mission in life is to provide support for
                                                        // resolving a single attribute 'nameInSchema' in a
                                                        // single type 'templateName'. Everything I need later,
                                                        // I have access to right now so I capture this information
                                                        // for later.
                                                        UnitSnapshot snap = env.getSource();
                                                        log.trace("Fetching data from unit {}: {}.{}", templateName, snap.tenantId, snap.unitId);

                                                        if (isVector) {
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

        private void load(
                ObjectTypeDefinition type,
                List<Directive> compoundAttributeDirectivesOnType,
                Map<String, ProposedAttributeMeta> attributes,
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
                                    ProposedAttributeMeta compoundAttributeMeta = attributes.get(_compoundAttributeName);
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

                                                    ProposedAttributeMeta attributeMeta = attributes.get(value.getName());
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
}
