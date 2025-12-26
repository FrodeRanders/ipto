package org.gautelis.ipto.graphql.configuration;

import graphql.language.*;
import org.gautelis.ipto.graphql.runtime.*;
import org.gautelis.ipto.repo.exceptions.ConfigurationException;
import graphql.GraphQL;
import graphql.schema.idl.RuntimeWiring;
import graphql.schema.idl.SchemaGenerator;
import graphql.schema.idl.SchemaParser;
import graphql.schema.idl.TypeDefinitionRegistry;
import org.gautelis.ipto.repo.db.Database;
import org.gautelis.ipto.graphql.runtime.scalars.BytesScalar;
import org.gautelis.ipto.graphql.runtime.scalars.DateTimeScalar;
import org.gautelis.ipto.graphql.runtime.scalars.LongScalar;
import org.gautelis.ipto.graphql.model.*;
import org.gautelis.ipto.repo.model.AttributeType;
import org.gautelis.ipto.repo.model.Context;
import org.gautelis.ipto.repo.model.KnownAttributes;
import org.gautelis.ipto.repo.model.Repository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;
import java.io.Reader;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class Configurator {
    private static final Logger log = LoggerFactory.getLogger(Configurator.class);

    public record GqlViewpoint(
            Map<String, GqlDatatypeShape> datatypes,
            Map<String, GqlAttributeShape> attributes,
            Map<String, GqlRecordShape> records,
            Map<String, GqlUnitTemplateShape> units,
            Map<String, GqlUnionShape> unions,
            Map<String, GqlOperationShape> operations
    ) {}

    public record CatalogViewpoint(
            Map<String, CatalogDatatype> datatypes,
            Map<String, CatalogAttribute> attributes,
            Map<String, CatalogRecord> records,
            Map<String, CatalogUnitTemplate> units
    ) {}

    private Configurator() {
    }

    public static Optional<GraphQL> load(
            Repository repo,
            Reader reader,
            PrintStream progress
    ) {
        final TypeDefinitionRegistry registry = new SchemaParser().parse(reader);

        // Prepare wiring up
        final RuntimeWiring.Builder runtimeWiring = RuntimeWiring.newRuntimeWiring();
        runtimeWiring
                .scalar(LongScalar.INSTANCE)
                .scalar(DateTimeScalar.INSTANCE)
                .scalar(BytesScalar.INSTANCE);

        // Determine queries, mutations and subscriptions
        Map<String, SchemaOperation> operationTypes = new HashMap<>();

        Optional<SchemaDefinition> _schemaDefinition = registry.schemaDefinition();
        if (_schemaDefinition.isPresent()) {
            SchemaDefinition schemaDefinition = _schemaDefinition.get();

            List<OperationTypeDefinition> otds = schemaDefinition.getOperationTypeDefinitions();
            for (OperationTypeDefinition otd : otds) {
                SchemaOperation operationType = switch (otd.getName()) {
                    case "query" -> SchemaOperation.QUERY;
                    case "mutation" -> SchemaOperation.MUTATION;
                    case "subscription" -> SchemaOperation.SUBSCRIPTION;
                    default -> null;
                };
                if (null == operationType) {
                    log.warn("\u21af Undefined operation type '{}' -- ignoring", otd.getName());
                    continue;
                }
                operationTypes.put(otd.getTypeName().getName(), operationType);
            }
        }

        // Setup GraphQL SDL view of things
        GqlViewpoint gql = loadFromFile(registry, operationTypes);
        //dump(gql, progress);

        // Setup Ipto view of things
        CatalogViewpoint ipto = loadFromCatalog(repo);
        //dump(ipto, progress);

        // Reconcile differences, i.e. create stuff if needed
        reconcile(repo, gql, ipto, ResolutionPolicy.PREFER_GQL, progress);

        RuntimeService runtimeService = new RuntimeService(repo, ipto);
        runtimeService.wire(runtimeWiring, gql, ipto);

        //
        repo.sync();

        //
        return Optional.of(
                GraphQL.newGraphQL(
                        new SchemaGenerator().makeExecutableSchema(registry, runtimeWiring.build())
                ).build()
        );
    }

    private static GqlViewpoint loadFromFile(TypeDefinitionRegistry registry, Map<String, SchemaOperation> operationTypes) {
        Map<String, GqlDatatypeShape> datatypes = Datatypes.derive(registry);
        Map<String, GqlAttributeShape> attributes = Attributes.derive(registry, datatypes);
        Map<String, GqlRecordShape> records = Records.derive(registry, attributes);
        Map<String, GqlUnitTemplateShape> templates = UnitTemplates.derive(registry, attributes);
        Map<String, GqlUnionShape> unions = Unions.derive(registry);
        Map<String, GqlOperationShape> operations  = Operations.derive(registry, operationTypes);

        return new GqlViewpoint(datatypes, attributes, records, templates, unions, operations);
    }

    private static CatalogViewpoint loadFromCatalog(Repository repo) {
        Map<String, CatalogDatatype> datatypes = Datatypes.read(repo);
        Map<String, CatalogAttribute> attributes = Attributes.read(repo);
        Map<String, CatalogRecord> records = Records.read(repo);
        Map<String, CatalogUnitTemplate> templates = UnitTemplates.read(repo);

        return new CatalogViewpoint(datatypes, attributes, records, templates);
    }

    private static void reconcile(
            Repository repo,
            GqlViewpoint gqlViewpoint,
            CatalogViewpoint catalogViewpoint,
            ResolutionPolicy policy,
            PrintStream progress
    ) {

        // --- Datatypes ---
        for (String key : gqlViewpoint.datatypes().keySet()) {
            if (!catalogViewpoint.datatypes().containsKey(key)) {
                log.error("\u21af Datatype '{}' not found in catalog", key);
                progress.println("Datatype '" + key + "' not found in catalog");
                continue;
            }
            GqlDatatypeShape gqlDatatype = gqlViewpoint.datatypes().get(key);
            CatalogDatatype iptoDatatype = catalogViewpoint.datatypes().get(key);
            if (!gqlDatatype.equals(iptoDatatype)) {
                log.error("\u21af GraphQL SDL and catalog datatype do not match: {} != {}", gqlDatatype, iptoDatatype);
                progress.println("GraphQL SDL and catalog datatype do not match: " + gqlDatatype +  " != " + iptoDatatype);
            }
        }

        // Attributes
        for (String key : gqlViewpoint.attributes().keySet()) {
            if (!catalogViewpoint.attributes().containsKey(key)) {
                log.info("\u21af Reconciling attribute '{}'...", key);
                progress.println("Reconciling attribute '" + key + "'...");

                CatalogAttribute attribute = addAttribute(repo, gqlViewpoint.attributes().get(key), progress);
                catalogViewpoint.attributes().put(key, attribute); // replace
                continue;
            }
            GqlAttributeShape gqlAttribute = gqlViewpoint.attributes().get(key);
            CatalogAttribute iptoAttribute = catalogViewpoint.attributes().get(key);
            if (!gqlAttribute.equals(iptoAttribute)) {
                log.error("\u21af GraphQL SDL and catalog attribute do not match: {} != {}", gqlAttribute, iptoAttribute);
                progress.println("GraphQL SDL and catalog attribute do not match: " + gqlAttribute +  " != " + iptoAttribute);
            }
        }

        // Records
        for (String key : gqlViewpoint.records().keySet()) {
            if (!catalogViewpoint.records().containsKey(key)) {
                log.info("\u21af Reconciling record '{}'...", key);
                progress.println("Reconciling record '" + key + "'...");

                CatalogRecord record = addRecord(
                        repo,
                        gqlViewpoint.records().get(key),
                        gqlViewpoint.attributes(),
                        catalogViewpoint.attributes(),
                        progress
                );
                catalogViewpoint.records().put(key, record); // replace
                continue;
            }
            GqlRecordShape gqlRecord = gqlViewpoint.records().get(key);
            CatalogRecord iptoRecord = catalogViewpoint.records().get(key);
            if (!gqlRecord.equals(iptoRecord)) {
                log.error("\u21af GraphQL SDL and catalog record do not match: {} != {}", gqlRecord, iptoRecord);
                progress.println("GraphQL SDL and catalog record do not match: " + gqlRecord +  " != " + iptoRecord);
            }
        }

        // Unit templates
        for (String key : gqlViewpoint.units.keySet()) {
            if (!catalogViewpoint.units().containsKey(key)) {
                log.info("\u21af Reconciling unit template '{}'...", key);
                progress.println("Reconciling unit template '" + key + "'...");

                CatalogUnitTemplate template = addUnitTemplate(
                        repo,
                        gqlViewpoint.units.get(key),
                        catalogViewpoint.attributes(),
                        progress
                );
                catalogViewpoint.units().put(key, template); // replace
                continue;
            }
            GqlUnitTemplateShape gqlTemplate = gqlViewpoint.units.get(key);
            CatalogUnitTemplate iptoTemplate = catalogViewpoint.units().get(key);

            if (!gqlTemplate.equals(iptoTemplate)) {
                log.error("\u21af GraphQL SDL and catalog template do not match: {} != {}", gqlTemplate, iptoTemplate);
                progress.println("GraphQL SDL and catalog template do not match: " + gqlTemplate +  " != " + iptoTemplate);
            }
        }

        if (log.isTraceEnabled()) {
            dump(gqlViewpoint, progress);
            dump(catalogViewpoint, progress);
        }
    }

    private static CatalogAttribute addAttribute(
            Repository repo,
            GqlAttributeShape gqlAttribute,
            PrintStream progress
    ) {
        // NOTE: attribute.attrId is adjusted later, after writing to repo_attribute
        CatalogAttribute attribute = new CatalogAttribute(
                gqlAttribute.alias,
                gqlAttribute.name,
                gqlAttribute.qualName,
                AttributeType.of(gqlAttribute.typeName),
                gqlAttribute.isArray
        );

        String sql = """
            INSERT INTO repo_attribute (attrtype, scalar, attrname, qualname, alias)
            VALUES (?,?,?,?,?)
            """;

        try {
            repo.withConnection(conn -> {
                try {
                    conn.setAutoCommit(false);

                    String[] generatedColumns = { "attrid" };
                    try (PreparedStatement pStmt = conn.prepareStatement(sql, generatedColumns)) {
                        int i = 0;
                        pStmt.setInt(++i, attribute.attrType().getType());
                        pStmt.setBoolean(++i, !attribute.isArray()); // Note negation
                        pStmt.setString(++i, attribute.attrName());
                        pStmt.setString(++i, attribute.qualifiedName());
                        pStmt.setString(++i, attribute.alias());

                        Database.executeUpdate(pStmt);

                        try (ResultSet rs = pStmt.getGeneratedKeys()) {
                            if (rs.next()) {
                                int attriId = rs.getInt(1);
                                attribute.setAttrId(attriId);
                            } else {
                                String info = "\u21af Failed to determine auto-generated attribute ID";
                                log.error(info); // This is nothing we can recover from
                                throw new ConfigurationException(info);
                            }
                        }
                    }

                    conn.commit();
                    log.info("\u21af Loaded attribute '{}' (attrid={}, name='{}', qual-name='{}')", attribute.alias(), attribute.attrId(), attribute.attrName(), attribute.qualifiedName());

                } catch (Throwable t) {
                    log.error("\u21af Failed to store attribute '{}' ({}, '{}'): {}", attribute.alias(), attribute.attrId(), gqlAttribute.name, t.getMessage(), t);
                    if (t.getCause() instanceof SQLException sqle) {
                        log.error("  ^--- {}", Database.squeeze(sqle));
                        String sqlState = sqle.getSQLState();

                        try {
                            conn.rollback();
                        } catch (SQLException rbe) {
                            log.error("\u21af Failed to rollback transaction: {}", Database.squeeze(rbe), rbe);
                        }

                        if (sqlState.startsWith("23")) {
                            // 23505 : duplicate key value violates unique constraint "repo_attribute_pk"
                            log.info("\u21af Attribute '{}' ({}, '{}') seems to already have been loaded", attribute.alias(), attribute.attrId(), attribute.attrName());
                        }
                    }
                }
            });
        } catch (SQLException sqle) {
            log.error("Failed to store attribute: {}", Database.squeeze(sqle));
        }

        return attribute;
    }

    private static CatalogRecord addRecord(
            Repository repo,
            GqlRecordShape gqlRecord,
            Map<String, GqlAttributeShape> gqlAttributes,
            Map<String, CatalogAttribute> catalogAttributes,
            PrintStream progress
    ) {

        String recordName = gqlRecord.typeName();
        String recordAttributeName = gqlRecord.attributeEnumName();
        List<GqlFieldShape> fields = gqlRecord.fields();

        // Determine attrId of record attribute
        CatalogAttribute recordAttribute = catalogAttributes.get(recordAttributeName);
        if (null == recordAttribute) {
            log.warn("\u21af No matching record attribute in catalog: {}", recordAttributeName);
            throw new RuntimeException("No matching record attribute: " + recordAttributeName);
        }

        final int recordAttributeId = recordAttribute.attrId();

        CatalogRecord catalogRecord = new CatalogRecord(recordName);
        catalogRecord.setRecordId(recordAttributeId);

        // repo_record_template (
        //    recordid  INT,
        //    name      TEXT,
        // )
        //
        // repo_record_template_elements (
        //    recordid   INT,     -- attribute id of record attribute
        //    idx        INT,
        //    attrid     INT,     -- sub-attribute
        //    alias      TEXT,
        //    mandatory  BOOLEAN
        // )
        String recordSql = """
                        INSERT INTO repo_record_template (recordid, name)
                        VALUES (?,?)
                        """;

        String elementsSql = """
                        INSERT INTO repo_record_template_elements (recordid, attrid, idx, alias)
                        VALUES (?,?,?,?)
                        """;

        try {

            //
            repo.withConnection(conn -> {
                try {
                    conn.setAutoCommit(false);

                    // Table: repo_record_template
                    Database.usePreparedStatement(conn, recordSql, pStmt -> {
                        try {
                            int i = 0;
                            pStmt.setInt(++i, recordAttributeId);
                            pStmt.setString(++i, catalogRecord.recordName);

                            Database.execute(pStmt);

                        } catch (SQLException sqle) {
                            String sqlState = sqle.getSQLState();
                            conn.rollback();

                            if (sqlState.startsWith("23")) {
                                // 23505 : duplicate key value violates unique constraint "repo_record_template_pk"
                                log.info("\u21af Record '{}' seems to already have been loaded", recordName);
                            } else {
                                throw sqle;
                            }
                        }
                    });

                    // Table: repo_record_template_elements
                    Database.usePreparedStatement(conn, elementsSql, pStmt -> {
                        int idx = 0; // index into record
                        for (GqlFieldShape field : fields) {
                            // Determine attrId of field in record
                            GqlAttributeShape fieldAttribute = gqlAttributes.get(field.fieldName());

                            if (null == fieldAttribute) {
                                log.warn("\u21af No matching field attribute: '{}' (name='{}')", field.fieldName(), field.usedAttributeName());
                                continue;
                            }

                            CatalogAttribute attribute = catalogAttributes.get(field.fieldName());
                            if (null == attribute) {
                                log.warn("\u21af No matching catalog attribute: '{}' (name='{}')", field.fieldName(), field.usedAttributeName());
                                continue;
                            }

                            //
                            pStmt.clearParameters();

                            int i = 0;
                            pStmt.setInt(++i, catalogRecord.recordAttrId);
                            pStmt.setInt(++i, attribute.attrId());
                            pStmt.setInt(++i, ++idx);
                            pStmt.setString(++i, field.fieldName());

                            Database.execute(pStmt);
                            catalogRecord.addField(attribute);
                        }
                    });

                    conn.commit();
                    log.info("\u21af Loaded record '{}'", recordName);

                } catch (Throwable t) {
                    log.error("\u21af Failed to store record '{}': {}", recordName, t.getMessage(), t);
                    if (t.getCause() instanceof SQLException sqle) {
                        log.error("  ^--- {}", Database.squeeze(sqle));
                        String sqlState = sqle.getSQLState();

                        try {
                            conn.rollback();

                        } catch (SQLException rbe) {
                            log.error("\u21af Failed to rollback transaction: {}", Database.squeeze(rbe), rbe);
                        }

                        if (sqlState.startsWith("23")) {
                            // 23505 : duplicate key value violates unique constraint "repo_record_template_pk"
                            log.info("\u21af Record '{}' seems to already have been loaded", recordName);
                        }
                    }
                }
            });
        } catch (SQLException sqle) {
            log.error("\u21af Failed to store record: {}", Database.squeeze(sqle));
        }

        return catalogRecord;
    }

    private static CatalogUnitTemplate addUnitTemplate(
            Repository repo,
            GqlUnitTemplateShape gqlUnitTemplate,
            Map<String, CatalogAttribute> catalogAttributes,
            PrintStream progress
    ) {

        // NOTE: template.templateId is adjusted later, after writing to repo_unit_template
        CatalogUnitTemplate template = new CatalogUnitTemplate(gqlUnitTemplate.typeName());

        String templateSql = """
                        INSERT INTO repo_unit_template (name)
                        VALUES (?)
                        """;

        String elementsSql = """
                        INSERT INTO repo_unit_template_elements (templateid, attrid, idx, alias)
                        VALUES (?,?,?,?)
                        """;

        try {
            repo.withConnection(conn -> {
                try {
                    conn.setAutoCommit(false);

                    // Table: repo_unit_template
                    String[] generatedColumns = { "templateid" };
                    try (PreparedStatement pStmt = conn.prepareStatement(templateSql, generatedColumns)) {
                        int i = 0;
                        pStmt.setString(++i, template.templateName);

                        Database.executeUpdate(pStmt);

                        try (ResultSet rs = pStmt.getGeneratedKeys()) {
                            if (rs.next()) {
                                int templateId = rs.getInt(1);
                                template.setTemplateId(templateId);
                            } else {
                                String info = "\u21af Failed to determine auto-generated unit template ID";
                                log.error(info); // This is nothing we can recover from
                                throw new ConfigurationException(info);
                            }
                        }
                    }

                    // Table: repo_unit_template_elements
                    Database.usePreparedStatement(conn, elementsSql, pStmt -> {
                        int idx = 0; // index into record
                        for (GqlFieldShape field : gqlUnitTemplate.fields()) {
                            // Determine attrId of field in record
                            CatalogAttribute attribute  = catalogAttributes.get(field.fieldName());
                            if (null == attribute) {
                                log.warn("\u21af No matching catalog attribute: '{}' ('{}')", field.fieldName(), field.usedAttributeName());
                                continue;
                            }

                            //
                            pStmt.clearParameters();

                            int i = 0;
                            pStmt.setInt(++i, template.templateId);
                            pStmt.setInt(++i, attribute.attrId());
                            pStmt.setInt(++i, ++idx);
                            pStmt.setString(++i, attribute.attrName());

                            Database.execute(pStmt);
                            template.addField(attribute);
                        }
                    });

                    conn.commit();
                    log.info("\u21af Loaded unit template '{}'", template.templateName);

                } catch (Throwable t) {
                    log.error("\u21af Failed to store unit template '{}': {}", template.templateName, t.getMessage(), t);
                    if (t.getCause() instanceof SQLException sqle) {
                        log.error("  ^--- {}", Database.squeeze(sqle));
                        String sqlState = sqle.getSQLState();

                        try {
                            conn.rollback();

                        } catch (SQLException rbe) {
                            log.error("\u21af Failed to rollback transaction: {}", Database.squeeze(rbe), rbe);
                        }

                        if (sqlState.startsWith("23")) {
                            // 23505 : duplicate key value violates unique constraint
                            log.info("\u21af Unit template '{}' seems to already have been loaded", template.templateName);
                        }
                    }
                }
            });
        } catch (SQLException sqle) {
            log.error("\u21af Failed to store template: {}", Database.squeeze(sqle));
        }

        return template;
    }

    private static void dump(GqlViewpoint gql, PrintStream out) {
        if (null == out)
            return;

        out.println("===< From external GraphQL SDL >===");
        out.println("--- Datatypes ---");
        for (Map.Entry<String, GqlDatatypeShape> entry : gql.datatypes().entrySet()) {
            out.println("  " + entry.getKey() + " -> " + entry.getValue());
        }
        out.println();

        out.println("--- Attributes ---");
        for (Map.Entry<String, GqlAttributeShape> entry : gql.attributes().entrySet()) {
            out.println("  " + entry.getKey() + " -> " + entry.getValue());
        }
        out.println();

        out.println("--- Records ---");
        for (Map.Entry<String, GqlRecordShape> entry : gql.records().entrySet()) {
            out.println("  " + entry.getKey() + " -> " + entry.getValue());
        }
        out.println();

        out.println("--- Unit templates ---");
        for (Map.Entry<String, GqlUnitTemplateShape> entry : gql.units().entrySet()) {
            out.println("  " + entry.getKey() + " -> " + entry.getValue());
        }
        out.println();

        out.println("--- Unions ---");
        for (Map.Entry<String, GqlUnionShape> entry : gql.unions().entrySet()) {
            out.println("  " + entry.getKey() + " -> " + entry.getValue());
        }
        out.println();

        out.println("--- Operations ---");
        for (Map.Entry<String, GqlOperationShape> entry : gql.operations().entrySet()) {
            out.println("  " + entry.getKey() + " -> " + entry.getValue());
        }
        out.println();
    }

    public static void dump(CatalogViewpoint ipto, PrintStream out) {
        if (null == out)
            return;

        out.println("===< From internal catalog >===");
        out.println("--- Datatypes ---");
        for (Map.Entry<String, CatalogDatatype> entry : ipto.datatypes().entrySet()) {
            out.println("  " + entry.getKey() + " -> " + entry.getValue());
        }
        out.println();

        out.println("--- Attributes ---");
        for (Map.Entry<String, CatalogAttribute> entry : ipto.attributes().entrySet()) {
            out.println("  " + entry.getKey() + " -> " + entry.getValue());
        }
        out.println();

        out.println("--- Records ---");
        for (Map.Entry<String, CatalogRecord> entry : ipto.records().entrySet()) {
            out.println("  " + entry.getKey() + " -> " + entry.getValue());
        }
        out.println();

        out.println("--- Unit templates ---");
        for (Map.Entry<String, CatalogUnitTemplate> entry : ipto.units().entrySet()) {
            out.println("  " + entry.getKey() + " -> " + entry.getValue());
        }
        out.println();
    }
}
