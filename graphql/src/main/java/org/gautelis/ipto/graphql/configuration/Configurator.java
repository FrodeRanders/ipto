/*
 * Copyright (C) 2025-2026 Frode Randers
 * All rights reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gautelis.ipto.graphql.configuration;

import graphql.language.*;
import org.gautelis.ipto.graphql.runtime.*;
import org.gautelis.ipto.repo.exceptions.BaseException;
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
import java.util.function.Consumer;

public class Configurator {
    private static final Logger log = LoggerFactory.getLogger(Configurator.class);
    private static final String DEFAULT_DESCRIPTION_LANG = "SE";

    public record GqlViewpoint(
            Map<String, GqlDatatypeShape> datatypes,
            Map<String, GqlAttributeShape> attributes,
            Map<String, GqlRecordShape> records,
            Map<String, GqlTemplateShape> templates,
            Map<String, GqlUnionShape> unions,
            Map<String, GqlOperationShape> operations
    ) {}

    public record CatalogViewpoint(
            Map<String, CatalogDatatype> datatypes,
            Map<String, CatalogAttribute> attributes,
            Map<String, CatalogRecord> records,
            Map<String, CatalogTemplate> templates
    ) {}

    private Configurator() {
    }

    /**
     * Loads GraphQL SDL and infers attributes, records and templates
     * from the various 'type' definitions. Will populate the Ipto database
     * accordingly.
     * <p>
     * Note: callers are responsible for invoking repo.sync() after load().
     * <p>
     * @param repo
     * @param reader
     * @param operationsWireBlock
     * @param progress
     * @return
     */
    public static Optional<GraphQL> load(
            Repository repo,
            Reader reader,
            Consumer<OperationsWireParameters> operationsWireBlock,
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
                    log.warn("↯ Undefined operation type '{}' -- ignoring", otd.getName());
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
        reconcile(repo, gql, ipto, progress);

        RuntimeService runtimeService = new RuntimeService(repo, ipto);
        runtimeService.wire(runtimeWiring, gql, ipto);

        // Wire operations
        if (null != operationsWireBlock) {
            operationsWireBlock.accept(new OperationsWireParameters(runtimeWiring, runtimeService, repo));
        }

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
        Map<String, GqlTemplateShape> templates = Templates.derive(registry, attributes);
        Map<String, GqlUnionShape> unions = Unions.derive(registry);
        Map<String, GqlOperationShape> operations  = Operations.derive(registry, operationTypes);

        return new GqlViewpoint(datatypes, attributes, records, templates, unions, operations);
    }

    private static CatalogViewpoint loadFromCatalog(Repository repo) {
        Map<String, CatalogDatatype> datatypes = Datatypes.read(repo);
        Map<String, CatalogAttribute> attributes = Attributes.read(repo);
        Map<String, CatalogRecord> records = Records.read(repo);
        Map<String, CatalogTemplate> templates = Templates.read(repo);

        return new CatalogViewpoint(datatypes, attributes, records, templates);
    }

    private static void reconcile(
            Repository repo,
            GqlViewpoint gqlViewpoint,
            CatalogViewpoint catalogViewpoint,
            PrintStream progress
    ) {

        // --- Datatypes ---
        for (String key : gqlViewpoint.datatypes().keySet()) {
            if (!catalogViewpoint.datatypes().containsKey(key)) {
                log.error("↯ Datatype '{}' not found in catalog", key);
                progress.println("Datatype '" + key + "' not found in catalog");
                continue;
            }
            GqlDatatypeShape gqlDatatype = gqlViewpoint.datatypes().get(key);
            CatalogDatatype iptoDatatype = catalogViewpoint.datatypes().get(key);
            if (!gqlDatatype.equals(iptoDatatype)) {
                log.error("↯ GraphQL SDL and catalog datatype do not match: {} != {}", gqlDatatype, iptoDatatype);
                progress.println("GraphQL SDL and catalog datatype do not match: " + gqlDatatype +  " != " + iptoDatatype);
            }
        }

        // Attributes
        for (String key : gqlViewpoint.attributes().keySet()) {
            if (!catalogViewpoint.attributes().containsKey(key)) {
                log.info("↯ Reconciling attribute '{}'...", key);
                progress.println("Reconciling attribute '" + key + "'...");

                CatalogAttribute attribute = addAttribute(repo, gqlViewpoint.attributes().get(key), progress);
                catalogViewpoint.attributes().put(key, attribute); // replace
                continue;
            }
            GqlAttributeShape gqlAttribute = gqlViewpoint.attributes().get(key);
            CatalogAttribute iptoAttribute = catalogViewpoint.attributes().get(key);
            if (!gqlAttribute.equals(iptoAttribute)) {
                log.error("↯ GraphQL SDL and catalog attribute do not match: {} != {}", gqlAttribute, iptoAttribute);
                progress.println("GraphQL SDL and catalog attribute do not match: " + gqlAttribute +  " != " + iptoAttribute);
            }
        }

        // Records
        for (String key : gqlViewpoint.records().keySet()) {
            if (!catalogViewpoint.records().containsKey(key)) {
                log.info("↯ Reconciling record '{}'...", key);
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
                log.error("↯ GraphQL SDL and catalog record do not match: {} != {}", gqlRecord, iptoRecord);
                progress.println("GraphQL SDL and catalog record do not match: " + gqlRecord +  " != " + iptoRecord);
            }
        }

        // Templates
        for (String key : gqlViewpoint.templates.keySet()) {
            if (!catalogViewpoint.templates().containsKey(key)) {
                log.info("↯ Reconciling template '{}'...", key);
                progress.println("Reconciling template '" + key + "'...");

                CatalogTemplate template = addUnitTemplate(
                        repo,
                        gqlViewpoint.templates.get(key),
                        catalogViewpoint.attributes(),
                        progress
                );
                catalogViewpoint.templates().put(key, template); // replace
                continue;
            }
            GqlTemplateShape gqlTemplate = gqlViewpoint.templates.get(key);
            CatalogTemplate iptoTemplate = catalogViewpoint.templates().get(key);

            if (!gqlTemplate.equals(iptoTemplate)) {
                log.error("↯ GraphQL SDL and catalog template do not match: {} != {}", gqlTemplate, iptoTemplate);
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

        try {
            Optional<KnownAttributes.AttributeInfo> info = repo.createAttribute(
                    attribute.alias(),
                    attribute.attrName(),
                    attribute.qualifiedName(),
                    attribute.attrType(),
                    attribute.isArray()
            );

            if (info.isPresent()) {
                attribute.setAttrId(info.get().id);
                log.info("↯ Loaded attribute '{}' (attrid={}, name='{}', qual-name='{}')", attribute.alias(), attribute.attrId(), attribute.attrName(), attribute.qualifiedName());
                storeAttributeDescription(repo, attribute.attrId(), gqlAttribute);
            } else {
                log.error("↯ Failed to store attribute '{}' ({}, '{}')", attribute.alias(), attribute.attrId(), gqlAttribute.name);
            }
        } catch (BaseException ex) {
            log.error("↯ Failed to store attribute '{}' ({}, '{}'): {}", attribute.alias(), attribute.attrId(), gqlAttribute.name, ex.getMessage(), ex);
        }

        return attribute;
    }

    private static void storeAttributeDescription(Repository repo, int attrId, GqlAttributeShape gqlAttribute) {
        if (gqlAttribute == null || gqlAttribute.description == null || gqlAttribute.description.isBlank()) {
            return;
        }

        String alias = gqlAttribute.name != null ? gqlAttribute.name : gqlAttribute.alias;
        if (alias == null || alias.isBlank()) {
            return;
        }

        String sql = """
                INSERT INTO repo_attribute_description (attrid, lang, alias, description)
                VALUES (?,?,?,?)
                """;

        try {
            repo.withConnection(conn -> {
                Database.usePreparedStatement(conn, sql, pStmt -> {
                    try {
                        int i = 0;
                        pStmt.setInt(++i, attrId);
                        pStmt.setString(++i, DEFAULT_DESCRIPTION_LANG);
                        pStmt.setString(++i, alias);
                        pStmt.setString(++i, gqlAttribute.description);
                        Database.executeUpdate(pStmt);
                    } catch (SQLException sqle) {
                        String sqlState = sqle.getSQLState();
                        if (sqlState != null && sqlState.startsWith("23")) {
                            log.info("↯ Attribute description already exists for attrId={} lang={}", attrId, DEFAULT_DESCRIPTION_LANG);
                            return;
                        }
                        throw sqle;
                    }
                });
            });
        } catch (SQLException sqle) {
            log.warn("↯ Failed to store attribute description for attrId={}: {}", attrId, Database.squeeze(sqle));
        }
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
            log.warn("↯ No matching record attribute in catalog: {}", recordAttributeName);
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
                                log.info("↯ Record '{}' seems to already have been loaded", recordName);
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
                                log.warn("↯ No matching field attribute: '{}' (name='{}')", field.fieldName(), field.usedAttributeName());
                                continue;
                            }

                            CatalogAttribute attribute = catalogAttributes.get(field.fieldName());
                            if (null == attribute) {
                                log.warn("↯ No matching catalog attribute: '{}' (name='{}')", field.fieldName(), field.usedAttributeName());
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
                    log.info("↯ Loaded record '{}'", recordName);

                } catch (Throwable t) {
                    log.error("↯ Failed to store record '{}': {}", recordName, t.getMessage(), t);
                    if (t.getCause() instanceof SQLException sqle) {
                        log.error("  ^--- {}", Database.squeeze(sqle));
                        String sqlState = sqle.getSQLState();

                        try {
                            conn.rollback();

                        } catch (SQLException rbe) {
                            log.error("↯ Failed to rollback transaction: {}", Database.squeeze(rbe), rbe);
                        }

                        if (sqlState.startsWith("23")) {
                            // 23505 : duplicate key value violates unique constraint "repo_record_template_pk"
                            log.info("↯ Record '{}' seems to already have been loaded", recordName);
                        }
                    }
                }
            });
        } catch (SQLException sqle) {
            log.error("↯ Failed to store record: {}", Database.squeeze(sqle));
        }

        return catalogRecord;
    }

    private static CatalogTemplate addUnitTemplate(
            Repository repo,
            GqlTemplateShape gqlUnitTemplate,
            Map<String, CatalogAttribute> catalogAttributes,
            PrintStream progress
    ) {

        // NOTE: template.templateId is adjusted later, after writing to repo_unit_template
        CatalogTemplate template = new CatalogTemplate(gqlUnitTemplate.typeName());

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
                                String info = "↯ Failed to determine auto-generated template ID";
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
                                log.warn("↯ No matching catalog attribute: '{}' ('{}')", field.fieldName(), field.usedAttributeName());
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
                    log.info("↯ Loaded template '{}'", template.templateName);

                } catch (Throwable t) {
                    log.error("↯ Failed to store template '{}': {}", template.templateName, t.getMessage(), t);
                    if (t.getCause() instanceof SQLException sqle) {
                        log.error("  ^--- {}", Database.squeeze(sqle));
                        String sqlState = sqle.getSQLState();

                        try {
                            conn.rollback();

                        } catch (SQLException rbe) {
                            log.error("↯ Failed to rollback transaction: {}", Database.squeeze(rbe), rbe);
                        }

                        if (sqlState.startsWith("23")) {
                            // 23505 : duplicate key value violates unique constraint
                            log.info("↯ Unit template '{}' seems to already have been loaded", template.templateName);
                        }
                    }
                }
            });
        } catch (SQLException sqle) {
            log.error("↯ Failed to store template: {}", Database.squeeze(sqle));
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

        out.println("--- Templates ---");
        for (Map.Entry<String, GqlTemplateShape> entry : gql.templates().entrySet()) {
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

        out.println("--- Templates ---");
        for (Map.Entry<String, CatalogTemplate> entry : ipto.templates().entrySet()) {
            out.println("  " + entry.getKey() + " -> " + entry.getValue());
        }
        out.println();
    }
}
