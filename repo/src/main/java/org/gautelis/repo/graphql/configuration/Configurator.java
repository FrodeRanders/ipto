package org.gautelis.repo.graphql.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import graphql.GraphQL;
import graphql.language.OperationTypeDefinition;
import graphql.language.SchemaDefinition;
import graphql.schema.DataFetcher;
import graphql.schema.idl.RuntimeWiring;
import graphql.schema.idl.SchemaGenerator;
import graphql.schema.idl.SchemaParser;
import graphql.schema.idl.TypeDefinitionRegistry;
import org.gautelis.repo.db.Database;
import org.gautelis.repo.graphql.runtime.Box;
import org.gautelis.repo.graphql.model.Query;
import org.gautelis.repo.graphql.runtime.scalars.BytesScalar;
import org.gautelis.repo.graphql.runtime.scalars.DateTimeScalar;
import org.gautelis.repo.graphql.runtime.scalars.LongScalar;
import org.gautelis.repo.graphql.model.*;
import org.gautelis.repo.graphql.runtime.RuntimeService;
import org.gautelis.repo.model.AttributeType;
import org.gautelis.repo.model.Repository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;
import java.io.Reader;
import java.sql.SQLException;
import java.util.*;

public class Configurator {
    private static final Logger log = LoggerFactory.getLogger(Configurator.class);

    private static ObjectMapper objectMapper = new ObjectMapper();


    public record GqlViewpoint(
            Map<String, GqlDatatypeShape> datatypes,
            Map<String, GqlAttributeShape> attributes,
            Map<String, GqlRecordShape> records,
            Map<String, GqlUnitShape> units,
            Map<String, GqlOperationShape> operations
    ) {}

    public record CatalogViewpoint(
            Map<String, CatalogDatatype> datatypes,
            Map<String, CatalogAttribute> attributes,
            Map<String, CatalogRecord> records,
            Map<String, CatalogUnit> units
    ) {}

    private Configurator() {
    }

    public static Optional<GraphQL> load(Repository repo, Reader reader, PrintStream progress) {
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
                    log.warn("Undefined operation type '{}' -- ignoring", otd.getName());
                    continue;
                }
                operationTypes.put(otd.getTypeName().getName(), operationType);
            }
        }

        // Setup GraphQL SDL view of things
        GqlViewpoint gql = loadFromFile(registry, operationTypes);
        dump(gql, progress);

        // Setup Ipto view of things
        CatalogViewpoint ipto = loadFromCatalog(repo);
        dump(ipto, progress);

        // Reconcile differences, i.e. create stuff if needed
        reconcile(repo, gql, ipto, ResolutionPolicy.PREFER_GQL, progress);

        RuntimeService runtimeService = new RuntimeService(repo, ipto);
        wire(runtimeWiring, repo, runtimeService, operationTypes, gql, ipto);

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
        Map<String, GqlUnitShape> templates = Templates.derive(registry, attributes);
        Map<String, GqlOperationShape> operations  = Operations.derive(registry, operationTypes);

        return new GqlViewpoint(datatypes, attributes, records, templates, operations);
    }

    private static CatalogViewpoint loadFromCatalog(Repository repo) {
        Map<String, CatalogDatatype> datatypes = Datatypes.read(repo);
        Map<String, CatalogAttribute> attributes = Attributes.read(repo);
        Map<String, CatalogRecord> records = Records.read(repo);
        Map<String, CatalogUnit> templates = Templates.read(repo);

        return new CatalogViewpoint(datatypes, attributes, records, templates);
    }

    private static void reconcile(Repository repo, GqlViewpoint gqlViewpoint, CatalogViewpoint catalogViewpoint, ResolutionPolicy policy, PrintStream progress) {

        // --- Datatypes ---
        for (String key : gqlViewpoint.datatypes().keySet()) {
            if (!catalogViewpoint.datatypes().containsKey(key)) {
                log.error("Datatype '{}' not found in catalog", key);
                progress.println("Datatype '" + key + "' not found in catalog");
                continue;
            }
            GqlDatatypeShape gqlDatatype = gqlViewpoint.datatypes().get(key);
            CatalogDatatype iptoDatatype = catalogViewpoint.datatypes().get(key);
            if (!gqlDatatype.equals(iptoDatatype)) {
                log.error("GraphQL SDL and catalog datatype do not match: {} != {}", gqlDatatype, iptoDatatype);
                progress.println("GraphQL SDL and catalog datatype do not match: " + gqlDatatype +  " != " + iptoDatatype);
            }
        }

        // Attributes
        for (String key : gqlViewpoint.attributes().keySet()) {
            if (!catalogViewpoint.attributes().containsKey(key)) {
                log.warn("Attribute '{}' not found in catalog", key);
                progress.println("Attribute '" + key + "' not found in catalog");

                CatalogAttribute attribute = addAttribute(repo, gqlViewpoint.attributes().get(key), progress);
                catalogViewpoint.attributes().put(key, attribute); // replace
                continue;
            }
            GqlAttributeShape gqlAttribute = gqlViewpoint.attributes().get(key);
            CatalogAttribute iptoAttribute = catalogViewpoint.attributes().get(key);
            if (!gqlAttribute.equals(iptoAttribute)) {
                log.error("GraphQL SDL and catalog attribute do not match: {} != {}", gqlAttribute, iptoAttribute);
                progress.println("GraphQL SDL and catalog attribute do not match: " + gqlAttribute +  " != " + iptoAttribute);
            }
        }

        // Records
        for (String key : gqlViewpoint.records().keySet()) {
            if (!catalogViewpoint.records().containsKey(key)) {
                log.warn("Record '{}' not found in catalog", key);
                progress.println("Record '" + key + "' not found in catalog");

                CatalogRecord record = addRecord(repo, gqlViewpoint.records().get(key), gqlViewpoint.attributes(), progress);
                catalogViewpoint.records().put(key, record); // replace
                continue;
            }
            GqlRecordShape gqlRecord = gqlViewpoint.records().get(key);
            CatalogRecord iptoRecord = catalogViewpoint.records().get(key);
            if (!gqlRecord.equals(iptoRecord)) {
                log.error("GraphQL SDL and catalog record do not match: {} != {}", gqlRecord, iptoRecord);
                progress.println("GraphQL SDL and catalog record do not match: " + gqlRecord +  " != " + iptoRecord);
            }
        }

        // Templates
        for (String key : gqlViewpoint.units.keySet()) {
            if (!catalogViewpoint.units().containsKey(key)) {
                log.warn("Unit template '{}' not found in catalog", key);
                progress.println("Unit template '" + key + "' not found in catalog");

                CatalogUnit template = addTemplate(repo, gqlViewpoint.units.get(key), gqlViewpoint.attributes(), progress);
                catalogViewpoint.units().put(key, template); // replace
                continue;
            }
            GqlUnitShape gqlTemplate = gqlViewpoint.units.get(key);
            CatalogUnit iptoTemplate = catalogViewpoint.units().get(key);

            if (!gqlTemplate.equals(iptoTemplate)) {
                log.error("GraphQL SDL and catalog template do not match: {} != {}", gqlTemplate, iptoTemplate);
                progress.println("GraphQL SDL and catalog template do not match: " + gqlTemplate +  " != " + iptoTemplate);
            }
        }
    }

    private static void wire(
            RuntimeWiring.Builder runtimeWiring,
            Repository repository,
            RuntimeService runtimeService,
            Map<String, SchemaOperation> operationTypes,
            GqlViewpoint gqlViewpoint,
            CatalogViewpoint catalogViewpoint
    ) {
        wireAttributes(runtimeWiring, repository, runtimeService, gqlViewpoint, catalogViewpoint);
        wireRecords(runtimeWiring, repository, runtimeService, gqlViewpoint, catalogViewpoint);
        wireUnits(runtimeWiring, repository, runtimeService, gqlViewpoint, catalogViewpoint);
        wireOperations(runtimeWiring, repository, runtimeService, operationTypes, gqlViewpoint, catalogViewpoint);
    }

    private static void wireAttributes(
            RuntimeWiring.Builder runtimeWiring,
            Repository repository,
            RuntimeService runtimeService,
            GqlViewpoint gqlViewpoint,
            CatalogViewpoint catalogViewpoint
    ) {
        Map<String, GqlAttributeShape> gqlAttributes = gqlViewpoint.attributes();
        Map<String, CatalogAttribute> iptoAttributes = catalogViewpoint.attributes();

        for (String key : gqlAttributes.keySet()) {
            GqlAttributeShape gqlAttribute = gqlAttributes.get(key);
            CatalogAttribute iptoAttribute = iptoAttributes.get(key);


        }
    }

    private static void wireRecords(
            RuntimeWiring.Builder runtimeWiring,
            Repository repository,
            RuntimeService runtimeService,
            GqlViewpoint gqlViewpoint,
            CatalogViewpoint catalogViewpoint
    ) {
        Map<String, GqlRecordShape> gqlRecords = gqlViewpoint.records();
        Map<String, CatalogRecord> iptoRecords = catalogViewpoint.records();

        for (String key : gqlRecords.keySet()) {
            GqlRecordShape gqlRecord = gqlRecords.get(key);
            CatalogRecord iptoRecord = iptoRecords.get(key);

            final String typeName = gqlRecord.typeName();

            Iterator<GqlFieldShape> fit = gqlRecord.fields().iterator();
            Iterator<CatalogAttribute> ait = iptoRecord.fields().iterator();

            runtimeWiring.type(typeName, builder -> {

                while (fit.hasNext() && ait.hasNext()) {
                    GqlFieldShape field = fit.next();
                    CatalogAttribute attribute = ait.next();

                    final String fieldName = field.fieldName();
                    final int fieldAttrId = attribute.attrId();
                    final boolean isArray = field.isArray();

                    // Tell GraphQL how to fetch this specific (record) type
                    DataFetcher<?> fetcher = env -> {
                        //**** Executed at runtime **********************************
                        // My mission in life is to resolve a specific attribute
                        // (the current 'fieldName') in a specific type (the current
                        // record). Everything needed at runtime is accessible
                        // right now so we capture it for later.
                        //***********************************************************
                        Box box = env.getSource();
                        if (null == box) {
                            log.warn("No box");
                            return null;
                        }

                        log.trace("Fetching attribute {} from record {}: {}.{}", isArray ? fieldName + "[]" : fieldName, typeName, box.getTenantId(), box.getUnitId());

                        // REPLACE return runtimeService.getRecord(box, childAttrid, _idx);
                        if (isArray) {
                            return runtimeService.getArray(box, fieldName);
                        } else {
                            return runtimeService.getScalar(box, fieldName);
                        }
                    };
                    builder.dataFetcher(fieldName, fetcher);
                    log.info("Wiring: {} > {}", typeName, fieldName);
                }

                return builder;
            });
        }
    }

    private static void wireUnits(
            RuntimeWiring.Builder runtimeWiring,
            Repository repository,
            RuntimeService runtimeService,
            GqlViewpoint gqlViewpoint,
            CatalogViewpoint catalogViewpoint
    ) {
        Map<String, GqlUnitShape> gqlTemplates = gqlViewpoint.units();
        Map<String, CatalogUnit> iptoTemplates = catalogViewpoint.units();

        for (String key : gqlTemplates.keySet()) {
            GqlUnitShape gqlTemplate = gqlTemplates.get(key);
            CatalogUnit iptoTemplate = iptoTemplates.get(key);

            final String typeName = gqlTemplate.typeName();

            Iterator<GqlFieldShape> fit = gqlTemplate.fields().iterator();
            Iterator<CatalogAttribute> ait = iptoTemplate.fields().iterator();

            runtimeWiring.type(typeName, builder -> {

                while (fit.hasNext() && ait.hasNext()) {
                    GqlFieldShape field = fit.next();
                    CatalogAttribute attribute = ait.next();

                    final String fieldName = field.fieldName();
                    final int fieldAttrId = attribute.attrId();
                    final boolean isArray = field.isArray();

                    // Tell GraphQL how to fetch this specific (template) type
                    DataFetcher<?> fetcher = env -> {
                        //**** Executed at runtime **********************************
                        // My mission in life is to resolve a specific attribute
                        // (the current 'fieldName') in a specific type (the current
                        // unit). Everything needed at runtime is accessible
                        // right now so we capture it for later.
                        //***********************************************************
                        Box box = env.getSource();
                        if (null == box) {
                            log.warn("No box");
                            return null;
                        }

                        log.trace("Fetching attribute {} ({}) from unit {}: {}.{}", isArray ? fieldName + "[]" : fieldName, fieldAttrId, typeName, box.getTenantId(), box.getUnitId());

                        if (isArray) {
                            return runtimeService.getArray(box, fieldName);
                        } else {
                            return runtimeService.getScalar(box, fieldName);
                        }
                    };
                    builder.dataFetcher(fieldName, fetcher);
                    log.info("Wiring: {} > {}", typeName, fieldName);
                }

                return builder;
            });

        }
    }

    private static void wireOperations(
            RuntimeWiring.Builder runtimeWiring,
            Repository repository,
            RuntimeService runtimeService,
            Map<String, SchemaOperation> operationTypes,
            GqlViewpoint gqlViewpoint,
            CatalogViewpoint catalogViewpoint
    ) {
        Map<String, GqlOperationShape> gqlOperations = gqlViewpoint.operations();

        for (String key : gqlOperations.keySet()) {
            GqlOperationShape gqlOperation = gqlOperations.get(key);
            log.info("*** wireing operation: {}", gqlOperation);

            final String type = gqlOperation.typeName();
            final String operationName = gqlOperation.operationName();
            final String parameterName = gqlOperation.parameterName();
            final String inputType = gqlOperation.inputTypeName();
            final String outputType = gqlOperation.outputTypeName();

            switch (inputType) {
                // "Hardcoded" point lookup for specific unit
                case "UnitIdentification" -> {
                    if ("Bytes".equals(outputType)) {
                        DataFetcher<?> rawUnitById = env -> {
                            //**** Executed at runtime **********************************
                            // My mission in life is to resolve a specific query
                            // (the current 'fieldName').
                            // Everything needed at runtime is accessible right
                            // now so it is captured for later.
                            //***********************************************************
                            if (log.isTraceEnabled()) {
                                log.trace("{}::{}({}) : {}", type, operationName, env.getArguments(), outputType);
                                log.trace("Datafetcher: parameterName='{}', inputType='{}', outputType='{}'", parameterName, inputType, outputType);
                            }

                            Query.UnitIdentification id = objectMapper.convertValue(env.getArgument(parameterName), Query.UnitIdentification.class);
                            return runtimeService.loadRawUnit(id.tenantId(), id.unitId());
                        };

                        runtimeWiring.type(type, t -> t.dataFetcher(operationName, rawUnitById));

                    } else {
                        DataFetcher<?> unitById = env -> {
                            //**** Executed at runtime **********************************
                            // My mission in life is to resolve a specific query
                            // (the current 'operationName').
                            // Everything needed at runtime is accessible right
                            // now so it is captured for later.
                            //***********************************************************
                            if (log.isTraceEnabled()) {
                                log.trace("{}::{}({}) : {}", type, operationName, env.getArguments(), outputType);
                                log.trace("Datafetcher: parameterName='{}', inputType='{}', outputType='{}'", parameterName, inputType, outputType);
                            }

                            Query.UnitIdentification id = objectMapper.convertValue(env.getArgument(parameterName), Query.UnitIdentification.class);
                            return runtimeService.loadUnit(id.tenantId(), id.unitId());
                        };

                        runtimeWiring.type(type, t -> t.dataFetcher(operationName, unitById));
                    }
                    log.info("Wiring: {}::{}(...) : {}", type, operationName, outputType);
                }

                //
                case "Filter" -> {
                    if ("Bytes".equals(outputType)) {
                        DataFetcher<?> rawUnitsByFilter = env -> {
                            //**** Executed at runtime **********************************
                            // My mission in life is to resolve a specific query
                            // (the current 'operationName').
                            // Everything needed at runtime is accessible right
                            // now so it is captured for later.
                            //***********************************************************

                            if (log.isTraceEnabled()) {
                                log.trace("{}::{}({}) : {}", type, operationName, env.getArguments(), outputType);
                                log.trace("Datafetcher: parameterName='{}', inputType='{}', outputType='{}'", parameterName, inputType, outputType);
                            }

                            Query.Filter filter = objectMapper.convertValue(env.getArgument(parameterName), Query.Filter.class);

                            return runtimeService.searchRaw(filter);
                        };

                        runtimeWiring.type(type, t -> t.dataFetcher(operationName, rawUnitsByFilter));

                    } else {
                        DataFetcher<?> unitsByFilter = env -> {
                            //**** Executed at runtime **********************************
                            // My mission in life is to resolve a specific query
                            // (the current 'operationName').
                            // Everything needed at runtime is accessible right
                            // now so it is captured for later.
                            //***********************************************************
                            if (log.isTraceEnabled()) {
                                log.trace("{}::{}({}) : {}", type, operationName, env.getArguments(), outputType);
                                log.trace("Datafetcher: parameterName='{}', inputType='{}', outputType='{}'", parameterName, inputType, outputType);
                            }

                            Query.Filter filter = objectMapper.convertValue(env.getArgument(parameterName), Query.Filter.class);

                            return runtimeService.search(filter);
                        };

                        runtimeWiring.type(type, t -> t.dataFetcher(operationName, unitsByFilter));
                    }
                    log.info("Wiring: {}::{}(...) : {}", type, operationName, outputType);
                }
            }
        }
    }

    private static CatalogAttribute addAttribute(Repository repo, GqlAttributeShape gqlAttribute, PrintStream progress) {

        CatalogAttribute attribute = new CatalogAttribute(
                gqlAttribute.attrId,
                gqlAttribute.alias,
                gqlAttribute.name,
                gqlAttribute.qualName,
                AttributeType.of(gqlAttribute.typeName),
                gqlAttribute.isArray
        );

        String sql = """
            INSERT INTO repo_attribute (attrid, attrtype, scalar, attrname, qualname, alias)
            VALUES (?,?,?,?,?,?)
            """;

        try {
            repo.withConnection(conn -> {
                try {
                    conn.setAutoCommit(false);

                    Database.usePreparedStatement(conn, sql, pStmt -> {
                        int i = 0;
                        pStmt.setInt(++i, attribute.attrId());
                        pStmt.setInt(++i, attribute.attrType().getType());
                        pStmt.setBoolean(++i, !attribute.isArray()); // Note negation
                        pStmt.setString(++i, attribute.attrName());
                        pStmt.setString(++i, attribute.qualifiedName());
                        pStmt.setString(++i, attribute.alias());

                        Database.execute(pStmt);
                    });

                    conn.commit();
                    log.info("Loaded attribute '{}' (attrid={}, name='{}', qual-name='{}')", attribute.alias(), attribute.attrId(), attribute.attrName(), attribute.qualifiedName());

                } catch (Throwable t) {
                    log.error("Failed to store attribute '{}' ({}, '{}'): {}", attribute.alias(), attribute.attrId(), gqlAttribute.name, t.getMessage(), t);
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
                            log.info("Attribute '{}' ({}, '{}') seems to already have been loaded", attribute.alias(), attribute.attrId(), attribute.attrName());
                        }
                    }
                }
            });
        } catch (SQLException sqle) {
            log.error("Failed to store attribute: {}", Database.squeeze(sqle));
        }

        return attribute;
    }

    private static CatalogRecord addRecord(Repository repo, GqlRecordShape gqlRecord, Map<String, GqlAttributeShape> gqlAttributes, PrintStream progress) {

        log.info("Adding record: {}", gqlRecord);

        String recordName = gqlRecord.typeName();
        String recordAttributeName = gqlRecord.attributeName();
        List<GqlFieldShape> fields = gqlRecord.fields();

        // Determine attrId of record attribute
        GqlAttributeShape recordAttribute = gqlAttributes.get(recordAttributeName);
        if (null == recordAttribute) {
            log.warn("No matching record attribute: {}", recordAttributeName);
            throw new RuntimeException("No matching record attribute: " + recordAttributeName);
        }

        int recordId = recordAttribute.attrId;
        CatalogRecord catalogRecord = new CatalogRecord(recordId, recordName);

        // repo_record_template (
        //    recordid  INT,  -- from @record(attribute: …)
        //    name      TEXT, -- type name
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
                        VALUES (?, ?)
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

                    Database.usePreparedStatement(conn, recordSql, pStmt -> {
                        try {
                            pStmt.setInt(1, catalogRecord.recordAttrId);
                            pStmt.setString(2, catalogRecord.recordName);

                            Database.execute(pStmt);

                        } catch (SQLException sqle) {
                            String sqlState = sqle.getSQLState();
                            conn.rollback();

                            if (sqlState.startsWith("23")) {
                                // 23505 : duplicate key value violates unique constraint "repo_record_template_pk"
                                log.info("Record '{}' seems to already have been loaded", recordName);
                            } else {
                                throw sqle;
                            }
                        }
                    });

                    Database.usePreparedStatement(conn, elementsSql, pStmt -> {
                        int idx = 0; // index into record
                        for (GqlFieldShape field : fields) {
                            // Determine attrId of field in record
                            GqlAttributeShape fieldAttribute = gqlAttributes.get(field.fieldName());
                            log.info("Adding record field: {}", fieldAttribute);

                            if (null == fieldAttribute) {
                                log.warn("No matching field attribute: '{}' (name='{}')", field.fieldName(), field.usedAttributeName());
                                continue;
                            }

                            CatalogAttribute attribute = new CatalogAttribute(
                                    fieldAttribute.attrId,
                                    field.fieldName(),
                                    fieldAttribute.name,
                                    fieldAttribute.qualName,
                                    AttributeType.of(fieldAttribute.typeName),
                                    field.isArray()
                            );

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
                    log.info("Loaded record '{}'", recordName);

                } catch (Throwable t) {
                    log.error("Failed to store record '{}': {}", recordName, t.getMessage(), t);
                    if (t.getCause() instanceof SQLException sqle) {
                        log.error("  ^--- {}", Database.squeeze(sqle));
                        String sqlState = sqle.getSQLState();

                        try {
                            conn.rollback();

                        } catch (SQLException rbe) {
                            log.error("Failed to rollback transaction: {}", Database.squeeze(rbe), rbe);
                        }

                        if (sqlState.startsWith("23")) {
                            // 23505 : duplicate key value violates unique constraint "repo_record_template_pk"
                            log.info("Record '{}' seems to already have been loaded", recordName);
                        }
                    }
                }
            });
        } catch (SQLException sqle) {
            log.error("Failed to store record: {}", Database.squeeze(sqle));
        }

        return catalogRecord;
    }

    private static CatalogUnit addTemplate(Repository repo, GqlUnitShape gqlTemplate, Map<String, GqlAttributeShape> gqlAttributes, PrintStream progress) {

        CatalogUnit template = new CatalogUnit(gqlTemplate.templateId(), gqlTemplate.typeName());

        String templateSql = """
                        INSERT INTO repo_unit_template (templateid, name)
                        VALUES (?, ?)
                        """;

        String elementsSql = """
                        INSERT INTO repo_unit_template_elements (templateid, attrid, idx, alias)
                        VALUES (?,?,?,?)
                        """;

        try {
            repo.withConnection(conn -> {
                try {
                    conn.setAutoCommit(false);

                    Database.usePreparedStatement(conn, templateSql, pStmt -> {
                        try {
                            pStmt.setInt(1, template.templateId);
                            pStmt.setString(2, template.templateName);

                            Database.execute(pStmt);

                        } catch (SQLException sqle) {
                            String sqlState = sqle.getSQLState();
                            conn.rollback();

                            if (sqlState.startsWith("23")) {
                                // 23505 : duplicate key value violates unique constraint "repo_unit_template_pk"
                                log.info("Unit template '{}' seems to already have been loaded", template.templateName);
                            } else {
                                throw sqle;
                            }
                        }
                    });

                    Database.usePreparedStatement(conn, elementsSql, pStmt -> {
                        int idx = 0; // index into record
                        for (GqlFieldShape field : gqlTemplate.fields()) {
                            // Determine attrId of field in record
                            GqlAttributeShape fieldAttribute = gqlAttributes.get(field.fieldName());
                            if (null == fieldAttribute) {
                                log.warn("No matching field attribute: '{}' ('{}')", field.fieldName(), field.usedAttributeName());
                                continue;
                            }
                            //int fieldAttrId = fieldAttribute.attrId;

                            CatalogAttribute attribute = new CatalogAttribute(
                                fieldAttribute.attrId,
                                field.fieldName(),
                                fieldAttribute.name,
                                fieldAttribute.qualName,
                                AttributeType.of(fieldAttribute.typeName),
                                fieldAttribute.isArray
                            );

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
                    log.info("Loaded unit template '{}'", template.templateName);

                } catch (Throwable t) {
                    log.error("Failed to store unit template '{}': {}", template.templateName, t.getMessage(), t);
                    if (t.getCause() instanceof SQLException sqle) {
                        log.error("  ^--- {}", Database.squeeze(sqle));
                        String sqlState = sqle.getSQLState();

                        try {
                            conn.rollback();

                        } catch (SQLException rbe) {
                            log.error("Failed to rollback transaction: {}", Database.squeeze(rbe), rbe);
                        }

                        if (sqlState.startsWith("23")) {
                            // 23505 : duplicate key value violates unique constraint
                            log.info("Unit template '{}' seems to already have been loaded", template.templateName);
                        }
                    }
                }
            });
        } catch (SQLException sqle) {
            log.error("Failed to store template: {}", Database.squeeze(sqle));
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

        out.println("--- Units ---");
        for (Map.Entry<String, GqlUnitShape> entry : gql.units().entrySet()) {
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

        out.println("--- Units ---");
        for (Map.Entry<String, CatalogUnit> entry : ipto.units().entrySet()) {
            out.println("  " + entry.getKey() + " -> " + entry.getValue());
        }
        out.println();
    }
}
