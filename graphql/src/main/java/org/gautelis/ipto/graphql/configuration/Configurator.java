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
import org.gautelis.ipto.graphql.runtime.service.RuntimeService;
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
import org.gautelis.ipto.graphql.runtime.wiring.SubscriptionWiringPolicy;
import org.gautelis.ipto.graphql.model.*;
import org.gautelis.ipto.repo.model.AttributeType;
import org.gautelis.ipto.repo.model.KnownAttributes;
import org.gautelis.ipto.repo.model.Repository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;
import java.io.Reader;
import java.util.*;
import java.util.function.Consumer;

public class Configurator {
    private static final Logger log = LoggerFactory.getLogger(Configurator.class);
    private static final String DEFAULT_DESCRIPTION_LANG = "SE";

    public record GqlViewpoint(
            Map<String, GqlDatatypeShape> datatypes,
            Map<AttributeKey, GqlAttributeShape> attributes,
            Map<RecordKey, GqlRecordShape> records,
            Map<TemplateKey, GqlTemplateShape> templates,
            Map<String, GqlUnionShape> unions,
            Map<OperationKey, GqlOperationShape> operations
    ) {}

    public record CatalogViewpoint(
            Map<String, CatalogDatatype> datatypes,
            Map<AttributeKey, CatalogAttribute> attributes,
            Map<RecordKey, CatalogRecord> records,
            Map<TemplateKey, CatalogTemplate> templates
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

        // Determine operation roots (defaults + schema overrides)
        Map<String, SchemaOperation> operationTypes = SdlObjectShapes.operationTypeMap(registry);

        // Setup GraphQL SDL view of things
        GqlViewpoint gql = loadFromFile(registry, operationTypes);
        validateOperationBindings(gql, progress);

        // Setup Ipto view of things
        CatalogViewpoint ipto = loadFromCatalog(repo);

        // Reconcile differences, i.e. create stuff if needed
        reconcile(repo, gql, ipto, progress);

        RuntimeService runtimeService = new RuntimeService(repo, ipto);
        runtimeService.wire(runtimeWiring, gql, ipto);
        runtimeService.wireOperations(runtimeWiring, gql, SubscriptionWiringPolicy.resolve());

        // Wire custom operations and/or overrides
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
        return ViewpointLoader.loadFromFile(registry, operationTypes);
    }

    private static void validateOperationBindings(
            GqlViewpoint gql,
            PrintStream progress
    ) {
        ViewpointLoader.validateOperationBindings(gql, progress);
    }

    private static CatalogViewpoint loadFromCatalog(Repository repo) {
        return ViewpointLoader.loadFromCatalog(repo);
    }

    private static void reconcile(
            Repository repo,
            GqlViewpoint gqlViewpoint,
            CatalogViewpoint catalogViewpoint,
            PrintStream progress
    ) {
        CatalogReconciler.reconcile(repo, gqlViewpoint, catalogViewpoint, progress);
        if (log.isTraceEnabled()) {
            dump(gqlViewpoint, progress);
            dump(catalogViewpoint, progress);
        }
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
        for (Map.Entry<AttributeKey, GqlAttributeShape> entry : gql.attributes().entrySet()) {
            out.println("  " + entry.getKey() + " -> " + entry.getValue());
        }
        out.println();

        out.println("--- Records ---");
        for (Map.Entry<RecordKey, GqlRecordShape> entry : gql.records().entrySet()) {
            out.println("  " + entry.getKey() + " -> " + entry.getValue());
        }
        out.println();

        out.println("--- Templates ---");
        for (Map.Entry<TemplateKey, GqlTemplateShape> entry : gql.templates().entrySet()) {
            out.println("  " + entry.getKey() + " -> " + entry.getValue());
        }
        out.println();

        out.println("--- Unions ---");
        for (Map.Entry<String, GqlUnionShape> entry : gql.unions().entrySet()) {
            out.println("  " + entry.getKey() + " -> " + entry.getValue());
        }
        out.println();

        out.println("--- Operations ---");
        for (Map.Entry<OperationKey, GqlOperationShape> entry : gql.operations().entrySet()) {
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
        for (Map.Entry<AttributeKey, CatalogAttribute> entry : ipto.attributes().entrySet()) {
            out.println("  " + entry.getKey() + " -> " + entry.getValue());
        }
        out.println();

        out.println("--- Records ---");
        for (Map.Entry<RecordKey, CatalogRecord> entry : ipto.records().entrySet()) {
            out.println("  " + entry.getKey() + " -> " + entry.getValue());
        }
        out.println();

        out.println("--- Templates ---");
        for (Map.Entry<TemplateKey, CatalogTemplate> entry : ipto.templates().entrySet()) {
            out.println("  " + entry.getKey() + " -> " + entry.getValue());
        }
        out.println();
    }
}
