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

import graphql.schema.idl.TypeDefinitionRegistry;
import org.gautelis.ipto.graphql.model.*;
import org.gautelis.ipto.repo.model.Repository;

import java.io.PrintStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

final class ViewpointLoader {
    private ViewpointLoader() {}

    static Configurator.GqlViewpoint loadFromFile(
            TypeDefinitionRegistry registry,
            Map<String, SchemaOperation> operationTypes
    ) {
        Map<String, GqlDatatypeShape> datatypes = Datatypes.derive(registry);
        Map<String, GqlAttributeShape> rawAttributes = Attributes.derive(registry, datatypes);
        Map<String, GqlRecordShape> rawRecords = Records.derive(registry, rawAttributes);
        Map<String, GqlTemplateShape> rawTemplates = Templates.derive(registry, rawAttributes);
        Map<String, GqlUnionShape> unions = Unions.derive(registry);
        Map<OperationKey, GqlOperationShape> operations  = Operations.derive(registry, operationTypes);

        Map<AttributeKey, GqlAttributeShape> attributes = indexGqlAttributes(rawAttributes.values());
        Map<RecordKey, GqlRecordShape> records = indexGqlRecords(rawRecords.values());
        Map<TemplateKey, GqlTemplateShape> templates = indexGqlTemplates(rawTemplates.values());

        return new Configurator.GqlViewpoint(datatypes, attributes, records, templates, unions, operations);
    }

    static Configurator.CatalogViewpoint loadFromCatalog(Repository repo) {
        Map<String, CatalogDatatype> datatypes = Datatypes.read(repo);
        Map<String, CatalogAttribute> rawAttributes = Attributes.read(repo);
        Map<String, CatalogRecord> rawRecords = Records.read(repo);
        Map<String, CatalogTemplate> rawTemplates = Templates.read(repo);

        Map<AttributeKey, CatalogAttribute> attributes = indexCatalogAttributes(rawAttributes.values());
        Map<RecordKey, CatalogRecord> records = indexCatalogRecords(rawRecords.values());
        Map<TemplateKey, CatalogTemplate> templates = indexCatalogTemplates(rawTemplates.values());

        return new Configurator.CatalogViewpoint(datatypes, attributes, records, templates);
    }

    static void validateOperationBindings(
            Configurator.GqlViewpoint gql,
            PrintStream progress
    ) {
        OperationBindingValidator.validate(gql.operations().values(), progress);
    }

    private static Map<AttributeKey, GqlAttributeShape> indexGqlAttributes(Collection<GqlAttributeShape> attributes) {
        Map<AttributeKey, GqlAttributeShape> indexed = new HashMap<>();
        for (GqlAttributeShape attribute : attributes) {
            indexed.put(attribute.key(), attribute);
        }
        return indexed;
    }

    private static Map<AttributeKey, CatalogAttribute> indexCatalogAttributes(Collection<CatalogAttribute> attributes) {
        Map<AttributeKey, CatalogAttribute> indexed = new HashMap<>();
        for (CatalogAttribute attribute : attributes) {
            indexed.put(attribute.key(), attribute);
        }
        return indexed;
    }

    private static Map<RecordKey, GqlRecordShape> indexGqlRecords(Collection<GqlRecordShape> records) {
        Map<RecordKey, GqlRecordShape> indexed = new HashMap<>();
        for (GqlRecordShape record : records) {
            indexed.put(record.key(), record);
        }
        return indexed;
    }

    private static Map<RecordKey, CatalogRecord> indexCatalogRecords(Collection<CatalogRecord> records) {
        Map<RecordKey, CatalogRecord> indexed = new HashMap<>();
        for (CatalogRecord record : records) {
            indexed.put(record.key(), record);
        }
        return indexed;
    }

    private static Map<TemplateKey, GqlTemplateShape> indexGqlTemplates(Collection<GqlTemplateShape> templates) {
        Map<TemplateKey, GqlTemplateShape> indexed = new HashMap<>();
        for (GqlTemplateShape template : templates) {
            indexed.put(template.key(), template);
        }
        return indexed;
    }

    private static Map<TemplateKey, CatalogTemplate> indexCatalogTemplates(Collection<CatalogTemplate> templates) {
        Map<TemplateKey, CatalogTemplate> indexed = new HashMap<>();
        for (CatalogTemplate template : templates) {
            indexed.put(template.key(), template);
        }
        return indexed;
    }
}
