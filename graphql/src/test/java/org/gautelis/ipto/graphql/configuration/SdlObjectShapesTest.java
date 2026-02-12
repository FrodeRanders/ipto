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

import graphql.schema.idl.SchemaParser;
import graphql.schema.idl.TypeDefinitionRegistry;
import org.gautelis.ipto.graphql.model.SchemaOperation;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SdlObjectShapesTest {
    @Test
    void operationTypeMapSupportsDefaultAndSchemaOverrides() {
        String sdl = """
                schema {
                  query: RootQuery
                  mutation: RootMutation
                  subscription: RootSubscription
                }
                                
                type RootQuery {
                  ping: String
                }
                                
                type RootMutation {
                  mutate: String
                }
                                
                type RootSubscription {
                  events: String
                }
                                
                type DomainType {
                  id: String
                }
                """;

        TypeDefinitionRegistry registry = new SchemaParser().parse(sdl);
        Map<String, SchemaOperation> operationTypes = SdlObjectShapes.operationTypeMap(registry);

        assertEquals(SchemaOperation.QUERY, operationTypes.get("RootQuery"));
        assertEquals(SchemaOperation.MUTATION, operationTypes.get("RootMutation"));
        assertEquals(SchemaOperation.SUBSCRIPTION, operationTypes.get("RootSubscription"));

        // Default names are also available when not used by schema overrides.
        assertEquals(SchemaOperation.QUERY, operationTypes.get("Query"));
        assertEquals(SchemaOperation.MUTATION, operationTypes.get("Mutation"));
        assertEquals(SchemaOperation.SUBSCRIPTION, operationTypes.get("Subscription"));
    }

    @Test
    void domainObjectTypesExcludesOperationRoots() {
        String sdl = """
                schema {
                  query: RootQuery
                }
                                
                type RootQuery {
                  ping: String
                }
                                
                type Order {
                  id: String
                }
                """;

        TypeDefinitionRegistry registry = new SchemaParser().parse(sdl);
        List<String> typeNames = SdlObjectShapes.domainObjectTypes(registry)
                .stream()
                .map(type -> type.getName())
                .toList();

        assertFalse(typeNames.contains("RootQuery"));
        assertTrue(typeNames.contains("Order"));
    }
}
