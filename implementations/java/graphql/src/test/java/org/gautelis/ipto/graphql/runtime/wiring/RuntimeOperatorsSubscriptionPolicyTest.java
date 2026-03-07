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
package org.gautelis.ipto.graphql.runtime.wiring;

import graphql.schema.idl.RuntimeWiring;
import org.gautelis.ipto.graphql.configuration.Configurator;
import org.gautelis.ipto.graphql.model.GqlOperationShape;
import org.gautelis.ipto.graphql.model.OperationKey;
import org.gautelis.ipto.graphql.model.SchemaOperation;
import org.gautelis.ipto.graphql.runtime.service.RuntimeService;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RuntimeOperatorsSubscriptionPolicyTest {
    @Test
    void failFastPolicyThrowsWhenSubscriptionOperationExists() {
        Configurator.GqlViewpoint gql = viewpointWithSubscriptionOperation();
        RuntimeService runtimeService = runtimeServiceForWiring();

        assertThrows(
                IllegalStateException.class,
                () -> RuntimeOperators.wireOperations(
                        RuntimeWiring.newRuntimeWiring(),
                        runtimeService,
                        gql,
                        SubscriptionWiringPolicy.FAIL_FAST
                )
        );
    }

    @Test
    void scaffoldPolicyAllowsWiringWhenSubscriptionOperationExists() {
        Configurator.GqlViewpoint gql = viewpointWithSubscriptionOperation();
        RuntimeService runtimeService = runtimeServiceForWiring();

        assertDoesNotThrow(
                () -> RuntimeOperators.wireOperations(
                        RuntimeWiring.newRuntimeWiring(),
                        runtimeService,
                        gql,
                        SubscriptionWiringPolicy.SCAFFOLD
                )
        );
    }

    private static RuntimeService runtimeServiceForWiring() {
        return new RuntimeService(
                null,
                new Configurator.CatalogViewpoint(Map.of(), Map.of(), Map.of(), Map.of())
        );
    }

    private static Configurator.GqlViewpoint viewpointWithSubscriptionOperation() {
        GqlOperationShape subscription = new GqlOperationShape(
                "Subscription",
                "events",
                SchemaOperation.SUBSCRIPTION,
                List.of(),
                "String",
                null
        );
        return new Configurator.GqlViewpoint(
                Map.of(),
                Map.of(),
                Map.of(),
                Map.of(),
                Map.of(),
                Map.of(new OperationKey("Subscription", "events"), subscription)
        );
    }
}
