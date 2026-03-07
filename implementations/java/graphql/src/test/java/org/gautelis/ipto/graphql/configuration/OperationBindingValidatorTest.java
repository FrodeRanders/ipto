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

import org.gautelis.ipto.graphql.model.GqlOperationShape;
import org.gautelis.ipto.graphql.model.ParameterDefinition;
import org.gautelis.ipto.graphql.model.RuntimeOperation;
import org.gautelis.ipto.graphql.model.SchemaOperation;
import org.gautelis.ipto.graphql.model.TypeDefinition;
import org.gautelis.ipto.repo.exceptions.ConfigurationException;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class OperationBindingValidatorTest {
    @Test
    void rejectsAmbiguousOperationWithoutBinding() {
        GqlOperationShape operation = new GqlOperationShape(
                "Query",
                "unitRaw",
                SchemaOperation.QUERY,
                List.of(new ParameterDefinition("id", new TypeDefinition("UnitIdentification", false, true))),
                "Bytes",
                null
        );

        assertThrows(
                ConfigurationException.class,
                () -> OperationBindingValidator.validate(List.of(operation), null)
        );
    }

    @Test
    void acceptsUnambiguousOperationWithoutBinding() {
        GqlOperationShape operation = new GqlOperationShape(
                "Query",
                "searchRawPayload",
                SchemaOperation.QUERY,
                List.of(new ParameterDefinition("filter", new TypeDefinition("Filter", false, true))),
                "SearchResult",
                null
        );

        assertDoesNotThrow(() -> OperationBindingValidator.validate(List.of(operation), null));
    }

    @Test
    void acceptsAmbiguousOperationWithExplicitBinding() {
        GqlOperationShape operation = new GqlOperationShape(
                "Query",
                "unitRaw",
                SchemaOperation.QUERY,
                List.of(new ParameterDefinition("id", new TypeDefinition("UnitIdentification", false, true))),
                "Bytes",
                RuntimeOperation.LOAD_UNIT_RAW
        );

        assertDoesNotThrow(() -> OperationBindingValidator.validate(List.of(operation), null));
    }
}
