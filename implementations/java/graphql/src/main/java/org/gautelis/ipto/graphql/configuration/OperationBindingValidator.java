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
import org.gautelis.ipto.repo.exceptions.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;
import java.util.Collection;

final class OperationBindingValidator {
    private static final Logger log = LoggerFactory.getLogger(OperationBindingValidator.class);

    private OperationBindingValidator() {}

    static void validate(
            Collection<GqlOperationShape> operations,
            PrintStream progress
    ) {
        for (GqlOperationShape operation : operations) {
            if (operation.runtimeOperation() != null) {
                continue;
            }

            String opRef = operation.typeName() + "::" + operation.operationName();
            String message;

            if (isAmbiguousWithoutBinding(operation)) {
                message = "Operation '" + opRef + "' has ambiguous shape without @ipto(operation: ...); "
                        + "add explicit runtime binding to avoid inference surprises.";
                log.warn("↯ {}", message);
                if (progress != null) {
                    progress.println(message);
                }
                throw new ConfigurationException(message);
            } else {
                message = "Operation '" + opRef + "' has no @ipto(operation: ...); "
                        + "runtime inference will be used.";
                log.info("↯ {}", message);
                if (progress != null) {
                    progress.println(message);
                }
            }
        }
    }

    private static boolean isAmbiguousWithoutBinding(GqlOperationShape operation) {
        if (operation.parameters().size() != 1) {
            return false;
        }

        ParameterDefinition parameter = operation.parameters().getFirst();
        String parameterName = parameter.parameterName();
        String outputType = operation.outputTypeName();
        String parameterType = parameter.parameterType().typeName();

        if ("id".equals(parameterName)) {
            return true;
        }

        if ("filter".equals(parameterName) && "Bytes".equals(outputType)) {
            return true;
        }

        return !("filter".equals(parameterName)
                || ("data".equals(parameterName) && "Bytes".equals(parameterType)));
    }
}
