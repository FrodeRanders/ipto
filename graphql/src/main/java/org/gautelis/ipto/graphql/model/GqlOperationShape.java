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
package org.gautelis.ipto.graphql.model;

public record GqlOperationShape(
        String typeName,
        String operationName,
        SchemaOperation category,    // QUERY | MUTATION | SUBSCRIPTION
        ParameterDefinition[] parameters, // Parameters to operation (e.g. UnitIdentification, Filter, ...)
        String outputTypeName,     // Type of output from operation (e.g. Bytes, <domain specific type>, ...)
        String runtimeOperation    // Optional runtime binding hint from SDL @ipto(operation: "...")
) {
    @Override
    public String toString() {
        String info = "GqlOperationShape{";
        info += "type-name='" + typeName + '\'';
        info += ", operation-name='" + operationName + '\'';
        info += ", category='" + category.name() + '\'';
        info += ", parameters=[";
        for (ParameterDefinition def : parameters) {
            info += "'" + def.parameterName() + " " + def.parameterType() + "', ";
        }
        info += "], type-of-output='" + outputTypeName + '\'';
        info += ", runtime-operation='" + runtimeOperation + '\'';
        info += "]}";
        return info;
    }
}
