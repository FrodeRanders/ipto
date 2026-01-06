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
import graphql.schema.idl.TypeDefinitionRegistry;
import org.gautelis.ipto.repo.exceptions.AttributeTypeException;
import org.gautelis.ipto.graphql.model.CatalogDatatype;
import org.gautelis.ipto.graphql.model.GqlDatatypeShape;
import org.gautelis.ipto.repo.model.AttributeType;
import org.gautelis.ipto.repo.model.Repository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


public final class Datatypes {
    private static final Logger log = LoggerFactory.getLogger(Datatypes.class);

    private Datatypes() {}

    /*
     * enum DataTypes @datatypeRegistry {
     *    STRING    @datatype(id: 1)
     *    TIME      @datatype(id: 2)
     *    ...
     *    RECORD    @datatype(id: 99)
     * }
     *
     *    STRING    @datatype(id: 1)
     *      ^                     ^
     *      | (a)                 | (b)
     */
    static Map<String, GqlDatatypeShape> derive(TypeDefinitionRegistry registry) {
        Map<String, GqlDatatypeShape> datatypes = new HashMap<>();

        // Locate enums having a "datatypeRegistry" directive
        for (EnumTypeDefinition enumeration : registry.getTypes(EnumTypeDefinition.class)) {
            List<Directive> enumDirectives = enumeration.getDirectives();
            for (Directive directive : enumDirectives) {
                if ("datatypeRegistry".equals(directive.getName())) {
                    for (EnumValueDefinition enumValueDefinition : enumeration.getEnumValueDefinitions()) {
                        // --- (a) ---
                        String name = enumValueDefinition.getName();

                        List<Directive> enumValueDirectives = enumValueDefinition.getDirectives();
                        for (Directive enumValueDirective : enumValueDirectives) {

                            // --- (b) ---
                            int id = -1; // INVALID
                            Argument arg = enumValueDirective.getArgument("id");
                            if (null != arg) {
                                IntValue _id = (IntValue) arg.getValue();
                                id = _id.getValue().intValue();

                                // Validation
                                try {
                                    var _officialType = AttributeType.of(_id.getValue().intValue());
                                } catch (AttributeTypeException ate) {
                                    log.error("\u21af Not an official data type: {} with numeric attrId {}", enumValueDefinition.getName(), _id.getValue().intValue(), ate);
                                    throw ate;
                                }
                            }

                            if (/* VALID? */ id > 0) {
                                datatypes.put(name, new GqlDatatypeShape(name, id));
                            }
                        }
                    }
                }
            }
        }

        return datatypes;
    }

    static Map<String, CatalogDatatype> read(Repository _repository) {
        Map<String, CatalogDatatype> datatypes = new HashMap<>();
        AttributeType[] attributeTypes = AttributeType.values();
        for (AttributeType attributeType : attributeTypes) {
            datatypes.put(attributeType.name(),  new CatalogDatatype(attributeType.getType(), attributeType.name()));
        }
        return datatypes;
    }
}