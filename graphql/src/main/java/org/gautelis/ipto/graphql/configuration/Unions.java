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
import org.gautelis.ipto.graphql.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;


public final class Unions {
    private static final Logger log = LoggerFactory.getLogger(Unions.class);

    private Unions() {
    }

    /*
     * type FysiskPerson @record(attribute: fysisk_person) {
     *     personnummer : String
     * }
     *
     * type JuridiskPerson @record(attribute: juridisk_person) {
     *     orgnummer : String
     * }
     *
     * union Person = FysiskPerson | JuridiskPerson
     *        ^             ^                ^
     *        | (a)         | (b)            | (...)
     */
    static Map<String, GqlUnionShape> derive(
            TypeDefinitionRegistry registry
    ) {
        Objects.requireNonNull(registry, "registry");

        Map<String, GqlUnionShape> unions = new HashMap<>();


        for (UnionTypeDefinition union : registry.getTypes(UnionTypeDefinition.class)) {
            // --- (a) ---
            String unionName = union.getName();

            List<UnionMember> members = new ArrayList<>();

            List<Type> memberTypes = union.getMemberTypes();
            for (Type memberType : memberTypes) {
                if (log.isTraceEnabled()) {
                    if (memberType instanceof TypeName memberTypeName) {
                        log.trace("\u21af Inspecting union : {} > {}", unionName, memberTypeName.getName());
                    } else {
                        log.trace("\u21af Inspecting union : {} > {}", unionName, memberType);
                    }
                }

                if (memberType instanceof TypeName unionTypeName) {
                    String memberName = unionTypeName.getName();
                    members.add(new UnionMember(unionName, memberName));
                } else {
                    log.warn("\u21af Ignoring member '{}' in union '{}'", memberType, unionName);
                }
            }

            unions.put(unionName, new GqlUnionShape(unionName, members));
            log.trace("\u21af Defining shape for {}: {}", unionName, unions.get(unionName));
        }

        return unions;
    }
}