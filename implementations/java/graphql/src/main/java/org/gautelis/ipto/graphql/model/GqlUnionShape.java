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

import java.util.List;

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
 *        ^             ^              ^
 *        | (a)         | (b)          | (...)
 */
public record GqlUnionShape(
    String unionName,     // (a)
    List<UnionMember> members    // (b)
) {
    @Override
    public String toString() {
        String info = "GqlUnionShape{";
        info += "union-name='" + unionName + '\'';
        info += ", members=[";
        for (UnionMember member : members) {
            info += member.memberType() + ", ";
        }
        info += "]}";
        return info;
    }
}
