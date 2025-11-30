package org.gautelis.repo.graphql.model;

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
