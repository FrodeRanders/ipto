package org.gautelis.ipto.graphql.model;

import graphql.language.ListType;
import graphql.language.NonNullType;
import graphql.language.Type;
import graphql.language.TypeName;

/*
 * type Query {
 *     yrkan(id : UnitIdentification!) : Yrkan
 *     yrkandenRaw(filter: Filter!) : Bytes
 * }
 *
 * type Mutation {
 *     lagraUnitRaw(tenantId : Int!, data : Bytes!) : Dataleverans
 * }
 *
 *
 * lagraUnitRaw(tenantId : Int!,    data : Bytes!) : Dataleverans
 *                 ^        ^        ^        ^
 *                 | (a)    | (b)    | (a*)    | (b*)
 *
 */
public record ParameterDefinition(
        String parameterName,         // (a)
        TypeDefinition parameterType  // (b)
) {
    @Override
    public String toString() {
        String info = "ParameterDefinition{";
        info += "parameter-name='" + parameterName + '\'';
        info += ", parameter-type=" + parameterType.toString();
        info += '}';
        return info;
    }
}
