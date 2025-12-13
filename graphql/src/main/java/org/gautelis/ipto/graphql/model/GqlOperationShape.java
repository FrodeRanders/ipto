package org.gautelis.ipto.graphql.model;

public record GqlOperationShape(
        String typeName,
        String operationName,
        SchemaOperation category,    // QUERY | MUTATION | SUBSCRIPTION
        String parameterName,
        String inputTypeName,     // Type of input to operation (e.g. UnitIdentification, Filter, ...)
        String outputTypeName     // Type of output from operation (e.g. Bytes, <domain specific type>, ...)
) {
    @Override
    public String toString() {
        String info = "GqlOperationShape{";
        info += "type-name='" + typeName + '\'';
        info += ", operation-name='" + operationName + '\'';
        info += ", category='" + category.name() + '\'';
        info += ", parameter-name='" + parameterName + '\'';
        info += ", type-of-input='" + inputTypeName + '\'';
        info += ", type-of-output='" + outputTypeName + '\'';
        info += "]}";
        return info;
    }
}

