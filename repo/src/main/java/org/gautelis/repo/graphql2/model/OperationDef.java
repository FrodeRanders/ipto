package org.gautelis.repo.graphql2.model;

public record OperationDef(
        String fieldName,
        String category,    // QUERY | MUTATION | SUBSCRIPTION
        String typeName     // e.g. object type listing the op
) {}

