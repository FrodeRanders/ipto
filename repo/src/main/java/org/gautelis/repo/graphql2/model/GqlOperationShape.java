package org.gautelis.repo.graphql2.model;

public record GqlOperationShape(
        String fieldName,
        String category,    // QUERY | MUTATION | SUBSCRIPTION
        String typeName     // e.g. object type listing the op
) {}

