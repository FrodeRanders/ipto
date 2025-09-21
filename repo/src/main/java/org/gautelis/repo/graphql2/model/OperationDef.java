package org.gautelis.repo.graphql2.model;

public record OperationDef(
        String graphQlName,
        String category,    // QUERY | MUTATION | SUBSCRIPTION
        String typeName     // e.g. object type implementing the op
) {}

