package org.gautelis.ipto.api;

public record SearchRequest(
        Integer tenantId,
        String where,
        Integer offset,
        Integer size
) {}
