package org.gautelis.ipto.api;

/**
 * REST request payload for search endpoints in the admin application.
 *
 * @param tenantId tenant scope for the search
 * @param where textual search predicate
 * @param offset row offset for paging
 * @param size page size
 * @param orderBy optional sort field
 * @param orderDirection optional sort direction
 */
public record SearchRequest(
        Integer tenantId,
        String where,
        Integer offset,
        Integer size,
        String orderBy,
        String orderDirection
) {}
