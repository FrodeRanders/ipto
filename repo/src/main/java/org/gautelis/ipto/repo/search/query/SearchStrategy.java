package org.gautelis.ipto.repo.search.query;

// Currently supports two strategies of SQL production:
//
//  SET_OPS:    WITH c1.. final AS (INTERSECT/UNION)
//  EXISTS:     kernel-driven WHERE ... AND EXISTS(...) AND EXISTS(...)
//
public enum SearchStrategy {
    SET_OPS,
    EXISTS
}

