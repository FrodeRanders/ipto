package org.gautelis.repo.graphql.model;

public record CatalogDatatype(
        int    type,
        String name
) {
    @Override
    public String toString() {
        String info = "CatalogDatatype{";
        info += "type=" + type;
        info += ", name=" + name;
        info += "}";
        return info;
    }
}
