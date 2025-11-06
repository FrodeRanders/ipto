package org.gautelis.repo.graphql2.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class CatalogTemplate {
    public final String templateName;
    public final int templateUnitId;
    private final List<CatalogAttribute> fields = new ArrayList<>();

    public CatalogTemplate(String templateName, int templateUnitId) {
        this.templateName = templateName;
        this.templateUnitId = templateUnitId;
    }

    public void addField(CatalogAttribute field) {
        fields.add(field);
    }

    public List<CatalogAttribute> fields() {
        return Collections.unmodifiableList(fields);
    }

    @Override
    public String toString() {
        String info = "CatalogTemplate{";
        info += "template-id=" + templateUnitId;
        info += ", template-name='" + templateName + '\'';
        info += ", fields=[";
        for (CatalogAttribute field : fields) {
            info += field.toString();
            info += ", ";
        }
        info += "]}";
        return info;
    }
}

