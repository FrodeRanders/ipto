package org.gautelis.repo.graphql.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class CatalogUnit {
    public final int templateId;
    public final String templateName;
    private final List<CatalogAttribute> fields = new ArrayList<>();

    public CatalogUnit(int templateId, String templateName) {
        this.templateId = templateId;
        this.templateName = templateName;
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
        info += "template-id=" + templateId;
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

