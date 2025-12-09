package org.gautelis.ipto.graphql.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class CatalogUnitTemplate {
    public int templateId;
    public final String templateName;
    private final List<CatalogAttribute> fields = new ArrayList<>();

    public CatalogUnitTemplate(int templateId, String templateName) {
        this.templateId = templateId;
        this.templateName = templateName;
    }

    public CatalogUnitTemplate(String templateName) {
        this(-1, templateName);
    }

    public void setTemplateId(int templateId) {
        this.templateId = templateId;
    }

    public void addField(CatalogAttribute field) {
        fields.add(field);
    }

    public List<CatalogAttribute> fields() {
        return Collections.unmodifiableList(fields);
    }

    @Override
    public String toString() {
        String info = "CatalogUnitTemplate{";
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

