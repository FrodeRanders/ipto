package org.gautelis.repo.graphql.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class CatalogRecord {
    public final int recordAttrId;
    public final String recordName;
    private final List<CatalogAttribute> fields = new ArrayList<>();

    public CatalogRecord(int recordAttrid, String recordName) {
        this.recordAttrId = recordAttrid;
        this.recordName = recordName;
    }

    public void addField(CatalogAttribute field) {
        fields.add(field);
    }

    public List<CatalogAttribute> fields() {
        return Collections.unmodifiableList(fields);
    }

    @Override
    public String toString() {
        String info = "CatalogRecord{";
        info += "record-id=" + recordAttrId;
        info += ", record-name='" + recordName + '\'';
        info += ", fields=[";
        for (CatalogAttribute field : fields) {
            info += field.toString();
            info += ", ";
        }
        info += "]}";
        return info;
    }
}

