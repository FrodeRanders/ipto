package org.gautelis.repo.graphql.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class CatalogRecord {
    public int recordAttrId;
    public final String recordName;
    private final List<CatalogAttribute> fields = new ArrayList<>();

    public CatalogRecord(int recordAttrid, String recordName) {
        this.recordAttrId = recordAttrid;
        this.recordName = recordName;
    }

    public CatalogRecord(String recordName) {
        this(-1, recordName);
    }

    public void setRecordId(int recordId) {
        this.recordAttrId = recordId;
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

