package org.gautelis.repo.graphql.runtime;

import java.util.*;

public class UnitSnapshot extends Snapshot {
    final int tenantId;
    final long unitId;

    /* package private */
    UnitSnapshot(int tenantId, long unitId, Map<Integer, ValueVector<?>> primitives, Map<Integer, AttributeVector> compounds) {
        super(primitives, compounds);
        this.tenantId = tenantId;
        this.unitId = unitId;
    }

    public int getTenantId() {
        return tenantId;
    }

    public long getUnitId() {
        return unitId;
    }
}
