package org.gautelis.repo.graphql;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.Map;

public class Snapshot {
    private static final Logger log = LoggerFactory.getLogger(Snapshot.class);

    // attrName in Schema -> vector
    final Map<Integer, ValueVector<?>> values;

    // attrName in Schema -> children (in idx order)
    final Map<Integer, AttributeVector> compoundAttributes;

    /* Package private */
    Snapshot(Map<Integer, ValueVector<?>> primitives, Map<Integer, AttributeVector> compounds) {
        log.trace("Creating Snapshot");
        this.values = primitives;
        this.compoundAttributes = compounds;
    }

    /* Used by generic DataFetchers */

    /** Null when the attribute is missing or vector is empty. */
    public String scalarString(int attrId) {
        log.trace("Snapshot::scalarString({})", attrId);
        return firstOrNull(values.get(attrId), String.class);
    }

    public Instant scalarTime(int attrId) {
        log.trace("Snapshot::scalarTime({})", attrId);
        return firstOrNull(values.get(attrId), Instant.class);
    }

    public Integer scalarInt(int attrId) {
        log.trace("Snapshot::scalarInt({})", attrId);
        return firstOrNull(values.get(attrId), Integer.class);
    }

    /*
    public Long scalarLong(int attrId) {
        log.trace("Snapshot::scalarLong({})", attrId);
        return firstOrNull(values.get(attrId), Long.class);
    }
    */

    public Double scalarDouble(int attrId) {
        log.trace("Snapshot::scalarDouble({})", attrId);
        return firstOrNull(values.get(attrId), Double.class);
    }

    public List<Double> doubleVector(int attrId) {
        log.trace("Snapshot::doubleVector({})", attrId);
        ValueVector<?> v = values.get(attrId);
        return v == null ? List.of() : v.stream(Double.class).toList();
    }

    /* Helpers */
    private static <T> T firstOrNull(ValueVector<?> v, Class<T> type) {
        return v == null || v.isEmpty() ? null : type.cast(v.getFirst());
    }

    /** Child snapshot inside a compound (or null). */
    public /* for now */ Object child(int compoundAttrId, int idx) {
        log.trace("Snapshot::child({}, {})", compoundAttrId, idx);
        AttributeVector kids = compoundAttributes.get(compoundAttrId);
        return kids != null && idx < kids.size() ? kids.get(idx) : null;
    }
}
