package org.gautelis.ipto.repo.model;

import org.gautelis.ipto.repo.exceptions.AssociationTypeException;

public enum AssociationType implements Type {
    UNKNOWN(0, /* is relational? */ false, /* allows multiples? */ false),
    PARENT_CHILD_RELATION(1, /* is relational? */ true, /* allows multiples? */ true),
    CASE_ASSOCIATION(2, /* is relational? */ false, /* allows multiples? */ false),
    REPLACEMENT_RELATION(3, /* is relational? */ true, /* allows multiples? */ false);

    private final int type;

    // Is this association an internal association (relation) or an external association to an external entity?
    private final boolean isRelational;

    // Does this association accept multiple associations/relations?
    private final boolean allowsMultiples;

    AssociationType(int type, boolean isRelational, boolean allowsMultiples) {
        this.type = type;
        this.isRelational = isRelational;
        this.allowsMultiples = allowsMultiples;
    }

    public static AssociationType of(int type) throws AssociationTypeException {
        for (AssociationType t : AssociationType.values()) {
            if (t.type == type) {
                return t;
            }
        }
        throw new AssociationTypeException("Unknown relation/association type: " + type);
    }

    public int getType() {
        return type;
    }

    public boolean isRelational() {
        return isRelational;
    }

    public boolean allowsMultiples() {
        return allowsMultiples;
    }
}
