package org.gautelis.repo.model.locks;

import org.gautelis.repo.exceptions.LockTypeException;

public enum LockType {
    READ(10),
    EXISTENCE(20),
    WRITE(30);

    private final int type;

    LockType(int type) {
        this.type = type;
    }

    static LockType of(int type) throws LockTypeException {
        for (LockType t : LockType.values()) {
            if (t.type == type) {
                return t;
            }
        }
        throw new LockTypeException("Unknown lock type: " + type);
    }

    public int getType() {
        return type;
    }
}
