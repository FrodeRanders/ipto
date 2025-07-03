package org.gautelis.repo.graphql;

import java.util.ArrayList;
import java.util.Collections;
import java.util.stream.Stream;

public class ValueVector<T> extends ArrayList<T>  {

    public ValueVector(ArrayList<T> values) {
        super(Collections.unmodifiableList(values));
    }

    public <C> Stream<C> stream(Class<C> as) {
        return this.stream().map(as::cast);
    }
}
