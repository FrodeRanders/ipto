package org.gautelis.repo.graphql2.model.internal;

import org.gautelis.repo.graphql2.model.DataTypeDef;

/*
 * enum DataTypes @datatypeRegistry {
 *    STRING    @datatype(id: 1,  backingtype: "text")
 *    TIME      @datatype(id: 2,  backingtype: "timestamptz")
 *    ...
 *    RECORD    @datatype(id: 99)
 * }
 *
 *    STRING    @datatype(id: 1,  backingtype: "text")
 *      ^                     ^                  ^
 *      | (a)                 | (b)              | (c)
 */
public class InternalDataTypeDef extends DataTypeDef {
    public InternalDataTypeDef(String name, int id) {
        super(name, id);
    }

    @Override
    public String toString() {
        String info = "InternalDataType{";
        info += "name='" + name + '\'';
        info += ", id=" + id;
        info += '}';
        return info;
    }

    /*
    public boolean compare(DataType other) {
        return name.equals(other.name) && id == other.id;
    }
    */
}
