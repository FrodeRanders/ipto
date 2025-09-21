package org.gautelis.repo.graphql2.model.external;

import org.gautelis.repo.graphql2.model.DataType;

/*
 * enum DataTypes @datatypeRegistry {
 *    STRING    @datatype(id: 1)
 *    TIME      @datatype(id: 2)
 *    ...
 *    RECORD    @datatype(id: 99)
 * }
 *
 *    STRING    @datatype(id: 1)
 *      ^                     ^
 *      | (a)                 | (b)
 */
public class ExternalDataType extends DataType {
    public ExternalDataType(String name, int id) {
        super(name, id);
    }

    @Override
    public String toString() {
        String info = "ExternalDataType{";
        info += "name='" + name + '\'';
        info += ", id=" + id;
        info += '}';
        return info;
    }

    /*
    public boolean compare(DataType other) {
        return name.equals(other.name);
    }
    */
}
