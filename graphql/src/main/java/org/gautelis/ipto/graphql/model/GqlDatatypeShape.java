package org.gautelis.ipto.graphql.model;

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
public class GqlDatatypeShape {
    public final String name;       // (a)
    public final int id;            // (b)

    public GqlDatatypeShape(String name, int id) {
        this.name = name;
        this.id = id;
    }

    public boolean equals(CatalogDatatype other) {
        return name.equals(other.name()) && id == other.type();
    }

    @Override
    public String toString() {
        String info = "GqlDatatypeShape{";
        info += "name='" + name + '\'';
        info += ", id=" + id;
        info += '}';
        return info;
    }
}
