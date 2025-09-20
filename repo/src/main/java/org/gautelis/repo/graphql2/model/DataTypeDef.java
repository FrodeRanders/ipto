package org.gautelis.repo.graphql2.model;

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
public record DataTypeDef(
        String name,       /* (a) GraphQL and Ipto shared */
        int id,            /* (b) Ipto specific */
        String backingtype /* (c) */
) {
    @Override
    public String toString() {
        String info = "DataTypeDef{";
        info += "name='" + name + '\'';
        info += ", id=" + id;
        if (null != backingtype) {
            info += ", backingtype='" + backingtype + '\'';
        }
        info += '}';
        return info;
    }

    public boolean compare(DataTypeDef other) {
        return name.equals(other.name) && id == other.id;
    }
}
