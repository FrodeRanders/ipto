package org.gautelis.repo.graphql2.model;

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
public abstract class DataType {
    public final String name;       /* (a) GraphQL and Ipto shared */
    public final int id;            /* (b) Ipto specific */

    public DataType(String name, int id) {
        this.name = name;
        this.id = id;
    }
}
