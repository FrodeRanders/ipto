package org.gautelis.repo.graphql2.model;

import org.jetbrains.annotations.NotNull;

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
        String name,       /* (a) */
        int id,            /* (b) */
        String backingtype /* (c) */
) {
    @NotNull
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
}
