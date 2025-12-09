package org.gautelis.ipto.graphql.model;

import graphql.language.ListType;
import graphql.language.NonNullType;
import graphql.language.Type;
import graphql.language.TypeName;

/*
 * type PurchaseOrder {
 *    orderId  : String    @use(attribute: orderId)
 *    shipment : Shipment! @use(attribute: shipment)
 * }
 *
 * Variations in field type definitions:
 *
 *    shipment : Shipment     @use(attribute: shipment)
 *    shipment : Shipment!    @use(attribute: shipment)
 *    shipment : [Shipment]!  @use(attribute: shipment)
 *                   ^
 *                   | (a)
 */
public record TypeDefinition(
        String typeName,
        boolean isArray,
        boolean isMandatory
) {
    public static TypeDefinition of(Type t) {
        boolean isArray = false;
        boolean isMandatory = false;
        String name = null;

        //
        if (t instanceof ListType listType) {
            isArray = true;
            if (listType.getType() instanceof NonNullType nonNullType) {
                isMandatory = true;
                name = ((TypeName) nonNullType.getType()).getName();
            } else {
                name = ((TypeName) listType.getType()).getName();
            }
        } else if (t instanceof NonNullType nonNullType) {
            isMandatory = true;
            if (nonNullType.getType() instanceof ListType listType) {
                isArray = true;
                name = ((TypeName) listType.getType()).getName();
            } else {
                name = ((TypeName) nonNullType.getType()).getName();
            }
        } else if (t instanceof TypeName typeName) {
            name = typeName.getName();
        }

        return new TypeDefinition(name, isArray, isMandatory);
    }

    @Override
    public String toString() {
        String info = "TypeDef{";
        info += "typeName='" + typeName + '\'';
        info += ", isArray=" + isArray;
        info += ", isMandatory=" + isMandatory;
        info += '}';
        return info;
    }
}
