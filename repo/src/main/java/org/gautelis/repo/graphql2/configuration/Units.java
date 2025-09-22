package org.gautelis.repo.graphql2.configuration;

import graphql.language.*;
import graphql.schema.idl.TypeDefinitionRegistry;
import org.gautelis.repo.exceptions.ConfigurationException;
import org.gautelis.repo.graphql2.model.*;
import org.gautelis.repo.graphql2.model.external.ExternalAttributeDef;
import org.gautelis.repo.model.Repository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public final class Units {
    private static final Logger log = LoggerFactory.getLogger(Units.class);

    private Units() {}

    /*
     * type PurchaseOrder @unit(id: 42) {
     *    orderId  : String    @use(attribute: ORDER_ID)
     *    shipment : Shipment! @use(attribute: SHIPMENT)
     * }
     *
     * type PurchaseOrder @unit(id: 42) {
     *             ^                 ^
     *             | (a)             | (b)
     *
     *    orderId  : String    @use(attribute: ORDER_ID)
     *     ^           ^                         ^
     *     | (c)       | (d)                     | (e)
     */
    static Map<String, UnitDef> derive(TypeDefinitionRegistry registry, Map<String, ExternalAttributeDef> attributes) {
        Map<String, UnitDef> units = new HashMap<>();

        for (ObjectTypeDefinition type : registry.getTypes(ObjectTypeDefinition.class)) {
            // --- (a) ---
            final String unitName = type.getName();

            List<Directive> unitDirectivesOnType = type.getDirectives("unit");
            if (!unitDirectivesOnType.isEmpty()) {

                // --- (b) ---
                int templateId = -1; // INVALID
                for (Directive directive : unitDirectivesOnType) {
                    Argument arg = directive.getArgument("id");
                    if (null != arg) {
                        IntValue _id = (IntValue) arg.getValue();
                        templateId = _id.getValue().intValue();
                    }
                }

                if (/* VALID? */ templateId > 0) {
                     List<TypeFieldDef> unitFields = new ArrayList<>();

                    // Handle field definitions on types
                    for (FieldDefinition f : type.getFieldDefinitions()) {
                        // --- (c) ---
                        final String fieldName = f.getName();

                        // --- (d) ---
                        final TypeDef fieldType = TypeDef.of(f.getType());

                        // Handle @use directive on field definitions
                        List<Directive> useDirectives = f.getDirectives("use");
                        if (!useDirectives.isEmpty()) {
                            for (Directive useDirective : useDirectives) {
                                // @use "attribute" argument
                                Argument arg = useDirective.getArgument("attribute");
                                EnumValue value = (EnumValue) arg.getValue();

                                ExternalAttributeDef fieldAttributeDef = attributes.get(value.getName());
                                if (null != fieldAttributeDef) {
                                    // --- (e) ---
                                    String fieldAttributeName = fieldAttributeDef.attributeName;
                                    int fieldAttributeId = fieldAttributeDef.attributeId;

                                    if (fieldAttributeDef.isArray != fieldType.isArray()) {
                                        String info = "Definition of field " + fieldName + " in unit " + unitName + " is invalid: " + f.getType() + " differs with respect to array capability from definition of attribute " + fieldAttributeName;
                                        log.error(info);
                                        System.out.println(info);
                                        throw new ConfigurationException(info);
                                    }

                                    unitFields.add(new TypeFieldDef(fieldName, fieldType, fieldAttributeName, fieldAttributeId));
                                    break; // In the unlikely case there are several @use
                                }
                            }
                        }
                    }
                    units.put(unitName, new UnitDef(unitName, templateId, unitFields));
                }
            }
        }

        return units;
    }

    static Map<String, UnitDef> read(Repository repository) {
        Map<String, UnitDef> units = new HashMap<>();

        // TODO

        return units;
    }
}