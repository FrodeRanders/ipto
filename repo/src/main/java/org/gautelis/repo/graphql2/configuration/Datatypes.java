package org.gautelis.repo.graphql2.configuration;

import graphql.language.*;
import graphql.schema.idl.TypeDefinitionRegistry;
import org.gautelis.repo.exceptions.AttributeTypeException;
import org.gautelis.repo.graphql2.model.DataTypeDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


public final class Datatypes {
    private static final Logger log = LoggerFactory.getLogger(Datatypes.class);

    private Datatypes() {}

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
    static Map<String, DataTypeDef> derive(TypeDefinitionRegistry registry) {
        Map<String, DataTypeDef> datatypes = new HashMap<>();

        // Locate enums having a "datatypeRegistry" directive
        for (EnumTypeDefinition enumeration : registry.getTypes(EnumTypeDefinition.class)) {
            List<Directive> enumDirectives = enumeration.getDirectives();
            for (Directive directive : enumDirectives) {
                if ("datatypeRegistry".equals(directive.getName())) {
                    for (EnumValueDefinition enumValueDefinition : enumeration.getEnumValueDefinitions()) {
                        // --- (a) ---
                        String name = enumValueDefinition.getName();

                        List<Directive> enumValueDirectives = enumValueDefinition.getDirectives();
                        for (Directive enumValueDirective : enumValueDirectives) {

                            // --- (b) ---
                            int id = -1; // INVALID
                            Argument arg = enumValueDirective.getArgument("id");
                            if (null != arg) {
                                IntValue _id = (IntValue) arg.getValue();
                                id = _id.getValue().intValue();

                                // Validation
                                try {
                                    var _officialType = org.gautelis.repo.model.AttributeType.of(_id.getValue().intValue());
                                } catch (AttributeTypeException ate) {
                                    log.error("Not an official data type: {} with numeric attrId {}", enumValueDefinition.getName(), _id.getValue().intValue(), ate);
                                    throw ate;
                                }
                            }

                            // --- (c) ---
                            String backingtype = null;
                            arg = enumValueDirective.getArgument("backingtype");
                            if (null != arg) {
                                // NOTE: 'RECORD' does not have a particular basictype,
                                // it being record of other attributes and all...
                                StringValue _type = (StringValue) arg.getValue();
                                backingtype = _type.getValue();
                            }

                            if (/* VALID? */ id > 0) {
                                datatypes.put(name, new DataTypeDef(name, id, backingtype));
                            }
                        }
                    }
                }
            }
        }

        return datatypes;
    }
}