package org.gautelis.repo.graphql2.configuration;

import graphql.language.*;
import graphql.schema.idl.TypeDefinitionRegistry;
import org.gautelis.repo.exceptions.AttributeTypeException;
import org.gautelis.repo.graphql2.model.CatalogDatatype;
import org.gautelis.repo.graphql2.model.GqlDataTypeShape;
import org.gautelis.repo.model.AttributeType;
import org.gautelis.repo.model.Repository;
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
    static Map<String, GqlDataTypeShape> derive(TypeDefinitionRegistry registry) {
        Map<String, GqlDataTypeShape> datatypes = new HashMap<>();

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

                            if (/* VALID? */ id > 0) {
                                datatypes.put(name, new GqlDataTypeShape(name, id));
                            }
                        }
                    }
                }
            }
        }

        return datatypes;
    }

    static Map<String, CatalogDatatype> read(Repository _repository) {
        Map<String, CatalogDatatype> datatypes = new HashMap<>();
        AttributeType[] attributeTypes = AttributeType.values();
        for (AttributeType attributeType : attributeTypes) {
            datatypes.put(attributeType.name(),  new CatalogDatatype(attributeType.getType(), attributeType.name()));
        }
        return datatypes;
    }
}