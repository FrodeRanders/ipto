package org.gautelis.repo.graphql2.configuration;

import graphql.language.*;
import graphql.schema.idl.TypeDefinitionRegistry;
import org.gautelis.repo.exceptions.AttributeTypeException;
import org.gautelis.repo.exceptions.ConfigurationException;
import org.gautelis.repo.graphql2.model.*;
import org.gautelis.repo.graphql2.model.TypeDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.gautelis.repo.model.AttributeType.RECORD;


public final class SdlToIr {
    private static final Logger log = LoggerFactory.getLogger(SdlToIr.class);

    private SdlToIr() {}

    public static ConfigIR derive(TypeDefinitionRegistry registry) {
        var datatypes   = deriveDatatypes(registry);
        var attributes  = deriveAttributes(registry, datatypes);
        var records     = deriveRecords(registry, attributes);
        var units       = deriveUnits(registry, attributes);
        var operations  = deriveOperations(registry);

        // Merge into a single immutable IR
        return new ConfigIR(datatypes, attributes, records, units, operations);
    }

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
    static Map<String, DataTypeDef> deriveDatatypes(TypeDefinitionRegistry registry) {
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


    /*
     * enum Attributes @attributeRegistry {
     *     "The name given to the resource. It''s a human-readable identifier that provides a concise representation of the resource''s content."
     *     TITLE @attribute(id: 1, datatype: STRING, array: false, alias: "dc:title", uri: "http://purl.org/dc/elements/1.1/title", description: "Namnet som ges till resursen...")
     *     ...
     *     SHIPMENT_ID @attribute(id: 1004, datatype: STRING)
     *     SHIPMENT    @attribute(id: 1099, datatype: RECORD, array: false)
     * }
     *
     * TITLE @attribute(id: 1, datatype: STRING, array: false, alias: "dc:title", qualname: "http:...", description: "...")
     *   ^                  ^              ^              ^               ^                    ^                       ^
     *   | (a)              | (b)          | (c)          | (d)           | (e)                | (f)                   | (g)
     */
    static Map<String, AttributeDef> deriveAttributes(TypeDefinitionRegistry registry, Map<String, DataTypeDef> datatypes) {
        Map<String, AttributeDef> attributes = new HashMap<>();

        // Locate enums having a "attributeRegistry" directive
        for (EnumTypeDefinition enumeration : registry.getTypes(EnumTypeDefinition.class)) {
            List<Directive> enumDirectives = enumeration.getDirectives();
            for (Directive directive : enumDirectives) {
                if ("attributeRegistry".equals(directive.getName())) {
                    for (EnumValueDefinition enumValueDefinition : enumeration.getEnumValueDefinitions()) {
                        // --- (a) ---
                        String nameInSchema = enumValueDefinition.getName();

                        List<Directive> enumValueDirectives = enumValueDefinition.getDirectives();
                        for (Directive enumValueDirective : enumValueDirectives) {
                            // --- (b) ---
                            int attrId = -1; // INVALID
                            Argument arg = enumValueDirective.getArgument("id");
                            if (null != arg) {
                                IntValue _id = (IntValue) arg.getValue();
                                attrId = _id.getValue().intValue();
                            }

                            // --- (c*) type and not name, converted later ---
                            int attrType = -1; // INVALID
                            arg = enumValueDirective.getArgument("datatype");
                            if (null != arg) {
                                EnumValue datatype = (EnumValue) arg.getValue();
                                DataTypeDef dataTypeDef = datatypes.get(datatype.getName());
                                if (null != dataTypeDef) {
                                    attrType = dataTypeDef.id();
                                } else {
                                    log.error("Not a valid datatype: {}", datatype.getName());
                                }
                            } else {
                                // If no datatype is specified, we assume this is a record
                                attrType = RECORD.getType();
                            }

                            // --- (d) ---
                            boolean isArray = false;
                            arg = enumValueDirective.getArgument("array");
                            if (null != arg) {
                                // NOTE: 'array' is optional
                                BooleanValue vector = (BooleanValue) arg.getValue();
                                isArray = vector.isValue();
                            }

                            // --- (e) attribute name in ipto (i.e. an alias) ---
                            String nameInIpto;
                            arg = enumValueDirective.getArgument("alias");
                            if (null != arg) {
                                StringValue alias = (StringValue) arg.getValue();
                                nameInIpto = alias.getValue();
                            } else {
                                // field name will have to do
                                nameInIpto = nameInSchema;
                            }

                            // --- (f) ---
                            String qualName = null;
                            arg = enumValueDirective.getArgument("uri");
                            if (null != arg) {
                                StringValue uri = (StringValue) arg.getValue();
                                qualName = uri.getValue();
                            }

                            // --- (g) ---
                            String description = null;
                            arg = enumValueDirective.getArgument("description");
                            if (null != arg) {
                                StringValue vector = (StringValue) arg.getValue();
                                description = vector.getValue();
                            }

                            if (/* VALID? */ attrId > 0) {
                                for (Map.Entry<String, DataTypeDef> entry : datatypes.entrySet()) {
                                    if (entry.getValue().id() == attrType) {
                                        // --- (c) ---
                                        String attrTypeName = entry.getValue().name();

                                        //
                                        AttributeDef attributeDef = new AttributeDef(nameInSchema, attrId, attrTypeName, attrType, isArray, /* alias */ nameInIpto, qualName, description);
                                        attributes.put(nameInSchema, attributeDef);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        return attributes;
    }

    /*
     * type Shipment @record(attribute: SHIPMENT) {
     *    shipmentId  : String  @use(attribute: SHIPMENT_ID)
     *    deadline : DateTime   @use(attribute: DEADLINE)
     *    reading  : [Float]    @use(attribute: READING)
     * }
     *
     * type Shipment @record(attribute: SHIPMENT) {
     *        ^                            ^
     *        | (a)                        | (b)
     *
     * shipmentId  : String  @use(attribute: SHIPMENT_ID)
     *     ^           ^                         ^
     *     | (c)       | (d)                     | (e)
     */
    static Map<String, RecordDef> deriveRecords(TypeDefinitionRegistry registry, Map<String, AttributeDef> attributes) {
        Map<String, RecordDef>  records = new HashMap<>();

        for (ObjectTypeDefinition type : registry.getTypes(ObjectTypeDefinition.class)) {
            // --- (a) ---
            String recordName = type.getName();

            List<Directive> recordDirectivesOnType = type.getDirectives("record");
            if (!recordDirectivesOnType.isEmpty()) {

                // --- (b) ---
                String recordAttributeName = null;
                for (Directive directive : recordDirectivesOnType) {
                    Argument arg = directive.getArgument("attribute");
                    if (null != arg) {
                        EnumValue alias = (EnumValue) arg.getValue();
                        recordAttributeName = alias.getName();
                    }
                }

                if (null != recordAttributeName && !recordAttributeName.isEmpty()) {
                    AttributeDef recordAttributeDef = attributes.get(recordAttributeName);
                    if (null != recordAttributeDef) {
                        final int recordAttributeId = recordAttributeDef.attributeId();

                        List<TypeFieldDef> recordFields = new ArrayList<>();
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

                                    AttributeDef fieldAttributeDef = attributes.get(value.getName());
                                    if (null != fieldAttributeDef) {
                                        // --- (e) ---
                                        String fieldAttributeName = fieldAttributeDef.attributeName();
                                        int fieldAttributeId = fieldAttributeDef.attributeId();

                                        recordFields.add(new TypeFieldDef(fieldName, fieldType, fieldAttributeName, fieldAttributeId));
                                        break; // In the unlikely case there are several @use
                                    }
                                }
                            }
                        }
                        records.put(recordName, new RecordDef(recordName, recordAttributeName, recordAttributeId, recordFields));
                    }
                } else {
                    String info = "Not a valid record definition: " + recordName;
                    log.error(info);
                    System.out.println(info);
                    throw new ConfigurationException(info);
                }
            }
        }

        return records;
    }

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
    static Map<String, UnitDef> deriveUnits(TypeDefinitionRegistry registry, Map<String, AttributeDef> attributes) {
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

                                AttributeDef fieldAttributeDef = attributes.get(value.getName());
                                if (null != fieldAttributeDef) {
                                    // --- (e) ---
                                    String fieldAttributeName = fieldAttributeDef.attributeName();
                                    int fieldAttributeId = fieldAttributeDef.attributeId();

                                    if (fieldAttributeDef.isArray() != fieldType.isArray()) {
                                        String info = "Not a valid unit definition: " + unitName + ": " + fieldName + " type and attribute type differs regarding array capability";
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

    enum SchemaOperation {QUERY, MUTATION}

    static Map<String, OperationDef> deriveOperations(TypeDefinitionRegistry registry) {
        Map<String, SchemaOperation> operationTypes = new HashMap<>();

        Optional<SchemaDefinition> _schemaDefinition = registry.schemaDefinition();
        if (_schemaDefinition.isPresent()) {
            SchemaDefinition schemaDefinition = _schemaDefinition.get();

            List<OperationTypeDefinition> otds = schemaDefinition.getOperationTypeDefinitions();
            for (OperationTypeDefinition otd : otds) {
                switch (otd.getName()) {
                    case "query" -> {
                        operationTypes.put(otd.getTypeName().getName(), SchemaOperation.QUERY);
                    }
                    case "mutation" -> {
                        operationTypes.put(otd.getTypeName().getName(), SchemaOperation.MUTATION);
                    }
                }
            }
        }

        Map<String, OperationDef> operations = new HashMap<>();

        for (ObjectTypeDefinition type : registry.getTypes(ObjectTypeDefinition.class)) {
            List<Directive> directives = type.getDirectives();
            boolean isOperation = true;
            for (Directive directive : directives) {
                String name = directive.getName();

                // Kind of negative logic here, treat as operation if not 'unit' nor 'record'
                isOperation &= !"unit".equals(name);
                isOperation &= !"record".equals(name);
            }

            if (isOperation) {
                // Handle Query and Mutation
                SchemaOperation operation = operationTypes.get(type.getName());
                if (operation != null) {
                    switch (operation) {
                        case QUERY -> deriveQueryOperations(type, operations);
                        case MUTATION -> deriveMutationOperations(type, operations);
                    };
                }
            }
        }

        return operations;
    }

    static void deriveQueryOperations(ObjectTypeDefinition type, Map<String, OperationDef> operations) {
        for (FieldDefinition f : type.getFieldDefinitions()) {
            // Operation name
            final String fieldName = f.getName();
            final TypeDef resultType = TypeDef.of(f.getType());

            List<InputValueDefinition> inputs = f.getInputValueDefinitions();

            if (!inputs.isEmpty()) {
                InputValueDefinition input = inputs.getFirst();
                String inputName = input.getName();
                TypeDef typeDef = TypeDef.of(input.getType());
            }

            operations.put(/* operation name */ fieldName, new OperationDef(fieldName, null, null)); // TODO
        }
   }

    static void deriveMutationOperations(ObjectTypeDefinition type, Map<String, OperationDef> operations) {
        for (FieldDefinition f : type.getFieldDefinitions()) {
            // Operation name
            final String fieldName = f.getName();
            final TypeDef resultType = TypeDef.of(f.getType());

            List<InputValueDefinition> inputs = f.getInputValueDefinitions();

            if (!inputs.isEmpty()) {
                InputValueDefinition input = inputs.getFirst();
                String inputName = input.getName();
                TypeDef typeDef = TypeDef.of(input.getType());
            }

            operations.put(/* operation name */ fieldName, new OperationDef(fieldName, null, null)); // TODO
        }
    }
}