package org.gautelis.repo.graphql2.configuration;

import graphql.language.*;
import graphql.schema.idl.TypeDefinitionRegistry;
import org.gautelis.repo.exceptions.ConfigurationException;
import org.gautelis.repo.graphql2.model.AttributeDef;
import org.gautelis.repo.graphql2.model.RecordDef;
import org.gautelis.repo.graphql2.model.TypeDef;
import org.gautelis.repo.graphql2.model.TypeFieldDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public final class Records {
    private static final Logger log = LoggerFactory.getLogger(Records.class);

    private Records() {}

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
    static Map<String, RecordDef> derive(TypeDefinitionRegistry registry, Map<String, AttributeDef> attributes) {
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

                                        if (fieldAttributeDef.isArray() != fieldType.isArray()) {
                                            String info = "Definition of field " + fieldName + " in record " + recordName + " is invalid: " + f.getType() + " differs with respect to array capability from definition of attribute " + fieldAttributeName;
                                            log.error(info);
                                            System.out.println(info);
                                            throw new ConfigurationException(info);
                                        }

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
}