package org.gautelis.ipto.graphql.runtime;

import graphql.schema.DataFetcher;
import graphql.schema.TypeResolver;
import graphql.schema.idl.RuntimeWiring;
import graphql.schema.idl.errors.StrictModeWiringException;
import org.gautelis.ipto.graphql.configuration.Configurator;
import org.gautelis.ipto.graphql.model.*;
import org.gautelis.ipto.repo.model.AttributeType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.databind.ObjectMapper;

import java.util.*;

public class RuntimeOperators {
    private static final Logger log = LoggerFactory.getLogger(RuntimeOperators.class);

    private static final ObjectMapper objectMapper = new ObjectMapper();

    /* package accessible only */
    static void wireRecords(
            RuntimeWiring.Builder runtimeWiring,
            RuntimeService runtimeService,
            Configurator.GqlViewpoint gqlViewpoint
    ) {
        Map<String, GqlAttributeShape> attributes = gqlViewpoint.attributes();
        Map<String, GqlUnionShape> gqlUnions  = gqlViewpoint.unions();
        Map<String, GqlRecordShape> gqlRecords = gqlViewpoint.records();

        for (String key : gqlRecords.keySet()) {
            GqlRecordShape gqlRecord = gqlRecords.get(key);

            final String typeName = gqlRecord.typeName();

            Iterator<GqlFieldShape> fit = gqlRecord.fields().iterator();

            runtimeWiring.type(typeName, builder -> {
                while (fit.hasNext()) {
                    GqlFieldShape field = fit.next();

                    final String fieldName = field.fieldName();
                    final String backingAttributeName = field.usedAttributeName();
                    final String fieldType = field.gqlTypeRef();
                    final boolean isArray = field.isArray();
                    final boolean isMandatory = field.isMandatory();

                    String attributeNameForField = null;
                    for (GqlAttributeShape shape : attributes.values()) {
                        if (shape.name.equals(backingAttributeName)) {
                            attributeNameForField = shape.alias; // may be same as fieldName
                        }
                    }

                    GqlAttributeShape attributeShape = attributes.get(attributeNameForField);
                    boolean isRecord = AttributeType.RECORD.name().equalsIgnoreCase(attributeShape.typeName);

                    //log.debug("↯ Wiring field '{}' for record '{}'", fieldName, typeName);

                    final List<String> fieldNames = new ArrayList<>();

                    if (null != attributeNameForField && !attributeNameForField.isEmpty()) {
                        fieldNames.add(attributeNameForField);
                    } else {
                        fieldNames.add(fieldName);
                    }

                    GqlUnionShape union = gqlUnions.get(fieldType);
                    if (null != union) {
                        // This field refers to a union type, so the
                        // actual instances will be of a union member type.
                        List<String> unionMemberTypes = union.members().stream().map(UnionMember::memberType).toList();
                        log.debug("\u21af Record '{}' contains a union field '{} : {}' with members {}",
                                typeName, fieldName, union.unionName(), unionMemberTypes
                        );

                        for (String memberType : unionMemberTypes) {
                            for (GqlRecordShape recrd : gqlRecords.values()) {
                                String unionMember = recrd.typeName();
                                if (unionMember.equals(memberType)) {
                                    String attributeEnumName = recrd.attributeEnumName();
                                    log.debug("\u21af Adding alternative {} for union {}", memberType, union.unionName());
                                    fieldNames.add(attributeEnumName);
                                }
                            }
                        }
                    }

                    // Tell GraphQL how to fetch this specific (record) type
                    DataFetcher<?> fetcher = env -> {
                        //**** Executed at runtime **********************************
                        // My mission in life is to resolve a specific attribute
                        // (the current 'fieldName') in a specific type (the current
                        // record). Everything needed at runtime is accessible
                        // right now so we capture it for later.
                        //***********************************************************
                        Box box = env.getSource();
                        if (null == box) {
                            log.warn("No box");
                            return null;
                        }

                        log.trace("\u21a9 Fetching {}attribute '{}' from record '{}': {}",
                                isRecord ? "record " : "",
                                isArray ? fieldName + "[]" : fieldName,
                                typeName, box.getUnit().getReference());

                        if (isRecord) {
                            if (box instanceof RecordBox recordBox) {
                                if (isArray) {
                                    return runtimeService.getValueArray(fieldNames, recordBox, isMandatory);
                                } else {
                                    return runtimeService.getValueScalar(fieldNames, recordBox, isMandatory);
                                }
                            } else if (box instanceof AttributeBox attributeBox) { // as is UnitBox
                                if (isArray) {
                                    return runtimeService.getAttributeArray(fieldNames, attributeBox);
                                } else {
                                    return runtimeService.getAttributeScalar(fieldNames, attributeBox);
                                }
                            } else {
                                log.warn("\u21a9 Unknown box: {}", box.getClass().getCanonicalName());
                                return null;
                            }
                        }
                        else {
                            if (box instanceof RecordBox recordBox) {
                                if (isArray) {
                                    return runtimeService.getValueArray(fieldNames, recordBox, isMandatory);
                                } else {
                                    return runtimeService.getValueScalar(fieldNames, recordBox, isMandatory);
                                }
                            } else if (box instanceof AttributeBox attributeBox) { // as is UnitBox
                                if (isArray) {
                                    return runtimeService.getAttributeArray(fieldNames, attributeBox);
                                } else {
                                    return runtimeService.getAttributeScalar(fieldNames, attributeBox);
                                }
                            } else {
                                log.warn("\u21a9 Unknown box: {}", box.getClass().getCanonicalName());
                                return null;
                            }
                        }
                    };
                    builder.dataFetcher(fieldName, fetcher);
                    log.info("\u21af Wiring: {} > {}", typeName, fieldName);
                }

                return builder;
            });
        }
    }

    /* package accessible only */
    static void wireUnits(
            RuntimeWiring.Builder runtimeWiring,
            RuntimeService runtimeService,
            Configurator.GqlViewpoint gqlViewpoint,
            Configurator.CatalogViewpoint catalogViewpoint
    ) {
        Map<String, GqlUnionShape> gqlUnions  = gqlViewpoint.unions();
        Map<String, GqlRecordShape> gqlRecords = gqlViewpoint.records();

        Map<String, GqlUnitTemplateShape> gqlTemplates = gqlViewpoint.units();
        Map<String, CatalogUnitTemplate> iptoTemplates = catalogViewpoint.units();

        for (String key : gqlTemplates.keySet()) {
            GqlUnitTemplateShape gqlTemplate = gqlTemplates.get(key);
            CatalogUnitTemplate iptoTemplate = iptoTemplates.get(key);

            final String typeName = gqlTemplate.typeName();

            // Assuming these are "synchronized" on ordinal/index
            Iterator<GqlFieldShape> fit = gqlTemplate.fields().iterator();
            Iterator<CatalogAttribute> ait = iptoTemplate.fields().iterator();

            runtimeWiring.type(typeName, builder -> {

                while (fit.hasNext() && ait.hasNext()) {
                    GqlFieldShape field = fit.next();
                    CatalogAttribute attribute = ait.next();

                    final String fieldName = field.fieldName();
                    final String backingAttributeName = field.usedAttributeName();
                    final String fieldType = field.gqlTypeRef();
                    final int fieldAttrId = attribute.attrId();
                    final boolean isArray = field.isArray();
                    final boolean isMandatory = field.isMandatory();

                    final List<String> fieldNames = new ArrayList<>();
                    fieldNames.add(fieldName);
                    if (!fieldName.equals(backingAttributeName)) {
                        fieldNames.add(backingAttributeName);
                    }

                    GqlUnionShape union = gqlUnions.get(fieldType);
                    if (null != union) {
                        // This field refers to a union type, so the
                        // actual instances will be of a union member type.
                        List<String> unionMemberTypes = union.members().stream().map(UnionMember::memberType).toList();
                        log.debug("\u21af Record '{}' contains a union field '{} : {}' with members {}",
                                typeName, fieldName, union.unionName(), unionMemberTypes
                        );

                        for (String memberType : unionMemberTypes) {
                            for (GqlRecordShape recrd : gqlRecords.values()) {
                                String unionMember = recrd.typeName();
                                if (unionMember.equals(memberType)) {
                                    String attributeEnumName = recrd.attributeEnumName();
                                    log.debug("\u21af Adding alternative {} for union {}", memberType, union.unionName());
                                    fieldNames.add(attributeEnumName);
                                }
                            }
                        }
                    }

                    // Tell GraphQL how to fetch this specific (template) type
                    DataFetcher<?> fetcher = env -> {
                        //**** Executed at runtime **********************************
                        // My mission in life is to resolve a specific attribute
                        // (the current 'fieldName') in a specific type (the current
                        // unit). Everything needed at runtime is accessible
                        // right now so we capture it for later.
                        //***********************************************************
                        Box box = env.getSource();
                        if (null == box) {
                            log.warn("No box");
                            return null;
                        }

                        log.trace("\u21a9 Fetching attribute '{}' ({}) from unit '{}': {}", isArray ? fieldName + "[]" : fieldName, fieldAttrId, typeName, box.getUnit().getReference());

                        if (box instanceof RecordBox recordBox) {
                            log.debug("\u21a9 DID NOT EXPECT RECORD BOX IN THIS CONTEXT: {}", typeName);
                            if (isArray) {
                                return runtimeService.getValueArray(fieldNames, recordBox, isMandatory);
                            } else {
                                return runtimeService.getValueScalar(fieldNames, recordBox, isMandatory);
                            }
                        }
                        else if (box instanceof AttributeBox attributeBox) { // UnitBox is an AttributeBox
                            if (isArray) {
                                return runtimeService.getAttributeArray(fieldNames, attributeBox);
                            } else {
                                return runtimeService.getAttributeScalar(fieldNames, attributeBox);
                            }
                        } else {
                            log.warn("\u21a9 Unknown box: {}", box.getClass().getCanonicalName());
                            return null;
                        }
                    };
                    builder.dataFetcher(fieldName, fetcher);
                    log.info("\u21af Wiring: {} > {}", typeName, fieldName);
                }

                return builder;
            });

        }
    }

    /* package accessible only */
    static void wireOperations(
            RuntimeWiring.Builder runtimeWiring,
            RuntimeService runtimeService,
            Configurator.GqlViewpoint gqlViewpoint
    ) {
        Map<String, GqlOperationShape> gqlOperations = gqlViewpoint.operations();

        for (String key : gqlOperations.keySet()) {
            GqlOperationShape gqlOperation = gqlOperations.get(key);

            final String type = gqlOperation.typeName();
            final String operationName = gqlOperation.operationName();
            final String parameterName = gqlOperation.parameterName();
            final String inputType = gqlOperation.inputTypeName();
            final String outputType = gqlOperation.outputTypeName();

            switch (inputType) {
                // "Hardcoded" point lookup for specific unit
                case "UnitIdentification" -> {
                    if ("Bytes".equals(outputType)) {
                        DataFetcher<?> rawUnitById = env -> {
                            //**** Executed at runtime **********************************
                            // My mission in life is to resolve a specific query
                            // (the current 'fieldName').
                            // Everything needed at runtime is accessible right
                            // now so it is captured for later.
                            //***********************************************************
                            if (log.isTraceEnabled()) {
                                log.trace("\u21a9 {}::{}({}) : {}", type, operationName, env.getArguments(), outputType);
                            }

                            Query.UnitIdentification id = objectMapper.convertValue(env.getArgument(parameterName), Query.UnitIdentification.class);
                            return runtimeService.loadRawUnit(id.tenantId(), id.unitId());
                        };

                        runtimeWiring.type(type, t -> t.dataFetcher(operationName, rawUnitById));

                    } else {
                        DataFetcher<?> unitById = env -> {
                            //**** Executed at runtime **********************************
                            // My mission in life is to resolve a specific query
                            // (the current 'operationName').
                            // Everything needed at runtime is accessible right
                            // now so it is captured for later.
                            //***********************************************************
                            if (log.isTraceEnabled()) {
                                log.trace("\u21a9 {}::{}({}) : {}", type, operationName, env.getArguments(), outputType);
                            }

                            Query.UnitIdentification id = objectMapper.convertValue(env.getArgument(parameterName), Query.UnitIdentification.class);
                            return runtimeService.loadUnit(id.tenantId(), id.unitId());
                        };

                        runtimeWiring.type(type, t -> t.dataFetcher(operationName, unitById));
                    }
                    log.info("\u21af Wiring: {}::{}(...) : {}", type, operationName, outputType);
                }

                //
                case "Filter" -> {
                    if ("Bytes".equals(outputType)) {
                        DataFetcher<?> rawUnitsByFilter = env -> {
                            //**** Executed at runtime **********************************
                            // My mission in life is to resolve a specific query
                            // (the current 'operationName').
                            // Everything needed at runtime is accessible right
                            // now so it is captured for later.
                            //***********************************************************

                            if (log.isTraceEnabled()) {
                                log.trace("\u21a9 {}::{}({}) : {}", type, operationName, env.getArguments(), outputType);
                            }

                            Query.Filter filter = objectMapper.convertValue(env.getArgument(parameterName), Query.Filter.class);

                            return runtimeService.searchRaw(filter);
                        };

                        runtimeWiring.type(type, t -> t.dataFetcher(operationName, rawUnitsByFilter));

                    } else {
                        DataFetcher<?> unitsByFilter = env -> {
                            //**** Executed at runtime **********************************
                            // My mission in life is to resolve a specific query
                            // (the current 'operationName').
                            // Everything needed at runtime is accessible right
                            // now so it is captured for later.
                            //***********************************************************
                            if (log.isTraceEnabled()) {
                                log.trace("\u21a9 {}::{}({}) : {}", type, operationName, env.getArguments(), outputType);
                            }

                            Query.Filter filter = objectMapper.convertValue(env.getArgument(parameterName), Query.Filter.class);

                            return runtimeService.search(filter);
                        };

                        runtimeWiring.type(type, t -> t.dataFetcher(operationName, unitsByFilter));
                    }
                    log.info("\u21af Wiring: {}::{}(...) : {}", type, operationName, outputType);
                }
            }
        }
    }

    /* package accessible only */
    static void wireUnions(
            RuntimeWiring.Builder runtimeWiring,
            Configurator.GqlViewpoint gqlViewpoint
    ) {
        Map<String, GqlUnionShape> gqlUnions  = gqlViewpoint.unions();
        Map<String, GqlRecordShape> gqlRecords = gqlViewpoint.records();

        for (String key : gqlUnions.keySet()) {
            GqlUnionShape  gqlUnion = gqlUnions.get(key);

            String unionName = gqlUnion.unionName();

            Map</* record attribute alias */ String, /* record type */ String> aliasToTypeName = new HashMap<>();

            List<UnionMember> members = gqlUnion.members();
            for (UnionMember member : members) {
                String memberName = member.memberType();
                log.info("\u21af Wiring union: {} > {}", unionName, memberName);

                GqlRecordShape gqlRecord = gqlRecords.get(memberName);
                String attributeAlias = gqlRecord.attributeEnumName();

                log.trace("\u21af Union '{}': attribute alias '{}' => type '{}'", unionName, attributeAlias, gqlRecord.typeName());
                aliasToTypeName.put(attributeAlias, gqlRecord.typeName());
            }

            try {
                // Plan ahead (as soon as I get my match together :)
                //   - Derive recordAttributeId from "value"
                //   - Look up GraphQL type name from idToName
                //   - Return that object type

                TypeResolver unionResolver = env -> {
                    //**** Executed at runtime **********************************
                    // My mission in life is to resolve a specific union
                    // (the current 'unionName').
                    // Everything needed at runtime is accessible right
                    // now so it is captured for later.
                    //***********************************************************
                    Object value = env.getObject(); // box
                    log.info("\u21a9 Union: '{}' <- {}", unionName, value);

                    if (value instanceof RecordBox recordBox) {
                        String typeName = aliasToTypeName.get(recordBox.getRecordAttribute().getAlias());
                        return env.getSchema().getObjectType(typeName);
                    }
                    log.warn("\u21a9 No resolver for union '{}': No (record) box: {}", unionName, value);
                    return null;
                };

                runtimeWiring.type(unionName,t -> t.typeResolver(unionResolver));

            } catch (StrictModeWiringException smwe) {
                log.warn("\u21af Could not wire unions for type {}", unionName, smwe);
            }
        }
    }
}
