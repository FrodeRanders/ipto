package org.gautelis.repo.graphql2.configuration;

import graphql.language.*;
import graphql.schema.idl.TypeDefinitionRegistry;
import org.gautelis.repo.db.Database;
import org.gautelis.repo.graphql2.model.AttributeDef;
import org.gautelis.repo.graphql2.model.DataTypeDef;
import org.gautelis.repo.model.AttributeType;
import org.gautelis.repo.model.Repository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.gautelis.repo.model.AttributeType.RECORD;


public final class Attributes {
    private static final Logger log = LoggerFactory.getLogger(Attributes.class);

    private Attributes() {
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
    static Map<String, AttributeDef> derive(TypeDefinitionRegistry registry, Map<String, DataTypeDef> datatypes) {
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

    static Map<String, AttributeDef> read(Repository repository) {
        Map<String, AttributeDef> attributes = new HashMap<>();

        String sql = """
                SELECT attrid, attrtype, scalar, attrname, qualname
                FROM repo_attribute
                """;

        try {
            repository.withConnection(conn -> {
                Database.useReadonlyPreparedStatement(conn, sql, pStmt -> {
                    try (ResultSet rs = pStmt.executeQuery()) {
                        while (rs.next()) {
                            int attributeId = rs.getInt("attrid");
                            int attributeType = rs.getInt("attrtype");
                            boolean isArray = !rs.getBoolean("scalar"); // Note negation
                            String attributeName = rs.getString("attrname");
                            String qualifiedName = rs.getString("qualname");

                             attributes.put(attributeName, new AttributeDef(
                                     /* GraphQL specific name */ null,
                                     /* Ipto specific attribute id */ attributeId,
                                     AttributeType.of(attributeType).name(), attributeType,
                                     isArray,
                                     /* Ipto specific attribute name */ attributeName,
                                     /* Ipto specific qualified name */ qualifiedName,
                                     /* Ipto specific description */ null));
                        }
                    }
                });
            });
        } catch (SQLException sqle) {
            log.error("Failed to read attributes: {}", Database.squeeze(sqle));
        }

        return attributes;
    }
}