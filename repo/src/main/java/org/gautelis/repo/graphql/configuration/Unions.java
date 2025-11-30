package org.gautelis.repo.graphql.configuration;

import graphql.language.*;
import graphql.schema.idl.TypeDefinitionRegistry;
import org.gautelis.repo.graphql.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public final class Unions {
    private static final Logger log = LoggerFactory.getLogger(Unions.class);

    private Unions() {
    }

    /*
     * type FysiskPerson @record(attribute: fysisk_person) {
     *     personnummer : String
     * }
     *
     * type JuridiskPerson @record(attribute: juridisk_person) {
     *     orgnummer : String
     * }
     *
     * union Person = FysiskPerson | JuridiskPerson
     *        ^             ^                ^
     *        | (a)         | (b)            | (...)
     */
    static Map<String, GqlUnionShape> derive(
            TypeDefinitionRegistry registry,
            Map<String, GqlAttributeShape> attributes,
            Map<String, GqlRecordShape> gqlRecords
    ) {
        Map<String, GqlUnionShape> unions = new HashMap<>();


        for (UnionTypeDefinition union : registry.getTypes(UnionTypeDefinition.class)) {
            // --- (a) ---
            String unionName = union.getName();

            List<UnionMember> members = new ArrayList<>();

            List<Type> memberTypes = union.getMemberTypes();
            for (Type memberType : memberTypes) {
                if (log.isTraceEnabled()) {
                    if (memberType instanceof TypeName memberTypeName) {
                        log.trace("Inspecting union : {} > {}", unionName, memberTypeName.getName());
                    } else {
                        log.trace("Inspecting union : {} > {}", unionName, memberType);
                    }
                }

                if (memberType instanceof TypeName unionTypeName) {
                    String memberName = unionTypeName.getName();

                    GqlRecordShape gqlRecord = gqlRecords.get(memberName);
                    //CatalogRecord iptoRecord = iptoRecords.get(memberName);

                    String attributeAlias = gqlRecord.attributeEnumName();
                    //log.trace("    attribute-alias: {}", attributeAlias);
                    //log.trace("    attribute-name: {} ({})", iptoRecord.recordName, iptoRecord.recordAttrId);

                    members.add(new UnionMember(unionName, memberName));
                } else {
                    log.warn("Ignoring member '{}' in union '{}'", memberType, unionName);
                }
            }

            unions.put(unionName, new GqlUnionShape(unionName, members));
            log.trace("Defining shape for {}: {}", unionName, unions.get(unionName));
        }

        return unions;
    }
}