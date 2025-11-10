package org.gautelis.repo.graphql.configuration;

import org.gautelis.repo.graphql.model.CatalogAttribute;
import org.gautelis.repo.graphql.model.GqlFieldShape;
import org.gautelis.repo.graphql.model.ResolvedField;
import org.gautelis.repo.graphql.model.Source;
import org.gautelis.repo.model.AttributeType;
import org.gautelis.repo.model.attributes.Attribute;
import org.gautelis.repo.model.attributes.Cardinality;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.EnumSet;
import java.util.Map;
import java.util.Optional;

public final class FieldResolver {
    private static final Logger log = LoggerFactory.getLogger(FieldResolver.class);

    public ResolvedField resolve(
            GqlFieldShape gql,
            Optional<CatalogAttribute> fromCatalog,
            Map<String, CatalogAttribute> attrByName,     // registry
            Map<Integer, CatalogAttribute> attrById,      // registry
            ResolutionPolicy policy                       // e.g. prefer GraphQL or Catalog for conflicts
    ) {
        EnumSet<Source> prov = EnumSet.noneOf(Source.class);
        prov.add(Source.GQL);
        fromCatalog.ifPresent(c -> prov.add(Source.CATALOG));

        // Choose attribute binding (id and name)
        CatalogAttribute cat = fromCatalog.orElseGet(() -> {
            if (null != gql.usedAttributeName()) {
                CatalogAttribute byName = attrByName.get(gql.usedAttributeName());
                if (null == byName) {
                    log.warn("Unknown attribute '{}' for {}.{}",
                            gql.usedAttributeName(), gql.typeName(), gql.fieldName()
                    );
                }
                return byName;
            }
            return null;
        });

        if (null == cat) {
            String info = "No attribute mapping for " + gql.typeName() + "." + gql.fieldName();
            log.error(info);
            throw new RuntimeException(info);
        }

        // Derive cardinality & type
        boolean isArray = gql.isArray();
        Cardinality cardinality = isArray ? Cardinality.VECTOR : Cardinality.SCALAR;
        AttributeType attrType = cat.attrType();

        /*
        int fieldAttributeId = fieldAttributeDef.attributeId;

        if (fieldAttributeDef.isArray != fieldType.isArray()) {
            String info = "Definition of field " + fieldName + " in record|type " + recordName + " is invalid: " + f.getType() + " differs with respect to array capability from definition of attribute " + fieldAttributeName;
            log.error(info);
            System.out.println(info);
            throw new ConfigurationException(info);
        }
        */

        // Build resolved
        return new ResolvedField(
                gql.typeName(),
                gql.fieldName(),
                new Attribute.Reference(cat.attrId(), cat.attrName()),
                attrType,
                cardinality,
                prov
        );
    }
}
