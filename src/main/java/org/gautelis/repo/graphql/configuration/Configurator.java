package org.gautelis.repo.graphql.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import graphql.GraphQL;
import graphql.language.*;
import graphql.schema.DataFetcher;
import graphql.schema.idl.RuntimeWiring;
import graphql.schema.idl.SchemaGenerator;
import graphql.schema.idl.SchemaParser;
import graphql.schema.idl.TypeDefinitionRegistry;
import org.gautelis.repo.db.Database;
import org.gautelis.repo.exceptions.AttributeTypeException;
import org.gautelis.repo.graphql.runtime.RepositoryService;
import org.gautelis.repo.graphql.runtime.scalars.BytesScalar;
import org.gautelis.repo.graphql.runtime.scalars.DateTimeScalar;
import org.gautelis.repo.graphql.runtime.scalars.LongScalar;
import org.gautelis.repo.model.Repository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Reader;
import java.sql.SQLException;
import java.util.*;

import static org.gautelis.repo.model.attributes.Type.of;

public class Configurator {
    private static final Logger log = LoggerFactory.getLogger(Configurator.class);

    //
    public record ExistingDatatypeMeta(int id, String nameInSchema) {}

    public record ProposedAttributeMeta(int attrId, String nameInSchema, int attrType, boolean isVector, String nameInIpto, String qualifiedName, String description) {}

    private final Repository repo;
    private final RepositoryService repoService;
    private final TypeDefinitionRegistry registry;

    private final Map<String, ExistingDatatypeMeta> datatypes = new HashMap<>();
    private final Map<String, ProposedAttributeMeta> attributesSchemaView = new HashMap<>();
    private final Map<Integer, ProposedAttributeMeta> attributesIptoView = new HashMap<>();

    public Configurator(Repository repo, Reader reader) {
        this.repo = repo;
        this.repoService = new RepositoryService(repo, datatypes, attributesIptoView);
        this.registry = new SchemaParser().parse(reader);
    }

    public Optional<GraphQL> load() {
        RuntimeWiring.Builder runtimeWiring = RuntimeWiring.newRuntimeWiring();

        // Register (known) scalars
        runtimeWiring.scalar(LongScalar.INSTANCE)
                     .scalar(DateTimeScalar.INSTANCE)
                     .scalar(BytesScalar.INSTANCE);

        wireQueries(runtimeWiring);

        // Load attributes
        AttributeConfigurator attributeLoader = new AttributeConfigurator(repo);

        try {
            for (EnumTypeDefinition enumeration : registry.getTypes(EnumTypeDefinition.class)) {
                List<Directive> enumDirectives = enumeration.getDirectives();
                for (Directive directive : enumDirectives) {
                    switch (directive.getName()) {
                        case "datatypeRegistry" -> {
                            // -----------------------------------------------------------
                            // This is not configurable, but it is part of the
                            // configuration SDL as reference.
                            // -----------------------------------------------------------
                            if (!verifyDatatypes(enumeration, datatypes)) {
                                return Optional.empty();
                            }
                        }
                        case "attributeRegistry" -> attributeLoader.load(enumeration, datatypes, attributesSchemaView, attributesIptoView, runtimeWiring, repoService);
                        default -> {
                        }
                    }
                }
            }

            // Iterate over types
            UnitConfigurator unitLoader = new UnitConfigurator(repo);
            RecordConfigurator recordLoader = new RecordConfigurator(repo);

            for (ObjectTypeDefinition type : registry.getTypes(ObjectTypeDefinition.class)) {
                Collection<String> info = new ArrayList<>();
                info.add("\n");

                // Handle @unit directives on object types
                List<Directive> unitDirectivesOnType = type.getDirectives("unit");
                if (!unitDirectivesOnType.isEmpty()) {
                    unitLoader.load(type, unitDirectivesOnType, attributesSchemaView, runtimeWiring, repoService, info);

                } else {
                    // Handle @record directives on object types
                    List<Directive> recordDirectivesOnType = type.getDirectives("record");
                    if (!recordDirectivesOnType.isEmpty()) {
                        recordLoader.load(type, recordDirectivesOnType, attributesSchemaView, runtimeWiring, repoService, info);
                    } else {
                        // Query or Mutation?
                        type.getFieldDefinitions().forEach(field -> {
                            if (field.getType() instanceof TypeName typeName) {
                                log.trace("{}(...) : {}", field.getName(), typeName.getName());
                            }
                        });
                    }
                }

                log.info(String.join("", info));
            }
        } catch (Throwable t) {
            log.error("Failed to load configuration: {}", t.getMessage(), t);

            if (t.getCause() instanceof SQLException sqle) {
                log.error("  ^-- more details: {}", Database.squeeze(sqle));
            }
        }

        return Optional.of(
                GraphQL.newGraphQL(
                    new SchemaGenerator().makeExecutableSchema(registry, runtimeWiring.build())
                ).build()
        );
    }

    private void wireQueries(RuntimeWiring.Builder runtimeWiring) {
        record UnitIdentification(int tenantId, long unitId) {}
        ObjectMapper objectMapper = new ObjectMapper();

        // Point lookup for specific unit
        DataFetcher<?> unitById = env -> {
            log.trace("Query::unit(id : {})", (Object) env.getArgument("id"));

            UnitIdentification id = objectMapper.convertValue(env.getArgument("id"), UnitIdentification.class);
            return repoService.loadUnit(id.tenantId(), id.unitId());
        };

        runtimeWiring.type("Query", t -> t.dataFetcher("unit", unitById));
        log.info("Wiring: Query unit");
    }

    private boolean verifyDatatypes(
            EnumTypeDefinition enumeration,
            Map<String, ExistingDatatypeMeta> datatypes
    ) {
        for (EnumValueDefinition enumValueDefinition : enumeration.getEnumValueDefinitions()) {
            List<Directive> enumValueDirectives = enumValueDefinition.getDirectives();
            for (Directive enumValueDirective : enumValueDirectives) {
                String info = "";

                int id = -1; // INVALID
                Argument arg = enumValueDirective.getArgument("id");
                if (null != arg) {
                    info += "(" + arg.getName();
                    IntValue _id = (IntValue) arg.getValue();
                    id = _id.getValue().intValue();
                    info += "=" + id;

                    // Validation
                    try {
                        var _officialType = of(_id.getValue().intValue());
                    } catch (AttributeTypeException ate) {
                        log.error("Not an official data type: {} with numeric attrId {}", enumValueDefinition.getName(), _id.getValue().intValue(), ate);
                        return false;
                    }
                }

                arg = enumValueDirective.getArgument("basictype");
                if (null != arg) {
                    // NOTE: 'COMPOUND' does not have a particular basictype,
                    // it being compound of other attributes and all...
                    info += ", " + arg.getName();
                    StringValue _type = (StringValue) arg.getValue();
                    info += "=" + _type.getValue();
                }
                info += ")";

                if (/* VALID? */ id > 0) {
                    log.debug("Valid datatype: {} {}", enumValueDefinition.getName(), info);

                    datatypes.put(enumValueDefinition.getName(),
                            new ExistingDatatypeMeta(id, enumValueDefinition.getName())
                    );
                }
            }
        }
        return true;
    }
}
