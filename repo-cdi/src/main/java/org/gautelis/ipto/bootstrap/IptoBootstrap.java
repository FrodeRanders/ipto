/*
 * Copyright (C) 2025-2026 Frode Randers
 * All rights reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gautelis.ipto.bootstrap;

import graphql.GraphQL;
import graphql.schema.DataFetcher;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.gautelis.ipto.repo.search.SearchResult;
import org.gautelis.ipto.repo.search.query.SearchExpression;
import org.gautelis.ipto.repo.search.query.SearchExpressionQueryParser;
import org.gautelis.ipto.repo.search.query.SearchOrder;
import org.gautelis.ipto.config.IptoConfig;
import org.gautelis.ipto.graphql.configuration.Configurator;
import org.gautelis.ipto.graphql.configuration.OperationsWireParameters;
import org.gautelis.ipto.graphql.model.Query;
import org.gautelis.ipto.repo.model.Repository;
import org.gautelis.ipto.repo.model.Unit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.quarkus.runtime.Startup;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.ObjectNode;

import java.io.InputStreamReader;
import java.time.Instant;
import java.util.Base64;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

import static org.gautelis.ipto.graphql.runtime.service.RuntimeService.headHex;
import static org.gautelis.ipto.graphql.runtime.wiring.RuntimeOperators.MAPPER;

@Startup
@ApplicationScoped
public class IptoBootstrap {
    private static final Logger log = LoggerFactory.getLogger(IptoBootstrap.class);

    private final Repository repository;
    private final IptoConfig config;
    private final Instance<IptoOperationsWiring> wiring;

    private volatile GraphQL graphQL;

    @Inject
    public IptoBootstrap(
            Repository repository,
            IptoConfig config,
            Instance<IptoOperationsWiring> wiring
    ) {
        this.repository = repository;
        this.config = config;
        this.wiring = wiring;
    }

    @PostConstruct
    void init() {
        String sdlResource = config.graphql().sdlResource();
        if (sdlResource == null || sdlResource.isBlank()) {
            log.info("No GraphQL SDL configured; skipping GraphQL bootstrap");
            return;
        }

        log.info("*** Bootstrapping IPTO from {} ***", sdlResource);

        try (InputStreamReader reader = new InputStreamReader(
                Objects.requireNonNull(
                        IptoBootstrap.class.getResourceAsStream(sdlResource),
                        sdlResource + " not found on classpath"
                )
        )) {
            Optional<GraphQL> gql = Configurator.load(repository, reader, this::wireOperations, System.out);
            if (gql.isEmpty()) {
                throw new IllegalStateException("Failed to load GraphQL configuration");
            }
            this.graphQL = gql.get();

            repository.sync(); // see note in Configurator::load

        } catch (Exception e) {
            throw new RuntimeException("Failed to bootstrap IPTO", e);
        }
    }

    public GraphQL graphQL() {
        return graphQL;
    }

    private void wireOperations(OperationsWireParameters params) {
        // Keep only domain-specific wiring that cannot be inferred from SDL + generic runtime dispatch.
        // Other query/mutation operations are now auto-wired by graphql runtime based on SDL shape.
        {
            String type = "Mutation";
            String operationName = "lagraYrkanRaw";
            String outputType = "Bytes";

            DataFetcher<?> storeYrkanJson = env -> {
                //**** Executed at runtime **********************************
                // This closure captures its environment, so at runtime
                // the wiring preamble will be available.
                //***********************************************************

                Map<String, Object> args = env.getArguments();
                int tenantId = (int) args.get("tenantId");
                byte[] bytes = (byte[]) args.get("data"); // Connection to schema

                if (log.isTraceEnabled()) {
                    log.trace("↩ {}::{}({}) : {}", type, operationName, headHex(bytes, 16), outputType);
                }

                byte[] translated = translateYrkanJsonLd(params, bytes, tenantId);
                return params.runtimeService().storeRawUnit(translated);
            };

            params.runtimeWiring().type(type, t -> t.dataFetcher(operationName, storeYrkanJson));
            log.info("↯ Wiring: {}::{}(...) : {}", type, operationName, outputType);
        }

        // Allow external CDI customizers to add/override operation wiring.
        wiring.forEach(custom -> custom.wire(params));
    }

    /**
     * This approach is based on the idea of mapping branded JSON-LD to IPTO native JSON.
     * This may not be the best (or even right) way to go about this, but it'll suffice for now.
     * @param params
     * @param bytes
     * @param tenantId
     * @return
     */
    private static byte[] translateYrkanJsonLd(OperationsWireParameters params, byte[] bytes, int tenantId) {
        try {
            JsonNode root = MAPPER.readTree(bytes);
            if (!(root instanceof ObjectNode payload)) {
                throw new IllegalArgumentException("Expected JSON object for yrkan payload");
            }

            UUID corrId = UUID.fromString(requiredText(payload, "id", "Missing 'id' for correlation id"));
            int providedVersion = requiredInt(payload, "version", "Missing or invalid 'version'");

            Unit existingUnit = findUnitByCorrId(params.repository(), tenantId, corrId);
            int expectedVersion = existingUnit == null ? 1 : existingUnit.getVersion() + 1;
            if (providedVersion != expectedVersion) {
                throw new IllegalArgumentException(
                        "Version mismatch for corrid " + corrId + ": expected " + expectedVersion + " but got " + providedVersion
                );
            }

            ObjectNode unit = MAPPER.createObjectNode();
            unit.put("@type", "ipto:unit");
            unit.put("@version", 2);
            unit.put("tenantid", tenantId);
            if (existingUnit == null) {
                unit.putNull("unitid");
            } else {
                unit.put("unitid", existingUnit.getUnitId());
            }
            unit.putNull("unitver");
            unit.put("corrid", corrId.toString());
            unit.put("status", Unit.Status.EFFECTIVE.getStatus());

            String unitName;
            if (null != existingUnit) {
                Optional<String> name = existingUnit.getName();
                unitName = name.orElseGet(() -> "yrkan-" + corrId);
            } else {
                unitName = "yrkan-" + corrId;
            }
            unit.put("unitname", unitName);

            String now = Instant.now().toString();
            unit.put("created", now);
            unit.put("modified", now);

            ArrayNode attributes = MAPPER.createArrayNode();
            attributes.add(buildDataScalar("raw_payload", "ffa:raw_payload", bytes));

            String description = textValue(payload, "beskrivning");
            addIfPresent(attributes, buildStringScalar("description", "dcterms:description", description));

            addIfPresent(attributes, buildFysiskPerson(payload.path("person")));
            addIfPresent(attributes, buildBeslut(payload.path("beslut")));
            addIfPresent(attributes, buildProduceradeResultat(payload.path("producerade_resultat")));

            unit.set("attributes", attributes);

            return MAPPER.writeValueAsBytes(unit);
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to translate yrkan JSON-LD payload", e);
        }
    }

    private static Unit findUnitByCorrId(Repository repo, int tenantId, UUID corrId) {
        String query = "tenantid = " + tenantId + " AND corrid = \"" + corrId + "\"";
        SearchExpression expr = SearchExpressionQueryParser.parse(query, repo);
        SearchResult result = repo.searchUnit(1, 1, 1, expr, SearchOrder.getDefaultOrder());
        if (result.results().size() > 1) {
            throw new IllegalArgumentException("Multiple units found for corrid " + corrId);
        }
        return result.results().stream().findFirst().orElse(null);
    }

    private static ObjectNode buildFysiskPerson(JsonNode node) {
        if (!(node instanceof ObjectNode personNode)) {
            return null;
        }

        JsonNode valueNode = personNode.path("varde");
        if (!(valueNode instanceof ObjectNode valueObj)) {
            return null;
        }

        String context = textValue(valueObj, "@context");
        if (!"https://data.fk.se/kontext/std/fysiskperson/1.0".equals(context)) {
            return null;
        }

        String personnummer = null;
        JsonNode pn = valueObj.path("personnummer");
        if (pn instanceof ObjectNode pnObj) {
            personnummer = textValue(pnObj, "varde");
        }

        ArrayNode nested = MAPPER.createArrayNode();
        addIfPresent(nested, buildStringScalar("personnummer", "ffa:personnummer", personnummer));

        return buildRecordScalar("fysisk_person", "ffa:fysisk_person", nested);
    }

    private static ObjectNode buildBeslut(JsonNode node) {
        if (!(node instanceof ObjectNode beslutNode)) {
            return null;
        }

        ArrayNode nested = MAPPER.createArrayNode();
        addIfPresent(nested, buildTimeScalar("date", "dcterms:date", textValue(beslutNode, "datum")));
        addIfPresent(nested, buildStringScalar("beslutsfattare", "ffa:beslutsfattare", textValue(beslutNode, "beslutsfattare")));
        addIfPresent(nested, buildStringScalar("beslutstyp", "ffa:beslutstyp", textValue(beslutNode, "beslutstyp")));
        addIfPresent(nested, buildStringScalar("beslutsutfall", "ffa:beslutsutfall", textValue(beslutNode, "beslutsutfall")));
        addIfPresent(nested, buildStringScalar("organisation", "ffa:organisation", textValue(beslutNode, "organisation")));
        addIfPresent(nested, buildStringScalar("lagrum", "ffa:lagrum", textValue(beslutNode, "lagrum")));
        addIfPresent(nested, buildStringScalar("avslagsanledning", "ffa:avslagsanledning", textValue(beslutNode, "avslagsanledning")));

        return buildRecordScalar("beslut", "ffa:beslut", nested);
    }

    private static ObjectNode buildProduceradeResultat(JsonNode node) {
        if (!(node instanceof ArrayNode arrayNode)) {
            return null;
        }

        ArrayNode nested = MAPPER.createArrayNode();
        for (JsonNode item : arrayNode) {
            ObjectNode entry = buildProduceratResultatEntry(item);
            addIfPresent(nested, entry);
        }

        return buildRecordScalar("producerade_resultat", "ffa:producerade_resultat", nested);
    }

    private static ObjectNode buildProduceratResultatEntry(JsonNode node) {
        if (!(node instanceof ObjectNode obj)) {
            return null;
        }

        String context = textValue(obj, "@context");
        if ("https://data.fk.se/kontext/std/ratten-till-period/1.0".equals(context)) {
            ArrayNode nested = MAPPER.createArrayNode();
            addIfPresent(nested, buildStringScalar("ersattningstyp", "ffa:ersattningstyp", textValue(obj, "ersattningstyp")));
            addIfPresent(nested, buildStringScalar("omfattning", "ffa:omfattning", textValue(obj, "omfattning")));
            return buildRecordVector("ratten_till_period", "ffa:ratten_till_period", nested);
        }

        if ("https://data.fk.se/kontext/std/ersattning/1.0".equals(context)) {
            ArrayNode nested = MAPPER.createArrayNode();
            addIfPresent(nested, buildStringScalar("ersattningstyp", "ffa:ersattningstyp", textValue(obj, "typ")));
            addIfPresent(nested, buildBelopp(obj.path("belopp")));
            addIfPresent(nested, buildPeriod(obj.path("period")));
            return buildRecordVector("ersattning", "ffa:ersattning", nested);
        }

        if ("https://data.fk.se/kontext/std/intyg/1.0".equals(context)) {
            ArrayNode nested = MAPPER.createArrayNode();
            addIfPresent(nested, buildStringScalar("description", "dcterms:description", textValue(obj, "beskrivning")));
            addIfPresent(nested, buildPeriod(obj.path("giltighetsperiod")));
            addIfPresent(nested, buildTimeScalar("date", "dcterms:date", textValue(obj, "utfardat_datum")));
            return buildRecordVector("intyg", "ffa:intyg", nested);
        }

        return null;
    }

    private static ObjectNode buildBelopp(JsonNode node) {
        if (!(node instanceof ObjectNode beloppNode)) {
            return null;
        }

        ArrayNode nested = MAPPER.createArrayNode();
        addIfPresent(nested, buildDoubleScalar("beloppsvarde", "ffa:beloppsvarde", beloppNode.path("varde")));
        addIfPresent(nested, buildStringScalar("valuta", "ffa:valuta", textValue(beloppNode, "valuta")));
        addIfPresent(nested, buildStringScalar("skattestatus", "ffa:skattestatus", textValue(beloppNode, "skattestatus")));
        addIfPresent(nested, buildStringScalar("beloppsperiod", "ffa:beloppsperiod", textValue(beloppNode, "beloppsperiod")));

        return buildRecordScalar("belopp", "ffa:belopp", nested);
    }

    private static ObjectNode buildPeriod(JsonNode node) {
        if (!(node instanceof ObjectNode periodNode)) {
            return null;
        }

        ArrayNode nested = MAPPER.createArrayNode();
        addIfPresent(nested, buildTimeScalar("from", "ffa:from", textValue(periodNode, "from")));
        addIfPresent(nested, buildTimeScalar("tom", "ffa:tom", textValue(periodNode, "tom")));

        return buildRecordScalar("period", "ffa:period", nested);
    }

    private static ObjectNode buildStringScalar(String alias, String attrName, String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        ArrayNode values = MAPPER.createArrayNode();
        values.add(value);
        return buildScalar("ipto:string-scalar", alias, "STRING", attrName, values);
    }

    private static ObjectNode buildTimeScalar(String alias, String attrName, String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        ArrayNode values = MAPPER.createArrayNode();
        values.add(value);
        return buildScalar("ipto:time-scalar", alias, "TIME", attrName, values);
    }

    private static ObjectNode buildDoubleScalar(String alias, String attrName, JsonNode node) {
        if (node == null || node.isMissingNode() || node.isNull()) {
            return null;
        }
        ArrayNode values = MAPPER.createArrayNode();
        values.add(node.asDouble());
        return buildScalar("ipto:double-scalar", alias, "DOUBLE", attrName, values);
    }

    private static ObjectNode buildDataScalar(String alias, String attrName, byte[] payload) {
        ArrayNode values = MAPPER.createArrayNode();
        values.add(Base64.getEncoder().encodeToString(payload));
        return buildScalar("ipto:data-scalar", alias, "DATA", attrName, values);
    }

    private static ObjectNode buildScalar(String type, String alias, String attrType, String attrName, ArrayNode values) {
        ObjectNode node = MAPPER.createObjectNode();
        node.put("@type", type);
        node.put("alias", alias);
        node.put("attrtype", attrType);
        node.put("attrname", attrName);
        node.set("value", values);
        return node;
    }

    private static ObjectNode buildRecordScalar(String alias, String attrName, ArrayNode attributes) {
        if (attributes == null || attributes.isEmpty()) {
            return null;
        }
        ObjectNode node = MAPPER.createObjectNode();
        node.put("@type", "ipto:record-scalar");
        node.put("alias", alias);
        node.put("attrtype", "RECORD");
        node.put("attrname", attrName);
        node.set("attributes", attributes);
        return node;
    }

    private static ObjectNode buildRecordVector(String alias, String attrName, ArrayNode attributes) {
        if (attributes == null || attributes.isEmpty()) {
            return null;
        }
        ObjectNode node = MAPPER.createObjectNode();
        node.put("@type", "ipto:record-vector");
        node.put("alias", alias);
        node.put("attrtype", "RECORD");
        node.put("attrname", attrName);
        node.set("attributes", attributes);
        return node;
    }

    private static void addIfPresent(ArrayNode target, ObjectNode node) {
        if (node != null) {
            target.add(node);
        }
    }

    private static String textValue(JsonNode node, String field) {
        if (node == null) {
            return null;
        }
        JsonNode value = node.path(field);
        if (value.isMissingNode() || value.isNull()) {
            return null;
        }
        return value.asText();
    }

    private static String requiredText(ObjectNode node, String field, String message) {
        String value = textValue(node, field);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(message);
        }
        return value;
    }

    private static int requiredInt(ObjectNode node, String field, String message) {
        JsonNode value = node.path(field);
        if (value.isMissingNode() || value.isNull()) {
            throw new IllegalArgumentException(message);
        }
        return value.asInt();
    }
}
