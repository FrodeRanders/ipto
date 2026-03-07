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
package org.gautelis.ipto;

import graphql.ExecutionInput;
import graphql.ExecutionResult;
import graphql.GraphQL;
import graphql.GraphQLError;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static io.restassured.RestAssured.given;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
class GraphQLResourceTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Inject
    GraphQL graphQL;

    @Test
    void graphQLBeanIsAvailable() {
        assertNotNull(graphQL);
    }

    @Test
    void rejectsMissingQuery() {
        given()
                .contentType("application/json")
                .body("{}")
                .when().post("/graphql")
                .then()
                .statusCode(400);
    }

    @Test
    void storesYrkanFromExampleJson() {
        byte[] payload;
        try (InputStream in = GraphQLResourceTest.class.getResourceAsStream("ffa-example.json")) {
            assertNotNull(in, "Missing test resource: ffa-example.json");
            payload = in.readAllBytes();
        } catch (Exception e) {
            throw new RuntimeException("Failed to read ffa-example.json", e);
        }
        UUID corrId = UUID.randomUUID();
        payload = withCorrId(payload, corrId);

        String mutation = """
                mutation Store($tenantId: Int!, $data: Bytes!) {
                  lagraYrkanRaw(tenantId: $tenantId, data: $data)
                }
                """;

        String b64Data = Base64.getEncoder().encodeToString(payload);
        ExecutionResult result = graphQL.execute(
                ExecutionInput.newExecutionInput()
                        .query(mutation)
                        .variables(Map.of(
                                "tenantId", 1,
                                "data", b64Data
                        ))
                        .build()
        );

        List<GraphQLError> errors = result.getErrors();
        assertTrue(errors.isEmpty(), "Unexpected GraphQL errors: " + errors);

        Map<String, String> data = result.getData();
        assertNotNull(data, "No GraphQL response payload");
        String b64Stored = data.get("lagraYrkanRaw");
        assertNotNull(b64Stored, "Missing mutation field: lagraYrkanRaw");

        String storedJson = new String(Base64.getDecoder().decode(b64Stored), StandardCharsets.UTF_8);
        assertFalse(storedJson.isBlank(), "Stored unit payload is blank");
        assertTrue(storedJson.contains("\"@type\":\"ipto:unit\""), "Stored payload is not an IPTO unit");
        assertTrue(storedJson.contains("\"tenantid\":1"), "Stored payload does not target tenant 1");
        assertTrue(storedJson.contains("\"corrid\":\"" + corrId + "\""), "Stored payload does not include expected corrid");
        assertTrue(storedJson.contains("\"attrname\":\"ffa:raw_payload\""), "Stored payload does not include raw_payload");
    }

    private static byte[] withCorrId(byte[] payload, UUID corrId) {
        try {
            ObjectNode root = (ObjectNode) MAPPER.readTree(payload);
            root.put("id", corrId.toString());
            return MAPPER.writeValueAsBytes(root);
        } catch (Exception e) {
            throw new RuntimeException("Failed to rewrite ffa-example corrid", e);
        }
    }
}
