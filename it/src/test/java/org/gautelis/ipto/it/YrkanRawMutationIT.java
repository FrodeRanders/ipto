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
package org.gautelis.ipto.it;

import graphql.ExecutionInput;
import graphql.ExecutionResult;
import graphql.GraphQL;
import graphql.GraphQLError;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ObjectNode;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("GraphQL")
@IptoIT
class YrkanRawMutationIT {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void storesFfaExampleThroughCustomMutation(GraphQL graphQL) throws Exception {
        byte[] payload;
        try (InputStream in = YrkanRawMutationIT.class.getResourceAsStream("ffa-example.json")) {
            assertNotNull(in, "Missing test resource: ffa-example.json");
            payload = in.readAllBytes();
        }
        UUID corrId = UUID.randomUUID();
        payload = withCorrId(payload, corrId);

        String mutation = """
                mutation Store($tenantId: Int!, $data: Bytes!) {
                  lagraYrkanRaw(tenantId: $tenantId, data: $data)
                }
                """;

        ExecutionResult result = graphQL.execute(
                ExecutionInput.newExecutionInput()
                        .query(mutation)
                        .variables(Map.of(
                                "tenantId", 1,
                                "data", Base64.getEncoder().encodeToString(payload)
                        ))
                        .build()
        );

        List<GraphQLError> errors = result.getErrors();
        assertTrue(errors.isEmpty(), "Unexpected GraphQL errors: " + errors);

        Map<String, String> data = result.getData();
        assertNotNull(data);
        String b64Stored = data.get("lagraYrkanRaw");
        assertNotNull(b64Stored, "Missing response value for lagraYrkanRaw");

        byte[] storedBytes = Base64.getDecoder().decode(b64Stored.getBytes(StandardCharsets.UTF_8));
        JsonNode stored = MAPPER.readTree(storedBytes);
        assertEquals("ipto:unit", stored.path("@type").asText());
        assertEquals(1, stored.path("tenantid").asInt());
        assertEquals(corrId.toString(), stored.path("corrid").asText());

        boolean hasRawPayload = false;
        for (JsonNode attribute : stored.path("attributes")) {
            if ("ffa:raw_payload".equals(attribute.path("attrname").asText())) {
                hasRawPayload = true;
                break;
            }
        }
        assertTrue(hasRawPayload, "Stored unit must include ffa:raw_payload");
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
