package org.gautelis.ipto;

import graphql.GraphQL;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import static io.restassured.RestAssured.given;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@QuarkusTest
class GraphQLResourceTest {
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
}
