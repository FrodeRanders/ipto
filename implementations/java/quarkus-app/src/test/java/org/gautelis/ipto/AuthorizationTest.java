package org.gautelis.ipto;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.Map;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;

@QuarkusTest
@TestProfile(LocalAccountsTestProfile.class)
class AuthorizationTest {
    @ParameterizedTest
    @CsvSource({"POST,/api/attributes", "POST,/api/records",
            "PUT,/api/attributes/1/descriptions", "PUT,/api/records/1"})
    void writesRequireAdministrator(String method, String path) {
        given().contentType("application/json").body("{}")
                .when().request(method, path).then().statusCode(401);
        given().auth().preemptive().basic("admin", "incorrect")
                .contentType("application/json").body("{}")
                .when().request(method, path).then().statusCode(401);
        given().auth().preemptive().basic("reader", LocalAccountsTestProfile.READER_PASSWORD)
                .contentType("application/json").body("{}")
                .when().request(method, path).then().statusCode(403);
    }

    @Test
    void sessionUsesServerIdentity() {
        given().when().get("/api/session").then().statusCode(401);
        given().auth().preemptive().basic("admin", LocalAccountsTestProfile.ADMIN_PASSWORD)
                .when().get("/api/session").then().statusCode(200)
                .header("Cache-Control", "no-store")
                .body("username", equalTo("admin")).body("role", equalTo("administrator"));
        given().auth().preemptive().basic("reader", LocalAccountsTestProfile.READER_PASSWORD)
                .when().get("/api/session").then().statusCode(200)
                .body("role", equalTo("reader"));
        given().auth().preemptive().basic("unknown", LocalAccountsTestProfile.ADMIN_PASSWORD)
                .when().get("/api/session").then().statusCode(401);
        given().auth().preemptive().basic("admin", "x".repeat(73))
                .when().get("/api/session").then().statusCode(401);
    }

    @Test
    void queriesStayPublicAndOnlySelectedMutationsRequireAdministrator() {
        String document = "# A comment before the operations\n"
                + "query Read { __typename } mutation Write { __typename }";
        given().contentType("application/json")
                .body(Map.of("query", document, "operationName", "Read"))
                .when().post("/graphql").then().statusCode(200)
                .body("data.__typename", equalTo("Query"));
        given().contentType("application/json")
                .body(Map.of("query", document, "operationName", "Write"))
                .when().post("/graphql").then().statusCode(401);
        given().contentType("application/json")
                .body(Map.of("query", "# query in a comment\nmutation { __typename }"))
                .when().post("/graphql").then().statusCode(401);
        given().auth().preemptive().basic("reader", LocalAccountsTestProfile.READER_PASSWORD)
                .contentType("application/json")
                .body(Map.of("query", document, "operationName", "Write"))
                .when().post("/graphql").then().statusCode(403);
        given().auth().preemptive().basic("admin", LocalAccountsTestProfile.ADMIN_PASSWORD)
                .contentType("application/json")
                .body(Map.of("query", document, "operationName", "Write"))
                .when().post("/graphql").then().statusCode(200)
                .body("data.__typename", equalTo("Mutation"));
        given().when().get("/api/attributes/metadata").then().statusCode(200);
    }
}
