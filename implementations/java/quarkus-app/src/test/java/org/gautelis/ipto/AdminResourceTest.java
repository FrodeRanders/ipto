package org.gautelis.ipto;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;

@QuarkusTest
@TestProfile(LocalAccountsTestProfile.class)
class AdminResourceTest {
    private static final String DEV_ORIGIN = "http://localhost:5173";

    private int createAttribute(String type) {
        String name = "api_" + UUID.randomUUID().toString().replace("-", "");
        return given().auth().preemptive().basic("admin", LocalAccountsTestProfile.ADMIN_PASSWORD)
                .contentType("application/json")
                .body(Map.of("alias", name, "attributeName", name,
                        "qualifiedName", "urn:ipto:test:" + name,
                        "type", type, "isArray", false))
                .when().post("/api/attributes")
                .then().statusCode(200)
                .extract().path("_id");
    }

    @Test
    void attributeDescriptionsCanBeSavedAndLoaded() {
        int id = createAttribute("STRING");
        for (String description : List.of("Original description", "Updated description")) {
            given().auth().preemptive().basic("admin", LocalAccountsTestProfile.ADMIN_PASSWORD).header("Origin", DEV_ORIGIN)
                    .contentType("application/json")
                    .body(Map.of("items", List.of(Map.of(
                            "lang", "EN", "alias", "Display title", "description", description))))
                    .when().put("/api/attributes/{id}/descriptions", id)
                    .then().statusCode(200)
                    .header("Access-Control-Allow-Origin", DEV_ORIGIN);

            var descriptions = given()
                    .when().get("/api/attributes/{id}/descriptions", id)
                    .then().statusCode(200)
                    .extract().jsonPath();
            assertEquals(List.of(description), descriptions.getList("description", String.class));
            assertEquals(List.of("EN"), descriptions.getList("lang", String.class));
        }
    }

    @Test
    void recordNameAndFieldOrderCanBeSavedAndLoaded() {
        int recordId = createAttribute("RECORD");
        int firstId = createAttribute("STRING");
        int secondId = createAttribute("STRING");

        given().auth().preemptive().basic("admin", LocalAccountsTestProfile.ADMIN_PASSWORD).contentType("application/json")
                .body(Map.of("recordId", recordId, "name", "Original record " + recordId,
                        "fields", List.of(Map.of("attrId", firstId, "alias", "first"))))
                .when().post("/api/records")
                .then().statusCode(200);

        given().auth().preemptive().basic("admin", LocalAccountsTestProfile.ADMIN_PASSWORD).header("Origin", DEV_ORIGIN)
                .contentType("application/json")
                .body(Map.of("name", "Updated record " + recordId, "fields", List.of(
                        Map.of("attrId", secondId, "alias", "second"),
                        Map.of("attrId", firstId, "alias", "first"))))
                .when().put("/api/records/{id}", recordId)
                .then().statusCode(200)
                .header("Access-Control-Allow-Origin", DEV_ORIGIN);

        var record = given()
                .when().get("/api/records/{id}", recordId)
                .then().statusCode(200)
                .extract().jsonPath();
        assertEquals("Updated record " + recordId, record.getString("_name"));
        assertEquals(List.of(secondId, firstId), record.getList("_fields.attrId", Integer.class));
        assertEquals(List.of("second", "first"), record.getList("_fields.alias", String.class));
    }

    @Test
    void developmentOriginCanPreflightPutRequests() {
        given().auth().preemptive().basic("admin", LocalAccountsTestProfile.ADMIN_PASSWORD).header("Origin", DEV_ORIGIN)
                .header("Access-Control-Request-Method", "PUT")
                .header("Access-Control-Request-Headers", "content-type")
                .when().options("/api/records/1")
                .then().statusCode(200)
                .header("Access-Control-Allow-Origin", DEV_ORIGIN)
                .header("Access-Control-Allow-Methods", containsString("PUT"));
    }
}
