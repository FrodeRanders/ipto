package org.gautelis.ipto;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.restassured.RestAssured.given;

@QuarkusTest
@TestProfile(DisabledAccountsTest.NoAccounts.class)
class DisabledAccountsTest {
    public static class NoAccounts implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            return Map.of("ipto-accounts.admin-name", "admin", "ipto-accounts.reader-name", "reader",
                    "ipto-accounts.admin-password-hash", "",
                    "ipto-accounts.reader-password-hash", "not-a-bcrypt-hash");
        }
    }

    @Test
    void unconfiguredAccountsHaveNoDefaultPassword() {
        given().auth().preemptive().basic("admin", "")
                .when().get("/api/session").then().statusCode(401);
        given().auth().preemptive().basic("reader", "")
                .when().get("/api/session").then().statusCode(401);
        given().when().get("/api/tenants").then().statusCode(200);
    }

    @Test
    void malformedHashRejectsCredentialsWithoutReturningServerError() {
        given().auth().preemptive().basic("reader", "any-password")
                .when().get("/api/session").then().statusCode(401);
    }
}
