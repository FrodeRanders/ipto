package org.gautelis.ipto;

import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.gautelis.ipto.repo.model.Repository;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static io.restassured.RestAssured.given;
import static org.hamcrest.CoreMatchers.is;

@QuarkusTest
class GreetingResourceTest {
    @Inject
    Repository repository;

    @Test
    void testHelloEndpoint() {
        assertNotNull(repository);
        given()
          .when().get("/hello")
          .then()
             .statusCode(200)
             .body(is("Hello from Quarkus REST"));
    }

}
