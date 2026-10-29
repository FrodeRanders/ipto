package org.gautelis.ipto;

import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

import java.util.regex.Pattern;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@QuarkusTest
class WebPackagingTest {
    @Test
    void packagedUiSupportsDirectNavigationAndItsEntryAssetsExist() {
        assumeTrue(getClass().getClassLoader().getResource("META-INF/resources/200.html") != null,
                "Build with -Pweb to verify UI packaging");
        given().get("/").then().statusCode(200).contentType("text/html")
                .body(containsString("Sign in"));
        for (String path : new String[]{"/admin", "/trees", "/search", "/admin/attributes/new",
                "/admin/attributes/1/edit", "/admin/records/1/edit"}) {
            String shell = given().get(path).then().statusCode(200)
                    .contentType("text/html").extract().asString();
            var entries = Pattern.compile("(?:\\./|/)(_app/immutable/entry/[^\"']+\\.js)").matcher(shell);
            int count = 0;
            while (entries.find()) {
                given().get("/" + entries.group(1)).then().statusCode(200);
                count++;
            }
            assertTrue(count >= 2, "Missing SvelteKit entry assets at " + path);
        }
    }

    @Test
    void unknownApiAndAssetRoutesRemainNotFound() {
        given().get("/api/does-not-exist").then().statusCode(404);
        given().get("/_app/does-not-exist.js").then().statusCode(404);
    }
}
