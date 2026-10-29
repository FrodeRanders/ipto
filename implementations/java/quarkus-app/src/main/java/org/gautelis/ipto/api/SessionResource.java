package org.gautelis.ipto.api;

import io.quarkus.security.Authenticated;
import io.quarkus.security.identity.SecurityIdentity;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.util.Map;

@Path("/api/session")
@Produces(MediaType.APPLICATION_JSON)
public class SessionResource {
    @Inject
    SecurityIdentity identity;

    @GET
    @Authenticated
    public Response current() {
        return Response.ok(Map.of("username", identity.getPrincipal().getName(),
                        "role", identity.hasRole("administrator") ? "administrator" : "reader"))
                .header("Cache-Control", "no-store")
                .build();
    }
}
