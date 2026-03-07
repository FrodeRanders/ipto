package org.gautelis.ipto.api;

import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.gautelis.ipto.repo.model.Repository;
import org.gautelis.ipto.repo.model.Unit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Path("/api/units")
public class UnitResource {
    private static final Logger log = LoggerFactory.getLogger(UnitResource.class);

    @Inject
    Repository repository;

    @GET
    @Path("/{tenantId}/{unitId}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response unitById(
            @PathParam("tenantId") int tenantId,
            @PathParam("unitId") long unitId,
            @QueryParam("unitver") Integer unitVersion
    ) {
        log.debug("UnitResource::unitById({}, {}, {})", tenantId, unitId, unitVersion);

        Optional<Unit> unit;
        try {
            unit = unitVersion == null
                    ? repository.getUnit(tenantId, unitId)
                    : repository.getUnit(tenantId, unitId, unitVersion);
        } catch (RuntimeException ex) {
            log.error("Failed to load unit {}.{}:{}", tenantId, unitId, unitVersion, ex);
            return Response.serverError()
                    .entity(Map.of("error", "Failed to load unit"))
                    .build();
        }

        if (unit.isEmpty()) {
            return Response.status(Response.Status.NOT_FOUND)
                    .entity(Map.of("error", "Unit not found"))
                    .build();
        }

        String json = unit.get().asJson(/* pretty? */ false);

        log.trace("-> unit: {}", json);
        return Response.ok(json, MediaType.APPLICATION_JSON).build();
    }
}
