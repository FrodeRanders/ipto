package org.gautelis.ipto.api;

import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.gautelis.ipto.repo.model.Repository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Path("/api/tenants")
@Produces(MediaType.APPLICATION_JSON)
public class TenantResource {
    private static final Logger log = LoggerFactory.getLogger(TenantResource.class);

    @Inject
    Repository repository;

    @GET
    public Response list() {
        log.debug("TenantResource::list()");

        List<Map<String, Object>> tenants = new ArrayList<>();

        String sql = """
            SELECT tenantid, name, description, created
            FROM repo_tenant
            ORDER BY tenantid
            """;

        try {
            repository.withConnection(conn -> {
                try (PreparedStatement stmt = conn.prepareStatement(sql);
                     ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        Map<String, Object> row = new LinkedHashMap<>();
                        row.put("_tenant_id", rs.getInt("tenantid"));
                        row.put("_name", rs.getString("name"));
                        row.put("_description", rs.getString("description"));
                        row.put("_created", rs.getTimestamp("created"));
                        tenants.add(row);
                    }
                } catch (SQLException sqle) {
                    throw new RuntimeException(sqle);
                }
            });
        } catch (SQLException | RuntimeException ex) {
            return Response.serverError()
                    .entity(Map.of("error", "Failed to load tenants"))
                    .build();
        }

        log.trace("-> tenants: {}", tenants);
        return Response.ok(tenants).build();
    }
}
