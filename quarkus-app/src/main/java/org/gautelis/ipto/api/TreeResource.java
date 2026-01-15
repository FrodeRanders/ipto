package org.gautelis.ipto.api;

import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.gautelis.ipto.repo.exceptions.InvalidParameterException;
import org.gautelis.ipto.repo.model.RelationType;
import org.gautelis.ipto.repo.model.Repository;
import org.gautelis.ipto.repo.model.Unit;
import org.gautelis.ipto.repo.model.AssociationType;
import org.gautelis.ipto.repo.search.SearchResult;
import org.gautelis.ipto.repo.search.model.LeftRelationSearchItem;
import org.gautelis.ipto.repo.search.query.QueryBuilder;
import org.gautelis.ipto.repo.search.query.SearchExpression;
import org.gautelis.ipto.repo.search.query.SearchOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Path("/api/trees")
@Produces(MediaType.APPLICATION_JSON)
public class TreeResource {
    private static final Logger log = LoggerFactory.getLogger(TreeResource.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Inject
    Repository repository;

    @GET
    @Path("/roots")
    public Response roots() {
        log.debug("TreeResource::roots()");

        List<Map<String, Object>> roots = new ArrayList<>();

        String sql = """
            SELECT t.tenantid,
                   t.name AS tenant_name,
                   t.description AS tenant_description,
                   uk.unitid,
                   uv.unitver,
                   COALESCE(uv.unitname, t.name) AS unit_name,
                   uk.status AS status,
                   uk.created AS created,
                   uv.modified AS modified,
                   (SELECT COUNT(*) FROM repo_unit_kernel ukc WHERE ukc.tenantid = t.tenantid) AS unit_count
            FROM repo_tenant t
            LEFT JOIN repo_unit_kernel uk
                ON uk.tenantid = t.tenantid AND uk.unitid = 0
            LEFT JOIN repo_unit_version uv
                ON uv.tenantid = uk.tenantid AND uv.unitid = uk.unitid AND uv.unitver = uk.lastver
            ORDER BY t.tenantid
            """;

        try {
            repository.withConnection(conn -> {
                try (PreparedStatement stmt = conn.prepareStatement(sql);
                     ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        Map<String, Object> row = new LinkedHashMap<>();
                        long unitId = rs.getLong("unitid");
                        if (rs.wasNull()) {
                            unitId = 0;
                        }
                        row.put("_tenant_id", rs.getInt("tenantid"));
                        row.put("_tenant_name", rs.getString("tenant_name"));
                        row.put("_tenant_description", rs.getString("tenant_description"));
                        row.put("_unit_id", unitId);
                        row.put("_unit_ver", rs.getInt("unitver"));
                        row.put("_unit_name", rs.getString("unit_name"));
                        row.put("_status", rs.getObject("status"));
                        row.put("_created", rs.getTimestamp("created"));
                        row.put("_modified", rs.getTimestamp("modified"));
                        row.put("_unit_count", rs.getInt("unit_count"));
                        roots.add(row);
                    }
                } catch (SQLException sqle) {
                    throw new RuntimeException(sqle);
                }
            });
        } catch (SQLException | RuntimeException ex) {
            return Response.serverError()
                    .entity(Map.of("error", "Failed to load tree roots"))
                    .build();
        }

        log.debug("-> roots: {}", roots);
        return Response.ok(roots).build();
    }

    @GET
    @Path("/{tenantId}/{unitId}/children")
    public Response children(
            @PathParam("tenantId") int tenantId,
            @PathParam("unitId") long unitId,
            @QueryParam("limit") Integer limit
    ) {
        log.debug("TreeResource::children({}, {}, {})", tenantId, unitId, limit);

        int size = limit == null ? 50 : Math.max(1, limit);

        SearchExpression expr = QueryBuilder.constrainToSpecificTenant(tenantId);
        try {
            expr = QueryBuilder.assembleAnd(expr, LeftRelationSearchItem.constrainOnLeftRelationEQ(
                    RelationType.PARENT_CHILD_RELATION,
                    new Unit.Id(tenantId, unitId)
            ));
        } catch (InvalidParameterException ex) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(Map.of("error", ex.getMessage()))
                    .build();
        }

        SearchOrder order = SearchOrder.orderByUnitId(true);
        List<JsonNode> units = new ArrayList<>();

        try {
            SearchResult result = repository.searchUnit(1, size, 0, expr, order);
            for (Unit unit : result.results()) {
                String json = unit.asJson(/* pretty? */ false);
                units.add(MAPPER.readTree(json.getBytes(StandardCharsets.UTF_8)));
            }
        } catch (RuntimeException ex) {
            log.error("Failed to load tree children", ex);
            return Response.serverError()
                    .entity(Map.of("error", "Failed to load tree children"))
                    .build();
        } catch (Exception ex) {
            log.error("Failed to parse child unit JSON", ex);
            return Response.serverError()
                    .entity(Map.of("error", "Failed to parse child unit payload"))
                    .build();
        }

        log.debug("-> children: {}", units);
        return Response.ok(units).build();
    }
}
