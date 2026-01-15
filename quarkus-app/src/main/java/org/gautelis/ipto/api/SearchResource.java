package org.gautelis.ipto.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.gautelis.ipto.repo.exceptions.InvalidParameterException;
import org.gautelis.ipto.repo.model.Repository;
import org.gautelis.ipto.repo.model.Unit;
import org.gautelis.ipto.repo.search.SearchResult;
import org.gautelis.ipto.repo.search.query.AndExpression;
import org.gautelis.ipto.repo.search.query.SearchExpression;
import org.gautelis.ipto.repo.search.query.SearchOrder;
import org.gautelis.ipto.repo.search.query.QueryBuilder;
import org.gautelis.ipto.repo.search.query.SearchExpressionQueryParser;
import org.gautelis.ipto.repo.search.query.SearchExpressionQueryParser.AttributeNameMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Path("/api/searches")
@Produces(MediaType.APPLICATION_JSON)
public class SearchResource {
    private static final Logger log = LoggerFactory.getLogger(SearchResource.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Inject
    Repository repository;

    @GET
    public Response list() {
        log.info("SearchResource::list()");

        log.debug("-> list: <empty>");
        return Response.ok(List.of()).build();
    }

    @POST
    @Path("/units")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response searchUnits(SearchRequest request) {
        log.debug("SearchResource::searchUnits({})", request);

        if (request == null || request.tenantId() == null) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(Map.of("error", "tenantId is required"))
                    .build();
        }

        int tenantId = request.tenantId();
        int offset = request.offset() == null ? 0 : Math.max(0, request.offset());
        int size = request.size() == null ? 20 : Math.max(1, request.size());

        SearchExpression expr = QueryBuilder.constrainToSpecificTenant(tenantId);
        expr = QueryBuilder.assembleAnd(expr, QueryBuilder.constrainToSpecificStatus(Unit.Status.EFFECTIVE));

        String where = request.where();
        if (where != null && !where.isBlank()) {
            try {
                SearchExpression textExpr = SearchExpressionQueryParser.parse(
                        where,
                        repository,
                        AttributeNameMode.NAMES_OR_ALIASES
                );
                expr = new AndExpression(expr, textExpr);
            } catch (InvalidParameterException ex) {
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity(Map.of("error", ex.getMessage()))
                        .build();
            }
        }

        SearchOrder order = SearchOrder.orderByUnitId(true);
        List<JsonNode> units = new ArrayList<>();

        try {
            int reqlow = offset + 1;
            int reqhigh = offset + size;
            SearchResult result = repository.searchUnit(reqlow, reqhigh, 0, expr, order);
            for (Unit unit : result.results()) {
                String json = unit.asJson(/* pretty? */ false);
                units.add(MAPPER.readTree(json.getBytes(StandardCharsets.UTF_8)));
            }
        } catch (RuntimeException ex) {
            log.error("Failed to execute search", ex);
            return Response.serverError()
                    .entity(Map.of("error", "Failed to execute search"))
                    .build();
        } catch (Exception ex) {
            log.error("Failed to parse unit JSON", ex);
            return Response.serverError()
                    .entity(Map.of("error", "Failed to parse unit payload"))
                    .build();
        }

        log.debug("-> units: {}", units);
        return Response.ok(units).build();
    }
}
