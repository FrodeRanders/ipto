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

@Path("/api/templates")
@Produces(MediaType.APPLICATION_JSON)
public class TemplateResource {
    private static final Logger log = LoggerFactory.getLogger(TemplateResource.class);

    @Inject
    Repository repository;

    @GET
    public Response list() {
        log.debug("TemplateResource::list()");

        List<Map<String, Object>> templates = new ArrayList<>();

        String templateSql = """
            SELECT templateid, name
            FROM repo_unit_template
            ORDER BY name
            """;

        String fieldsSql = """
            SELECT templateid, idx, alias
            FROM repo_unit_template_elements
            ORDER BY templateid, idx
            """;

        try {
            repository.withConnection(conn -> {
                Map<Integer, Map<String, Object>> byId = new LinkedHashMap<>();
                try {
                    try (PreparedStatement stmt = conn.prepareStatement(templateSql);
                         ResultSet rs = stmt.executeQuery()) {
                        while (rs.next()) {
                            int templateId = rs.getInt("templateid");
                            Map<String, Object> row = new LinkedHashMap<>();
                            row.put("_id", templateId);
                            row.put("_name", rs.getString("name"));
                            row.put("_attributes", new ArrayList<String>());
                            byId.put(templateId, row);
                        }
                    }

                    try (PreparedStatement stmt = conn.prepareStatement(fieldsSql);
                         ResultSet rs = stmt.executeQuery()) {
                        while (rs.next()) {
                            int templateId = rs.getInt("templateid");
                            Map<String, Object> row = byId.get(templateId);
                            if (row == null) {
                                continue;
                            }
                            @SuppressWarnings("unchecked")
                            List<String> attributes = (List<String>) row.get("_attributes");
                            if (attributes != null) {
                                attributes.add(rs.getString("alias"));
                            }
                        }
                    }
                } catch (SQLException sqle) {
                    throw new RuntimeException(sqle);
                }

                templates.addAll(byId.values());
            });
        } catch (SQLException | RuntimeException ex) {
            return Response.serverError()
                    .entity(Map.of("error", "Failed to load templates"))
                    .build();
        }

        log.trace("-> templates: {}", templates);
        return Response.ok(templates).build();
    }
}
