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

@Path("/api/records")
@Produces(MediaType.APPLICATION_JSON)
public class RecordResource {
    private static final Logger log = LoggerFactory.getLogger(RecordResource.class);

    @Inject
    Repository repository;

    @GET
    public Response list() {
        log.debug("RecordResource::list()");

        List<Map<String, Object>> records = new ArrayList<>();

        String recordSql = """
            SELECT recordid, name
            FROM repo_record_template
            ORDER BY name
            """;

        String fieldsSql = """
            SELECT recordid, idx, alias
            FROM repo_record_template_elements
            ORDER BY recordid, idx
            """;

        try {
            repository.withConnection(conn -> {
                Map<Integer, Map<String, Object>> byId = new LinkedHashMap<>();
                try {
                    try (PreparedStatement stmt = conn.prepareStatement(recordSql);
                         ResultSet rs = stmt.executeQuery()) {
                        while (rs.next()) {
                            int recordId = rs.getInt("recordid");
                            Map<String, Object> row = new LinkedHashMap<>();
                            row.put("_id", recordId);
                            row.put("_name", rs.getString("name"));
                            row.put("_fields", new ArrayList<String>());
                            byId.put(recordId, row);
                        }
                    }

                    try (PreparedStatement stmt = conn.prepareStatement(fieldsSql);
                         ResultSet rs = stmt.executeQuery()) {
                        while (rs.next()) {
                            int recordId = rs.getInt("recordid");
                            Map<String, Object> row = byId.get(recordId);
                            if (row == null) {
                                continue;
                            }
                            @SuppressWarnings("unchecked")
                            List<String> fields = (List<String>) row.get("_fields");
                            if (fields != null) {
                                fields.add(rs.getString("alias"));
                            }
                        }
                    }
                } catch (SQLException sqle) {
                    throw new RuntimeException(sqle);
                }

                records.addAll(byId.values());
            });
        } catch (SQLException | RuntimeException ex) {
            return Response.serverError()
                    .entity(Map.of("error", "Failed to load records"))
                    .build();
        }

        log.debug("-> records: {}", records);
        return Response.ok(records).build();
    }
}
