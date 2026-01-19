package org.gautelis.ipto.api;

import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.gautelis.ipto.repo.model.Repository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.sql.Connection;
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

    private static class RecordNotFoundException extends RuntimeException {
        RecordNotFoundException(String message) {
            super(message);
        }
    }

    private static String trimToNull(Object value) {
        if (value == null) {
            return null;
        }
        String text = String.valueOf(value).trim();
        return text.isEmpty() ? null : text;
    }

    private static Integer toInteger(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Number number) {
            return number.intValue();
        }
        String text = String.valueOf(value).trim();
        if (text.isEmpty()) {
            return null;
        }
        try {
            return Integer.parseInt(text);
        } catch (NumberFormatException ex) {
            return null;
        }
    }

    private static void safeRollback(Connection conn) {
        try {
            conn.rollback();
        } catch (SQLException sqle) {
            log.warn("Failed to rollback transaction: {}", sqle.getMessage());
        }
    }

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

        log.trace("-> records: {}", records);
        return Response.ok(records).build();
    }

    @GET
    @Path("/{recordId}")
    public Response get(@PathParam("recordId") int recordId) {
        log.debug("RecordResource::get({})", recordId);

        if (recordId <= 0) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(Map.of("error", "recordId must be positive"))
                    .build();
        }

        String recordSql = """
            SELECT recordid, name
            FROM repo_record_template
            WHERE recordid = ?
            """;

        String fieldsSql = """
            SELECT rte.attrid, rte.idx, rte.alias, attr.attrname, attr.alias AS attr_alias
            FROM repo_record_template_elements AS rte
            JOIN repo_attribute AS attr ON attr.attrid = rte.attrid
            WHERE rte.recordid = ?
            ORDER BY rte.idx
            """;

        Map<String, Object> record = new LinkedHashMap<>();
        List<Map<String, Object>> fields = new ArrayList<>();

        try {
            repository.withConnection(conn -> {
                try {
                    try (PreparedStatement stmt = conn.prepareStatement(recordSql)) {
                        stmt.setInt(1, recordId);
                        try (ResultSet rs = stmt.executeQuery()) {
                            if (rs.next()) {
                                record.put("_id", rs.getInt("recordid"));
                                record.put("_name", rs.getString("name"));
                            }
                        }
                    }

                    if (record.isEmpty()) {
                        return;
                    }

                    try (PreparedStatement stmt = conn.prepareStatement(fieldsSql)) {
                        stmt.setInt(1, recordId);
                        try (ResultSet rs = stmt.executeQuery()) {
                            while (rs.next()) {
                                Map<String, Object> field = new LinkedHashMap<>();
                                field.put("attrId", rs.getInt("attrid"));
                                field.put("alias", rs.getString("alias"));
                                String attrName = rs.getString("attr_alias");
                                if (attrName == null || attrName.isBlank()) {
                                    attrName = rs.getString("attrname");
                                }
                                field.put("name", attrName);
                                fields.add(field);
                            }
                        }
                    }
                } catch (SQLException sqle) {
                    throw new RuntimeException(sqle);
                }
            });
        } catch (SQLException | RuntimeException ex) {
            return Response.serverError()
                    .entity(Map.of("error", "Failed to load record template"))
                    .build();
        }

        if (record.isEmpty()) {
            return Response.status(Response.Status.NOT_FOUND)
                    .entity(Map.of("error", "Record template not found"))
                    .build();
        }

        record.put("_fields", fields);

        log.trace("-> record: {}", record);
        return Response.ok(record).build();
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    public Response create(Map<String, Object> payload) {
        log.debug("RecordResource::create()");

        Integer recordId = toInteger(payload.get("recordId"));
        String name = trimToNull(payload.get("name"));
        Object fieldsObj = payload.get("fields");

        if (recordId == null || recordId <= 0 || name == null) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(Map.of("error", "recordId and name are required"))
                    .build();
        }

        List<Map<String, Object>> fields = new ArrayList<>();
        if (fieldsObj instanceof List<?> rawList) {
            for (Object entry : rawList) {
                if (entry instanceof Map<?, ?> map) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> casted = (Map<String, Object>) map;
                    fields.add(casted);
                }
            }
        }

        String recordSql = """
            INSERT INTO repo_record_template (recordid, name)
            VALUES (?,?)
            """;

        String elementsSql = """
            INSERT INTO repo_record_template_elements (recordid, attrid, idx, alias)
            VALUES (?,?,?,?)
            """;

        try {
            repository.withConnection(conn -> {
                try {
                    conn.setAutoCommit(false);

                    try (PreparedStatement stmt = conn.prepareStatement(recordSql)) {
                        stmt.setInt(1, recordId);
                        stmt.setString(2, name);
                        stmt.executeUpdate();
                    }

                    if (!fields.isEmpty()) {
                        try (PreparedStatement stmt = conn.prepareStatement(elementsSql)) {
                            int idx = 0;
                            for (Map<String, Object> field : fields) {
                                Integer attrId = toInteger(field.get("attrId"));
                                String alias = trimToNull(field.get("alias"));
                                if (attrId == null || attrId <= 0) {
                                    throw new IllegalArgumentException("Invalid attrId for record field");
                                }

                                stmt.setInt(1, recordId);
                                stmt.setInt(2, attrId);
                                stmt.setInt(3, ++idx);
                                if (alias == null) {
                                    stmt.setNull(4, Types.VARCHAR);
                                } else {
                                    stmt.setString(4, alias);
                                }
                                stmt.addBatch();
                            }
                            stmt.executeBatch();
                        }
                    }

                    conn.commit();
                } catch (IllegalArgumentException ex) {
                    safeRollback(conn);
                    throw ex;
                } catch (SQLException sqle) {
                    String sqlState = sqle.getSQLState();
                    safeRollback(conn);
                    if (sqlState != null && sqlState.startsWith("23")) {
                        throw new IllegalStateException("Record template already exists", sqle);
                    }
                    throw new RuntimeException(sqle);
                }
            });
        } catch (IllegalArgumentException ex) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(Map.of("error", ex.getMessage()))
                    .build();
        } catch (IllegalStateException ex) {
            return Response.status(Response.Status.CONFLICT)
                    .entity(Map.of("error", ex.getMessage()))
                    .build();
        } catch (SQLException | RuntimeException ex) {
            return Response.serverError()
                    .entity(Map.of("error", "Failed to create record template"))
                    .build();
        }

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("_id", recordId);
        response.put("_name", name);
        response.put("_fields", fields);

        log.trace("-> response: {}", response);
        return Response.ok(response).build();
    }

    @PUT
    @Path("/{recordId}")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response update(@PathParam("recordId") int recordId, Map<String, Object> payload) {
        log.debug("RecordResource::update({})", recordId);

        if (recordId <= 0) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(Map.of("error", "recordId must be positive"))
                    .build();
        }

        String name = trimToNull(payload.get("name"));
        Object fieldsObj = payload.get("fields");

        List<Map<String, Object>> fields = new ArrayList<>();
        if (fieldsObj instanceof List<?> rawList) {
            for (Object entry : rawList) {
                if (entry instanceof Map<?, ?> map) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> casted = (Map<String, Object>) map;
                    fields.add(casted);
                }
            }
        } else {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(Map.of("error", "fields must be an array"))
                    .build();
        }

        String recordSql = """
            SELECT name
            FROM repo_record_template
            WHERE recordid = ?
            """;

        String updateSql = """
            UPDATE repo_record_template
            SET name = ?
            WHERE recordid = ?
            """;

        String deleteSql = """
            DELETE FROM repo_record_template_elements
            WHERE recordid = ?
            """;

        String elementsSql = """
            INSERT INTO repo_record_template_elements (recordid, attrid, idx, alias)
            VALUES (?,?,?,?)
            """;

        final String[] resolvedName = { name };

        try {
            repository.withConnection(conn -> {
                try {
                    conn.setAutoCommit(false);

                    String existingName = null;
                    try (PreparedStatement stmt = conn.prepareStatement(recordSql)) {
                        stmt.setInt(1, recordId);
                        try (ResultSet rs = stmt.executeQuery()) {
                            if (rs.next()) {
                                existingName = rs.getString("name");
                            }
                        }
                    }

                    if (existingName == null) {
                        throw new RecordNotFoundException("Record template does not exist");
                    }

                    if (resolvedName[0] == null) {
                        resolvedName[0] = existingName;
                    } else {
                        try (PreparedStatement stmt = conn.prepareStatement(updateSql)) {
                            stmt.setString(1, resolvedName[0]);
                            stmt.setInt(2, recordId);
                            stmt.executeUpdate();
                        }
                    }

                    try (PreparedStatement stmt = conn.prepareStatement(deleteSql)) {
                        stmt.setInt(1, recordId);
                        stmt.executeUpdate();
                    }

                    if (!fields.isEmpty()) {
                        try (PreparedStatement stmt = conn.prepareStatement(elementsSql)) {
                            int idx = 0;
                            for (Map<String, Object> field : fields) {
                                Integer attrId = toInteger(field.get("attrId"));
                                String alias = trimToNull(field.get("alias"));
                                if (attrId == null || attrId <= 0) {
                                    throw new IllegalArgumentException("Invalid attrId for record field");
                                }

                                stmt.setInt(1, recordId);
                                stmt.setInt(2, attrId);
                                stmt.setInt(3, ++idx);
                                if (alias == null) {
                                    stmt.setNull(4, Types.VARCHAR);
                                } else {
                                    stmt.setString(4, alias);
                                }
                                stmt.addBatch();
                            }
                            stmt.executeBatch();
                        }
                    }

                    conn.commit();
                } catch (RecordNotFoundException | IllegalArgumentException ex) {
                    safeRollback(conn);
                    throw ex;
                } catch (SQLException sqle) {
                    safeRollback(conn);
                    throw new RuntimeException(sqle);
                }
            });
        } catch (RecordNotFoundException ex) {
            return Response.status(Response.Status.NOT_FOUND)
                    .entity(Map.of("error", ex.getMessage()))
                    .build();
        } catch (IllegalArgumentException ex) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(Map.of("error", ex.getMessage()))
                    .build();
        } catch (SQLException | RuntimeException ex) {
            return Response.serverError()
                    .entity(Map.of("error", "Failed to update record template"))
                    .build();
        }

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("_id", recordId);
        response.put("_name", resolvedName[0]);
        response.put("_fields", fields);

        log.trace("-> response: {}", response);
        return Response.ok(response).build();
    }
}
